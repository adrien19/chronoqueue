package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservicepb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/domainerror"
	"github.com/adrien19/chronoqueue/pkg/metrics"
	repositorycommon "github.com/adrien19/chronoqueue/pkg/repository/common"
	repositorysql "github.com/adrien19/chronoqueue/pkg/repository/sql"
	"github.com/adrien19/chronoqueue/pkg/repository/sql/priority"
)

// generateID generates a random ID
func (s *Storage) generateID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		s.Logger.Fatal("crypto/rand.Read failed", "error", err)
	}
	return hex.EncodeToString(b)
}

func (s *Storage) requireOneOwnedMessage(ctx context.Context, tx *sql.Tx, result sql.Result, queueName string, messageId string, attemptId string, workerId string, nowMs int64) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if rows != 1 {
		return s.classifyOwnedMessageFailure(ctx, tx, queueName, messageId, attemptId, workerId, nowMs)
	}
	return nil
}

func (s *Storage) classifyOwnedMessageFailure(ctx context.Context, tx *sql.Tx, queueName string, messageId string, attemptId string, workerId string, nowMs int64) error {
	var state messagepb.Message_Metadata_State
	var currentAttemptID, currentWorkerID sql.NullString
	var leaseExpiry, heartbeatExpiry sql.NullInt64
	err := tx.QueryRowContext(ctx, `
		SELECT state, current_attempt_id, current_worker_id, lease_expiry, heartbeat_expiry
		FROM cq_messages
		WHERE queue_name = ? AND message_id = ? AND deleted_at IS NULL`, queueName, messageId).
		Scan(&state, &currentAttemptID, &currentWorkerID, &leaseExpiry, &heartbeatExpiry)
	if err == sql.ErrNoRows {
		return domainerror.New(domainerror.NotFound, fmt.Sprintf("message %q not found", messageId), err)
	}
	if err != nil {
		return fmt.Errorf("classify message ownership: %w", err)
	}
	if state != messagepb.Message_Metadata_RUNNING {
		return domainerror.New(domainerror.FailedPrecondition, "message is not running", nil)
	}
	if !currentAttemptID.Valid || !currentWorkerID.Valid || currentAttemptID.String != attemptId || currentWorkerID.String != workerId {
		return domainerror.New(domainerror.FailedPrecondition, "message is owned by another attempt", nil)
	}
	if !leaseExpiry.Valid || leaseExpiry.Int64 <= nowMs || (heartbeatExpiry.Valid && heartbeatExpiry.Int64 > 0 && heartbeatExpiry.Int64 <= nowMs) {
		return domainerror.New(domainerror.DeadlineExceeded, "message lease has expired", nil)
	}
	return domainerror.New(domainerror.FailedPrecondition, "message state changed during the operation", nil)
}

// calculateDeletion determines deletion behavior based on retention policy.
// Returns whether to hard delete immediately and the deleted_at timestamp (if soft delete).
func (s *Storage) calculateDeletion(policy *queuepb.MessageRetentionPolicy) (shouldHardDelete bool, deletedAtMs *int64) {
	if policy == nil || policy.Mode == queuepb.MessageRetentionPolicy_DELETE_IMMEDIATELY {
		return true, nil
	}

	nowMs := s.nowMs()

	switch policy.Mode {
	case queuepb.MessageRetentionPolicy_RETAIN_DURATION:
		retentionMs := policy.RetentionSeconds * 1000
		futureDeleteMs := nowMs + retentionMs
		return false, &futureDeleteMs

	case queuepb.MessageRetentionPolicy_RETAIN_FOREVER:
		return false, nil

	default:
		return true, nil
	}
}

func (s *Storage) EnqueueMessage(ctx context.Context, queueName string, message *messagepb.Message) error {
	start := time.Now()
	defer func() {
		metrics.ObserveDBTransaction("sqlite", "enqueue_message", time.Since(start))
	}()

	var stateStr string
	err := s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		var txErr error
		stateStr, txErr = s.enqueueMessageInTx(ctx, tx, queueName, message)
		return txErr
	})
	if err == nil {
		metrics.IncrementMessagesEnqueued(queueName)
		metrics.RecordStateTransition(queueName, "none", stateStr)
	}
	return err
}

// EnqueueMessagesBulk enqueues multiple messages in a single database operation.
// transactionMode controls the behavior:
//   - ALL_OR_NOTHING (0): All messages succeed or all fail (atomic transaction)
//   - BEST_EFFORT (1): Each message processed independently, partial success allowed
//
// Returns:
//   - []error: Per-message errors (nil for success, error for failure)
//   - error: Overall operation error (non-nil only for ALL_OR_NOTHING rollback)
func (s *Storage) EnqueueMessagesBulk(ctx context.Context, queueName string, messages []*messagepb.Message, transactionMode queueservicepb.PostMessagesBulkRequest_TransactionMode) ([]error, error) {
	start := time.Now()
	defer func() {
		metrics.ObserveDBTransaction("sqlite", "enqueue_messages_bulk", time.Since(start))
	}()

	messageErrors := make([]error, len(messages))

	// ALL_OR_NOTHING: Single transaction, all succeed or all fail
	if transactionMode == queueservicepb.PostMessagesBulkRequest_ALL_OR_NOTHING {
		txErr := s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
			for i, message := range messages {
				if _, err := s.enqueueMessageInTx(ctx, tx, queueName, message); err != nil {
					messageErrors[i] = err
					return fmt.Errorf("message[%d] failed: %w", i, err)
				}
			}
			return nil
		})

		if txErr != nil {
			// Transaction rolled back - mark all messages as failed
			for j := range messages {
				if messageErrors[j] == nil {
					messageErrors[j] = fmt.Errorf("transaction failed: %w", txErr)
				}
			}
			return messageErrors, txErr
		}

		// Transaction committed, all messages succeeded - record metrics
		for _, message := range messages {
			metrics.RecordStateTransition(queueName, "none", message.GetMetadata().GetState().String())
		}
		metrics.IncrementMessagesBulkEnqueued(queueName, int64(len(messages)))
		return messageErrors, nil
	}

	// BEST_EFFORT: Process each message independently
	successCount := int64(0)
	for i, message := range messages {
		var stateStr string
		err := s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
			var txErr error
			stateStr, txErr = s.enqueueMessageInTx(ctx, tx, queueName, message)
			return txErr
		})

		messageErrors[i] = err
		if err == nil {
			metrics.RecordStateTransition(queueName, "none", stateStr)
			successCount++
		}
	}

	if successCount > 0 {
		metrics.IncrementMessagesBulkEnqueued(queueName, successCount)
	}

	return messageErrors, nil
}

// enqueueMessageInTx enqueues a single message within an existing transaction.
// This is the internal implementation used by both EnqueueMessage and EnqueueMessagesBulk.
// Returns the state string for deferred metrics recording.
func (s *Storage) enqueueMessageInTx(ctx context.Context, tx *sql.Tx, queueName string, message *messagepb.Message) (string, error) {
	if err := repositorycommon.EncryptMessagePayload(message, s.KeyManager); err != nil {
		return "", fmt.Errorf("encrypt message payload: %w", err)
	}

	messageBytes, err := s.Serializer.MarshalMessage(message)
	if err != nil {
		return "", fmt.Errorf("marshal message: %w", err)
	}

	metadata := message.GetMetadata()
	if metadata == nil {
		return "", fmt.Errorf("message metadata is nil")
	}

	query := `
		INSERT INTO cq_messages (
			message_id, queue_name, state, attempts_left, max_attempts,
			priority, scheduled_at, metadata_pb, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT(queue_name, message_id) DO NOTHING
	`

	// Extract scheduled time in milliseconds if present
	var scheduledTimeMs *int64
	if metadata.ScheduledTime != nil {
		ms := metadata.ScheduledTime.AsTime().UnixMilli()
		scheduledTimeMs = &ms
	}

	result, err := tx.ExecContext(
		ctx, query,
		message.MessageId,
		queueName,
		metadata.State,
		metadata.AttemptsLeft,
		metadata.MaxAttempts,
		metadata.Priority,
		scheduledTimeMs,
		messageBytes,
	)
	if err != nil {
		return "", fmt.Errorf("insert message: %w", err)
	}

	// Check if the row was actually inserted (not a duplicate)
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("check rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return "", fmt.Errorf("%w: %s", repositorycommon.ErrDuplicateMessageID, message.MessageId)
	}

	if err := s.StateManager.UpdateCounters(ctx, tx, queueName, 0, metadata.State); err != nil {
		return "", fmt.Errorf("update counters: %w", err)
	}

	return metadata.State.String(), nil
}

// ClaimMessage claims the next available message from a queue
func (s *Storage) ClaimMessage(ctx context.Context, queueName string, workerId string, attemptId string, exclusivityKey string) (*messagepb.Message, error) {
	return s.ClaimMessageWithLeaseDuration(ctx, queueName, workerId, attemptId, exclusivityKey, 0)
}

// ClaimMessageWithLeaseDuration claims the next available message with an optional lease override.
func (s *Storage) ClaimMessageWithLeaseDuration(ctx context.Context, queueName string, workerId string, attemptId string, exclusivityKey string, leaseDuration time.Duration) (*messagepb.Message, error) {
	start := time.Now()
	defer func() {
		metrics.ObserveMessageClaimLatency(queueName, time.Since(start))
		metrics.ObserveDBTransaction("sqlite", "claim_message", time.Since(start))
	}()

	queueMeta, err := s.GetQueueMetadata(ctx, queueName)
	if err != nil {
		return nil, fmt.Errorf("get queue metadata: %w", err)
	}
	if queueMeta == nil {
		queueMeta = &queuepb.QueueMetadata{}
	}
	if err := repositorycommon.ValidateClaimExclusivity(queueMeta, exclusivityKey); err != nil {
		return nil, domainerror.New(domainerror.InvalidArgument, err.Error(), err)
	}
	isExclusive := queueMeta.GetType() == queuepb.QueueType_EXCLUSIVE

	// Generate IDs if not provided
	if attemptId == "" {
		attemptId = s.generateID()
	}
	if workerId == "" {
		workerId = "worker-" + s.generateID()[:8]
	}

	priorityCfg := queueMeta.GetPriorityConfig()
	useWeighted := priorityCfg != nil && priorityCfg.GetPolicy() != queuepb.FairnessPolicy_STRICT

	selectedLevel := ""
	if useWeighted {
		calculator := priority.NewPriorityWeightCalculator(s.BaseSQL)
		weights, err := calculator.CalculateWeights(ctx, queueName, queueMeta)
		if err != nil {
			return nil, err
		}

		selectedLevel = calculator.SelectPriorityLevel(weights)
		if selectedLevel == "" {
			return nil, nil
		}
	}

	var message *messagepb.Message

	err = s.WithSerializableTransaction(ctx, func(tx *sql.Tx) error {
		if isExclusive {
			result, err := tx.ExecContext(ctx, `UPDATE cq_queues SET updated_at = updated_at WHERE name = ?`, queueName)
			if err != nil {
				return fmt.Errorf("lock exclusive queue: %w", err)
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("lock exclusive queue: %w", err)
			}
			if rows != 1 {
				return domainerror.New(domainerror.NotFound, fmt.Sprintf("queue %q not found", queueName), nil)
			}

			var hasActiveLease bool
			nowMs := s.Clock.NowMs()
			if err := tx.QueryRowContext(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM cq_messages
					WHERE queue_name = ? AND state = ? AND lease_expiry > ? AND deleted_at IS NULL
				)`, queueName, messagepb.Message_Metadata_RUNNING, nowMs).Scan(&hasActiveLease); err != nil {
				return fmt.Errorf("check exclusive queue lease: %w", err)
			}
			if hasActiveLease {
				return nil
			}
		}

		// SQLite doesn't support SKIP LOCKED, so we use simple SELECT with LIMIT
		strictQuery := `
			SELECT id, message_id, metadata_pb, state, attempts_left
			FROM cq_messages
			WHERE queue_name = ? AND state = ? AND (scheduled_at IS NULL OR scheduled_at <= ?)
			  AND deleted_at IS NULL
			ORDER BY priority DESC, id ASC
			LIMIT 1
		`

		weightedQuery := `
			SELECT id, message_id, metadata_pb, state, attempts_left
			FROM cq_messages
			WHERE queue_name = ? AND state = ? AND (scheduled_at IS NULL OR scheduled_at <= ?)
			  AND priority BETWEEN ? AND ?
			  AND deleted_at IS NULL
			ORDER BY priority DESC, id ASC
			LIMIT 1
		`

		nowMs := s.Clock.NowMs()
		var id int64
		var messageId string
		var messageBytes []byte
		var oldState messagepb.Message_Metadata_State
		var attemptsLeft int32

		query := strictQuery
		args := []interface{}{queueName, messagepb.Message_Metadata_PENDING, nowMs}

		if useWeighted {
			minPriority, maxPriority := priority.PriorityLevelToRange(selectedLevel)
			query = weightedQuery
			args = append(args, minPriority, maxPriority)
		}

		err := tx.QueryRowContext(ctx, query, args...).Scan(&id, &messageId, &messageBytes, &oldState, &attemptsLeft)
		if err == sql.ErrNoRows && useWeighted {
			query = strictQuery
			args = args[:3]
			err = tx.QueryRowContext(ctx, query, args...).Scan(&id, &messageId, &messageBytes, &oldState, &attemptsLeft)
		}
		if err == sql.ErrNoRows {
			// Gracefully indicate empty queue: no message claimed, no error.
			return nil
		}
		if err != nil {
			return fmt.Errorf("query message: %w", err)
		}

		// Unmarshal message
		msg, err := s.Serializer.UnmarshalMessage(messageBytes)
		if err != nil {
			return fmt.Errorf("unmarshal message: %w", err)
		}

		if err := repositorycommon.DecryptMessagePayload(msg, s.KeyManager); err != nil {
			return fmt.Errorf("decrypt message payload: %w", err)
		}

		// Calculate lease runtime
		leasePolicy := msg.GetMetadata().GetLeasePolicy()
		if leasePolicy == nil {
			return fmt.Errorf("message has no lease policy")
		}
		var leaseRuntime *repositorysql.LeaseRuntime
		if leaseDuration > 0 {
			leaseRuntime = s.LeaseRuntime.CalculateLeaseRuntimeWithDuration(leasePolicy, leaseDuration)
		} else {
			leaseRuntime = s.LeaseRuntime.CalculateLeaseRuntime(leasePolicy)
		}

		// Update message to RUNNING state
		updateQuery := `
			UPDATE cq_messages
			SET state = ?,
				current_attempt_id = ?,
				current_worker_id = ?,
				lease_started_at = ?,
				lease_expiry = ?,
				lease_extension_used = 0,
				lease_renewal_count = 0,
				last_heartbeat_at = ?,
				heartbeat_expiry = ?,
				updated_at = CURRENT_TIMESTAMP
			WHERE id = ?
		`
		_, err = tx.ExecContext(
			ctx, updateQuery,
			messagepb.Message_Metadata_RUNNING,
			attemptId,
			workerId,
			leaseRuntime.LeaseStartedAt,
			leaseRuntime.LeaseExpiry,
			leaseRuntime.LastHeartbeatAt,
			leaseRuntime.HeartbeatExpiry,
			id,
		)
		if err != nil {
			return fmt.Errorf("update message: %w", err)
		}

		// Update message object's metadata to reflect the current runtime state
		// Note: We don't update metadata_pb in the database - runtime state is stored in columns
		if msg.Metadata == nil {
			msg.Metadata = &messagepb.Message_Metadata{}
		}
		msg.Metadata.State = messagepb.Message_Metadata_RUNNING
		msg.Metadata.AttemptsLeft = attemptsLeft
		if msg.Metadata.CurrentAttempt == nil {
			msg.Metadata.CurrentAttempt = &messagepb.Message_Metadata_AttemptRuntime{}
		}
		msg.Metadata.CurrentAttempt.AttemptId = attemptId
		msg.Metadata.CurrentAttempt.WorkerId = workerId
		msg.Metadata.LeaseRenewalCount = 0
		msg.Metadata.LeaseExpiry = leaseRuntime.LeaseExpiry

		// Update state counts
		if err := s.StateManager.UpdateCounters(ctx, tx, queueName, oldState, messagepb.Message_Metadata_RUNNING); err != nil {
			return fmt.Errorf("update state counts: %w", err)
		}

		message = msg
		return nil
	})

	if err == nil && message != nil {
		// Record successful claim
		metrics.IncrementMessagesDequeued(queueName)
		metrics.RecordStateTransition(queueName, "PENDING", "RUNNING")
	}

	return message, err
}

// AcknowledgeMessage marks a message as completed
func (s *Storage) AcknowledgeMessage(ctx context.Context, queueName string, messageId string, attemptId string, workerId string) error {
	// Fetch queue metadata outside transaction to avoid connection pool contention
	queueMeta, err := s.GetQueueMetadata(ctx, queueName)
	if err != nil {
		return fmt.Errorf("get queue metadata: %w", err)
	}
	retentionPolicy := queueMeta.GetMessageRetentionPolicy()
	var leaseStartedAt sql.NullInt64

	err = s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		shouldDelete, deletedAt := s.calculateDeletion(retentionPolicy)
		nowMs := s.nowMs()
		if err := tx.QueryRowContext(ctx, `SELECT lease_started_at FROM cq_messages WHERE queue_name = ? AND message_id = ?`, queueName, messageId).Scan(&leaseStartedAt); err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("query lease start: %w", err)
		}

		if shouldDelete {
			result, err := tx.ExecContext(ctx, `
				DELETE FROM cq_messages
				WHERE queue_name = ? AND message_id = ? AND state = ?
				  AND current_attempt_id = ? AND current_worker_id = ?
				  AND lease_expiry > ?
				  AND (heartbeat_expiry IS NULL OR heartbeat_expiry <= 0 OR heartbeat_expiry > ?)
				  AND deleted_at IS NULL`,
				queueName, messageId, messagepb.Message_Metadata_RUNNING, attemptId, workerId, nowMs, nowMs)
			if err != nil {
				return fmt.Errorf("delete message: %w", err)
			}

			rows, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("get rows affected: %w", err)
			}
			if rows == 0 {
				return s.classifyOwnedMessageFailure(ctx, tx, queueName, messageId, attemptId, workerId, nowMs)
			}
		} else {
			result, err := tx.ExecContext(ctx, `
				UPDATE cq_messages 
				SET state = ?,
					completed_at = ?,
					deleted_at = ?,
					current_attempt_id = NULL,
					current_worker_id = NULL,
					lease_started_at = NULL,
					lease_expiry = NULL,
					updated_at = ?
				WHERE queue_name = ? AND message_id = ? AND state = ?
				  AND current_attempt_id = ? AND current_worker_id = ?
				  AND lease_expiry > ?
				  AND (heartbeat_expiry IS NULL OR heartbeat_expiry <= 0 OR heartbeat_expiry > ?)
				  AND deleted_at IS NULL`,
				messagepb.Message_Metadata_COMPLETED,
				nowMs,
				deletedAt,
				nowMs,
				queueName, messageId, messagepb.Message_Metadata_RUNNING,
				attemptId, workerId, nowMs, nowMs)
			if err != nil {
				return fmt.Errorf("soft delete message: %w", err)
			}

			rows, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("get rows affected: %w", err)
			}
			if rows == 0 {
				return s.classifyOwnedMessageFailure(ctx, tx, queueName, messageId, attemptId, workerId, nowMs)
			}
		}

		return s.StateManager.UpdateCounters(ctx, tx, queueName, messagepb.Message_Metadata_RUNNING, messagepb.Message_Metadata_COMPLETED)
	})

	if err == nil {
		metrics.RecordStateTransition(queueName, "RUNNING", "COMPLETED")
		if leaseStartedAt.Valid {
			metrics.ObserveMessageProcessingDuration(queueName, time.Duration(s.nowMs()-leaseStartedAt.Int64)*time.Millisecond)
		}
	}

	return err
}

// NackMessage marks a message as failed
func (s *Storage) NackMessage(ctx context.Context, queueName string, messageId string, attemptId string, workerId string) error {
	var movedToDLQ bool
	var exhausted bool
	var dlqName string

	// Fetch queue metadata outside transaction to avoid connection pool contention
	queueMeta, err := s.GetQueueMetadata(ctx, queueName)
	if err != nil {
		return fmt.Errorf("get queue metadata: %w", err)
	}
	retentionPolicy := queueMeta.GetMessageRetentionPolicy()
	dlqName = queueMeta.GetDeadLetterQueueName()
	terminalQueueName := queueName
	if dlqName != "" {
		terminalQueueName = dlqName
	}

	err = s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		var messageBytes []byte
		var oldState messagepb.Message_Metadata_State
		var attemptsLeft int32
		nowMs := s.nowMs()
		query := `
			SELECT metadata_pb, state, attempts_left
			FROM cq_messages
			WHERE queue_name = ? AND message_id = ? AND state = ?
			  AND current_attempt_id = ? AND current_worker_id = ?
			  AND lease_expiry > ?
			  AND (heartbeat_expiry IS NULL OR heartbeat_expiry <= 0 OR heartbeat_expiry > ?)
			  AND deleted_at IS NULL`
		err := tx.QueryRowContext(ctx, query, queueName, messageId, messagepb.Message_Metadata_RUNNING, attemptId, workerId, nowMs, nowMs).Scan(&messageBytes, &oldState, &attemptsLeft)
		if err == sql.ErrNoRows {
			return s.classifyOwnedMessageFailure(ctx, tx, queueName, messageId, attemptId, workerId, nowMs)
		}
		if err != nil {
			return fmt.Errorf("query message: %w", err)
		}

		newState := messagepb.Message_Metadata_PENDING
		newAttemptsLeft := attemptsLeft
		if attemptsLeft != -1 {
			newAttemptsLeft--
		}
		if newAttemptsLeft != -1 && newAttemptsLeft <= 0 {
			exhausted = true
			newState = messagepb.Message_Metadata_ERRORED
			movedToDLQ = dlqName != ""
			shouldDelete, deletedAt := s.calculateDeletion(retentionPolicy)
			if dlqName != "" {
				shouldDelete, deletedAt = false, nil
			}

			if shouldDelete {
				deleteQuery := `
					DELETE FROM cq_messages
					WHERE queue_name = ? AND message_id = ? AND state = ?
					  AND current_attempt_id = ? AND current_worker_id = ?
					  AND lease_expiry > ?
					  AND (heartbeat_expiry IS NULL OR heartbeat_expiry <= 0 OR heartbeat_expiry > ?)
					  AND deleted_at IS NULL`
				result, deleteErr := tx.ExecContext(ctx, deleteQuery, queueName, messageId, messagepb.Message_Metadata_RUNNING, attemptId, workerId, nowMs, nowMs)
				err = deleteErr
				if err != nil {
					return fmt.Errorf("delete errored message: %w", err)
				}
				if err := s.requireOneOwnedMessage(ctx, tx, result, queueName, messageId, attemptId, workerId, nowMs); err != nil {
					return err
				}
			} else {
				updateQuery := `
					UPDATE cq_messages
					SET queue_name = ?, state = ?,
						attempts_left = ?,
						completed_at = ?,
						deleted_at = ?,
						current_attempt_id = NULL,
						current_worker_id = NULL,
						lease_started_at = NULL,
						lease_expiry = NULL,
						lease_extension_used = 0,
						lease_renewal_count = 0,
						last_heartbeat_at = NULL,
						heartbeat_expiry = NULL,
						updated_at = ?
					WHERE queue_name = ? AND message_id = ? AND state = ?
					  AND current_attempt_id = ? AND current_worker_id = ?
					  AND lease_expiry > ?
					  AND (heartbeat_expiry IS NULL OR heartbeat_expiry <= 0 OR heartbeat_expiry > ?)
					  AND deleted_at IS NULL
				`
				result, updateErr := tx.ExecContext(
					ctx, updateQuery,
					terminalQueueName,
					newState,
					newAttemptsLeft,
					nowMs,
					deletedAt,
					nowMs,
					queueName, messageId, messagepb.Message_Metadata_RUNNING,
					attemptId, workerId, nowMs, nowMs,
				)
				err = updateErr
				if err != nil {
					return fmt.Errorf("soft delete errored message: %w", err)
				}
				if err := s.requireOneOwnedMessage(ctx, tx, result, queueName, messageId, attemptId, workerId, nowMs); err != nil {
					return err
				}
			}
		} else {
			updateQuery := `
				UPDATE cq_messages
				SET state = ?,
					attempts_left = ?,
					current_attempt_id = NULL,
					current_worker_id = NULL,
					lease_started_at = NULL,
					lease_expiry = NULL,
					lease_extension_used = 0,
					lease_renewal_count = 0,
					last_heartbeat_at = NULL,
					heartbeat_expiry = NULL,
					updated_at = ?
				WHERE queue_name = ? AND message_id = ? AND state = ?
				  AND current_attempt_id = ? AND current_worker_id = ?
				  AND lease_expiry > ?
				  AND (heartbeat_expiry IS NULL OR heartbeat_expiry <= 0 OR heartbeat_expiry > ?)
				  AND deleted_at IS NULL
			`
			result, err := tx.ExecContext(ctx, updateQuery, newState, newAttemptsLeft, nowMs, queueName, messageId, messagepb.Message_Metadata_RUNNING, attemptId, workerId, nowMs, nowMs)
			if err != nil {
				return fmt.Errorf("update message: %w", err)
			}

			if err := s.requireOneOwnedMessage(ctx, tx, result, queueName, messageId, attemptId, workerId, nowMs); err != nil {
				return err
			}
		}

		if movedToDLQ && dlqName != "" {
			return s.StateManager.MoveCounter(ctx, tx, queueName, oldState, dlqName, newState)
		}
		return s.StateManager.UpdateCounters(ctx, tx, queueName, oldState, newState)
	})

	if err == nil {
		if exhausted {
			metrics.RecordStateTransition(queueName, "RUNNING", "ERRORED")
			if movedToDLQ {
				metrics.IncrementDLQIngestion(dlqName, queueName, "max_attempts")
			}
		} else {
			metrics.RecordStateTransition(queueName, "RUNNING", "PENDING")
		}
	}

	return err
}

// CancelMessage cancels a pending message before it has been processed.
// Only messages in INVISIBLE or PENDING state can be cancelled.
// The reason parameter is stored for audit trail purposes.
func (s *Storage) CancelMessage(ctx context.Context, queueName string, messageId string, reason string) error {
	var currentState messagepb.Message_Metadata_State
	var shouldDelete bool

	// Fetch queue metadata outside transaction to avoid connection pool contention
	queueMeta, err := s.GetQueueMetadata(ctx, queueName)
	if err != nil {
		return fmt.Errorf("get queue metadata: %w", err)
	}
	retentionPolicy := queueMeta.GetMessageRetentionPolicy()

	err = s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		// Verify message exists and is in cancellable state
		query := `SELECT state FROM cq_messages WHERE message_id = ? AND queue_name = ?`
		err := tx.QueryRowContext(ctx, query, messageId, queueName).Scan(&currentState)
		if err == sql.ErrNoRows {
			return domainerror.New(domainerror.NotFound, fmt.Sprintf("message %q not found", messageId), err)
		}
		if err != nil {
			return fmt.Errorf("query message: %w", err)
		}

		// Only allow cancellation of INVISIBLE or PENDING messages
		if currentState != messagepb.Message_Metadata_INVISIBLE && currentState != messagepb.Message_Metadata_PENDING {
			return domainerror.New(domainerror.FailedPrecondition, fmt.Sprintf("message cannot be canceled in state %s", currentState), nil)
		}

		// Calculate deletion behavior based on retention policy
		var deletedAt *int64
		shouldDelete, deletedAt = s.calculateDeletion(retentionPolicy)
		nowMs := s.nowMs()

		if shouldDelete {
			// Hard delete immediately - verify state hasn't changed
			deleteQuery := `DELETE FROM cq_messages WHERE message_id = ? AND queue_name = ? AND state IN (?, ?)`
			result, err := tx.ExecContext(ctx, deleteQuery, messageId, queueName, messagepb.Message_Metadata_INVISIBLE, messagepb.Message_Metadata_PENDING)
			if err != nil {
				return fmt.Errorf("delete cancelled message: %w", err)
			}

			rows, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("get rows affected: %w", err)
			}
			if rows == 0 {
				return domainerror.New(domainerror.FailedPrecondition, "message state changed during cancellation", nil)
			}
		} else {
			// Soft delete - mark as CANCELED and set deleted_at, verify state hasn't changed
			// Store cancellation reason for audit trail
			var reasonPtr *string
			if reason != "" {
				reasonPtr = &reason
			}
			updateQuery := `
				UPDATE cq_messages
				SET state = ?,
					completed_at = ?,
					deleted_at = ?,
					cancellation_reason = ?,
					current_attempt_id = NULL,
					current_worker_id = NULL,
					lease_started_at = NULL,
					lease_expiry = NULL,
					last_heartbeat_at = NULL,
					heartbeat_expiry = NULL,
					updated_at = ?
				WHERE message_id = ? AND queue_name = ? AND state IN (?, ?)
			`
			result, err := tx.ExecContext(
				ctx, updateQuery,
				messagepb.Message_Metadata_CANCELED,
				nowMs,
				deletedAt,
				reasonPtr,
				nowMs,
				messageId,
				queueName,
				messagepb.Message_Metadata_INVISIBLE,
				messagepb.Message_Metadata_PENDING,
			)
			if err != nil {
				return fmt.Errorf("soft delete cancelled message: %w", err)
			}

			rows, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("get rows affected: %w", err)
			}
			if rows == 0 {
				return domainerror.New(domainerror.FailedPrecondition, "message state changed during cancellation", nil)
			}
		}

		// Update state counters
		return s.StateManager.UpdateCounters(ctx, tx, queueName, currentState, messagepb.Message_Metadata_CANCELED)
	})

	if err == nil {
		// Log cancellation for hard-deleted messages (audit trail)
		if shouldDelete && reason != "" {
			s.Logger.Info("Message cancelled and deleted",
				"queue", queueName,
				"message_id", messageId,
				"state", currentState.String(),
				"reason", reason)
		}
		// Record state transition metric
		oldStateStr := currentState.String()
		if oldStateStr == "" {
			oldStateStr = "INVISIBLE"
		}
		metrics.RecordStateTransition(queueName, oldStateStr, "CANCELED")
	}

	return err
}

// ExtendMessageLease extends the lease on a message
func (s *Storage) ExtendMessageLease(ctx context.Context, queueName string, messageId string, attemptId string, workerId string, extensionMs int64) (int64, error) {
	var remainingTimeMs int64
	err := s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		var messageBytes []byte
		var currentLeaseExpiry int64
		var leaseExtensionUsed int64
		var currentRenewalCount int32
		nowMs := s.nowMs()
		query := `
			SELECT metadata_pb, lease_expiry, lease_extension_used, lease_renewal_count
			FROM cq_messages
			WHERE queue_name = ? AND message_id = ? AND state = ?
			  AND current_attempt_id = ? AND current_worker_id = ?
			  AND lease_expiry > ?
			  AND (heartbeat_expiry IS NULL OR heartbeat_expiry <= 0 OR heartbeat_expiry > ?)
			  AND deleted_at IS NULL`
		err := tx.QueryRowContext(ctx, query, queueName, messageId, messagepb.Message_Metadata_RUNNING, attemptId, workerId, nowMs, nowMs).Scan(&messageBytes, &currentLeaseExpiry, &leaseExtensionUsed, &currentRenewalCount)
		if err == sql.ErrNoRows {
			return s.classifyOwnedMessageFailure(ctx, tx, queueName, messageId, attemptId, workerId, nowMs)
		}
		if err != nil {
			return fmt.Errorf("query message: %w", err)
		}

		msg, err := s.Serializer.UnmarshalMessage(messageBytes)
		if err != nil {
			return fmt.Errorf("unmarshal message: %w", err)
		}

		// Check max_renewals limit
		leasePolicy := msg.GetMetadata().GetLeasePolicy()
		if leasePolicy != nil && leasePolicy.GetMaxRenewals() > 0 {
			if currentRenewalCount >= leasePolicy.GetMaxRenewals() {
				metrics.IncrementLeaseRenewals(queueName, "denied_max_renewals")
				message := fmt.Sprintf("lease renewal limit reached: %d/%d renewals used", currentRenewalCount, leasePolicy.GetMaxRenewals())
				return domainerror.New(domainerror.FailedPrecondition, message, nil)
			}
		}

		// Calculate new lease
		newLeaseRuntime, err := s.LeaseRuntime.ExtendLease(leasePolicy, currentLeaseExpiry, leaseExtensionUsed, extensionMs)
		if err != nil {
			return domainerror.New(domainerror.FailedPrecondition, "maximum lease extension reached", err)
		}

		updateQuery := `
			UPDATE cq_messages
			SET lease_expiry = ?,
				lease_extension_used = ?,
				lease_renewal_count = lease_renewal_count + 1,
				updated_at = CURRENT_TIMESTAMP
			WHERE queue_name = ? AND message_id = ? AND state = ?
			  AND current_attempt_id = ? AND current_worker_id = ?
			  AND lease_expiry > ?
			  AND (heartbeat_expiry IS NULL OR heartbeat_expiry <= 0 OR heartbeat_expiry > ?)
			  AND deleted_at IS NULL
		`
		result, err := tx.ExecContext(
			ctx, updateQuery,
			newLeaseRuntime.LeaseExpiry,
			newLeaseRuntime.LeaseExtensionUsed,
			queueName, messageId, messagepb.Message_Metadata_RUNNING,
			attemptId, workerId, nowMs, nowMs,
		)
		if err != nil {
			metrics.IncrementLeaseRenewals(queueName, "failed")
			return err
		}
		if err := s.requireOneOwnedMessage(ctx, tx, result, queueName, messageId, attemptId, workerId, nowMs); err != nil {
			metrics.IncrementLeaseRenewals(queueName, "failed")
			return err
		}
		metrics.IncrementLeaseRenewals(queueName, "success")
		remainingTimeMs = max(newLeaseRuntime.LeaseExpiry-s.nowMs(), 0)
		return nil
	})
	return remainingTimeMs, err
}

// HeartbeatMessage updates the heartbeat for a message
// Returns the current message state and remaining lease time in milliseconds.
func (s *Storage) HeartbeatMessage(ctx context.Context, queueName string, messageId string, attemptId string, workerId string) (messagepb.Message_Metadata_State, int64, error) {
	var currentState messagepb.Message_Metadata_State
	var remainingTimeMs int64

	err := s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		var messageBytes []byte
		var state messagepb.Message_Metadata_State
		var leaseExpiry, leaseExtensionUsed int64
		var leaseRenewalCount int32
		nowMs := s.Clock.NowMs()
		query := `
			SELECT metadata_pb, state, lease_expiry, lease_extension_used, lease_renewal_count
			FROM cq_messages
			WHERE queue_name = ? AND message_id = ? AND state = ?
			  AND current_attempt_id = ? AND current_worker_id = ?
			  AND lease_expiry > ?
			  AND (heartbeat_expiry IS NULL OR heartbeat_expiry <= 0 OR heartbeat_expiry > ?)
			  AND deleted_at IS NULL`
		err := tx.QueryRowContext(ctx, query, queueName, messageId, messagepb.Message_Metadata_RUNNING, attemptId, workerId, nowMs, nowMs).Scan(&messageBytes, &state, &leaseExpiry, &leaseExtensionUsed, &leaseRenewalCount)
		if err == sql.ErrNoRows {
			return s.classifyOwnedMessageFailure(ctx, tx, queueName, messageId, attemptId, workerId, nowMs)
		}
		if err != nil {
			return fmt.Errorf("query message: %w", err)
		}

		msg, err := s.Serializer.UnmarshalMessage(messageBytes)
		if err != nil {
			return fmt.Errorf("unmarshal message: %w", err)
		}

		leasePolicy := msg.GetMetadata().GetLeasePolicy()
		var heartbeatExpiry any
		if timeout := leasePolicy.GetHeartbeatTimeout(); timeout != nil && timeout.AsDuration() > 0 {
			heartbeatExpiry = nowMs + timeout.AsDuration().Milliseconds()
		}

		newLeaseExpiry := leaseExpiry
		newExtensionUsed := leaseExtensionUsed
		newRenewalCount := leaseRenewalCount
		canRenew := leasePolicy.GetMaxRenewals() == 0 || leaseRenewalCount < leasePolicy.GetMaxRenewals()
		if step := leasePolicy.GetExtendStep(); canRenew && step != nil && step.AsDuration() > 0 && leasePolicy.GetMaxExtension() != nil && leaseExtensionUsed < leasePolicy.GetMaxExtension().AsDuration().Milliseconds() {
			runtime, extendErr := s.LeaseRuntime.ExtendLease(leasePolicy, leaseExpiry, leaseExtensionUsed, step.AsDuration().Milliseconds())
			if extendErr != nil {
				return fmt.Errorf("extend lease for heartbeat: %w", extendErr)
			}
			newLeaseExpiry = runtime.LeaseExpiry
			newExtensionUsed = runtime.LeaseExtensionUsed
			newRenewalCount++
		}

		update := `
			UPDATE cq_messages
			SET last_heartbeat_at = ?,
				heartbeat_expiry = ?,
				lease_expiry = ?,
				lease_extension_used = ?,
				lease_renewal_count = ?,
				updated_at = ?
			WHERE queue_name = ? AND message_id = ? AND state = ?
			  AND current_attempt_id = ? AND current_worker_id = ?
			  AND lease_expiry > ?
			  AND (heartbeat_expiry IS NULL OR heartbeat_expiry <= 0 OR heartbeat_expiry > ?)
			  AND deleted_at IS NULL
		`
		result, err := tx.ExecContext(ctx, update, nowMs, heartbeatExpiry, newLeaseExpiry, newExtensionUsed, newRenewalCount, nowMs, queueName, messageId, messagepb.Message_Metadata_RUNNING, attemptId, workerId, nowMs, nowMs)
		if err != nil {
			return fmt.Errorf("update heartbeat: %w", err)
		}

		if err := s.requireOneOwnedMessage(ctx, tx, result, queueName, messageId, attemptId, workerId, nowMs); err != nil {
			return err
		}

		// Calculate remaining lease time based on the extended lease
		remainingTimeMs = max(newLeaseExpiry-nowMs, 0)
		currentState = state

		return nil
	})

	return currentState, remainingTimeMs, err
}

// PeekMessages retrieves messages without claiming them
func (s *Storage) PeekMessages(ctx context.Context, queueName string, limit int32) ([]*messagepb.Message, error) {
	return s.PeekMessagesWithPriorityRange(ctx, queueName, limit, nil)
}

// PeekMessagesWithPriorityRange retrieves messages within an optional inclusive priority range.
func (s *Storage) PeekMessagesWithPriorityRange(ctx context.Context, queueName string, limit int32, priorityRange *repositorysql.PriorityRange) ([]*messagepb.Message, error) {
	return s.PeekMessagesPage(ctx, queueName, limit, 0, priorityRange)
}

func (s *Storage) PeekMessagesPage(ctx context.Context, queueName string, limit int32, offset int64, priorityRange *repositorysql.PriorityRange) ([]*messagepb.Message, error) {
	nowMs := s.Clock.NowMs()
	query := `
		SELECT metadata_pb,
		       state,
		       attempts_left,
		       max_attempts,
		       current_attempt_id,
		       current_worker_id,
		       lease_started_at,
		       lease_expiry,
		       lease_extension_used,
		       lease_renewal_count,
		       last_heartbeat_at,
		       heartbeat_expiry
		FROM cq_messages
		WHERE queue_name = ?
		  AND state = ?
		  AND (deleted_at IS NULL OR deleted_at > ?)
	`
	args := []any{queueName, messagepb.Message_Metadata_PENDING, nowMs}
	if priorityRange != nil {
		query += ` AND priority BETWEEN ? AND ?`
		args = append(args, priorityRange.Min, priorityRange.Max)
	}
	query += `
		ORDER BY priority DESC, id ASC
		LIMIT ? OFFSET ?
	`
	args = append(args, limit, offset)

	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query messages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var messages []*messagepb.Message
	for rows.Next() {
		var messageBytes []byte
		var stateInt int32
		var attemptsLeft int32
		var maxAttempts int32
		var currentAttemptID sql.NullString
		var currentWorkerID sql.NullString
		var leaseStartedAt sql.NullInt64
		var leaseExpiry sql.NullInt64
		var leaseExtensionUsed sql.NullInt64
		var leaseRenewalCount sql.NullInt32
		var lastHeartbeatAt sql.NullInt64
		var heartbeatExpiry sql.NullInt64

		if err := rows.Scan(
			&messageBytes,
			&stateInt,
			&attemptsLeft,
			&maxAttempts,
			&currentAttemptID,
			&currentWorkerID,
			&leaseStartedAt,
			&leaseExpiry,
			&leaseExtensionUsed,
			&leaseRenewalCount,
			&lastHeartbeatAt,
			&heartbeatExpiry,
		); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}

		msg, err := s.Serializer.UnmarshalMessage(messageBytes)
		if err != nil {
			return nil, fmt.Errorf("unmarshal message: %w", err)
		}

		if err := repositorycommon.DecryptMessagePayload(msg, s.KeyManager); err != nil {
			return nil, fmt.Errorf("decrypt message payload: %w", err)
		}

		repositorycommon.ApplyRuntimeMetadata(msg, repositorycommon.RuntimeMetadata{
			State:                   messagepb.Message_Metadata_State(stateInt),
			AttemptsLeft:            attemptsLeft,
			HasAttemptsLeft:         true,
			MaxAttempts:             maxAttempts,
			HasMaxAttempts:          true,
			CurrentAttemptID:        currentAttemptID.String,
			HasCurrentAttemptID:     currentAttemptID.Valid,
			CurrentWorkerID:         currentWorkerID.String,
			HasCurrentWorkerID:      currentWorkerID.Valid,
			LeaseStartedAtMs:        leaseStartedAt.Int64,
			HasLeaseStartedAtMs:     leaseStartedAt.Valid,
			LeaseExpiryMs:           leaseExpiry.Int64,
			HasLeaseExpiryMs:        leaseExpiry.Valid,
			LeaseExtensionUsedMs:    leaseExtensionUsed.Int64,
			HasLeaseExtensionUsedMs: leaseExtensionUsed.Valid,
			LeaseRenewalCount:       leaseRenewalCount.Int32,
			HasLeaseRenewalCount:    leaseRenewalCount.Valid,
			LastHeartbeatAtMs:       lastHeartbeatAt.Int64,
			HasLastHeartbeatAtMs:    lastHeartbeatAt.Valid,
			HeartbeatExpiryMs:       heartbeatExpiry.Int64,
			HasHeartbeatExpiryMs:    heartbeatExpiry.Valid,
		})

		messages = append(messages, msg)
	}

	return messages, rows.Err()
}

// CreateSchedule creates a new schedule
