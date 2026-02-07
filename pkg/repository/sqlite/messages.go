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
	"github.com/adrien19/chronoqueue/pkg/metrics"
	repositorycommon "github.com/adrien19/chronoqueue/pkg/repository/common"
	"github.com/adrien19/chronoqueue/pkg/repository/sql/priority"
)

// generateID generates a random ID
func generateID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand.Read failed: %v", err))
	}
	return hex.EncodeToString(b)
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
func (s *Storage) EnqueueMessagesBulk(ctx context.Context, queueName string, messages []*messagepb.Message, transactionMode int32) ([]error, error) {
	start := time.Now()
	defer func() {
		metrics.ObserveDBTransaction("sqlite", "enqueue_messages_bulk", time.Since(start))
	}()

	const (
		modeAllOrNothing = 0
		modeBestEffort   = 1
	)

	messageErrors := make([]error, len(messages))

	// ALL_OR_NOTHING: Single transaction, all succeed or all fail
	if transactionMode == modeAllOrNothing {
		var failedIdx int
		txErr := s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
			for i, message := range messages {
				if _, err := s.enqueueMessageInTx(ctx, tx, queueName, message); err != nil {
					messageErrors[i] = err
					failedIdx = i
					return fmt.Errorf("message[%d] failed: %w", i, err)
				}
			}
			return nil
		})

		if txErr != nil {
			// Transaction rolled back - mark all attempted messages as failed
			for j := 0; j < failedIdx; j++ {
				messageErrors[j] = fmt.Errorf("rolled back due to message[%d] failure", failedIdx)
			}
			return messageErrors, txErr
		}

		// Transaction committed, all messages succeeded - record metrics
		for _, message := range messages {
			metrics.IncrementMessagesEnqueued(queueName)
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
			metrics.IncrementMessagesEnqueued(queueName)
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
		ON CONFLICT(message_id) DO NOTHING
	`

	// Extract scheduled time in milliseconds if present
	var scheduledTimeMs *int64
	if metadata.ScheduledTime != nil {
		ms := metadata.ScheduledTime.AsTime().UnixMilli()
		scheduledTimeMs = &ms
	}

	result, err := tx.ExecContext(ctx, query,
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
func (s *Storage) ClaimMessage(ctx context.Context, queueName string, workerId string, attemptId string) (*messagepb.Message, error) {
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

	// Generate IDs if not provided
	if attemptId == "" {
		attemptId = generateID()
	}
	if workerId == "" {
		workerId = "worker-" + generateID()[:8]
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
		// SQLite doesn't support SKIP LOCKED, so we use simple SELECT with LIMIT
		strictQuery := `
			SELECT id, message_id, metadata_pb, state
			FROM cq_messages
			WHERE queue_name = ? AND state = ? AND (scheduled_at IS NULL OR scheduled_at <= ?)
			  AND deleted_at IS NULL
			ORDER BY priority DESC, id ASC
			LIMIT 1
		`

		weightedQuery := `
			SELECT id, message_id, metadata_pb, state
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

		query := strictQuery
		args := []interface{}{queueName, messagepb.Message_Metadata_PENDING, nowMs}

		if useWeighted {
			minPriority, maxPriority := priority.PriorityLevelToRange(selectedLevel)
			query = weightedQuery
			args = append(args, minPriority, maxPriority)
		}

		err := tx.QueryRowContext(ctx, query, args...).Scan(&id, &messageId, &messageBytes, &oldState)
		if err == sql.ErrNoRows && useWeighted {
			query = strictQuery
			args = args[:3]
			err = tx.QueryRowContext(ctx, query, args...).Scan(&id, &messageId, &messageBytes, &oldState)
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
		leaseRuntime := s.LeaseRuntime.CalculateLeaseRuntime(leasePolicy)

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
		_, err = tx.ExecContext(ctx, updateQuery,
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
func (s *Storage) AcknowledgeMessage(ctx context.Context, queueName string, messageId string, attemptId string) error {
	// Fetch queue metadata outside transaction to avoid connection pool contention
	queueMeta, err := s.GetQueueMetadata(ctx, queueName)
	if err != nil {
		return fmt.Errorf("get queue metadata: %w", err)
	}
	retentionPolicy := queueMeta.GetMessageRetentionPolicy()

	err = s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		var oldState messagepb.Message_Metadata_State
		err := tx.QueryRowContext(ctx, `SELECT state FROM cq_messages WHERE message_id = ?`, messageId).Scan(&oldState)
		if err == sql.ErrNoRows {
			return fmt.Errorf("message not found: %s", messageId)
		}
		if err != nil {
			return fmt.Errorf("query message state: %w", err)
		}
		shouldDelete, deletedAt := s.calculateDeletion(retentionPolicy)

		nowMs := s.nowMs()

		if shouldDelete {
			// Hard delete - must match attempt_id for idempotency
			result, err := tx.ExecContext(ctx, `DELETE FROM cq_messages WHERE message_id = ? AND current_attempt_id = ?`, messageId, attemptId)
			if err != nil {
				return fmt.Errorf("delete message: %w", err)
			}

			rows, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("get rows affected: %w", err)
			}
			if rows == 0 {
				return fmt.Errorf("message not found or attempt mismatch")
			}
		} else {
			// Soft delete - must match attempt_id for idempotency
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
				WHERE message_id = ? AND current_attempt_id = ?`,
				messagepb.Message_Metadata_COMPLETED,
				nowMs,
				deletedAt,
				nowMs,
				messageId,
				attemptId)
			if err != nil {
				return fmt.Errorf("soft delete message: %w", err)
			}

			rows, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("get rows affected: %w", err)
			}
			if rows == 0 {
				return fmt.Errorf("message not found or attempt mismatch")
			}
		}

		return s.StateManager.UpdateCounters(ctx, tx, queueName, oldState, messagepb.Message_Metadata_COMPLETED)
	})

	if err == nil {
		metrics.RecordStateTransition(queueName, "RUNNING", "COMPLETED")
	}

	return err
}

// NackMessage marks a message as failed
func (s *Storage) NackMessage(ctx context.Context, queueName string, messageId string, attemptId string) error {
	var movedToDLQ bool
	var dlqName string

	// Fetch queue metadata outside transaction to avoid connection pool contention
	queueMeta, err := s.GetQueueMetadata(ctx, queueName)
	if err != nil {
		return fmt.Errorf("get queue metadata: %w", err)
	}
	retentionPolicy := queueMeta.GetMessageRetentionPolicy()

	err = s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		var messageBytes []byte
		var oldState messagepb.Message_Metadata_State
		var attemptsLeft int32
		query := `SELECT metadata_pb, state, attempts_left FROM cq_messages WHERE message_id = ?`
		err := tx.QueryRowContext(ctx, query, messageId).Scan(&messageBytes, &oldState, &attemptsLeft)
		if err == sql.ErrNoRows {
			return fmt.Errorf("message not found: %s", messageId)
		}
		if err != nil {
			return fmt.Errorf("query message: %w", err)
		}

		newState := messagepb.Message_Metadata_PENDING
		newAttemptsLeft := attemptsLeft - 1
		if newAttemptsLeft <= 0 {
			newState = messagepb.Message_Metadata_ERRORED
			movedToDLQ = true
			dlqName = queueName + "-dlq"

			shouldDelete, deletedAt := s.calculateDeletion(retentionPolicy)

			nowMs := s.nowMs()

			if shouldDelete {
				deleteQuery := `DELETE FROM cq_messages WHERE message_id = ?`
				_, err = tx.ExecContext(ctx, deleteQuery, messageId)
				if err != nil {
					return fmt.Errorf("delete errored message: %w", err)
				}
			} else {
				updateQuery := `
					UPDATE cq_messages
					SET state = ?,
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
					WHERE message_id = ?
				`
				_, err = tx.ExecContext(ctx, updateQuery,
					newState,
					newAttemptsLeft,
					nowMs,
					deletedAt,
					nowMs,
					messageId,
				)
				if err != nil {
					return fmt.Errorf("soft delete errored message: %w", err)
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
				WHERE message_id = ? AND current_attempt_id = ?
			`
			result, err := tx.ExecContext(ctx, updateQuery, newState, newAttemptsLeft, s.nowMs(), messageId, attemptId)
			if err != nil {
				return fmt.Errorf("update message: %w", err)
			}

			rows, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("get rows affected: %w", err)
			}
			if rows == 0 {
				return fmt.Errorf("message not found or attempt mismatch")
			}
		}

		return s.StateManager.UpdateCounters(ctx, tx, queueName, oldState, newState)
	})

	if err == nil {
		if movedToDLQ {
			metrics.RecordStateTransition(queueName, "RUNNING", "ERRORED")
			metrics.IncrementDLQIngestion(dlqName, queueName, "max_attempts")
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
			return fmt.Errorf("message not found: %s", messageId)
		}
		if err != nil {
			return fmt.Errorf("query message: %w", err)
		}

		// Only allow cancellation of INVISIBLE or PENDING messages
		if currentState != messagepb.Message_Metadata_INVISIBLE && currentState != messagepb.Message_Metadata_PENDING {
			return fmt.Errorf("cannot cancel message in state %s (only INVISIBLE or PENDING messages can be cancelled)", currentState)
		}

		// Calculate deletion behavior based on retention policy
		var deletedAt *int64
		shouldDelete, deletedAt = s.calculateDeletion(retentionPolicy)
		nowMs := s.nowMs()

		if shouldDelete {
			// Hard delete immediately - verify state hasn't changed
			deleteQuery := `DELETE FROM cq_messages WHERE message_id = ? AND state IN (?, ?)`
			result, err := tx.ExecContext(ctx, deleteQuery, messageId, messagepb.Message_Metadata_INVISIBLE, messagepb.Message_Metadata_PENDING)
			if err != nil {
				return fmt.Errorf("delete cancelled message: %w", err)
			}

			rows, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("get rows affected: %w", err)
			}
			if rows == 0 {
				return fmt.Errorf("message not found or state changed")
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
				WHERE message_id = ? AND state IN (?, ?)
			`
			result, err := tx.ExecContext(ctx, updateQuery,
				messagepb.Message_Metadata_CANCELED,
				nowMs,
				deletedAt,
				reasonPtr,
				nowMs,
				messageId,
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
				return fmt.Errorf("message not found or state changed")
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
func (s *Storage) ExtendMessageLease(ctx context.Context, queueName string, messageId string, attemptId string, extensionMs int64) error {
	return s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		// Resolve current attempt if not provided to avoid immediate mismatches.
		if attemptId == "" {
			var currentAttempt sql.NullString
			err := tx.QueryRowContext(ctx, `SELECT current_attempt_id FROM cq_messages WHERE message_id = ?`, messageId).Scan(&currentAttempt)
			if err == sql.ErrNoRows {
				return fmt.Errorf("message not found or attempt mismatch")
			}
			if err != nil {
				return fmt.Errorf("query current attempt: %w", err)
			}
			if !currentAttempt.Valid || currentAttempt.String == "" {
				return fmt.Errorf("message not found or attempt mismatch")
			}
			attemptId = currentAttempt.String
		}

		// Get message metadata and lease usage for this attempt
		var messageBytes []byte
		var leaseExtensionUsed int64
		var currentRenewalCount int32
		query := `SELECT metadata_pb, lease_extension_used, lease_renewal_count FROM cq_messages WHERE message_id = ? AND current_attempt_id = ?`
		err := tx.QueryRowContext(ctx, query, messageId, attemptId).Scan(&messageBytes, &leaseExtensionUsed, &currentRenewalCount)
		if err == sql.ErrNoRows {
			return fmt.Errorf("message not found or attempt mismatch")
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
		if leasePolicy != nil && leasePolicy.MaxRenewals > 0 {
			if currentRenewalCount >= leasePolicy.MaxRenewals {
				metrics.IncrementLeaseRenewals(queueName, "denied_max_renewals")
				return fmt.Errorf("lease renewal limit reached: %d/%d renewals used", currentRenewalCount, leasePolicy.MaxRenewals)
			}
		}

		// Calculate new lease
		newLeaseRuntime, err := s.LeaseRuntime.ExtendLease(leasePolicy, leaseExtensionUsed, extensionMs)
		if err != nil {
			return err
		}

		// Update message
		updateQuery := `
			UPDATE cq_messages
			SET lease_expiry = ?,
				lease_extension_used = ?,
				lease_renewal_count = lease_renewal_count + 1,
				updated_at = CURRENT_TIMESTAMP
			WHERE message_id = ? AND current_attempt_id = ?
		`
		_, err = tx.ExecContext(ctx, updateQuery,
			newLeaseRuntime.LeaseExpiry,
			newLeaseRuntime.LeaseExtensionUsed,
			messageId,
			attemptId,
		)
		if err == nil {
			metrics.IncrementLeaseRenewals(queueName, "success")
		} else {
			metrics.IncrementLeaseRenewals(queueName, "failed")
		}
		return err
	})
}

// HeartbeatMessage updates the heartbeat for a message
// Returns the current message state and remaining lease time in milliseconds.
func (s *Storage) HeartbeatMessage(ctx context.Context, queueName string, messageId string, attemptId string) (messagepb.Message_Metadata_State, int64, error) {
	var currentState messagepb.Message_Metadata_State
	var remainingTimeMs int64

	err := s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		var messageBytes []byte
		var currentAttempt sql.NullString
		var leaseExpiry int64
		var state messagepb.Message_Metadata_State
		query := `SELECT metadata_pb, current_attempt_id, lease_expiry, state FROM cq_messages WHERE message_id = ?`
		err := tx.QueryRowContext(ctx, query, messageId).Scan(&messageBytes, &currentAttempt, &leaseExpiry, &state)
		if err == sql.ErrNoRows {
			return fmt.Errorf("message not found or attempt mismatch")
		}
		if err != nil {
			return fmt.Errorf("query message: %w", err)
		}

		if !currentAttempt.Valid || currentAttempt.String == "" {
			return fmt.Errorf("message not found or attempt mismatch")
		}

		if attemptId == "" {
			attemptId = currentAttempt.String
		} else if attemptId != currentAttempt.String {
			return fmt.Errorf("message not found or attempt mismatch")
		}

		msg, err := s.Serializer.UnmarshalMessage(messageBytes)
		if err != nil {
			return fmt.Errorf("unmarshal message: %w", err)
		}

		heartbeatTimeoutMs := int64(30000)
		if lp := msg.GetMetadata().GetLeasePolicy(); lp != nil && lp.GetHeartbeatTimeout() != nil {
			heartbeatTimeoutMs = lp.GetHeartbeatTimeout().AsDuration().Milliseconds()
			if heartbeatTimeoutMs <= 0 {
				heartbeatTimeoutMs = 30000
			}
		}

		nowMs := s.Clock.NowMs()
		heartbeatExpiry := nowMs + heartbeatTimeoutMs
		// Extend the lease when heartbeat is received
		newLeaseExpiry := nowMs + heartbeatTimeoutMs

		update := `
			UPDATE cq_messages
			SET last_heartbeat_at = ?,
				heartbeat_expiry = ?,
				lease_expiry = ?,
				updated_at = ?
			WHERE message_id = ? AND current_attempt_id = ?
		`
		result, err := tx.ExecContext(ctx, update, nowMs, heartbeatExpiry, newLeaseExpiry, nowMs, messageId, attemptId)
		if err != nil {
			return fmt.Errorf("update heartbeat: %w", err)
		}

		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("get rows affected: %w", err)
		}
		if rows == 0 {
			return fmt.Errorf("message not found or attempt mismatch")
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
	query := `
		SELECT metadata_pb
		FROM cq_messages
		WHERE queue_name = ? AND deleted_at IS NULL
		ORDER BY priority DESC, id ASC
		LIMIT ?
	`

	rows, err := s.DB.QueryContext(ctx, query, queueName, limit)
	if err != nil {
		return nil, fmt.Errorf("query messages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var messages []*messagepb.Message
	for rows.Next() {
		var messageBytes []byte
		if err := rows.Scan(&messageBytes); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}

		msg, err := s.Serializer.UnmarshalMessage(messageBytes)
		if err != nil {
			return nil, fmt.Errorf("unmarshal message: %w", err)
		}

		if err := repositorycommon.DecryptMessagePayload(msg, s.KeyManager); err != nil {
			return nil, fmt.Errorf("decrypt message payload: %w", err)
		}

		messages = append(messages, msg)
	}

	return messages, rows.Err()
}

// CreateSchedule creates a new schedule
