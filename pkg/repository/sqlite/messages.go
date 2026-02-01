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
	_, _ = rand.Read(b)
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

	if err := repositorycommon.EncryptMessagePayload(message, s.KeyManager); err != nil {
		return fmt.Errorf("encrypt message payload: %w", err)
	}

	messageBytes, err := s.Serializer.MarshalMessage(message)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	metadata := message.GetMetadata()
	if metadata == nil {
		return fmt.Errorf("message metadata is nil")
	}

	err = s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
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

		_, err := tx.ExecContext(ctx, query,
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
			return fmt.Errorf("insert message: %w", err)
		}

		// Update queue state counts (increment from zero to new state)
		return s.StateManager.UpdateCounters(ctx, tx, queueName, 0, metadata.State)
	})

	if err == nil {
		// Record successful enqueue
		metrics.IncrementMessagesEnqueued(queueName)
		metrics.RecordStateTransition(queueName, "none", metadata.State.String())
	}

	return err
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
