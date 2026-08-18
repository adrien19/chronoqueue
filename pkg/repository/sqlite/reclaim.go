package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	repositorycommon "github.com/adrien19/chronoqueue/pkg/repository/common"
)

// This file implements the ReclaimableBackend interface from pkg/repository/sql/background.
// These methods are INTERNAL operations used only by the background ReclaimService.
// They are NOT part of the public BackendStorage interface.
//
// See pkg/repository/sql/background/reclaim.go for interface documentation.

// FindExpiredMessages locates messages with expired leases or heartbeats.
// Implements: ReclaimableBackend.FindExpiredMessages
func (s *Storage) FindExpiredMessages(ctx context.Context, queueName string, limit int32) ([]*messagepb.Message, error) {
	nowMs := s.Clock.NowMs()
	query := `
		SELECT metadata_pb, state, attempts_left, max_attempts, current_attempt_id
		FROM cq_messages
		WHERE queue_name = ? AND state = ?
		  AND (
		        lease_expiry <= ?
		        OR (heartbeat_expiry IS NOT NULL AND heartbeat_expiry > 0 AND heartbeat_expiry <= ?)
		      )
		  AND deleted_at IS NULL
		LIMIT ?
	`

	rows, err := s.DB.QueryContext(ctx, query, queueName, messagepb.Message_Metadata_RUNNING, nowMs, nowMs, limit)
	if err != nil {
		return nil, fmt.Errorf("query expired messages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var messages []*messagepb.Message
	for rows.Next() {
		var messageBytes []byte
		var state messagepb.Message_Metadata_State
		var attemptsLeft int32
		var maxAttempts int32
		var currentAttemptID sql.NullString
		if err := rows.Scan(&messageBytes, &state, &attemptsLeft, &maxAttempts, &currentAttemptID); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}

		msg, err := s.Serializer.UnmarshalMessage(messageBytes)
		if err != nil {
			return nil, fmt.Errorf("unmarshal message: %w", err)
		}
		repositorycommon.ApplyRuntimeMetadata(msg, repositorycommon.RuntimeMetadata{
			State:               state,
			AttemptsLeft:        attemptsLeft,
			HasAttemptsLeft:     true,
			MaxAttempts:         maxAttempts,
			HasMaxAttempts:      true,
			CurrentAttemptID:    currentAttemptID.String,
			HasCurrentAttemptID: currentAttemptID.Valid,
		})

		messages = append(messages, msg)
	}

	return messages, rows.Err()
}

// ReclaimExpiredMessage moves an expired message back to pending or DLQ.
// Implements: ReclaimableBackend.ReclaimExpiredMessage
func (s *Storage) ReclaimExpiredMessage(ctx context.Context, queueName string, message *messagepb.Message) error {
	var newState messagepb.Message_Metadata_State
	var newAttemptsLeft int32
	attemptID := message.GetMetadata().GetCurrentAttempt().GetAttemptId()
	err := s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		nowMs := s.Clock.NowMs()

		updateQuery := `
			UPDATE cq_messages
			SET state = CASE WHEN max_attempts = -1 OR attempts_left > 1 THEN ? ELSE ? END,
				attempts_left = CASE WHEN max_attempts = -1 THEN -1 ELSE attempts_left - 1 END,
				current_attempt_id = NULL,
				current_worker_id = NULL,
				lease_started_at = NULL,
				lease_expiry = NULL,
				lease_extension_used = 0,
				lease_renewal_count = 0,
				last_heartbeat_at = NULL,
				heartbeat_expiry = NULL,
				updated_at = CURRENT_TIMESTAMP
			WHERE queue_name = ?
			  AND message_id = ?
			  AND state = ?
			  AND ((? = '' AND current_attempt_id IS NULL) OR (? <> '' AND current_attempt_id = ?))
			  AND (
				lease_expiry <= ?
				OR (heartbeat_expiry IS NOT NULL AND heartbeat_expiry > 0 AND heartbeat_expiry <= ?)
			  )
			  AND deleted_at IS NULL
			RETURNING state, attempts_left
		`
		err := tx.QueryRowContext(
			ctx,
			updateQuery,
			messagepb.Message_Metadata_PENDING,
			messagepb.Message_Metadata_ERRORED,
			queueName,
			message.GetMessageId(),
			messagepb.Message_Metadata_RUNNING,
			attemptID,
			attemptID,
			attemptID,
			nowMs,
			nowMs,
		).Scan(&newState, &newAttemptsLeft)
		if err == sql.ErrNoRows {
			return fmt.Errorf("message is no longer expired")
		}
		if err != nil {
			return fmt.Errorf("update message: %w", err)
		}

		return s.StateManager.UpdateCounters(ctx, tx, queueName, messagepb.Message_Metadata_RUNNING, newState)
	})
	if err != nil {
		return err
	}

	if message.GetMetadata() != nil {
		message.Metadata.State = newState
		message.Metadata.AttemptsLeft = newAttemptsLeft
	}
	return nil
}
