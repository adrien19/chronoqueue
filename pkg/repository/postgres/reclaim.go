package postgres

import (
	"context"
	"database/sql"
	"fmt"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	repositorycommon "github.com/adrien19/chronoqueue/pkg/repository/common"
	repositorysql "github.com/adrien19/chronoqueue/pkg/repository/sql"
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
	query := s.ph(`
		SELECT metadata_pb, state, attempts_left, max_attempts, current_attempt_id, lease_expiry, heartbeat_expiry
        FROM cq_messages
				WHERE queue_name = ? AND state = ?
					AND (
								lease_expiry <= ?
								OR (heartbeat_expiry IS NOT NULL AND heartbeat_expiry > 0 AND heartbeat_expiry <= ?)
							)
          AND deleted_at IS NULL
        LIMIT ?
    `)

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
		var leaseExpiry sql.NullInt64
		var heartbeatExpiry sql.NullInt64
		if err := rows.Scan(&messageBytes, &state, &attemptsLeft, &maxAttempts, &currentAttemptID, &leaseExpiry, &heartbeatExpiry); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}

		msg, err := s.Serializer.UnmarshalMessage(messageBytes)
		if err != nil {
			return nil, fmt.Errorf("unmarshal message: %w", err)
		}
		repositorycommon.ApplyRuntimeMetadata(msg, repositorycommon.RuntimeMetadata{
			State:                state,
			AttemptsLeft:         attemptsLeft,
			HasAttemptsLeft:      true,
			MaxAttempts:          maxAttempts,
			HasMaxAttempts:       true,
			CurrentAttemptID:     currentAttemptID.String,
			HasCurrentAttemptID:  currentAttemptID.Valid,
			LeaseExpiryMs:        leaseExpiry.Int64,
			HasLeaseExpiryMs:     leaseExpiry.Valid,
			HeartbeatExpiryMs:    heartbeatExpiry.Int64,
			HasHeartbeatExpiryMs: heartbeatExpiry.Valid,
		})

		messages = append(messages, msg)
	}

	return messages, rows.Err()
}

// ReclaimExpiredMessage moves an expired message back to pending or DLQ.
// Implements: ReclaimableBackend.ReclaimExpiredMessage
func (s *Storage) ReclaimExpiredMessage(ctx context.Context, queueName string, message *messagepb.Message) (*repositorysql.ReclaimResult, error) {
	var newState messagepb.Message_Metadata_State
	var newAttemptsLeft int32
	result := &repositorysql.ReclaimResult{}
	attemptID := message.GetMetadata().GetCurrentAttempt().GetAttemptId()
	queueMetadata, err := s.GetQueueMetadata(ctx, queueName)
	if err != nil {
		return nil, fmt.Errorf("get queue metadata: %w", err)
	}
	dlqName := queueMetadata.GetDeadLetterQueueName()
	shouldDelete, deletedAt := s.calculateDeletion(queueMetadata.GetMessageRetentionPolicy())
	err = s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		nowMs := s.nowMs()
		var leaseExpiry, heartbeatExpiry sql.NullInt64
		expiryQuery := s.ph(`
			SELECT lease_expiry, heartbeat_expiry
			FROM cq_messages
			WHERE queue_name = ? AND message_id = ? AND state = ?
			  AND ((? = '' AND current_attempt_id IS NULL) OR (? <> '' AND current_attempt_id = ?))
			  AND deleted_at IS NULL
			FOR UPDATE
		`)
		if err := tx.QueryRowContext(ctx, expiryQuery, queueName, message.GetMessageId(), messagepb.Message_Metadata_RUNNING, attemptID, attemptID, attemptID).Scan(&leaseExpiry, &heartbeatExpiry); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("message is no longer expired")
			}
			return fmt.Errorf("query message expiry: %w", err)
		}
		result.LeaseExpired = leaseExpiry.Valid && leaseExpiry.Int64 <= nowMs
		result.HeartbeatExpired = heartbeatExpiry.Valid && heartbeatExpiry.Int64 > 0 && heartbeatExpiry.Int64 <= nowMs
		if !result.LeaseExpired && !result.HeartbeatExpired {
			return fmt.Errorf("message is no longer expired")
		}
		updateQuery := s.ph(`
            UPDATE cq_messages
            SET state = CASE WHEN max_attempts = -1 OR attempts_left > 1 THEN ?::integer ELSE ?::integer END,
                attempts_left = CASE WHEN max_attempts = -1 THEN -1 ELSE attempts_left - 1 END,
                current_attempt_id = NULL,
                current_worker_id = NULL,
                lease_started_at = NULL,
                lease_expiry = NULL,
                lease_extension_used = 0,
                lease_renewal_count = 0,
                last_heartbeat_at = NULL,
                heartbeat_expiry = NULL,
				completed_at = NULL,
				deleted_at = NULL,
                updated_at = ?
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
        `)
		err := tx.QueryRowContext(
			ctx,
			updateQuery,
			messagepb.Message_Metadata_PENDING,
			messagepb.Message_Metadata_ERRORED,
			nowMs,
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
		if newState == messagepb.Message_Metadata_ERRORED && dlqName == "" && !shouldDelete {
			timestampResult, err := tx.ExecContext(ctx, `UPDATE cq_messages SET completed_at = $1, deleted_at = $2 WHERE queue_name = $3 AND message_id = $4 AND state = $5`, nowMs, deletedAt, queueName, message.GetMessageId(), newState)
			if err != nil {
				return fmt.Errorf("set exhausted message retention timestamps: %w", err)
			}
			rows, err := timestampResult.RowsAffected()
			if err != nil {
				return fmt.Errorf("get timestamped message rows: %w", err)
			}
			if rows != 1 {
				return fmt.Errorf("set exhausted message retention timestamps: expected one row, updated %d", rows)
			}
		}
		if newState == messagepb.Message_Metadata_ERRORED && dlqName == "" && shouldDelete {
			deleteResult, err := tx.ExecContext(ctx, `DELETE FROM cq_messages WHERE queue_name = $1 AND message_id = $2 AND state = $3`, queueName, message.GetMessageId(), newState)
			if err != nil {
				return fmt.Errorf("delete exhausted message: %w", err)
			}
			rows, rowsErr := deleteResult.RowsAffected()
			if rowsErr != nil || rows != 1 {
				return fmt.Errorf("delete exhausted message: expected one row, deleted %d: %w", rows, rowsErr)
			}
			return s.StateManager.RemoveCounter(ctx, tx, queueName, messagepb.Message_Metadata_RUNNING)
		}

		if newState == messagepb.Message_Metadata_ERRORED && dlqName != "" {
			moveResult, err := tx.ExecContext(ctx, s.ph(`UPDATE cq_messages SET queue_name = ? WHERE queue_name = ? AND message_id = ? AND state = ?`), dlqName, queueName, message.GetMessageId(), newState)
			if err != nil {
				return fmt.Errorf("move message to DLQ: %w", err)
			}
			rows, err := moveResult.RowsAffected()
			if err != nil {
				return fmt.Errorf("get moved rows: %w", err)
			}
			if rows != 1 {
				return fmt.Errorf("move message to DLQ: expected one row, moved %d", rows)
			}
			result.DLQTarget = dlqName
			return s.StateManager.MoveCounter(ctx, tx, queueName, messagepb.Message_Metadata_RUNNING, dlqName, newState)
		}
		return s.StateManager.UpdateCounters(ctx, tx, queueName, messagepb.Message_Metadata_RUNNING, newState)
	})
	if err != nil {
		return nil, err
	}
	result.State = newState

	if message.GetMetadata() != nil {
		message.Metadata.State = newState
		message.Metadata.AttemptsLeft = newAttemptsLeft
	}
	return result, nil
}
