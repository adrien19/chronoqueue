package postgres

import (
	"context"
	"database/sql"
	"fmt"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
)

func (s *Storage) FindExpiredMessages(ctx context.Context, queueName string, limit int32) ([]*messagepb.Message, error) {
	nowMs := s.Clock.NowMs()
	query := s.ph(`
        SELECT metadata_pb
        FROM cq_messages
        WHERE queue_name = ? AND state = ? AND (lease_expiry <= ? OR heartbeat_expiry <= ?)
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
		if err := rows.Scan(&messageBytes); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}

		msg, err := s.Serializer.UnmarshalMessage(messageBytes)
		if err != nil {
			return nil, fmt.Errorf("unmarshal message: %w", err)
		}

		messages = append(messages, msg)
	}

	return messages, rows.Err()
}

// ReclaimExpiredMessage moves an expired message back to pending or DLQ.
func (s *Storage) ReclaimExpiredMessage(ctx context.Context, queueName string, message *messagepb.Message) error {
	return s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		newState := messagepb.Message_Metadata_PENDING
		newAttemptsLeft := message.GetMetadata().GetAttemptsLeft() - 1
		if newAttemptsLeft <= 0 {
			newState = messagepb.Message_Metadata_ERRORED
		}

		updateQuery := s.ph(`
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
            WHERE message_id = ?
        `)
		_, err := tx.ExecContext(ctx, updateQuery, newState, newAttemptsLeft, s.nowMs(), message.GetMessageId())
		if err != nil {
			return fmt.Errorf("update message: %w", err)
		}

		return s.StateManager.UpdateCounters(ctx, tx, queueName, message.GetMetadata().GetState(), newState)
	})
}
