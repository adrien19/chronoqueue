package postgres

import (
	"context"
	"database/sql"
	"fmt"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	"github.com/adrien19/chronoqueue/pkg/metrics"
)

func (s *Storage) GetDLQMessages(ctx context.Context, queueName string, limit int32) ([]*messagepb.Message, error) {
	query := s.ph(`
        SELECT metadata_pb
        FROM cq_messages
        WHERE queue_name = ? AND state = ?
        ORDER BY updated_at DESC
        LIMIT ?
    `)

	rows, err := s.DB.QueryContext(ctx, query, queueName, messagepb.Message_Metadata_ERRORED, limit)
	if err != nil {
		return nil, fmt.Errorf("query DLQ messages: %w", err)
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

// RetryDLQMessage moves a message from DLQ back to pending.
func (s *Storage) RetryDLQMessage(ctx context.Context, queueName string, messageId string) error {
	dlqName := queueName + "-dlq" // DLQ naming convention

	err := s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		var messageBytes []byte
		var oldState messagepb.Message_Metadata_State
		query := s.ph(`SELECT metadata_pb, state FROM cq_messages WHERE message_id = ?`)
		err := tx.QueryRowContext(ctx, query, messageId).Scan(&messageBytes, &oldState)
		if err == sql.ErrNoRows {
			return fmt.Errorf("message not found: %s", messageId)
		}
		if err != nil {
			return fmt.Errorf("query message: %w", err)
		}

		if oldState != messagepb.Message_Metadata_ERRORED {
			return fmt.Errorf("message is not in DLQ state")
		}

		msg, err := s.Serializer.UnmarshalMessage(messageBytes)
		if err != nil {
			return fmt.Errorf("unmarshal message: %w", err)
		}

		updateQuery := s.ph(`
            UPDATE cq_messages
            SET state = ?,
                attempts_left = ?,
                updated_at = ?
            WHERE message_id = ?
        `)
		_, err = tx.ExecContext(ctx, updateQuery, messagepb.Message_Metadata_PENDING, msg.GetMetadata().GetMaxAttempts(), s.nowMs(), messageId)
		if err != nil {
			return fmt.Errorf("update message: %w", err)
		}

		return s.StateManager.UpdateCounters(ctx, tx, queueName, oldState, messagepb.Message_Metadata_PENDING)
	})

	if err == nil {
		// Record successful DLQ retry
		metrics.IncrementDLQRetry(dlqName, queueName)
		metrics.RecordStateTransition(queueName, "ERRORED", "PENDING")
	}

	return err
}

// DeleteDLQMessage permanently deletes a message from DLQ.
func (s *Storage) DeleteDLQMessage(ctx context.Context, queueName string, messageId string) error {
	return s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		var oldState messagepb.Message_Metadata_State
		err := tx.QueryRowContext(ctx, s.ph(`SELECT state FROM cq_messages WHERE message_id = ?`), messageId).Scan(&oldState)
		if err == sql.ErrNoRows {
			return fmt.Errorf("message not found: %s", messageId)
		}
		if err != nil {
			return fmt.Errorf("query message state: %w", err)
		}

		if oldState != messagepb.Message_Metadata_ERRORED {
			return fmt.Errorf("message is not in DLQ state")
		}

		query := s.ph(`DELETE FROM cq_messages WHERE message_id = ?`)
		result, err := tx.ExecContext(ctx, query, messageId)
		if err != nil {
			return fmt.Errorf("delete message: %w", err)
		}

		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("get rows affected: %w", err)
		}
		if rows == 0 {
			return fmt.Errorf("message not found: %s", messageId)
		}

		return s.StateManager.UpdateCounters(ctx, tx, queueName, oldState, messagepb.Message_Metadata_COMPLETED)
	})
}

// PurgeDLQ deletes all errored messages for a queue and returns the count deleted.
func (s *Storage) PurgeDLQ(ctx context.Context, queueName string) (int64, error) {
	dlqName := queueName + "_dlq"

	var deletedCount int64
	err := s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		countQuery := s.ph(`SELECT COUNT(*) FROM cq_messages WHERE queue_name = ? AND state = ?`)
		var count int64
		if err := tx.QueryRowContext(ctx, countQuery, dlqName, int(messagepb.Message_Metadata_ERRORED)).Scan(&count); err != nil {
			return fmt.Errorf("count DLQ messages: %w", err)
		}

		if count == 0 {
			deletedCount = 0
			return nil
		}

		deleteQuery := s.ph(`DELETE FROM cq_messages WHERE queue_name = ? AND state = ?`)
		result, err := tx.ExecContext(ctx, deleteQuery, dlqName, int(messagepb.Message_Metadata_ERRORED))
		if err != nil {
			return fmt.Errorf("delete DLQ messages: %w", err)
		}

		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("get rows affected: %w", err)
		}

		deletedCount = rows

		for i := int64(0); i < rows; i++ {
			if err := s.StateManager.UpdateCounters(ctx, tx, dlqName, messagepb.Message_Metadata_ERRORED, messagepb.Message_Metadata_COMPLETED); err != nil {
				return fmt.Errorf("update state counters: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	return deletedCount, nil
}

// FindExpiredMessages finds messages with expired leases or heartbeats.
