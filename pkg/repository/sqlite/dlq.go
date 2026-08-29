package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	"github.com/adrien19/chronoqueue/internal/domainerror"
	"github.com/adrien19/chronoqueue/pkg/metrics"
)

func (s *Storage) GetDLQMessages(ctx context.Context, queueName string, limit int32) ([]*messagepb.Message, error) {
	query := `
		SELECT metadata_pb, state, attempts_left
		FROM cq_messages
		WHERE queue_name = ? AND state = ?
		ORDER BY updated_at DESC
		LIMIT ?
	`

	rows, err := s.DB.QueryContext(ctx, query, queueName, messagepb.Message_Metadata_ERRORED, limit)
	if err != nil {
		return nil, fmt.Errorf("query DLQ messages: %w", err)
	}
	var messages []*messagepb.Message
	var scanErr error
	for rows.Next() {
		var messageBytes []byte
		var state messagepb.Message_Metadata_State
		var attemptsLeft int32
		if err := rows.Scan(&messageBytes, &state, &attemptsLeft); err != nil {
			scanErr = fmt.Errorf("scan message: %w", err)
			break
		}
		msg, err := s.Serializer.UnmarshalMessage(messageBytes)
		if err != nil {
			scanErr = fmt.Errorf("unmarshal message: %w", err)
			break
		}
		if msg.GetMetadata() == nil {
			scanErr = fmt.Errorf("DLQ message %q has no metadata", msg.GetMessageId())
			break
		}
		msg.Metadata.State = state
		msg.Metadata.AttemptsLeft = attemptsLeft

		messages = append(messages, msg)
	}

	if scanErr == nil {
		scanErr = rows.Err()
	}
	closeErr := rows.Close()
	if scanErr != nil {
		return nil, scanErr
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close DLQ message rows: %w", closeErr)
	}
	return messages, nil
}

// RetryDLQMessage moves a message from DLQ back to pending
func (s *Storage) RetryDLQMessage(ctx context.Context, dlqName string, messageId string, targetQueueName string, resetRetries bool) error {
	err := s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		// Get message
		var messageBytes []byte
		var oldState messagepb.Message_Metadata_State
		var attemptsLeft int32
		query := `SELECT metadata_pb, state, attempts_left FROM cq_messages WHERE message_id = ? AND queue_name = ?`
		err := tx.QueryRowContext(ctx, query, messageId, dlqName).Scan(&messageBytes, &oldState, &attemptsLeft)
		if err == sql.ErrNoRows {
			return domainerror.New(domainerror.NotFound, fmt.Sprintf("message not found: %s", messageId), err)
		}
		if err != nil {
			return fmt.Errorf("query message: %w", err)
		}

		if oldState != messagepb.Message_Metadata_ERRORED {
			return domainerror.New(domainerror.FailedPrecondition, "message is not in DLQ state", nil)
		}

		msg, err := s.Serializer.UnmarshalMessage(messageBytes)
		if err != nil {
			return fmt.Errorf("unmarshal message: %w", err)
		}

		// Reset message to pending
		if resetRetries {
			attemptsLeft = msg.GetMetadata().GetMaxAttempts()
		}
		updateQuery := `
			UPDATE cq_messages
			SET queue_name = ?, state = ?,
				attempts_left = ?,
				updated_at = CURRENT_TIMESTAMP
			WHERE message_id = ? AND queue_name = ?
		`
		_, err = tx.ExecContext(ctx, updateQuery, targetQueueName, messagepb.Message_Metadata_PENDING, attemptsLeft, messageId, dlqName)
		if err != nil {
			if isUniqueConstraintError(err) {
				return domainerror.New(domainerror.AlreadyExists, fmt.Sprintf("message %q already exists in queue %q", messageId, targetQueueName), err)
			}
			return fmt.Errorf("update message: %w", err)
		}

		// Update state counts
		return s.StateManager.MoveCounter(ctx, tx, dlqName, oldState, targetQueueName, messagepb.Message_Metadata_PENDING)
	})

	if err == nil {
		// Record successful DLQ retry
		metrics.IncrementDLQRetry(dlqName, targetQueueName)
		metrics.RecordStateTransition(targetQueueName, "ERRORED", "PENDING")
	}

	return err
}

// DeleteDLQMessage permanently deletes a message from DLQ
func (s *Storage) DeleteDLQMessage(ctx context.Context, queueName string, messageId string) error {
	return s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		// Get current state
		var oldState messagepb.Message_Metadata_State
		err := tx.QueryRowContext(ctx, `SELECT state FROM cq_messages WHERE message_id = ? AND queue_name = ?`, messageId, queueName).Scan(&oldState)
		if err == sql.ErrNoRows {
			return domainerror.New(domainerror.NotFound, fmt.Sprintf("message not found: %s", messageId), err)
		}
		if err != nil {
			return fmt.Errorf("query message state: %w", err)
		}

		if oldState != messagepb.Message_Metadata_ERRORED {
			return domainerror.New(domainerror.FailedPrecondition, "message is not in DLQ state", nil)
		}

		// Delete message
		query := `DELETE FROM cq_messages WHERE message_id = ? AND queue_name = ?`
		result, err := tx.ExecContext(ctx, query, messageId, queueName)
		if err != nil {
			return fmt.Errorf("delete message: %w", err)
		}

		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("get rows affected: %w", err)
		}
		if rows == 0 {
			return domainerror.New(domainerror.NotFound, fmt.Sprintf("message not found: %s", messageId), err)
		}

		// Update state counts
		return s.StateManager.RemoveCounter(ctx, tx, queueName, oldState)
	})
}

// PurgeDLQ bulk-deletes all messages in ERRORED state for a queue
func (s *Storage) PurgeDLQ(ctx context.Context, queueName string) (int64, error) {
	var deletedCount int64

	err := s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		// Count messages before deletion
		var count int64
		countQuery := `SELECT COUNT(*) FROM cq_messages WHERE queue_name = ? AND state = ?`
		err := tx.QueryRowContext(ctx, countQuery, queueName, messagepb.Message_Metadata_ERRORED).Scan(&count)
		if err != nil {
			return fmt.Errorf("count DLQ messages: %w", err)
		}

		if count == 0 {
			deletedCount = 0
			return nil
		}

		// Delete all ERRORED messages
		deleteQuery := `DELETE FROM cq_messages WHERE queue_name = ? AND state = ?`
		result, err := tx.ExecContext(ctx, deleteQuery, queueName, messagepb.Message_Metadata_ERRORED)
		if err != nil {
			return fmt.Errorf("delete DLQ messages: %w", err)
		}

		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("get rows affected: %w", err)
		}

		deletedCount = rows

		// Update state counts (decrement ERRORED state by count)
		for i := int64(0); i < count; i++ {
			if err := s.StateManager.RemoveCounter(ctx, tx, queueName, messagepb.Message_Metadata_ERRORED); err != nil {
				return fmt.Errorf("update state counts: %w", err)
			}
		}

		return nil
	})

	return deletedCount, err
}
