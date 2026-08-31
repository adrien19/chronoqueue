package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/lib/pq"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	"github.com/adrien19/chronoqueue/internal/domainerror"
	"github.com/adrien19/chronoqueue/pkg/metrics"
	repositorycommon "github.com/adrien19/chronoqueue/pkg/repository/common"
)

func (s *Storage) GetDLQMessages(ctx context.Context, queueName string, limit int32) ([]*messagepb.Message, error) {
	messages, _, err := s.GetDLQMessagesPage(ctx, queueName, limit, "")
	return messages, err
}

// GetDLQMessagesPage uses immutable insertion IDs so updates cannot reorder an in-progress traversal.
func (s *Storage) GetDLQMessagesPage(ctx context.Context, queueName string, limit int32, cursor string) ([]*messagepb.Message, string, error) {
	if limit <= 0 {
		return nil, "", nil
	}
	cursorID := int64(math.MaxInt64)
	if cursor != "" {
		var err error
		cursorID, err = strconv.ParseInt(cursor, 10, 64)
		if err != nil || cursorID < 1 {
			return nil, "", domainerror.New(domainerror.InvalidArgument, "invalid DLQ cursor", err)
		}
	}
	query := s.ph(`
		SELECT id, metadata_pb, state, attempts_left
        FROM cq_messages
		WHERE queue_name = ? AND state = ? AND id < ?
		ORDER BY id DESC
		LIMIT ?
    `)

	rows, err := s.DB.QueryContext(ctx, query, queueName, messagepb.Message_Metadata_ERRORED, cursorID, int64(limit)+1)
	if err != nil {
		return nil, "", fmt.Errorf("query DLQ messages: %w", err)
	}
	var messages []*messagepb.Message
	var rowIDs []int64
	var scanErr error
	for rows.Next() {
		var rowID int64
		var messageBytes []byte
		var state messagepb.Message_Metadata_State
		var attemptsLeft int32
		if err := rows.Scan(&rowID, &messageBytes, &state, &attemptsLeft); err != nil {
			scanErr = fmt.Errorf("scan message: %w", err)
			break
		}
		msg, err := s.Serializer.UnmarshalMessage(messageBytes)
		if err != nil {
			scanErr = fmt.Errorf("unmarshal message: %w", err)
			break
		}
		if err := repositorycommon.DecryptMessagePayload(msg, s.KeyManager); err != nil {
			scanErr = fmt.Errorf("decrypt message payload: %w", err)
			break
		}
		if msg.GetMetadata() == nil {
			scanErr = fmt.Errorf("DLQ message %q has no metadata", msg.GetMessageId())
			break
		}
		msg.Metadata.State = state
		msg.Metadata.AttemptsLeft = attemptsLeft

		messages = append(messages, msg)
		rowIDs = append(rowIDs, rowID)
	}

	if scanErr == nil {
		scanErr = rows.Err()
	}
	closeErr := rows.Close()
	if scanErr != nil {
		return nil, "", scanErr
	}
	if closeErr != nil {
		return nil, "", fmt.Errorf("close DLQ message rows: %w", closeErr)
	}
	if len(messages) <= int(limit) {
		return messages, "", nil
	}
	messages = messages[:limit]
	return messages, strconv.FormatInt(rowIDs[limit-1], 10), nil
}

// RetryDLQMessage moves a message from DLQ back to pending.
func (s *Storage) RetryDLQMessage(ctx context.Context, dlqName string, messageId string, targetQueueName string, resetRetries bool) error {
	err := s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		var messageBytes []byte
		var oldState messagepb.Message_Metadata_State
		var attemptsLeft int32
		query := s.ph(`SELECT metadata_pb, state, attempts_left FROM cq_messages WHERE message_id = ? AND queue_name = ?`)
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

		if resetRetries {
			attemptsLeft = msg.GetMetadata().GetMaxAttempts()
		}
		updateQuery := s.ph(`
            UPDATE cq_messages
            SET queue_name = ?, state = ?,
                attempts_left = ?,
                updated_at = ?
			WHERE message_id = ? AND queue_name = ?
        `)
		_, err = tx.ExecContext(ctx, updateQuery, targetQueueName, messagepb.Message_Metadata_PENDING, attemptsLeft, s.nowMs(), messageId, dlqName)
		if err != nil {
			var pqErr *pq.Error
			if errors.As(err, &pqErr) && pqErr.Code == "23505" {
				return domainerror.New(domainerror.AlreadyExists, fmt.Sprintf("message %q already exists in queue %q", messageId, targetQueueName), err)
			}
			return fmt.Errorf("update message: %w", err)
		}

		return s.StateManager.MoveCounter(ctx, tx, dlqName, oldState, targetQueueName, messagepb.Message_Metadata_PENDING)
	})

	if err == nil {
		// Record successful DLQ retry
		metrics.IncrementDLQRetry(dlqName, targetQueueName)
		metrics.RecordStateTransition(targetQueueName, "ERRORED", "PENDING")
	}

	return err
}

// DeleteDLQMessage permanently deletes a message from DLQ.
func (s *Storage) DeleteDLQMessage(ctx context.Context, queueName string, messageId string) error {
	return s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		var oldState messagepb.Message_Metadata_State
		err := tx.QueryRowContext(ctx, s.ph(`SELECT state FROM cq_messages WHERE message_id = ? AND queue_name = ?`), messageId, queueName).Scan(&oldState)
		if err == sql.ErrNoRows {
			return domainerror.New(domainerror.NotFound, fmt.Sprintf("message not found: %s", messageId), err)
		}
		if err != nil {
			return fmt.Errorf("query message state: %w", err)
		}

		if oldState != messagepb.Message_Metadata_ERRORED {
			return domainerror.New(domainerror.FailedPrecondition, "message is not in DLQ state", nil)
		}

		query := s.ph(`DELETE FROM cq_messages WHERE message_id = ? AND queue_name = ?`)
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

		return s.StateManager.RemoveCounter(ctx, tx, queueName, oldState)
	})
}

// PurgeDLQ deletes all errored messages for a queue and returns the count deleted.
func (s *Storage) PurgeDLQ(ctx context.Context, queueName string) (int64, error) {
	var deletedCount int64
	err := s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		countQuery := s.ph(`SELECT COUNT(*) FROM cq_messages WHERE queue_name = ? AND state = ?`)
		var count int64
		if err := tx.QueryRowContext(ctx, countQuery, queueName, int(messagepb.Message_Metadata_ERRORED)).Scan(&count); err != nil {
			return fmt.Errorf("count DLQ messages: %w", err)
		}

		if count == 0 {
			deletedCount = 0
			return nil
		}

		deleteQuery := s.ph(`DELETE FROM cq_messages WHERE queue_name = ? AND state = ?`)
		result, err := tx.ExecContext(ctx, deleteQuery, queueName, int(messagepb.Message_Metadata_ERRORED))
		if err != nil {
			return fmt.Errorf("delete DLQ messages: %w", err)
		}

		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("get rows affected: %w", err)
		}

		deletedCount = rows

		for i := int64(0); i < rows; i++ {
			if err := s.StateManager.RemoveCounter(ctx, tx, queueName, messagepb.Message_Metadata_ERRORED); err != nil {
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
