package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	"github.com/adrien19/chronoqueue/internal/domainerror"
)

// CreateQueue creates a new queue
func (s *Storage) CreateQueue(ctx context.Context, queue *queuepb.Queue) error {
	return s.WithSerializableTransaction(ctx, func(tx *sql.Tx) error {
		metadata := queue.GetMetadata()
		if dlqName := metadata.GetDeadLetterQueueName(); dlqName != "" && !metadata.GetAutoCreateDlq() {
			var exists int
			if err := tx.QueryRowContext(ctx, `SELECT 1 FROM cq_queues WHERE name = ?`, dlqName).Scan(&exists); err != nil {
				if err == sql.ErrNoRows {
					return domainerror.New(domainerror.NotFound, fmt.Sprintf("dead letter queue %q not found", dlqName), err)
				}
				return fmt.Errorf("query dead letter queue: %w", err)
			}
		}

		return s.insertQueue(ctx, tx, queue)
	})
}

func (s *Storage) CreateQueueWithDLQ(ctx context.Context, queue *queuepb.Queue, dlq *queuepb.Queue) error {
	if err := validateQueueDLQPair(queue, dlq); err != nil {
		return err
	}
	return s.WithSerializableTransaction(ctx, func(tx *sql.Tx) error {
		if err := s.insertQueue(ctx, tx, dlq); err != nil {
			return fmt.Errorf("create automatic DLQ %q: %w", dlq.GetName(), err)
		}
		return s.insertQueue(ctx, tx, queue)
	})
}

func validateQueueDLQPair(queue *queuepb.Queue, dlq *queuepb.Queue) error {
	if queue == nil || dlq == nil {
		return domainerror.New(domainerror.InvalidArgument, "source queue and dead letter queue are required", nil)
	}
	target := queue.GetMetadata().GetDeadLetterQueueName()
	if target == "" || dlq.GetName() == "" {
		return domainerror.New(domainerror.InvalidArgument, "dead letter queue target is required", nil)
	}
	if target != dlq.GetName() {
		return domainerror.New(domainerror.InvalidArgument, fmt.Sprintf("source queue dead letter target %q does not match queue %q", target, dlq.GetName()), nil)
	}
	return nil
}

func (s *Storage) insertQueue(ctx context.Context, tx *sql.Tx, queue *queuepb.Queue) error {
	queueBytes, err := s.Serializer.MarshalQueue(queue)
	if err != nil {
		return fmt.Errorf("marshal queue: %w", err)
	}
	query := `INSERT INTO cq_queues (name, metadata_pb, dead_letter_queue_name, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`
	nowMs := s.nowMs()
	if _, err := tx.ExecContext(ctx, query, queue.Name, queueBytes, nullableDLQName(queue.GetMetadata().GetDeadLetterQueueName()), nowMs, nowMs); err != nil {
		if isUniqueConstraintError(err) {
			return domainerror.New(domainerror.AlreadyExists, fmt.Sprintf("queue %q already exists", queue.Name), err)
		}
		return fmt.Errorf("insert queue: %w", err)
	}
	return nil
}

func (s *Storage) IsDLQ(ctx context.Context, name string) (bool, error) {
	var exists bool
	if err := s.DB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM cq_queues WHERE dead_letter_queue_name = ?)`, name).Scan(&exists); err != nil {
		return false, fmt.Errorf("query DLQ relationship: %w", err)
	}
	return exists, nil
}

// GetQueue retrieves a queue by name
func (s *Storage) GetQueue(ctx context.Context, name string) (*queuepb.Queue, error) {
	query := `SELECT metadata_pb FROM cq_queues WHERE name = ?`
	var queueBytes []byte
	err := s.DB.QueryRowContext(ctx, query, name).Scan(&queueBytes)
	if err == sql.ErrNoRows {
		return nil, domainerror.New(domainerror.NotFound, fmt.Sprintf("queue %q not found", name), err)
	}
	if err != nil {
		return nil, fmt.Errorf("query queue: %w", err)
	}

	queue, err := s.Serializer.UnmarshalQueue(queueBytes)
	if err != nil {
		return nil, fmt.Errorf("unmarshal queue: %w", err)
	}

	return queue, nil
}

// GetQueueMetadata retrieves queue metadata by name
func (s *Storage) GetQueueMetadata(ctx context.Context, name string) (*queuepb.QueueMetadata, error) {
	queue, err := s.GetQueue(ctx, name)
	if err != nil {
		return nil, err
	}
	return queue.GetMetadata(), nil
}

// ListQueues returns all queues
func (s *Storage) ListQueues(ctx context.Context) ([]*queuepb.Queue, error) {
	return s.ListQueuesWithPrefix(ctx, "")
}

// ListQueuesWithPrefix returns queues whose names start with prefix.
func (s *Storage) ListQueuesWithPrefix(ctx context.Context, prefix string) ([]*queuepb.Queue, error) {
	queues, _, err := s.ListQueuesPage(ctx, prefix, int32(^uint32(0)>>1), "")
	return queues, err
}

func (s *Storage) ListQueuesPage(ctx context.Context, prefix string, limit int32, cursor string) ([]*queuepb.Queue, string, error) {
	if limit <= 0 {
		return nil, "", nil
	}
	query := `SELECT name, metadata_pb FROM cq_queues WHERE instr(name, ?) = 1 AND name > ? ORDER BY name LIMIT ?`
	rows, err := s.DB.QueryContext(ctx, query, prefix, cursor, int64(limit)+1)
	if err != nil {
		return nil, "", fmt.Errorf("query queues: %w", err)
	}
	var queues []*queuepb.Queue
	var scanErr error
	for rows.Next() {
		var name string
		var queueBytes []byte
		if err := rows.Scan(&name, &queueBytes); err != nil {
			scanErr = fmt.Errorf("scan queue: %w", err)
			break
		}

		queue, err := s.Serializer.UnmarshalQueue(queueBytes)
		if err != nil {
			scanErr = fmt.Errorf("unmarshal queue: %w", err)
			break
		}

		queues = append(queues, queue)
	}

	if scanErr == nil {
		scanErr = rows.Err()
	}
	closeErr := rows.Close()
	if scanErr != nil {
		return nil, "", scanErr
	}
	if closeErr != nil {
		return nil, "", fmt.Errorf("close queue rows: %w", closeErr)
	}
	if len(queues) <= int(limit) {
		return queues, "", nil
	}
	queues = queues[:limit]
	return queues, queues[len(queues)-1].GetName(), nil
}

// DeleteQueue deletes a queue
func (s *Storage) DeleteQueue(ctx context.Context, name string) error {
	return s.WithSerializableTransaction(ctx, func(tx *sql.Tx) error {
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM cq_queues WHERE name = ?`, name).Scan(&exists); err != nil {
			if err == sql.ErrNoRows {
				return domainerror.New(domainerror.NotFound, fmt.Sprintf("queue %q not found", name), err)
			}
			return fmt.Errorf("query queue for deletion: %w", err)
		}

		var referringQueue string
		if err := tx.QueryRowContext(ctx, `SELECT name FROM cq_queues WHERE dead_letter_queue_name = ? LIMIT 1`, name).Scan(&referringQueue); err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("query queue reference: %w", err)
		} else if err == nil {
			return domainerror.New(domainerror.FailedPrecondition, fmt.Sprintf("queue %q is referenced as a dead letter queue by %q", name, referringQueue), nil)
		}

		nowMs := s.Clock.NowMs()
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO cq_schedule_archive (schedule_id, metadata_pb, deleted_at)
			SELECT id, metadata_pb, ? FROM cq_schedules WHERE queue_name = ?
			ON CONFLICT(schedule_id) DO UPDATE SET metadata_pb = excluded.metadata_pb, deleted_at = excluded.deleted_at`, nowMs, name); err != nil {
			return fmt.Errorf("archive queue schedules: %w", err)
		}

		if _, err := tx.ExecContext(ctx, `DELETE FROM cq_queues WHERE name = ?`, name); err != nil {
			return fmt.Errorf("delete queue: %w", err)
		}
		return nil
	})
}
