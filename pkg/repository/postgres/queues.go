package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/lib/pq"

	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	"github.com/adrien19/chronoqueue/internal/domainerror"
)

const lockQueuesForRelationshipMutation = `LOCK TABLE cq_queues IN SHARE ROW EXCLUSIVE MODE`

func (s *Storage) CreateQueue(ctx context.Context, queue *queuepb.Queue) error {
	queueBytes, err := s.Serializer.MarshalQueue(queue)
	if err != nil {
		return fmt.Errorf("marshal queue: %w", err)
	}

	return s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, lockQueuesForRelationshipMutation); err != nil {
			return fmt.Errorf("lock queues for creation: %w", err)
		}

		metadata := queue.GetMetadata()
		if dlqName := metadata.GetDeadLetterQueueName(); dlqName != "" && !metadata.GetAutoCreateDlq() {
			var exists int
			if err := tx.QueryRowContext(ctx, `SELECT 1 FROM cq_queues WHERE name = $1`, dlqName).Scan(&exists); err != nil {
				if err == sql.ErrNoRows {
					return domainerror.New(domainerror.NotFound, fmt.Sprintf("dead letter queue %q not found", dlqName), err)
				}
				return fmt.Errorf("query dead letter queue: %w", err)
			}
		}

		query := s.ph(`INSERT INTO cq_queues (name, metadata_pb, state_counts, created_at, updated_at) VALUES (?, ?, '{}', ?, ?)`)
		now := s.nowMs()
		if _, err := tx.ExecContext(ctx, query, queue.Name, queueBytes, now, now); err != nil {
			var pqErr *pq.Error
			if errors.As(err, &pqErr) && pqErr.Code == "23505" {
				return domainerror.New(domainerror.AlreadyExists, fmt.Sprintf("queue %q already exists", queue.Name), err)
			}
			return fmt.Errorf("insert queue: %w", err)
		}

		return nil
	})
}

// GetQueue retrieves a queue by name.
func (s *Storage) GetQueue(ctx context.Context, name string) (*queuepb.Queue, error) {
	query := s.ph(`SELECT metadata_pb FROM cq_queues WHERE name = ?`)
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

// GetQueueMetadata retrieves queue metadata by name.
func (s *Storage) GetQueueMetadata(ctx context.Context, name string) (*queuepb.QueueMetadata, error) {
	queue, err := s.GetQueue(ctx, name)
	if err != nil {
		return nil, err
	}
	return queue.GetMetadata(), nil
}

// ListQueues returns all queues.
func (s *Storage) ListQueues(ctx context.Context) ([]*queuepb.Queue, error) {
	return s.ListQueuesWithPrefix(ctx, "")
}

// ListQueuesWithPrefix returns queues whose names start with prefix.
func (s *Storage) ListQueuesWithPrefix(ctx context.Context, prefix string) ([]*queuepb.Queue, error) {
	query := `SELECT metadata_pb FROM cq_queues WHERE STRPOS(name, $1) = 1 ORDER BY name`
	rows, err := s.DB.QueryContext(ctx, query, prefix)
	if err != nil {
		return nil, fmt.Errorf("query queues: %w", err)
	}
	var queues []*queuepb.Queue
	var scanErr error
	for rows.Next() {
		var queueBytes []byte
		if err := rows.Scan(&queueBytes); err != nil {
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
		return nil, scanErr
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close queue rows: %w", closeErr)
	}
	return queues, nil
}

// DeleteQueue deletes a queue.
func (s *Storage) DeleteQueue(ctx context.Context, name string) error {
	return s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, lockQueuesForRelationshipMutation); err != nil {
			return fmt.Errorf("lock queues for deletion: %w", err)
		}
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM cq_queues WHERE name = $1`, name).Scan(&exists); err != nil {
			if err == sql.ErrNoRows {
				return domainerror.New(domainerror.NotFound, fmt.Sprintf("queue %q not found", name), err)
			}
			return fmt.Errorf("query queue for deletion: %w", err)
		}

		rows, err := tx.QueryContext(ctx, `SELECT metadata_pb FROM cq_queues WHERE name <> $1`, name)
		if err != nil {
			return fmt.Errorf("query queue references: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var queueBytes []byte
			if err := rows.Scan(&queueBytes); err != nil {
				return fmt.Errorf("scan queue reference: %w", err)
			}
			queue, err := s.Serializer.UnmarshalQueue(queueBytes)
			if err != nil {
				return fmt.Errorf("unmarshal queue reference: %w", err)
			}
			if queue.GetMetadata().GetDeadLetterQueueName() == name {
				return domainerror.New(domainerror.FailedPrecondition, fmt.Sprintf("queue %q is referenced as a dead letter queue by %q", name, queue.GetName()), nil)
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate queue references: %w", err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close queue references: %w", err)
		}

		if _, err := tx.ExecContext(ctx, `DELETE FROM cq_queues WHERE name = $1`, name); err != nil {
			return fmt.Errorf("delete queue: %w", err)
		}
		return nil
	})
}

// EnqueueMessage adds a message to a queue.
