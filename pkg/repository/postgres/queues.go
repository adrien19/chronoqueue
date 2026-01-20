package postgres

import (
	"context"
	"database/sql"
	"fmt"

	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
)

func (s *Storage) CreateQueue(ctx context.Context, queue *queuepb.Queue) error {
	queueBytes, err := s.Serializer.MarshalQueue(queue)
	if err != nil {
		return fmt.Errorf("marshal queue: %w", err)
	}

	query := s.ph(`INSERT INTO cq_queues (name, metadata_pb, state_counts, created_at, updated_at) VALUES (?, ?, '{}', ?, ?)`)
	now := s.nowMs()
	_, err = s.DB.ExecContext(ctx, query, queue.Name, queueBytes, now, now)
	if err != nil {
		return fmt.Errorf("insert queue: %w", err)
	}

	return nil
}

// GetQueue retrieves a queue by name.
func (s *Storage) GetQueue(ctx context.Context, name string) (*queuepb.Queue, error) {
	query := s.ph(`SELECT metadata_pb FROM cq_queues WHERE name = ?`)
	var queueBytes []byte
	err := s.DB.QueryRowContext(ctx, query, name).Scan(&queueBytes)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("queue not found: %s", name)
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
	query := `SELECT metadata_pb FROM cq_queues ORDER BY name`
	rows, err := s.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query queues: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var queues []*queuepb.Queue
	for rows.Next() {
		var queueBytes []byte
		if err := rows.Scan(&queueBytes); err != nil {
			return nil, fmt.Errorf("scan queue: %w", err)
		}

		queue, err := s.Serializer.UnmarshalQueue(queueBytes)
		if err != nil {
			return nil, fmt.Errorf("unmarshal queue: %w", err)
		}

		queues = append(queues, queue)
	}

	return queues, rows.Err()
}

// DeleteQueue deletes a queue.
func (s *Storage) DeleteQueue(ctx context.Context, name string) error {
	query := s.ph(`DELETE FROM cq_queues WHERE name = ?`)
	result, err := s.DB.ExecContext(ctx, query, name)
	if err != nil {
		return fmt.Errorf("delete queue: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("queue not found: %s", name)
	}

	return nil
}

// EnqueueMessage adds a message to a queue.
