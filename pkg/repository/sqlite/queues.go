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
	queueBytes, err := s.Serializer.MarshalQueue(queue)
	if err != nil {
		return fmt.Errorf("marshal queue: %w", err)
	}

	query := `INSERT INTO cq_queues (name, metadata_pb, created_at, updated_at) VALUES (?, ?, ?, ?)`
	nowMs := s.nowMs()
	_, err = s.DB.ExecContext(ctx, query, queue.Name, queueBytes, nowMs, nowMs)
	if err != nil {
		if isUniqueConstraintError(err) {
			return domainerror.New(domainerror.AlreadyExists, fmt.Sprintf("queue %q already exists", queue.Name), err)
		}
		return fmt.Errorf("insert queue: %w", err)
	}

	return nil
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
	query := `SELECT metadata_pb FROM cq_queues WHERE instr(name, ?) = 1 ORDER BY name`
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

// DeleteQueue deletes a queue
func (s *Storage) DeleteQueue(ctx context.Context, name string) error {
	query := `DELETE FROM cq_queues WHERE name = ?`
	result, err := s.DB.ExecContext(ctx, query, name)
	if err != nil {
		return fmt.Errorf("delete queue: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if rows == 0 {
		return domainerror.New(domainerror.NotFound, fmt.Sprintf("queue %q not found", name), nil)
	}

	return nil
}
