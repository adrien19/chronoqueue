package background

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	"github.com/adrien19/chronoqueue/pkg/metrics"
	sqlbase "github.com/adrien19/chronoqueue/pkg/repository/sql"
)

// SchedulerService handles moving INVISIBLE messages to PENDING when their scheduled time arrives.
// This service is generic and works with any SQL backend (SQLite, Postgres, etc.)
// by using the BaseSQL abstraction layer.
type SchedulerService struct {
	base     *sqlbase.BaseSQL
	interval time.Duration
	stopChan chan struct{}
	doneChan chan struct{}
}

// NewSchedulerService creates a new scheduler service.
func NewSchedulerService(base *sqlbase.BaseSQL, interval time.Duration) *SchedulerService {
	return &SchedulerService{
		base:     base,
		interval: interval,
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),
	}
}

// Start begins the scheduler service loop.
func (s *SchedulerService) Start(ctx context.Context) {
	defer close(s.doneChan)
	s.base.Logger.Info("Starting SQL scheduler service", "interval", s.interval)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.processScheduledMessages(ctx); err != nil {
				s.base.Logger.ErrorWithFields("Scheduler cycle failed", "error", err)
			}
		case <-s.stopChan:
			s.base.Logger.Info("Stopping SQL scheduler service")
			return
		case <-ctx.Done():
			s.base.Logger.Info("SQL scheduler service stopped due to context cancellation")
			return
		}
	}
}

// Stop signals the service to stop and waits for graceful shutdown.
// Returns when the service has fully stopped or context times out.
func (s *SchedulerService) StopGracefully(ctx context.Context) error {
	close(s.stopChan)

	select {
	case <-s.doneChan:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("scheduler service shutdown timeout: %w", ctx.Err())
	}
}

// Stop signals the service to stop (legacy method for backward compatibility).
func (s *SchedulerService) Stop() {
	close(s.stopChan)
}

// scheduledMessage holds data for a message that needs activation
type scheduledMessage struct {
	id        int64
	queueName string
	messageID string
	message   *messagepb.Message
}

// processScheduledMessages moves INVISIBLE messages to PENDING when their time arrives.
// Uses two-phase pattern to avoid read-write deadlocks in single-writer databases.
func (s *SchedulerService) processScheduledMessages(ctx context.Context) error {
	start := time.Now()
	status := "success"
	defer func() {
		metrics.IncrementBackgroundServiceIterations("scheduler", status)
		metrics.ObserveBackgroundServiceIterationDuration("scheduler", time.Since(start).Seconds())
	}()

	nowMs := s.base.Clock.NowMs()

	// Phase 1: Collect scheduled messages (READ)
	messages, err := s.collectScheduledMessages(ctx, nowMs)
	if err != nil {
		status = "error"
		return fmt.Errorf("collect scheduled messages: %w", err)
	}

	if len(messages) == 0 {
		return nil
	}

	// Phase 2: Process updates (WRITE)
	var activated int

	for _, msg := range messages {
		if msg.message == nil || msg.message.Metadata == nil {
			s.base.Logger.ErrorWithFields("Scheduled message missing metadata", "message_id", msg.messageID)
			continue
		}

		oldState := msg.message.Metadata.State
		msg.message.Metadata.State = messagepb.Message_Metadata_PENDING

		// Update state in transaction
		if err := s.activateMessage(ctx, msg.id, msg.queueName, msg.messageID, msg.message, oldState); err != nil {
			s.base.Logger.ErrorWithFields(
				"Failed to activate scheduled message",
				"message_id", msg.messageID,
				"error", err,
			)
			continue
		}

		activated++

		// Record metrics for activation
		metrics.IncrementScheduleActivations(msg.queueName)
		metrics.IncrementBackgroundServiceProcessedMessages("scheduler", msg.queueName)
		metrics.RecordStateTransition(msg.queueName, "INVISIBLE", "PENDING")

		// Calculate and record scheduler lag
		if msg.message.GetMetadata().GetScheduledTime() != nil {
			scheduledTime := msg.message.GetMetadata().GetScheduledTime().AsTime()
			lag := time.Since(scheduledTime).Seconds()
			metrics.SetScheduleLag(msg.queueName, lag)
		}

		s.base.Logger.DebugWithFields(
			"Activated scheduled message",
			"message_id", msg.messageID,
			"queue", msg.queueName,
		)
	}

	if activated > 0 {
		s.base.Logger.InfoWithFields(
			"Scheduler cycle completed",
			"activated", activated,
		)
	}

	return nil
}

// collectScheduledMessages queries for INVISIBLE messages whose time has arrived
func (s *SchedulerService) collectScheduledMessages(ctx context.Context, nowMs int64) ([]scheduledMessage, error) {
	query := fmt.Sprintf(`
		SELECT id, queue_name, message_id, metadata_pb
		FROM cq_messages
		WHERE state = %s
		  AND scheduled_at <= %s
		ORDER BY scheduled_at ASC, priority DESC
		LIMIT 100
	`, s.base.Dialect.Placeholder(1),
		s.base.Dialect.Placeholder(2))

	rows, err := s.base.DB.QueryContext(
		ctx, query,
		int(messagepb.Message_Metadata_INVISIBLE),
		nowMs,
	)
	if err != nil {
		return nil, fmt.Errorf("query scheduled messages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var messages []scheduledMessage

	for rows.Next() {
		var msg scheduledMessage
		var messageBytes []byte
		if err := rows.Scan(&msg.id, &msg.queueName, &msg.messageID, &messageBytes); err != nil {
			s.base.Logger.ErrorWithFields("Failed to scan scheduled message", "error", err)
			continue
		}

		message, err := s.base.Serializer.UnmarshalMessage(messageBytes)
		if err != nil {
			s.base.Logger.ErrorWithFields("Failed to unmarshal scheduled message", "message_id", msg.messageID, "error", err)
			continue
		}

		msg.message = message
		messages = append(messages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	return messages, nil
}

// activateMessage updates a scheduled message to PENDING state
func (s *SchedulerService) activateMessage(
	ctx context.Context,
	id int64,
	queueName string,
	messageID string,
	message *messagepb.Message,
	oldState messagepb.Message_Metadata_State,
) error {
	return s.base.WithTransaction(ctx, &sqlbase.TxOptions{Timeout: 5 * time.Second}, func(tx *sql.Tx) error {
		// Marshal updated message
		metadataBytes, err := s.base.Serializer.MarshalMessage(message)
		if err != nil {
			return fmt.Errorf("marshal message: %w", err)
		}

		// Update message state
		updateQuery := fmt.Sprintf(`
			UPDATE cq_messages
			SET state = %s,
			    metadata_pb = %s,
			    updated_at = %s
			WHERE id = %s
		`, s.base.Dialect.Placeholder(1),
			s.base.Dialect.Placeholder(2),
			s.base.Dialect.Placeholder(3),
			s.base.Dialect.Placeholder(4))

		_, err = tx.ExecContext(
			ctx, updateQuery,
			int(message.GetMetadata().GetState()),
			metadataBytes,
			s.base.Clock.NowMs(),
			id,
		)
		if err != nil {
			return fmt.Errorf("update message: %w", err)
		}

		// Update state counters
		if err := s.base.StateManager.UpdateCounters(ctx, tx, queueName, oldState, message.GetMetadata().GetState()); err != nil {
			return fmt.Errorf("update counters: %w", err)
		}

		return nil
	})
}
