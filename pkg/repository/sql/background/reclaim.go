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

// ReclaimService handles expired lease reclamation for SQL storage backends.
// It periodically scans for messages with expired leases or heartbeat timeouts
// and either requeues them (if attempts remain) or moves them to DLQ.
//
// This service is generic and works with any SQL backend (SQLite, Postgres, etc.)
// by using the BaseSQL abstraction layer.
type ReclaimService struct {
	base     *sqlbase.BaseSQL
	interval time.Duration
	stopChan chan struct{}
	doneChan chan struct{}
}

// NewReclaimService creates a new reclaim service.
func NewReclaimService(base *sqlbase.BaseSQL, interval time.Duration) *ReclaimService {
	return &ReclaimService{
		base:     base,
		interval: interval,
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),
	}
}

// Start begins the reclaim service loop.
func (r *ReclaimService) Start(ctx context.Context) {
	defer close(r.doneChan)
	r.base.Logger.Info("Starting SQL reclaim service", "interval", r.interval)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := r.reclaimExpiredMessages(ctx); err != nil {
				r.base.Logger.ErrorWithFields("Reclaim cycle failed", "error", err)
			}
		case <-r.stopChan:
			r.base.Logger.Info("Stopping SQL reclaim service")
			return
		case <-ctx.Done():
			r.base.Logger.Info("SQL reclaim service stopped due to context cancellation")
			return
		}
	}
}

// StopGracefully signals the service to stop and waits for graceful shutdown.
// Returns when the service has fully stopped or context times out.
func (r *ReclaimService) StopGracefully(ctx context.Context) error {
	close(r.stopChan)

	select {
	case <-r.doneChan:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("reclaim service shutdown timeout: %w", ctx.Err())
	}
}

// Stop signals the service to stop (legacy method for backward compatibility).
func (r *ReclaimService) Stop() {
	close(r.stopChan)
}

// reclaimExpiredMessages scans all queues for expired messages and processes them.
// Uses two-phase pattern to avoid read-write deadlocks in single-writer databases.
func (r *ReclaimService) reclaimExpiredMessages(ctx context.Context) error {
	start := time.Now()
	status := "success"
	defer func() {
		metrics.IncrementBackgroundServiceIterations("reclaim", status)
		metrics.ObserveBackgroundServiceIterationDuration("reclaim", time.Since(start).Seconds())
	}()

	// Get all queue names
	queues, err := r.listQueueNames(ctx)
	if err != nil {
		status = "error"
		return fmt.Errorf("list queues: %w", err)
	}

	nowMs := r.base.Clock.NowMs()

	for _, queueName := range queues {
		if err := r.reclaimQueueMessages(ctx, queueName, nowMs); err != nil {
			r.base.Logger.ErrorWithFields("Failed to reclaim messages for queue",
				"queue", queueName,
				"error", err,
			)
		}
	}

	return nil
}

// expiredMessage holds data for a message that needs reclamation
type expiredMessage struct {
	id        int64
	messageID string
	message   *messagepb.Message
}

// reclaimQueueMessages processes expired messages for a specific queue.
func (r *ReclaimService) reclaimQueueMessages(ctx context.Context, queueName string, nowMs int64) error {
	// Phase 1: Collect expired messages (READ)
	// This avoids holding read locks while doing writes
	expiredMessages, err := r.collectExpiredMessages(ctx, queueName, nowMs)
	if err != nil {
		return fmt.Errorf("collect expired messages: %w", err)
	}

	if len(expiredMessages) == 0 {
		return nil
	}

	r.base.Logger.DebugWithFields("Found expired messages",
		"queue", queueName,
		"count", len(expiredMessages),
	)

	// Phase 2: Process updates (WRITE)
	var reclaimed, errored int

	for _, msg := range expiredMessages {
		meta := msg.message.GetMetadata()
		if meta == nil {
			meta = &messagepb.Message_Metadata{}
		}
		oldState := meta.State

		r.base.Logger.InfoWithFields("Reclaiming timed-out message",
			"message_id", msg.messageID,
			"queue", queueName,
			"attempts_left", meta.AttemptsLeft,
			"max_attempts", meta.GetMaxAttempts(),
		)

		// Determine action based on attempts remaining
		if meta.AttemptsLeft == 0 && meta.MaxAttempts != -1 {
			// Move to ERRORED state
			meta.State = messagepb.Message_Metadata_ERRORED
			errored++

			if err := r.updateMessageState(ctx, msg.id, queueName, msg.messageID, msg.message, oldState); err != nil {
				r.base.Logger.ErrorWithFields("Failed to update message to ERRORED",
					"message_id", msg.messageID,
					"error", err,
				)
				continue
			}

			// Record metrics for DLQ ingestion
			dlqName := queueName + "-dlq"
			metrics.IncrementDLQIngestion(dlqName, queueName, "lease_timeout")
			metrics.IncrementLeaseExpirations(queueName, "lease")
			metrics.IncrementBackgroundServiceProcessedMessages("reclaim", queueName)
			metrics.RecordStateTransition(queueName, "RUNNING", "ERRORED")

			// Push to DLQ
			if err := r.pushMessageToDLQ(ctx, queueName, msg.messageID, "lease/heartbeat timeout", meta); err != nil {
				r.base.Logger.ErrorWithFields("Failed to push to DLQ",
					"message_id", msg.messageID,
					"error", err,
				)
			}
		} else {
			// Retry: decrement attempts and return to PENDING
			if meta.AttemptsLeft > 0 {
				meta.AttemptsLeft--
			}
			meta.State = messagepb.Message_Metadata_PENDING
			meta.CurrentAttempt = nil // Clear attempt runtime
			reclaimed++

			if err := r.updateMessageState(ctx, msg.id, queueName, msg.messageID, msg.message, oldState); err != nil {
				r.base.Logger.ErrorWithFields("Failed to requeue message",
					"message_id", msg.messageID,
					"error", err,
				)
				continue
			}

			// Record metrics for lease expiration and requeue
			metrics.IncrementLeaseExpirations(queueName, "lease")
			metrics.IncrementBackgroundServiceProcessedMessages("reclaim", queueName)
			metrics.RecordStateTransition(queueName, "RUNNING", "PENDING")
		}
	}

	if reclaimed > 0 || errored > 0 {
		r.base.Logger.InfoWithFields("Reclaim cycle completed for queue",
			"queue", queueName,
			"reclaimed", reclaimed,
			"errored", errored,
		)
	}

	return nil
}

// collectExpiredMessages queries for messages with expired leases/heartbeats
func (r *ReclaimService) collectExpiredMessages(ctx context.Context, queueName string, nowMs int64) ([]expiredMessage, error) {
	query := fmt.Sprintf(`
		SELECT id, message_id, metadata_pb
		FROM cq_messages
		WHERE queue_name = %s
		  AND state = %s
		  AND (lease_expiry < %s OR (heartbeat_expiry IS NOT NULL AND heartbeat_expiry < %s))
		ORDER BY priority DESC, id ASC
		LIMIT 100
	`, r.base.Dialect.Placeholder(1),
		r.base.Dialect.Placeholder(2),
		r.base.Dialect.Placeholder(3),
		r.base.Dialect.Placeholder(4))

	rows, err := r.base.DB.QueryContext(ctx, query,
		queueName,
		int(messagepb.Message_Metadata_RUNNING),
		nowMs,
		nowMs,
	)
	if err != nil {
		return nil, fmt.Errorf("query expired messages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var messages []expiredMessage

	for rows.Next() {
		var msg expiredMessage
		var metadataBytes []byte
		if err := rows.Scan(&msg.id, &msg.messageID, &metadataBytes); err != nil {
			r.base.Logger.ErrorWithFields("Failed to scan message row", "error", err)
			continue
		}

		m, err := r.base.Serializer.UnmarshalMessage(metadataBytes)
		if err != nil {
			r.base.Logger.ErrorWithFields("Failed to unmarshal message",
				"message_id", msg.messageID,
				"error", err,
			)
			// Delete corrupt row to stop perpetual failures
			deleteQuery := fmt.Sprintf("DELETE FROM cq_messages WHERE id = %s", r.base.Dialect.Placeholder(1))
			_, delErr := r.base.DB.ExecContext(ctx, deleteQuery, msg.id)
			if delErr != nil {
				r.base.Logger.ErrorWithFields("Failed to delete corrupt message",
					"message_id", msg.messageID,
					"error", delErr,
				)
			}
			continue
		}
		msg.message = m
		messages = append(messages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	return messages, nil
}

// updateMessageState updates the message state and metadata in a transaction
func (r *ReclaimService) updateMessageState(
	ctx context.Context,
	id int64,
	queueName string,
	messageID string,
	message *messagepb.Message,
	oldState messagepb.Message_Metadata_State,
) error {
	return r.base.WithTransaction(ctx, &sqlbase.TxOptions{Timeout: 5 * time.Second}, func(tx *sql.Tx) error {
		// Marshal updated message
		metadataBytes, err := r.base.Serializer.MarshalMessage(message)
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
		`, r.base.Dialect.Placeholder(1),
			r.base.Dialect.Placeholder(2),
			r.base.Dialect.Placeholder(3),
			r.base.Dialect.Placeholder(4))

		_, err = tx.ExecContext(ctx, updateQuery,
			int(message.GetMetadata().GetState()),
			metadataBytes,
			r.base.Clock.NowMs(),
			id,
		)
		if err != nil {
			return fmt.Errorf("update message: %w", err)
		}

		// Update state counters
		if err := r.base.StateManager.UpdateCounters(ctx, tx, queueName, oldState, message.GetMetadata().GetState()); err != nil {
			return fmt.Errorf("update counters: %w", err)
		}

		return nil
	})
}

// pushMessageToDLQ adds a message to the dead letter queue
func (r *ReclaimService) pushMessageToDLQ(
	ctx context.Context,
	queueName string,
	messageID string,
	reason string,
	meta *messagepb.Message_Metadata,
) error {
	return r.base.WithTransaction(ctx, &sqlbase.TxOptions{Timeout: 5 * time.Second}, func(tx *sql.Tx) error {
		insertQuery := fmt.Sprintf(`
			INSERT INTO cq_dlq (queue_name, message_id, reason, metadata_pb, created_at)
			VALUES (%s, %s, %s, %s, %s)
		`, r.base.Dialect.Placeholder(1),
			r.base.Dialect.Placeholder(2),
			r.base.Dialect.Placeholder(3),
			r.base.Dialect.Placeholder(4),
			r.base.Dialect.Placeholder(5))

		metadataBytes, err := r.base.Serializer.MarshalMessageMetadata(meta)
		if err != nil {
			return fmt.Errorf("marshal metadata: %w", err)
		}

		_, err = tx.ExecContext(ctx, insertQuery,
			queueName,
			messageID,
			reason,
			metadataBytes,
			r.base.Clock.NowMs(),
		)
		if err != nil {
			return fmt.Errorf("insert DLQ entry: %w", err)
		}

		return nil
	})
}

// listQueueNames returns all queue names in the system
func (r *ReclaimService) listQueueNames(ctx context.Context) ([]string, error) {
	query := "SELECT name FROM cq_queues ORDER BY name"
	rows, err := r.base.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query queues: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var queues []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan queue name: %w", err)
		}
		queues = append(queues, name)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	return queues, nil
}
