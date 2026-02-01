package background

import (
	"context"
	"fmt"
	"time"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	"github.com/adrien19/chronoqueue/pkg/metrics"
	sqlbase "github.com/adrien19/chronoqueue/pkg/repository/sql"
)

// ReclaimableBackend defines internal operations needed for lease reclamation.
//
// Design Note: This is an INTERNAL interface, NOT part of the public BackendStorage interface.
//
// Why keep it separate?
//
//  1. Implementation-Specific: The find-and-update pattern is SQL-specific.
//     Other backends (Redis, in-memory) might use different mechanisms:
//     - Redis: ZRANGEBYSCORE on sorted set by expiry time
//     - In-memory: Timer-based callbacks
//     - Message broker: Native TTL handling
//
//  2. Prevents Interface Pollution: BackendStorage defines public operations
//     (EnqueueMessage, ClaimMessage, etc.). Reclaim operations are internal
//     background maintenance, not part of the user-facing API.
//
//  3. Flexible Evolution: We can change reclaim logic without affecting
//     the core storage interface contract.
//
// Only SQL-based backends (Postgres, SQLite) need to implement this interface.
// The ReclaimService uses this interface to remain type-safe while avoiding
// tight coupling to the main storage interface.
type ReclaimableBackend interface {
	// FindExpiredMessages locates messages with expired leases or heartbeats.
	//
	// Returns up to 'limit' messages from the specified queue that have:
	// - lease_expiry < current time, OR
	// - heartbeat_expiry < current time
	//
	// Used by: ReclaimService to identify messages that need reprocessing
	FindExpiredMessages(ctx context.Context, queueName string, limit int32) ([]*messagepb.Message, error)

	// ReclaimExpiredMessage processes an expired message.
	//
	// Behavior depends on remaining attempts:
	// - If attempts remain: Decrements attempts_left, moves to PENDING (requeue)
	// - If no attempts: Moves to ERRORED state and pushes to DLQ
	//
	// This method handles:
	// - State transitions
	// - Attempt counting
	// - Lease cleanup
	// - Metrics recording
	//
	// Used by: ReclaimService during periodic scans
	ReclaimExpiredMessage(ctx context.Context, queueName string, message *messagepb.Message) error
}

// ReclaimService handles expired lease reclamation for SQL storage backends.
// It periodically scans for messages with expired leases or heartbeat timeouts
// and either requeues them (if attempts remain) or moves them to DLQ.
//
// This service is generic and works with any SQL backend (SQLite, Postgres, etc.)
// by using the BaseSQL abstraction layer and the ReclaimableBackend interface.
type ReclaimService struct {
	backend  ReclaimableBackend // Type-safe backend implementing reclaim operations
	base     *sqlbase.BaseSQL   // For database utilities (logger, clock, serializer, etc.)
	interval time.Duration
	stopChan chan struct{}
	doneChan chan struct{}
}

// NewReclaimService creates a new reclaim service.
// The backend parameter must implement ReclaimableBackend (Postgres/SQLite Storage do this).
func NewReclaimService(backend ReclaimableBackend, base *sqlbase.BaseSQL, interval time.Duration) *ReclaimService {
	return &ReclaimService{
		backend:  backend,
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

// reclaimQueueMessages processes expired messages for a specific queue.
func (r *ReclaimService) reclaimQueueMessages(ctx context.Context, queueName string, nowMs int64) error {
	// Phase 1: Find expired messages using the backend interface
	expiredMessages, err := r.backend.FindExpiredMessages(ctx, queueName, 100)
	if err != nil {
		return fmt.Errorf("find expired messages: %w", err)
	}

	if len(expiredMessages) == 0 {
		return nil
	}

	r.base.Logger.DebugWithFields("Found expired messages",
		"queue", queueName,
		"count", len(expiredMessages),
	)

	// Phase 2: Process each message using the backend interface
	var reclaimed, errored int

	for _, msg := range expiredMessages {
		meta := msg.GetMetadata()
		if meta == nil {
			meta = &messagepb.Message_Metadata{}
			msg.Metadata = meta
		}

		r.base.Logger.InfoWithFields("Reclaiming timed-out message",
			"message_id", msg.GetMessageId(),
			"queue", queueName,
			"attempts_left", meta.AttemptsLeft,
			"max_attempts", meta.GetMaxAttempts(),
		)

		// Pre-compute expected state transition for metrics tracking
		// Backend will decrement AttemptsLeft by 1, then check if <= 0
		newAttemptsLeft := meta.GetAttemptsLeft() - 1
		transitionsToErrored := newAttemptsLeft <= 0 && meta.GetMaxAttempts() != -1

		// Use backend interface to reclaim the message
		if err := r.backend.ReclaimExpiredMessage(ctx, queueName, msg); err != nil {
			r.base.Logger.ErrorWithFields("Failed to reclaim message",
				"message_id", msg.GetMessageId(),
				"error", err,
			)
			continue
		}

		// Determine what happened for metrics based on pre-computed state
		if transitionsToErrored {
			errored++
			// Record metrics for DLQ ingestion
			dlqName := queueName + "-dlq"
			metrics.IncrementDLQIngestion(dlqName, queueName, "lease_timeout")
			metrics.IncrementLeaseExpirations(queueName, "lease")
			metrics.IncrementBackgroundServiceProcessedMessages("reclaim", queueName)
			metrics.RecordStateTransition(queueName, "RUNNING", "ERRORED")
		} else {
			reclaimed++
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
