package background

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	"github.com/adrien19/chronoqueue/pkg/metrics"
	sqlbase "github.com/adrien19/chronoqueue/pkg/repository/sql"
)

// CleanupService handles permanent deletion of soft-deleted messages
// after their retention period expires.
type CleanupService struct {
	base             *sqlbase.BaseSQL
	interval         time.Duration
	batchSize        int
	maxDrainBatches  int
	maxCycleDuration time.Duration
	stopChan         chan struct{}
	doneChan         chan struct{}
}

// NewCleanupService creates a new cleanup service.
// Recommended interval: 1 hour (cleanup is not time-sensitive).
func NewCleanupService(base *sqlbase.BaseSQL, interval time.Duration) *CleanupService {
	return &CleanupService{
		base:             base,
		interval:         interval,
		batchSize:        defaultBackgroundBatchSize,
		maxDrainBatches:  defaultBackgroundMaxDrainBatches,
		maxCycleDuration: defaultBackgroundCycleDuration,
		stopChan:         make(chan struct{}),
		doneChan:         make(chan struct{}),
	}
}

// Start begins the cleanup service loop.
func (s *CleanupService) Start(ctx context.Context) {
	defer close(s.doneChan)
	s.base.Logger.Info("Starting SQL cleanup service", "interval", s.interval)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	// Run immediately on startup
	if err := s.cleanupExpiredMessages(ctx); err != nil {
		s.base.Logger.ErrorWithFields("Initial cleanup failed", "error", err)
	}

	for {
		select {
		case <-ticker.C:
			if err := s.cleanupExpiredMessages(ctx); err != nil {
				s.base.Logger.ErrorWithFields("Cleanup cycle failed", "error", err)
			}
		case <-s.stopChan:
			s.base.Logger.Info("Stopping SQL cleanup service")
			return
		case <-ctx.Done():
			s.base.Logger.Info("SQL cleanup service stopped due to context cancellation")
			return
		}
	}
}

// StopGracefully signals the service to stop and waits for graceful shutdown.
func (s *CleanupService) StopGracefully(ctx context.Context) error {
	close(s.stopChan)

	select {
	case <-s.doneChan:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("cleanup service shutdown timeout: %w", ctx.Err())
	}
}

// cleanupExpiredMessages permanently deletes messages where deleted_at < now().
func (s *CleanupService) cleanupExpiredMessages(ctx context.Context) error {
	start := time.Now()
	status := "success"
	deletedCount := int64(0)
	batches := 0

	defer func() {
		metrics.IncrementBackgroundServiceIterations("cleanup", status)
		metrics.ObserveBackgroundServiceIterationDuration("cleanup", time.Since(start).Seconds())
		metrics.ObserveBackgroundServiceCycle("cleanup", batches, deletedCount)
		if deletedCount > 0 {
			s.base.Logger.InfoWithFields(
				"Cleanup completed",
				"deleted_messages", deletedCount,
				"duration_ms", time.Since(start).Milliseconds(),
			)
		}
	}()

	nowMs := s.base.Clock.NowMs()

	budgetExhausted := true
	for batch := 0; batch < s.maxDrainBatches && time.Since(start) < s.maxCycleDuration; batch++ {
		if err := ctx.Err(); err != nil {
			status = "error"
			return fmt.Errorf("cleanup cycle canceled: %w", err)
		}
		deleted, err := s.cleanupExpiredBatch(ctx, nowMs, s.batchSize)
		if err != nil {
			status = "error"
			return fmt.Errorf("delete expired message batch: %w", err)
		}
		batches++
		deletedCount += deleted
		metrics.ObserveBackgroundServiceBatchSize("cleanup", int(deleted))
		if deleted < int64(s.batchSize) {
			budgetExhausted = false
			break
		}
		yieldBetweenBatches(s.base.Dialect.SupportsSkipLocked())
	}
	if budgetExhausted {
		metrics.IncrementBackgroundServiceBudgetExhausted("cleanup")
	}

	if deletedCount > 0 {
		metrics.RecordMessagesCleanedUp(deletedCount)
	}

	return nil
}

func (s *CleanupService) cleanupExpiredBatch(ctx context.Context, nowMs int64, limit int) (int64, error) {
	var deleted int64
	err := s.base.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		lockClause := ""
		if s.base.Dialect.SupportsSkipLocked() {
			lockClause = " FOR UPDATE SKIP LOCKED"
		}
		query := fmt.Sprintf(`
			WITH expired AS (
				SELECT id
				FROM cq_messages
				WHERE deleted_at IS NOT NULL AND deleted_at <= %s
				ORDER BY deleted_at, id
				LIMIT %d%s
			)
			DELETE FROM cq_messages
			WHERE id IN (SELECT id FROM expired)
			RETURNING queue_name, state
		`, s.base.Dialect.Placeholder(1), limit, lockClause)

		rows, err := tx.QueryContext(ctx, strings.TrimSpace(query), nowMs)
		if err != nil {
			return fmt.Errorf("delete expired messages: %w", err)
		}
		counts := make(map[string]map[messagepb.Message_Metadata_State]int64)
		for rows.Next() {
			var queueName string
			var state messagepb.Message_Metadata_State
			if err := rows.Scan(&queueName, &state); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan deleted message: %w", err)
			}
			if counts[queueName] == nil {
				counts[queueName] = make(map[messagepb.Message_Metadata_State]int64)
			}
			counts[queueName][state]++
			deleted++
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("iterate deleted messages: %w", err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close deleted message rows: %w", err)
		}
		for queueName, stateCounts := range counts {
			for state, count := range stateCounts {
				if err := s.base.StateManager.RemoveCounters(ctx, tx, queueName, state, count); err != nil {
					return fmt.Errorf("update %s cleanup counters: %w", queueName, err)
				}
			}
		}
		return nil
	})
	return deleted, err
}
