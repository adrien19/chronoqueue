package background

import (
	"context"
	"fmt"
	"time"

	"github.com/adrien19/chronoqueue/pkg/metrics"
	sqlbase "github.com/adrien19/chronoqueue/pkg/repository/sql"
)

// CleanupService handles permanent deletion of soft-deleted messages
// after their retention period expires.
type CleanupService struct {
	base     *sqlbase.BaseSQL
	interval time.Duration
	stopChan chan struct{}
	doneChan chan struct{}
}

// NewCleanupService creates a new cleanup service.
// Recommended interval: 1 hour (cleanup is not time-sensitive).
func NewCleanupService(base *sqlbase.BaseSQL, interval time.Duration) *CleanupService {
	return &CleanupService{
		base:     base,
		interval: interval,
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),
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

	defer func() {
		metrics.IncrementBackgroundServiceIterations("cleanup", status)
		metrics.ObserveBackgroundServiceIterationDuration("cleanup", time.Since(start).Seconds())
		if deletedCount > 0 {
			s.base.Logger.InfoWithFields(
				"Cleanup completed",
				"deleted_messages", deletedCount,
				"duration_ms", time.Since(start).Milliseconds(),
			)
		}
	}()

	nowMs := s.base.Clock.NowMs()

	deleteQuery := fmt.Sprintf(`
		DELETE FROM cq_messages 
		WHERE deleted_at IS NOT NULL 
		  AND deleted_at <= %s
	`, s.base.Dialect.Placeholder(1))

	result, err := s.base.DB.ExecContext(ctx, deleteQuery, nowMs)
	if err != nil {
		status = "error"
		return fmt.Errorf("delete expired messages: %w", err)
	}

	deletedCount, err = result.RowsAffected()
	if err != nil {
		status = "error"
		return fmt.Errorf("get deleted count: %w", err)
	}

	if deletedCount > 0 {
		metrics.RecordMessagesCleanedUp(deletedCount)
	}

	return nil
}
