package background

import (
	"context"
	"sync"
	"time"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	"github.com/adrien19/chronoqueue/pkg/metrics"
	repositorysql "github.com/adrien19/chronoqueue/pkg/repository/sql"
)

// MetricsReporterService periodically reports queue state metrics to Prometheus
type MetricsReporterService struct {
	baseSQL        *repositorysql.BaseSQL
	interval       time.Duration
	stopChan       chan struct{}
	stoppedChan    chan struct{}
	wg             sync.WaitGroup
	lastReportedAt time.Time
	mu             sync.Mutex
}

// NewMetricsReporterService creates a new metrics reporter service
// interval specifies how often to refresh queue state metrics (recommended: 30s-60s)
func NewMetricsReporterService(baseSQL *repositorysql.BaseSQL, interval time.Duration) *MetricsReporterService {
	return &MetricsReporterService{
		baseSQL:     baseSQL,
		interval:    interval,
		stopChan:    make(chan struct{}),
		stoppedChan: make(chan struct{}),
	}
}

// Start begins the periodic metrics reporting
func (s *MetricsReporterService) Start(ctx context.Context) {
	s.wg.Add(1)
	defer s.wg.Done()
	defer close(s.stoppedChan)

	metrics.IncrementBackgroundServiceIterations("metrics_reporter", "started")

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	// Report immediately on startup
	s.reportMetrics(ctx)

	for {
		select {
		case <-ctx.Done():
			metrics.IncrementBackgroundServiceIterations("metrics_reporter", "stopped")
			return
		case <-s.stopChan:
			metrics.IncrementBackgroundServiceIterations("metrics_reporter", "stopped")
			return
		case <-ticker.C:
			s.reportMetrics(ctx)
		}
	}
}

// reportMetrics queries all queues and updates Prometheus gauge metrics
func (s *MetricsReporterService) reportMetrics(ctx context.Context) {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		metrics.ObserveBackgroundServiceIterationDuration("metrics_reporter", duration.Seconds())
	}()

	// Query all queue names from the database
	query := "SELECT name FROM cq_queues ORDER BY name"
	rows, err := s.baseSQL.DB.QueryContext(ctx, query)
	if err != nil {
		metrics.IncrementBackgroundServiceIterations("metrics_reporter", "error")
		return
	}
	defer func() {
		_ = rows.Close() // Best effort close
	}()

	var queueNames []string
	for rows.Next() {
		var queueName string
		if err := rows.Scan(&queueName); err != nil {
			continue
		}
		queueNames = append(queueNames, queueName)
	}

	// Update queues total metric
	metrics.SetQueuesTotal(float64(len(queueNames)))

	// For each queue, get state counts and update metrics
	for _, queueName := range queueNames {
		s.updateQueueStateMetrics(ctx, queueName)
	}

	s.mu.Lock()
	s.lastReportedAt = time.Now()
	s.mu.Unlock()

	metrics.IncrementBackgroundServiceIterations("metrics_reporter", "success")
}

// updateQueueStateMetrics queries and updates messagesByState metrics for a specific queue
func (s *MetricsReporterService) updateQueueStateMetrics(ctx context.Context, queueName string) {
	// Query message counts by state for this queue
	// Use ? for SQLite, $1 for Postgres
	placeholder := "$1"
	if !s.baseSQL.Dialect.SupportsReturning() {
		// SQLite doesn't support RETURNING, use ? placeholder
		placeholder = "?"
	}

	query := `
		SELECT state, COUNT(*) as count
		FROM cq_messages
		WHERE queue_name = ` + placeholder + `
		GROUP BY state
	`
	rows, err := s.baseSQL.DB.QueryContext(ctx, query, queueName)
	if err != nil {
		return
	}
	defer func() {
		_ = rows.Close() // Best effort close
	}()

	// Initialize all states to 0 first (to clear old data for queues with no messages)
	states := []string{"INVISIBLE", "PENDING", "RUNNING", "COMPLETED", "CANCELED", "ERRORED"}
	for _, state := range states {
		metrics.SetMessagesByState(queueName, state, 0)
	}

	// Update with actual counts
	for rows.Next() {
		var stateInt int32
		var count int64
		if err := rows.Scan(&stateInt, &count); err != nil {
			continue
		}

		var stateName string
		switch messagepb.Message_Metadata_State(stateInt) {
		case messagepb.Message_Metadata_INVISIBLE:
			stateName = "INVISIBLE"
		case messagepb.Message_Metadata_PENDING:
			stateName = "PENDING"
		case messagepb.Message_Metadata_RUNNING:
			stateName = "RUNNING"
		case messagepb.Message_Metadata_COMPLETED:
			stateName = "COMPLETED"
		case messagepb.Message_Metadata_CANCELED:
			stateName = "CANCELED"
		case messagepb.Message_Metadata_ERRORED:
			stateName = "ERRORED"
		default:
			continue
		}

		metrics.SetMessagesByState(queueName, stateName, float64(count))
	}
}

// StopGracefully stops the metrics reporter and waits for completion
func (s *MetricsReporterService) StopGracefully(ctx context.Context) error {
	close(s.stopChan)

	// Wait for service to stop or timeout
	select {
	case <-s.stoppedChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// LastReportedAt returns when metrics were last reported
func (s *MetricsReporterService) LastReportedAt() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastReportedAt
}
