package monitoring

import (
	"context"
	"fmt"
	"time"

	"github.com/adrien19/chronoqueue/client"
)

// QueueStats represents statistics for a single queue
type QueueStats struct {
	Name              string
	PendingMessages   int
	ProcessedMessages int
	FailedMessages    int
	DLQMessages       int
	AvgProcessingTime time.Duration
	LastActivity      time.Time
}

// SystemStats represents overall system statistics
type AgentHealth struct {
	Name            string
	Status          string // HEALTHY, DEGRADED, UNHEALTHY
	QueueDepth      int64
	DLQDepth        int64
	Throughput      float64 // messages/second
	ErrorRate       float64 // percentage
	LastActivity    time.Time
	ProcessingCount int64
}

type SystemStats struct {
	Queues         []QueueStats
	AgentHealth    []AgentHealth
	TotalTasks     int64
	CompletedTasks int64
	FailedTasks    int64
	ActiveAgents   int
	SystemUptime   time.Duration
	LastRefresh    time.Time
}

// Monitor provides monitoring capabilities for the orchestrator
type Monitor struct {
	client       *client.ChronoQueueClient
	redisMonitor *RedisMonitor
	queueNames   []string
	startTime    time.Time
	refreshRate  time.Duration
}

// NewMonitor creates a new monitoring instance
func NewMonitor(c *client.ChronoQueueClient, refreshRate time.Duration) *Monitor {
	// Initialize monitor using ChronoQueue API
	redisMonitor := NewRedisMonitor(c)

	return &Monitor{
		client:       c,
		redisMonitor: redisMonitor,
		queueNames: []string{
			"agent-coordinator",
			"agent-web-search",
			"agent-code-analyzer",
			"agent-data-processor",
			"agent-aggregator",
			"agent-notification",
			"agent-results",
		},
		startTime:   time.Now(),
		refreshRate: refreshRate,
	}
}

// GetSystemStats retrieves current system statistics
func (m *Monitor) GetSystemStats(ctx context.Context) (*SystemStats, error) {
	stats := &SystemStats{
		Queues:       make([]QueueStats, 0, len(m.queueNames)),
		AgentHealth:  make([]AgentHealth, 0, len(m.queueNames)),
		SystemUptime: time.Since(m.startTime),
		LastRefresh:  time.Now(),
		ActiveAgents: len(m.queueNames),
	}

	for _, queueName := range m.queueNames {
		queueStats, err := m.getQueueStats(ctx, queueName)
		if err != nil {
			// Continue on error, just mark queue as unavailable
			queueStats = &QueueStats{
				Name:         queueName,
				LastActivity: time.Time{},
			}
		}

		stats.Queues = append(stats.Queues, *queueStats)
		stats.TotalTasks += int64(queueStats.PendingMessages + queueStats.ProcessedMessages)
		stats.CompletedTasks += int64(queueStats.ProcessedMessages)
		stats.FailedTasks += int64(queueStats.FailedMessages)

		// Calculate agent health
		health := m.calculateAgentHealth(queueStats)
		stats.AgentHealth = append(stats.AgentHealth, health)
	}

	return stats, nil
}

// calculateAgentHealth determines agent health status based on queue metrics
func (m *Monitor) calculateAgentHealth(queue *QueueStats) AgentHealth {
	health := AgentHealth{
		Name:            queue.Name,
		QueueDepth:      int64(queue.PendingMessages),
		DLQDepth:        int64(queue.DLQMessages),
		LastActivity:    queue.LastActivity,
		ProcessingCount: int64(queue.ProcessedMessages),
	}

	// Calculate error rate
	totalProcessed := queue.ProcessedMessages + queue.FailedMessages
	if totalProcessed > 0 {
		health.ErrorRate = float64(queue.FailedMessages) / float64(totalProcessed) * 100
	}

	// Determine status based on metrics
	if queue.DLQMessages > 10 || health.ErrorRate > 50 {
		health.Status = "UNHEALTHY"
	} else if queue.DLQMessages > 5 || health.ErrorRate > 20 || queue.PendingMessages > 100 {
		health.Status = "DEGRADED"
	} else {
		health.Status = "HEALTHY"
	}

	return health
}

// getQueueStats retrieves statistics for a single queue using ChronoQueue API
func (m *Monitor) getQueueStats(ctx context.Context, queueName string) (*QueueStats, error) {
	stats := &QueueStats{
		Name:              queueName,
		AvgProcessingTime: 500 * time.Millisecond,
		LastActivity:      time.Now(),
	}

	// Get queue state from ChronoQueue API
	queueState, err := m.redisMonitor.GetQueueState(ctx, queueName)
	if err != nil {
		// Queue may not exist yet, return zero stats
		return stats, nil
	}

	// Pending: messages waiting to be consumed
	stats.PendingMessages = int(queueState.Pending)

	// Processed: completed + canceled messages
	stats.ProcessedMessages = int(queueState.Completed + queueState.Canceled)

	// Failed: errored messages
	stats.FailedMessages = int(queueState.Errored)
	stats.DLQMessages = int(queueState.Errored)

	return stats, nil
}

// Close closes the monitor and its resources
func (m *Monitor) Close() error {
	if m.redisMonitor != nil {
		return m.redisMonitor.Close()
	}
	return nil
}

// DisplaySystemStats prints formatted system statistics
func (m *Monitor) DisplaySystemStats(stats *SystemStats) {
	fmt.Println("\n╔══════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║              AI AGENT ORCHESTRATOR - SYSTEM MONITOR                          ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// System overview
	fmt.Println("📊 SYSTEM OVERVIEW")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────")
	fmt.Printf("  Uptime:          %s\n", formatDuration(stats.SystemUptime))
	fmt.Printf("  Total Tasks:     %d\n", stats.TotalTasks)
	fmt.Printf("  Completed:       %d\n", stats.CompletedTasks)
	fmt.Printf("  Failed:          %d\n", stats.FailedTasks)
	fmt.Printf("  Active Agents:   %d\n", len(stats.Queues))
	fmt.Printf("  Last Refresh:    %s\n", stats.LastRefresh.Format("15:04:05"))
	fmt.Println()

	// Queue details
	fmt.Println("🎯 QUEUE STATUS")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────")
	fmt.Printf("  %-25s  %8s  %10s  %8s  %8s\n", "QUEUE", "PENDING", "COMPLETED", "ERRORED", "DLQ")
	fmt.Println("  ─────────────────────────  ────────  ──────────  ────────  ────────")

	for _, queue := range stats.Queues {
		status := "●"
		if queue.PendingMessages > 0 {
			status = "⚡"
		}

		fmt.Printf("  %s %-23s  %8d  %10d  %8d  %8d\n",
			status,
			queue.Name,
			queue.PendingMessages,
			queue.ProcessedMessages,
			queue.FailedMessages,
			queue.DLQMessages,
		)
	}

	fmt.Println()

	// Agent health
	fmt.Println("💚 AGENT HEALTH")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────")
	fmt.Printf("  %-25s  %10s  %8s  %10s  %12s\n", "AGENT", "STATUS", "DLQ", "ERROR RATE", "THROUGHPUT")
	fmt.Println("  ─────────────────────────  ──────────  ────────  ──────────  ────────────")

	for _, agent := range stats.AgentHealth {
		var statusIcon string
		switch agent.Status {
		case "DEGRADED":
			statusIcon = "⚠️ "
		case "UNHEALTHY":
			statusIcon = "❌"
		default:
			statusIcon = "✅"
		}

		fmt.Printf("  %-25s  %s %-8s  %8d  %9.1f%%  %11.2f/s\n",
			agent.Name,
			statusIcon,
			agent.Status,
			agent.DLQDepth,
			agent.ErrorRate,
			agent.Throughput,
		)
	}

	fmt.Println()
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────")
	fmt.Println("  Legend: ● Idle  ⚡ Active  |  ✅ Healthy  ⚠️  Degraded  ❌ Unhealthy")
	fmt.Println()
}

// DisplayCompactStats prints a compact single-line status
func (m *Monitor) DisplayCompactStats(stats *SystemStats) {
	pendingTotal := 0
	for _, queue := range stats.Queues {
		pendingTotal += queue.PendingMessages
	}

	fmt.Printf("[%s] Tasks: %d total, %d completed, %d failed | Pending: %d | Uptime: %s\n",
		time.Now().Format("15:04:05"),
		stats.TotalTasks,
		stats.CompletedTasks,
		stats.FailedTasks,
		pendingTotal,
		formatDuration(stats.SystemUptime),
	)
}

// WatchSystem continuously monitors and displays system stats
func (m *Monitor) WatchSystem(ctx context.Context, compact bool) error {
	ticker := time.NewTicker(m.refreshRate)
	defer ticker.Stop()

	// Display initial stats
	stats, err := m.GetSystemStats(ctx)
	if err != nil {
		return err
	}

	if compact {
		m.DisplayCompactStats(stats)
	} else {
		m.DisplaySystemStats(stats)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			stats, err := m.GetSystemStats(ctx)
			if err != nil {
				fmt.Printf("Error fetching stats: %v\n", err)
				continue
			}

			if compact {
				m.DisplayCompactStats(stats)
			} else {
				// Clear screen and redisplay
				fmt.Print("\033[H\033[2J")
				m.DisplaySystemStats(stats)
			}
		}
	}
}

// formatDuration formats a duration into human-readable string
func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds", h, m, s)
	} else if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}
