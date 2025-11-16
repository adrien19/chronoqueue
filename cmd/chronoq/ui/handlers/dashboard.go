package handlers

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"sync"
	"time"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/pkg/log"
)

// DashboardHandler handles dashboard-related requests
type DashboardHandler struct {
	BaseHandler
}

// ProblemQueue represents a queue that needs attention
type ProblemQueue struct {
	Name     string
	Message  string
	Severity string // "high", "warning"
	Link     string
}

// DashboardMetrics holds aggregated system metrics
type DashboardMetrics struct {
	TotalQueues    int
	TotalPending   int64
	TotalRunning   int64
	TotalCompleted int64
	TotalDLQ       int64
	ProblemQueues  []ProblemQueue
	Timestamp      time.Time
}

// NewDashboardHandler creates a new dashboard handler
func NewDashboardHandler(templates *template.Template, client *client.ChronoQueueClient, logger *log.Logger) *DashboardHandler {
	return &DashboardHandler{
		BaseHandler: BaseHandler{
			templates: templates,
			client:    client,
			logger:    logger,
		},
	}
}

// Index renders the main dashboard page
func (h *DashboardHandler) Index(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second) // Longer timeout for aggregation
	defer cancel()

	// Fetch and aggregate metrics
	metrics := h.aggregateMetrics(ctx)

	data := map[string]interface{}{
		"Title":          "Dashboard",
		"CurrentPage":    "dashboard",
		"TotalQueues":    metrics.TotalQueues,
		"TotalPending":   metrics.TotalPending,
		"TotalRunning":   metrics.TotalRunning,
		"TotalCompleted": metrics.TotalCompleted,
		"TotalDLQ":       metrics.TotalDLQ,
		"ProblemQueues":  metrics.ProblemQueues,
		"Timestamp":      metrics.Timestamp,
	}

	h.renderTemplate(w, "dashboard.html", data)
}

// aggregateMetrics fetches and aggregates metrics from all queues
func (h *DashboardHandler) aggregateMetrics(ctx context.Context) DashboardMetrics {
	metrics := DashboardMetrics{
		Timestamp:     time.Now(),
		ProblemQueues: []ProblemQueue{},
	}

	// Fetch all queues
	queuesResp, err := h.client.ListQueues(ctx, "")
	if err != nil {
		h.logger.ErrorWithFields("Failed to list queues", "error", err)
		return metrics
	}

	queues := queuesResp.GetQueues()
	metrics.TotalQueues = len(queues)

	// Use goroutines for parallel API calls
	type queueStateResult struct {
		queueName string
		state     map[string]int32
		deadline  *time.Time
		err       error
	}

	type dlqStatsResult struct {
		dlqName string
		count   int64
		err     error
	}

	stateChan := make(chan queueStateResult, len(queues))
	dlqChan := make(chan dlqStatsResult, len(queues))
	var wg sync.WaitGroup

	// Fetch state for each queue in parallel
	for _, queue := range queues {
		wg.Add(1)
		go func(qName string) {
			defer wg.Done()
			stateResp, err := h.client.GetQueueState(ctx, qName)
			result := queueStateResult{queueName: qName, err: err}
			if err == nil {
				result.state = stateResp.GetStateCounts()
				if stateResp.GetEarliestDeadline() != nil {
					deadline := stateResp.GetEarliestDeadline().AsTime()
					result.deadline = &deadline
				}
			}
			stateChan <- result
		}(queue.GetName())

		// If this is a DLQ, also fetch DLQ stats
		if isDLQ(queue.GetName()) {
			wg.Add(1)
			go func(dlqName string) {
				defer wg.Done()
				dlqStats, err := h.client.GetDLQStats(ctx, dlqName)
				result := dlqStatsResult{dlqName: dlqName, err: err}
				if err == nil {
					result.count = dlqStats.GetMessageCount()
				}
				dlqChan <- result
			}(queue.GetName())
		}
	}

	// Close channels when all goroutines complete
	go func() {
		wg.Wait()
		close(stateChan)
		close(dlqChan)
	}()

	// Aggregate state counts
	for result := range stateChan {
		if result.err != nil {
			h.logger.WarnWithFields("Failed to get queue state", "queue", result.queueName, "error", result.err)
			continue
		}

		pending := int64(result.state["PENDING"])
		running := int64(result.state["RUNNING"])

		metrics.TotalPending += pending
		metrics.TotalRunning += running
		metrics.TotalCompleted += int64(result.state["COMPLETED"])

		// Detect stale messages: Only flag if there are PENDING or RUNNING messages AND oldest message > 2h
		// This avoids false positives for idle queues with old completed messages
		if result.deadline != nil && (pending > 0 || running > 0) {
			age := time.Since(*result.deadline)
			if age > 2*time.Hour {
				metrics.ProblemQueues = append(metrics.ProblemQueues, ProblemQueue{
					Name:     result.queueName,
					Message:  fmt.Sprintf("Stuck messages: oldest %s ago", formatDuration(age)),
					Severity: "warning",
					Link:     "/queues/" + result.queueName,
				})
			}
		}
	}

	// Aggregate DLQ counts and detect high DLQ issues
	for result := range dlqChan {
		if result.err != nil {
			// Silently skip DLQ stats errors - DLQ stream may not exist yet if never had failures
			// This is expected behavior and not a real error
			continue
		}

		metrics.TotalDLQ += result.count

		// Detect high DLQ message count (> 5 messages)
		if result.count > 5 {
			metrics.ProblemQueues = append(metrics.ProblemQueues, ProblemQueue{
				Name:     result.dlqName,
				Message:  fmt.Sprintf("%d messages in DLQ", result.count),
				Severity: "high",
				Link:     "/queues/" + result.dlqName,
			})
		}
	}

	return metrics
}

// formatDuration converts a duration to a human-readable string
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
}

// Metrics returns real-time metrics for HTMX polling
func (h *DashboardHandler) Metrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second) // Longer timeout for aggregation
	defer cancel()

	// Fetch and aggregate metrics
	metrics := h.aggregateMetrics(ctx)

	data := map[string]interface{}{
		"TotalQueues":    metrics.TotalQueues,
		"TotalPending":   metrics.TotalPending,
		"TotalRunning":   metrics.TotalRunning,
		"TotalCompleted": metrics.TotalCompleted,
		"TotalDLQ":       metrics.TotalDLQ,
		"ProblemQueues":  metrics.ProblemQueues,
		"Timestamp":      metrics.Timestamp,
	}

	h.renderComponent(w, "dashboard-metrics", data)
}
