package handlers

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"sync"
	"time"

	clusterstore "github.com/adrien19/chronoqueue/cmd/chronoq/web-ui/cluster"
	"github.com/adrien19/chronoqueue/pkg/log"

	pb_queue "github.com/adrien19/chronoqueue/api/queue/v1"
)

// DashboardHandler handles the home / dashboard page.
type DashboardHandler struct {
	BaseHandler
}

// NewDashboardHandler creates a DashboardHandler.
func NewDashboardHandler(
	templates *template.Template,
	store *clusterstore.Store,
	logger *log.Logger,
) *DashboardHandler {
	return &DashboardHandler{
		BaseHandler: BaseHandler{
			templates: templates,
			store:     store,
			logger:    logger,
		},
	}
}

// Index renders the home dashboard.
func (h *DashboardHandler) Index(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	queuesResp, err := h.activeClient().ListQueues(ctx, "")
	if err != nil {
		h.logger.WarnWithFields("Backend unavailable, rendering empty dashboard", "error", err)
	}

	var queues []*pb_queue.Queue
	if queuesResp != nil {
		queues = queuesResp.GetQueues()
	}
	rows, totalPending, totalRunning, totalCompleted, totalDLQ := h.buildQueueRows(ctx, queues)

	brokerStatus := "Healthy"
	if err != nil {
		brokerStatus = "Unreachable"
	}

	data := map[string]any{
		"PageTitle":      "Home",
		"Active":         "home",
		"Rows":           rows,
		"BrokerStatus":   brokerStatus,
		"TotalReady":     fmt.Sprintf("%d", totalPending),
		"TotalRunning":   fmt.Sprintf("%d", totalRunning),
		"TotalCompleted": fmt.Sprintf("%d", totalCompleted),
		"TotalPending":   fmt.Sprintf("%d", totalPending),
		"TotalDLQ":       fmt.Sprintf("%d", totalDLQ),
		"RatePoints":     []int{},
	}

	h.render(w, "home_content", data)
}

// DashboardStats renders the broker state + totals fragment, polled by HTMX every 5 s.
// All sections share a single buildQueueRows call.
func (h *DashboardHandler) DashboardStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	queuesResp, err := h.activeClient().ListQueues(ctx, "")
	if err != nil {
		h.logger.WarnWithFields("Backend unavailable for dashboard stats fragment", "error", err)
	}

	var queues []*pb_queue.Queue
	if queuesResp != nil {
		queues = queuesResp.GetQueues()
	}
	_, totalPending, totalRunning, totalCompleted, totalDLQ := h.buildQueueRows(ctx, queues)

	brokerStatus := "Healthy"
	if err != nil {
		brokerStatus = "Unreachable"
	}

	data := map[string]any{
		"BrokerStatus":   brokerStatus,
		"TotalReady":     fmt.Sprintf("%d", totalPending),
		"TotalRunning":   fmt.Sprintf("%d", totalRunning),
		"TotalCompleted": fmt.Sprintf("%d", totalCompleted),
		"TotalPending":   fmt.Sprintf("%d", totalPending),
		"TotalDLQ":       fmt.Sprintf("%d", totalDLQ),
		"RatePoints":     []int{},
	}
	h.injectBaseData(data)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.templates.ExecuteTemplate(w, "dashboard_stats", data); err != nil {
		h.logger.ErrorWithFields("Failed to render dashboard_stats fragment", "error", err)
	}
}

// LiveOverview streams a Server-Sent Events fragment for the home page live surfaces panel.
func (h *DashboardHandler) LiveOverview(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	queuesResp, err := h.activeClient().ListQueues(ctx, "")
	if err != nil {
		h.logger.WarnWithFields("Backend unavailable for live overview", "error", err)
	}

	type queueSummary struct {
		Name  string
		Ready string
	}

	var summaries []queueSummary
	var liveQueues []*pb_queue.Queue
	if queuesResp != nil {
		liveQueues = queuesResp.GetQueues()
	}
	for _, q := range liveQueues {
		stateResp, err := h.activeClient().GetQueueState(ctx, q.GetName())
		if err != nil {
			continue
		}
		counts := stateResp.GetStateCounts()
		summaries = append(summaries, queueSummary{
			Name:  q.GetName(),
			Ready: fmt.Sprintf("%d", counts["PENDING"]),
		})
		if len(summaries) >= 5 {
			break
		}
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	fragmentData := map[string]any{
		"QueueSummary":    summaries,
		"InflightSummary": []any{},
	}

	var buf []byte
	buf = append(buf, "event: overview\ndata: "...)
	// Write the rendered fragment as the SSE data
	rw := &bufWriter{}
	if err := h.templates.ExecuteTemplate(rw, "live_overview", fragmentData); err != nil {
		h.logger.ErrorWithFields("Failed to render live_overview fragment", "error", err)
		return
	}
	// SSE data lines must not contain raw newlines - collapse them
	for _, b := range rw.b {
		if b == '\n' {
			buf = append(buf, ' ')
		} else {
			buf = append(buf, b)
		}
	}
	buf = append(buf, "\n\n"...)

	if _, err := w.Write(buf); err != nil {
		h.logger.ErrorWithFields("Failed to write SSE data", "error", err)
		return
	}
	flusher.Flush()
}

// bufWriter is a minimal io.Writer that accumulates bytes.
type bufWriter struct {
	b  []byte
	mu sync.Mutex
}

func (bw *bufWriter) Write(p []byte) (int, error) {
	bw.mu.Lock()
	bw.b = append(bw.b, p...)
	bw.mu.Unlock()
	return len(p), nil
}

// buildQueueRows fetches state for each queue concurrently and returns QueueRow view models.
func (h *DashboardHandler) buildQueueRows(ctx context.Context, queues []*pb_queue.Queue) (
	rows []QueueRow,
	totalPending, totalRunning, totalCompleted, totalDLQ int64,
) {
	type result struct {
		row        QueueRow
		pending    int64
		running    int64
		completed  int64
		dlqPending int64
	}

	results := make([]result, len(queues))
	var wg sync.WaitGroup

	for i, q := range queues {
		wg.Add(1)
		go func(idx int, queue *pb_queue.Queue) {
			defer wg.Done()
			name := queue.GetName()

			stateResp, err := h.activeClient().GetQueueState(ctx, name)
			if err != nil {
				h.logger.ErrorWithFields("Failed to get queue state", "error", err, "queue", name)
				results[idx].row = QueueRow{Name: name, Href: "/queues/" + name, IsDLQ: isDLQ(name)}
				return
			}

			counts := stateResp.GetStateCounts()
			pending := int64(counts["PENDING"])
			running := int64(counts["RUNNING"])
			completed := int64(counts["COMPLETED"])
			errored := int64(counts["ERRORED"])

			var dlqCount int64
			dlqName := name + "-dlq"
			dlqResp, dlqErr := h.activeClient().GetDLQStats(ctx, dlqName)
			if dlqErr == nil && dlqResp != nil {
				dlqCount = int64(dlqResp.GetMessageCount())
			}

			results[idx] = result{
				row: QueueRow{
					Name:       name,
					Ready:      fmt.Sprintf("%d", pending),
					InFlight:   fmt.Sprintf("%d", running),
					Delayed:    "0",
					Retries:    fmt.Sprintf("%d", errored),
					RetriesInt: int(errored),
					DLQ:        fmt.Sprintf("%d", dlqCount),
					DLQInt:     int(dlqCount),
					Href:       "/queues/" + name,
					IsDLQ:      isDLQ(name),
				},
				pending:    pending,
				running:    running,
				completed:  completed,
				dlqPending: dlqCount,
			}
		}(i, q)
	}

	wg.Wait()

	for _, res := range results {
		rows = append(rows, res.row)
		totalPending += res.pending
		totalRunning += res.running
		totalCompleted += res.completed
		totalDLQ += res.dlqPending
	}

	return rows, totalPending, totalRunning, totalCompleted, totalDLQ
}

// formatDuration formats a duration in a human-readable short form.
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
}
