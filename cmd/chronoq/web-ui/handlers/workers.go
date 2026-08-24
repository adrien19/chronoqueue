package handlers

import (
	"context"
	"html/template"
	"net/http"
	"time"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	"github.com/adrien19/chronoqueue/client"
	clusterstore "github.com/adrien19/chronoqueue/cmd/chronoq/web-ui/cluster"
	"github.com/adrien19/chronoqueue/pkg/log"
)

// WorkersHandler handles the workers page (informational, no worker API available).
type WorkersHandler struct {
	BaseHandler
}

// NewWorkersHandler creates a WorkersHandler.
func NewWorkersHandler(
	templates *template.Template,
	store *clusterstore.Store,
	logger *log.Logger,
) *WorkersHandler {
	return &WorkersHandler{
		BaseHandler: BaseHandler{
			templates: templates,
			store:     store,
			logger:    logger,
		},
	}
}

// List renders the workers page with an informational empty state.
func (h *WorkersHandler) List(w http.ResponseWriter, r *http.Request) {
	data := map[string]any{
		"PageTitle": "Workers",
		"Active":    "workers",
	}
	h.render(w, "workers_content", data)
}

// LeaseMonitorHandler handles the lease-monitor page.
type LeaseMonitorHandler struct {
	BaseHandler
}

// NewLeaseMonitorHandler creates a LeaseMonitorHandler.
func NewLeaseMonitorHandler(
	templates *template.Template,
	store *clusterstore.Store,
	logger *log.Logger,
) *LeaseMonitorHandler {
	return &LeaseMonitorHandler{
		BaseHandler: BaseHandler{
			templates: templates,
			store:     store,
			logger:    logger,
		},
	}
}

// QueueInflight summarises inflight (RUNNING) message activity for one queue.
type QueueInflight struct {
	Queue        string
	RunningCount int32
	// Rows contains individual message detail when available via best-effort peek.
	Rows []LeaseRow
}

// List renders the lease monitor page. The table auto-refreshes via HTMX polling.
func (h *LeaseMonitorHandler) List(w http.ResponseWriter, r *http.Request) {
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	inflight, totalRunning, partialData, err := h.collectInflight(ctx, activeClient)
	if err != nil {
		h.writeRPCError(w, r, "load lease monitor", err)
		return
	}

	data := map[string]any{
		"PageTitle":    "Lease Monitor",
		"Active":       "lease-monitor",
		"Inflight":     inflight,
		"TotalRunning": totalRunning,
	}
	if partialData {
		data["PartialDataWarning"] = "Some queue state or message details could not be loaded. Lease data is incomplete."
	}
	h.render(w, "lease_monitor_content", data)
}

// Table renders the polling fragment used by the lease monitor page to auto-refresh.
func (h *LeaseMonitorHandler) Table(w http.ResponseWriter, r *http.Request) {
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	inflight, totalRunning, partialData, err := h.collectInflight(ctx, activeClient)
	if err != nil {
		h.writeRPCError(w, r, "load lease monitor", err)
		return
	}

	data := map[string]any{
		"Inflight":     inflight,
		"TotalRunning": totalRunning,
		"PartialData":  partialData,
	}
	h.renderFragment(w, "lease_monitor_table", data)
}

// collectInflight fetches RUNNING message counts (and best-effort details) for all queues.
func (h *LeaseMonitorHandler) collectInflight(ctx context.Context, activeClient *client.ChronoQueueClient) ([]QueueInflight, int32, bool, error) {
	queuesResp, err := activeClient.ListQueues(ctx, "")
	if err != nil {
		return nil, 0, false, err
	}

	var inflight []QueueInflight
	var totalRunning int32
	partialData := false

	for _, q := range queuesResp.GetQueues() {
		name := q.GetName()

		stateResp, err := activeClient.GetQueueState(ctx, name)
		if err != nil {
			h.logger.WarnWithFields("Failed to get queue state for lease monitor", "error", err, "queue", name)
			partialData = true
			continue
		}

		runningCount := stateResp.GetStateCounts()["RUNNING"]
		if runningCount == 0 {
			continue
		}
		totalRunning += runningCount

		qi := QueueInflight{
			Queue:        name,
			RunningCount: runningCount,
		}

		peekResp, err := activeClient.PeekQueueMessages(ctx, name, 200, client.TimeRangeOption{})
		if err != nil {
			h.logger.WarnWithFields("Failed to peek queue messages for lease monitor", "error", err, "queue", name)
			partialData = true
		} else {
			for _, msg := range peekResp.GetMessages() {
				if msg == nil {
					continue
				}
				meta := msg.GetMetadata()
				if meta == nil || meta.GetState() != message_pb.Message_Metadata_RUNNING {
					continue
				}
				qi.Rows = append(qi.Rows, LeaseRow{
					MessageID: shortenID(msg.GetMessageId()),
					Queue:     name,
					Status:    "RUNNING",
					Renewals:  "—",
					Duration:  "—",
					ExpiresIn: "—",
				})
			}
		}

		inflight = append(inflight, qi)
	}
	return inflight, totalRunning, partialData, nil
}
