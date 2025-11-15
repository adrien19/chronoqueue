package handlers

import (
	"context"
	"html/template"
	"net/http"
	"time"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/pkg/log"
)

// DashboardHandler handles dashboard-related requests
type DashboardHandler struct {
	BaseHandler
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
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Fetch initial metrics
	queuesResp, err := h.client.ListQueues(ctx, "")
	if err != nil {
		h.logger.ErrorWithFields("Failed to list queues", "error", err)
		h.renderError(w, http.StatusInternalServerError, "Failed to load dashboard")
		return
	}

	data := map[string]interface{}{
		"Title":        "Dashboard",
		"CurrentPage":  "dashboard",
		"Queues":       queuesResp.GetQueues(),
		"Timestamp":    time.Now(),
		"TotalQueues":  len(queuesResp.GetQueues()),
		"TotalPending": 0,
		"TotalRunning": 0,
		"TotalDLQ":     0,
	}

	h.renderTemplate(w, "dashboard.html", data)
}

// Metrics returns real-time metrics for HTMX polling
func (h *DashboardHandler) Metrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	queuesResp, err := h.client.ListQueues(ctx, "")
	if err != nil {
		h.logger.ErrorWithFields("Failed to list queues for metrics", "error", err)
		http.Error(w, "Failed to load metrics", http.StatusInternalServerError)
		return
	}

	queues := queuesResp.GetQueues()

	// For now, return basic queue count
	// TODO: Implement GetQueueState API calls to fetch pending/running counts
	data := map[string]interface{}{
		"TotalQueues":  len(queues),
		"TotalPending": 0,
		"TotalRunning": 0,
		"TotalDLQ":     0,
		"Timestamp":    time.Now(),
	}

	h.renderComponent(w, "metrics", data)
}
