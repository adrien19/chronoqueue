package handlers

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/pkg/log"
)

// QueuesHandler handles queue-related requests
type QueuesHandler struct {
	BaseHandler
}

// NewQueuesHandler creates a new queues handler
func NewQueuesHandler(templates *template.Template, client *client.ChronoQueueClient, logger *log.Logger) *QueuesHandler {
	return &QueuesHandler{
		BaseHandler: BaseHandler{
			templates: templates,
			client:    client,
			logger:    logger,
		},
	}
}

// List renders the queue listing page
func (h *QueuesHandler) List(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	queuesResp, err := h.client.ListQueues(ctx, "")
	if err != nil {
		h.logger.ErrorWithFields("Failed to list queues", "error", err)
		h.renderError(w, http.StatusInternalServerError, "Failed to load queues")
		return
	}

	data := map[string]interface{}{
		"Title":       "Queues",
		"CurrentPage": "queues",
		"Queues":      queuesResp.GetQueues(),
	}

	h.renderTemplate(w, "queues.html", data)
}

// Detail renders the queue detail page with messages
func (h *QueuesHandler) Detail(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")

	// TODO: Implement GetQueueState API to fetch actual message counts
	// For now, using placeholder values
	data := map[string]interface{}{
		"Title":          "Queue: " + queueName,
		"CurrentPage":    "queues",
		"QueueName":      queueName,
		"PendingCount":   0,
		"RunningCount":   0,
		"CompletedCount": 0,
		"FailedCount":    0,
		"Messages":       []interface{}{}, // Empty for now
	}

	h.renderTemplate(w, "queue_detail.html", data)
}

// Stats returns queue statistics for HTMX updates
func (h *QueuesHandler) Stats(w http.ResponseWriter, r *http.Request) {
	_ = r.PathValue("name") // queueName will be used when implementing GetQueueState API

	// TODO: Implement GetQueueState API to fetch actual message counts
	// For now, using placeholder values
	data := map[string]interface{}{
		"PendingCount":   0,
		"RunningCount":   0,
		"CompletedCount": 0,
		"FailedCount":    0,
	}

	// Return just the stats cards HTML fragment (not a full page)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	// Render the stats cards directly
	html := `
        <div class="bg-white rounded-lg shadow p-6">
            <div class="text-sm font-medium text-gray-600">Pending</div>
            <div class="mt-2 text-3xl font-semibold text-blue-600">` + fmt.Sprintf("%d", data["PendingCount"]) + `</div>
        </div>
        <div class="bg-white rounded-lg shadow p-6">
            <div class="text-sm font-medium text-gray-600">Running</div>
            <div class="mt-2 text-3xl font-semibold text-yellow-600">` + fmt.Sprintf("%d", data["RunningCount"]) + `</div>
        </div>
        <div class="bg-white rounded-lg shadow p-6">
            <div class="text-sm font-medium text-gray-600">Completed</div>
            <div class="mt-2 text-3xl font-semibold text-green-600">` + fmt.Sprintf("%d", data["CompletedCount"]) + `</div>
        </div>
        <div class="bg-white rounded-lg shadow p-6">
            <div class="text-sm font-medium text-gray-600">Failed</div>
            <div class="mt-2 text-3xl font-semibold text-red-600">` + fmt.Sprintf("%d", data["FailedCount"]) + `</div>
        </div>
	`

	if _, err := w.Write([]byte(html)); err != nil {
		h.logger.ErrorWithFields("Failed to write stats response", "error", err)
	}
}

// PeekMessages returns messages for a queue (for message browser)
func (h *QueuesHandler) PeekMessages(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement peek messages API call
	w.WriteHeader(http.StatusOK)
}
