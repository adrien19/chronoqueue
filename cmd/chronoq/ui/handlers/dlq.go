package handlers

import (
	"html/template"
	"net/http"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/pkg/log"
)

// DLQHandler handles dead letter queue requests
type DLQHandler struct {
	BaseHandler
}

// NewDLQHandler creates a new DLQ handler
func NewDLQHandler(templates *template.Template, client *client.ChronoQueueClient, logger *log.Logger) *DLQHandler {
	return &DLQHandler{
		BaseHandler: BaseHandler{
			templates: templates,
			client:    client,
			logger:    logger,
		},
	}
}

// List renders the DLQ overview page
func (h *DLQHandler) List(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement ListDLQs API call
	// For now, showing empty state
	data := map[string]interface{}{
		"Title":       "Dead Letter Queues",
		"CurrentPage": "dlq",
		"DLQs":        []interface{}{}, // Empty for now
	}

	h.renderTemplate(w, "dlq.html", data)
}

// Detail renders a specific DLQ's messages
func (h *DLQHandler) Detail(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queue")

	data := map[string]interface{}{
		"Title":       "DLQ: " + queueName,
		"CurrentPage": "dlq",
		"QueueName":   queueName,
	}

	h.renderTemplate(w, "dlq_detail.html", data)
}

// Messages returns DLQ messages (HTMX endpoint)
func (h *DLQHandler) Messages(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement DLQ messages API call
	w.WriteHeader(http.StatusOK)
}

// Requeue requeues a message from DLQ (HTMX POST)
func (h *DLQHandler) Requeue(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement requeue from DLQ
	w.WriteHeader(http.StatusOK)
}

// Purge purges all messages from DLQ (HTMX POST with confirmation)
func (h *DLQHandler) Purge(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement DLQ purge
	w.WriteHeader(http.StatusOK)
}
