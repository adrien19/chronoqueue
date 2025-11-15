package handlers

import (
	"fmt"
	"html/template"
	"net/http"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/pkg/log"
)

// BaseHandler provides common functionality for all handlers
type BaseHandler struct {
	templates *template.Template
	client    *client.ChronoQueueClient
	logger    *log.Logger
}

// renderTemplate renders a page template with layout
func (h *BaseHandler) renderTemplate(w http.ResponseWriter, name string, data interface{}) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Map template names to their content block names
	contentMap := map[string]string{
		"dashboard.html":       "dashboard-page-content",
		"queues.html":          "queues-page-content",
		"queue_detail.html":    "queue-detail-page-content",
		"schedules.html":       "schedules-page-content",
		"schedule_new.html":    "schedule-new-page-content",
		"schedule_detail.html": "schedule-detail-page-content",
		"dlq.html":             "dlq-page-content",
		"dlq_detail.html":      "dlq-detail-page-content",
	}

	contentName, ok := contentMap[name]
	if !ok {
		h.logger.ErrorWithFields("Unknown template", "template", name)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Clone the layout template and add the specific content
	pageTemplate, err := h.templates.Clone()
	if err != nil {
		h.logger.ErrorWithFields("Failed to clone template", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Define page-content to call the specific content template
	_, err = pageTemplate.New("page-content").Parse(fmt.Sprintf(`{{template "%s" .}}`, contentName))
	if err != nil {
		h.logger.ErrorWithFields("Failed to create page content wrapper", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Execute layout.html with the page-content wrapper
	if err := pageTemplate.ExecuteTemplate(w, "layout.html", data); err != nil {
		h.logger.ErrorWithFields("Failed to render template", "template", name, "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

// renderComponent renders a component template without layout (for HTMX)
func (h *BaseHandler) renderComponent(w http.ResponseWriter, name string, data interface{}) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	if err := h.templates.ExecuteTemplate(w, name, data); err != nil {
		h.logger.ErrorWithFields("Failed to render component", "component", name, "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

// renderError renders an error page
func (h *BaseHandler) renderError(w http.ResponseWriter, statusCode int, message string) {
	w.WriteHeader(statusCode)
	data := map[string]interface{}{
		"Error":   message,
		"Code":    statusCode,
		"Message": http.StatusText(statusCode),
	}
	if err := h.templates.ExecuteTemplate(w, "error", data); err != nil {
		h.logger.Error("Failed to render error template", "error", err)
	}
}
