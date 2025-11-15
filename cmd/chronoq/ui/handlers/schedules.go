package handlers

import (
	"html/template"
	"net/http"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/pkg/log"
)

// SchedulesHandler handles schedule-related requests
type SchedulesHandler struct {
	BaseHandler
}

// NewSchedulesHandler creates a new schedules handler
func NewSchedulesHandler(templates *template.Template, client *client.ChronoQueueClient, logger *log.Logger) *SchedulesHandler {
	return &SchedulesHandler{
		BaseHandler: BaseHandler{
			templates: templates,
			client:    client,
			logger:    logger,
		},
	}
}

// List renders the schedule listing page
func (h *SchedulesHandler) List(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement ListSchedules API call
	// For now, showing empty state
	data := map[string]interface{}{
		"Title":       "Schedules",
		"CurrentPage": "schedules",
		"Schedules":   []interface{}{}, // Empty for now
	}

	h.renderTemplate(w, "schedules.html", data)
}

// New renders the create schedule form
func (h *SchedulesHandler) New(w http.ResponseWriter, r *http.Request) {
	data := map[string]interface{}{
		"Title":       "Create Schedule",
		"CurrentPage": "schedules",
	}

	h.renderTemplate(w, "schedule_new.html", data)
}

// Detail renders the schedule detail/edit page
func (h *SchedulesHandler) Detail(w http.ResponseWriter, r *http.Request) {
	scheduleID := r.PathValue("id")

	data := map[string]interface{}{
		"Title":       "Schedule: " + scheduleID,
		"CurrentPage": "schedules",
		"ScheduleID":  scheduleID,
	}

	h.renderTemplate(w, "schedule_detail.html", data)
}

// Create handles schedule creation (HTMX POST)
func (h *SchedulesHandler) Create(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement schedule creation
	w.WriteHeader(http.StatusOK)
}

// Update handles schedule updates (HTMX POST)
func (h *SchedulesHandler) Update(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement schedule update
	w.WriteHeader(http.StatusOK)
}

// Toggle handles pause/resume (HTMX POST)
func (h *SchedulesHandler) Toggle(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement schedule pause/resume
	w.WriteHeader(http.StatusOK)
}

// Delete handles schedule deletion (HTMX DELETE)
func (h *SchedulesHandler) Delete(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement schedule deletion
	w.WriteHeader(http.StatusOK)
}
