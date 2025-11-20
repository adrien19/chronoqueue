package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	schedule_pb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/pkg/calendar"
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
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	schedulesResp, err := h.client.ListSchedules(ctx, "")
	if err != nil {
		h.logger.ErrorWithFields("Failed to list schedules", "error", err)
		h.renderError(w, http.StatusInternalServerError, "Failed to load schedules")
		return
	}

	// Get list of existing queues to check if schedule queues exist
	queuesResp, err := h.client.ListQueues(ctx, "")
	if err != nil {
		h.logger.ErrorWithFields("Failed to list queues", "error", err)
		// Don't fail the whole page, just log the error
	}

	// Create a map of existing queue names for quick lookup
	existingQueues := make(map[string]bool)
	if queuesResp != nil {
		for _, q := range queuesResp.GetQueues() {
			existingQueues[q.Name] = true
		}
	}

	// Check for missing queues
	hasMissingQueues := false
	for _, schedule := range schedulesResp.GetSchedules() {
		queueName := schedule.Metadata.GetQueueName()
		if !existingQueues[queueName] {
			hasMissingQueues = true
			break
		}
	}

	data := map[string]interface{}{
		"Title":            "Schedules",
		"CurrentPage":      "schedules",
		"Schedules":        schedulesResp.GetSchedules(),
		"ExistingQueues":   existingQueues,
		"HasMissingQueues": hasMissingQueues,
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
	if err := r.ParseForm(); err != nil {
		h.logger.ErrorWithFields("Failed to parse form", "error", err)
		http.Error(w, "Invalid form data", http.StatusBadRequest)
		return
	}

	scheduleID := r.FormValue("schedule_id")
	scheduleType := r.FormValue("schedule_type")
	queueName := r.FormValue("queue_name")
	payloadData := r.FormValue("payload_data")

	if scheduleID == "" || queueName == "" {
		http.Error(w, "Schedule ID and Queue Name are required", http.StatusBadRequest)
		return
	}

	// Validate that queue name is not the same as schedule ID
	if queueName == scheduleID {
		http.Error(w, "Queue Name cannot be the same as Schedule ID", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// Validate and ensure queue exists
	if err := h.ensureQueueExists(ctx, w, r, queueName, scheduleID); err != nil {
		return // Response already written by ensureQueueExists
	}

	// Build schedule options
	scheduleOpts, err := h.buildScheduleOptions(ctx, r, scheduleType, queueName, payloadData)
	if err != nil {
		h.logger.ErrorWithFields("Failed to build schedule options", "error", err, "schedule_id", scheduleID)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create the schedule
	if _, err := h.client.CreateSchedule(ctx, scheduleID, *scheduleOpts); err != nil {
		h.logger.ErrorWithFields("Failed to create schedule", "error", err, "schedule_id", scheduleID)
		http.Error(w, fmt.Sprintf("Failed to create schedule: %v", err), http.StatusInternalServerError)
		return
	}

	// Return success response
	w.Header().Set("HX-Redirect", "/schedules")
	w.WriteHeader(http.StatusCreated)
	if _, err := w.Write([]byte("Schedule created successfully")); err != nil {
		h.logger.Error("Failed to write response", "error", err)
	}
}

// ensureQueueExists checks if queue exists and handles auto-creation or returns warning dialog
func (h *SchedulesHandler) ensureQueueExists(ctx context.Context, w http.ResponseWriter, r *http.Request, queueName, scheduleID string) error {
	autoCreateQueue := r.FormValue("auto_create_queue") == "true"

	queuesResp, err := h.client.ListQueues(ctx, "")
	if err != nil {
		h.logger.ErrorWithFields("Failed to check if queue exists", "error", err)
		http.Error(w, "Failed to verify queue existence", http.StatusInternalServerError)
		return err
	}

	queueExists := false
	for _, q := range queuesResp.GetQueues() {
		if q.Name == queueName {
			queueExists = true
			break
		}
	}

	h.logger.InfoWithFields("Queue existence check", "queue", queueName, "exists", queueExists, "auto_create", autoCreateQueue)

	if !queueExists && !autoCreateQueue {
		h.renderQueueWarningDialog(w, queueName)
		return fmt.Errorf("queue does not exist")
	}

	if !queueExists && autoCreateQueue {
		return h.autoCreateQueue(ctx, queueName, scheduleID)
	}

	return nil
}

// autoCreateQueue creates a queue with sensible defaults
func (h *SchedulesHandler) autoCreateQueue(ctx context.Context, queueName, scheduleID string) error {
	h.logger.InfoWithFields("Auto-creating queue for schedule", "queue", queueName, "schedule", scheduleID)

	queueOpts := client.QueueOptions{
		Type:                0, // SIMPLE
		DequeueAttempts:     3,
		LeaseDuration:       "5m",
		AutoCreateDLQ:       true,
		DeadLetterQueueName: queueName + "-dlq",
	}

	if _, err := h.client.CreateQueue(ctx, queueName, queueOpts); err != nil {
		h.logger.ErrorWithFields("Failed to auto-create queue", "error", err, "queue", queueName)
		return fmt.Errorf("failed to create queue '%s': %w", queueName, err)
	}

	h.logger.InfoWithFields("Queue auto-created successfully", "queue", queueName)
	return nil
}

// renderQueueWarningDialog writes an HTMX-compatible warning dialog for missing queue
func (h *SchedulesHandler) renderQueueWarningDialog(w http.ResponseWriter, queueName string) {
	h.logger.InfoWithFields("Returning queue warning dialog", "queue", queueName)
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)

	// Escape queueName to prevent XSS injection
	escapedQueueName := html.EscapeString(queueName)

	htmlContent := fmt.Sprintf(`
		<div class="bg-yellow-50 border border-yellow-400 rounded-lg p-4">
			<div class="flex">
				<div class="flex-shrink-0">
					<svg class="h-5 w-5 text-yellow-400" fill="currentColor" viewBox="0 0 20 20">
						<path fill-rule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clip-rule="evenodd"/>
					</svg>
				</div>
				<div class="ml-3 flex-1">
					<h3 class="text-sm font-medium text-yellow-800">Queue '%s' does not exist</h3>
					<div class="mt-2 text-sm text-yellow-700">
						<p>The schedule will be created but messages won't be processed until the queue exists and has workers.</p>
					</div>
					<div class="mt-4 space-x-3">
						<button type="button" onclick="createWithQueue()" 
							class="inline-flex items-center px-3 py-2 border border-transparent text-sm font-medium rounded-md text-yellow-800 bg-yellow-100 hover:bg-yellow-200">
							Create Queue & Schedule
						</button>
						<a href="/queues" class="inline-flex items-center px-3 py-2 text-sm font-medium text-yellow-800">
							Create Queue First
						</a>
					</div>
				</div>
			</div>
		</div>
		<script>
			function createWithQueue() {
				const form = document.querySelector('form');
				const input = document.createElement('input');
				input.type = 'hidden';
				input.name = 'auto_create_queue';
				input.value = 'true';
				form.appendChild(input);
				htmx.trigger(form, 'submit');
			}
		</script>
	`, escapedQueueName)

	if _, err := w.Write([]byte(htmlContent)); err != nil {
		h.logger.Error("Failed to write response", "error", err)
	}
}

// buildScheduleOptions constructs schedule options based on schedule type
func (h *SchedulesHandler) buildScheduleOptions(ctx context.Context, r *http.Request, scheduleType, queueName, payloadData string) (*client.ScheduleOptions, error) {
	// Parse the JSON payload data
	var payloadMap map[string]interface{}
	if payloadData != "" {
		if err := json.Unmarshal([]byte(payloadData), &payloadMap); err != nil {
			return nil, fmt.Errorf("invalid JSON in payload data: %w", err)
		}
	} else {
		payloadMap = make(map[string]interface{})
	}

	payloadStruct, err := structpb.NewStruct(payloadMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create payload struct: %w", err)
	}

	scheduleOpts := &client.ScheduleOptions{
		QueueName: queueName,
		Payload: client.Payload{
			Data: payloadStruct,
		},
		State: client.State(schedule_pb.Schedule_Metadata_SCHEDULED),
	}

	switch scheduleType {
	case "cron":
		cronExpression := r.FormValue("cron_expression")
		if cronExpression == "" {
			return nil, fmt.Errorf("cron expression is required")
		}
		scheduleOpts.CronSchedule = cronExpression

	case "calendar":
		calendarSchedule, err := h.buildCalendarSchedule(ctx, r)
		if err != nil {
			return nil, err
		}
		scheduleOpts.CalendarSchedule = calendarSchedule

	default:
		return nil, fmt.Errorf("invalid schedule type: %s", scheduleType)
	}

	return scheduleOpts, nil
}

// buildCalendarSchedule constructs and validates a calendar schedule from form data
func (h *SchedulesHandler) buildCalendarSchedule(ctx context.Context, r *http.Request) (*schedule_pb.CalendarSchedule, error) {
	timezone := r.FormValue("timezone")
	if timezone == "" {
		timezone = "UTC"
	}

	calendarType := r.FormValue("calendar_type")
	executionTimes, err := h.parseExecutionTimes(r.FormValue("execution_times"))
	if err != nil {
		return nil, err
	}

	calendarRule, err := h.buildCalendarRule(r, calendarType, executionTimes)
	if err != nil {
		return nil, err
	}

	scheduleTypeEnum := h.mapCalendarTypeToEnum(calendarType)

	calendarSchedule := &schedule_pb.CalendarSchedule{
		Type:     scheduleTypeEnum,
		Timezone: timezone,
		Rules:    []*schedule_pb.CalendarRule{calendarRule},
	}

	// Validate using calendar engine
	engine := calendar.NewDefaultEngine()
	if err := engine.ValidateSchedule(ctx, calendarSchedule); err != nil {
		return nil, fmt.Errorf("calendar schedule validation failed: %w", err)
	}

	return calendarSchedule, nil
}

// parseExecutionTimes parses comma-separated HH:MM time strings
func (h *SchedulesHandler) parseExecutionTimes(executionTimesStr string) ([]*schedule_pb.TimeOfDay, error) {
	executionTimes := []*schedule_pb.TimeOfDay{}

	if executionTimesStr != "" {
		timeStrs := strings.Split(executionTimesStr, ",")
		for _, timeStr := range timeStrs {
			timeStr = strings.TrimSpace(timeStr)
			parts := strings.Split(timeStr, ":")
			if len(parts) == 2 {
				hour, err := strconv.Atoi(parts[0])
				if err != nil || hour < 0 || hour > 23 {
					return nil, fmt.Errorf("invalid hour in time '%s': must be 0-23", timeStr)
				}
				minute, err := strconv.Atoi(parts[1])
				if err != nil || minute < 0 || minute > 59 {
					return nil, fmt.Errorf("invalid minute in time '%s': must be 0-59", timeStr)
				}
				executionTimes = append(executionTimes, &schedule_pb.TimeOfDay{
					Hour:   int32(hour),
					Minute: int32(minute),
				})
			} else {
				return nil, fmt.Errorf("invalid time format '%s': expected HH:MM", timeStr)
			}
		}
	}

	// Default to midnight if no times specified
	if len(executionTimes) == 0 {
		executionTimes = []*schedule_pb.TimeOfDay{{Hour: 0, Minute: 0}}
	}

	return executionTimes, nil
}

// buildCalendarRule constructs a calendar rule based on calendar type
func (h *SchedulesHandler) buildCalendarRule(r *http.Request, calendarType string, executionTimes []*schedule_pb.TimeOfDay) (*schedule_pb.CalendarRule, error) {
	switch calendarType {
	case "DAILY":
		return &schedule_pb.CalendarRule{
			Rule: &schedule_pb.CalendarRule_Daily{
				Daily: &schedule_pb.DailyRule{
					DayInterval: 1,
				},
			},
			ExecutionTimes: executionTimes,
		}, nil

	case "WEEKLY":
		daysOfWeek := h.parseDaysOfWeek(r.Form["days_of_week"])
		return &schedule_pb.CalendarRule{
			Rule: &schedule_pb.CalendarRule_Weekly{
				Weekly: &schedule_pb.WeeklyRule{
					DaysOfWeek:   daysOfWeek,
					WeekInterval: 1,
				},
			},
			ExecutionTimes: executionTimes,
		}, nil

	case "MONTHLY":
		dayOfMonth := h.parseDayOfMonth(r.FormValue("day_of_month"))
		return &schedule_pb.CalendarRule{
			Rule: &schedule_pb.CalendarRule_Monthly{
				Monthly: &schedule_pb.MonthlyRule{
					DayType:  schedule_pb.MonthlyRule_DAY_OF_MONTH,
					DayValue: dayOfMonth,
				},
			},
			ExecutionTimes: executionTimes,
		}, nil

	case "BUSINESS_DAYS":
		return &schedule_pb.CalendarRule{
			Rule: &schedule_pb.CalendarRule_BusinessDays{
				BusinessDays: &schedule_pb.BusinessDaysRule{},
			},
			ExecutionTimes: executionTimes,
		}, nil

	default:
		return nil, fmt.Errorf("invalid calendar type: %s", calendarType)
	}
}

// parseDaysOfWeek parses checkbox values into day numbers
func (h *SchedulesHandler) parseDaysOfWeek(daysOfWeekStrs []string) []int32 {
	daysOfWeek := []int32{}
	for _, day := range daysOfWeekStrs {
		if dayNum, err := strconv.Atoi(day); err == nil {
			// Validate day is in range 1-7 (ISO 8601: 1=Monday, 7=Sunday)
			if dayNum < 1 || dayNum > 7 {
				continue
			}
			daysOfWeek = append(daysOfWeek, int32(dayNum))
		}
	}
	// Default to weekdays if none selected
	if len(daysOfWeek) == 0 {
		daysOfWeek = []int32{1, 2, 3, 4, 5} // Mon-Fri
	}
	return daysOfWeek
}

// parseDayOfMonth parses day of month string with default
func (h *SchedulesHandler) parseDayOfMonth(dayOfMonthStr string) int32 {
	if dayOfMonthStr != "" {
		if day, err := strconv.Atoi(dayOfMonthStr); err == nil {
			// Validate day is in range 1-31
			if day < 1 || day > 31 {
				return 1 // Default to 1st if invalid
			}
			return int32(day)
		}
	}
	return 1 // Default to 1st
}

// mapCalendarTypeToEnum converts string calendar type to protobuf enum
func (h *SchedulesHandler) mapCalendarTypeToEnum(calendarType string) schedule_pb.CalendarSchedule_ScheduleType {
	switch calendarType {
	case "MONTHLY":
		return schedule_pb.CalendarSchedule_MONTHLY
	case "WEEKLY":
		return schedule_pb.CalendarSchedule_WEEKLY
	case "DAILY":
		return schedule_pb.CalendarSchedule_DAILY
	case "BUSINESS_DAYS":
		return schedule_pb.CalendarSchedule_BUSINESS_DAYS
	default:
		return schedule_pb.CalendarSchedule_DAILY
	}
}

// Update handles schedule updates (HTMX POST)
func (h *SchedulesHandler) Update(w http.ResponseWriter, r *http.Request) {
	scheduleID := r.PathValue("id")
	if scheduleID == "" {
		http.Error(w, "Schedule ID is required", http.StatusBadRequest)
		return
	}

	// For now, updates require deleting and recreating
	// TODO: Implement proper update if API supports it
	http.Error(w, "Schedule updates not yet implemented", http.StatusNotImplemented)
}

// Toggle handles pause/resume (HTMX POST)
func (h *SchedulesHandler) Toggle(w http.ResponseWriter, r *http.Request) {
	scheduleID := r.FormValue("schedule_id")
	action := r.FormValue("action") // "pause" or "resume"

	if scheduleID == "" || action == "" {
		http.Error(w, "Schedule ID and action are required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var err error
	switch action {
	case "pause":
		_, err = h.client.PauseSchedule(ctx, scheduleID)
	case "resume":
		_, err = h.client.ResumeSchedule(ctx, scheduleID)
	default:
		http.Error(w, "Invalid action", http.StatusBadRequest)
		return
	}

	if err != nil {
		h.logger.ErrorWithFields("Failed to toggle schedule", "error", err, "schedule_id", scheduleID, "action", action)
		http.Error(w, fmt.Sprintf("Failed to %s schedule: %v", action, err), http.StatusInternalServerError)
		return
	}

	// Return updated status badge
	statusClass := "bg-green-100 text-green-800"
	statusText := "Active"
	if action == "pause" {
		statusClass = "bg-yellow-100 text-yellow-800"
		statusText = "Paused"
	}

	html := fmt.Sprintf(`<span class="px-2 py-1 text-xs font-medium rounded %s">%s</span>`, statusClass, statusText)
	w.Header().Set("Content-Type", "text/html")
	if _, err := w.Write([]byte(html)); err != nil {
		h.logger.Error("Failed to write response", "error", err)
	}
}

// Delete handles schedule deletion (HTMX DELETE)
func (h *SchedulesHandler) Delete(w http.ResponseWriter, r *http.Request) {
	scheduleID := r.PathValue("id")
	if scheduleID == "" {
		scheduleID = r.FormValue("schedule_id")
	}

	if scheduleID == "" {
		http.Error(w, "Schedule ID is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	_, err := h.client.DeleteSchedule(ctx, scheduleID)
	if err != nil {
		h.logger.ErrorWithFields("Failed to delete schedule", "error", err, "schedule_id", scheduleID)
		http.Error(w, fmt.Sprintf("Failed to delete schedule: %v", err), http.StatusInternalServerError)
		return
	}

	// Return success - HTMX will remove the row
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("Schedule deleted successfully")); err != nil {
		h.logger.Error("Failed to write response", "error", err)
	}
}
