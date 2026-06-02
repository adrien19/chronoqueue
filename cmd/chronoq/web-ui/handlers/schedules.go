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
	clusterstore "github.com/adrien19/chronoqueue/cmd/chronoq/web-ui/cluster"
	"github.com/adrien19/chronoqueue/pkg/calendar"
	"github.com/adrien19/chronoqueue/pkg/log"
)

// SchedulesHandler handles schedule-related pages and HTMX actions.
type SchedulesHandler struct {
	BaseHandler
}

// NewSchedulesHandler creates a SchedulesHandler.
func NewSchedulesHandler(
	templates *template.Template,
	store *clusterstore.Store,
	logger *log.Logger,
) *SchedulesHandler {
	return &SchedulesHandler{
		BaseHandler: BaseHandler{
			templates: templates,
			store:     store,
			logger:    logger,
		},
	}
}

// List renders the schedules listing page.
func (h *SchedulesHandler) List(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	schedulesResp, err := h.activeClient().ListSchedules(ctx, "")
	if err != nil {
		h.logger.ErrorWithFields("Failed to list schedules", "error", err)
		h.renderError(w, http.StatusInternalServerError, "Failed to load schedules")
		return
	}

	queuesResp, err := h.activeClient().ListQueues(ctx, "")
	if err != nil {
		h.logger.ErrorWithFields("Failed to list queues", "error", err)
	}

	existingQueues := make(map[string]bool)
	if queuesResp != nil {
		for _, q := range queuesResp.GetQueues() {
			existingQueues[q.GetName()] = true
		}
	}

	hasMissingQueues := false
	for _, schedule := range schedulesResp.GetSchedules() {
		if !existingQueues[schedule.GetMetadata().GetQueueName()] {
			hasMissingQueues = true
			break
		}
	}

	data := map[string]any{
		"PageTitle":        "Schedules",
		"Active":           "schedules",
		"Schedules":        schedulesResp.GetSchedules(),
		"ExistingQueues":   existingQueues,
		"HasMissingQueues": hasMissingQueues,
	}
	h.render(w, "schedules_content", data)
}

// New renders the create-schedule form.
func (h *SchedulesHandler) New(w http.ResponseWriter, r *http.Request) {
	data := map[string]any{
		"PageTitle": "Create Schedule",
		"Active":    "schedules",
		"DaysOfWeek": []DayOption{
			{Label: "Mon", Value: "1"},
			{Label: "Tue", Value: "2"},
			{Label: "Wed", Value: "3"},
			{Label: "Thu", Value: "4"},
			{Label: "Fri", Value: "5"},
			{Label: "Sat", Value: "6"},
			{Label: "Sun", Value: "7"},
		},
	}
	h.render(w, "schedule_new_content", data)
}

// Create handles schedule creation (HTMX POST).
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

	if queueName == scheduleID {
		http.Error(w, "Queue Name cannot be the same as Schedule ID", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := h.ensureQueueExists(ctx, w, r, queueName, scheduleID); err != nil {
		return
	}

	scheduleOpts, err := h.buildScheduleOptions(ctx, r, scheduleType, queueName, payloadData)
	if err != nil {
		h.logger.ErrorWithFields("Failed to build schedule options", "error", err, "schedule_id", scheduleID)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if _, err := h.activeClient().CreateSchedule(ctx, scheduleID, *scheduleOpts); err != nil {
		h.logger.ErrorWithFields("Failed to create schedule", "error", err, "schedule_id", scheduleID)
		http.Error(w, fmt.Sprintf("Failed to create schedule: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("HX-Redirect", "/schedules")
	w.WriteHeader(http.StatusCreated)
	if _, err := w.Write([]byte("Schedule created successfully")); err != nil {
		h.logger.Error("Failed to write response", "error", err)
	}
}

// ensureQueueExists checks queue existence, auto-creates if requested, or returns a warning fragment.
func (h *SchedulesHandler) ensureQueueExists(ctx context.Context, w http.ResponseWriter, r *http.Request, queueName, scheduleID string) error {
	autoCreate := r.FormValue("auto_create_queue") == "true"

	queuesResp, err := h.activeClient().ListQueues(ctx, "")
	if err != nil {
		h.logger.ErrorWithFields("Failed to check queue existence", "error", err)
		http.Error(w, "Failed to verify queue existence", http.StatusInternalServerError)
		return err
	}

	exists := false
	for _, q := range queuesResp.GetQueues() {
		if q.GetName() == queueName {
			exists = true
			break
		}
	}

	if !exists && !autoCreate {
		h.renderQueueWarningDialog(w, queueName)
		return fmt.Errorf("queue does not exist")
	}

	if !exists && autoCreate {
		return h.autoCreateQueue(ctx, queueName, scheduleID)
	}

	return nil
}

// autoCreateQueue creates a queue with sensible defaults.
func (h *SchedulesHandler) autoCreateQueue(ctx context.Context, queueName, scheduleID string) error {
	h.logger.InfoWithFields("Auto-creating queue for schedule", "queue", queueName, "schedule", scheduleID)
	opts := client.QueueOptions{
		DequeueAttempts:     3,
		LeaseDuration:       "5m",
		AutoCreateDLQ:       true,
		DeadLetterQueueName: queueName + "-dlq",
	}
	if _, err := h.activeClient().CreateQueue(ctx, queueName, opts); err != nil {
		h.logger.ErrorWithFields("Failed to auto-create queue", "error", err, "queue", queueName)
		return fmt.Errorf("failed to create queue '%s': %w", queueName, err)
	}
	return nil
}

// renderQueueWarningDialog writes an HTMX-compatible warning fragment for missing queues.
func (h *SchedulesHandler) renderQueueWarningDialog(w http.ResponseWriter, queueName string) {
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	escaped := html.EscapeString(queueName)
	htmlContent := fmt.Sprintf(`
<div class="rounded-lg border border-amber-500/30 bg-amber-500/10 px-4 py-3">
  <div class="flex items-start gap-3">
    <span class="mt-0.5 text-amber-300">⚠</span>
    <div class="flex-1">
      <p class="text-sm font-medium text-amber-200">Queue '%s' does not exist</p>
      <p class="mt-1 text-sm text-amber-300">The schedule will be created but messages won't be processed until the queue exists.</p>
      <div class="mt-3 flex gap-3">
        <button type="button" onclick="(function(){var f=document.querySelector('form');var i=document.createElement('input');i.type='hidden';i.name='auto_create_queue';i.value='true';f.appendChild(i);htmx.trigger(f,'submit');})()"
          class="cq-btn cq-btn-primary !text-xs">Create Queue &amp; Schedule</button>
        <a href="/queues" class="cq-btn !text-xs">Create Queue First</a>
      </div>
    </div>
  </div>
</div>`, escaped)
	if _, err := w.Write([]byte(htmlContent)); err != nil {
		h.logger.Error("Failed to write response", "error", err)
	}
}

// Toggle handles pause/resume (HTMX POST).
func (h *SchedulesHandler) Toggle(w http.ResponseWriter, r *http.Request) {
	scheduleID := r.FormValue("schedule_id")
	action := r.FormValue("action")

	if scheduleID == "" || action == "" {
		http.Error(w, "Schedule ID and action are required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var err error
	switch action {
	case "pause":
		_, err = h.activeClient().PauseSchedule(ctx, scheduleID)
	case "resume":
		_, err = h.activeClient().ResumeSchedule(ctx, scheduleID)
	default:
		http.Error(w, "Invalid action", http.StatusBadRequest)
		return
	}

	if err != nil {
		h.logger.ErrorWithFields("Failed to toggle schedule", "error", err, "schedule_id", scheduleID, "action", action)
		http.Error(w, fmt.Sprintf("Failed to %s schedule: %v", action, err), http.StatusInternalServerError)
		return
	}

	// Return the updated controls div so both the badge and the toggle button reflect the new state.
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	sid := html.EscapeString(scheduleID)
	var controlsHTML string
	if action == "pause" {
		controlsHTML = fmt.Sprintf(
			`<div id="schedule-controls-%s" class="flex items-center justify-end gap-2">`+
				`<span class="cq-badge cq-badge-warn">Paused</span>`+
				`<button class="cq-btn cq-btn-primary !px-2 !py-1 text-xs"`+
				` hx-post="/api/schedules/toggle"`+
				` hx-vals="{&quot;schedule_id&quot;: &quot;%s&quot;, &quot;action&quot;: &quot;resume&quot;}"`+
				` hx-target="#schedule-controls-%s"`+
				` hx-swap="outerHTML">Resume</button>`+
				`<button class="cq-btn !px-2 !py-1 text-xs text-red-300 border-red-500/20 hover:border-red-500/40"`+
				` hx-delete="/api/schedules/%s"`+
				` hx-confirm="Delete schedule &#39;%s&#39;?"`+
				` hx-target="#schedule-row-%s"`+
				` hx-swap="outerHTML">Delete</button>`+
				`</div>`,
			sid, sid, sid, sid, sid, sid,
		)
	} else {
		controlsHTML = fmt.Sprintf(
			`<div id="schedule-controls-%s" class="flex items-center justify-end gap-2">`+
				`<span class="cq-badge cq-badge-good">Active</span>`+
				`<button class="cq-btn !px-2 !py-1 text-xs"`+
				` hx-post="/api/schedules/toggle"`+
				` hx-vals="{&quot;schedule_id&quot;: &quot;%s&quot;, &quot;action&quot;: &quot;pause&quot;}"`+
				` hx-target="#schedule-controls-%s"`+
				` hx-swap="outerHTML">Pause</button>`+
				`<button class="cq-btn !px-2 !py-1 text-xs text-red-300 border-red-500/20 hover:border-red-500/40"`+
				` hx-delete="/api/schedules/%s"`+
				` hx-confirm="Delete schedule &#39;%s&#39;?"`+
				` hx-target="#schedule-row-%s"`+
				` hx-swap="outerHTML">Delete</button>`+
				`</div>`,
			sid, sid, sid, sid, sid, sid,
		)
	}
	if _, err := w.Write([]byte(controlsHTML)); err != nil {
		h.logger.Error("Failed to write toggle response", "error", err)
	}
}

// Delete deletes a schedule (HTMX DELETE).
func (h *SchedulesHandler) Delete(w http.ResponseWriter, r *http.Request) {
	scheduleID := r.PathValue("id")
	if scheduleID == "" {
		http.Error(w, "Schedule ID required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if _, err := h.activeClient().DeleteSchedule(ctx, scheduleID); err != nil {
		h.logger.ErrorWithFields("Failed to delete schedule", "error", err, "schedule_id", scheduleID)
		http.Error(w, fmt.Sprintf("Failed to delete schedule: %v", err), http.StatusInternalServerError)
		return
	}

	// Return empty string so HTMX removes the row
	w.WriteHeader(http.StatusOK)
}

// buildScheduleOptions constructs schedule options from form data.
func (h *SchedulesHandler) buildScheduleOptions(ctx context.Context, r *http.Request, scheduleType, queueName, payloadData string) (*client.ScheduleOptions, error) {
	var payloadMap map[string]any
	if payloadData != "" {
		if err := json.Unmarshal([]byte(payloadData), &payloadMap); err != nil {
			return nil, fmt.Errorf("invalid JSON in payload data: %w", err)
		}
	} else {
		payloadMap = make(map[string]any)
	}

	payloadStruct, err := structpb.NewStruct(payloadMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create payload struct: %w", err)
	}

	opts := &client.ScheduleOptions{
		QueueName: queueName,
		Payload:   client.Payload{Data: payloadStruct},
		State:     client.State(schedule_pb.Schedule_Metadata_SCHEDULED),
	}

	switch scheduleType {
	case "cron":
		cronExpr := r.FormValue("cron_expression")
		if cronExpr == "" {
			return nil, fmt.Errorf("cron expression is required")
		}
		opts.CronSchedule = cronExpr

	case "calendar":
		cal, err := h.buildCalendarSchedule(ctx, r)
		if err != nil {
			return nil, err
		}
		opts.CalendarSchedule = cal

	default:
		return nil, fmt.Errorf("invalid schedule type: %s", scheduleType)
	}

	return opts, nil
}

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

	rule, err := h.buildCalendarRule(r, calendarType, executionTimes)
	if err != nil {
		return nil, err
	}

	cal := &schedule_pb.CalendarSchedule{
		Type:     h.mapCalendarTypeToEnum(calendarType),
		Timezone: timezone,
		Rules:    []*schedule_pb.CalendarRule{rule},
	}

	engine := calendar.NewDefaultEngine()
	if err := engine.ValidateSchedule(ctx, cal); err != nil {
		return nil, fmt.Errorf("calendar schedule validation failed: %w", err)
	}

	return cal, nil
}

func (h *SchedulesHandler) parseExecutionTimes(s string) ([]*schedule_pb.TimeOfDay, error) {
	var times []*schedule_pb.TimeOfDay
	if s != "" {
		for _, t := range strings.Split(s, ",") {
			t = strings.TrimSpace(t)
			parts := strings.Split(t, ":")
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid time format '%s': expected HH:MM", t)
			}
			hour, err := strconv.Atoi(parts[0])
			if err != nil || hour < 0 || hour > 23 {
				return nil, fmt.Errorf("invalid hour in time '%s': must be 0-23", t)
			}
			min, err := strconv.Atoi(parts[1])
			if err != nil || min < 0 || min > 59 {
				return nil, fmt.Errorf("invalid minute in time '%s': must be 0-59", t)
			}
			times = append(times, &schedule_pb.TimeOfDay{Hour: int32(hour), Minute: int32(min)})
		}
	}
	if len(times) == 0 {
		times = []*schedule_pb.TimeOfDay{{Hour: 0, Minute: 0}}
	}
	return times, nil
}

func (h *SchedulesHandler) buildCalendarRule(r *http.Request, calendarType string, executionTimes []*schedule_pb.TimeOfDay) (*schedule_pb.CalendarRule, error) {
	switch calendarType {
	case "DAILY":
		return &schedule_pb.CalendarRule{
			Rule:           &schedule_pb.CalendarRule_Daily{Daily: &schedule_pb.DailyRule{DayInterval: 1}},
			ExecutionTimes: executionTimes,
		}, nil

	case "WEEKLY":
		return &schedule_pb.CalendarRule{
			Rule: &schedule_pb.CalendarRule_Weekly{Weekly: &schedule_pb.WeeklyRule{
				DaysOfWeek:   h.parseDaysOfWeek(r.Form["days_of_week"]),
				WeekInterval: 1,
			}},
			ExecutionTimes: executionTimes,
		}, nil

	case "MONTHLY":
		return &schedule_pb.CalendarRule{
			Rule: &schedule_pb.CalendarRule_Monthly{Monthly: &schedule_pb.MonthlyRule{
				DayType:  schedule_pb.MonthlyRule_DAY_OF_MONTH,
				DayValue: h.parseDayOfMonth(r.FormValue("day_of_month")),
			}},
			ExecutionTimes: executionTimes,
		}, nil

	case "BUSINESS_DAYS":
		return &schedule_pb.CalendarRule{
			Rule:           &schedule_pb.CalendarRule_BusinessDays{BusinessDays: &schedule_pb.BusinessDaysRule{}},
			ExecutionTimes: executionTimes,
		}, nil

	default:
		return nil, fmt.Errorf("invalid calendar type: %s", calendarType)
	}
}

func (h *SchedulesHandler) parseDaysOfWeek(strs []string) []int32 {
	var days []int32
	for _, s := range strs {
		if d, err := strconv.Atoi(s); err == nil && d >= 1 && d <= 7 {
			days = append(days, int32(d))
		}
	}
	if len(days) == 0 {
		days = []int32{1, 2, 3, 4, 5}
	}
	return days
}

func (h *SchedulesHandler) parseDayOfMonth(s string) int32 {
	if s != "" {
		if d, err := strconv.Atoi(s); err == nil && d >= 1 && d <= 31 {
			return int32(d)
		}
	}
	return 1
}

func (h *SchedulesHandler) mapCalendarTypeToEnum(t string) schedule_pb.CalendarSchedule_ScheduleType {
	switch t {
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
