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

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	schedule_pb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/client"
	clusterstore "github.com/adrien19/chronoqueue/cmd/chronoq/web-ui/cluster"
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
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	schedulesResp, err := activeClient.ListSchedules(ctx, "")
	if err != nil {
		h.writeRPCError(w, r, "list schedules", err)
		return
	}

	queuesResp, err := activeClient.ListQueues(ctx, "")
	partialDataWarning := ""
	if err != nil {
		h.logger.ErrorWithFields("Failed to list queues", "error", err)
		partialDataWarning = "Queue availability could not be loaded. Schedule rows may have incomplete queue status."
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
		"PageTitle":          "Schedules",
		"Active":             "schedules",
		"Schedules":          schedulesResp.GetSchedules(),
		"ExistingQueues":     existingQueues,
		"HasMissingQueues":   hasMissingQueues,
		"PartialDataWarning": partialDataWarning,
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

// Detail renders schedule configuration and execution history.
func (h *SchedulesHandler) Detail(w http.ResponseWriter, r *http.Request) {
	scheduleID := r.PathValue("id")
	if scheduleID == "" {
		h.renderError(w, http.StatusBadRequest, "Schedule ID required")
		return
	}
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}
	limit := int64(100)
	if raw := r.URL.Query().Get("history_limit"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 1 || parsed > 1000 {
			h.renderError(w, http.StatusBadRequest, "History limit must be between 1 and 1000")
			return
		}
		limit = parsed
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	scheduleResponse, err := activeClient.GetSchedule(ctx, scheduleID)
	if err != nil {
		h.writeRPCError(w, r, "get schedule", err)
		return
	}
	historyResponse, err := activeClient.GetScheduleHistory(ctx, scheduleID, limit)
	if err != nil {
		h.writeRPCError(w, r, "get schedule history", err)
		return
	}
	h.render(w, "schedule_detail_content", map[string]any{"PageTitle": "Schedule: " + scheduleID, "Active": "schedules", "Schedule": scheduleResponse.GetSchedule(), "History": historyResponse.GetScheduleHistory(), "HistoryLimit": limit})
}

// Create handles schedule creation (HTMX POST).
func (h *SchedulesHandler) Create(w http.ResponseWriter, r *http.Request) {
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}
	if err := r.ParseForm(); err != nil {
		h.logger.ErrorWithFields("Failed to parse form", "error", err)
		h.writeInlineFormError(w, r, "Invalid form data")
		return
	}

	scheduleID := strings.TrimSpace(r.FormValue("schedule_id"))
	scheduleType := r.FormValue("schedule_type")
	queueName := strings.TrimSpace(r.FormValue("queue_name"))
	payloadData := r.FormValue("payload_data")

	if scheduleID == "" || queueName == "" {
		h.writeInlineFormError(w, r, "Schedule ID and Queue Name are required")
		return
	}

	if !isValidScheduleID(scheduleID) {
		h.writeInlineFormError(w, r, "Schedule ID must not contain spaces, '/', '?', or '#'")
		return
	}

	if queueName == scheduleID {
		h.writeInlineFormError(w, r, "Queue Name cannot be the same as Schedule ID")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	scheduleOpts, err := h.buildScheduleOptions(ctx, r, scheduleType, queueName, payloadData)
	if err != nil {
		h.logger.ErrorWithFields("Failed to build schedule options", "error", err, "schedule_id", scheduleID)
		h.writeInlineFormError(w, r, err.Error())
		return
	}
	if scheduleOpts.CalendarSchedule != nil {
		calendarJSON, marshalErr := protojson.Marshal(scheduleOpts.CalendarSchedule)
		if marshalErr != nil {
			h.writeInlineFormError(w, r, "Could not encode calendar schedule")
			return
		}
		validation, validationErr := activeClient.ValidateCalendarSchedule(ctx, string(calendarJSON))
		if validationErr != nil {
			h.writeRPCError(w, r, "validate calendar schedule", validationErr)
			return
		}
		if !validation.GetValid() {
			message := validation.GetErrorMessage()
			if message == "" {
				messages := make([]string, 0, len(validation.GetValidationIssues()))
				for _, issue := range validation.GetValidationIssues() {
					messages = append(messages, issue.GetMessage())
				}
				message = strings.Join(messages, "\n")
			}
			if message == "" {
				message = "Calendar schedule is invalid"
			}
			h.writeInlineFormError(w, r, message)
			return
		}
	}
	if err := h.ensureQueueExists(ctx, activeClient, w, r, queueName, scheduleID); err != nil {
		return
	}

	if _, err := activeClient.CreateSchedule(ctx, scheduleID, *scheduleOpts); err != nil {
		h.writeRPCError(w, r, "create schedule", err)
		return
	}

	w.Header().Set("HX-Redirect", "/schedules")
	w.WriteHeader(http.StatusCreated)
	if _, err := w.Write([]byte("Schedule created successfully")); err != nil {
		h.logger.Error("Failed to write response", "error", err)
	}
}

// ensureQueueExists checks queue existence, auto-creates if requested, or returns a warning fragment.
func (h *SchedulesHandler) ensureQueueExists(ctx context.Context, activeClient *client.ChronoQueueClient, w http.ResponseWriter, r *http.Request, queueName, scheduleID string) error {
	autoCreate := r.FormValue("auto_create_queue") == "true"

	queuesResp, err := activeClient.ListQueues(ctx, "")
	if err != nil {
		h.writeRPCError(w, r, "verify queue existence", err)
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
		if err := h.autoCreateQueue(ctx, activeClient, queueName, scheduleID); err != nil {
			h.writeRPCError(w, r, "create schedule queue", err)
			return err
		}
	}

	return nil
}

// autoCreateQueue creates a queue with sensible defaults.
func (h *SchedulesHandler) autoCreateQueue(ctx context.Context, activeClient *client.ChronoQueueClient, queueName, scheduleID string) error {
	h.logger.InfoWithFields("Auto-creating queue for schedule", "queue", queueName, "schedule", scheduleID)
	opts := client.QueueOptions{
		DequeueAttempts:     3,
		LeaseDuration:       "5m",
		AutoCreateDLQ:       true,
		DeadLetterQueueName: queueName + "-dlq",
	}
	if _, err := activeClient.CreateQueue(ctx, queueName, opts); err != nil {
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
		<button type="button" hx-post="/api/schedules/create" hx-include="#schedule-form" hx-vals='{"auto_create_queue":"true"}' hx-target="#form-result"
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
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}
	scheduleID := strings.TrimSpace(r.FormValue("schedule_id"))
	action := r.FormValue("action")

	if scheduleID == "" || action == "" {
		http.Error(w, "Schedule ID and action are required", http.StatusBadRequest)
		return
	}

	if !isValidScheduleID(scheduleID) {
		http.Error(w, "Schedule ID contains invalid characters", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var err error
	switch action {
	case "pause":
		_, err = activeClient.PauseSchedule(ctx, scheduleID)
	case "resume":
		_, err = activeClient.ResumeSchedule(ctx, scheduleID)
	default:
		http.Error(w, "Invalid action", http.StatusBadRequest)
		return
	}

	if err != nil {
		h.writeRPCError(w, r, action+" schedule", err)
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
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}
	scheduleID := r.PathValue("id")
	if scheduleID == "" {
		http.Error(w, "Schedule ID required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if _, err := activeClient.DeleteSchedule(ctx, scheduleID); err != nil {
		h.writeRPCError(w, r, "delete schedule", err)
		return
	}

	// Return empty string so HTMX removes the row
	w.WriteHeader(http.StatusOK)
}

// ValidateCalendar validates the complete protobuf JSON calendar contract on the active server.
func (h *SchedulesHandler) ValidateCalendar(w http.ResponseWriter, r *http.Request) {
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}
	if err := r.ParseForm(); err != nil {
		h.writeInlineFormError(w, r, "Invalid form data")
		return
	}
	raw := strings.TrimSpace(r.FormValue("calendar_json"))
	if raw == "" {
		calendarSchedule, err := h.buildCalendarSchedule(r.Context(), r)
		if err != nil {
			h.writeInlineFormError(w, r, err.Error())
			return
		}
		calendarJSON, err := protojson.Marshal(calendarSchedule)
		if err != nil {
			h.writeInlineFormError(w, r, "Could not encode calendar schedule")
			return
		}
		raw = string(calendarJSON)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	response, err := activeClient.ValidateCalendarSchedule(ctx, raw)
	if err != nil {
		h.writeRPCError(w, r, "validate calendar schedule", err)
		return
	}
	h.renderFragment(w, "calendar_validation_result", response)
}

// PreviewCalendar returns upcoming execution times from the active server.
func (h *SchedulesHandler) PreviewCalendar(w http.ResponseWriter, r *http.Request) {
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}
	if err := r.ParseForm(); err != nil {
		h.writeInlineFormError(w, r, "Invalid form data")
		return
	}
	raw := strings.TrimSpace(r.FormValue("calendar_json"))
	count, err := strconv.ParseInt(strings.TrimSpace(r.FormValue("preview_count")), 10, 32)
	if raw == "" {
		calendarSchedule, buildErr := h.buildCalendarSchedule(r.Context(), r)
		if buildErr != nil {
			h.writeInlineFormError(w, r, buildErr.Error())
			return
		}
		calendarJSON, marshalErr := protojson.Marshal(calendarSchedule)
		if marshalErr != nil {
			h.writeInlineFormError(w, r, "Could not encode calendar schedule")
			return
		}
		raw = string(calendarJSON)
	}
	if err != nil || count < 1 || count > 100 {
		h.writeInlineFormError(w, r, "Preview count must be between 1 and 100")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	response, err := activeClient.PreviewCalendarSchedule(ctx, raw, int32(count))
	if err != nil {
		h.writeRPCError(w, r, "preview calendar schedule", err)
		return
	}
	h.renderFragment(w, "calendar_preview_result", response)
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
	metadata, err := parsePayloadMetadata(r.FormValue("payload_metadata"))
	if err != nil {
		return nil, err
	}
	headers, err := parseMessageHeaders(r.FormValue("headers"))
	if err != nil {
		return nil, err
	}
	opts.Payload.Metadata = metadata
	opts.Payload.ContentType = strings.TrimSpace(r.FormValue("content_type"))
	opts.Payload.SchemaID = strings.TrimSpace(r.FormValue("schema_id"))
	if raw := strings.TrimSpace(r.FormValue("schema_version")); raw != "" {
		version, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || version < 0 {
			return nil, fmt.Errorf("schema version must be a non-negative integer")
		}
		opts.Payload.SchemaVersion = int32(version)
	}
	opts.Headers = headers
	if raw := strings.TrimSpace(r.FormValue("priority")); raw != "" {
		priority, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || priority < 0 || priority > 4 {
			return nil, fmt.Errorf("priority must be between 0 and 4")
		}
		opts.Priority = priority
	}
	if raw := strings.TrimSpace(r.FormValue("max_messages")); raw != "" {
		maximum, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || maximum <= 0 {
			return nil, fmt.Errorf("maximum messages must be a positive integer")
		}
		opts.MaxMessages = maximum
	}
	opts.LeaseDuration = strings.TrimSpace(r.FormValue("lease_duration"))
	if opts.LeaseDuration != "" {
		if duration, err := time.ParseDuration(opts.LeaseDuration); err != nil || duration <= 0 {
			return nil, fmt.Errorf("lease duration must be a positive duration")
		}
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
	if raw := strings.TrimSpace(r.FormValue("calendar_json")); raw != "" {
		var schedule schedule_pb.CalendarSchedule
		if err := protojson.Unmarshal([]byte(raw), &schedule); err != nil {
			return nil, fmt.Errorf("invalid calendar schedule JSON: %w", err)
		}
		if schedule.GetType() == schedule_pb.CalendarSchedule_CUSTOM {
			return nil, fmt.Errorf("CUSTOM calendar schedules are not supported")
		}
		return &schedule, nil
	}
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
		daysOfWeek, err := h.parseDaysOfWeek(r.Form["days_of_week"])
		if err != nil {
			return nil, err
		}
		return &schedule_pb.CalendarRule{
			Rule: &schedule_pb.CalendarRule_Weekly{Weekly: &schedule_pb.WeeklyRule{
				DaysOfWeek:   daysOfWeek,
				WeekInterval: 1,
			}},
			ExecutionTimes: executionTimes,
		}, nil

	case "MONTHLY":
		dayOfMonth, err := h.parseDayOfMonth(r.FormValue("day_of_month"))
		if err != nil {
			return nil, err
		}
		return &schedule_pb.CalendarRule{
			Rule: &schedule_pb.CalendarRule_Monthly{Monthly: &schedule_pb.MonthlyRule{
				DayType:  schedule_pb.MonthlyRule_DAY_OF_MONTH,
				DayValue: dayOfMonth,
			}},
			ExecutionTimes: executionTimes,
		}, nil

	case "YEARLY":
		month, day, err := h.parseYearlyDate(r.FormValue("yearly_month"), r.FormValue("yearly_day"))
		if err != nil {
			return nil, err
		}
		return &schedule_pb.CalendarRule{
			Rule: &schedule_pb.CalendarRule_Yearly{Yearly: &schedule_pb.YearlyRule{
				Month:             month,
				Day:               day,
				AdjustForLeapYear: r.FormValue("adjust_for_leap_year") == "true",
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

func (h *SchedulesHandler) parseDaysOfWeek(strs []string) ([]int32, error) {
	if len(strs) == 0 {
		return []int32{1, 2, 3, 4, 5}, nil
	}
	var days []int32
	for _, s := range strs {
		d, err := strconv.Atoi(s)
		if err != nil || d < 1 || d > 7 {
			return nil, fmt.Errorf("invalid day of week %q: must be an integer 1–7", s)
		}
		days = append(days, int32(d))
	}
	return days, nil
}

func (h *SchedulesHandler) parseDayOfMonth(s string) (int32, error) {
	if s == "" {
		return 1, nil
	}
	d, err := strconv.Atoi(s)
	if err != nil || d < 1 || d > 31 {
		return 0, fmt.Errorf("invalid day of month %q: must be an integer 1–31", s)
	}
	return int32(d), nil
}

func (h *SchedulesHandler) parseYearlyDate(monthRaw, dayRaw string) (int32, int32, error) {
	month, err := strconv.Atoi(monthRaw)
	if err != nil || month < 1 || month > 12 {
		return 0, 0, fmt.Errorf("invalid yearly month %q: must be an integer 1–12", monthRaw)
	}
	day, err := strconv.Atoi(dayRaw)
	if err != nil || day < 1 || day > 31 {
		return 0, 0, fmt.Errorf("invalid yearly day %q: must be an integer 1–31", dayRaw)
	}
	return int32(month), int32(day), nil
}

// isValidScheduleID returns false when the ID contains characters that break
// URL routing, gRPC path construction, or DOM selectors.
func isValidScheduleID(id string) bool {
	return !strings.ContainsAny(id, "/?# \t\n\r")
}

func (h *SchedulesHandler) mapCalendarTypeToEnum(t string) schedule_pb.CalendarSchedule_ScheduleType {
	switch t {
	case "MONTHLY":
		return schedule_pb.CalendarSchedule_MONTHLY
	case "WEEKLY":
		return schedule_pb.CalendarSchedule_WEEKLY
	case "DAILY":
		return schedule_pb.CalendarSchedule_DAILY
	case "YEARLY":
		return schedule_pb.CalendarSchedule_YEARLY
	case "BUSINESS_DAYS":
		return schedule_pb.CalendarSchedule_BUSINESS_DAYS
	default:
		return schedule_pb.CalendarSchedule_DAILY
	}
}
