package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"html/template"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	"github.com/adrien19/chronoqueue/client"
	clusterstore "github.com/adrien19/chronoqueue/cmd/chronoq/web-ui/cluster"
	"github.com/adrien19/chronoqueue/pkg/log"
)

// QueuesHandler handles queue-related pages and HTMX fragments.
type QueuesHandler struct {
	BaseHandler
}

// MessageDisplay is the view model for a single message row in the message table.
type MessageDisplay struct {
	Id           string
	ShortId      string
	State        string
	Priority     int64
	AttemptCount int32
	ScheduledAt  *time.Time
}

// QueueDetail contains the view model for the queue detail page.
type QueueDetail struct {
	Name         string
	Ready        string
	InFlight     string
	Delayed      string
	Errored      string
	DLQ          string
	DLQName      string // non-empty when this queue has an associated DLQ
	SourceQueue  string // non-empty when this queue is itself a DLQ
	SourceQueues []string
	Completed    string
	IsDLQ        bool
}

// QueueSchemaOption is the view model for queue create schema suggestions.
type QueueSchemaOption struct {
	SchemaID      string
	LatestVersion int32
	Name          string
}

// shortenID truncates an ID to the first 12 characters.
func shortenID(id string) string {
	if len(id) <= 12 {
		return id
	}
	return id[:12]
}

type queueAssociations struct {
	dlqBySource  map[string]string
	sourcesByDLQ map[string][]string
}

var errDLQMetadataUnavailable = errors.New("DLQ metadata unavailable")

func buildQueueAssociations(queues []*queue_pb.Queue) queueAssociations {
	associations := queueAssociations{
		dlqBySource:  make(map[string]string),
		sourcesByDLQ: make(map[string][]string),
	}
	for _, queue := range queues {
		if queue == nil || queue.GetMetadata() == nil || queue.GetMetadata().GetDeadLetterQueueName() == "" {
			continue
		}
		source, dlq := queue.GetName(), queue.GetMetadata().GetDeadLetterQueueName()
		associations.dlqBySource[source] = dlq
		associations.sourcesByDLQ[dlq] = append(associations.sourcesByDLQ[dlq], source)
	}
	return associations
}

// NewQueuesHandler creates a QueuesHandler.
func NewQueuesHandler(
	templates *template.Template,
	store *clusterstore.Store,
	logger *log.Logger,
) *QueuesHandler {
	return &QueuesHandler{
		BaseHandler: BaseHandler{
			templates: templates,
			store:     store,
			logger:    logger,
		},
	}
}

// List renders the queue listing page.
func (h *QueuesHandler) List(w http.ResponseWriter, r *http.Request) {
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	query := r.URL.Query().Get("q")

	queuesResp, err := activeClient.ListQueues(ctx, "")
	if err != nil {
		h.writeRPCError(w, r, "list queues", err)
		return
	}

	var rows []QueueRow
	partialData := false
	associations := buildQueueAssociations(queuesResp.GetQueues())
	for _, q := range queuesResp.GetQueues() {
		name := q.GetName()
		if query != "" && !strings.Contains(strings.ToLower(name), strings.ToLower(query)) {
			continue
		}

		stateResp, err := activeClient.GetQueueState(ctx, name)
		if err != nil {
			h.logger.ErrorWithFields("Failed to get queue state", "error", err, "queue", name)
			partialData = true
			rows = append(rows, QueueRow{Name: name, Href: "/queues/" + name, IsDLQ: len(associations.sourcesByDLQ[name]) > 0})
			continue
		}
		counts := stateResp.GetStateCounts()
		pending := int64(counts["PENDING"])
		running := int64(counts["RUNNING"])
		errored := int64(counts["ERRORED"])
		delayed := int64(counts["INVISIBLE"])
		dlqDisplay := "—"
		var dlqCount int
		if dlqName := associations.dlqBySource[name]; dlqName != "" {
			dlqResp, err := activeClient.GetDLQStats(ctx, dlqName)
			if err != nil {
				h.logger.ErrorWithFields("Failed to get DLQ stats", "error", err, "queue", name, "dlq", dlqName)
				partialData = true
			} else {
				dlqCount = int(dlqResp.GetMessageCount())
				dlqDisplay = strconv.Itoa(dlqCount)
			}
		}

		rows = append(rows, QueueRow{
			Name:       name,
			Ready:      fmt.Sprintf("%d", pending),
			InFlight:   fmt.Sprintf("%d", running),
			Delayed:    fmt.Sprintf("%d", delayed),
			Errored:    fmt.Sprintf("%d", errored),
			ErroredInt: int(errored),
			DLQ:        dlqDisplay,
			DLQInt:     dlqCount,
			Href:       "/queues/" + name,
			IsDLQ:      len(associations.sourcesByDLQ[name]) > 0,
		})
	}

	data := map[string]any{
		"PageTitle": "Queues",
		"Active":    "queues",
		"Query":     query,
		"Rows":      rows,
	}
	if partialData {
		warning := "Some queue state or dead-letter statistics could not be loaded. Unknown values are left blank."
		if r.Header.Get("HX-Request") == "true" {
			data["PartialDataFragmentWarning"] = warning
		} else {
			data["PartialDataWarning"] = warning
		}
	}

	if r.Header.Get("HX-Request") == "true" {
		h.renderFragment(w, "queue_table", data)
		return
	}
	h.render(w, "queues_content", data)
}

// queueNamePattern validates queue names: letters, digits, hyphens, underscores, starting with a letter or digit.
var queueNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)

// New renders the create queue form.
func (h *QueuesHandler) New(w http.ResponseWriter, r *http.Request) {
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	schemaOptions, schemaErr := h.loadSchemaOptions(ctx, activeClient)

	data := map[string]any{
		"PageTitle": "New Queue",
		"Active":    "queues",
		"Schemas":   schemaOptions,
	}
	if schemaErr != nil {
		data["PartialDataWarning"] = "Schema suggestions could not be loaded. You can still enter a schema ID manually."
	}
	h.render(w, "queue_new_content", data)
}

// Create handles queue creation (HTMX POST).
func (h *QueuesHandler) Create(w http.ResponseWriter, r *http.Request) {
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}
	if err := r.ParseForm(); err != nil {
		h.logger.ErrorWithFields("Failed to parse form", "error", err)
		h.writeInlineFormError(w, r, "Invalid form data")
		return
	}

	name := strings.TrimSpace(r.FormValue("name"))
	queueType := r.FormValue("type")
	exclusivityKey := strings.TrimSpace(r.FormValue("exclusivity_key"))
	leaseDuration := strings.TrimSpace(r.FormValue("lease_duration"))
	dlqName := strings.TrimSpace(r.FormValue("dlq_name"))
	autoCreateDLQ := r.FormValue("auto_create_dlq") == "true"
	maxAttemptsStr := r.FormValue("default_max_attempts")
	schemaID := strings.TrimSpace(r.FormValue("schema_id"))
	schemaRequired := r.FormValue("schema_required") == "true"

	if name == "" {
		h.writeInlineFormError(w, r, "Queue name is required")
		return
	}
	if !queueNamePattern.MatchString(name) {
		h.writeInlineFormError(w, r, "Queue name may only contain letters, digits, hyphens, and underscores, and must start with a letter or digit")
		return
	}
	if queueType == "exclusive" && exclusivityKey == "" {
		h.writeInlineFormError(w, r, "Exclusivity key is required for exclusive queues")
		return
	}
	if leaseDuration == "" {
		leaseDuration = "30s"
	}
	if _, err := time.ParseDuration(leaseDuration); err != nil {
		h.writeInlineFormError(w, r, fmt.Sprintf("Invalid lease duration %q — use Go duration syntax, e.g. 30s, 5m, 1h", leaseDuration))
		return
	}

	maxAttempts := int32(3)
	if maxAttemptsStr != "" {
		v, err := strconv.ParseInt(maxAttemptsStr, 10, 32)
		if err != nil || v < 1 {
			h.writeInlineFormError(w, r, "Max attempts must be a positive integer")
			return
		}
		maxAttempts = int32(v)
	}

	if autoCreateDLQ && dlqName == "" {
		dlqName = name + "-dlq"
	}

	if schemaRequired && schemaID == "" {
		h.writeInlineFormError(w, r, "Schema ID is required when schema validation is mandatory")
		return
	}

	opts := client.QueueOptions{
		Type:                client.ParseQueueType(queueType),
		DequeueAttempts:     maxAttempts,
		LeaseDuration:       leaseDuration,
		ExclusivityKey:      exclusivityKey,
		DeadLetterQueueName: dlqName,
		AutoCreateDLQ:       autoCreateDLQ,
		SchemaID:            schemaID,
		SchemaRequired:      schemaRequired,
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if _, err := activeClient.CreateQueue(ctx, name, opts); err != nil {
		h.writeRPCError(w, r, "create queue", err)
		return
	}

	if isHTMXRequest(r) {
		w.Header().Set("HX-Redirect", "/queues")
		w.WriteHeader(http.StatusCreated)
		if _, err := w.Write([]byte("Queue created successfully")); err != nil {
			h.logger.Error("Failed to write response", "error", err)
		}
		return
	}

	http.Redirect(w, r, "/queues", http.StatusSeeOther)
}

// Detail renders the queue detail page with message list.
func (h *QueuesHandler) Detail(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")
	if queueName == "" {
		h.renderError(w, http.StatusBadRequest, "Queue name required")
		return
	}
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	listResp, err := activeClient.ListQueues(ctx, "")
	if err != nil {
		h.writeRPCError(w, r, "load queue metadata", err)
		return
	}
	associations := buildQueueAssociations(listResp.GetQueues())
	sourceQueues := associations.sourcesByDLQ[queueName]
	isDeadLetterQueue := len(sourceQueues) > 0

	stateResp, err := activeClient.GetQueueState(ctx, queueName)
	if err != nil {
		h.writeRPCError(w, r, "load queue state", err)
		return
	}
	counts := stateResp.GetStateCounts()

	var rawMessages []*message_pb.Message
	if isDeadLetterQueue {
		dlqResp, err := activeClient.GetDLQMessages(ctx, queueName, 100)
		if err != nil {
			h.writeRPCError(w, r, "load DLQ messages", err)
			return
		}
		rawMessages = dlqResp.GetMessages()
	} else {
		peekResp, err := activeClient.PeekQueueMessages(ctx, queueName, 100, client.TimeRangeOption{})
		if err != nil {
			h.writeRPCError(w, r, "load queue messages", err)
			return
		}
		rawMessages = peekResp.GetMessages()
	}

	messages := buildMessageDisplays(rawMessages)

	queue := QueueDetail{
		Name:         queueName,
		Ready:        fmt.Sprintf("%d", counts["PENDING"]),
		InFlight:     fmt.Sprintf("%d", counts["RUNNING"]),
		Delayed:      fmt.Sprintf("%d", counts["INVISIBLE"]),
		Errored:      fmt.Sprintf("%d", counts["ERRORED"]),
		DLQ:          "—",
		Completed:    fmt.Sprintf("%d", counts["COMPLETED"]),
		IsDLQ:        isDeadLetterQueue,
		SourceQueues: sourceQueues,
	}

	if len(sourceQueues) == 1 {
		queue.SourceQueue = sourceQueues[0]
	}
	queue.DLQName = associations.dlqBySource[queueName]
	if queue.DLQName != "" {
		dlqStats, err := activeClient.GetDLQStats(ctx, queue.DLQName)
		if err != nil {
			h.writeRPCError(w, r, "load DLQ statistics", err)
			return
		}
		queue.DLQ = fmt.Sprintf("%d", dlqStats.GetMessageCount())
	}

	data := map[string]any{
		"PageTitle":     "Queue: " + queueName,
		"Active":        "queues",
		"Queue":         queue,
		"QueueMessages": messages,
	}
	h.render(w, "queue_detail_content", data)
}

func buildMessageDisplays(rawMessages []*message_pb.Message) []MessageDisplay {
	messages := make([]MessageDisplay, 0, len(rawMessages))
	for _, msg := range rawMessages {
		if msg == nil {
			continue
		}
		meta := msg.GetMetadata()
		if meta == nil {
			continue
		}
		var scheduledAt *time.Time
		if ts := meta.GetScheduledTime(); ts != nil {
			t := ts.AsTime()
			scheduledAt = &t
		}
		attemptCount := max(meta.GetMaxAttempts()-meta.GetAttemptsLeft(), 0)
		messages = append(messages, MessageDisplay{
			Id:           msg.GetMessageId(),
			ShortId:      shortenID(msg.GetMessageId()),
			State:        meta.GetState().String(),
			Priority:     meta.GetPriority(),
			AttemptCount: attemptCount,
			ScheduledAt:  scheduledAt,
		})
	}
	return messages
}

// NewMessage renders the post-message form for a queue.
func (h *QueuesHandler) NewMessage(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")
	if queueName == "" {
		h.renderError(w, http.StatusBadRequest, "Queue name required")
		return
	}
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	schemaOptions, schemaErr := h.loadSchemaOptions(ctx, activeClient)

	queueSchemaID, queueSchemaRequired, err := h.resolveQueueSchemaDefaults(ctx, activeClient, queueName)
	queueSchemaLookupFailed := err != nil
	if err != nil {
		h.logger.WarnWithFields("Failed to load queue schema defaults", "error", err, "queue", queueName)
	}

	data := map[string]any{
		"PageTitle":               "New Message — " + queueName,
		"Active":                  "queues",
		"QueueName":               queueName,
		"Schemas":                 schemaOptions,
		"QueueSchemaID":           queueSchemaID,
		"QueueSchemaRequired":     queueSchemaRequired,
		"QueueSchemaLookupFailed": queueSchemaLookupFailed,
	}
	if schemaErr != nil || queueSchemaLookupFailed {
		data["PartialDataWarning"] = "Some queue schema settings could not be loaded. Verify schema values before posting."
	}
	h.render(w, "queue_message_new_content", data)
}

// PostMessage handles message creation for a queue (HTMX POST).
func (h *QueuesHandler) PostMessage(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")
	if queueName == "" {
		h.writeInlineFormError(w, r, "Queue name required")
		return
	}
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}

	if err := r.ParseForm(); err != nil {
		h.logger.ErrorWithFields("Failed to parse form", "error", err)
		h.writeInlineFormError(w, r, "Invalid form data")
		return
	}

	messageID := strings.TrimSpace(r.FormValue("message_id"))
	if messageID == "" {
		messageID = uuid.New().String()
	}

	payloadRaw := strings.TrimSpace(r.FormValue("payload_data"))
	if payloadRaw == "" {
		h.writeInlineFormError(w, r, "Payload is required")
		return
	}
	var payloadMap map[string]any
	if err := json.Unmarshal([]byte(payloadRaw), &payloadMap); err != nil {
		h.writeInlineFormError(w, r, fmt.Sprintf("Invalid JSON payload: %v", err))
		return
	}
	payloadStruct, err := structpb.NewStruct(payloadMap)
	if err != nil {
		h.writeInlineFormError(w, r, fmt.Sprintf("Failed to build payload: %v", err))
		return
	}

	var maxAttempts int32
	if v := strings.TrimSpace(r.FormValue("max_attempts")); v != "" {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil || n < 1 {
			h.writeInlineFormError(w, r, "Max attempts must be a positive integer")
			return
		}
		maxAttempts = int32(n)
	}

	var priority int64
	if v := strings.TrimSpace(r.FormValue("priority")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			h.writeInlineFormError(w, r, "Priority must be a non-negative integer")
			return
		}
		if n > 4 {
			h.writeInlineFormError(w, r, "Priority must be between 0 and 4")
			return
		}
		priority = n
	}

	leaseDuration := strings.TrimSpace(r.FormValue("lease_duration"))
	if leaseDuration != "" {
		if _, err := time.ParseDuration(leaseDuration); err != nil {
			h.writeInlineFormError(w, r, fmt.Sprintf("Invalid lease duration %q — use Go duration syntax, e.g. 30s, 5m", leaseDuration))
			return
		}
	}

	contentType := strings.TrimSpace(r.FormValue("content_type"))
	schemaID := strings.TrimSpace(r.FormValue("schema_id"))
	schemaVersionStr := strings.TrimSpace(r.FormValue("schema_version"))

	var schemaVersion int32
	if schemaVersionStr != "" {
		n, err := strconv.ParseInt(schemaVersionStr, 10, 32)
		if err != nil || n < 0 {
			h.writeInlineFormError(w, r, "Schema version must be a non-negative integer")
			return
		}
		schemaVersion = int32(n)
	}
	if schemaVersion > 0 && schemaID == "" {
		h.writeInlineFormError(w, r, "Schema ID is required when schema version is provided")
		return
	}

	var scheduledTime *time.Time
	if v := strings.TrimSpace(r.FormValue("deliver_at")); v != "" {
		// datetime-local values are submitted as local time and converted to UTC
		// by the browser-side JS before sending. Parse with or without seconds.
		var parsed time.Time
		var err error
		for _, layout := range []string{"2006-01-02T15:04:05", "2006-01-02T15:04"} {
			parsed, err = time.Parse(layout, v)
			if err == nil {
				break
			}
		}
		if err != nil {
			h.writeInlineFormError(w, r, fmt.Sprintf("Invalid deliver-at time %q — expected YYYY-MM-DDTHH:MM", v))
			return
		}
		parsed = parsed.UTC()
		if !parsed.After(time.Now().UTC()) {
			h.writeInlineFormError(w, r, "Deliver at must be a future time")
			return
		}
		scheduledTime = &parsed
	}

	opts := client.MessageOptions{
		Payload: client.Payload{
			Data:          payloadStruct,
			ContentType:   contentType,
			SchemaID:      schemaID,
			SchemaVersion: schemaVersion,
		},
		MaxAttempts:   maxAttempts,
		AttemptsLeft:  maxAttempts,
		Priority:      priority,
		LeaseDuration: leaseDuration,
		ScheduledTime: scheduledTime,
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if _, err := activeClient.PostMessage(ctx, queueName, messageID, opts); err != nil {
		h.writeRPCError(w, r, "post message", err)
		return
	}

	redirectTarget := "/queues/" + url.PathEscape(queueName)
	if isHTMXRequest(r) {
		w.Header().Set("HX-Redirect", redirectTarget)
		w.WriteHeader(http.StatusCreated)
		if _, err := w.Write([]byte("Message posted")); err != nil {
			h.logger.Error("Failed to write response", "error", err)
		}
		return
	}

	http.Redirect(w, r, redirectTarget, http.StatusSeeOther)
}

// ValidateMessage validates payload JSON against selected schema in the post-message flow.
func (h *QueuesHandler) ValidateMessage(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")
	if queueName == "" {
		h.writeInlineFormError(w, r, "Queue name required")
		return
	}
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}

	if err := r.ParseForm(); err != nil {
		h.writeInlineFormError(w, r, "Invalid form data")
		return
	}

	payloadRaw := strings.TrimSpace(r.FormValue("payload_data"))
	if payloadRaw == "" {
		h.writeInlineFormError(w, r, "Payload is required")
		return
	}
	var payloadJSON any
	if err := json.Unmarshal([]byte(payloadRaw), &payloadJSON); err != nil {
		h.writeInlineFormError(w, r, fmt.Sprintf("Invalid JSON payload: %v", err))
		return
	}

	schemaID := strings.TrimSpace(r.FormValue("schema_id"))
	schemaVersionStr := strings.TrimSpace(r.FormValue("schema_version"))

	var schemaVersion int32
	if schemaVersionStr != "" {
		n, err := strconv.ParseInt(schemaVersionStr, 10, 32)
		if err != nil || n < 0 {
			h.writeInlineFormError(w, r, "Schema version must be a non-negative integer")
			return
		}
		schemaVersion = int32(n)
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if schemaID == "" {
		queueSchemaID, _, err := h.resolveQueueSchemaDefaults(ctx, activeClient, queueName)
		if err != nil {
			h.writeRPCError(w, r, "load queue schema defaults", err)
			return
		}
		schemaID = queueSchemaID
	}
	if schemaID == "" {
		h.writeInlineFormError(w, r, "Select a schema or configure a queue default schema before validating")
		return
	}

	if err := activeClient.ValidatePayload(ctx, schemaID, schemaVersion, payloadRaw); err != nil {
		h.writeRPCError(w, r, "validate message payload", err)
		return
	}

	versionLabel := "latest active version"
	if schemaVersion > 0 {
		versionLabel = fmt.Sprintf("v%d", schemaVersion)
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := fmt.Fprintf(w, `<div class="rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-300">Payload is valid for schema <span class="font-mono">%s</span> (%s).</div>`, html.EscapeString(schemaID), html.EscapeString(versionLabel)); err != nil {
		h.logger.ErrorWithFields("Failed to write message validation response", "error", err)
	}
}

func (h *QueuesHandler) resolveQueueSchemaDefaults(ctx context.Context, activeClient *client.ChronoQueueClient, queueName string) (string, bool, error) {
	listResp, err := activeClient.ListQueues(ctx, queueName)
	if err != nil {
		return "", false, err
	}

	for _, q := range listResp.GetQueues() {
		if q.GetName() != queueName {
			continue
		}
		if meta := q.GetMetadata(); meta != nil {
			return meta.GetSchemaId(), meta.GetSchemaRequired(), nil
		}
		break
	}

	return "", false, nil
}

func (h *QueuesHandler) loadSchemaOptions(ctx context.Context, activeClient *client.ChronoQueueClient) ([]QueueSchemaOption, error) {
	schemaOptions := make([]QueueSchemaOption, 0)
	schemas, err := activeClient.ListSchemas(ctx, "", 200, true)
	if err != nil {
		h.logger.WarnWithFields("Failed to load schema options", "error", err)
		return schemaOptions, err
	}

	for _, item := range schemas {
		schemaOptions = append(schemaOptions, QueueSchemaOption{
			SchemaID:      mapString(item, "schema_id"),
			LatestVersion: mapInt32(item, "version"),
			Name:          mapString(item, "name"),
		})
	}

	return schemaOptions, nil
}

// MessageDetail returns the modal HTML for a specific message (HTMX partial).
func (h *QueuesHandler) MessageDetail(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")
	messageID := r.URL.Query().Get("message_id")

	if messageID == "" {
		http.Error(w, "message_id required", http.StatusBadRequest)
		return
	}
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	listResp, err := activeClient.ListQueues(ctx, "")
	if err != nil {
		h.writeRPCError(w, r, "load queue metadata", err)
		return
	}
	associations := buildQueueAssociations(listResp.GetQueues())
	var messages []*message_pb.Message
	if len(associations.sourcesByDLQ[queueName]) > 0 {
		dlqResp, err := activeClient.GetDLQMessages(ctx, queueName, 100)
		if err != nil {
			h.writeRPCError(w, r, "load DLQ message", err)
			return
		}
		messages = dlqResp.GetMessages()
	} else {
		peekResp, err := activeClient.PeekQueueMessages(ctx, queueName, 100, client.TimeRangeOption{})
		if err != nil {
			h.writeRPCError(w, r, "load queue message", err)
			return
		}
		messages = peekResp.GetMessages()
	}

	for _, msg := range messages {
		if msg == nil {
			continue
		}
		if msg.GetMessageId() != messageID {
			continue
		}
		meta := msg.GetMetadata()
		if meta == nil {
			continue
		}

		payload := "{}"
		if p := meta.GetPayload(); p != nil {
			if d := p.GetData(); d != nil {
				dm := d.AsMap()
				if dataStr, ok := dm["data"].(string); ok {
					var nested any
					if json.Unmarshal([]byte(dataStr), &nested) == nil {
						if b, err := json.MarshalIndent(nested, "", "  "); err == nil {
							payload = string(b)
						}
					} else if b, err := json.MarshalIndent(dm, "", "  "); err == nil {
						payload = string(b)
					}
				} else if b, err := json.MarshalIndent(dm, "", "  "); err == nil {
					payload = string(b)
				}
			}
		}

		escapedID := html.EscapeString(messageID)
		escapedPayload := html.EscapeString(payload)

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		htmlContent := fmt.Sprintf(`
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4" onclick="document.getElementById('message-modal').innerHTML=''">
  <div class="w-full max-w-2xl cq-panel p-5 bg-[linear-gradient(180deg,rgba(255,255,255,0.04),rgba(11,15,13,0.97))]" onclick="event.stopPropagation()">
    <div class="mb-4 flex items-center justify-between">
      <h3 class="text-xl font-semibold text-white">Message Details</h3>
      <button onclick="document.getElementById('message-modal').innerHTML=''" class="text-zinc-400 hover:text-white">✕</button>
    </div>
    <div class="mb-4">
      <div class="cq-label mb-1">Message ID</div>
      <div class="flex items-center gap-2 rounded-lg border border-line bg-zinc-950/80 px-3 py-2">
        <code class="flex-1 break-all font-mono text-xs text-zinc-200">%s</code>
        <button onclick="navigator.clipboard.writeText('%s')" class="shrink-0 text-zinc-400 hover:text-white text-xs">Copy</button>
      </div>
    </div>
    <div>
      <div class="cq-label mb-1">Payload</div>
      <pre class="max-h-96 overflow-auto rounded-xl border border-line bg-zinc-950/80 p-4 text-xs text-zinc-200 font-mono whitespace-pre-wrap break-words">%s</pre>
    </div>
    <div class="mt-5 flex justify-end">
      <button onclick="document.getElementById('message-modal').innerHTML=''" class="cq-btn">Close</button>
    </div>
  </div>
</div>`, escapedID, escapedID, escapedPayload)

		if _, err := w.Write([]byte(htmlContent)); err != nil {
			h.logger.ErrorWithFields("Failed to write message detail", "error", err)
		}
		return
	}

	http.Error(w, "Message not found", http.StatusNotFound)
}

// RequeueAll requeues all messages from a DLQ back to the source queue.
func (h *QueuesHandler) RequeueAll(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")
	if err := r.ParseForm(); err != nil {
		h.logger.ErrorWithFields("Failed to parse requeue form", "error", err)
		http.Error(w, "Invalid form data", http.StatusBadRequest)
		return
	}
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	sourceQueue, err := h.resolveDLQTarget(ctx, activeClient, queueName, r.FormValue("target_queue"))
	if err != nil {
		if errors.Is(err, errDLQMetadataUnavailable) {
			h.writeRPCError(w, r, "verify DLQ target", err)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	const pageSize = int32(100)
	requeued := 0
	seen := make(map[string]struct{})
	for {
		dlqResp, err := activeClient.GetDLQMessages(ctx, queueName, pageSize)
		if err != nil {
			h.writeRPCError(w, r, "load DLQ messages", err)
			return
		}
		msgs := dlqResp.GetMessages()
		if len(msgs) == 0 {
			break
		}
		pageRequeued := 0
		for _, msg := range msgs {
			if msg == nil {
				continue
			}
			messageID := msg.GetMessageId()
			if messageID == "" {
				http.Error(w, "DLQ returned a message without an ID", http.StatusInternalServerError)
				return
			}
			if _, exists := seen[messageID]; exists {
				http.Error(w, "DLQ did not advance while requeueing messages", http.StatusInternalServerError)
				return
			}
			if _, err := activeClient.RequeueFromDLQ(ctx, queueName, messageID, sourceQueue); err != nil {
				h.writeRPCError(w, r, "requeue DLQ message", err)
				return
			}
			seen[messageID] = struct{}{}
			requeued++
			pageRequeued++
		}
		if pageRequeued == 0 {
			http.Error(w, "DLQ returned no requeueable messages", http.StatusInternalServerError)
			return
		}
		if len(msgs) < int(pageSize) {
			break
		}
	}

	h.logger.InfoWithFields("Requeued messages from DLQ", "queue", queueName, "count", requeued)
	http.Redirect(w, r, "/queues/"+queueName, http.StatusSeeOther)
}

// RequeueMessage requeues a single message from a DLQ back to its source queue.
func (h *QueuesHandler) RequeueMessage(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")
	messageID := r.PathValue("messageId")
	if err := r.ParseForm(); err != nil {
		h.logger.ErrorWithFields("Failed to parse requeue form", "error", err)
		http.Error(w, "Invalid form data", http.StatusBadRequest)
		return
	}

	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	sourceQueue, err := h.resolveDLQTarget(ctx, activeClient, queueName, r.FormValue("target_queue"))
	if err != nil {
		if errors.Is(err, errDLQMetadataUnavailable) {
			h.writeRPCError(w, r, "verify DLQ target", err)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := activeClient.RequeueFromDLQ(ctx, queueName, messageID, sourceQueue); err != nil {
		h.writeRPCError(w, r, "requeue DLQ message", err)
		return
	}

	h.logger.InfoWithFields("Requeued message from DLQ", "queue", queueName, "message", messageID)
	http.Redirect(w, r, "/queues/"+queueName, http.StatusSeeOther)
}

// DeleteDLQMessage permanently deletes a single message from a DLQ.
func (h *QueuesHandler) DeleteDLQMessage(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")
	messageID := r.PathValue("messageId")

	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	isDLQ, err := h.isConfiguredDLQ(ctx, activeClient, queueName)
	if err != nil {
		h.writeRPCError(w, r, "verify DLQ", err)
		return
	}
	if !isDLQ {
		http.Error(w, "Not a configured DLQ", http.StatusBadRequest)
		return
	}
	if _, err := activeClient.DeleteFromDLQ(ctx, queueName, messageID); err != nil {
		h.writeRPCError(w, r, "delete DLQ message", err)
		return
	}

	h.logger.InfoWithFields("Deleted message from DLQ", "queue", queueName, "message", messageID)
	http.Redirect(w, r, "/queues/"+queueName, http.StatusSeeOther)
}

// Purge removes all messages from a queue (DLQ only).
func (h *QueuesHandler) Purge(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	isDLQ, err := h.isConfiguredDLQ(ctx, activeClient, queueName)
	if err != nil {
		h.writeRPCError(w, r, "verify DLQ", err)
		return
	}
	if !isDLQ {
		http.Error(w, "Purge is only supported for configured DLQ queues", http.StatusBadRequest)
		return
	}

	if _, err := activeClient.PurgeDLQ(ctx, queueName); err != nil {
		h.writeRPCError(w, r, "purge DLQ", err)
		return
	}

	h.logger.InfoWithFields("DLQ purged", "queue", queueName)
	http.Redirect(w, r, "/queues/"+queueName, http.StatusSeeOther)
}

func (h *QueuesHandler) resolveDLQTarget(ctx context.Context, activeClient *client.ChronoQueueClient, dlqName, requestedTarget string) (string, error) {
	queuesResp, err := activeClient.ListQueues(ctx, "")
	if err != nil {
		return "", fmt.Errorf("%w: %w", errDLQMetadataUnavailable, err)
	}
	sources := buildQueueAssociations(queuesResp.GetQueues()).sourcesByDLQ[dlqName]
	return selectDLQTarget(dlqName, sources, requestedTarget)
}

func selectDLQTarget(dlqName string, sources []string, requestedTarget string) (string, error) {
	if len(sources) == 0 {
		return "", fmt.Errorf("queue %q is not a configured DLQ", dlqName)
	}
	if requestedTarget == "" {
		if len(sources) == 1 {
			return sources[0], nil
		}
		return "", fmt.Errorf("target queue is required for a shared DLQ")
	}
	for _, source := range sources {
		if requestedTarget == source {
			return source, nil
		}
	}
	return "", fmt.Errorf("queue %q is not a source for DLQ %q", requestedTarget, dlqName)
}

func (h *QueuesHandler) isConfiguredDLQ(ctx context.Context, activeClient *client.ChronoQueueClient, queueName string) (bool, error) {
	queuesResp, err := activeClient.ListQueues(ctx, "")
	if err != nil {
		return false, err
	}
	return len(buildQueueAssociations(queuesResp.GetQueues()).sourcesByDLQ[queueName]) > 0, nil
}
