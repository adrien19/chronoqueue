package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"html/template"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"

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
	CreatedAt    time.Time
	DeliverAt    *time.Time // non-nil when the message has a future scheduled delivery time
}

// QueueDetail contains the view model for the queue detail page.
type QueueDetail struct {
	Name      string
	Ready     string
	InFlight  string
	Retries   string
	DLQ       string
	Completed string
	IsDLQ     bool
}

// shortenID truncates an ID to the first 12 characters.
func shortenID(id string) string {
	if len(id) <= 12 {
		return id
	}
	return id[:12]
}

// isDLQ checks if a queue is a Dead Letter Queue by naming convention.
func isDLQ(queueName string) bool {
	return len(queueName) >= 4 && (strings.HasSuffix(queueName, "-dlq") || strings.HasSuffix(queueName, "_dlq"))
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
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	query := r.URL.Query().Get("q")

	queuesResp, err := h.activeClient().ListQueues(ctx, "")
	if err != nil {
		h.logger.ErrorWithFields("Failed to list queues", "error", err)
		h.renderError(w, http.StatusInternalServerError, "Failed to load queues")
		return
	}

	var rows []QueueRow
	for _, q := range queuesResp.GetQueues() {
		name := q.GetName()
		if query != "" && !strings.Contains(strings.ToLower(name), strings.ToLower(query)) {
			continue
		}

		stateResp, err := h.activeClient().GetQueueState(ctx, name)
		if err != nil {
			h.logger.ErrorWithFields("Failed to get queue state", "error", err, "queue", name)
			rows = append(rows, QueueRow{Name: name, Href: "/queues/" + name, IsDLQ: isDLQ(name)})
			continue
		}
		counts := stateResp.GetStateCounts()
		pending := int64(counts["PENDING"])
		running := int64(counts["RUNNING"])
		errored := int64(counts["ERRORED"])

		rows = append(rows, QueueRow{
			Name:       name,
			Ready:      fmt.Sprintf("%d", pending),
			InFlight:   fmt.Sprintf("%d", running),
			Delayed:    "0",
			Retries:    fmt.Sprintf("%d", errored),
			RetriesInt: int(errored),
			DLQ:        "—",
			Href:       "/queues/" + name,
			IsDLQ:      isDLQ(name),
		})
	}

	data := map[string]any{
		"PageTitle": "Queues",
		"Active":    "queues",
		"Query":     query,
		"Rows":      rows,
	}

	if r.Header.Get("HX-Request") == "true" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := h.templates.ExecuteTemplate(w, "queue_table", data); err != nil {
			h.logger.ErrorWithFields("Failed to render queue_table fragment", "error", err)
		}
		return
	}
	h.render(w, "queues_content", data)
}

// queueNamePattern validates queue names: letters, digits, hyphens, underscores, starting with a letter or digit.
var queueNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)

// New renders the create queue form.
func (h *QueuesHandler) New(w http.ResponseWriter, r *http.Request) {
	h.render(w, "queue_new_content", map[string]any{
		"PageTitle": "New Queue",
		"Active":    "queues",
	})
}

// Create handles queue creation (HTMX POST).
func (h *QueuesHandler) Create(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.logger.ErrorWithFields("Failed to parse form", "error", err)
		h.writeFormError(w, "Invalid form data")
		return
	}

	name := strings.TrimSpace(r.FormValue("name"))
	queueType := r.FormValue("type")
	exclusivityKey := strings.TrimSpace(r.FormValue("exclusivity_key"))
	leaseDuration := strings.TrimSpace(r.FormValue("lease_duration"))
	dlqName := strings.TrimSpace(r.FormValue("dlq_name"))
	autoCreateDLQ := r.FormValue("auto_create_dlq") == "true"
	maxAttemptsStr := r.FormValue("default_max_attempts")

	if name == "" {
		h.writeFormError(w, "Queue name is required")
		return
	}
	if !queueNamePattern.MatchString(name) {
		h.writeFormError(w, "Queue name may only contain letters, digits, hyphens, and underscores, and must start with a letter or digit")
		return
	}
	if queueType == "exclusive" && exclusivityKey == "" {
		h.writeFormError(w, "Exclusivity key is required for exclusive queues")
		return
	}
	if leaseDuration == "" {
		leaseDuration = "30s"
	}
	if _, err := time.ParseDuration(leaseDuration); err != nil {
		h.writeFormError(w, fmt.Sprintf("Invalid lease duration %q — use Go duration syntax, e.g. 30s, 5m, 1h", leaseDuration))
		return
	}

	maxAttempts := int32(3)
	if maxAttemptsStr != "" {
		v, err := strconv.ParseInt(maxAttemptsStr, 10, 32)
		if err != nil || v < 1 {
			h.writeFormError(w, "Max attempts must be a positive integer")
			return
		}
		maxAttempts = int32(v)
	}

	if autoCreateDLQ && dlqName == "" {
		dlqName = name + "-dlq"
	}

	opts := client.QueueOptions{
		Type:                client.ParseQueueType(queueType),
		DequeueAttempts:     maxAttempts,
		LeaseDuration:       leaseDuration,
		ExclusivityKey:      exclusivityKey,
		DeadLetterQueueName: dlqName,
		AutoCreateDLQ:       autoCreateDLQ,
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if _, err := h.activeClient().CreateQueue(ctx, name, opts); err != nil {
		h.logger.ErrorWithFields("Failed to create queue", "error", err, "queue", name)
		h.writeFormError(w, fmt.Sprintf("Failed to create queue: %v", err))
		return
	}

	w.Header().Set("HX-Redirect", "/queues")
	w.WriteHeader(http.StatusCreated)
	if _, err := w.Write([]byte("Queue created successfully")); err != nil {
		h.logger.Error("Failed to write response", "error", err)
	}
}

// writeFormError writes an inline error fragment into the HTMX form result target.
func (h *QueuesHandler) writeFormError(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusBadRequest)
	escaped := html.EscapeString(message)
	_, _ = fmt.Fprintf(w, `<div class="rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-300">%s</div>`, escaped)
}

// Detail renders the queue detail page with message list.
func (h *QueuesHandler) Detail(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")
	if queueName == "" {
		h.renderError(w, http.StatusBadRequest, "Queue name required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	stateResp, err := h.activeClient().GetQueueState(ctx, queueName)
	if err != nil {
		h.logger.ErrorWithFields("Failed to get queue state", "error", err, "queue", queueName)
		h.renderError(w, http.StatusInternalServerError, "Failed to load queue")
		return
	}
	counts := stateResp.GetStateCounts()

	peekResp, err := h.activeClient().PeekQueueMessages(ctx, queueName, 100, client.TimeRangeOption{})
	if err != nil {
		h.logger.ErrorWithFields("Failed to peek queue messages", "error", err, "queue", queueName)
	}

	var messages []MessageDisplay
	if peekResp != nil {
		for _, msg := range peekResp.GetMessages() {
			if msg == nil {
				continue
			}
			meta := msg.GetMetadata()
			if meta == nil {
				continue
			}
			createdAt := time.Now()
			var deliverAt *time.Time
			if ts := meta.GetScheduledTime(); ts != nil {
				t := ts.AsTime()
				if t.After(time.Now().UTC().Add(5 * time.Second)) {
					// scheduled_time is meaningfully in the future → this is a delayed message
					deliverAt = &t
				} else {
					createdAt = t
				}
			}
			attemptCount := meta.GetMaxAttempts() - meta.GetAttemptsLeft()
			if attemptCount < 0 {
				attemptCount = 0
			}
			messages = append(messages, MessageDisplay{
				Id:           msg.GetMessageId(),
				ShortId:      shortenID(msg.GetMessageId()),
				State:        meta.GetState().String(),
				Priority:     meta.GetPriority(),
				AttemptCount: attemptCount,
				CreatedAt:    createdAt,
				DeliverAt:    deliverAt,
			})
		}
	}

	queue := QueueDetail{
		Name:      queueName,
		Ready:     fmt.Sprintf("%d", counts["PENDING"]),
		InFlight:  fmt.Sprintf("%d", counts["RUNNING"]),
		Retries:   fmt.Sprintf("%d", counts["ERRORED"]),
		DLQ:       "0",
		Completed: fmt.Sprintf("%d", counts["COMPLETED"]),
		IsDLQ:     isDLQ(queueName),
	}

	data := map[string]any{
		"PageTitle":     "Queue: " + queueName,
		"Active":        "queues",
		"Queue":         queue,
		"QueueMessages": messages,
	}
	h.render(w, "queue_detail_content", data)
}

// NewMessage renders the post-message form for a queue.
func (h *QueuesHandler) NewMessage(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")
	if queueName == "" {
		h.renderError(w, http.StatusBadRequest, "Queue name required")
		return
	}
	h.render(w, "queue_message_new_content", map[string]any{
		"PageTitle": "New Message — " + queueName,
		"Active":    "queues",
		"QueueName": queueName,
	})
}

// PostMessage handles message creation for a queue (HTMX POST).
func (h *QueuesHandler) PostMessage(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")
	if queueName == "" {
		h.writeFormError(w, "Queue name required")
		return
	}

	if err := r.ParseForm(); err != nil {
		h.logger.ErrorWithFields("Failed to parse form", "error", err)
		h.writeFormError(w, "Invalid form data")
		return
	}

	messageID := strings.TrimSpace(r.FormValue("message_id"))
	if messageID == "" {
		messageID = uuid.New().String()
	}

	payloadRaw := strings.TrimSpace(r.FormValue("payload_data"))
	if payloadRaw == "" {
		h.writeFormError(w, "Payload is required")
		return
	}
	var payloadMap map[string]any
	if err := json.Unmarshal([]byte(payloadRaw), &payloadMap); err != nil {
		h.writeFormError(w, fmt.Sprintf("Invalid JSON payload: %v", err))
		return
	}
	payloadStruct, err := structpb.NewStruct(payloadMap)
	if err != nil {
		h.writeFormError(w, fmt.Sprintf("Failed to build payload: %v", err))
		return
	}

	var maxAttempts int32
	if v := strings.TrimSpace(r.FormValue("max_attempts")); v != "" {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil || n < 1 {
			h.writeFormError(w, "Max attempts must be a positive integer")
			return
		}
		maxAttempts = int32(n)
	}

	var priority int64
	if v := strings.TrimSpace(r.FormValue("priority")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			h.writeFormError(w, "Priority must be a non-negative integer")
			return
		}
		if n > 4 {
			h.writeFormError(w, "Priority must be between 0 and 4")
			return
		}
		priority = n
	}

	leaseDuration := strings.TrimSpace(r.FormValue("lease_duration"))
	if leaseDuration != "" {
		if _, err := time.ParseDuration(leaseDuration); err != nil {
			h.writeFormError(w, fmt.Sprintf("Invalid lease duration %q — use Go duration syntax, e.g. 30s, 5m", leaseDuration))
			return
		}
	}

	contentType := strings.TrimSpace(r.FormValue("content_type"))

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
			h.writeFormError(w, fmt.Sprintf("Invalid deliver-at time %q — expected YYYY-MM-DDTHH:MM", v))
			return
		}
		parsed = parsed.UTC()
		if !parsed.After(time.Now().UTC()) {
			h.writeFormError(w, "Deliver at must be a future time")
			return
		}
		scheduledTime = &parsed
	}

	opts := client.MessageOptions{
		Payload: client.Payload{
			Data:        payloadStruct,
			ContentType: contentType,
		},
		MaxAttempts:   maxAttempts,
		AttemptsLeft:  maxAttempts,
		Priority:      priority,
		LeaseDuration: leaseDuration,
		ScheduledTime: scheduledTime,
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if _, err := h.activeClient().PostMessage(ctx, queueName, messageID, opts); err != nil {
		h.logger.ErrorWithFields("Failed to post message", "error", err, "queue", queueName)
		h.writeFormError(w, fmt.Sprintf("Failed to post message: %v", err))
		return
	}

	w.Header().Set("HX-Redirect", "/queues/"+html.EscapeString(queueName))
	w.WriteHeader(http.StatusCreated)
	if _, err := w.Write([]byte("Message posted")); err != nil {
		h.logger.Error("Failed to write response", "error", err)
	}
}

// MessageDetail returns the modal HTML for a specific message (HTMX partial).
func (h *QueuesHandler) MessageDetail(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")
	messageID := r.URL.Query().Get("message_id")

	if messageID == "" {
		http.Error(w, "message_id required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	peekResp, err := h.activeClient().PeekQueueMessages(ctx, queueName, 100, client.TimeRangeOption{})
	if err != nil {
		h.logger.ErrorWithFields("Failed to peek messages", "error", err, "queue", queueName)
		http.Error(w, "Failed to load message", http.StatusInternalServerError)
		return
	}

	for _, msg := range peekResp.GetMessages() {
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
	if !isDLQ(queueName) {
		http.Error(w, "Not a DLQ", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	sourceQueue := strings.TrimSuffix(strings.TrimSuffix(queueName, "-dlq"), "_dlq")
	const pageSize = int32(100)
	requeued := 0
	for {
		peekResp, err := h.activeClient().PeekQueueMessages(ctx, queueName, pageSize, client.TimeRangeOption{})
		if err != nil {
			h.logger.ErrorWithFields("Failed to peek DLQ messages", "error", err, "queue", queueName)
			http.Error(w, "Failed to load DLQ messages", http.StatusInternalServerError)
			return
		}
		msgs := peekResp.GetMessages()
		if len(msgs) == 0 {
			break
		}
		for _, msg := range msgs {
			if msg == nil {
				continue
			}
			if _, err := h.activeClient().RequeueFromDLQ(ctx, queueName, msg.GetMessageId(), sourceQueue); err != nil {
				h.logger.ErrorWithFields("Failed to requeue message", "error", err, "queue", queueName, "message", msg.GetMessageId())
				continue
			}
			requeued++
		}
		if len(msgs) < int(pageSize) {
			break
		}
	}

	h.logger.InfoWithFields("Requeued messages from DLQ", "queue", queueName, "count", requeued)
	http.Redirect(w, r, "/queues/"+queueName, http.StatusSeeOther)
}

// Purge removes all messages from a queue (DLQ only).
func (h *QueuesHandler) Purge(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if !isDLQ(queueName) {
		http.Error(w, "Purge is only supported for DLQ queues", http.StatusBadRequest)
		return
	}

	if _, err := h.activeClient().PurgeDLQ(ctx, queueName); err != nil {
		h.logger.ErrorWithFields("Failed to purge DLQ", "error", err, "queue", queueName)
		http.Error(w, "Failed to purge queue", http.StatusInternalServerError)
		return
	}

	h.logger.InfoWithFields("DLQ purged", "queue", queueName)
	http.Redirect(w, r, "/queues/"+queueName, http.StatusSeeOther)
}
