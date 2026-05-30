package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"html/template"
	"net/http"
	"strings"
	"time"

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
	h.render(w, "queues_content", data)
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
			if ts := meta.GetScheduledTime(); ts != nil {
				createdAt = ts.AsTime()
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

	peekResp, err := h.activeClient().PeekQueueMessages(ctx, queueName, 100, client.TimeRangeOption{})
	if err != nil {
		h.logger.ErrorWithFields("Failed to peek DLQ messages", "error", err, "queue", queueName)
		http.Error(w, "Failed to load DLQ messages", http.StatusInternalServerError)
		return
	}

	sourceQueue := strings.TrimSuffix(strings.TrimSuffix(queueName, "-dlq"), "_dlq")
	requeued := 0
	for _, msg := range peekResp.GetMessages() {
		if msg == nil {
			continue
		}
		if _, err := h.activeClient().RequeueFromDLQ(ctx, queueName, msg.GetMessageId(), sourceQueue); err != nil {
			h.logger.ErrorWithFields("Failed to requeue message", "error", err, "queue", queueName, "message", msg.GetMessageId())
			continue
		}
		requeued++
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
