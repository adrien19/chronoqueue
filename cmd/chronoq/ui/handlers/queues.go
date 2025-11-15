package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"time"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/pkg/log"
)

// QueuesHandler handles queue-related requests
type QueuesHandler struct {
	BaseHandler
}

// MessageDisplay represents a simplified message for template rendering
type MessageDisplay struct {
	Id           string
	ShortId      string // Shortened ID for display (first 12 chars)
	State        string
	Priority     int64
	AttemptCount int32
	CreatedAt    time.Time
}

// shortenID truncates an ID to the first 12 characters (similar to Docker)
func shortenID(id string) string {
	if len(id) <= 12 {
		return id
	}
	return id[:12]
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

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Fetch queue state
	stateResp, err := h.client.GetQueueState(ctx, queueName)
	if err != nil {
		h.logger.ErrorWithFields("Failed to get queue state", "error", err, "queue", queueName)
		h.renderError(w, http.StatusInternalServerError, "Failed to load queue state")
		return
	}

	// Extract state counts (state_counts is a map[string]int32)
	stateCounts := stateResp.GetStateCounts()
	pendingCount := stateCounts["PENDING"]
	runningCount := stateCounts["RUNNING"]
	completedCount := stateCounts["COMPLETED"]
	failedCount := stateCounts["ERRORED"]

	// Parse limit from query parameter (default 20)
	limit := int32(20)
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsedLimit, err := strconv.ParseInt(limitStr, 10, 32); err == nil {
			limit = int32(parsedLimit)
			if limit <= 0 {
				limit = 20
			} else if limit > 1000 {
				limit = 1000 // Cap at 1000
			}
		}
	}

	// Fetch recent messages with specified limit
	peekResp, err := h.client.PeekQueueMessages(ctx, queueName, limit, client.TimeRangeOption{})
	if err != nil {
		h.logger.ErrorWithFields("Failed to peek messages", "error", err, "queue", queueName)
		// Don't fail the whole page, just log the error
		peekResp = nil
	}

	// Log for debugging
	if peekResp != nil {
		h.logger.DebugWithFields("Peeked messages", "count", len(peekResp.GetMessages()), "queue", queueName)
	}

	// Transform messages to display format
	messages := []MessageDisplay{}
	if peekResp != nil {
		for _, msg := range peekResp.GetMessages() {
			// Skip if message or metadata is nil
			if msg == nil {
				h.logger.WarnWithFields("Skipping nil message", "queue", queueName)
				continue
			}

			messageID := msg.GetMessageId()
			metadata := msg.GetMetadata()

			if metadata == nil {
				h.logger.WarnWithFields("Skipping message with nil metadata", "queue", queueName, "messageId", messageID)
				continue
			}

			// Determine creation time (use scheduled_time if available)
			createdAt := time.Now()
			if scheduledTime := metadata.GetScheduledTime(); scheduledTime != nil {
				createdAt = scheduledTime.AsTime()
			}

			// Calculate attempt count
			attemptCount := metadata.GetMaxAttempts() - metadata.GetAttemptsLeft()
			if attemptCount < 0 {
				attemptCount = 0
			}

			messages = append(messages, MessageDisplay{
				Id:           messageID,
				ShortId:      shortenID(messageID),
				State:        metadata.GetState().String(),
				Priority:     metadata.GetPriority(),
				AttemptCount: attemptCount,
				CreatedAt:    createdAt,
			})
		}
	}

	data := map[string]interface{}{
		"Title":          "Queue: " + queueName,
		"CurrentPage":    "queues",
		"QueueName":      queueName,
		"PendingCount":   pendingCount,
		"RunningCount":   runningCount,
		"CompletedCount": completedCount,
		"FailedCount":    failedCount,
		"Messages":       messages,
	}

	h.renderTemplate(w, "queue_detail.html", data)
}

// Stats returns queue statistics for HTMX updates
func (h *QueuesHandler) Stats(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Fetch queue state
	stateResp, err := h.client.GetQueueState(ctx, queueName)
	if err != nil {
		h.logger.ErrorWithFields("Failed to get queue state", "error", err, "queue", queueName)
		http.Error(w, "Failed to load queue state", http.StatusInternalServerError)
		return
	}

	// Extract state counts
	stateCounts := stateResp.GetStateCounts()
	pendingCount := stateCounts["PENDING"]
	runningCount := stateCounts["RUNNING"]
	completedCount := stateCounts["COMPLETED"]
	failedCount := stateCounts["ERRORED"]

	// Prepare data for template
	data := map[string]interface{}{
		"PendingCount":   pendingCount,
		"RunningCount":   runningCount,
		"CompletedCount": completedCount,
		"FailedCount":    failedCount,
	}

	// Render the stats fragment using the base handler's renderComponent method
	h.renderComponent(w, "queue-stats-fragment", data)
}

// MessageDetail returns detailed information about a specific message (for modal display)
func (h *QueuesHandler) MessageDetail(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")
	messageID := r.URL.Query().Get("message_id")

	if messageID == "" {
		http.Error(w, "message_id parameter required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Peek messages and find the specific one
	peekResp, err := h.client.PeekQueueMessages(ctx, queueName, 100, client.TimeRangeOption{})
	if err != nil {
		h.logger.ErrorWithFields("Failed to peek messages", "error", err, "queue", queueName)
		http.Error(w, "Failed to load message details", http.StatusInternalServerError)
		return
	}

	// Find the specific message
	var foundMsg *struct {
		ID      string
		Payload string
	}

	for _, msg := range peekResp.GetMessages() {
		if msg.GetMessageId() == messageID {
			metadata := msg.GetMetadata()

			// Format payload as JSON
			payload := "{}"
			if p := metadata.GetPayload(); p != nil {
				if data := p.GetData(); data != nil {
					dataMap := data.AsMap()

					// Check if "data" field contains a nested JSON string
					if dataStr, ok := dataMap["data"].(string); ok {
						// Try to parse the nested JSON string
						var nestedData interface{}
						if err := json.Unmarshal([]byte(dataStr), &nestedData); err == nil {
							// Successfully parsed nested JSON, use it instead
							jsonBytes, err := json.MarshalIndent(nestedData, "", "  ")
							if err == nil {
								payload = string(jsonBytes)
							}
						} else {
							// Not valid JSON, just format the outer structure
							jsonBytes, err := json.MarshalIndent(dataMap, "", "  ")
							if err == nil {
								payload = string(jsonBytes)
							}
						}
					} else {
						// No nested JSON string, format as-is
						jsonBytes, err := json.MarshalIndent(dataMap, "", "  ")
						if err == nil {
							payload = string(jsonBytes)
						}
					}
				}
			}

			foundMsg = &struct {
				ID      string
				Payload string
			}{
				ID:      messageID,
				Payload: payload,
			}
			break
		}
	}

	if foundMsg == nil {
		http.Error(w, "Message not found", http.StatusNotFound)
		return
	}

	// Return modal HTML
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	html := fmt.Sprintf(`
		<div class="fixed inset-0 bg-gray-600 bg-opacity-50 flex items-center justify-center z-50" onclick="closeModal(event)">
			<div class="bg-white rounded-lg shadow-xl max-w-3xl w-full mx-4 max-h-[90vh] overflow-y-auto" onclick="event.stopPropagation()">
				<!-- Header -->
				<div class="px-6 py-4 border-b border-gray-200 flex justify-between items-center">
					<h3 class="text-lg font-semibold text-gray-900">Message Details</h3>
					<button onclick="closeModal()" class="text-gray-400 hover:text-gray-600">
						<svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
							<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/>
						</svg>
					</button>
				</div>

				<!-- Content -->
				<div class="px-6 py-4 space-y-4">
					<!-- Message ID -->
					<div>
						<label class="text-sm font-medium text-gray-700 mb-2 block">Message ID</label>
						<div class="flex items-center space-x-2 bg-gray-50 p-3 rounded">
							<code class="text-xs font-mono text-gray-800 flex-1 break-all">%s</code>
							<button onclick="copyToClipboard('%s')" class="text-gray-400 hover:text-gray-600 flex-shrink-0" title="Copy ID">
								<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
									<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"/>
								</svg>
							</button>
						</div>
					</div>

					<!-- Payload -->
					<div>
						<label class="text-sm font-medium text-gray-700 mb-2 block">Payload</label>
						<div class="bg-gray-900 p-4 rounded overflow-auto max-h-96" style="word-break: break-word;">
							<pre class="text-xs text-green-400 font-mono whitespace-pre-wrap break-words" style="overflow-wrap: break-word;">%s</pre>
						</div>
					</div>
				</div>

				<!-- Footer -->
				<div class="px-6 py-4 border-t border-gray-200 flex justify-end">
					<button onclick="closeModal()" class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
						Close
					</button>
				</div>
			</div>
		</div>

		<script>
		function closeModal(event) {
			if (!event || event.target === event.currentTarget) {
				document.getElementById('message-modal').innerHTML = '';
			}
		}
		</script>
	`, foundMsg.ID, foundMsg.ID, foundMsg.Payload)

	if _, err := w.Write([]byte(html)); err != nil {
		h.logger.ErrorWithFields("Failed to write response", "error", err)
	}
}
