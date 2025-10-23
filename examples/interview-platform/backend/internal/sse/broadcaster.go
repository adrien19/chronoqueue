package sse

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

// Event types
const (
	EventInterviewCreated   = "interview_created"
	EventInterviewScheduled = "interview_scheduled"
	EventInterviewUpdated   = "interview_updated"
	EventEvaluationCreated  = "evaluation_created"
	EventEvaluationUpdated  = "evaluation_updated"
	EventReportGenerated    = "report_generated"
	EventNotificationSent   = "notification_sent"
)

// Event represents a server-sent event
type Event struct {
	Type      string                 `json:"type"`
	Data      map[string]interface{} `json:"data"`
	Timestamp time.Time              `json:"timestamp"`
}

// Client represents a connected SSE client
type Client struct {
	ID        string
	Channel   chan Event
	Connected bool
	mu        sync.Mutex
}

// Broadcaster manages SSE clients and broadcasts events
type Broadcaster struct {
	clients map[string]*Client
	mu      sync.RWMutex
}

// NewBroadcaster creates a new SSE broadcaster
func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		clients: make(map[string]*Client),
	}
}

// AddClient registers a new SSE client
func (b *Broadcaster) AddClient(id string) *Client {
	b.mu.Lock()
	defer b.mu.Unlock()

	client := &Client{
		ID:        id,
		Channel:   make(chan Event, 10), // Buffer to prevent blocking
		Connected: true,
	}

	b.clients[id] = client
	log.Printf("[SSE] Client connected: %s (total: %d)", id, len(b.clients))

	return client
}

// RemoveClient removes a disconnected client
func (b *Broadcaster) RemoveClient(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if client, exists := b.clients[id]; exists {
		client.mu.Lock()
		client.Connected = false
		close(client.Channel)
		client.mu.Unlock()

		delete(b.clients, id)
		log.Printf("[SSE] Client disconnected: %s (total: %d)", id, len(b.clients))
	}
}

// Broadcast sends an event to all connected clients
func (b *Broadcaster) Broadcast(eventType string, data map[string]interface{}) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	event := Event{
		Type:      eventType,
		Data:      data,
		Timestamp: time.Now(),
	}

	log.Printf("[SSE] Broadcasting event: %s to %d clients", eventType, len(b.clients))

	for _, client := range b.clients {
		client.mu.Lock()
		if client.Connected {
			select {
			case client.Channel <- event:
				// Event sent successfully
			default:
				// Channel full, skip this client
				log.Printf("[SSE] Warning: Client %s channel full, skipping event", client.ID)
			}
		}
		client.mu.Unlock()
	}
}

// GetClientCount returns the number of connected clients
func (b *Broadcaster) GetClientCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}

// FormatSSE formats an event for SSE protocol
func FormatSSE(event Event) string {
	data, err := json.Marshal(event)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("event: %s\ndata: %s\n\n", event.Type, string(data))
}
