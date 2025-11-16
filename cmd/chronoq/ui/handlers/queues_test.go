package handlers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestShortenID tests the ID shortening utility function
func TestShortenID(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "long ID gets truncated",
			input:    "1234567890abcdefghijk",
			expected: "1234567890ab",
		},
		{
			name:     "short ID unchanged",
			input:    "short123",
			expected: "short123",
		},
		{
			name:     "exactly 12 chars unchanged",
			input:    "exactly12chr",
			expected: "exactly12chr",
		},
		{
			name:     "empty string returns empty",
			input:    "",
			expected: "",
		},
		{
			name:     "UUID-like ID truncated",
			input:    "550e8400-e29b-41d4-a716-446655440000",
			expected: "550e8400-e29",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shortenID(tt.input)
			assert.Equal(t, tt.expected, result)
			if len(tt.input) > 12 {
				assert.Equal(t, 12, len(result), "shortened ID should be exactly 12 chars")
			}
		})
	}
}

// TestMessageDisplay_Structure validates the MessageDisplay struct
func TestMessageDisplay_Structure(t *testing.T) {
	now := time.Now()

	md := MessageDisplay{
		Id:           "full-message-id-123456789",
		ShortId:      shortenID("full-message-id-123456789"),
		State:        "PENDING",
		Priority:     100,
		AttemptCount: 2,
		CreatedAt:    now,
	}

	assert.Equal(t, "full-message-id-123456789", md.Id)
	assert.Equal(t, "full-message", md.ShortId)
	assert.Equal(t, "PENDING", md.State)
	assert.Equal(t, int64(100), md.Priority)
	assert.Equal(t, int32(2), md.AttemptCount)
	assert.Equal(t, now, md.CreatedAt)
}

// TestQueueDisplay_Structure validates the QueueDisplay struct
func TestQueueDisplay_Structure(t *testing.T) {
	metadata := map[string]interface{}{
		"max_attempts":   3,
		"lease_duration": "30s",
		"default_dlq":    "orders-queue-dlq",
	}

	qd := QueueDisplay{
		Name:     "orders-queue",
		Metadata: metadata,
		IsDLQ:    false,
	}

	assert.Equal(t, "orders-queue", qd.Name)
	assert.Equal(t, metadata, qd.Metadata)
	assert.False(t, qd.IsDLQ)

	// Test DLQ queue
	dlqQueue := QueueDisplay{
		Name:     "orders-queue-dlq",
		Metadata: nil,
		IsDLQ:    isDLQ("orders-queue-dlq"),
	}

	assert.Equal(t, "orders-queue-dlq", dlqQueue.Name)
	assert.True(t, dlqQueue.IsDLQ)
}
