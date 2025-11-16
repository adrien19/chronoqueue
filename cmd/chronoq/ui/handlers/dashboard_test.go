package handlers

import (
	"html/template"
	"testing"
	"time"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/pkg/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDashboardHandler(t *testing.T) {
	tmpl := template.New("test")
	logger := log.NewLogger()
	mockClient := &client.ChronoQueueClient{}

	handler := NewDashboardHandler(tmpl, mockClient, logger)

	require.NotNil(t, handler)
	assert.Equal(t, tmpl, handler.templates)
	assert.Equal(t, mockClient, handler.client)
	assert.Equal(t, logger, handler.logger)
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		expected string
	}{
		{
			name:     "seconds",
			duration: 30 * time.Second,
			expected: "30s",
		},
		{
			name:     "minutes",
			duration: 5 * time.Minute,
			expected: "5m",
		},
		{
			name:     "hours and minutes",
			duration: 2*time.Hour + 30*time.Minute,
			expected: "2h 30m",
		},
		{
			name:     "just hours",
			duration: 3 * time.Hour,
			expected: "3h 0m",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatDuration(tt.duration)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Note: TestDashboardHandler_aggregateMetrics requires a proper mock client
// which would need significant test infrastructure. Skipped for now.

func TestIsDLQ(t *testing.T) {
	tests := []struct {
		name      string
		queueName string
		expected  bool
	}{
		{
			name:      "regular queue",
			queueName: "orders",
			expected:  false,
		},
		{
			name:      "DLQ queue",
			queueName: "orders-dlq",
			expected:  true,
		},
		{
			name:      "DLQ with different suffix",
			queueName: "notifications_dlq",
			expected:  true,
		},
		{
			name:      "queue containing dlq but not as suffix",
			queueName: "dlq-processor",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isDLQ(tt.queueName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Note: TestDashboardHandler_Metrics requires a proper mock client.
// Integration tests cover this functionality.

func TestProblemQueue_Structure(t *testing.T) {
	pq := ProblemQueue{
		Name:     "test-queue",
		Message:  "Test message",
		Severity: "high",
		Link:     "/queues/test-queue",
	}

	assert.Equal(t, "test-queue", pq.Name)
	assert.Equal(t, "Test message", pq.Message)
	assert.Equal(t, "high", pq.Severity)
	assert.Equal(t, "/queues/test-queue", pq.Link)
}

func TestDashboardMetrics_Structure(t *testing.T) {
	now := time.Now()
	metrics := DashboardMetrics{
		TotalQueues:    5,
		TotalPending:   100,
		TotalRunning:   50,
		TotalCompleted: 1000,
		TotalDLQ:       10,
		ProblemQueues: []ProblemQueue{
			{
				Name:     "queue1",
				Message:  "High DLQ count",
				Severity: "high",
				Link:     "/queues/queue1",
			},
		},
		Timestamp: now,
	}

	assert.Equal(t, 5, metrics.TotalQueues)
	assert.Equal(t, int64(100), metrics.TotalPending)
	assert.Equal(t, int64(50), metrics.TotalRunning)
	assert.Equal(t, int64(1000), metrics.TotalCompleted)
	assert.Equal(t, int64(10), metrics.TotalDLQ)
	assert.Len(t, metrics.ProblemQueues, 1)
	assert.Equal(t, now, metrics.Timestamp)
}

// Note: Full handler tests require integration testing with a real or mock gRPC server.
