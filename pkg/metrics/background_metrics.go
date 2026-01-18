package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Background service metrics track scheduler, calendar, and reclaim service health
var (
	// backgroundServiceIterations tracks each iteration of background services
	// Service: "scheduler", "calendar", "reclaim"
	// Status: "success", "error"
	// Use this to detect if background services are running and healthy
	backgroundServiceIterations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chronoqueue_background_service_iterations_total",
			Help: "Total number of background service iterations",
		},
		[]string{"service", "status"},
	)

	// backgroundServiceProcessedMessages tracks messages handled by each service
	// Service: "scheduler" (activating INVISIBLE→PENDING), "calendar" (triggering schedule executions), "reclaim" (reclaiming expired leases)
	// This shows the actual work done by background services
	backgroundServiceProcessedMessages = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chronoqueue_background_service_processed_messages_total",
			Help: "Total messages processed by background services",
		},
		[]string{"service", "queue_name"},
	)

	// backgroundServiceIterationDuration measures how long each service iteration takes
	// Long durations may indicate the service is overloaded or database is slow
	backgroundServiceIterationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "chronoqueue_background_service_iteration_duration_seconds",
			Help: "Duration of background service iterations",
			// Buckets optimized for background tasks (10ms to 30s)
			Buckets: []float64{.01, .05, .1, .5, 1, 5, 10, 30},
		},
		[]string{"service"},
	)

	// messagesCleanedUpTotal tracks the total number of messages permanently deleted
	// by the cleanup service after their retention period expires.
	messagesCleanedUpTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "chronoqueue_messages_cleaned_up_total",
			Help: "Total number of messages permanently deleted after retention period",
		},
	)
)

// IncrementBackgroundServiceIterations records a background service iteration
// Service should be: "scheduler" or "reclaim"
// Status should be: "success" or "error"
func IncrementBackgroundServiceIterations(service, status string) {
	backgroundServiceIterations.WithLabelValues(service, status).Inc()
}

// IncrementBackgroundServiceProcessedMessages records a message processed by a background service
// Service should be: "scheduler" or "reclaim"
func IncrementBackgroundServiceProcessedMessages(service, queueName string) {
	backgroundServiceProcessedMessages.WithLabelValues(service, queueName).Inc()
}

// ObserveBackgroundServiceIterationDuration records how long a service iteration took
// Service should be: "scheduler" or "reclaim"
func ObserveBackgroundServiceIterationDuration(service string, durationSeconds float64) {
	backgroundServiceIterationDuration.WithLabelValues(service).Observe(durationSeconds)
}

// RecordMessagesCleanedUp records the number of messages permanently deleted by cleanup service.
func RecordMessagesCleanedUp(count int64) {
	messagesCleanedUpTotal.Add(float64(count))
}
