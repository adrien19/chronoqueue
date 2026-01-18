package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Message lifecycle metrics track message state transitions and processing performance
var (
	// messageStateTransitions tracks all state changes for messages
	// Labels: queue_name, from_state, to_state
	// States: INVISIBLE, PENDING, RUNNING, COMPLETED, ERRORED, CANCELED
	messageStateTransitions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chronoqueue_message_state_transitions_total",
			Help: "Total number of message state transitions",
		},
		[]string{"queue_name", "from_state", "to_state"},
	)

	// messagesByState tracks the current count of messages in each state per queue
	// This is a gauge that should be updated periodically by querying GetQueueState
	messagesByState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "chronoqueue_messages_by_state",
			Help: "Number of messages in each state per queue",
		},
		[]string{"queue_name", "state"},
	)

	// messageClaimLatency measures how long it takes to claim a message from a queue
	// This includes the database query time and any locking/transaction overhead
	messageClaimLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "chronoqueue_message_claim_duration_seconds",
			Help: "Time taken to claim a message from queue",
			// Buckets optimized for typical claim operations (1ms to 5s)
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		},
		[]string{"queue_name"},
	)

	// messageProcessingDuration measures end-to-end message processing time
	// From claim (PENDING→RUNNING) to acknowledgment (RUNNING→COMPLETED)
	// This reflects how long workers take to process messages
	messageProcessingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "chronoqueue_message_processing_duration_seconds",
			Help: "Time from claim to acknowledgment (completed state)",
			// Buckets optimized for typical processing (100ms to 10min)
			Buckets: []float64{.1, .5, 1, 5, 10, 30, 60, 120, 300, 600},
		},
		[]string{"queue_name"},
	)
)

// RecordStateTransition records a message state change
// Use this whenever a message transitions between states
func RecordStateTransition(queueName, fromState, toState string) {
	messageStateTransitions.WithLabelValues(queueName, fromState, toState).Inc()
}

// SetMessagesByState updates the count of messages in a specific state for a queue
// This should be called periodically (e.g., every 30s) by querying GetQueueState
func SetMessagesByState(queueName, state string, count float64) {
	messagesByState.WithLabelValues(queueName, state).Set(count)
}

// ObserveMessageClaimLatency records how long it took to claim a message
// Call this after ClaimMessage operation completes
func ObserveMessageClaimLatency(queueName string, duration time.Duration) {
	messageClaimLatency.WithLabelValues(queueName).Observe(duration.Seconds())
}

// ObserveMessageProcessingDuration records how long a message took to process
// This requires tracking claim time and comparing to ack time
func ObserveMessageProcessingDuration(queueName string, duration time.Duration) {
	messageProcessingDuration.WithLabelValues(queueName).Observe(duration.Seconds())
}
