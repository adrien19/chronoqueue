package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// DLQ metrics track dead letter queue health and message failures
var (
	// dlqMessagesTotal tracks the current number of messages in each DLQ
	// This is a gauge that should be updated periodically or after DLQ operations
	dlqMessagesTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "chronoqueue_dlq_messages_total",
			Help: "Total number of messages in dead letter queues",
		},
		[]string{"dlq_name", "source_queue"},
	)

	// dlqIngestionRate tracks messages being moved to DLQ
	// The reason label helps identify why messages failed
	// Reasons: max_attempts (exhausted retries), lease_timeout (worker died), nack (explicit failure)
	dlqIngestionRate = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chronoqueue_dlq_ingestion_total",
			Help: "Total number of messages moved to DLQ",
		},
		[]string{"dlq_name", "source_queue", "reason"},
	)

	// dlqRetryTotal tracks messages being retried from DLQ back to source queue
	// Use this to monitor DLQ recovery operations
	dlqRetryTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chronoqueue_dlq_retry_total",
			Help: "Total number of messages retried from DLQ",
		},
		[]string{"dlq_name", "destination_queue"},
	)
)

// SetDLQMessagesTotal sets the current count of messages in a DLQ
// Call this after DLQ operations or periodically via GetDLQStats
func SetDLQMessagesTotal(dlqName, sourceQueue string, count float64) {
	dlqMessagesTotal.WithLabelValues(dlqName, sourceQueue).Set(count)
}

// IncrementDLQIngestion records a message being moved to DLQ
// Reason should be: "max_attempts", "lease_timeout", or "nack"
func IncrementDLQIngestion(dlqName, sourceQueue, reason string) {
	dlqIngestionRate.WithLabelValues(dlqName, sourceQueue, reason).Inc()
}

// IncrementDLQRetry records a message being retried from DLQ
// Call this when RetryDLQMessage succeeds
func IncrementDLQRetry(dlqName, destinationQueue string) {
	dlqRetryTotal.WithLabelValues(dlqName, destinationQueue).Inc()
}
