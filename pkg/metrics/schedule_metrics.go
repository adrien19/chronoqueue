package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Schedule metrics track scheduler performance and execution health
var (
	// scheduleExecutionsTotal tracks schedule trigger events
	// Status values: "success" (message created), "failed" (error during execution)
	// Use this to monitor scheduled job reliability
	scheduleExecutionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chronoqueue_schedule_executions_total",
			Help: "Total number of schedule executions",
		},
		[]string{"schedule_id", "queue_name", "status"},
	)

	cronScheduleExecutionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chronoqueue_cron_schedule_executions_total",
			Help: "Total number of cron schedule executions",
		},
		[]string{"schedule_id", "queue_name", "status"},
	)

	// scheduleActivationsTotal tracks messages transitioning from INVISIBLE to PENDING
	// This is the core scheduler operation - activating messages when scheduled_time arrives
	// High activation rate indicates heavy use of delayed/scheduled messages
	scheduleActivationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chronoqueue_schedule_activations_total",
			Help: "Total number of messages activated from INVISIBLE to PENDING by scheduler",
		},
		[]string{"queue_name"},
	)

	// scheduleLagSeconds measures how far behind schedule the scheduler is running
	// Negative values mean scheduler is ahead (activated before scheduled_time)
	// Positive values mean scheduler is lagging (activated after scheduled_time)
	// Values >60s indicate scheduler is falling behind and may need tuning
	scheduleLagSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "chronoqueue_schedule_lag_seconds",
			Help: "How far behind scheduled_time the scheduler is (negative = ahead, positive = behind)",
		},
		[]string{"queue_name"},
	)
)

// IncrementScheduleExecutions records a schedule execution attempt
// Status should be: "success" or "failed"
func IncrementScheduleExecutions(scheduleID, queueName, status string) {
	scheduleExecutionsTotal.WithLabelValues(scheduleID, queueName, status).Inc()
}

// IncrementCronScheduleExecutions records a cron schedule execution success
func IncrementCronScheduleExecutions(scheduleID, queueName string) {
	cronScheduleExecutionsTotal.WithLabelValues(scheduleID, queueName, "success").Inc()
}

// IncrementCronScheduleFailures records a cron schedule failure
func IncrementCronScheduleFailures(scheduleID, queueName string) {
	cronScheduleExecutionsTotal.WithLabelValues(scheduleID, queueName, "error").Inc()
}

// IncrementScheduleActivations records a message being activated by the scheduler
// Call this when scheduler moves a message from INVISIBLE to PENDING
func IncrementScheduleActivations(queueName string) {
	scheduleActivationsTotal.WithLabelValues(queueName).Inc()
}

// SetScheduleLag updates the scheduler lag metric for a queue
// lagSeconds should be: current_time - scheduled_time
// Negative values are normal (scheduler runs slightly ahead)
// Positive values indicate scheduler is behind schedule
func SetScheduleLag(queueName string, lagSeconds float64) {
	scheduleLagSeconds.WithLabelValues(queueName).Set(lagSeconds)
}
