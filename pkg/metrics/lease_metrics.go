package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Lease management metrics track lease renewals, expirations, and heartbeat health
var (
	// leaseRenewalsTotal tracks lease renewal attempts and outcomes
	// Status values: "success", "denied_max_renewals", "failed"
	// Use this to detect workers that are stuck renewing leases
	leaseRenewalsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chronoqueue_lease_renewals_total",
			Help: "Total number of lease renewals",
		},
		[]string{"queue_name", "status"},
	)

	// leaseExpirationsTotal tracks messages reclaimed due to expired leases
	// Expiry type indicates what expired: "lease" (lease_expiry) or "heartbeat" (heartbeat_expiry)
	// High lease expiration rate may indicate workers are crashing or overloaded
	leaseExpirationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chronoqueue_lease_expirations_total",
			Help: "Total number of expired leases reclaimed",
		},
		[]string{"queue_name", "expiry_type"},
	)

	// heartbeatTimeoutsTotal specifically tracks heartbeat timeouts
	// This is a subset of leaseExpirationsTotal but important for monitoring worker health
	// Workers are expected to send heartbeats regularly - timeouts indicate worker issues
	heartbeatTimeoutsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chronoqueue_heartbeat_timeouts_total",
			Help: "Total number of heartbeat timeouts",
		},
		[]string{"queue_name"},
	)
)

// IncrementLeaseRenewals records a lease renewal attempt
// Status should be: "success", "denied_max_renewals", or "failed"
func IncrementLeaseRenewals(queueName, status string) {
	leaseRenewalsTotal.WithLabelValues(queueName, status).Inc()
}

// IncrementLeaseExpirations records a message being reclaimed due to lease/heartbeat expiry
// Expiry type should be: "lease" or "heartbeat"
func IncrementLeaseExpirations(queueName, expiryType string) {
	leaseExpirationsTotal.WithLabelValues(queueName, expiryType).Inc()
}

// IncrementHeartbeatTimeouts records a heartbeat timeout event
// Call this when reclaim service finds a message with expired heartbeat_expiry
func IncrementHeartbeatTimeouts(queueName string) {
	heartbeatTimeoutsTotal.WithLabelValues(queueName).Inc()
}
