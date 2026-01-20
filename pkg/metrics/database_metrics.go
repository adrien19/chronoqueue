package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Database metrics track query performance and connection health
var (
	// dbQueryDuration measures individual database query execution time
	// Backend: "sqlite", "postgres"
	// Operation: "claim_message", "enqueue_message", "ack_message", "get_queue_state", etc.
	dbQueryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "chronoqueue_db_query_duration_seconds",
			Help: "Duration of database queries",
			// Buckets optimized for database operations (0.1ms to 1s)
			Buckets: []float64{.0001, .0005, .001, .005, .01, .05, .1, .5, 1},
		},
		[]string{"backend", "operation"},
	)

	// dbTransactionDuration measures complete transaction execution time
	// This includes begin, queries, and commit/rollback
	// Higher than query duration due to transaction overhead
	dbTransactionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "chronoqueue_db_transaction_duration_seconds",
			Help: "Duration of database transactions",
			// Buckets optimized for transactions (1ms to 5s)
			Buckets: []float64{.001, .005, .01, .05, .1, .5, 1, 5},
		},
		[]string{"backend", "operation"},
	)

	// dbConnectionsActive tracks the current number of active database connections
	// Use sql.DBStats() to populate this metric periodically
	// High values may indicate connection pool exhaustion
	dbConnectionsActive = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "chronoqueue_db_connections_active",
			Help: "Number of active database connections",
		},
		[]string{"backend"},
	)

	// dbConnectionsIdle tracks idle connections in the pool
	// Low values during high load indicate pool sizing issues
	dbConnectionsIdle = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "chronoqueue_db_connections_idle",
			Help: "Number of idle database connections in pool",
		},
		[]string{"backend"},
	)

	// dbConnectionsWait tracks connections waiting for availability
	// Non-zero values indicate connection pool is too small
	dbConnectionsWait = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "chronoqueue_db_connections_wait",
			Help: "Number of connections waiting for availability",
		},
		[]string{"backend"},
	)
)

// ObserveDBQuery records a database query execution time
// Backend should be: "sqlite" or "postgres"
// Operation should be: "claim_message", "enqueue_message", etc.
func ObserveDBQuery(backend, operation string, duration time.Duration) {
	dbQueryDuration.WithLabelValues(backend, operation).Observe(duration.Seconds())
}

// ObserveDBTransaction records a database transaction execution time
// This should be called for the entire transaction (begin to commit/rollback)
func ObserveDBTransaction(backend, operation string, duration time.Duration) {
	dbTransactionDuration.WithLabelValues(backend, operation).Observe(duration.Seconds())
}

// SetDBConnectionsActive sets the current count of active database connections
// Call this periodically using sql.DB.Stats().OpenConnections
func SetDBConnectionsActive(backend string, count float64) {
	dbConnectionsActive.WithLabelValues(backend).Set(count)
}

// SetDBConnectionsIdle sets the current count of idle database connections
// Call this periodically using sql.DB.Stats().Idle
func SetDBConnectionsIdle(backend string, count float64) {
	dbConnectionsIdle.WithLabelValues(backend).Set(count)
}

// SetDBConnectionsWait sets the current count of connections waiting
// Call this periodically using sql.DB.Stats().WaitCount
func SetDBConnectionsWait(backend string, count float64) {
	dbConnectionsWait.WithLabelValues(backend).Set(count)
}
