package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/status"
)

// Prometheus metrics
var (
	// gRPC metrics
	grpcRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chronoqueue_grpc_requests_total",
			Help: "Total number of gRPC requests",
		},
		[]string{"method", "status_code"},
	)

	grpcRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "chronoqueue_grpc_request_duration_seconds",
			Help:    "Duration of gRPC requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)

	// HTTP Gateway metrics
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chronoqueue_http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status_code"},
	)

	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "chronoqueue_http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	// Business metrics
	queuesTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "chronoqueue_queues_total",
			Help: "Total number of queues",
		},
	)

	messagesEnqueued = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chronoqueue_messages_enqueued_total",
			Help: "Total number of messages enqueued",
		},
		[]string{"queue_name"},
	)

	messagesDequeued = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chronoqueue_messages_dequeued_total",
			Help: "Total number of messages dequeued",
		},
		[]string{"queue_name"},
	)

	messagesPending = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "chronoqueue_messages_pending",
			Help: "Number of pending messages in queues",
		},
		[]string{"queue_name"},
	)

	messagesValidatedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chronoqueue_messages_validated_total",
			Help: "Total number of messages validated successfully",
		},
		[]string{"queue_name"},
	)

	validationFailuresTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chronoqueue_validation_failures_total",
			Help: "Total number of message validation failures",
		},
		[]string{"queue_name", "reason"},
	)
)

// MetricsRegistry holds the Prometheus registry and provides methods for metrics
type MetricsRegistry struct {
	registry *prometheus.Registry
}

// NewMetricsRegistry creates a new metrics registry with all ChronoQueue metrics
func NewMetricsRegistry() *MetricsRegistry {
	registry := prometheus.NewRegistry()

	// Register HTTP/gRPC metrics
	registry.MustRegister(
		grpcRequestsTotal,
		grpcRequestDuration,
		httpRequestsTotal,
		httpRequestDuration,
	)

	// Register legacy business metrics
	registry.MustRegister(
		queuesTotal,
		messagesEnqueued,
		messagesDequeued,
		messagesPending,
		messagesValidatedTotal,
		validationFailuresTotal,
	)

	// Register message lifecycle metrics
	registry.MustRegister(
		messageStateTransitions,
		messagesByState,
		messageClaimLatency,
		messageProcessingDuration,
	)

	// Register DLQ metrics
	registry.MustRegister(
		dlqMessagesTotal,
		dlqIngestionRate,
		dlqRetryTotal,
	)

	// Register lease management metrics
	registry.MustRegister(
		leaseRenewalsTotal,
		leaseExpirationsTotal,
		heartbeatTimeoutsTotal,
	)

	// Register schedule metrics
	registry.MustRegister(
		scheduleExecutionsTotal,
		cronScheduleExecutionsTotal,
		scheduleActivationsTotal,
		scheduleLagSeconds,
	)

	// Register database metrics
	registry.MustRegister(
		dbQueryDuration,
		dbTransactionDuration,
		dbConnectionsActive,
		dbConnectionsIdle,
		dbConnectionsWait,
	)

	// Register background service metrics
	registry.MustRegister(
		backgroundServiceIterations,
		backgroundServiceProcessedMessages,
		backgroundServiceIterationDuration,
		messagesCleanedUpTotal,
	)

	return &MetricsRegistry{
		registry: registry,
	}
}

// Handler returns an HTTP handler for Prometheus metrics
func (m *MetricsRegistry) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// Business Metrics Methods

// IncrementQueuesTotal increments the total queue count
func IncrementQueuesTotal() {
	queuesTotal.Inc()
}

// DecrementQueuesTotal decrements the total queue count
func DecrementQueuesTotal() {
	queuesTotal.Dec()
}

// SetQueuesTotal sets the total queue count
func SetQueuesTotal(count float64) {
	queuesTotal.Set(count)
}

// IncrementMessagesEnqueued increments enqueued message count for a queue
func IncrementMessagesEnqueued(queueName string) {
	messagesEnqueued.WithLabelValues(queueName).Inc()
}

// IncrementMessagesDequeued increments dequeued message count for a queue
func IncrementMessagesDequeued(queueName string) {
	messagesDequeued.WithLabelValues(queueName).Inc()
}

// IncrementMessagesValidated increments validated message count for a queue
func IncrementMessagesValidated(queueName string) {
	messagesValidatedTotal.WithLabelValues(queueName).Inc()
}

// IncrementValidationFailures increments validation failure count for a queue with reason
func IncrementValidationFailures(queueName string, reason string) {
	validationFailuresTotal.WithLabelValues(queueName, reason).Inc()
}

// SetMessagesPending sets the pending message count for a queue
func SetMessagesPending(queueName string, count float64) {
	messagesPending.WithLabelValues(queueName).Set(count)
}

// HTTP Metrics Middleware

// HTTPMetricsMiddleware wraps an HTTP handler with Prometheus metrics
func HTTPMetricsMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Create a response writer that captures the status code
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// Call the original handler
		handler.ServeHTTP(rw, r)

		// Record metrics
		duration := time.Since(start)
		statusCode := strconv.Itoa(rw.statusCode)

		httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path, statusCode).Inc()
		httpRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(duration.Seconds())
	})
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// RecordGRPCMetrics records gRPC request metrics (called from the interceptor)
func RecordGRPCMetrics(method string, duration time.Duration, err error) {
	statusCode := "OK"
	if err != nil {
		if s, ok := status.FromError(err); ok {
			statusCode = s.Code().String()
		} else {
			statusCode = "Unknown"
		}
	}

	grpcRequestsTotal.WithLabelValues(method, statusCode).Inc()
	grpcRequestDuration.WithLabelValues(method).Observe(duration.Seconds())
}
