# ChronoQueue Metrics Package

This package provides comprehensive Prometheus metrics for monitoring ChronoQueue operations.

## Structure

The metrics are organized into separate files by domain:

### Core Files

- **metrics.go** - Base infrastructure, HTTP/gRPC metrics, and legacy business metrics
- **message_metrics.go** - Message lifecycle tracking (state transitions, processing latency)
- **dlq_metrics.go** - Dead letter queue health metrics
- **lease_metrics.go** - Lease renewal and expiration tracking
- **schedule_metrics.go** - Scheduler performance and execution metrics
- **database_metrics.go** - Database query performance and connection pool metrics
- **background_metrics.go** - Background service health metrics

## Metrics Categories

### Message Lifecycle Metrics

**State Transitions** (`chronoqueue_message_state_transitions_total`)

- Tracks all state changes: INVISIBLE→PENDING, PENDING→RUNNING, RUNNING→COMPLETED, etc.
- Labels: `queue_name`, `from_state`, `to_state`

**Message Counts by State** (`chronoqueue_messages_by_state`)

- Current count of messages in each state per queue
- Labels: `queue_name`, `state`
- Update periodically via `GetQueueState()`

**Claim Latency** (`chronoqueue_message_claim_duration_seconds`)

- Time to claim a message from queue
- Labels: `queue_name`
- Histogram buckets: 1ms to 5s

**Processing Duration** (`chronoqueue_message_processing_duration_seconds`)

- End-to-end processing time (claim to acknowledgment)
- Labels: `queue_name`
- Histogram buckets: 100ms to 10min

### DLQ Metrics

**DLQ Message Count** (`chronoqueue_dlq_messages_total`)

- Current number of messages in each DLQ
- Labels: `dlq_name`, `source_queue`

**DLQ Ingestion Rate** (`chronoqueue_dlq_ingestion_total`)

- Messages moved to DLQ with failure reason
- Labels: `dlq_name`, `source_queue`, `reason`
- Reasons: `max_attempts`, `lease_timeout`, `nack`

**DLQ Retry Count** (`chronoqueue_dlq_retry_total`)

- Messages retried from DLQ back to source queue
- Labels: `dlq_name`, `destination_queue`

### Lease Management Metrics

**Lease Renewals** (`chronoqueue_lease_renewals_total`)

- Renewal attempts and outcomes
- Labels: `queue_name`, `status`
- Status: `success`, `denied_max_renewals`, `failed`

**Lease Expirations** (`chronoqueue_lease_expirations_total`)

- Messages reclaimed due to expired leases
- Labels: `queue_name`, `expiry_type`
- Types: `lease`, `heartbeat`

**Heartbeat Timeouts** (`chronoqueue_heartbeat_timeouts_total`)

- Heartbeat timeout events
- Labels: `queue_name`

### Schedule Metrics

**Schedule Executions** (`chronoqueue_schedule_executions_total`)

- Schedule trigger events
- Labels: `schedule_id`, `queue_name`, `status`

**Schedule Activations** (`chronoqueue_schedule_activations_total`)

- Messages activated from INVISIBLE to PENDING
- Labels: `queue_name`

**Schedule Lag** (`chronoqueue_schedule_lag_seconds`)

- How far behind schedule the scheduler is running
- Labels: `queue_name`
- Negative = ahead, Positive = behind

### Database Metrics

**Query Duration** (`chronoqueue_db_query_duration_seconds`)

- Individual query execution time
- Labels: `backend` (sqlite/postgres), `operation`
- Histogram buckets: 0.1ms to 1s

**Transaction Duration** (`chronoqueue_db_transaction_duration_seconds`)

- Complete transaction execution time
- Labels: `backend`, `operation`
- Histogram buckets: 1ms to 5s

**Connection Pool Stats**

- `chronoqueue_db_connections_active` - Active connections
- `chronoqueue_db_connections_idle` - Idle connections
- `chronoqueue_db_connections_wait` - Connections waiting for availability
- Labels: `backend`

### Background Service Metrics

**Service Iterations** (`chronoqueue_background_service_iterations_total`)

- Iteration count per service
- Labels: `service` (scheduler/reclaim), `status` (success/error)

**Processed Messages** (`chronoqueue_background_service_processed_messages_total`)

- Messages handled by each service
- Labels: `service`, `queue_name`

**Iteration Duration** (`chronoqueue_background_service_iteration_duration_seconds`)

- How long each iteration takes
- Labels: `service`
- Histogram buckets: 10ms to 30s

## Usage Examples

### Recording Message State Transitions

```go
import "github.com/adrien19/chronoqueue/pkg/metrics"

// When claiming a message
metrics.RecordStateTransition(queueName, "PENDING", "RUNNING")

// When acknowledging
metrics.RecordStateTransition(queueName, "RUNNING", "COMPLETED")
```

### Recording DLQ Operations

```go
// Message moved to DLQ after max attempts
metrics.IncrementDLQIngestion(dlqName, sourceQueue, "max_attempts")

// Message retried from DLQ
metrics.IncrementDLQRetry(dlqName, targetQueue)
```

### Recording Lease Operations

```go
// Successful lease renewal
metrics.IncrementLeaseRenewals(queueName, "success")

// Lease renewal denied due to max_renewals limit
metrics.IncrementLeaseRenewals(queueName, "denied_max_renewals")

// Lease expired and message reclaimed
metrics.IncrementLeaseExpirations(queueName, "lease")
```

### Recording Database Operations

```go
start := time.Now()
// ... execute query ...
metrics.ObserveDBQuery("sqlite", "claim_message", time.Since(start))
```

### Recording Background Service Work

```go
// In scheduler service
metrics.IncrementBackgroundServiceIterations("scheduler", "success")
metrics.IncrementBackgroundServiceProcessedMessages("scheduler", queueName)
```

## Prometheus Queries

### Message Processing Rate

```promql
rate(chronoqueue_messages_enqueued_total[5m])
rate(chronoqueue_messages_dequeued_total[5m])
```

### P95 Message Claim Latency

```promql
histogram_quantile(0.95, rate(chronoqueue_message_claim_duration_seconds_bucket[5m]))
```

### DLQ Health

```promql
# Total messages in all DLQs
sum(chronoqueue_dlq_messages_total)

# DLQ ingestion rate by reason
rate(chronoqueue_dlq_ingestion_total[5m]) by (reason)
```

### Lease Expiration Rate

```promql
rate(chronoqueue_lease_expirations_total[5m]) by (expiry_type)
```

### Scheduler Performance

```promql
# Scheduler lag
chronoqueue_schedule_lag_seconds

# Activation rate
rate(chronoqueue_schedule_activations_total[5m])
```

### Database Performance

```promql
# P99 query latency
histogram_quantile(0.99, rate(chronoqueue_db_query_duration_seconds_bucket[5m])) by (operation)

# Connection pool utilization
chronoqueue_db_connections_active / (chronoqueue_db_connections_active + chronoqueue_db_connections_idle)
```

## Alerting Examples

```yaml
# High DLQ ingestion rate
- alert: HighDLQIngestionRate
  expr: rate(chronoqueue_dlq_ingestion_total[5m]) > 10
  for: 5m
  annotations:
    summary: "High DLQ ingestion for {{ $labels.source_queue }}"

# Scheduler falling behind
- alert: SchedulerLag
  expr: chronoqueue_schedule_lag_seconds > 60
  for: 2m
  annotations:
    summary: "Scheduler >60s behind for {{ $labels.queue_name }}"

# High lease expiration rate
- alert: HighLeaseExpirationRate
  expr: rate(chronoqueue_lease_expirations_total[5m]) > 5
  for: 3m
  annotations:
    summary: "High lease expiration for {{ $labels.queue_name }}"

# Database performance degradation
- alert: SlowDatabaseQueries
  expr: histogram_quantile(0.95, rate(chronoqueue_db_query_duration_seconds_bucket[5m])) > 0.5
  for: 5m
  annotations:
    summary: "P95 query latency >500ms for {{ $labels.operation }}"
```

## Integration Points

The repository layer should call these metric functions at appropriate points:

1. **Message operations** - `EnqueueMessage`, `ClaimMessage`, `AcknowledgeMessage`, `NackMessage`
2. **Lease operations** - `ExtendMessageLease`
3. **DLQ operations** - `RetryDLQMessage`, `DeleteDLQMessage`
4. **Background services** - Scheduler and Reclaim service iterations
5. **Periodic updates** - Queue state, DLQ counts, connection pool stats

