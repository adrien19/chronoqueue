# ChronoQueue Monitoring

This directory contains monitoring and observability configuration for ChronoQueue.

## Contents

- `grafana-dashboard.json`: Pre-built Grafana dashboard visualizing all ChronoQueue metrics
- `prometheus-alerts.yml`: Prometheus alerting rules for production monitoring

## Quick Start

### 1. Prometheus Configuration

Add ChronoQueue as a scrape target in your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'chronoqueue'
    static_configs:
      - targets: ['localhost:9090']  # Adjust to your ChronoQueue metrics port
    scrape_interval: 15s
```

Load the alerting rules:

```yaml
rule_files:
  - '/path/to/chronoqueue/monitoring/prometheus-alerts.yml'
```

Restart Prometheus to apply changes.

### 2. Grafana Dashboard

1. Open Grafana UI
2. Navigate to **Dashboards** → **Import**
3. Upload `grafana-dashboard.json` or paste its contents
4. Select your Prometheus datasource
5. Click **Import**

The dashboard will be available at: **ChronoQueue - Main Dashboard**

## Dashboard Panels

The Grafana dashboard includes the following sections:

### Overview

- **Total Queues**: Current queue count
- **Message Enqueue Rate**: Messages/sec being added to queues
- **Message Dequeue Rate**: Messages/sec being claimed from queues
- **Total Pending Messages**: Count of messages waiting to be processed

### Message State Distribution

- **Messages by State (Stacked)**: Visual breakdown of message counts across all states (INVISIBLE, PENDING, RUNNING, COMPLETED, ERRORED, CANCELED)

### Message Performance

- **Message Claim Latency**: P50/P95/P99 time to claim a message from queue
- **Message Processing Duration**: P50/P95/P99 end-to-end processing time

### Dead Letter Queue (DLQ)

- **Total DLQ Messages**: Messages currently in all DLQs
- **DLQ Ingestion Rate by Reason**: Why messages are moving to DLQ (max_attempts, processing_error, etc.)
- **DLQ Retry Rate**: Messages being retried from DLQ back to main queue

### Lease Management

- **Lease Renewals by Status**: Success/denied/failed renewal breakdown
- **Lease Expirations by Type**: Lease timeout vs heartbeat timeout

### Scheduler Performance

- **Scheduler Lag**: How far behind (or ahead) scheduled message activation is
- **Schedule Activation Rate**: INVISIBLE→PENDING transitions per second

### Background Services

- **Background Service Iterations**: Iteration rate for scheduler and reclaim services
- **Background Service Iteration Duration (P95)**: How long each iteration takes

### Database Performance

- **Database Query Duration (P95)**: Query latency by operation
- **Database Transaction Duration (P95)**: Transaction latency by operation
- **Database Connection Pool**: Active/idle/waiting connections

## Prometheus Alerts

The `prometheus-alerts.yml` file contains 20+ production-ready alerts organized into groups:

### chronoqueue_message_processing

- `HighPendingMessageCount`: Queue backlog exceeds 1000 messages
- `HighMessageClaimLatency`: P95 claim latency > 500ms
- `SlowMessageProcessing`: P95 processing duration > 5 minutes

### chronoqueue_dlq_health

- `HighDLQIngestionRate`: More than 10 messages/sec moving to DLQ
- `DLQMessageAccumulation`: DLQ has > 100 messages
- `PoisonMessagesDetected`: Messages hitting max_attempts repeatedly

### chronoqueue_lease_management

- `HighLeaseExpirationRate`: More than 5 leases expiring per second
- `FrequentHeartbeatTimeouts`: Workers not sending heartbeats
- `LeaseRenewalLimitReached`: Messages hitting max_renewals limit

### chronoqueue_scheduler

- `SchedulerLagging`: Scheduler is > 60 seconds behind schedule
- `SchedulerServiceStalled`: No scheduler iterations in 5 minutes
- `SchedulerIterationFailures`: Scheduler encountering errors
- `SlowSchedulerIterations`: P95 iteration time > 10 seconds

### chronoqueue_reclaim_service

- `ReclaimServiceStalled`: Reclaim service not running
- `ReclaimIterationFailures`: Reclaim encountering errors

### chronoqueue_database

- `SlowDatabaseQueries`: P95 query latency > 100ms
- `SlowDatabaseTransactions`: P95 transaction latency > 500ms
- `DatabaseConnectionPoolExhausted`: Connection pool has waiters
- `LowIdleConnections`: < 20% idle connections during high load

### chronoqueue_general_health

- `QueueCountDropped`: Queue count dropped to zero unexpectedly
- `NoMessageThroughput`: No message processing despite pending messages

## Alert Severity Levels

Alerts use standard severity labels:

- **critical**: Immediate action required, service degradation likely
- **warning**: Attention needed, potential issue developing
- **info**: Informational, investigate when convenient

## Metrics Reference

See [../pkg/metrics/README.md](../pkg/metrics/README.md) for complete metrics documentation.

## Customization

### Adjusting Alert Thresholds

Edit `prometheus-alerts.yml` to tune thresholds for your environment:

```yaml
# Example: Increase pending message threshold
expr: chronoqueue_messages_by_state{state="PENDING"} > 5000  # Changed from 1000
```

### Dashboard Time Ranges

Default dashboard shows last 1 hour with 10-second refresh. Adjust in the dashboard JSON:

```json
"refresh": "10s",
"time": {
  "from": "now-1h",
  "to": "now"
}
```

### Adding Custom Panels

1. Edit dashboard in Grafana UI
2. Add new panels using PromQL queries
3. Export updated JSON
4. Replace `grafana-dashboard.json`

## Production Deployment

### docker-compose Example

```yaml
version: '3.8'
services:
  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./monitoring/prometheus-alerts.yml:/etc/prometheus/alerts.yml
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana-storage:/var/lib/grafana
      - ./monitoring/grafana-dashboard.json:/etc/grafana/provisioning/dashboards/chronoqueue.json

  chronoqueue:
    # Your ChronoQueue service
    ports:
    ports:
      - "9000:9000"  # gRPC
      - "8080:8080"  # HTTP/REST API and metrics endpoint

volumes:
  grafana-storage:
```

### Kubernetes Example

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: chronoqueue-alerts
data:
  alerts.yml: |
    # Content of prometheus-alerts.yml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: chronoqueue-dashboard
data:
  dashboard.json: |
    # Content of grafana-dashboard.json
```

## Troubleshooting

### No Data in Grafana

1. Verify Prometheus is scraping ChronoQueue:

   ```bash
   curl http://localhost:8080/metrics | grep chronoqueue
   ```

2. Check Prometheus targets:
   - Open <http://localhost:9090/targets>
   - Ensure ChronoQueue target is "UP"

3. Verify datasource in Grafana:
   - Navigate to Configuration → Data Sources
   - Test Prometheus connection

### Alerts Not Firing

1. Check alert rules loaded in Prometheus:

   ```bash
   curl http://localhost:9090/api/v1/rules | jq '.data.groups[] | select(.name | contains("chronoqueue"))'
   ```

2. Verify alert expression in Prometheus:
   - Open <http://localhost:9090/alerts>
   - Check alert state and last evaluation

3. Ensure AlertManager is configured (if using)

### High Cardinality Issues

If you see performance degradation with many queues:

1. Limit metrics to specific queues:

   ```yaml
   metric_relabel_configs:
     - source_labels: [queue_name]
       regex: '(important-queue-1|important-queue-2)'
       action: keep
   ```

2. Increase Prometheus retention/storage
3. Use recording rules to pre-aggregate

## Support

For issues or questions:

- Check [ChronoQueue documentation](../README.md)
- Review [metrics documentation](../pkg/metrics/README.md)
- File an issue on GitHub
