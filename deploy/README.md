# ChronoQueue Deployment

This directory contains Docker Compose configurations for deploying ChronoQueue with different storage backends and monitoring stack.

## Files

- `docker-compose.postgres.yaml` - ChronoQueue with PostgreSQL storage (default, **instrumented**)
- `docker-compose.sqlite.yaml` - ChronoQueue with SQLite storage (**instrumented**)
- `docker-compose.redis.yaml` - ChronoQueue with Redis storage (**not instrumented**)
- `docker-compose.monitoring.yaml` - Monitoring stack (Prometheus + Grafana)
- `prometheus.yml` - Prometheus scrape configuration
- `grafana/` - Grafana provisioning configuration

## Storage Backend Selection

ChronoQueue supports three storage backends:

| Backend | Status | Metrics | Use Case |
|---------|--------|---------|----------|
| **PostgreSQL** | ✅ Recommended | ✅ Fully instrumented | Production, high availability |
| **SQLite** | ✅ Supported | ✅ Fully instrumented | Development, embedded deployments |
| **Redis** | ⚠️ Legacy | ❌ Not instrumented | Legacy support only |

**Note**: Only PostgreSQL and SQLite have full metrics instrumentation. Redis storage does not expose detailed metrics.

## Quick Start

### 1. Start ChronoQueue Services (PostgreSQL - Recommended)

```bash
# Using Makefile (recommended)
make deploy-up STORAGE=postgres

# Or directly with docker-compose
cd /workspaces/chronoqueue/deploy
docker-compose -f docker-compose.postgres.yaml up -d
```

This starts:

- **ChronoQueue Server** on ports:
  - `9000` - gRPC API
  - `8080` - HTTP/REST API
  - `8080` - HTTP/REST API and metrics endpoint
- **PostgreSQL** on port:
  - `5432` - PostgreSQL server

### Alternative: SQLite Storage

```bash
# Using Makefile
make deploy-up STORAGE=sqlite

# Or directly
cd /workspaces/chronoqueue/deploy
docker-compose -f docker-compose.sqlite.yaml up -d
```

No external database needed - data stored in volume at `/data/chronoqueue.db`.

### 2. Start Monitoring Stack

```bash
# Using Makefile
make monitoring-up

# Or directly
cd /workspaces/chronoqueue/deploy
docker-compose -f docker-compose.monitoring.yaml up -d
```

This starts:

- **Prometheus** on `http://localhost:9090`
- **Grafana** on `http://localhost:3000`

Default Grafana credentials:

- Username: `admin`
- Password: `admin`

### 3. Access Services

| Service | URL | Purpose |
|---------|-----|---------|
| Grafana | <http://localhost:3000> | Metrics visualization |
| Prometheus | <http://localhost:9090> | Metrics storage & queries |
| ChronoQueue REST API | <http://localhost:8080> | HTTP API |
| ChronoQueue Metrics | <http://localhost:8080/metrics> | Raw metrics endpoint |
| PostgreSQL | localhost:5432 | Database (postgres storage only) |

### 4. View ChronoQueue Dashboard

1. Open Grafana at <http://localhost:3000>
2. Login with `admin/admin`
3. Navigate to **Dashboards** → **ChronoQueue** folder → **ChronoQueue - Main Dashboard**

The dashboard is automatically provisioned on startup.

## Running Everything Together

Start all services in one command:

```bash
# Using Makefile (PostgreSQL storage)
make deploy-all STORAGE=postgres

# Or SQLite storage
make deploy-all STORAGE=sqlite

# Or manually
cd /workspaces/chronoqueue/deploy
docker-compose -f docker-compose.postgres.yaml up -d && \
docker-compose -f docker-compose.monitoring.yaml up -d
```

Stop all services:

```bash
# Using Makefile
make deploy-down STORAGE=postgres
make monitoring-down

# Or manually
docker-compose -f docker-compose.postgres.yaml down
docker-compose -f docker-compose.monitoring.yaml down
```

## Verifying the Setup

### Check ChronoQueue is Running

```bash
# Check health
curl http://localhost:8080/health

# Check metrics are exposed
curl http://localhost:8080/metrics | grep chronoqueue
```

### Check Prometheus is Scraping

1. Open <http://localhost:9090/targets>
2. Verify `chronoqueue` target shows as **UP**
3. Run a test query: `chronoqueue_queues_total`

### Check Grafana Dashboard

1. Open <http://localhost:3000>
2. Navigate to **Dashboards** → **ChronoQueue** → **ChronoQueue - Main Dashboard**
3. Verify panels are showing data (may take 30-60 seconds for first data points)

## Configuration

### Choosing a Storage Backend

Set the `STORAGE` environment variable or Makefile parameter:

```bash
# PostgreSQL (default, recommended for production)
make deploy-up STORAGE=postgres

# SQLite (good for development, embedded deployments)
make deploy-up STORAGE=sqlite

# Redis (legacy, not instrumented - not recommended)
make deploy-up STORAGE=redis
```

### PostgreSQL Configuration

Edit [`docker-compose.postgres.yaml`](./docker-compose.postgres.yaml):

```yaml
environment:
  - POSTGRES_HOST=postgres
  - POSTGRES_PORT=5432
  - POSTGRES_USER=chronoqueue
  - POSTGRES_PASSWORD=chronoqueue_dev_password  # Change for production!
  - POSTGRES_DATABASE=chronoqueue
  - POSTGRES_SSLMODE=disable  # Use 'require' for production
```

### SQLite Configuration

Edit [`docker-compose.sqlite.yaml`](./docker-compose.sqlite.yaml):

```yaml
environment:
  - SQLITE_DB_PATH=/data/chronoqueue.db
volumes:
  - sqlite-data:/data  # Persistent storage location
```

### Prometheus Configuration

Edit [`prometheus.yml`](./prometheus.yml) to:

- Adjust scrape intervals
- Add additional ChronoQueue instances
- Configure external labels

After changes, reload Prometheus:

```bash
docker-compose -f docker-compose.monitoring.yaml restart prometheus
```

### Grafana Configuration

Datasources and dashboards are auto-provisioned from:

- [`grafana/provisioning/datasources/prometheus.yml`](./grafana/provisioning/datasources/prometheus.yml)
- [`grafana/provisioning/dashboards/chronoqueue.yml`](./grafana/provisioning/dashboards/chronoqueue.yml)
- [`../monitoring/grafana-dashboard.json`](../monitoring/grafana-dashboard.json)

## Makefile Targets

The root Makefile provides convenient targets:

```bash
# Start services
make deploy-up STORAGE=postgres     # Start ChronoQueue with PostgreSQL
make deploy-up STORAGE=sqlite       # Start ChronoQueue with SQLite
make monitoring-up                   # Start monitoring stack

# Stop services
make deploy-down STORAGE=postgres    # Stop ChronoQueue
make monitoring-down                 # Stop monitoring

# View logs
make deploy-logs STORAGE=postgres    # ChronoQueue logs
make monitoring-logs                 # Monitoring logs

# All-in-one
make deploy-all STORAGE=postgres     # Start everything
make deploy-clean STORAGE=postgres   # Stop and remove volumes

# Rebuild
make deploy-rebuild STORAGE=postgres # Rebuild ChronoQueue

# Validate
make deploy-validate                 # Check monitoring stack
make deploy-status STORAGE=postgres  # Show service status
```

## ChronoQueue Environment Variables

Common across all storage backends:

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_MODE` | `development` | Server mode (development/production) |
| `STORAGE_TYPE` | varies | Storage backend (postgres/sqlite/redis) |
| `LOG_LEVEL` | `debug` | Log level (debug/info/warn/error) |
| `LOG_FORMAT` | `text` | Log format (text/json) |
| `ENABLE_ENCRYPTION` | `true` | Enable message encryption |
| `CHRONOQUEUE_TLS_ENABLED` | `false` | Enable TLS for gRPC |

### PostgreSQL-specific

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `postgres` | PostgreSQL hostname |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_USER` | `chronoqueue` | PostgreSQL username |
| `POSTGRES_PASSWORD` | `chronoqueue_dev_password` | PostgreSQL password |
| `POSTGRES_DATABASE` | `chronoqueue` | Database name |
| `POSTGRES_SSLMODE` | `disable` | SSL mode (disable/require/verify-full) |

### SQLite-specific

| Variable | Default | Description |
|----------|---------|-------------|
| `SQLITE_DB_PATH` | `/data/chronoqueue.db` | Path to SQLite database file |

### Redis-specific (legacy)

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_ADDR` | `redis_container:6379` | Redis address |
| `REDIS_PASSWORD` | empty | Redis password |
| `REDIS_DB` | `0` | Redis database number |

## Persistent Data

Volumes are automatically created for data persistence:

```bash
# List volumes
docker volume ls | grep chronoqueue

# Volumes created (depending on storage backend):
# PostgreSQL:
#   - postgres-data (PostgreSQL database)
# SQLite:
#   - sqlite-data (SQLite database file)
# Redis:
#   - deploy_redis_data (Redis data)
# Monitoring:
#   - prometheus-data (Prometheus time-series data)
#   - grafana-data (Grafana dashboards, users, settings)
```

### Backup Data

```bash
# Backup PostgreSQL
docker run --rm -v postgres-data:/data -v $(pwd):/backup ubuntu tar czf /backup/postgres-backup.tar.gz -C /data .

# Backup SQLite
docker run --rm -v sqlite-data:/data -v $(pwd):/backup ubuntu tar czf /backup/sqlite-backup.tar.gz -C /data .

# Backup Prometheus data
docker run --rm -v prometheus-data:/data -v $(pwd):/backup ubuntu tar czf /backup/prometheus-backup.tar.gz -C /data .

# Backup Grafana data
docker run --rm -v grafana-data:/data -v $(pwd):/backup ubuntu tar czf /backup/grafana-backup.tar.gz -C /data .
```

### Restore Data

```bash
# Restore PostgreSQL
docker run --rm -v postgres-data:/data -v $(pwd):/backup ubuntu tar xzf /backup/postgres-backup.tar.gz -C /data

# Similar for other volumes
```

## Alerting

Prometheus alert rules are loaded from [`../monitoring/prometheus-alerts.yml`](../monitoring/prometheus-alerts.yml).

View active alerts:

- Prometheus: <http://localhost:9090/alerts>
- Grafana: Navigate to **Alerting** → **Alert rules**

### Adding AlertManager (Optional)

To send alert notifications (email, Slack, PagerDuty):

1. Add AlertManager service to `docker-compose.monitoring.yaml`:

```yaml
  alertmanager:
    image: prom/alertmanager:v0.26.0
    container_name: chronoqueue-alertmanager
    ports:
      - "9093:9093"
    volumes:
      - ./alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro
      - alertmanager-data:/alertmanager
    networks:
      - chronoqueue-network
    restart: unless-stopped
```

1. Uncomment the `alerting` section in `prometheus.yml`

2. Create `alertmanager.yml` with your notification channels

## Troubleshooting

### No Metrics in Grafana

1. **Check ChronoQueue metrics endpoint**:

   ```bash
   curl http://localhost:8080/metrics
   ```

   Should return Prometheus metrics

2. **Verify storage backend is instrumented**:
   - ✅ PostgreSQL and SQLite have full instrumentation
   - ❌ Redis does NOT have metrics instrumentation
   - If using Redis, switch to PostgreSQL or SQLite for metrics

3. **Check Prometheus targets**:
   - Visit <http://localhost:9090/targets>
   - Ensure `chronoqueue` target is UP
   - Check for scrape errors

4. **Check Grafana datasource**:
   - Navigate to Configuration → Data Sources
   - Test the Prometheus connection

### ChronoQueue Not Starting

```bash
# View logs
make deploy-logs STORAGE=postgres

# Common issues:
# - Port conflicts: Another service using 9000, 8080, or 9090
# - Database not ready: Wait for PostgreSQL healthcheck to pass
# - Volume permissions: Check Docker volume permissions
```

### PostgreSQL Connection Issues

```bash
# Check PostgreSQL is healthy
docker exec chronoqueue-postgres pg_isready -U chronoqueue

# Connect to PostgreSQL
docker exec -it chronoqueue-postgres psql -U chronoqueue -d chronoqueue

# View tables
\dt

# Check connection from ChronoQueue
docker logs chronoqueue-server | grep -i postgres
```

### SQLite Issues

```bash
# Check SQLite database exists
docker exec chronoqueue-server ls -lh /data/chronoqueue.db

# Inspect SQLite database
docker exec -it chronoqueue-server sqlite3 /data/chronoqueue.db ".tables"
```

### Reset Everything

```bash
# Stop all services
make deploy-down STORAGE=postgres
make monitoring-down

# Or with specific storage
make deploy-down STORAGE=sqlite

# Remove volumes (WARNING: Deletes all data)
docker volume rm postgres-data prometheus-data grafana-data
# or
docker volume rm sqlite-data prometheus-data grafana-data

# Restart
make deploy-all STORAGE=postgres
```

## Production Deployment

### PostgreSQL Production Settings

1. **Use strong passwords**:

   ```yaml
   environment:
     - POSTGRES_PASSWORD=your-strong-password-here
   ```

2. **Enable SSL**:

   ```yaml
   environment:
     - POSTGRES_SSLMODE=require  # or verify-full
   ```

3. **Use external managed PostgreSQL**:

   ```yaml
   environment:
     - POSTGRES_HOST=your-postgres-instance.cloud:5432
     - POSTGRES_USER=chronoqueue
     - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}  # From secrets
     - POSTGRES_SSLMODE=verify-full
   ```

   Remove the `postgres` service from docker-compose

### General Production Settings

1. **Enable TLS**:

   ```yaml
   environment:
     - CHRONOQUEUE_TLS_ENABLED=true
   ```

2. **Configure proper resource limits**:

   ```yaml
   deploy:
     resources:
       limits:
         cpus: '2'
         memory: 4G
       reservations:
         cpus: '1'
         memory: 2G
   ```

3. **Use secrets management** instead of environment variables:

   ```yaml
   secrets:
     - encryption_key
     - postgres_password
   ```

4. **Set up AlertManager** for production alerts

5. **Configure Grafana authentication** (OAuth, LDAP, etc.)

6. **Use production-grade logging**:

   ```yaml
   environment:
     - LOG_LEVEL=info
     - LOG_FORMAT=json
   ```

## Development Workflow

### View Real-time Logs

```bash
# All services
make deploy-logs STORAGE=postgres

# Monitoring stack
make monitoring-logs

# Specific service
docker logs -f chronoqueue-server
docker logs -f chronoqueue-postgres
docker logs -f chronoqueue-prometheus
```

### Restart After Code Changes

```bash
# Rebuild and restart ChronoQueue
make deploy-rebuild STORAGE=postgres

# Or manually
cd deploy
docker-compose -f docker-compose.postgres.yaml up -d --build chronoqueuesvc
```

### Execute Commands Inside Container

```bash
# Access ChronoQueue container
docker exec -it chronoqueue-server /bin/bash

# Check ChronoQueue CLI
docker exec -it chronoqueue-server chronoq --help

# Access PostgreSQL
docker exec -it chronoqueue-postgres psql -U chronoqueue -d chronoqueue

# Run SQL queries
docker exec -it chronoqueue-postgres psql -U chronoqueue -d chronoqueue -c "SELECT * FROM queues;"
```

### Switch Storage Backends

```bash
# Stop current deployment
make deploy-down STORAGE=postgres

# Start with different storage
make deploy-up STORAGE=sqlite

# Or switch to Redis (not recommended - no metrics)
make deploy-up STORAGE=redis
```

## Next Steps

- Customize alert thresholds in [`../monitoring/prometheus-alerts.yml`](../monitoring/prometheus-alerts.yml)
- Add custom Grafana dashboards
- Set up AlertManager for notifications
- Configure Grafana authentication
- Add additional ChronoQueue instances for load testing
- Integrate with your CI/CD pipeline

## Support

For more information:

- [ChronoQueue Documentation](../README.md)
- [Monitoring Guide](../monitoring/README.md)
- [Metrics Reference](../pkg/metrics/README.md)
