# ChronoQueue Docker Entrypoint

The `entrypoint.sh` script provides a flexible way to start the ChronoQueue server with proper validation and configuration based on environment variables.

## Features

- **Environment-Based Configuration**: Configure server mode and settings via environment variables
- **Startup Logging**: Clear logging of configuration and startup process
- **Validation**: Validates storage configuration before starting

## Environment Variables

### Required

| Variable | Description | Default |
|----------|-------------|---------|
| `STORAGE_TYPE` | Storage backend type | `postgres` |

### Server Mode

| Variable | Description | Default | Values |
|----------|-------------|---------|--------|
| `SERVER_MODE` | Server operation mode | `production` | `production`, `development`, `dev` |

**Production Mode:**

- JSON logging format
- CORS disabled
- Optimized for production use

**Development Mode:**

- Text logging format
- CORS enabled (all origins)
- gRPC reflection enabled
- Additional debugging features

### Network Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `GRPC_ADDR` | gRPC server listen address | `:9000` |
| `HTTP_ADDR` | HTTP gateway listen address | `:8080` |

### Storage Configuration

| Variable | Description | Default | Values |
|----------|-------------|---------|--------|
| `STORAGE_TYPE` | Storage backend | `postgres` | `postgres`, `sqlite`|

### PostgreSQL Configuration

| Variable | Description | Default |
|----------|-------------|---------||
| `POSTGRES_HOST` | PostgreSQL host | `localhost` |
| `POSTGRES_PORT` | PostgreSQL port | `5432` |
| `POSTGRES_USER` | PostgreSQL user | `chronoqueue` |
| `POSTGRES_PASSWORD` | PostgreSQL password | _(required)_ |
| `POSTGRES_DB` | PostgreSQL database | `chronoqueue` |
| `POSTGRES_SSLMODE` | SSL mode | `disable` |

### SQLite Configuration

| Variable | Description | Default |
|----------|-------------|---------||
| `SQLITE_DB_PATH` | SQLite database file path | `chronoqueue.db` |

### Logging Configuration

| Variable | Description | Default | Values |
|----------|-------------|---------|--------|
| `LOG_LEVEL` | Logging level | `info` | `debug`, `info`, `warn`, `error` |

### TLS Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `ENABLE_TLS` | Enable TLS for gRPC/HTTP | `false` |
| `CERT_FILE` | Path to server certificate | _(empty)_ |
| `KEY_FILE` | Path to server private key | _(empty)_ |
| `CA_CERT_FILE` | Path to CA certificate | _(empty)_ |

## Usage Examples

### Docker Run - Development Mode (PostgreSQL)

```bash
docker run -d \
  -e SERVER_MODE=development \
  -e STORAGE_TYPE=postgres \
  -e POSTGRES_HOST=postgres \
  -e POSTGRES_USER=chronoqueue \
  -e POSTGRES_PASSWORD=secret \
  -e POSTGRES_DB=chronoqueue \
  -e LOG_LEVEL=debug \
  -p 9000:9000 \
  -p 8080:8080 \
  chronoqueue:latest
```

### Docker Run - Development Mode (SQLite)

```bash
docker run -d \
  -e SERVER_MODE=development \
  -e STORAGE_TYPE=sqlite \
  -e SQLITE_DB_PATH=/data/chronoqueue.db \
  -e LOG_LEVEL=debug \
  -v /path/to/data:/data \
  -p 9000:9000 \
  -p 8080:8080 \
  chronoqueue:sqlite
```

### Docker Run - Production Mode (PostgreSQL)

```bash
docker run -d \
  -e SERVER_MODE=production \
  -e STORAGE_TYPE=postgres \
  -e POSTGRES_HOST=postgres-prod \
  -e POSTGRES_USER=chronoqueue \
  -e POSTGRES_PASSWORD=secret \
  -e POSTGRES_DB=chronoqueue \
  -e POSTGRES_SSLMODE=require \
  -e LOG_LEVEL=info \
  -p 9000:9000 \
  -p 8080:8080 \
  chronoqueue:latest
```

### Docker Run - Production with TLS

```bash
docker run -d \
  -e SERVER_MODE=production \
  -e STORAGE_TYPE=postgres \
  -e POSTGRES_HOST=postgres-prod \
  -e POSTGRES_PASSWORD=secret \
  -e ENABLE_TLS=true \
  -e CERT_FILE=/secrets/server.crt \
  -e KEY_FILE=/secrets/server.key \
  -e CA_CERT_FILE=/secrets/ca.crt \
  -v /path/to/certs:/secrets:ro \
  -p 9000:9000 \
  -p 8080:8080 \
  chronoqueue:latest
```

### Docker Compose

```yaml
version: '3.8'

services:
  chronoqueue:
    image: chronoqueue:latest
    environment:
      - SERVER_MODE=production
      - STORAGE_TYPE=postgres
      - POSTGRES_HOST=postgres
      - POSTGRES_USER=chronoqueue
      - POSTGRES_PASSWORD=secret
      - POSTGRES_DB=chronoqueue
      - LOG_LEVEL=info
    ports:
      - "9000:9000"
      - "8080:8080"
    depends_on:
      - postgres
  
  postgres:
    image: postgres:16-alpine
    environment:
      - POSTGRES_USER=chronoqueue
      - POSTGRES_PASSWORD=secret
      - POSTGRES_DB=chronoqueue
    volumes:
      - postgres-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

volumes:
  postgres-data:
```

## Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: chronoqueue
spec:
  replicas: 3
  selector:
    matchLabels:
      app: chronoqueue
  template:
    metadata:
      labels:
        app: chronoqueue
    spec:
      containers:
      - name: chronoqueue
        image: chronoqueue:latest
        env:
        - name: SERVER_MODE
          value: "production"
        - name: STORAGE_TYPE
          value: "postgres"
        - name: POSTGRES_HOST
          value: "postgres-service"
        - name: POSTGRES_DB
          value: "chronoqueue"
        - name: POSTGRES_USER
          valueFrom:
            secretKeyRef:
              name: postgres-secret
              key: username
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: postgres-secret
              key: password
        - name: LOG_LEVEL
          value: "info"
        ports:
        - containerPort: 9000
          name: grpc
        - containerPort: 8080
          name: http
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 10
```

## Startup Flow

1. **Display Configuration**: Logs all configuration values including storage backend
2. **Validate Storage Configuration**: Ensures required environment variables are set for chosen storage
3. **Build Command**: Constructs server command based on `SERVER_MODE`, `STORAGE_TYPE`, and other env vars
4. **Start Server**: Executes the ChronoQueue server with the constructed arguments

## Troubleshooting

### Storage Connection Failures

If the container fails to start with storage connection errors:

**PostgreSQL:**

```
[ERROR] Failed to connect to PostgreSQL. Check connection settings.
```

**Solutions:**

- Verify PostgreSQL is running: `docker ps | grep postgres`
- Check network connectivity: `docker network inspect <network-name>`
- Verify connection settings: `echo $POSTGRES_HOST $POSTGRES_PORT`
- Ensure PostgreSQL accepts connections: Check `pg_hba.conf`
- Verify credentials are correct

**SQLite:**

```
[ERROR] Failed to open SQLite database
```

**Solutions:**

- Ensure the directory exists and is writable
- Verify volume mount is correct: `docker inspect <container-id>`
- Check file permissions on the host

### Server Mode Issues

If the server doesn't start with expected mode:

**Check Logs:**

```bash
docker logs <container-id>
```

**Expected Output:**

```
[INFO] ChronoQueue Server Startup
[INFO] ==========================
[INFO] Server Mode: development
[INFO] Storage Type: postgres
[INFO] PostgreSQL Host: postgres:5432
[INFO] ...
```

### TLS Configuration

If TLS fails to enable:

- Ensure certificate files exist in the container
- Verify file permissions: `ls -l /secrets/`
- Check certificate validity: `openssl x509 -in /secrets/server.crt -text -noout`

## Script Modification

To customize the entrypoint script behavior, edit `images/entrypoint.sh` and rebuild the Docker image:

```bash
make docker-build
```

## Related Documentation

- [Server Configuration](../internal/server/README.md)
- [Deployment Guide](../deploy/README.md)
- [Docker Compose Setup](../deploy/docker-compose.yaml)
