# ChronoQueue Docker Entrypoint

The `entrypoint.sh` script provides a flexible way to start the ChronoQueue server with proper validation and configuration based on environment variables.

## Features

- **Redis Connectivity Check**: Validates Redis is reachable before starting the server (30s timeout)
- **Environment-Based Configuration**: Configure server mode and settings via environment variables
- **Startup Logging**: Clear logging of configuration and startup process
- **Error Handling**: Fails fast if Redis is not available

## Environment Variables

### Required

| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_ADDR` | Redis server address | `localhost:6379` |

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

### Redis Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_ADDR` | Redis server address | `localhost:6379` |
| `REDIS_DB` | Redis database number | `0` |
| `REDIS_PASSWORD` | Redis password (if required) | _(empty)_ |
| `REDIS_USERNAME` | Redis username (Redis 6+ ACL) | _(empty)_ |

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

### Docker Run - Development Mode

```bash
docker run -d \
  -e SERVER_MODE=development \
  -e REDIS_ADDR=redis:6379 \
  -e LOG_LEVEL=debug \
  -p 9000:9000 \
  -p 8080:8080 \
  chronoqueue:latest
```

### Docker Run - Production Mode

```bash
docker run -d \
  -e SERVER_MODE=production \
  -e REDIS_ADDR=redis-prod:6379 \
  -e REDIS_PASSWORD=secret \
  -e LOG_LEVEL=info \
  -p 9000:9000 \
  -p 8080:8080 \
  chronoqueue:latest
```

### Docker Run - Production with TLS

```bash
docker run -d \
  -e SERVER_MODE=production \
  -e REDIS_ADDR=redis-prod:6379 \
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
      - REDIS_ADDR=redis:6379
      - LOG_LEVEL=info
    ports:
      - "9000:9000"
      - "8080:8080"
    depends_on:
      - redis
  
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
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
        - name: REDIS_ADDR
          value: "redis-service:6379"
        - name: LOG_LEVEL
          value: "info"
        - name: REDIS_PASSWORD
          valueFrom:
            secretKeyRef:
              name: redis-secret
              key: password
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

1. **Display Configuration**: Logs all configuration values
2. **Redis Connectivity Check**: Attempts to connect to Redis (30s timeout with 2s intervals)
3. **Build Command**: Constructs server command based on `SERVER_MODE` and other env vars
4. **Start Server**: Executes the ChronoQueue server with the constructed arguments

## Troubleshooting

### Redis Connection Failures

If the container fails to start with Redis connection errors:

```
[ERROR] Redis is not reachable at redis:6379 after 30s
[ERROR] Failed to connect to Redis. Server will not start.
```

**Solutions:**

- Verify Redis is running: `docker ps | grep redis`
- Check network connectivity: `docker network inspect <network-name>`
- Verify Redis address is correct: `echo $REDIS_ADDR`
- Ensure Redis port is exposed: `docker port <redis-container>`

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
[INFO] Redis Address: redis:6379
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
