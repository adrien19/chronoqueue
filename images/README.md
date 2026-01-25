# ChronoQueue Docker Images

This directory contains Dockerfiles for building ChronoQueue container images.

## Available Dockerfiles

### `Dockerfile` (Production)

**Purpose**: Production-ready builds for Postgres and Redis backends  
**Features**:

- ✅ Pure Go build (CGO_ENABLED=0)
- ✅ Fully static binary - no runtime dependencies
- ✅ Cross-platform compatible (can be built for any architecture)
- ✅ Smallest image size (~20MB total)
- ✅ Fast build times (no C compiler needed)
- ✅ Optimized for production deployment

**Supported Backends**: PostgreSQL, Redis  
**Build Command**:

```bash
docker build -f images/Dockerfile -t chronoqueue:latest .
```

**Usage**:

- Production deployments
- Kubernetes/cloud environments
- Any environment requiring pure Go binary

### `Dockerfile.sqlite` (Development/Testing)

**Purpose**: Development and testing builds with SQLite support  
**Features**:

- ✅ CGO enabled for SQLite support
- ✅ Includes C compiler dependencies
- ⚠️ Larger image size (~50MB total)
- ⚠️ Platform-specific binary (build architecture dependent)
- ⚠️ Slower build times (requires gcc/musl-dev)

**Supported Backends**: SQLite (plus PostgreSQL, Redis)  
**Build Command**:

```bash
docker build -f images/Dockerfile.sqlite -t chronoqueue:sqlite .
```

**Usage**:

- Local development
- Integration testing
- CI/CD pipelines
- Single-node deployments

## Docker Compose Integration

The docker-compose files automatically use the appropriate Dockerfile:

```yaml
# deploy/docker-compose.postgres.yaml - uses Dockerfile (production)
# deploy/docker-compose.sqlite.yaml - uses Dockerfile.sqlite (dev/test)
```

## Build Arguments

Both Dockerfiles support version injection via build arguments:

```bash
docker build \
  --build-arg VERSION=1.2.3 \
  --build-arg GIT_COMMIT=$(git rev-parse HEAD) \
  --build-arg BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ") \
  -f images/Dockerfile \
  -t chronoqueue:1.2.3 .
```

## Image Comparison

| Feature | Dockerfile (Prod) | Dockerfile.sqlite (Dev) |
|---------|------------------|------------------------|
| CGO | Disabled | Enabled |
| Static Binary | ✅ Yes | ❌ No |
| Image Size | ~20MB | ~50MB |
| Build Time | Fast | Slower |
| Cross-compile | ✅ Easy | ❌ Complex |
| SQLite Support | ❌ No | ✅ Yes |
| Postgres/Redis | ✅ Yes | ✅ Yes |
| Production Ready | ✅ Yes | ⚠️ Dev/Test only |

## Best Practices

### When to Use Production Dockerfile

- ✅ Production deployments
- ✅ Kubernetes clusters
- ✅ Cloud platforms (AWS, GCP, Azure)
- ✅ Multi-architecture builds
- ✅ When using Postgres or Redis

### When to Use SQLite Dockerfile

- ✅ Local development
- ✅ Integration tests
- ✅ Demo/POC environments
- ✅ Single-node deployments
- ✅ When SQLite is specifically required

## Multi-Stage Build Structure

Both Dockerfiles use multi-stage builds for optimization:

```dockerfile
# Stage 1: Builder - compiles the Go binary
FROM golang:6.0-alpine3.22 as builder
# ... build steps ...

# Stage 2: Runtime - minimal image with only the binary
FROM alpine:latest
# ... copy binary and minimal runtime files ...
```

Benefits:

- Final image contains only the binary (no build tools/source code)
- Reduced attack surface
- Faster container startup
- Smaller image size

## Maintenance Notes

When adding new features or dependencies:

1. **If pure Go**: Update only `Dockerfile`
2. **If requires CGO**: Update both Dockerfiles
3. **If SQLite-specific**: Update only `Dockerfile.sqlite`
4. Keep both files in sync for common changes (version args, entrypoint, etc.)

## Troubleshooting

### SQLite binary not working

Error: `SQLite storage requested but not available`

**Solution**: Ensure you're using `Dockerfile.sqlite`:

```bash
docker-compose -f deploy/docker-compose.sqlite.yaml build
```

### Production build includes unnecessary dependencies

**Solution**: Verify you're using `Dockerfile` (not `Dockerfile.sqlite`) for production.

### Cross-compilation issues

**Solution**: Production `Dockerfile` uses `CGO_ENABLED=0` for pure Go compilation.
