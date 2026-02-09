# Production Dockerfile (Postgres/Redis)
# Pure Go build - no CGO, fully static binary, cross-platform compatible
# For SQLite support, use Dockerfile.sqlite instead

FROM golang:1.25.7-alpine3.22 as builder

# Set working directory
WORKDIR /build

# Copy dependency files first (better layer caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY api api
COPY cmd/chronoq cmd/chronoq
COPY client client
COPY internal internal
COPY pkg pkg
COPY main.go ./

# Build arguments for version injection
ARG VERSION=1.0.0
ARG GIT_COMMIT=unknown
ARG BUILD_DATE=unknown

# Build static binary (no CGO, fully portable)
# Production backends: Postgres, Redis - no SQLite
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s -X 'github.com/adrien19/chronoqueue/pkg/version.Version=${VERSION}' \
    -X 'github.com/adrien19/chronoqueue/pkg/version.GitCommit=${GIT_COMMIT}' \
    -X 'github.com/adrien19/chronoqueue/pkg/version.BuildDate=${BUILD_DATE}'" \
    -o chronoqueue \
    main.go

## Runtime stage - minimal image with just the binary
FROM alpine:latest

# Install CA certificates for HTTPS/TLS and timezone data for calendar schedules
RUN apk add --no-cache ca-certificates tzdata

# Set working directory
WORKDIR /

# Create necessary directories
RUN mkdir -p /config /secrets

# Copy binary from builder
COPY --from=builder /build/chronoqueue /chronoqueue

# Copy entrypoint script
COPY images/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# just an indication that this port will be exposed by this container
EXPOSE 9000 8080

# command to run at the immediate start of the container
ENTRYPOINT ["/entrypoint.sh"]