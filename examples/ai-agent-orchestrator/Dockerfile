# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache git make

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN go build -o ai-orchestrator .

# Runtime stage
FROM alpine:latest

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates curl

# Copy binary from builder
COPY --from=builder /build/ai-orchestrator .

# Copy task files
COPY tasks/ ./tasks/

# Create logs directory
RUN mkdir -p /app/logs

# Expose any necessary ports (if needed)
# EXPOSE 8080

# Default command (can be overridden in docker-compose)
CMD ["./ai-orchestrator", "--help"]
