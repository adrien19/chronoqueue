#!/bin/sh
set -e

# Colors for output (if terminal supports it)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo "${RED}[ERROR]${NC} $1"
}

# Default values
SERVER_MODE="${SERVER_MODE:-production}"
REDIS_ADDR="${REDIS_ADDR:-localhost:6379}"
REDIS_DB="${REDIS_DB:-0}"
LOG_LEVEL="${LOG_LEVEL:-info}"
GRPC_ADDR="${GRPC_ADDR:-:9000}"
HTTP_ADDR="${HTTP_ADDR:-:8080}"

log_info "ChronoQueue Server Startup"
log_info "=========================="
log_info "Server Mode: $SERVER_MODE"
log_info "Redis Address: $REDIS_ADDR"
log_info "Redis DB: $REDIS_DB"
log_info "Log Level: $LOG_LEVEL"
log_info "gRPC Address: $GRPC_ADDR"
log_info "HTTP Address: $HTTP_ADDR"
log_info "=========================="

# Note: We don't pre-check Redis connectivity here for security reasons
# (avoiding netcat or similar tools that could introduce vulnerabilities).
# Instead, we rely on:
# 1. Container orchestration (Docker Compose, Kubernetes) to manage dependency startup
# 2. The ChronoQueue server itself to report Redis connection errors with clear logs
# 3. Health check endpoints to verify the server is working correctly
# This is the industry-standard approach for production containers.

# Build command arguments based on SERVER_MODE
CMD_ARGS="server"

case "$SERVER_MODE" in
    production)
        log_info "Starting in PRODUCTION mode"
        CMD_ARGS="$CMD_ARGS --production"
        ;;
    dev|development)
        log_info "Starting in DEVELOPMENT mode"
        CMD_ARGS="$CMD_ARGS --dev"
        ;;
    *)
        log_warn "Unknown SERVER_MODE '$SERVER_MODE', defaulting to production mode"
        CMD_ARGS="$CMD_ARGS --production"
        ;;
esac

# Add other flags
CMD_ARGS="$CMD_ARGS --grpc-addr $GRPC_ADDR"
CMD_ARGS="$CMD_ARGS --http-addr $HTTP_ADDR"
CMD_ARGS="$CMD_ARGS --redis-addr $REDIS_ADDR"
CMD_ARGS="$CMD_ARGS --redis-db $REDIS_DB"
CMD_ARGS="$CMD_ARGS --log-level $LOG_LEVEL"

# Handle TLS configuration if enabled
if [ -n "$ENABLE_TLS" ] && [ "$ENABLE_TLS" = "true" ]; then
    log_info "TLS is enabled"
    CMD_ARGS="$CMD_ARGS --enable-tls"

    if [ -n "$CERT_FILE" ]; then
        CMD_ARGS="$CMD_ARGS --cert-file $CERT_FILE"
    fi

    if [ -n "$KEY_FILE" ]; then
        CMD_ARGS="$CMD_ARGS --key-file $KEY_FILE"
    fi

    if [ -n "$CA_CERT_FILE" ]; then
        CMD_ARGS="$CMD_ARGS --ca-file $CA_CERT_FILE"
    fi
fi

# Handle Redis authentication if provided
if [ -n "$REDIS_PASSWORD" ]; then
    CMD_ARGS="$CMD_ARGS --redis-password $REDIS_PASSWORD"
fi

if [ -n "$REDIS_USERNAME" ]; then
    CMD_ARGS="$CMD_ARGS --redis-username $REDIS_USERNAME"
fi

log_info "Starting ChronoQueue server..."
log_info "Command: ./chronoqueue $CMD_ARGS"
log_info ""

# Execute the server
exec ./chronoqueue $CMD_ARGS
