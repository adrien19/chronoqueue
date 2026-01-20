#!/bin/sh
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo "${RED}[ERROR]${NC} $1"
}

SERVER_MODE="${SERVER_MODE:-production}"
STORAGE_TYPE="${STORAGE_TYPE:-postgres}"
LOG_LEVEL="${LOG_LEVEL:-info}"
GRPC_ADDR="${GRPC_ADDR:-:9000}"
HTTP_ADDR="${HTTP_ADDR:-:8080}"

log_info "ChronoQueue Server Startup"
log_info "=========================="
log_info "Server Mode: $SERVER_MODE"
log_info "Storage Type: $STORAGE_TYPE"
log_info "Log Level: $LOG_LEVEL"
log_info "gRPC Address: $GRPC_ADDR"
log_info "HTTP Address: $HTTP_ADDR"

case "$STORAGE_TYPE" in
    postgres)
        log_info "PostgreSQL Host: ${POSTGRES_HOST:-localhost}:${POSTGRES_PORT:-5432}"
        log_info "PostgreSQL Database: ${POSTGRES_DB:-chronoqueue}"
        ;;
    sqlite)
        log_info "SQLite Database: ${SQLITE_DB_PATH:-chronoqueue.db}"
        ;;
    *)
        log_error "Unknown storage type: $STORAGE_TYPE"
        exit 1
        ;;
esac

log_info "=========================="

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

log_info "Starting ChronoQueue server..."
log_info "Command: /chronoqueue $CMD_ARGS"
log_info ""

exec /chronoqueue $CMD_ARGS
