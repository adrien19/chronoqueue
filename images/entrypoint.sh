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

# Add storage-type flag
CMD_ARGS="$CMD_ARGS --storage-type $STORAGE_TYPE"

# Add storage-specific configuration
case "$STORAGE_TYPE" in
    postgres)
        # Note: POSTGRES_PASSWORD and POSTGRES_DSN are NOT passed as CLI arguments
        # for security reasons (visible in ps, /proc/<pid>/cmdline, logs).
        # The chronoqueue binary reads them directly from environment variables.
        if [ -n "$POSTGRES_DSN" ]; then
            log_info "Using PostgreSQL DSN from environment variable (not shown for security)"
            # DSN contains embedded credentials, so we don't pass it via CLI
        else
            [ -n "$POSTGRES_HOST" ] && CMD_ARGS="$CMD_ARGS --postgres-host $POSTGRES_HOST"
            [ -n "$POSTGRES_PORT" ] && CMD_ARGS="$CMD_ARGS --postgres-port $POSTGRES_PORT"
            [ -n "$POSTGRES_USER" ] && CMD_ARGS="$CMD_ARGS --postgres-user $POSTGRES_USER"
            # POSTGRES_PASSWORD is read from environment variable by the binary (not passed via CLI for security)
            [ -n "$POSTGRES_DB" ] && CMD_ARGS="$CMD_ARGS --postgres-db $POSTGRES_DB"
            [ -n "$POSTGRES_SSLMODE" ] && CMD_ARGS="$CMD_ARGS --postgres-sslmode $POSTGRES_SSLMODE"
            # PostgreSQL Client Certificate Configuration (for mTLS with database)
            [ -n "$POSTGRES_CLIENT_CERT" ] && CMD_ARGS="$CMD_ARGS --postgres-client-cert $POSTGRES_CLIENT_CERT"
            [ -n "$POSTGRES_CLIENT_KEY" ] && CMD_ARGS="$CMD_ARGS --postgres-client-key $POSTGRES_CLIENT_KEY"
            [ -n "$POSTGRES_ROOT_CERT" ] && CMD_ARGS="$CMD_ARGS --postgres-root-cert $POSTGRES_ROOT_CERT"
        fi
        ;;
    sqlite)
        SQLITE_DB_PATH="${SQLITE_DB_PATH:-chronoqueue.db}"
        CMD_ARGS="$CMD_ARGS --sqlite-db-path $SQLITE_DB_PATH"
        ;;
esac

# Add optional flags
[ -n "$LOG_LEVEL" ] && CMD_ARGS="$CMD_ARGS --log-level $LOG_LEVEL"
[ -n "$GRPC_ADDR" ] && CMD_ARGS="$CMD_ARGS --grpc-addr $GRPC_ADDR"
[ -n "$HTTP_ADDR" ] && CMD_ARGS="$CMD_ARGS --http-addr $HTTP_ADDR"

# Add TLS configuration flags
if [ "$ENABLE_TLS" = "true" ]; then
    log_info "TLS enabled"
    CMD_ARGS="$CMD_ARGS --enable-tls"

    if [ -n "$CERT_FILE" ]; then
        CMD_ARGS="$CMD_ARGS --cert-file $CERT_FILE"
        log_info "TLS Certificate: $CERT_FILE"
    fi

    if [ -n "$KEY_FILE" ]; then
        CMD_ARGS="$CMD_ARGS --key-file $KEY_FILE"
        log_info "TLS Key: $KEY_FILE"
    fi

    if [ -n "$CA_CERT_FILE" ]; then
        CMD_ARGS="$CMD_ARGS --ca-cert-file $CA_CERT_FILE"
        log_info "CA Certificate: $CA_CERT_FILE (mTLS enabled)"
    fi
fi

# Add gateway TLS configuration flags
if [ "$GATEWAY_USE_TLS" = "true" ]; then
    CMD_ARGS="$CMD_ARGS --gateway-use-tls"
    log_info "Gateway TLS enabled"
fi

if [ "$GATEWAY_INSECURE" = "true" ]; then
    CMD_ARGS="$CMD_ARGS --gateway-insecure"
    log_info "Gateway TLS verification disabled"
fi

# Add CORS configuration flags
if [ "$ENABLE_CORS" = "true" ]; then
    CMD_ARGS="$CMD_ARGS --enable-cors"
    log_info "CORS enabled"

    if [ -n "$CORS_ORIGINS" ]; then
        CMD_ARGS="$CMD_ARGS --cors-origins $CORS_ORIGINS"
        log_info "CORS Origins: $CORS_ORIGINS"
    fi
fi

log_info "Starting ChronoQueue server..."
log_info "Command: /chronoqueue $CMD_ARGS"
log_info ""

exec /chronoqueue $CMD_ARGS
