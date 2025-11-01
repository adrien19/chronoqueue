# ChronoQueue CLI

A unified command-line interface for ChronoQueue, providing both server operations and client management commands for queue management, message operations, and schedule management.

## Installation

Build from source:

```bash
go build -o chronoqueue .
```

## Usage

### Global Options

All client commands support these global flags:

- `--server` - ChronoQueue server address (default: localhost:8080)
- `--insecure` - Use insecure connection (no TLS)
- `--cert-file` - Path to client certificate file for mTLS
- `--key-file` - Path to client private key file for mTLS
- `--ca-file` - Path to CA certificate file
- `--output` - Output format: table, json, yaml (default: table)
- `--timeout` - Request timeout (0 for no timeout)
- `--verbose` - Enable verbose output

## Commands

### Server Operations

#### Start ChronoQueue Server

```bash
chronoqueue server [flags]

Flags:
      --dev                    Start in development mode (enables CORS, API docs, reflection)
      --production             Start in production mode (optimized for production use)
      --grpc-addr string       gRPC server address (default ":9000")
      --http-addr string       HTTP gateway address (default ":8080")
      --redis-addr string      Redis server address (default "localhost:6379")
      --log-level string       Log level: debug, info, warn, error (default "info")
      --log-format string      Log format: text, json (default "text")
      --enable-tls             Enable TLS
      --cert-file string       TLS certificate file
      --key-file string        TLS key file
      --ca-cert-file string    CA certificate file for mutual TLS (optional)
      --enable-cors            Enable CORS for HTTP gateway
      --cors-origins strings   Allowed CORS origins

Examples:
    # Development server with default settings
  chronoqueue server --dev
  
  # Production server with custom configuration
  chronoqueue server --grpc-addr :9000 --http-addr :8080 --redis-addr localhost:6379
  
  # Server with TLS enabled
  chronoqueue server --enable-tls --cert-file server.crt --key-file server.key
```

This starts both gRPC and HTTP gateway servers:

- **gRPC Server**: Serves on `:9000` by default
- **HTTP Gateway**: Serves on `:8080` by default
- **Health Check**: Available at `http://localhost:8080/health`
- **Metrics**: Available at `http://localhost:8080/metrics`
- **API Documentation**: Available at `http://localhost:8080/docs/` (dev mode)
- **Redis Storage**: Connects to Redis for message persistence

### Queue Management

#### Create Queue

```bash
chronoqueue queue create <queue-name> [flags]

Flags:
  -t, --type string                    Queue type: simple, exclusive (default: simple)
  -a, --dequeue-attempts int32         Maximum dequeue attempts (default: 3)
  -l, --lease-duration string          Message lease duration (default: 30s)
  -i, --invisibility-duration string   Message invisibility duration (default: 0s)
  -k, --exclusivity-key string         Exclusivity key (required for exclusive queues)

Examples:
  chronoqueue queue create my-queue --type simple
  chronoqueue queue create exclusive-queue --type exclusive --exclusivity-key "key1"
```

#### Delete Queue

```bash
chronoqueue queue delete <queue-name>

Examples:
  chronoqueue queue delete my-queue
```

#### List Queues

```bash
chronoqueue queue list

Examples:
  chronoqueue queue list
  chronoqueue queue list --output json
```

#### Get Queue State

```bash
chronoqueue queue state <queue-name>

Examples:
  chronoqueue queue state my-queue
```

### Message Operations

#### Post Message

```bash
chronoqueue message post <queue-name> <message-data> [flags]

Flags:
  -i, --id string                      Message ID (auto-generated if not provided)
  -l, --lease-duration string          Message lease duration
  -v, --invisibility-duration string   Message invisibility duration
  -p, --priority int32                 Message priority
  -a, --max-attempts int32             Maximum attempts for this message
  -m, --metadata string                Message metadata as JSON

Examples:
  chronoqueue message post my-queue "Hello World"
  chronoqueue message post my-queue "Priority message" --priority 10
  chronoqueue message post my-queue '{"user": "john", "action": "login"}' --id msg-123
```

#### Get Message

```bash
chronoqueue message get <queue-name> [flags]

Flags:
  -l, --lease-duration string     Message lease duration (default: 30s)
  -b, --enable-heartbeat          Enable automatic heartbeat to renew lease while processing
  -k, --exclusivity-key string    Exclusivity key for exclusive queues

Examples:
  chronoqueue message get my-queue
  chronoqueue message get my-queue --enable-heartbeat
  chronoqueue message get exclusive-queue --exclusivity-key "key1"

Note: The get command returns a Stream Entry ID which should be used with ack and heartbeat commands
for optimal performance with Redis Streams architecture.
```

#### Acknowledge Message

```bash
chronoqueue message ack <queue-name> <message-id> <message-state> [stream-entry-id]

States:
  COMPLETED - Message processed successfully
  CANCELED  - Message processing was canceled
  ERRORED   - Message processing failed

Arguments:
  stream-entry-id - (Optional) Redis Stream entry ID from get command for optimal performance

Examples:
  # Basic acknowledgment
  chronoqueue message ack my-queue msg-123 COMPLETED
  
  # With stream entry ID (recommended for Redis Streams architecture)
  chronoqueue message ack my-queue msg-123 COMPLETED 1234567890-0
  
  chronoqueue message ack my-queue msg-456 ERRORED

Examples:
#### Peek Messages

```bash
chronoqueue message peek <queue-name> [flags]

Flags:
  --time-range strings   Time range for messages to peek (min=0,max=0)

Examples:
  chronoqueue message peek my-queue
```

#### Renew Message Lease

```bash
chronoqueue message renew <queue-name> <message-id> <lease-duration>

Examples:
  chronoqueue message renew my-queue msg-123 60s
```

#### Send Message Heartbeat

```bash
chronoqueue message heartbeat <queue-name> <message-id> [stream-entry-id]

Arguments:
  stream-entry-id - (Optional) Redis Stream entry ID from get command for optimal performance

Examples:
  # Basic heartbeat
  chronoqueue message heartbeat my-queue msg-123
  
  # With stream entry ID (recommended for Redis Streams architecture)
  chronoqueue message heartbeat my-queue msg-123 1234567890-0
```

### Schedule Management

#### Create Schedule

```bash
chronoqueue schedule create <queue-name> <message-data> [flags]

Flags:
  -i, --id string                  Schedule ID (auto-generated if not provided)
  -c, --cron string                Cron expression for the schedule (required)
  -k, --exclusivity-key string     Exclusivity key for exclusive queues
  -d, --metadata string            Message metadata as JSON
  -a, --max-attempts int32         Maximum attempts for scheduled messages
  -l, --lease-duration string      Lease duration for scheduled messages (default: 30s)

Examples:
  chronoqueue schedule create my-queue "Scheduled task" --cron "0 */5 * * *"
  chronoqueue schedule create my-queue '{"task": "cleanup"}' --cron "0 2 * * *" --id daily-cleanup
  chronoq schedule create --cron "0 0 * * 0" --queue weekly-queue --message "Weekly report"
```

#### Delete Schedule

```bash
chronoqueue schedule delete <schedule-id>

Examples:
  chronoqueue schedule delete sched-12345
```

#### List Schedules

```bash
chronoqueue schedule list

Examples:
  chronoqueue schedule list
  chronoqueue schedule list --output json
```

#### Get Schedule Details

```bash
chronoqueue schedule get <schedule-id>

Examples:
  chronoqueue schedule get sched-12345
```

#### Pause Schedule

```bash
chronoqueue schedule pause <schedule-id>

Examples:
  chronoqueue schedule pause sched-12345
```

#### Resume Schedule

```bash
chronoqueue schedule resume <schedule-id>

Examples:
  chronoqueue schedule resume sched-12345
```

## Output Formats

The CLI supports multiple output formats:

- **Table** (default): Human-readable tabular format
- **JSON**: Machine-readable JSON format  
- **YAML**: Human and machine-readable YAML format

```bash
# Table format (default)
chronoqueue queue list

# JSON format
chronoqueue queue list --output json

# YAML format
chronoqueue queue list --output yaml
```

## TLS Configuration

### Insecure Connection

```bash
chronoqueue --insecure queue list
```

### Server-side TLS

```bash
chronoqueue --server secure-server:8443 queue list
```

### Mutual TLS (mTLS)

```bash
chronoqueue --server secure-server:8443 \
        --cert-file client.crt \
        --key-file client.key \
        --ca-file ca.crt \
        queue list
```

## Complete Workflow Examples

### Basic Message Queue Workflow

```bash
# 1. Start the ChronoQueue server in development mode
chronoqueue server --dev

# 2. Create a work queue
chronoqueue queue create work-queue --type simple --insecure --server 0.0.0.0:9000

# 3. Post some messages
chronoqueue message post work-queue "Task 1" --insecure --server 0.0.0.0:9000
chronoqueue message post work-queue "Task 2" --priority 5 --insecure --server 0.0.0.0:9000

# 4. Check queue state
chronoqueue queue state work-queue --insecure --server 0.0.0.0:9000

# 5. Get and process messages (note the Stream Entry ID in output)
chronoqueue message get work-queue --lease-duration 30s --insecure --server 0.0.0.0:9000
# Output:
# Message ID: msg-123
# Stream Entry ID: 1234567890-0
# ...

# 6. Acknowledge processed message (use both message ID and stream entry ID from step 5)
chronoqueue message ack work-queue msg-123 COMPLETED 1234567890-0 --insecure --server 0.0.0.0:9000
```

### Scheduled Tasks Workflow

```bash
# 1. Create a schedule for daily reports
chronoqueue schedule create reports-queue "Generate daily report" \
    --cron "0 9 * * *" \
    --id daily-reports \
    --insecure --server 0.0.0.0:9000

# 2. List all schedules
chronoqueue schedule list --insecure --server 0.0.0.0:9000

# 3. Pause a schedule
chronoqueue schedule pause daily-reports --insecure --server 0.0.0.0:9000

# 4. Resume a schedule
chronoqueue schedule resume daily-reports --insecure --server 0.0.0.0:9000
```

### Exclusive Queue Workflow

```bash
# 1. Create exclusive queue
chronoqueue queue create exclusive-work \
    --type exclusive \
    --exclusivity-key worker-1 \
    --insecure --server 0.0.0.0:9000

# 2. Post message to exclusive queue
chronoqueue message post exclusive-work "Exclusive task" \
    --insecure --server 0.0.0.0:9000

# 3. Get message with correct exclusivity key
chronoqueue message get exclusive-work \
    --exclusivity-key worker-1 \
    --insecure --server 0.0.0.0:9000
```

## Implementation Status

✅ **Server Operations**: Fully implemented with development and production modes  
✅ **Queue Management**: All operations (create, delete, list, state) are fully functional  
✅ **Message Operations**: All operations (post, get, ack, peek, renew, heartbeat) are fully functional  
✅ **Schedule Management**: All operations (create, delete, list, get, pause, resume) are fully functional  
✅ **Client Integration**: Complete gRPC client with connection management and retry logic  
✅ **Output Formats**: Support for table, JSON, and YAML output formats  
✅ **TLS Support**: Full TLS and mTLS capabilities for secure connections  

## Performance Optimizations

The CLI and server include several performance optimizations:

- **State-Based Sorted Sets**: O(log n) performance for background message processing
- **Optimized Metadata Operations**: Eliminated redundant database calls
- **Connection Pooling**: Efficient gRPC connection management
- **Atomic Transactions**: Redis pipeline operations for consistency

## Architecture

The CLI is built using:

- **Cobra**: Command-line interface framework for robust CLI experience
- **gRPC Client**: High-performance ChronoQueue gRPC client integration  
- **Multi-format Output**: Structured output supporting table, JSON, and YAML formats
- **TLS Support**: Complete TLS and mutual TLS capabilities
- **Modular Design**: Separate command modules for maintainable code organization
- **Error Handling**: Comprehensive error handling with user-friendly messages

## Troubleshooting

### Connection Issues

```bash
# Test server connectivity
chronoqueue --insecure --server 0.0.0.0:9000 queue list

# Enable verbose output for debugging
chronoqueue --verbose --insecure queue create test-queue
```

### Common Error Solutions

- **Connection refused**: Ensure the server is running with `chronoqueue server --dev`
- **Context deadline exceeded**: Check network connectivity and server health
- **Permission denied**: Verify TLS certificates or use `--insecure` for development
