# Interview Platform Backend

REST API backend for the Interview Platform example, integrating with ChronoQueue for asynchronous job processing.

## Architecture

- **Framework**: Go with Chi router
- **Database**: SQLite for data persistence
- **Queue System**: ChronoQueue (using official `github.com/adrien19/chronoqueue/client`)
- **API**: RESTful endpoints with JSON responses

## Project Structure

```
backend/
├── main.go                 # HTTP server and app initialization
├── internal/
│   ├── api/               # HTTP handlers
│   │   └── handlers.go
│   ├── db/                # Database layer
│   │   ├── database.go    # SQLite setup and schema
│   │   ├── interviews.go  # Interview CRUD operations
│   │   ├── evaluations.go # Evaluation CRUD operations
│   │   └── reports.go     # Report CRUD operations
│   └── models/            # Data models and types
│       └── models.go
└── go.mod                 # Go dependencies
```

## ChronoQueue Integration

The backend uses the **official ChronoQueue Go client** (`github.com/adrien19/chronoqueue/client`) instead of a custom wrapper. This provides:

- Full API support (CreateQueue, PostMessage, GetNextMessage, AcknowledgeMessage, etc.)
- Built-in retry logic and exponential backoff
- Automatic heartbeat management for long-running tasks
- TLS support
- Connection pooling

### Queues

The platform uses 4 queues for different workflows:

1. **interview-scheduler** - Handles interview scheduling and notifications
2. **evaluation-processor** - Processes evaluation submissions
3. **report-generator** - Generates consolidated reports
4. **notification-sender** - Sends emails and notifications

All queues are configured with:

- 3 max retry attempts
- 30s lease duration
- 5s invisibility duration
- Auto-created Dead Letter Queues (DLQ)

## Building

```bash
go build -o backend
```

## Running

```bash
# Start with defaults
./backend

# Custom configuration
./backend -port 8080 -chronoqueue localhost:50051 -db ./data.db
```

### Command-line Flags

- `-port` - HTTP server port (default: 8080)
- `-chronoqueue` - ChronoQueue gRPC address (default: localhost:50051)
- `-db` - SQLite database path (default: ./interview-platform.db)

## API Endpoints

### Dashboard

- `GET /api/dashboard/stats` - Get dashboard statistics
- `GET /api/dashboard/activity` - Get recent activity

### Interviews

- `GET /api/interviews` - List interviews (with pagination)
- `POST /api/interviews` - Create new interview
- `GET /api/interviews/:id` - Get interview details
- `PUT /api/interviews/:id` - Update interview
- `POST /api/interviews/:id/cancel` - Cancel interview
- `POST /api/interviews/:id/complete` - Mark as completed
- `GET /api/interviews/:id/evaluations` - Get evaluations
- `GET /api/interviews/:id/report` - Get report

### Evaluations

- `GET /api/evaluations` - List all evaluations
- `POST /api/evaluations` - Create evaluation
- `GET /api/evaluations/pending` - Get pending evaluations
- `GET /api/evaluations/:id` - Get evaluation details
- `PUT /api/evaluations/:id` - Update evaluation

### Reports

- `GET /api/reports` - List all reports
- `POST /api/reports/generate` - Generate new report
- `GET /api/reports/:id` - Get report details
- `POST /api/reports/:id/send` - Send report via email
- `GET /api/reports/:id/pdf` - Download PDF

### Queues (Monitoring)

- `GET /api/queues` - List all queues
- `GET /api/queues/:name/stats` - Get queue statistics
- `GET /api/queues/:name/messages` - Get recent messages

## Development

### Prerequisites

1. **ChronoQueue Server** running on `localhost:50051`
2. **Go 1.21+**

### Local Development

```bash
# Run with hot reload (using air or similar)
go run main.go

# Or build and run
go build -o backend && ./backend
```

### Testing

```bash
# Run tests
go test ./...

# With coverage
go test -cover ./...
```

## Dependencies

The backend uses the local ChronoQueue module via a `replace` directive in `go.mod`:

```go
replace github.com/adrien19/chronoqueue => ../../..
```

This allows the backend to use the latest local version of ChronoQueue during development.

