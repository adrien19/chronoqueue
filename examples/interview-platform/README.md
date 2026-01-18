# 📋 Interview Evaluation Platform

> **A comprehensive sample application demonstrating all ChronoQueue features through a practical interview evaluation system.**

This example application showcases how to integrate ChronoQueue into a real-world application, demonstrating priority queues, scheduled messages, DLQ handling, schema validation, multi-tenant isolation, and more.

## 🎯 Purpose

This is a **learning-focused sample application** designed to:

- Demonstrate **all ChronoQueue features** in a practical context
- Provide **reference implementation** patterns for developers
- Show **best practices** for queue-based architectures
- Offer **hands-on experience** with message queue workflows

**Note**: This is not a production-ready interview platform. The focus is on demonstrating ChronoQueue capabilities with minimalist supporting infrastructure.

---

## 📖 Application Overview

### Concept

An interview evaluation platform where:

- **Candidates** submit interview responses (video/text)
- **Responses** are queued for evaluation by different types of reviewers
- **Evaluations** go through multiple stages with priority handling
- **Failed evaluations** retry automatically with Dead Letter Queue (DLQ) support
- **Scheduled batch processing** for analytics and reports
- **Real-time status updates** via Server-Sent Events (SSE)

### ChronoQueue Features Demonstrated

| Feature | Demonstration |
|---------|---------------|
| **Priority Queues** | Urgent candidates (CEO positions) processed before standard candidates |
| **Scheduled Messages** | Delay evaluations until business hours or specific times |
| **Calendar Schedules** | Daily analytics reports, weekly summaries (business days only) |
| **DLQ & Retry Logic** | Failed AI API calls retry with exponential backoff, then move to DLQ |
| **Schema Validation** | Validate evaluation request structure before queuing |
| **Multi-tenant Isolation** | Separate queues per company with complete isolation |
| **Heartbeat & Lease Renewal** | Long-running evaluations maintain lease to prevent redelivery |
| **Message Metadata** | Track evaluation stages, timestamps, and context |
| **Message Retention Policy** | Configurable retention: immediate delete, timed retention, or forever |

---

## 🏗️ Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Frontend (Next.js)                       │
│  - Candidate Dashboard  - Recruiter Dashboard  - Admin UI   │
│  - Real-time Updates (SSE)  - Queue Visualization           │
└────────────────────────┬────────────────────────────────────┘
                         │ REST/gRPC
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                   Backend API Server (Go)                    │
│  - REST endpoints  - Auth middleware  - ChronoQueue client  │
└────────────────────────┬────────────────────────────────────┘
                         │ gRPC
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                    ChronoQueue Server                        │
│  - Message queuing  - Priority handling  - Scheduling       │
└────────────────────────┬────────────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         ↓               ↓               ↓
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ Evaluation  │  │Notification │  │ Analytics   │
│   Worker    │  │   Worker    │  │   Worker    │
└─────────────┘  └─────────────┘  └─────────────┘
         │               │               │
         └───────────────┼───────────────┘
                         ↓
                 ┌───────────────┐
                 │SQLite Database│
                 └───────────────┘
```

### Components

#### 1. **Frontend** (Next.js + Tailwind + Clerk)

- **Technology**: Next.js 14 (App Router), TypeScript, Tailwind CSS
- **Authentication**: Clerk
- **Features**:
  - Candidate dashboard (submit interviews, view status)
  - Recruiter dashboard (view evaluations, manage queue)
  - Admin dashboard (DLQ management, queue health)
  - Real-time queue visualization
  - SSE for live status updates

#### 2. **Backend API Server** (Go)

- **Technology**: Go, Chi Router, gRPC client
- **Responsibilities**:
  - REST API endpoints for frontend
  - ChronoQueue client wrapper
  - Authentication middleware (Clerk JWT validation)
  - SSE handler for real-time updates
  - Business logic orchestration

#### 3. **Worker Services** (Go)

- **Evaluation Worker**: Processes interview evaluations (mock AI service)
- **Notification Worker**: Sends email/webhook notifications with retry logic
- **Analytics Worker**: Scheduled report generation (daily/weekly/monthly)
- **DLQ Processor**: Monitors and manages dead letter queues

#### 4. **Storage** (SQLite)

- **Minimalist approach** for simplicity
- **Tables**:
  - `companies` - Multi-tenant support
  - `candidates` - Candidate profiles
  - `interviews` - Interview submissions
  - `evaluations` - Evaluation requests
  - `evaluation_results` - Completed evaluations
  - `queue_metrics` - Queue health monitoring

#### 5. **External Services** (Mocked)

- **Mock AI Evaluation Service**: Simulates AI scoring with random delays and occasional failures
- **Mock Webhook Endpoint**: Receives status update notifications

---

## 📊 Queue Architecture

### Queue Structure

```yaml
# Priority Queue Hierarchy
evaluation-urgent:
  type: PRIORITY
  priority: 90-100
  use_case: VIP candidates, C-level positions, urgent evaluations
  max_attempts: 3
  dlq: evaluation-urgent-dlq
  
evaluation-standard:
  type: PRIORITY
  priority: 40-60
  use_case: Regular candidates, standard processing
  max_attempts: 5
  dlq: evaluation-standard-dlq
  
evaluation-bulk:
  type: SIMPLE
  priority: 10-20
  use_case: Batch imports, re-evaluations, low-priority
  max_attempts: 3
  dlq: evaluation-bulk-dlq

# Notification Queue
notifications:
  type: PRIORITY
  priority: 50-80
  use_case: Email/webhook notifications
  max_attempts: 5
  dlq: notifications-dlq
  retry_strategy: exponential_backoff

# Scheduled Analytics
analytics-scheduled:
  type: PRIORITY
  schedule: calendar
  use_case: Daily reports (business days), weekly summaries
  cron_expressions:
    - "0 8 * * 1-5"  # Daily at 8 AM (weekdays)
    - "0 9 * * 1"    # Weekly on Monday at 9 AM

# Multi-tenant Queues
company-{id}-evaluations:
  type: PRIORITY
  use_case: Per-company isolated queues
  demonstrates: Tenant isolation
```

### Message Flow Examples

#### Example 1: Priority Queue Processing

```
Timeline:
11:00 AM - Intern candidate submits (priority: 15) → evaluation-bulk
11:05 AM - Senior Engineer submits (priority: 50) → evaluation-standard
11:10 AM - CEO candidate submits (priority: 95) → evaluation-urgent

Processing Order:
1. CEO candidate (priority: 95) ← Processed first
2. Senior Engineer (priority: 50) ← Processed second
3. Intern candidate (priority: 15) ← Processed last
```

#### Example 2: Scheduled Message

```
Scenario: Evaluation submitted outside business hours
- Submit: Friday 11:00 PM
- Scheduled for: Monday 9:00 AM
- InvisibilityDuration: 58 hours
- Worker retrieves: Monday 9:00 AM (business hours)
```

#### Example 3: DLQ & Retry Flow

```
Attempt 1: AI service timeout (30s) → Retry after 1s
Attempt 2: AI service error 500 → Retry after 2s
Attempt 3: AI service unavailable → Retry after 4s
Attempt 4: Max attempts reached → Move to DLQ

Admin Action:
- Views DLQ dashboard
- Investigates error details
- Fixes root cause (AI service restored)
- Re-queues message from DLQ
- Message successfully processed
```

---

## 🎭 Feature Demonstrations

### 1. Priority Queue Demonstration

**Scenario**: Three candidates submit interviews simultaneously

```go
// CEO Position - Highest Priority
candidateCEO := PostMessage(queue: "evaluation-urgent", 
  priority: 95,
  data: {candidate_id: "123", position: "CEO"})

// Senior Engineer - Medium Priority  
candidateSE := PostMessage(queue: "evaluation-standard",
  priority: 50,
  data: {candidate_id: "456", position: "Senior Engineer"})

// Intern - Low Priority
candidateIntern := PostMessage(queue: "evaluation-bulk",
  priority: 15,
  data: {candidate_id: "789", position: "Intern"})

// Processing Order: CEO → Senior Engineer → Intern
// UI shows real-time queue position changes
```

**Demo Script**: `scripts/demo-priority.sh`

---

### 2. Scheduled Messages

**Scenario**: Delay evaluation until business hours

```go
// Current time: Friday 11:00 PM
// Business hours: Monday-Friday 9 AM - 5 PM

scheduleTime := NextBusinessDay(time.Now(), hour: 9)
invisibilityDuration := scheduleTime.Sub(time.Now())

PostMessage(
  queue: "evaluation-standard",
  invisibility_duration: invisibilityDuration, // ~34 hours
  data: {candidate_id: "123", scheduled: true}
)

// Message invisible until Monday 9 AM
// Worker retrieves Monday 9:00 AM
```

**Demo Script**: `scripts/demo-scheduled.sh`

---

### 3. Calendar Schedules

**Scenario**: Daily analytics report generation

```yaml
schedule:
  schedule_id: "daily-analytics-report"
  queue_name: "analytics-scheduled"
  schedule_config:
    cron_schedule: "0 8 * * 1-5"  # Weekdays at 8 AM
  payload:
    type: "daily_report"
    metrics: ["evaluations_completed", "avg_score", "queue_health"]
```

**Demo Script**: `scripts/demo-calendar.sh`

---

### 4. DLQ & Retry Logic

**Scenario**: AI service temporarily unavailable

```go
// Evaluation Worker
for {
  msg := queue.GetNextMessage()
  
  // Attempt to call AI service
  result, err := aiService.Evaluate(msg.Data)
  if err != nil {
    // Message automatically retries based on MaxAttempts
    // Exponential backoff: 1s, 2s, 4s, 8s...
    log.Error("Evaluation failed, will retry", err)
    return err // ChronoQueue handles retry
  }
  
  // Success - acknowledge message
  queue.AckMessage(msg.MessageID)
}

// After MaxAttempts exceeded → Moved to DLQ
// Admin sees in DLQ dashboard:
// - Error: "AI service timeout after 30s"
// - Attempts: 5/5
// - Last error: "connection refused"
// - Actions: [Re-queue] [Delete] [Inspect]
```

**Demo Script**: `scripts/demo-dlq.sh`

---

### 5. Schema Validation

**Scenario**: Ensure evaluation request integrity

```json
// schema: evaluation_request.json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["candidate_id", "interview_id", "rubric"],
  "properties": {
    "candidate_id": {"type": "string"},
    "interview_id": {"type": "string"},
    "rubric": {
      "type": "object",
      "required": ["technical_skills", "communication"],
      "properties": {
        "technical_skills": {"type": "number", "min": 0, "max": 100},
        "communication": {"type": "number", "min": 0, "max": 100}
      }
    }
  }
}
```

```go
// Register schema
client.RegisterSchema(ctx, &RegisterSchemaRequest{
  SchemaId: "evaluation-request-v1",
  SchemaDefinition: evaluationRequestSchema,
})

// Post message with validation
client.PostMessage(ctx, &PostMessageRequest{
  QueueName: "evaluation-standard",
  Message: message,
  SchemaId: "evaluation-request-v1", // Validates before queuing
})
// Invalid messages rejected immediately
```

---

### 6. Heartbeat & Lease Renewal

**Scenario**: Long-running AI evaluation (3 minutes)

```go
// Worker gets message with 30s lease
msg := queue.GetNextMessage(leaseDuration: 30 * time.Second)

// Start long-running evaluation
go func() {
  ticker := time.NewTicker(20 * time.Second)
  defer ticker.Stop()
  
  for range ticker.C {
    // Renew lease every 20s during processing
    queue.RenewLease(msg.MessageID, extension: 30 * time.Second)
    log.Info("Lease renewed for message", msg.MessageID)
  }
}()

// Process evaluation (3 minutes)
result := aiService.EvaluateInDepth(msg.Data) // 3 min

// Complete and acknowledge
queue.AckMessage(msg.MessageID)
```

---

### 7. Multi-tenant Isolation

**Scenario**: Multiple companies using the platform

```go
// Company A: Acme Corp
acmeQueue := "company-acme-evaluations"
PostMessage(queue: acmeQueue, data: {candidate: "Alice"})

// Company B: TechCorp
techcorpQueue := "company-techcorp-evaluations"
PostMessage(queue: techcorpQueue, data: {candidate: "Bob"})

// Complete isolation:
// - Acme worker only consumes from acmeQueue
// - TechCorp worker only consumes from techcorpQueue
// - No cross-tenant data access
// - Separate metrics and monitoring
```

---

### 8. Message Retention Policy

**Scenario**: Different retention requirements for different queue types

```go
// Queue 1: Transient scheduling events - delete immediately after processing
schedulerQueue := client.QueueOptions{
  LeaseDuration:   "30s",
  AutoCreateDLQ:   true,
  RetentionPolicy: nil, // DELETE_IMMEDIATELY (default behavior)
}

// Queue 2: Evaluation audit trail - retain for 7 days for compliance
evaluationQueue := client.QueueOptions{
  LeaseDuration: "30s",
  AutoCreateDLQ: true,
  RetentionPolicy: &client.RetentionPolicyOption{
    Mode:             client.RETENTION_RETAIN_DURATION,
    RetentionSeconds: 7 * 24 * 60 * 60, // 7 days
  },
}

// Queue 3: Report compliance - retain for 30 days
reportQueue := client.QueueOptions{
  LeaseDuration: "30s",
  AutoCreateDLQ: true,
  RetentionPolicy: &client.RetentionPolicyOption{
    Mode:             client.RETENTION_RETAIN_DURATION,
    RetentionSeconds: 30 * 24 * 60 * 60, // 30 days
  },
}

// Queue 4: Notification history - retain forever for complete audit
notificationQueue := client.QueueOptions{
  LeaseDuration: "30s",
  AutoCreateDLQ: true,
  RetentionPolicy: &client.RetentionPolicyOption{
    Mode: client.RETENTION_RETAIN_FOREVER,
  },
}

// Retention modes:
// - DELETE_IMMEDIATELY: Message hard-deleted on ack (default, backward compatible)
// - RETAIN_DURATION: Soft-delete, auto-cleanup after retention_seconds
// - RETAIN_FOREVER: Soft-delete, no auto-cleanup (manual cleanup required)
```

**Use Cases**:

- **Transient queues**: Use `DELETE_IMMEDIATELY` for ephemeral events
- **Audit trails**: Use `RETAIN_DURATION` with 7-30 days for compliance
- **Legal holds**: Use `RETAIN_FOREVER` for messages that must never be auto-deleted

---

## 🗂️ Project Structure

```
chronoqueue/examples/interview-platform/
├── README.md                          # This file
├── docker-compose.yml                 # All services orchestration
├── .env.example                       # Environment variables template
│
├── docs/
│   ├── ARCHITECTURE.md               # Detailed architecture
│   ├── API_DOCUMENTATION.md          # REST API reference
│   ├── QUEUE_DESIGN.md               # Queue patterns and best practices
│   └── DEPLOYMENT.md                 # Deployment guide
│
├── frontend/                          # Next.js application
│   ├── app/
│   │   ├── (auth)/
│   │   │   ├── sign-in/             # Clerk sign-in
│   │   │   └── sign-up/             # Clerk sign-up
│   │   ├── candidate/               # Candidate dashboard
│   │   │   ├── page.tsx
│   │   │   ├── submit/              # Submit interview
│   │   │   └── status/              # Evaluation status
│   │   ├── recruiter/               # Recruiter dashboard
│   │   │   ├── page.tsx
│   │   │   ├── queue/               # Queue management
│   │   │   └── results/             # Evaluation results
│   │   ├── admin/                   # Admin dashboard
│   │   │   ├── page.tsx
│   │   │   ├── dlq/                 # DLQ management
│   │   │   ├── metrics/             # Queue health
│   │   │   └── schedules/           # Schedule configuration
│   │   └── api/                     # Next.js API routes (proxy)
│   ├── components/
│   │   ├── ui/                      # shadcn/ui components
│   │   ├── EvaluationQueue.tsx      # Real-time queue visualization
│   │   ├── PriorityBadge.tsx
│   │   ├── DLQManager.tsx           # DLQ UI
│   │   ├── ScheduleViewer.tsx       # Calendar schedules UI
│   │   └── QueueMetrics.tsx         # Health dashboard
│   ├── lib/
│   │   ├── chronoqueue-client.ts    # API client wrapper
│   │   ├── sse-client.ts            # Server-sent events client
│   │   └── utils.ts
│   ├── package.json
│   └── next.config.js
│
├── backend/                           # Go API server
│   ├── cmd/
│   │   └── server/
│   │       └── main.go               # HTTP server entry point
│   ├── internal/
│   │   ├── api/
│   │   │   ├── handlers.go          # REST endpoint handlers
│   │   │   ├── middleware.go        # Auth, CORS, logging
│   │   │   └── sse.go               # Server-sent events
│   │   ├── chronoqueue/
│   │   │   ├── client.go            # ChronoQueue client wrapper
│   │   │   ├── queues.go            # Queue configurations
│   │   │   ├── schemas.go           # Message schemas
│   │   │   └── publisher.go         # Message publishing helpers
│   │   ├── database/
│   │   │   ├── sqlite.go            # SQLite connection
│   │   │   ├── models.go            # Data models
│   │   │   └── migrations.go        # Schema migrations
│   │   └── auth/
│   │       └── clerk.go             # Clerk JWT validation
│   ├── go.mod
│   └── go.sum
│
├── workers/                           # Worker services
│   ├── cmd/
│   │   ├── evaluation-worker/
│   │   │   └── main.go
│   │   ├── notification-worker/
│   │   │   └── main.go
│   │   ├── analytics-worker/
│   │   │   └── main.go
│   │   └── dlq-processor/
│   │       └── main.go              # DLQ management worker
│   ├── internal/
│   │   ├── evaluator/
│   │   │   ├── ai_mock.go          # Mock AI service
│   │   │   ├── processor.go        # Evaluation processing
│   │   │   └── heartbeat.go        # Lease renewal
│   │   ├── notifier/
│   │   │   ├── email_mock.go
│   │   │   └── webhook.go
│   │   ├── analytics/
│   │   │   ├── reporter.go         # Report generation
│   │   │   └── aggregator.go
│   │   └── shared/
│   │       └── worker.go           # Base worker implementation
│   ├── go.mod
│   └── go.sum
│
├── database/
│   ├── schema.sql                    # SQLite schema definition
│   ├── seed.sql                      # Sample data for testing
│   └── .gitkeep                      # (interview_platform.db gitignored)
│
├── config/
│   ├── queues.yaml                   # Queue configurations
│   ├── schedules.yaml                # Calendar schedule definitions
│   └── schemas/                      # JSON schemas
│       ├── evaluation_request.json
│       ├── notification_request.json
│       └── analytics_event.json
│
└── scripts/
    ├── setup.sh                      # Initial setup script
    ├── seed-data.sh                  # Load sample data
    ├── demo-priority.sh              # Demo: Priority queues
    ├── demo-scheduled.sh             # Demo: Scheduled messages
    ├── demo-calendar.sh              # Demo: Calendar schedules
    ├── demo-dlq.sh                   # Demo: DLQ handling
    ├── demo-multitenant.sh           # Demo: Multi-tenant isolation
    └── simulate-load.sh              # Load testing
```

---

## 🛠️ Technology Stack

### Frontend

- **Framework**: Next.js 14 (App Router)
- **Language**: TypeScript
- **Styling**: Tailwind CSS
- **Authentication**: Clerk
- **UI Components**: shadcn/ui
- **State Management**: TanStack Query (React Query)
- **Charts**: Recharts
- **Real-time**: Server-Sent Events (SSE)

### Backend

- **Language**: Go 1.21+
- **HTTP Router**: Chi
- **Database**: SQLite (mattn/go-sqlite3)
- **Queue Client**: ChronoQueue gRPC Client
- **Authentication**: Clerk Go SDK
- **Validation**: go-playground/validator

### Workers

- **Language**: Go 1.21+
- **Queue Client**: ChronoQueue gRPC Client
- **Database**: SQLite (shared with backend)

### Infrastructure

- **Containerization**: Docker & Docker Compose
- **Queue System**: ChronoQueue Server
- **Cache/Store**: Redis (ChronoQueue dependency)

---

## 🚀 Getting Started

### Prerequisites

- **Docker** & **Docker Compose** (for ChronoQueue server & Redis)
- **Go 1.21+** (for backend & workers)
- **Node.js 18+** & **npm/pnpm** (for frontend)
- **Clerk Account** (free tier available at <https://clerk.com>)

### Installation Steps

1. **Clone the repository**

   ```bash
   cd /workspaces/chronoqueue/examples/interview-platform
   ```

2. **Run setup script**

   ```bash
   ./scripts/setup.sh
   ```

   This will:
   - Install frontend dependencies
   - Install backend dependencies
   - Create SQLite database
   - Set up environment variables

3. **Configure environment**

   ```bash
   cp .env.example .env
   # Edit .env and add your Clerk API keys
   ```

4. **Start ChronoQueue server**

   ```bash
   # From chronoqueue root
   docker-compose up -d
   # Or use the main server binary
   ```

5. **Start the backend API**

   ```bash
   cd backend
   go run cmd/server/main.go
   # Runs on http://localhost:8081
   ```

6. **Start workers**

   ```bash
   # Terminal 1: Evaluation Worker
   cd workers
   go run cmd/evaluation-worker/main.go
   
   # Terminal 2: Notification Worker
   go run cmd/notification-worker/main.go
   
   # Terminal 3: Analytics Worker
   go run cmd/analytics-worker/main.go
   ```

7. **Start frontend**

   ```bash
   cd frontend
   npm install
   npm run dev
   # Runs on http://localhost:3000
   ```

8. **Seed sample data**

   ```bash
   ./scripts/seed-data.sh
   ```

### Quick Test

```bash
# Run the priority queue demo
./scripts/demo-priority.sh

# Expected output:
# ✓ Posted CEO candidate (priority: 95)
# ✓ Posted Senior Engineer (priority: 50)
# ✓ Posted Intern (priority: 15)
# → Worker processing: CEO candidate (processed first)
# → Worker processing: Senior Engineer (processed second)
# → Worker processing: Intern (processed third)
```

---

## 📚 Learning Paths

### Path 1: Queue Basics (Beginner)

1. Run `demo-priority.sh` - Understand priority queues
2. Explore `backend/internal/chronoqueue/client.go` - See how to post messages
3. Review `workers/cmd/evaluation-worker/main.go` - Learn worker patterns
4. Open frontend and submit an interview - See end-to-end flow

### Path 2: Advanced Features (Intermediate)

1. Run `demo-scheduled.sh` - Learn scheduled messages
2. Run `demo-dlq.sh` - Understand error handling
3. Configure a calendar schedule in `config/schedules.yaml`
4. Implement schema validation for custom message types

### Path 3: Production Patterns (Advanced)

1. Study `workers/internal/evaluator/heartbeat.go` - Lease management
2. Implement multi-tenant isolation in your own app
3. Review `backend/internal/api/sse.go` - Real-time updates
4. Optimize worker concurrency and throughput

---

## 🎓 Key Concepts Explained

### When to Use Priority Queues

- **Use when**: Different messages have different urgency levels
- **Example**: VIP customers, urgent alerts, time-sensitive tasks
- **Implementation**: Assign priority 0-100 (higher = more urgent)

### When to Use Scheduled Messages

- **Use when**: Messages should be processed at specific times
- **Example**: Send reminder at 9 AM, process batch at midnight
- **Implementation**: Set `InvisibilityDuration` or use cron schedules

### When to Use DLQ

- **Use when**: Messages might fail and need manual intervention
- **Example**: External API failures, data validation errors
- **Implementation**: Set `MaxAttempts` and configure DLQ

### When to Use Schema Validation

- **Use when**: Message structure must be guaranteed
- **Example**: Payment processing, critical workflows
- **Implementation**: Register JSON schema, validate on post

---

## 🔧 Configuration

### Queue Configuration Example

```yaml
# config/queues.yaml
queues:
  - name: evaluation-urgent
    type: PRIORITY
    metadata:
      invisibility_duration: 0s
      max_attempts: 3
      auto_create_dlq: true
      dlq_name: evaluation-urgent-dlq
    
  - name: evaluation-standard
    type: PRIORITY
    metadata:
      invisibility_duration: 2s
      max_attempts: 5
      auto_create_dlq: true
```

### Schedule Configuration Example

```yaml
# config/schedules.yaml
schedules:
  - schedule_id: daily-analytics
    queue_name: analytics-scheduled
    schedule_config:
      cron_schedule: "0 8 * * 1-5"  # Weekdays at 8 AM
    payload:
      type: daily_report
      
  - schedule_id: weekly-summary
    queue_name: analytics-scheduled
    schedule_config:
      cron_schedule: "0 9 * * 1"    # Mondays at 9 AM
    payload:
      type: weekly_summary
```

---

## 📊 Monitoring & Observability

### Queue Health Dashboard

- **Pending messages**: Messages waiting for processing
- **Processing messages**: Messages currently being worked on
- **Failed messages**: Messages in DLQ
- **Throughput**: Messages processed per second
- **Average processing time**: Time from queue to completion

### Worker Metrics

- **Messages processed**: Total successful completions
- **Errors**: Total failures before DLQ
- **Current load**: Active message processing count
- **Lease renewals**: Heartbeat activity

### Available Endpoints

- `GET /api/metrics/queues` - Queue statistics
- `GET /api/metrics/workers` - Worker health
- `GET /api/dlq/{queue_name}` - DLQ messages
- `GET /api/schedules` - Active schedules

---

## 🐛 Troubleshooting

### Common Issues

**Issue**: Worker not receiving messages

- **Check**: ChronoQueue server running
- **Check**: Queue created with correct name
- **Check**: Messages in PENDING state (not INVISIBLE)
- **Solution**: Wait for invisibility period to expire

**Issue**: Messages going to DLQ immediately

- **Check**: Schema validation configuration
- **Check**: Worker error logs
- **Solution**: Review message format, check MaxAttempts

**Issue**: Frontend not showing real-time updates

- **Check**: SSE endpoint accessible
- **Check**: CORS configuration
- **Solution**: Check browser console, verify API connection

---

## 📖 Additional Resources

- **ChronoQueue Documentation**: ../../README.md
- **API Reference**: docs/API_DOCUMENTATION.md
- **Queue Design Patterns**: docs/QUEUE_DESIGN.md
- **Deployment Guide**: docs/DEPLOYMENT.md

---

## 🤝 Contributing

This is a sample application for educational purposes. Contributions that improve:

- Documentation clarity
- Code examples
- Additional feature demonstrations
- Bug fixes

are welcome!

---

## 📄 License

This example application is part of the ChronoQueue project and follows the same license.

---

## 🎯 Next Steps

1. **Run the demos**: Start with `./scripts/demo-priority.sh`
2. **Explore the code**: Read through `backend/internal/chronoqueue/`
3. **Build something**: Modify the UI or add a new worker
4. **Learn patterns**: Study how features are implemented
5. **Apply to your project**: Use these patterns in your own applications

**Ready to start?** Follow the [Getting Started](#-getting-started) section above!
