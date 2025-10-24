# Architecture Documentation - Interview Evaluation Platform

> Detailed technical architecture, design decisions, and implementation patterns

## Table of Contents

- [System Overview](#system-overview)
- [Component Architecture](#component-architecture)
- [Queue Design](#queue-design)
- [Data Flow](#data-flow)
- [Design Decisions](#design-decisions)
- [Scalability Considerations](#scalability-considerations)

---

## System Overview

The Interview Evaluation Platform is built using a **queue-centric microservices architecture** where ChronoQueue acts as the central message broker coordinating all asynchronous operations.

### Architecture Principles

1. **Separation of Concerns**: Frontend, API, and Workers are independent services
2. **Asynchronous Processing**: All heavy operations handled through queues
3. **Fault Tolerance**: Built-in retry logic and DLQ handling
4. **Real-time Updates**: SSE for live status updates to UI
5. **Multi-tenancy**: Complete isolation between companies

---

## Component Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         FRONTEND LAYER                          │
│  Next.js 14 App (React Server Components + Client Components)  │
│                                                                 │
│  Routes:                                                        │
│  ├── /candidate/*    (Candidate dashboard & submission)        │
│  ├── /recruiter/*    (Queue management & results)              │
│  └── /admin/*        (DLQ, metrics, schedules)                 │
└────────────────────────┬────────────────────────────────────────┘
                         │ HTTP/REST + SSE
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│                       BACKEND API LAYER                         │
│  Go HTTP Server (Chi Router)                                    │
│                                                                 │
│  Responsibilities:                                              │
│  ├── REST API endpoints                                         │
│  ├── Authentication (Clerk JWT validation)                      │
│  ├── ChronoQueue client operations                             │
│  ├── Database CRUD operations                                   │
│  ├── SSE connection management                                  │
│  └── Business logic orchestration                              │
└────────────────────────┬────────────────────────────────────────┘
                         │ gRPC
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│                    CHRONOQUEUE LAYER                            │
│  Message Queue System                                           │
│                                                                 │
│  Queues:                                                        │
│  ├── evaluation-urgent       (Priority: 90-100)                │
│  ├── evaluation-standard     (Priority: 40-60)                 │
│  ├── evaluation-bulk         (Priority: 10-20)                 │
│  ├── notifications           (Priority: 50-80)                 │
│  ├── analytics-scheduled     (Cron-based)                      │
│  └── company-{id}-evaluations (Multi-tenant)                   │
│                                                                 │
│  Features Used:                                                 │
│  ├── Priority queue ordering                                    │
│  ├── Scheduled/delayed messages                                 │
│  ├── Calendar-based schedules                                   │
│  ├── Dead letter queues (DLQ)                                  │
│  ├── Schema validation                                          │
│  └── Lease/heartbeat management                                │
└────────────────────────┬────────────────────────────────────────┘
                         │ gRPC (Consumer)
         ┌───────────────┼───────────────┬──────────────┐
         ↓               ↓               ↓              ↓
┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ Evaluation  │ │Notification │ │ Analytics   │ │    DLQ      │
│   Worker    │ │   Worker    │ │   Worker    │ │  Processor  │
│             │ │             │ │             │ │             │
│ - Polls     │ │ - Polls     │ │ - Polls     │ │ - Monitors  │
│   queue     │ │   queue     │ │   scheduled │ │   DLQs      │
│ - Calls AI  │ │ - Sends     │ │ - Generates │ │ - Alerts    │
│   service   │ │   emails/   │ │   reports   │ │ - Requeues  │
│ - Updates   │ │   webhooks  │ │ - Updates   │ │             │
│   DB        │ │ - Retries   │ │   metrics   │ │             │
│ - Acks msg  │ │   failures  │ │             │ │             │
└─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘
         │               │               │              │
         └───────────────┴───────────────┴──────────────┘
                         │
                         ↓
                ┌─────────────────┐
                │SQLite Database  │
                │                 │
                │ Tables:         │
                │ - companies     │
                │ - candidates    │
                │ - interviews    │
                │ - evaluations   │
                │ - results       │
                │ - queue_metrics │
                └─────────────────┘
```

---

## Queue Design

### Queue Hierarchy

```yaml
# Priority-based Routing
evaluation-urgent:
  type: PRIORITY
  priority_range: 90-100
  use_case: C-level positions, VIP candidates
  max_attempts: 3
  retry_strategy: exponential_backoff
  dlq: evaluation-urgent-dlq
  
evaluation-standard:
  type: PRIORITY
  priority_range: 40-60
  use_case: Regular positions
  max_attempts: 5
  retry_strategy: exponential_backoff
  dlq: evaluation-standard-dlq
  
evaluation-bulk:
  type: SIMPLE
  priority_range: 10-20
  use_case: Batch processing, low-priority
  max_attempts: 3
  dlq: evaluation-bulk-dlq
```

### Queue Selection Logic

```go
func DetermineQueue(interview Interview) string {
    // Priority calculation based on multiple factors
    priority := CalculatePriority(interview)
    
    switch {
    case priority >= 90:
        return "evaluation-urgent"
    case priority >= 40:
        return "evaluation-standard"
    default:
        return "evaluation-bulk"
    }
}

func CalculatePriority(interview Interview) int {
    basePriority := 50
    
    // Job level multiplier
    switch interview.JobLevel {
    case "Executive", "C-Level":
        basePriority += 40
    case "Senior":
        basePriority += 20
    case "Mid":
        basePriority += 10
    case "Junior", "Intern":
        basePriority -= 20
    }
    
    // Urgency flag
    if interview.IsUrgent {
        basePriority += 20
    }
    
    // VIP company
    if interview.Company.IsVIP {
        basePriority += 15
    }
    
    // Age penalty (older interviews get higher priority)
    hoursSinceSubmission := time.Since(interview.SubmittedAt).Hours()
    if hoursSinceSubmission > 24 {
        basePriority += int(hoursSinceSubmission / 24 * 5)
    }
    
    // Clamp to valid range
    return clamp(basePriority, 0, 100)
}
```

---

## Data Flow

### Flow 1: Interview Submission → Evaluation

```
1. Candidate submits interview via frontend
   POST /api/interviews
   
2. Backend API:
   ├── Validates input
   ├── Authenticates user (Clerk)
   ├── Stores interview in SQLite
   ├── Determines queue & priority
   └── Posts message to ChronoQueue
   
3. ChronoQueue:
   ├── Validates against schema (if configured)
   ├── Stores message with priority
   └── Makes available based on invisibility duration
   
4. Evaluation Worker:
   ├── Polls queue (GetNextMessage)
   ├── Receives message with lease
   ├── Processes evaluation:
   │   ├── Calls mock AI service (10-30s)
   │   ├── Renews lease if processing > 20s
   │   └── Handles errors with retry
   ├── Updates database with results
   └── Acknowledges message
   
5. Real-time Update:
   ├── SSE pushes status to frontend
   └── UI updates evaluation status
```

### Flow 2: Failed Evaluation → DLQ → Recovery

```
1. Evaluation fails (AI service down)
   └── Worker returns error

2. ChronoQueue retry logic:
   Attempt 1: Immediate retry → Failed
   Attempt 2: 2s delay → Failed
   Attempt 3: 4s delay → Failed
   Attempt 4: 8s delay → Failed
   Attempt 5: Max attempts reached
   
3. Move to DLQ:
   ├── ChronoQueue moves message to evaluation-standard-dlq
   └── Message includes failure metadata

4. Admin notification:
   ├── SSE event to admin dashboard
   └── Alert email sent (via notification worker)

5. Admin recovery:
   ├── Reviews DLQ in admin UI
   ├── Investigates error: "AI service unavailable"
   ├── Waits for service restoration
   └── Re-queues message from DLQ

6. Successful processing:
   ├── Message back in main queue
   ├── Worker processes successfully
   └── Candidate receives results
```

### Flow 3: Scheduled Analytics Report

```
1. Calendar schedule trigger:
   Time: Monday 8:00 AM
   Schedule: "0 8 * * 1"
   
2. ChronoQueue:
   ├── Matches cron expression
   └── Posts message to analytics-scheduled queue
   
3. Analytics Worker:
   ├── Receives scheduled message
   ├── Queries database for weekly metrics
   ├── Generates report:
   │   ├── Evaluations completed
   │   ├── Average scores
   │   ├── Queue health metrics
   │   └── Top performers
   ├── Stores report in database
   └── Triggers notification worker

4. Notification Worker:
   ├── Sends report via email
   └── Posts to webhook (Slack, Teams, etc.)
```

---

## Design Decisions

### Why SQLite?

**Decision**: Use SQLite instead of PostgreSQL/MySQL

**Rationale**:

- ✅ **Simplicity**: Single file, no server setup
- ✅ **Learning focus**: Reduces infrastructure complexity
- ✅ **Sufficient**: Handles expected load for demo
- ✅ **Portability**: Easy to share and reset

**Trade-offs**:

- ❌ Limited concurrent writes
- ❌ Not production-grade for high scale
- ✅ Perfect for learning and demos

**Production Alternative**: PostgreSQL with connection pooling

---

### Why Mock AI Service?

**Decision**: Mock AI evaluation instead of real ML model

**Rationale**:

- ✅ **No dependencies**: No API keys or external services needed
- ✅ **Controlled failures**: Can simulate errors for DLQ demo
- ✅ **Adjustable latency**: Can test lease renewal with long processing
- ✅ **Focus on queues**: ChronoQueue is the star, not AI

**Implementation**:

```go
func MockAIEvaluate(interview Interview) (Result, error) {
    // Simulate processing time (10-30s)
    duration := rand.Intn(20) + 10
    time.Sleep(time.Duration(duration) * time.Second)
    
    // Simulate occasional failures (10% chance)
    if rand.Float32() < 0.1 {
        return Result{}, errors.New("AI service temporarily unavailable")
    }
    
    // Generate mock scores
    return Result{
        TechnicalScore: rand.Intn(40) + 60,  // 60-100
        CommunicationScore: rand.Intn(40) + 60,
        OverallScore: rand.Intn(40) + 60,
    }, nil
}
```

---

### Why SSE over WebSockets?

**Decision**: Server-Sent Events (SSE) for real-time updates

**Rationale**:

- ✅ **Simpler**: HTTP-based, no special protocol
- ✅ **One-way**: Server → Client (perfect for status updates)
- ✅ **Auto-reconnect**: Built-in browser support
- ✅ **Firewall-friendly**: HTTP/HTTPS only

**Trade-offs**:

- ❌ No client → server messaging (not needed here)
- ❌ Less efficient than WebSockets for high-frequency updates

**Production Consideration**: WebSockets if bidirectional needed

---

### Why Clerk for Auth?

**Decision**: Clerk instead of custom auth or Auth0

**Rationale**:

- ✅ **Free tier**: Generous limits for demos
- ✅ **Quick setup**: Pre-built UI components
- ✅ **Full-featured**: MFA, social logins, user management
- ✅ **Good DX**: Excellent documentation and Next.js integration

---

## Scalability Considerations

### Horizontal Scaling

**Workers**: Easily scalable

```bash
# Run multiple evaluation workers
docker-compose up --scale evaluation-worker=5

# Each worker competes for messages from ChronoQueue
# No coordination needed - ChronoQueue handles distribution
```

**Backend API**: Stateless, can scale horizontally

```bash
# Add more API instances behind load balancer
docker-compose up --scale backend-api=3

# Use Redis for session storage if needed
```

**ChronoQueue**: Scales with Redis cluster (see ChronoQueue docs)

---

### Performance Optimization

**Database**:

- Add indexes on frequently queried fields
- Consider read replicas for analytics
- Migrate to PostgreSQL for production

**Caching**:

- Add Redis cache for:
  - Queue metrics
  - Company configurations
  - User profiles

**Message Batching**:

```go
// Batch message posting for bulk operations
func BulkPostEvaluations(interviews []Interview) error {
    batch := make([]*PostMessageRequest, len(interviews))
    
    for i, interview := range interviews {
        batch[i] = &PostMessageRequest{
            QueueName: DetermineQueue(interview),
            Message:   BuildMessage(interview),
        }
    }
    
    // Post all in one gRPC call (if ChronoQueue supports)
    return chronoqClient.BulkPostMessages(ctx, batch)
}
```

---

### Monitoring Strategy

**Key Metrics to Track**:

```yaml
Queue Metrics:
  - pending_messages: Messages waiting for processing
  - processing_messages: Currently being processed
  - dlq_messages: Failed messages in DLQ
  - throughput: Messages/second
  - average_wait_time: Time in queue before processing
  
Worker Metrics:
  - messages_processed: Successful completions
  - errors_total: Failures before DLQ
  - processing_duration: Time to process message
  - lease_renewals: Heartbeat activity
  - worker_health: Last heartbeat timestamp
  
Application Metrics:
  - evaluation_requests: Total submissions
  - evaluation_completions: Successful evaluations
  - average_evaluation_time: End-to-end time
  - user_satisfaction: If feedback collected
```

**Alerting Rules**:

- DLQ size > 100 messages
- Queue depth > 1000 messages for > 5 minutes
- Worker not processing for > 2 minutes
- Average processing time > 5 minutes

---

## Security Considerations

### Authentication & Authorization

```go
// Middleware: Verify Clerk JWT
func AuthMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        token := ExtractBearerToken(r)
        
        claims, err := clerk.VerifyToken(token)
        if err != nil {
            http.Error(w, "Unauthorized", http.StatusUnauthorized)
            return
        }
        
        // Inject user context
        ctx := context.WithValue(r.Context(), "user_id", claims.Subject)
        ctx = context.WithValue(ctx, "company_id", claims.CompanyID)
        
        next.ServeHTTP(w, r.WithContext(ctx))
    })
}

// Authorization: Check company ownership
func RequireCompanyAccess(companyID string) Middleware {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            userCompanyID := r.Context().Value("company_id").(string)
            
            if userCompanyID != companyID {
                http.Error(w, "Forbidden", http.StatusForbidden)
                return
            }
            
            next.ServeHTTP(w, r)
        })
    }
}
```

### Message Security

```go
// Encrypt sensitive data in messages
func PostSensitiveEvaluation(interview Interview) error {
    // Encrypt PII before queuing
    encrypted, err := EncryptPII(interview.CandidateData)
    if err != nil {
        return err
    }
    
    message := &Message{
        MessageId: GenerateID(),
        Metadata: &Metadata{
            Payload: encrypted,
            EncryptionKey: "company-key-id", // Key reference, not actual key
        },
    }
    
    return chronoqClient.PostMessage(ctx, message)
}

// Worker decrypts before processing
func ProcessEncryptedEvaluation(msg *Message) error {
    keyID := msg.Metadata.EncryptionKey
    key := keyManager.GetKey(keyID)
    
    decrypted, err := Decrypt(msg.Metadata.Payload, key)
    if err != nil {
        return err
    }
    
    return ProcessEvaluation(decrypted)
}
```

---

## Future Enhancements

### Phase 2 Features

- [ ] Real AI integration (OpenAI, Anthropic)
- [ ] Video processing pipeline
- [ ] Advanced analytics dashboard
- [ ] Multi-language support
- [ ] Mobile app (React Native)

### Production Readiness

- [ ] Migrate to PostgreSQL
- [ ] Add comprehensive logging (structured)
- [ ] Implement distributed tracing (OpenTelemetry)
- [ ] Add Prometheus metrics
- [ ] Set up Grafana dashboards
- [ ] Implement rate limiting
- [ ] Add API versioning

---

## References

- [ChronoQueue Documentation](../../../README.md)
- [Queue Design Patterns](./QUEUE_DESIGN.md)
- [API Documentation](./API_DOCUMENTATION.md)
- [Deployment Guide](./DEPLOYMENT.md)
