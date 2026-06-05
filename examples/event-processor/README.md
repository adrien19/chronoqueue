# Event Processing System Demo

A comprehensive demonstration of ChronoQueue's SQL-based architecture for high-throughput event processing, webhooks, and notifications.

## 🎯 What This Demo Demonstrates

- ✅ **High-throughput message processing** - Process thousands of events efficiently
- ✅ **Bulk message posting** - Post multiple messages in a single API call (up to 1000)
- ✅ **Transaction modes** - ALL_OR_NOTHING (atomic) vs BEST_EFFORT (partial success)
- ✅ **Priority routing** - Critical/normal/low priority event handling
- ✅ **Scheduled/delayed messages** - Future message delivery using sorted sets
- ✅ **Automatic heartbeats** - Processing acknowledgment with lease renewal
- ✅ **Retry logic** - Exponential backoff for failed events
- ✅ **DLQ handling** - Dead letter queue for undeliverable events
- ✅ **Multiple worker types** - Email, webhook, and SMS processors
- ✅ **JSON payload files** - Easy event definition without CLI formatting
- ✅ **Message retention policies** - Configurable retention per queue (immediate delete, timed, or forever)

## 📋 Prerequisites

- Docker and Docker Compose (for PostgreSQL/SQLite)
- Go 1.21+
- ChronoQueue server running

## 🚀 Quick Start

### 1. Start ChronoQueue Server

```bash
# From the repository root
make docker-up

# Or start manually
chronoqueue server --dev
```

### 2. Build the Event Processor CLI

```bash
cd examples/event-processor
go build -o event-processor .
```

### 3. Initialize the System

```bash
# Create event queues for different priority levels
./event-processor init
```

Expected output:

```
Initializing Event Processing System...
  ✓ Created queue: email-notifications (dlq: email-notifications-dlq)
  ✓ Created queue: webhook-events (dlq: webhook-events-dlq)
  ✓ Created queue: sms-alerts (dlq: sms-alerts-dlq)

✨ System initialized successfully!

Queues created:
  • email-notifications - High-priority email notifications (7-day retention)
  • webhook-events - Webhook delivery with retries (30-day retention)
  • sms-alerts - SMS alerts for critical events (immediate delete)
```

## 📚 Step-by-Step Tutorial

### Step 1: Publish Events

ChronoQueue supports two ways to publish events: traditional one-by-one posting and high-performance bulk posting.

#### 1.1 Traditional Publishing (One-by-One)

Publish messages individually using the standard `publish` command:

```bash
./event-processor publish events/critical-webhook.json
```

**Expected Output:**

```
📤 Publishing events...
✓ Published event: evt-critical-001 (priority: 4, type: webhook)
  Queue: events-critical
  
📊 Summary:
  Total events: 1
  Critical: 1
  Normal: 0
  Low: 0
```

#### 1.2 Bulk Publishing (Recommended for Multiple Messages)

**NEW!** Use bulk posting to send multiple messages in a single API call for significantly better performance:

```bash
# Post with ALL_OR_NOTHING mode (default - atomic)
./event-processor publish-bulk events/bulk-demo.json

# Post with BEST_EFFORT mode (partial success allowed)
./event-processor publish-bulk events/bulk-demo.json --mode best-effort
```

**Expected Output (ALL_OR_NOTHING mode):**

```text
📦 Publishing 10 events in BULK mode from events/bulk-demo.json
   Transaction Mode: ALL-OR-NOTHING

📤 Posting 4 events to email-notifications...
  ✓ Success: 4/4 messages posted

📤 Posting 4 events to webhook-events...
  ✓ Success: 4/4 messages posted

📤 Posting 2 events to sms-alerts...
  ✓ Success: 2/2 messages posted

📊 Bulk Posting Summary:
  • Total Events: 10
  • Published: 10
  • Failed: 0
  • Duration: 234ms
  • Transaction Mode: ALL-OR-NOTHING

  By Queue:
    • email-notifications: 4 events
    • webhook-events: 4 events
    • sms-alerts: 2 events

  ⚡ Average: 23ms per message
```

**Benefits of Bulk Posting:**

- 🚀 **10-100x faster** than individual posts for large batches
- 🔒 **Atomic guarantees** with ALL_OR_NOTHING mode
- 📊 **Detailed per-message results** showing which messages succeeded/failed
- 💪 **Partial success handling** with BEST_EFFORT mode
- ⚡ **Single network round-trip** reduces latency

#### 1.3 Bulk Publishing with Scheduled Messages

Combine bulk posting with scheduled delivery:

```bash
./event-processor publish-bulk events/bulk-scheduled.json
```

This posts 10 messages scheduled for future delivery (5, 10, 15, and 30 minutes from now).

#### 1.4 Performance Comparison

Compare traditional vs bulk posting with a large batch:

```bash
# Traditional: Posts 20 messages one-by-one
time ./event-processor publish events/bulk-large-batch.json

# Bulk: Posts 20 messages in a single operation
time ./event-processor publish-bulk events/bulk-large-batch.json
```

You should see 5-10x performance improvement with bulk posting!

#### 1.5 Publish Batch of Mixed Priority Events

```bash
./event-processor publish events/mixed-batch.json
```

**Expected Output:**

```
📤 Publishing events...
✓ Published event: evt-001 (priority: 3, type: email)
✓ Published event: evt-002 (priority: 2, type: webhook)
✓ Published event: evt-003 (priority: 1, type: sms)
✓ Published event: evt-004 (priority: 4, type: webhook)

📊 Summary:
  Total events: 4
  Critical: 1 (priority 4)
  High: 1 (priority 3)
  Medium: 1 (priority 2)
  Low: 1 (priority 1)
  
⏱️  Published in 45ms
```

### Step 2: View Queue Statistics

```bash
./event-processor stats
```

**Expected Output:**

```
📊 Event Queue Statistics

Queue: events-critical
  ├─ Pending Messages: 2
  ├─ Running Messages: 0
  ├─ Completed: 0
  ├─ Priority Config: STRICT (high-priority first)
  └─ Status: Active

Queue: events-normal
  ├─ Pending Messages: 1
  ├─ Running Messages: 0
  ├─ Completed: 0
  ├─ Priority Config: WEIGHTED (fair distribution)
  └─ Status: Active

Queue: events-low
  ├─ Pending Messages: 1
  ├─ Running Messages: 0
  ├─ Completed: 0
  └─ Status: Active

DLQ: events-dlq
  ├─ Messages: 0
  └─ Status: Empty

Total Events: 4 pending
```

### Step 3: Start Workers

#### 3.1 Start Email Worker (Terminal 1)

```bash
./event-processor worker --type email --workers 2
```

**Expected Output:**

```
🚀 Starting Email Worker
   Workers: 2
   Queue: events-critical, events-normal, events-low
   Heartbeat: enabled (every 5s)
   Max Retries: 3
   
👷 Worker email-1 started (PID: 12345)
👷 Worker email-2 started (PID: 12346)

[Worker email-1] 📧 Processing event: evt-001
  Type: email
  Priority: 9
  Recipient: user@example.com
  Stream Entry ID: 1730470123457-0
  
[Worker email-1] ❤️  Heartbeat sent (lease extended: 30s)
[Worker email-1] ✓ Email sent successfully
[Worker email-1] ✓ Event evt-001 acknowledged (COMPLETED)
  PEL Status: Removed
  Processing Time: 2.3s

[Worker email-2] 📧 Processing event: evt-003
  Type: email
  Priority: 2
  Recipient: admin@example.com
  Stream Entry ID: 1730470123459-0
  
[Worker email-2] ✓ Email sent successfully
[Worker email-2] ✓ Event evt-003 acknowledged (COMPLETED)
  Processing Time: 1.8s

Workers processed: 2 events
Waiting for more events...
```

#### 3.2 Start Webhook Worker (Terminal 2)

```bash
./event-processor worker --type webhook --workers 3
```

**Expected Output:**

```
🚀 Starting Webhook Worker
   Workers: 3
   Queue: events-critical, events-normal, events-low
   Heartbeat: enabled (every 5s)
   Max Retries: 3
   
👷 Worker webhook-1 started (PID: 12347)
👷 Worker webhook-2 started (PID: 12348)
👷 Worker webhook-3 started (PID: 12349)

[Worker webhook-1] 🪝 Processing event: evt-critical-001
  Type: webhook
  Priority: 10 (CRITICAL)
  URL: https://api.example.com/webhooks/urgent
  Stream Entry ID: 1730470123456-0
  
[Worker webhook-1] ❤️  Heartbeat sent (lease extended: 30s)
[Worker webhook-1] → POST https://api.example.com/webhooks/urgent
[Worker webhook-1] ← 200 OK (543ms)
[Worker webhook-1] ✓ Webhook delivered successfully
[Worker webhook-1] ✓ Event evt-critical-001 acknowledged (COMPLETED)
  Processing Time: 3.1s

[Worker webhook-2] 🪝 Processing event: evt-002
  Type: webhook
  Priority: 2
  URL: https://api.example.com/webhooks/standard
  Stream Entry ID: 1730470123458-0
  
[Worker webhook-2] ❤️  Heartbeat sent (lease extended: 30s)
[Worker webhook-2] → POST https://api.example.com/webhooks/standard
[Worker webhook-2] ← 200 OK (234ms)
[Worker webhook-2] ✓ Webhook delivered successfully
[Worker webhook-2] ✓ Event evt-002 acknowledged (COMPLETED)

Workers processed: 2 events
Waiting for more events...
```

### Step 4: Scheduled/Delayed Messages

ChronoQueue supports scheduling messages to be processed in the future using the sorted set `schedule:{queueName}` for delayed message storage.

#### 4.1 Publish Scheduled Events

```bash
./event-processor publish events/scheduled-events.json
```

**Expected Output:**

```
📤 Publishing 5 events from events/scheduled-events.json...

  ✓ Event 1: Published email event (ID: evt-1730470200-0, Priority: high, Scheduled: 15:05:00 (5m))
  ✓ Event 2: Published email event (ID: evt-1730470200-1, Priority: medium, Scheduled: 15:10:00 (10m))
  ✓ Event 3: Published webhook event (ID: evt-1730470200-2, Priority: high, Scheduled: 15:15:00 (15m))
  ✓ Event 4: Published sms event (ID: evt-1730470200-3, Priority: medium, Scheduled: 15:20:00 (20m))
  ✓ Event 5: Published email event (ID: evt-1730470200-4, Priority: low, Scheduled: 15:03:00 (3m))

📊 Summary:
  • Total Events: 5
  • Published: 5
  • Duration: 45ms

  By Queue:
    • email-notifications: 3 events (scheduled)
    • webhook-events: 1 event (scheduled)
    • sms-alerts: 1 event (scheduled)
```

#### 4.2 Verify Scheduled Messages

You can use the ChronoQueue CLI to inspect scheduled messages:

```bash
# Check queue state to see scheduled message counts
chronoqueue queue state email-notifications --server localhost:9000 --insecure

# Output shows:
# - Messages in INVISIBLE state (scheduled, not yet ready)
# - Messages in PENDING state (ready for processing)
# - Other state counts

# Peek at messages in the queue
chronoqueue message peek email-notifications --server localhost:9000 --insecure
```

#### 4.3 Watch Messages Become Available

When the scheduled time arrives, ChronoQueue's background scheduler automatically:

1. Checks for messages in INVISIBLE state whose scheduled time has passed
2. Transitions them to PENDING state (ready for consumption)
3. Makes them available for workers to claim

You can monitor this by repeatedly checking queue state:

```bash
# Monitor queue state changes
watch -n 2 'chronoqueue queue state email-notifications --server localhost:9000 --insecure'

# You'll see:
# - INVISIBLE count decrease as scheduled time passes
# - PENDING count increase as messages become ready
# - RUNNING count increase as workers claim messages
```

Alternative monitoring using ChronoQueue API:

```bash
# Check message counts by state
curl -s http://localhost:8080/v1/queues/email-notifications/state | jq
```

#### 4.4 JSON Format for Scheduled Events

```json
{
    "events": [
        {
            "type": "email",
            "priority": "high",
            "schedule_in_minutes": 5,
            "data": {
                "recipient": "user@example.com",
                "subject": "Scheduled Reminder",
                "template": "meeting_reminder"
            }
        }
    ]
}
```

The `schedule_in_minutes` field sets the `invisibility_expiry` timestamp, causing the message to be placed in the sorted set until the scheduled time.

### Step 5: Test Failure and Retry

#### 5.1 Publish Event to Failing Endpoint

```bash
./event-processor publish events/webhook-failure.json
```

#### 5.2 Watch Worker Handle Retry

**Expected Output (Webhook Worker):**

```
[Worker webhook-1] 🪝 Processing event: evt-fail-001
  Type: webhook
  Priority: 3
  URL: https://api.example.com/webhooks/failing-endpoint
  Retry Attempt: 1/3

[Worker webhook-1] → POST https://api.example.com/webhooks/failing-endpoint
[Worker webhook-1] ← 503 Service Unavailable
[Worker webhook-1] ⚠️  Webhook delivery failed (attempt 1/3)
[Worker webhook-1] 🔄 Retrying in 2s (exponential backoff)...

[Worker webhook-1] → POST https://api.example.com/webhooks/failing-endpoint
[Worker webhook-1] ← 503 Service Unavailable
[Worker webhook-1] ⚠️  Webhook delivery failed (attempt 2/3)
[Worker webhook-1] 🔄 Retrying in 4s (exponential backoff)...

[Worker webhook-1] → POST https://api.example.com/webhooks/failing-endpoint
[Worker webhook-1] ← 503 Service Unavailable
[Worker webhook-1] ❌ Webhook delivery failed (attempt 3/3)
[Worker webhook-1] ➡️  Moving to DLQ: webhook-events-dlq
[Worker webhook-1] ✓ Event evt-fail-001 moved to DLQ
  Reason: Max retries exceeded (3 attempts)
  Last Error: HTTP 503 Service Unavailable
```

### Step 6: Manage Dead Letter Queue

#### 6.1 List Failed Events

```bash
./event-processor dlq list
```

#### 6.2 View Detailed Event Information

```bash
./event-processor dlq inspect evt-fail-001
```

#### 6.3 Requeue Failed Event

```bash
./event-processor dlq requeue evt-fail-001
```

**Expected Output:**

```
🔄 Requeuing event from DLQ...

✓ Event evt-fail-001 requeued successfully
  ├─ Removed from DLQ: webhook-events-dlq
  ├─ Added to queue: webhook-events
  ├─ Priority: 3
  ├─ Retry count reset: 0/3
  └─ Status: PENDING

Event will be processed by next available worker.
```

#### 6.4 Delete Event from DLQ

```bash
./event-processor dlq delete evt-fail-001
```

**Expected Output:**

```
🗑️  Deleting event from DLQ...

✓ Event evt-fail-001 deleted successfully
  ├─ Removed from: webhook-events-dlq
  └─ This action is permanent

DLQ now contains: 0 event(s)
```

#### 6.5 Purge Entire DLQ

```bash
./event-processor dlq purge --confirm
```

**Expected Output:**

```
⚠️  WARNING: This will permanently delete ALL events from DLQ!

Purging DLQ: webhook-events-dlq...

✓ Purged 5 event(s) from DLQ
  └─ DLQ is now empty

This action cannot be undone.
```

### Step 6: Monitor Real-time Statistics

```bash
./event-processor monitor --interval 2s
```

**Expected Output (updates every 2 seconds):**

```
📊 Live Event Processing Monitor (updating every 2s)

═══════════════════════════════════════════════════════════════
Time: 2025-11-01 14:35:22

Queue Statistics:
  events-critical  │ ████████░░ 80% │ Pending: 2  Running: 8  Completed: 40
  events-normal    │ ██████░░░░ 60% │ Pending: 5  Running: 5  Completed: 90
  events-low       │ ███░░░░░░░ 30% │ Pending: 12 Running: 3  Completed: 35

Workers:
  email-worker     │ 2 active │ Processed: 45 │ Avg time: 2.1s
  webhook-worker   │ 3 active │ Processed: 87 │ Avg time: 1.8s
  sms-worker       │ 1 active │ Processed: 33 │ Avg time: 0.9s

System Metrics:
  ├─ Total Events Processed: 165
  ├─ Success Rate: 96.4%
  ├─ Failed (in DLQ): 6
  ├─ Average Latency: 1.7s
  ├─ Throughput: 55 events/min
  └─ Active Leases: 16

DLQ: 6 events │ Oldest: 15m ago

[Press Ctrl+C to stop monitoring]
```

### Step 7: Peek at Queue Contents

```bash
./event-processor peek events-critical --limit 5
```

**Expected Output:**

```
👀 Peeking into queue: events-critical (limit: 5)

┌─────────────────┬──────────┬──────────┬──────────────────────┐
│ Event ID        │ Type     │ Priority │ Scheduled For        │
├─────────────────┼──────────┼──────────┼──────────────────────┤
│ evt-urgent-023  │ webhook  │ 4        │ 2025-11-01 14:35:30  │
│ evt-alert-045   │ email    │ 4        │ 2025-11-01 14:35:31  │
│ evt-critical-12 │ sms      │ 4        │ 2025-11-01 14:35:32  │
│ evt-high-078    │ webhook  │ 3        │ 2025-11-01 14:35:33  │
│ evt-urgent-099  │ email    │ 3        │ 2025-11-01 14:35:35  │
└─────────────────┴──────────┴──────────┴──────────────────────┘

Showing 5 of 12 pending events
```

## 📄 JSON Payload Examples

The `events/` directory contains sample JSON files for different use cases:

### Traditional Publishing Files

### events/critical-webhook.json

```json
{
  "events": [
    {
      "type": "webhook",
      "priority": "critical",
      "data": {
        "url": "https://api.example.com/webhooks/urgent",
        "method": "POST",
        "event_type": "system.alert",
        "severity": "critical",
        "message": "Database connection pool exhausted"
      }
    }
  ]
}
```

### events/mixed-batch.json

```json
{
  "events": [
    {
      "type": "email",
      "priority": "high",
      "data": {
        "to": "user@example.com",
        "subject": "Important Update",
        "template": "notification"
      }
    },
    {
      "type": "webhook",
      "priority": "medium",
      "data": {
        "url": "https://api.example.com/webhooks/standard",
        "event_type": "user.action"
      }
    }
  ]
}
```

### Bulk Publishing Files

### events/bulk-demo.json

10 events across all three queues (email, webhook, SMS) with mixed priorities. Perfect for demonstrating bulk posting basics.

**Usage:**

```bash
./event-processor publish-bulk events/bulk-demo.json
```

### events/bulk-large-batch.json

20 events demonstrating larger batch operations. Shows performance benefits of bulk posting.

**Usage:**

```bash
# Compare performance
time ./event-processor publish events/bulk-large-batch.json
time ./event-processor publish-bulk events/bulk-large-batch.json
```

### events/bulk-scheduled.json

10 scheduled events with different delivery times (5, 10, 15, 30 minutes). Demonstrates bulk posting combined with scheduled delivery.

**Usage:**

```bash
./event-processor publish-bulk events/bulk-scheduled.json

# Then monitor as they become available
./event-processor monitor
```

**Event Structure:**

```json
{
  "type": "email",
  "priority": "high",
  "schedule_in_minutes": 5,
  "data": {
    "to": "user@example.com",
    "subject": "Cart Reminder"
  }
}
```

## 🧪 Testing Scenarios

    {
      "id": "evt-critical-001",
      "type": "webhook",
      "priority": 10,
      "url": "https://api.example.com/webhooks/urgent",
      "method": "POST",
      "headers": {
        "Content-Type": "application/json",
        "X-Api-Key": "secret-key"
      },
      "body": {
        "event": "critical.alert",
        "severity": "high",
        "message": "System threshold exceeded",
        "timestamp": "2025-11-01T14:30:00Z"
      },
      "max_attempts": 3
    }
  ]
}

```

### events/mixed-batch.json

```json
{
  "events": [
    {
      "id": "evt-001",
      "type": "email",
      "priority": 9,
      "to": "user@example.com",
      "subject": "Critical Alert",
      "body": "Your account requires immediate attention",
      "max_attempts": 3
    },
    {
      "id": "evt-002",
      "type": "webhook",
      "priority": 5,
      "url": "https://api.example.com/webhooks/standard",
      "method": "POST",
      "body": {"event": "order.created", "order_id": "12345"}
    },
    {
      "id": "evt-003",
      "type": "sms",
      "priority": 2,
      "to": "+1234567890",
      "message": "Your order has shipped",
      "max_attempts": 2
    },
    {
      "id": "evt-004",
      "type": "webhook",
      "priority": 10,
      "url": "https://api.example.com/webhooks/urgent",
      "method": "POST",
      "body": {"event": "payment.failed", "amount": 99.99}
    }
  ]
}
```

## 🧪 Testing Scenarios

### Quick Demo: Bulk Posting

Run the automated demo script to see bulk posting in action:

```bash
./demo-bulk-posting.sh
```

This script demonstrates:

1. ✅ System initialization
2. ✅ Traditional publishing (one-by-one) with timing
3. ✅ Bulk publishing with ALL_OR_NOTHING mode
4. ✅ Bulk publishing with BEST_EFFORT mode  
5. ✅ Bulk publishing with scheduled messages
6. ✅ Queue statistics after bulk operations

**Expected Performance:**

- Traditional: ~1-2 seconds for 10 messages
- Bulk ALL_OR_NOTHING: ~200-300ms for 10 messages
- Bulk BEST_EFFORT: ~200-300ms for 20 messages

### Scenario 1: High Throughput Test

```bash
# Generate 1000 events
./event-processor generate --count 1000 --output events/load-test.json

# Publish batch
./event-processor publish events/load-test.json

# Start multiple workers
./event-processor worker --type email --workers 5 & \
./event-processor worker --type webhook --workers 10 & \
./event-processor worker --type sms --workers 3 &

# Monitor throughput
./event-processor monitor
```

### Scenario 2: Priority Validation

```bash
# Publish events with different priorities
./event-processor publish events/priority-test.json

# Start single worker to observe processing order
./event-processor worker --type webhook --workers 1

# Verify high-priority events processed first
```

### Scenario 3: Failure Recovery

```bash
# Publish events to failing endpoints
./event-processor publish events/webhook-failures.json

# Watch automatic retry with exponential backoff
./event-processor worker --type webhook --workers 2

# Inspect DLQ
./event-processor dlq list

# Requeue after fixing endpoint
./event-processor dlq requeue --all
```

### Scenario 4: Failure Recovery with DLQ

### Redis Streams Benefits Demonstrated

1. **Consumer Groups**: Multiple workers coordinate automatically
2. **PEL Tracking**: Pending Entry List prevents message loss
3. **Stream Entry IDs**: Precise message acknowledgment with XACK
4. **XAUTOCLAIM**: Automatic reclaim of stuck messages
5. **Priority Streams**: Separate streams for priority routing
6. **Heartbeat via XCLAIM**: Worker liveliness tracking

### Worker Coordination

```
┌─────────────────────────────────────────────────────────┐
│                    ChronoQueue Server                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │ Critical    │  │ Normal      │  │ Low         │    │
│  │ Stream      │  │ Stream      │  │ Stream      │    │
│  │ (Pri 8-10)  │  │ (Pri 4-7)   │  │ (Pri 1-3)   │    │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘    │
│         │                │                │            │
│    ┌────┴────────────────┴────────────────┴────┐       │
│    │         Consumer Group: processors        │       │
│    │              (PEL Tracking)               │       │
│    └───────────────────┬───────────────────────┘       │
└────────────────────────┼───────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
    ┌────▼────┐    ┌────▼────┐    ┌────▼────┐
    │ Email   │    │ Webhook │    │ SMS     │
    │ Worker  │    │ Worker  │    │ Worker  │
    │  (x2)   │    │  (x3)   │    │  (x1)   │
    └─────────┘    └─────────┘    └─────────┘
```

## 🛠️ Troubleshooting

### Worker Not Processing

```bash
# Check queue stats
./event-processor stats

# Verify worker is connected
./event-processor worker --type email --verbose

# Check for stuck messages
./event-processor monitor
```

### High DLQ Count

```bash
# Inspect failed events
./event-processor dlq list

# Check common errors
./event-processor dlq stats

# Test endpoint manually
curl -X POST https://api.example.com/webhooks/test
```

### Performance Issues

```bash
# Monitor ChronoQueue server
chronoqueue server --log-level debug

# Adjust worker count
./event-processor worker --type webhook --workers 10
```

## 📚 Learn More

- [ChronoQueue Documentation](../../README.md)
- [Message Priority Guide](../../docs/features/priority.md)
- [DLQ Best Practices](../../docs/features/dlq.md)

## 🎯 Next Steps

1. **Customize Event Types**: Add your own event processors
2. **Production Deployment**: Deploy workers as separate services
3. **Monitoring Integration**: Add Prometheus/Grafana
4. **Advanced Patterns**: Implement saga patterns, event sourcing
5. **Performance Tuning**: Optimize worker count and batch sizes

---

**Questions or Issues?** Open an issue on GitHub or check the main documentation.
