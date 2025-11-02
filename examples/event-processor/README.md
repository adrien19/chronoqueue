# Event Processing System Demo

A comprehensive demonstration of ChronoQueue's Redis Streams architecture for high-throughput event processing, webhooks, and notifications.

## 🎯 What This Demo Demonstrates

- ✅ **High-throughput message processing** - Process thousands of events efficiently
- ✅ **Priority routing** - Critical/normal/low priority event handling
- ✅ **Scheduled/delayed messages** - Future message delivery using sorted sets
- ✅ **Automatic heartbeats** - Processing acknowledgment with lease renewal
- ✅ **Retry logic** - Exponential backoff for failed events
- ✅ **DLQ handling** - Dead letter queue for undeliverable events
- ✅ **Stream architecture** - Consumer groups, PEL tracking, stream entry IDs
- ✅ **Multiple worker types** - Email, webhook, and SMS processors
- ✅ **JSON payload files** - Easy event definition without CLI formatting

## 📋 Prerequisites

- Docker and Docker Compose (for Redis)
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
✓ Created queue: events-critical (priority: strict)
✓ Created queue: events-normal (priority: weighted)
✓ Created queue: events-low (priority: weighted)
✓ Created DLQ queue: events-dlq
✓ Initialization complete!
```

## 📚 Step-by-Step Tutorial

### Step 1: Publish Events

#### 1.1 Publish a Single Critical Event

```bash
./event-processor publish events/critical-webhook.json
```

**Expected Output:**

```
📤 Publishing events...
✓ Published event: evt-critical-001 (priority: 10, type: webhook)
  Stream Entry ID: 1730470123456-0
  Queue: events-critical
  
📊 Summary:
  Total events: 1
  Critical: 1
  Normal: 0
  Low: 0
```

#### 1.2 Publish Batch of Mixed Priority Events

```bash
./event-processor publish events/mixed-batch.json
```

**Expected Output:**

```
📤 Publishing events...
✓ Published event: evt-001 (priority: 9, type: email)
  Stream Entry ID: 1730470123457-0
✓ Published event: evt-002 (priority: 5, type: webhook)
  Stream Entry ID: 1730470123458-0
✓ Published event: evt-003 (priority: 2, type: sms)
  Stream Entry ID: 1730470123459-0
✓ Published event: evt-004 (priority: 10, type: webhook)
  Stream Entry ID: 1730470123460-0

📊 Summary:
  Total events: 4
  Critical: 2 (priority 8-10)
  Normal: 1 (priority 4-7)
  Low: 1 (priority 1-3)
  
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
  Priority: 5
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

#### 4.2 Verify Scheduled Messages in Redis

Scheduled messages are stored in the sorted set `schedule:{queueName}` with their scheduled timestamp as the score:

```bash
# Check scheduled messages in email queue
redis-cli ZRANGE schedule:email-notifications 0 -1 WITHSCORES

# Output:
# 1) "evt-1730470200-4"
# 2) "1730470380"  # Unix timestamp for 3 minutes from now
# 3) "evt-1730470200-0"
# 4) "1730470500"  # Unix timestamp for 5 minutes from now
# 5) "evt-1730470200-1"
# 6) "1730470800"  # Unix timestamp for 10 minutes from now

# Check scheduled messages across all queues
redis-cli KEYS "schedule:*"
```

#### 4.3 Watch Messages Move to Stream

When the scheduled time arrives, ChronoQueue's background scheduler automatically:

1. Checks the sorted set for messages whose timestamp <= current time
2. Moves them to the appropriate priority stream (`stream:{priority}:{queue}`)
3. Removes them from the sorted set

Use the included monitoring script to watch this in real-time:

```bash
# Monitor scheduled messages moving to streams
./monitor-scheduled.sh email-notifications

# Output:
# 🔍 Monitoring: email-notifications
# Time: 16:37:15
#
# 📅 Scheduled Messages (schedule:email-notifications):
#   Count: 2
#   Messages:
#     • evt-1762014681-4 → 16:34:21  (past due, will move soon)
#     • evt-1762014681-1 → 16:41:21
#
# 🌊 Active Streams:
#   Total in Streams: 0
#
# 📊 Summary:
#   Scheduled (waiting): 2
#   Active (processing): 0

# After a few seconds, you'll see the count decrease in scheduled
# and increase in active streams as messages are moved
```

You can also check manually in Redis:

```bash
# Check scheduled message count
redis-cli ZCARD schedule:email-notifications

# Check stream length (by priority)
redis-cli XLEN stream:75:email-notifications  # high priority
redis-cli XLEN stream:50:email-notifications  # medium priority
redis-cli XLEN stream:25:email-notifications  # low priority
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
  Priority: 8
  URL: https://api.example.com/webhooks/failing-endpoint
  Stream Entry ID: 1730470124000-0
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
[Worker webhook-1] ➡️  Moving to DLQ: events-dlq
[Worker webhook-1] ✓ Event evt-fail-001 moved to DLQ
  Reason: Max retries exceeded (3 attempts)
  Last Error: HTTP 503 Service Unavailable
```

### Step 6: Manage Dead Letter Queue

#### 6.1 List Failed Events

```bash
./event-processor dlq list
```

**Expected Output:**

```
💀 Dead Letter Queue: events-dlq

┌─────────────────┬──────────┬──────────────────────┬──────────┬──────────────────────┐
│ Event ID        │ Type     │ Failed At            │ Attempts │ Error                │
├─────────────────┼──────────┼──────────────────────┼──────────┼──────────────────────┤
│ evt-fail-001    │ webhook  │ 2025-11-01 14:30:45  │ 3        │ HTTP 503 Service ... │
└─────────────────┴──────────┴──────────────────────┴──────────┴──────────────────────┘

Total: 1 failed event(s)

💡 Tip: Use 'event-processor dlq requeue <event-id>' to retry
```

#### 6.2 View Detailed Event Information

```bash
./event-processor dlq inspect evt-fail-001
```

**Expected Output:**

```
💀 DLQ Event Details: evt-fail-001

Event Information:
  ├─ Event ID: evt-fail-001
  ├─ Type: webhook
  ├─ Priority: 8
  ├─ Created: 2025-11-01 14:30:30
  ├─ Failed: 2025-11-01 14:30:45
  └─ Stream Entry ID: 1730470124000-0

Failure Details:
  ├─ Total Attempts: 3
  ├─ Last Attempt: 2025-11-01 14:30:45
  ├─ Error: HTTP 503 Service Unavailable
  └─ Failure Reason: Max retries exceeded

Payload:
{
  "type": "webhook",
  "url": "https://api.example.com/webhooks/failing-endpoint",
  "method": "POST",
  "headers": {
    "Content-Type": "application/json"
  },
  "body": {
    "event": "order.created",
    "data": {...}
  }
}

Retry History:
  1. 2025-11-01 14:30:38 → HTTP 503 (delay: 2s)
  2. 2025-11-01 14:30:42 → HTTP 503 (delay: 4s)
  3. 2025-11-01 14:30:45 → HTTP 503 (final attempt)
```

#### 5.3 Requeue Failed Event

```bash
./event-processor dlq requeue evt-fail-001
```

**Expected Output:**

```
🔄 Requeuing event from DLQ...

✓ Event evt-fail-001 requeued successfully
  ├─ Removed from DLQ: events-dlq
  ├─ Added to queue: events-critical
  ├─ New Stream Entry ID: 1730470125000-0
  ├─ Priority: 8
  ├─ Retry count reset: 0/3
  └─ Status: PENDING

Event will be processed by next available worker.
```

#### 5.4 Delete Event from DLQ

```bash
./event-processor dlq delete evt-fail-001
```

**Expected Output:**

```
🗑️  Deleting event from DLQ...

✓ Event evt-fail-001 deleted successfully
  ├─ Removed from: events-dlq
  └─ This action is permanent

DLQ now contains: 0 event(s)
```

#### 5.5 Purge Entire DLQ

```bash
./event-processor dlq purge --confirm
```

**Expected Output:**

```
⚠️  WARNING: This will permanently delete ALL events from DLQ!

Purging DLQ: events-dlq...

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

┌─────────────────┬──────────┬──────────┬──────────────────────┬────────────────────┐
│ Event ID        │ Type     │ Priority │ Scheduled For        │ Stream Entry ID    │
├─────────────────┼──────────┼──────────┼──────────────────────┼────────────────────┤
│ evt-urgent-023  │ webhook  │ 10       │ 2025-11-01 14:35:30  │ 1730470130000-0    │
│ evt-alert-045   │ email    │ 9        │ 2025-11-01 14:35:31  │ 1730470131000-0    │
│ evt-critical-12 │ sms      │ 9        │ 2025-11-01 14:35:32  │ 1730470132000-0    │
│ evt-high-078    │ webhook  │ 8        │ 2025-11-01 14:35:33  │ 1730470133000-0    │
│ evt-urgent-099  │ email    │ 8        │ 2025-11-01 14:35:35  │ 1730470135000-0    │
└─────────────────┴──────────┴──────────┴──────────────────────┴────────────────────┘

Showing 5 of 12 pending events
```

## 📄 JSON Payload Examples

The `events/` directory contains sample JSON files:

### events/critical-webhook.json

```json
{
  "events": [
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

### Scenario 1: High Throughput Test

```bash
# Generate 1000 events
./event-processor generate --count 1000 --output events/load-test.json

# Publish batch
./event-processor publish events/load-test.json

# Start multiple workers
./event-processor worker --type email --workers 5 &
./event-processor worker --type webhook --workers 10 &
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

### Scenario 4: Stream Architecture Benefits

```bash
# Demonstrate consumer group coordination
./event-processor worker --type email --workers 3 --name group-1 &
./event-processor worker --type email --workers 3 --name group-2 &

# Publish events
./event-processor publish events/mixed-batch.json

# Observe:
# - Each event processed by only one worker
# - Automatic load distribution
# - PEL tracking prevents duplicate processing
# - Stream entry IDs enable precise acknowledgment
```

## 🔍 Architecture Insights

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
# Check Redis connection
redis-cli ping

# Monitor ChronoQueue server
chronoqueue server --log-level debug

# Adjust worker count
./event-processor worker --type webhook --workers 10
```

## 📚 Learn More

- [ChronoQueue Documentation](../../README.md)
- [Redis Streams Architecture](../../docs/architecture/redis-streams.md)
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
