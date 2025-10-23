# ChronoQueue Test Suite - Quick Start Guide

🚀 **Get up and running with the ChronoQueue test suite in 5 minutes!**

---

## Step 1: Prerequisites Check

```bash
# Verify Docker is running
docker info

# Verify Go version (1.25+ required)
go version

# Install dependencies
cd /workspaces/chronoqueue
go mod download
```

---

## Step 2: Run Your First Test

```bash
# Run a single integration test
go test -v ./tests/integration/ -run TestMessageLifecycle_PostSimpleMessage

# Expected output:
# === RUN   TestMessageLifecycle_PostSimpleMessage
# === PAUSE TestMessageLifecycle_PostSimpleMessage
# === CONT  TestMessageLifecycle_PostSimpleMessage
# --- PASS: TestMessageLifecycle_PostSimpleMessage (X.XXs)
# PASS
```

---

## Step 3: Run Test Suites

```bash
# Run all integration tests (5-10 minutes)
go test -v ./tests/integration/...

# Run E2E tests (10-15 minutes)
go test -v ./tests/e2e/...

# Run everything (15-20 minutes)
go test -v -timeout 30m ./tests/...
```

---

## Step 4: Understand Test Output

### ✅ Successful Test

```
=== RUN   TestMessageLifecycle_PostSimpleMessage
=== PAUSE TestMessageLifecycle_PostSimpleMessage
=== CONT  TestMessageLifecycle_PostSimpleMessage
--- PASS: TestMessageLifecycle_PostSimpleMessage (2.34s)
```

### ❌ Failed Test

```
=== RUN   TestMessageLifecycle_PostSimpleMessage
--- FAIL: TestMessageLifecycle_PostSimpleMessage (1.23s)
    message_lifecycle_test.go:45: 
        Error:      Not equal: 
                    expected: true
                    actual  : false
```

### Common Failure Reasons

1. **Docker not running**: `Cannot connect to the Docker daemon`
   - **Fix**: Start Docker Desktop or Docker daemon

2. **Port conflicts**: `bind: address already in use`
   - **Fix**: Stop conflicting services or wait for previous tests to complete

3. **Timeout**: `test timed out after 10m0s`
   - **Fix**: Increase timeout with `-timeout 30m`

---

## Step 5: Generate Coverage Report

```bash
# Generate coverage
go test -v -coverprofile=coverage.out ./tests/...

# View in terminal
go tool cover -func=coverage.out

# View in browser (HTML report)
go tool cover -html=coverage.out
```

---

## Common Test Commands Cheat Sheet

```bash
# Run specific test file
go test -v ./tests/integration/message_lifecycle_test.go

# Run tests matching pattern
go test -v ./tests/integration/ -run TestMessage

# Run in parallel (faster)
go test -v -parallel 4 ./tests/integration/...

# Skip long-running tests
go test -v -short ./tests/...

# Run with race detector
go test -v -race ./tests/...

# Verbose + save output
go test -v ./tests/... 2>&1 | tee test-output.log
```

---

## Understanding Test Structure

### Test Files

```
tests/
├── integration/          # Feature-specific tests
│   ├── message_lifecycle_test.go
│   ├── priority_queue_test.go
│   ├── retry_dlq_test.go
│   ├── scheduling_test.go
│   └── schema_registry_test.go
└── e2e/                 # Complete workflow tests
    └── workflows_test.go
```

### Test Naming Convention

```
Test<Feature>_<Scenario>_<ExpectedBehavior>
```

Examples:

- `TestMessageLifecycle_PostSimpleMessage` - Post a simple message
- `TestPriorityQueue_HighPriorityFirst` - High priority messages first
- `TestDLQ_ExhaustedRetries_MovesToDLQ` - Failed messages go to DLQ

---

## Using Test Fixtures

### Load Message Templates

```go
// In your test
msgFixture := helpers.LoadMessageFixture(t, "order_created")

// Available fixtures:
// - order_created
// - high_priority_alert
// - low_priority_log
// - special_characters
// - large_payload
// - minimal_message
// - json_payload
// - empty_payload
// - unicode_content
// - scheduled_reminder
```

### Load Queue Configurations

```go
// In your test
queueFixture := helpers.LoadFixture(t, "fixtures/queues.json", "simple_queue")

// Available fixtures:
// - simple_queue
// - exclusive_queue
// - priority_queue
// - schema_enforced_queue
// - no_dlq_queue
```

### Load Schedules

```go
// In your test
scheduleFixture := helpers.LoadFixture(t, "fixtures/schedules.json", "business_days_only")

// Available fixtures:
// - business_days_only
// - day_of_month
// - quarterly_first_business_day
// - exclude_holidays
// - timezone_aware
// - complex_business_calendar
```

---

## Debugging Failed Tests

### Enable Verbose Logging

```bash
# Run single test with verbose output
go test -v ./tests/integration/ -run TestSpecificTest

# Check container logs
docker ps  # Find container ID
docker logs <container-id>
```

### Inspect Test Environment

```go
// In testcontainer.go, add logging:
t.Logf("Redis running at: %s", redisContainer.Endpoint)
t.Logf("ChronoQueue running at: %s", chronoQueueContainer.Endpoint)
```

### Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| Docker containers not stopping | `docker ps -a` and `docker rm -f <container>` |
| Port conflicts | Wait or change port mapping in testcontainer.go |
| Tests timeout | Increase with `-timeout 60m` |
| Flaky tests | Run with `-count=10` to verify stability |

---

## Next Steps

1. **Explore Tests**: Check `tests/integration/` for examples
2. **Read Guide**: See [TESTING_GUIDE.md](TESTING_GUIDE.md) for comprehensive scenarios
3. **Review Implementation**: See [TEST_IMPLEMENTATION_SUMMARY.md](TEST_IMPLEMENTATION_SUMMARY.md)
4. **Add Your Tests**: Follow patterns in existing tests

---

## Test Execution Flow

```
Start
  ↓
Setup Testcontainers (Redis + ChronoQueue)
  ↓
Load Fixtures (queues.json, messages.json, etc.)
  ↓
Run Test Logic (Arrange → Act → Assert)
  ↓
Assertions & Validation
  ↓
Cleanup (Automatic via t.Cleanup())
  ↓
End
```

---

## Quick Reference: Key Helper Functions

```go
// Setup
env := helpers.SetupTestEnvironment(t)
conn := env.NewGRPCClient(t)
client := queueservice_pb.NewQueueServiceClient(conn)

// Generate unique names
queueName := helpers.GenerateUniqueQueueName(t, "test-queue")

// Load fixtures
msgFixture := helpers.LoadMessageFixture(t, "order_created")
schema := helpers.LoadJSONSchema(t, "order_schema")

// Assertions
helpers.AssertQueueExists(t, client, ctx, queueName)
helpers.AssertMessagePriority(t, message, 50)
helpers.AssertLeaseActive(t, message)
```

---

## Performance Tips

1. **Use Parallel Tests**: Add `t.Parallel()` to independent tests
2. **Reuse Containers**: Consider shared test containers for faster execution
3. **Skip Heavy Tests**: Use `-short` flag for quick feedback
4. **Optimize Fixtures**: Keep test data minimal but realistic

---

## Success Checklist

- [ ] Docker is running
- [ ] Go dependencies installed (`go mod download`)
- [ ] Can compile tests (`go build ./tests/...`)
- [ ] Can run single test successfully
- [ ] Can run full integration suite
- [ ] Can generate coverage report
- [ ] Understand test output format
- [ ] Know how to debug failed tests

---

## Getting Help

1. **Documentation**: Check [TESTING_GUIDE.md](TESTING_GUIDE.md)
2. **Examples**: Review tests in `integration/` and `e2e/`
3. **Implementation Details**: See [TEST_IMPLEMENTATION_SUMMARY.md](TEST_IMPLEMENTATION_SUMMARY.md)

---

**🎉 You're ready to run ChronoQueue tests!**

Start with: `go test -v ./tests/integration/ -run TestMessageLifecycle_PostSimpleMessage`
