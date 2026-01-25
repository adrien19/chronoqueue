# ChronoQueue Test Suite

**Comprehensive integration and end-to-end tests for the ChronoQueue message queue system.**

---

## 📁 Directory Structure

```text
tests/
├── README.md                          # This file
├── TESTING_GUIDE.md                   # Comprehensive testing guide
├── TEST_IMPLEMENTATION_SUMMARY.md     # Implementation summary
├── DELIVERABLES.md                    # Project deliverables
├── TESTING_ENHANCEMENT_SUMMARY.md     # Testing enhancements
│
├── fixtures/                          # Test data
│   ├── queues.json                   # Queue configurations
│   ├── messages.json                 # Message templates
│   ├── schedules.json                # Schedule configurations
│   └── schemas/                      # JSON schemas
│       ├── order_schema.json
│       ├── event_schema.json
│       └── notification_schema.json
│
├── helpers/                           # Test utilities
│   ├── testcontainer.go              # Container setup
│   ├── fixtures.go                   # Fixture loaders
│   └── assertions.go                 # Custom assertions
│
├── integration/                       # Integration tests
│   ├── queue_operations_test.go      # Queue CRUD
│   ├── message_lifecycle_test.go     # Message operations
│   ├── priority_queue_test.go        # Priority ordering
│   ├── retry_dlq_test.go             # Retry and DLQ
│   ├── scheduling_test.go            # Cron & calendar
│   └── schema_registry_test.go       # Schema validation
│
└── e2e/                               # End-to-end tests
    └── workflows_test.go              # Complete workflows
```

---

## 🚀 Quick Start

### Prerequisites

1. **Docker** must be running (for Testcontainers)
2. **Go 1.25+** installed
3. **ChronoQueue dependencies** installed: `go mod download`

### Run All Tests

```bash
# From project root
go test -v ./tests/...
```

### Run Specific Test Suites

```bash
# Integration tests only
go test -v ./tests/integration/...

# E2E tests only
go test -v ./tests/e2e/...

# Specific test file
go test -v ./tests/integration/message_lifecycle_test.go

# Specific test function
go test -v ./tests/integration/ -run TestMessageLifecycle_PostSimpleMessage
```

### Run with Options

```bash
# Skip long-running tests
go test -v -short ./tests/...

# Run with timeout
go test -v -timeout 30m ./tests/...

# Run in parallel
go test -v -parallel 4 ./tests/integration/...

# Generate coverage
go test -v -coverprofile=coverage.out ./tests/...
go tool cover -html=coverage.out
```

---

## 📊 Test Coverage

| Test Suite | Tests | Features Covered |
|------------|-------|------------------|
| **Queue Operations** | 5+ | Queue CRUD, listing, state management |
| **Message Lifecycle** | 10+ | Post, get, acknowledge, lease, heartbeat, peek |
| **Priority Queues** | 5+ | Priority ordering, FIFO within priority |
| **Retry & DLQ** | 8+ | Exponential backoff, DLQ operations, requeue |
| **Scheduling** | 8+ | Cron expressions, calendar rules, management |
| **Schema Registry** | 9+ | Register, validate, version, delete |
| **E2E Workflows** | 3+ | Complete real-world scenarios |

**Total**: 50+ comprehensive test cases

---

## 🧪 Test Categories

### Integration Tests (`./integration/`)

Test individual features with real PostgreSQL/SQLite and ChronoQueue containers:

- ✅ **Queue Management**: Create, delete, list queues
- ✅ **Message Operations**: Post, get, acknowledge, renew lease
- ✅ **Priority Handling**: Priority-based message ordering
- ✅ **Retry System**: Exponential backoff, max retries
- ✅ **Dead Letter Queue**: Auto-creation, requeue, purge
- ✅ **Scheduling**: Cron and calendar-based scheduling
- ✅ **Schema Registry**: JSON Schema validation

### End-to-End Tests (`./e2e/`)

Test complete workflows combining multiple features:

- ✅ **Complete Message Workflow**: Post → Consume → Ack → DLQ → Requeue
- ✅ **High-Priority Alert System**: Priority-based processing
- ✅ **Multi-Tenant Isolation**: Complete isolation between tenants

---

## 🛠️ Test Utilities

### Fixtures (`./fixtures/`)

Pre-defined test data for consistent testing:

- **Queues**: 5 configurations (simple, exclusive, priority, schema-enforced, no-DLQ)
- **Messages**: 10 templates (various content types, priorities)
- **Schedules**: 10 configurations (cron and calendar-based)
- **Schemas**: 3 JSON schemas (order, event, notification)

### Helpers (`./helpers/`)

Utility functions for common test operations:

**Container Management:**

- `SetupTestEnvironment()` - Creates PostgreSQL/SQLite + ChronoQueue containers
- `NewGRPCClient()` - gRPC client connection
- `Cleanup()` - Resource teardown

**Fixture Loading:**

- `LoadFixture()` - Load JSON fixtures
- `LoadMessageFixture()` - Load message templates
- `LoadJSONSchema()` - Load schema files

**Assertions:**

- `AssertQueueExists()` - Verify queue presence
- `AssertMessagePriority()` - Validate priority ordering
- `AssertLeaseActive()` - Check lease state
- `AssertErrorContains()` - Validate error messages

---

## 📖 Documentation

- **[TESTING_GUIDE.md](TESTING_GUIDE.md)** - Comprehensive testing guide with all test scenarios
- **[TEST_IMPLEMENTATION_SUMMARY.md](TEST_IMPLEMENTATION_SUMMARY.md)** - Detailed implementation summary
- **[TESTING_ENHANCEMENT_SUMMARY.md](TESTING_ENHANCEMENT_SUMMARY.md)** - Testing enhancements overview

---

## 🎯 Example Test

```go
func TestMessageLifecycle_PostSimpleMessage(t *testing.T) {
    t.Parallel()

    // Arrange
    ctx := context.Background()
    env := helpers.SetupTestEnvironment(t)
    conn := env.NewGRPCClient(t)
    client := queueservice_pb.NewQueueServiceClient(conn)

    queueName := helpers.GenerateUniqueQueueName(t, "test-queue")

    // Create queue
    _, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
        Name: queueName,
        Metadata: &queue_pb.QueueMetadata{
            Type: queue_pb.QueueType_SIMPLE,
        },
    })
    require.NoError(t, err)

    // Load message from fixture
    msgFixture := helpers.LoadMessageFixture(t, "low_priority_log")

    // Act
    response, err := client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
        QueueName: queueName,
        Message:   createMessageFromFixture(t, msgFixture),
    })

    // Assert
    require.NoError(t, err)
    assert.True(t, response.Success)
}
```

---

## 🔍 Troubleshooting

### Docker Issues

```bash
# Verify Docker is running
docker info

# Clean up Docker resources
docker system prune -a
```

### Test Failures

```bash
# Run with verbose output
go test -v ./tests/integration/... 2>&1 | tee test-output.log

# Run single test for debugging
go test -v ./tests/integration/ -run TestSpecificTest

# Check container logs (if test fails)
docker logs <container-id>
```

### Timing Issues

Some tests are timing-dependent. If they fail intermittently:

```bash
# Skip timing-dependent tests
go test -v -short ./tests/...

# Increase timeouts
go test -v -timeout 60m ./tests/...
```

---

## 🤝 Contributing

When adding new tests:

1. **Follow existing patterns**: Use Arrange-Act-Assert structure
2. **Use fixtures**: Load test data from `fixtures/` directory
3. **Add assertions**: Use custom assertions from `helpers/assertions.go`
4. **Document**: Add test scenario comments
5. **Parallel where safe**: Use `t.Parallel()` for independent tests
6. **Clean up**: Use `t.Cleanup()` for resource teardown

---

## 📝 Test Naming Convention

```text
Test<Feature>_<Scenario>_<ExpectedBehavior>
```

**Examples:**

- `TestQueueOperations_CreateSimpleQueue_Success`
- `TestMessageLifecycle_GetMessage_ReturnsHighestPriority`
- `TestDLQ_ExhaustedRetries_MovesToDLQ`

---

## ✅ Success Criteria

All tests should:

- ✅ Run in isolation (no shared state)
- ✅ Clean up resources automatically
- ✅ Produce consistent results
- ✅ Execute in reasonable time
- ✅ Provide clear failure messages

---

## 📞 Support

For issues or questions:

1. Check [TESTING_GUIDE.md](TESTING_GUIDE.md) for detailed documentation
2. Review [TEST_IMPLEMENTATION_SUMMARY.md](TEST_IMPLEMENTATION_SUMMARY.md) for implementation details
3. Check existing test examples in `integration/` and `e2e/` directories
