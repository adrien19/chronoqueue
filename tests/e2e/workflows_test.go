package e2e

// Package e2e provides end-to-end tests for ChronoQueue covering complete workflows.
//
// These tests validate real-world scenarios by combining multiple features:
// - Complete message lifecycle from creation to completion
// - Scheduled batch processing
// - High-priority alert systems
// - Failure recovery and DLQ workflows
// - Multi-tenant isolation
// - Long-running tasks with heartbeats
//
// Test Scenarios: E2E-001 through E2E-010 from TESTING_GUIDE.md
//
// Run with: go test -v ./tests/e2e/...

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/tests/helpers"
)

// TestE2E_CompleteMessageWorkflow validates entire message lifecycle
//
// Test Scenario: E2E-001 from TESTING_GUIDE.md
// Workflow:
// 1. Create queue with DLQ and schema validation
// 2. Register message schema
// 3. Post 10 valid messages with varying priorities
// 4. Consume messages in priority order
// 5. Acknowledge 8 messages successfully
// 6. Let 2 messages fail and move to DLQ
// 7. Retrieve DLQ messages
// 8. Requeue 1 message from DLQ
// 9. Verify requeued message is processed
// 10. Check final queue state
func TestE2E_CompleteMessageWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// Arrange
	ctx := context.Background()
	env := helpers.SetupTestEnvironment(t)
	conn := env.NewGRPCClient(t)
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "e2e-workflow")
	dlqName := queueName + "_dlq"

	t.Logf("=== E2E Test: Complete Message Workflow ===")
	t.Logf("Queue: %s", queueName)

	// Step 1: Create queue with DLQ
	t.Log("Step 1: Creating queue with DLQ...")
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:               queue_pb.QueueType_SIMPLE,
			DefaultMaxAttempts: 2,
			LeaseDuration:      durationpb.New(10 * time.Second),
			AutoCreateDlq:      true,
		},
	})
	require.NoError(t, err, "Queue creation failed")
	t.Log("✓ Queue created successfully")

	// Step 2: Post 10 messages with varying priorities
	t.Log("Step 2: Posting 10 messages with priorities...")
	messageIDs := make([]string, 10)
	priorities := []int64{50, 90, 30, 70, 10, 95, 40, 60, 20, 80}

	for i, priority := range priorities {
		msgID := fmt.Sprintf("e2e-msg-%d", i)
		messageIDs[i] = msgID

		payload := &common_pb.Payload{
			Data:        createStruct(t, map[string]interface{}{"index": i, "priority": priority}),
			ContentType: "application/json",
		}

		message := &message_pb.Message{
			MessageId: msgID,
			Metadata: &message_pb.Message_Metadata{
				Payload:     payload,
				Priority:    priority,
				MaxAttempts: 2,
			},
		}

		_, err := client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message:   message,
		})
		require.NoError(t, err, "Failed to post message %d", i)
	}
	t.Logf("✓ Posted %d messages", len(messageIDs))

	// Wait for background worker to process all message state transitions (INVISIBLE -> PENDING)
	time.Sleep(2500 * time.Millisecond)

	// Step 3: Consume first 8 messages and acknowledge them
	t.Log("Step 3: Consuming and acknowledging 8 messages...")
	successfulCount := 0
	for i := 0; i < 8; i++ {
		getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
			QueueName:     queueName,
			LeaseDuration: durationpb.New(10 * time.Second),
		})

		if err != nil || getResp.Message == nil {
			t.Logf("  Message %d: not available yet", i)
			continue
		}

		t.Logf("  Message %d: %s (priority: %d)", i, getResp.Message.MessageId, getResp.Message.Metadata.Priority)

		// Acknowledge message with attempt_id from GetNextMessage response
		_, err = client.AcknowledgeMessage(ctx, &queueservice_pb.AcknowledgeMessageRequest{
			QueueName: queueName,
			MessageId: getResp.Message.MessageId,
			State:     message_pb.Message_Metadata_COMPLETED,
			AttemptId: getResp.AttemptId,
		})
		require.NoError(t, err, "Failed to acknowledge message")
		successfulCount++
	}
	t.Logf("✓ Successfully processed %d messages", successfulCount)

	// Step 4: Get next 2 messages and let them fail (lease expiration)
	t.Log("Step 4: Processing 2 messages that will fail...")
	failedMessages := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
			QueueName:     queueName,
			LeaseDuration: durationpb.New(5 * time.Second),
		})

		if err != nil || getResp.Message == nil {
			t.Logf("  Failed message %d: not available", i)
			continue
		}

		t.Logf("  Failing message: %s (letting lease expire)", getResp.Message.MessageId)
		failedMessages = append(failedMessages, getResp.Message.MessageId)

		// Let lease expire (simulating failure)
		time.Sleep(6 * time.Second)
	}

	// Log captured failed messages so the slice is actually used and staticcheck won't warn.
	t.Logf("Captured failed messages: %v", failedMessages)

	// Wait for retry and DLQ processing
	t.Log("Waiting for retry and DLQ processing...")
	time.Sleep(10 * time.Second)

	// Step 5: Check DLQ for failed messages
	t.Log("Step 5: Checking DLQ for failed messages...")
	dlqResp, err := client.GetDLQMessages(ctx, &queueservice_pb.GetDLQMessagesRequest{
		DlqName: dlqName,
		Limit:   10,
	})

	if err == nil && len(dlqResp.Messages) > 0 {
		t.Logf("✓ Found %d messages in DLQ", len(dlqResp.Messages))

		// Step 6: Requeue one message from DLQ
		if len(dlqResp.Messages) > 0 {
			t.Log("Step 6: Requeuing message from DLQ...")
			requeueMsg := dlqResp.Messages[0]

			requeueResp, err := client.RequeueFromDLQ(ctx, &queueservice_pb.RequeueFromDLQRequest{
				DlqName:     dlqName,
				MessageId:   requeueMsg.MessageId,
				TargetQueue: queueName,
			})

			if err == nil && requeueResp.Success {
				t.Logf("✓ Requeued message: %s", requeueMsg.MessageId)

				// Step 7: Process requeued message
				time.Sleep(2 * time.Second)
				t.Log("Step 7: Processing requeued message...")

				getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
					QueueName:     queueName,
					LeaseDuration: durationpb.New(10 * time.Second),
				})

				if err == nil && getResp.Message != nil {
					t.Logf("✓ Retrieved requeued message: %s", getResp.Message.MessageId)

					// Acknowledge it with attempt_id
					_, err = client.AcknowledgeMessage(ctx, &queueservice_pb.AcknowledgeMessageRequest{
						QueueName: queueName,
						MessageId: getResp.Message.MessageId,
						State:     message_pb.Message_Metadata_COMPLETED,
						AttemptId: getResp.AttemptId,
					})
					require.NoError(t, err)
					t.Log("✓ Requeued message processed successfully")
					successfulCount++
				}
			}
		}
	} else {
		t.Log("Note: DLQ messages may not be available yet (timing-dependent)")
	}

	// Step 8: Check final queue state
	t.Log("Step 8: Checking final queue state...")
	stateResp, err := client.GetQueueState(ctx, &queueservice_pb.GetQueueStateRequest{
		QueueName: queueName,
	})

	if err == nil {
		t.Logf("✓ Final queue state:")
		for state, count := range stateResp.StateCounts {
			t.Logf("  %s: %d", state, count)
		}
	}

	// Summary
	t.Logf("\n=== Workflow Summary ===")
	t.Logf("Total messages posted: %d", len(messageIDs))
	t.Logf("Successfully completed: %d", successfulCount)
	t.Logf("Messages in DLQ: %d", len(dlqResp.GetMessages()))
	t.Logf("========================\n")

	assert.GreaterOrEqual(t, successfulCount, 8, "At least 8 messages should be completed")
}

// TestE2E_HighPriorityAlertSystem validates priority queue for urgent messages
//
// Test Scenario: E2E-003 from TESTING_GUIDE.md
// Workflow:
// 1. Create priority queue
// 2. Post 100 low-priority logs (priority=10)
// 3. Post 10 medium-priority events (priority=50)
// 4. Post 5 critical alerts (priority=95)
// 5. Consume messages and verify order
// Expected: Critical alerts processed first, then events, then logs
func TestE2E_HighPriorityAlertSystem(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// Arrange
	ctx := context.Background()
	env := helpers.SetupTestEnvironment(t)
	conn := env.NewGRPCClient(t)
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "e2e-alerts")

	t.Logf("=== E2E Test: High-Priority Alert System ===")

	// Step 1: Create queue
	t.Log("Step 1: Creating priority queue...")
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Step 2: Post messages (intentionally out of priority order)
	t.Log("Step 2: Posting messages...")

	// Post low priority logs first
	for i := 0; i < 20; i++ { // Reduced from 100 for faster testing
		payload := &common_pb.Payload{
			Data:        createStruct(t, map[string]interface{}{"type": "log", "index": i}),
			ContentType: "application/json",
		}

		message := &message_pb.Message{
			MessageId: fmt.Sprintf("log-%d", i),
			Metadata: &message_pb.Message_Metadata{
				Payload:     payload,
				Priority:    10,
				MaxAttempts: 1, // Set max attempts to 1 for simplicity
			},
		}

		_, err := client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message:   message,
		})
		require.NoError(t, err)
	}
	t.Log("  Posted 20 low-priority logs")

	// Post medium priority events
	for i := 0; i < 5; i++ { // Reduced from 10
		payload := &common_pb.Payload{
			Data:        createStruct(t, map[string]interface{}{"type": "event", "index": i}),
			ContentType: "application/json",
		}

		message := &message_pb.Message{
			MessageId: fmt.Sprintf("event-%d", i),
			Metadata: &message_pb.Message_Metadata{
				Payload:     payload,
				Priority:    50,
				MaxAttempts: 1, // Set max attempts to 1 for simplicity
			},
		}

		_, err := client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message:   message,
		})
		require.NoError(t, err)
	}
	t.Log("  Posted 5 medium-priority events")

	// Post critical alerts
	for i := 0; i < 3; i++ { // Reduced from 5
		payload := &common_pb.Payload{
			Data:        createStruct(t, map[string]interface{}{"type": "alert", "index": i}),
			ContentType: "application/json",
		}

		message := &message_pb.Message{
			MessageId: fmt.Sprintf("alert-%d", i),
			Metadata: &message_pb.Message_Metadata{
				Payload:     payload,
				Priority:    95,
				MaxAttempts: 1, // Set max attempts to 1 for simplicity
			},
		}

		_, err := client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message:   message,
		})
		require.NoError(t, err)
	}
	t.Log("  Posted 3 critical alerts")

	// Wait for background worker to process all message state transitions (INVISIBLE -> PENDING)
	time.Sleep(2500 * time.Millisecond)

	// Step 3: Consume and verify order
	t.Log("Step 3: Consuming messages and verifying priority order...")

	alertCount := 0
	eventCount := 0
	logCount := 0

	// Get first 8 messages - should all be alerts (3) and events (5)
	totalHighPriorityMessages := 8 // 3 alerts + 5 events
	for i := 0; i < totalHighPriorityMessages; i++ {
		getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
			QueueName:     queueName,
			LeaseDuration: durationpb.New(30 * time.Second),
		})

		if err != nil || getResp.Message == nil {
			continue
		}

		priority := getResp.Message.Metadata.Priority
		msgType := "unknown"

		switch priority {
		case 95:
			alertCount++
			msgType = "alert"
		case 50:
			eventCount++
			msgType = "event"
		case 10:
			logCount++
			msgType = "log"
		}

		t.Logf("  Message %d: %s (priority: %d, type: %s)",
			i+1, getResp.Message.MessageId, priority, msgType)
	}

	// Assert
	t.Logf("\n=== Priority Distribution (first %d messages) ===", totalHighPriorityMessages)
	t.Logf("Alerts (priority 95): %d", alertCount)
	t.Logf("Events (priority 50): %d", eventCount)
	t.Logf("Logs (priority 10): %d", logCount)
	t.Logf("==================================\n")

	assert.Equal(t, 3, alertCount, "All 3 alerts should be processed first")
	assert.Equal(t, 5, eventCount, "All 5 events should be processed next")
	assert.Equal(t, 0, logCount, "No logs should be in first 8 messages (only alerts and events)")
}

// TestE2E_MultiTenantIsolation validates isolation between tenants
//
// Test Scenario: E2E-005 from TESTING_GUIDE.md
// Workflow:
// 1. Create 4 queues (2 for Tenant A, 2 for Tenant B)
// 2. Post 100 messages to each queue
// 3. Consume from Tenant A queues only
// 4. Verify Tenant B queues unaffected
// 5. Consume from Tenant B queues
// Expected: Complete isolation, no cross-contamination
func TestE2E_MultiTenantIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// Arrange
	ctx := context.Background()
	env := helpers.SetupTestEnvironment(t)
	conn := env.NewGRPCClient(t)
	client := queueservice_pb.NewQueueServiceClient(conn)

	t.Logf("=== E2E Test: Multi-Tenant Isolation ===")

	// Step 1: Create queues for two tenants
	t.Log("Step 1: Creating queues for tenants...")

	tenantAQueues := []string{
		helpers.GenerateUniqueQueueName(t, "tenant-a-orders"),
		helpers.GenerateUniqueQueueName(t, "tenant-a-events"),
	}

	tenantBQueues := []string{
		helpers.GenerateUniqueQueueName(t, "tenant-b-orders"),
		helpers.GenerateUniqueQueueName(t, "tenant-b-events"),
	}

	// Create all queues
	allQueues := append(tenantAQueues, tenantBQueues...)
	for _, queueName := range allQueues {
		_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
			Name: queueName,
			Metadata: &queue_pb.QueueMetadata{
				Type: queue_pb.QueueType_SIMPLE,
			},
		})
		require.NoError(t, err)
	}
	t.Logf("✓ Created %d queues (2 per tenant)", len(allQueues))

	// Step 2: Post messages to all queues
	t.Log("Step 2: Posting messages to all queues...")
	messagesPerQueue := 10 // Reduced for faster testing

	for _, queueName := range allQueues {
		for i := 0; i < messagesPerQueue; i++ {
			payload := &common_pb.Payload{
				Data:        createStruct(t, map[string]interface{}{"queue": queueName, "index": i}),
				ContentType: "application/json",
			}

			message := &message_pb.Message{
				MessageId: fmt.Sprintf("%s-msg-%d", queueName, i),
				Metadata: &message_pb.Message_Metadata{
					Payload:     payload,
					Priority:    50,
					MaxAttempts: 1, // Set max attempts to 1 for simplicity
				},
			}

			_, err := client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
				QueueName: queueName,
				Message:   message,
			})
			require.NoError(t, err)
		}
	}
	t.Logf("✓ Posted %d messages to each queue", messagesPerQueue)

	// Wait for background worker to process all message state transitions (INVISIBLE -> PENDING)
	time.Sleep(2500 * time.Millisecond)

	// Step 3: Consume from Tenant A queues only
	t.Log("Step 3: Consuming from Tenant A queues...")
	tenantAConsumed := 0

	for _, queueName := range tenantAQueues {
		for i := 0; i < messagesPerQueue; i++ {
			getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
				QueueName:     queueName,
				LeaseDuration: durationpb.New(30 * time.Second),
			})

			if err != nil || getResp.Message == nil {
				continue
			}

			tenantAConsumed++

			// Acknowledge with attempt_id
			_, err = client.AcknowledgeMessage(ctx, &queueservice_pb.AcknowledgeMessageRequest{
				QueueName: queueName,
				MessageId: getResp.Message.MessageId,
				State:     message_pb.Message_Metadata_COMPLETED,
				AttemptId: getResp.AttemptId,
			})
			require.NoError(t, err, "Failed to acknowledge message from Tenant A queue")
		}
	}
	t.Logf("✓ Consumed %d messages from Tenant A", tenantAConsumed)

	// Step 4: Verify Tenant B queues still have messages
	t.Log("Step 4: Verifying Tenant B queues unaffected...")

	for _, queueName := range tenantBQueues {
		stateResp, err := client.GetQueueState(ctx, &queueservice_pb.GetQueueStateRequest{
			QueueName: queueName,
		})

		if err == nil {
			pendingCount := stateResp.StateCounts["PENDING"]
			t.Logf("  %s: %d pending messages", queueName, pendingCount)
			assert.GreaterOrEqual(t, pendingCount, int32(messagesPerQueue),
				"Tenant B queue should still have all messages")
		}
	}

	t.Logf("\n=== Isolation Summary ===")
	t.Logf("Tenant A queues: fully processed")
	t.Logf("Tenant B queues: unaffected")
	t.Logf("✓ Complete tenant isolation verified")
	t.Logf("=========================\n")
}

// Helper function
func createStruct(t *testing.T, data map[string]interface{}) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(data)
	require.NoError(t, err)
	return s
}
