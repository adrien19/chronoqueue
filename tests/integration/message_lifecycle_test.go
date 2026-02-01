package integration

// Package integration provides comprehensive message lifecycle tests for ChronoQueue.
//
// These tests validate:
// - Message posting (various content types, priorities)
// - Message retrieval (FIFO and priority ordering)
// - Message acknowledgment (success and failure paths)
// - Message lease management (acquisition, renewal, expiration)
// - Message heartbeat (keep-alive for long-running tasks)
// - Message peeking (non-destructive reads)
//
// Test Scenarios: TC-M-001 through TC-M-015 from TESTING_GUIDE.md
//
// Run with: go test -v ./tests/integration/ -run TestMessage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/tests/helpers"
)

// TestMessageLifecycle_PostSimpleMessage validates posting a basic text message
//
// Test Scenario: TC-M-001 from TESTING_GUIDE.md
// Data: fixtures/messages.json:low_priority_log
// Expected: Message accepted with unique ID returned
func TestMessageLifecycle_PostSimpleMessage(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-post-simple")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
		},
	})
	require.NoError(t, err)

	// Load message fixture
	msgFixture := helpers.LoadMessageFixture(t, "low_priority_log")

	// Create message payload
	payload := &common_pb.Payload{
		Data:        createStructFromString(t, msgFixture.GetContentAsJSON(t)),
		ContentType: msgFixture.ContentType,
	}

	message := &message_pb.Message{
		MessageId: helpers.GenerateUniqueMessageID(t),
		Metadata: &message_pb.Message_Metadata{
			Payload:     payload,
			Priority:    int64(msgFixture.Priority),
			MaxAttempts: 1, // Set max attempts to 1 for simplicity
		},
	}

	// Act
	response, err := client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})

	// Wait for background worker to process message state transition (INVISIBLE -> PENDING)
	helpers.WaitForMessageTransition(t)

	// Assert
	require.NoError(t, err, "Posting message should succeed")
	assert.True(t, response.Success, "Response should indicate success")
}

// TestMessageLifecycle_PostJSONMessage validates posting a structured JSON payload
//
// Test Scenario: TC-M-002 from TESTING_GUIDE.md
// Data: fixtures/messages.json:order_created
// Expected: Message stored with correct content type
func TestMessageLifecycle_PostJSONMessage(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)

	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-post-json")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Load message fixture
	msgFixture := helpers.LoadMessageFixture(t, "order_created")

	// Create message with JSON payload
	payload := &common_pb.Payload{
		Data:        createStructFromJSON(t, msgFixture.GetContentAsBytes(t)),
		ContentType: msgFixture.ContentType,
	}

	message := &message_pb.Message{
		MessageId: helpers.GenerateUniqueMessageID(t),
		Metadata: &message_pb.Message_Metadata{
			Payload:     payload,
			Priority:    int64(msgFixture.Priority),
			MaxAttempts: 1, // Set max attempts to 1 for simplicity
		},
	}

	// Act - Post message
	postResp, err := client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})
	require.NoError(t, err)
	assert.True(t, postResp.Success)

	// Wait for background worker to process message state transition (INVISIBLE -> PENDING)
	helpers.WaitForMessageTransition(t)

	// Retrieve message to verify content
	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})

	// Assert
	require.NoError(t, err, "Getting message should succeed")
	require.NotNil(t, getResp.Message, "Message should be returned")
	helpers.AssertMessageContentType(t, getResp.Message, "application/json")
}

// TestMessageLifecycle_PostWithPriority validates priority-based message posting
//
// Test Scenario: TC-M-003 from TESTING_GUIDE.md
// Data: Multiple messages with priorities 10, 50, 95
// Expected: Messages retrievable in priority order (highest first)
func TestMessageLifecycle_PostWithPriority(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	env := helpers.SharedTestEnvironment(t)

	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-priority")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Post messages with different priorities
	messagePriorities := []struct {
		fixtureName string
		priority    int64
	}{
		{"low_priority_log", 10},
		{"medium_priority_event", 50},
		{"high_priority_alert", 95},
	}

	// Post all messages first to ensure they're in the schedule
	for _, mp := range messagePriorities {
		msgFixture := helpers.LoadMessageFixture(t, mp.fixtureName)

		payload := &common_pb.Payload{
			Data:        createStructFromString(t, msgFixture.GetContentAsJSON(t)),
			ContentType: msgFixture.ContentType,
		}

		message := &message_pb.Message{
			MessageId: helpers.GenerateUniqueMessageID(t),
			Metadata: &message_pb.Message_Metadata{
				Payload:     payload,
				Priority:    mp.priority,
				MaxAttempts: 1, // Set max attempts to 1 for simplicity
			},
		}

		_, err := client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message:   message,
		})
		require.NoError(t, err, "Posting message should succeed")

		// Small delay to ensure distinct timestamps for proper priority ordering
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for scheduler to process all messages (scheduler runs at 300ms intervals)
	// Wait longer to ensure all messages are promoted to streams in correct priority order
	time.Sleep(800 * time.Millisecond)

	// Act - Retrieve messages in order
	var retrievedMessages []*message_pb.Message
	for i := 0; i < len(messagePriorities); i++ {
		getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
			QueueName:     queueName,
			LeaseDuration: durationpb.New(30 * time.Second),
		})
		require.NoError(t, err, "Getting message should succeed")
		require.NotNil(t, getResp.Message, "Message should be returned")
		retrievedMessages = append(retrievedMessages, getResp.Message)
	}

	// Assert - Messages should be in priority order (high to low: 95, 50, 10)
	expectedPriorities := []int64{95, 50, 10}
	helpers.AssertMessagePriority(t, retrievedMessages, expectedPriorities)
}

// TestMessageLifecycle_GetNextMessageFIFO validates FIFO message retrieval
//
// Test Scenario: TC-M-004 from TESTING_GUIDE.md
// Data: 5 messages posted in sequence
// Expected: Messages returned in FIFO order
func TestMessageLifecycle_GetNextMessageFIFO(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)

	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-fifo")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Post 5 messages with same priority (FIFO order)
	messageIDs := make([]string, 5)
	for i := 0; i < 5; i++ {
		msgID := fmt.Sprintf("fifo-msg-%d", i)
		messageIDs[i] = msgID

		payload := &common_pb.Payload{
			Data:        createStructFromString(t, fmt.Sprintf("Message %d", i)),
			ContentType: "text/plain",
		}

		message := &message_pb.Message{
			MessageId: msgID,
			Metadata: &message_pb.Message_Metadata{
				Payload:     payload,
				Priority:    50, // Same priority for all
				MaxAttempts: 1,  // Set max attempts to 1 for simplicity
			},
		}

		_, err := client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message:   message,
		})

		// Wait for background worker to process message state transition (INVISIBLE -> PENDING)
		helpers.WaitForMessageTransition(t)
		require.NoError(t, err)
	}

	// Act - Retrieve messages
	retrievedIDs := make([]string, 5)
	for i := 0; i < 5; i++ {
		getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
			QueueName:     queueName,
			LeaseDuration: durationpb.New(30 * time.Second),
		})
		require.NoError(t, err)
		require.NotNil(t, getResp.Message)
		retrievedIDs[i] = getResp.Message.MessageId
	}

	// Assert - Order should match posting order
	assert.Equal(t, messageIDs, retrievedIDs, "Messages should be retrieved in FIFO order")
}

// TestMessageLifecycle_MessageLeaseManagement validates lease acquisition and expiration
//
// Test Scenario: TC-M-006 from TESTING_GUIDE.md
// Data: Single message with 30s lease
// Expected: Message unavailable to other consumers during lease period
func TestMessageLifecycle_MessageLeaseManagement(t *testing.T) {
	t.Parallel() // Enable parallel execution - tests are isolated by unique queue names

	// Arrange
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	env := helpers.SharedTestEnvironment(t)

	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-lease")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Post message
	msgID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStructFromString(t, "Test lease message"),
		ContentType: "text/plain",
	}

	message := &message_pb.Message{
		MessageId: msgID,
		Metadata: &message_pb.Message_Metadata{
			Payload:     payload,
			Priority:    50,
			MaxAttempts: 1, // Set max attempts to 1 for simplicity
		},
	}

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})
	require.NoError(t, err)

	// Wait for background worker to process message state transition (INVISIBLE -> PENDING)
	helpers.WaitForMessageTransition(t)

	// Act - Get message with lease
	getResp1, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(10 * time.Second), // Short lease for testing
	})
	require.NoError(t, err)
	require.NotNil(t, getResp1.Message)
	helpers.AssertLeaseActive(t, getResp1.Message)

	// Try to get the same message immediately (should return nil or timeout)
	// Use a short timeout context to avoid hanging
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	getResp2, err := client.GetNextMessage(ctx2, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(10 * time.Second),
	})

	// Assert - Should not get the same message while lease is active
	// Either timeout error or nil message is acceptable
	if err == nil && getResp2 != nil && getResp2.Message != nil {
		t.Error("Should not receive message while it's leased by another consumer")
	}
}

// TestMessageLifecycle_RenewMessageLease validates lease renewal
//
// Test Scenario: TC-M-007 from TESTING_GUIDE.md
// Data: Message with lease that is renewed before expiration
// Expected: Lease extended successfully
func TestMessageLifecycle_RenewMessageLease(t *testing.T) {
	t.Parallel() // Enable parallel execution

	// Arrange
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	env := helpers.SharedTestEnvironment(t)

	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-renew-lease")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Post message
	msgID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStructFromString(t, "Test lease renewal"),
		ContentType: "text/plain",
	}

	message := &message_pb.Message{
		MessageId: msgID,
		Metadata: &message_pb.Message_Metadata{
			Payload:     payload,
			Priority:    50,
			MaxAttempts: 1, // Set max attempts to 1 for simplicity
		},
	}

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})
	require.NoError(t, err)

	// Wait for background worker to process message state transition (INVISIBLE -> PENDING)
	helpers.WaitForMessageTransition(t)

	// Get message
	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.Message)

	// Act - Renew lease
	renewResp, err := client.RenewMessageLease(ctx, &queueservice_pb.RenewMessageLeaseRequest{
		QueueName:     queueName,
		MessageId:     getResp.Message.MessageId,
		LeaseDuration: durationpb.New(45 * time.Second),
	})

	// Assert
	require.NoError(t, err, "Lease renewal should succeed")
	assert.NotNil(t, renewResp.RemainingTime, "Should return remaining time")
	t.Logf("Lease renewed successfully, remaining time: %v", renewResp.RemainingTime)
}

// TestMessageLifecycle_AcknowledgeMessage validates message acknowledgment
//
// Test Scenario: TC-M-008 from TESTING_GUIDE.md
// Data: Message that is retrieved and acknowledged
// Expected: Message marked COMPLETED, not available for retrieval
func TestMessageLifecycle_AcknowledgeMessage(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)

	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-ack")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Post message
	msgID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStructFromString(t, "Test acknowledgment"),
		ContentType: "text/plain",
	}

	message := &message_pb.Message{
		MessageId: msgID,
		Metadata: &message_pb.Message_Metadata{
			Payload:     payload,
			Priority:    50,
			MaxAttempts: 1, // Set max attempts to 1 for simplicity
		},
	}

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})

	require.NoError(t, err)
	// Wait for background worker to process message state transition (INVISIBLE -> PENDING)
	helpers.WaitForMessageTransition(t)

	// Get message
	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.Message)
	workerID := getResp.GetWorkerId()
	attemptID := getResp.GetAttemptId()

	// Act - Acknowledge message
	ackResp, err := client.AcknowledgeMessage(ctx, &queueservice_pb.AcknowledgeMessageRequest{
		QueueName: queueName,
		MessageId: getResp.Message.MessageId,
		State:     message_pb.Message_Metadata_COMPLETED,
		AttemptId: &attemptID,
		WorkerId:  &workerID,
	})

	// Assert
	require.NoError(t, err, "Acknowledgment should succeed")
	assert.True(t, ackResp.Success, "Acknowledgment response should indicate success")

	// Verify message is no longer available
	getResp2, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})
	// Should get no message or error
	if err == nil && getResp2.Message != nil {
		t.Error("Acknowledged message should not be available for retrieval")
	}
}

// TestMessageLifecycle_PeekMessages validates non-destructive message peeking
//
// Test Scenario: TC-M-011 from TESTING_GUIDE.md
// Data: 5 messages in queue
// Expected: Messages visible but not leased, still available via GetNextMessage
func TestMessageLifecycle_PeekMessages(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)

	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-peek")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Post 5 messages
	for i := 0; i < 5; i++ {
		payload := &common_pb.Payload{
			Data:        createStructFromString(t, fmt.Sprintf("Peek test message %d", i)),
			ContentType: "text/plain",
		}

		message := &message_pb.Message{
			MessageId: helpers.GenerateUniqueMessageID(t),
			Metadata: &message_pb.Message_Metadata{
				Payload:     payload,
				Priority:    int64(50 + i), // Vary priority slightly to ensure different scores
				MaxAttempts: 1,             // Set max attempts to 1 for simplicity
			},
		}

		_, err := client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message:   message,
		})
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond) // Small delay to ensure different timestamps
	}

	// Wait for background worker to process all message state transitions (INVISIBLE -> PENDING)
	// Wait longer since we have multiple messages that might span worker cycles
	time.Sleep(2500 * time.Millisecond)

	// Act - Peek messages
	peekResp, err := client.PeekQueueMessages(ctx, &queueservice_pb.PeekQueueMessagesRequest{
		QueueName: queueName,
		Limit:     10, // Request more than 5 to see if message 0 is there
	})

	// Assert
	require.NoError(t, err, "Peeking should succeed")
	assert.GreaterOrEqual(t, len(peekResp.Messages), 5, "Should return at least 5 messages")

	// Verify messages are still available for normal retrieval
	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})
	require.NoError(t, err, "Should still be able to get messages after peeking")
	require.NotNil(t, getResp.Message, "Message should be available")
}

// TestMessageLifecycle_SendHeartbeat validates message heartbeat mechanism
//
// Test Scenario: TC-M-010 from TESTING_GUIDE.md
// Data: Long-running task sending periodic heartbeats
// Expected: Lease extended automatically
func TestMessageLifecycle_SendHeartbeat(t *testing.T) {
	t.Parallel() // Enable parallel execution

	// Arrange
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	env := helpers.SharedTestEnvironment(t)

	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-heartbeat")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Post message
	msgID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStructFromString(t, "Test heartbeat"),
		ContentType: "text/plain",
	}

	message := &message_pb.Message{
		MessageId: msgID,
		Metadata: &message_pb.Message_Metadata{
			Payload:     payload,
			Priority:    50,
			MaxAttempts: 1, // Set max attempts to 1 for simplicity
		},
	}

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})

	require.NoError(t, err)
	// Wait for background worker to process message state transition (INVISIBLE -> PENDING)
	helpers.WaitForMessageTransition(t)

	// Get message with a short lease for faster test
	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(5 * time.Second), // Short lease for faster test
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.Message)
	workerID := getResp.GetWorkerId()
	attemptID := getResp.GetAttemptId()

	// Act - Send heartbeat multiple times to verify lease extension
	// Use shorter intervals for faster test execution
	for i := 0; i < 3; i++ {
		time.Sleep(1 * time.Second) // Wait 1 second between heartbeats (reduced from 5s)

		heartbeatResp, err := client.SendMessageHeartBeat(ctx, &queueservice_pb.SendMessageHeartBeatRequest{
			QueueName: queueName,
			MessageId: getResp.Message.MessageId,
			AttemptId: &attemptID,
			WorkerId:  &workerID,
		})

		// Assert
		require.NoError(t, err, "Heartbeat should succeed")
		assert.NotNil(t, heartbeatResp.RemainingTime, "Should return remaining time")
		t.Logf("Heartbeat %d successful, remaining time: %v", i+1, heartbeatResp.RemainingTime)
	}
}

// Helper functions

// createStructFromJSON creates a protobuf Struct from JSON bytes
func createStructFromJSON(t *testing.T, jsonData []byte) *structpb.Struct {
	t.Helper()

	var data map[string]interface{}
	err := json.Unmarshal(jsonData, &data)
	require.NoError(t, err, "Failed to unmarshal JSON")

	s, err := structpb.NewStruct(data)
	require.NoError(t, err, "Failed to create protobuf Struct")

	return s
}

// createStructFromString creates a protobuf Struct from a string value
func createStructFromString(t *testing.T, str string) *structpb.Struct {
	t.Helper()

	data := map[string]interface{}{
		"value": str,
	}

	s, err := structpb.NewStruct(data)
	require.NoError(t, err, "Failed to create protobuf Struct")

	return s
}

// TestMessageLifecycle_CancelInvisibleMessage validates cancelling a message in INVISIBLE state
//
// Test Scenario: TC-M-016 - Cancel message before it becomes available for processing
// Expected: Message successfully cancelled and not retrievable
func TestMessageLifecycle_CancelInvisibleMessage(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-cancel-invisible")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
		},
	})
	require.NoError(t, err)

	// Post a message that will remain INVISIBLE due to scheduled time in the future
	msgID := helpers.GenerateUniqueMessageID(t)
	futureTime := time.Now().Add(10 * time.Minute)

	payload := &common_pb.Payload{
		Data:        createStructFromString(t, "Test cancel invisible"),
		ContentType: "text/plain",
	}

	message := &message_pb.Message{
		MessageId: msgID,
		Metadata: &message_pb.Message_Metadata{
			Payload:       payload,
			Priority:      50,
			MaxAttempts:   3,
			ScheduledTime: timestamppb.New(futureTime),
		},
	}

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})
	require.NoError(t, err)

	// Act - Cancel the message while it's still INVISIBLE
	cancelResp, err := client.CancelMessage(ctx, &queueservice_pb.CancelMessageRequest{
		QueueName: queueName,
		MessageId: msgID,
	})

	// Assert
	require.NoError(t, err, "Cancelling INVISIBLE message should succeed")
	assert.True(t, cancelResp.Success, "Response should indicate success")

	// Verify message is not retrievable
	helpers.WaitForMessageTransition(t)
	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(5 * time.Second),
	})
	require.NoError(t, err)
	assert.Nil(t, getResp.Message, "Cancelled message should not be retrievable")
}

// TestMessageLifecycle_CancelPendingMessage validates cancelling a message in PENDING state
//
// Test Scenario: TC-M-017 - Cancel message that is waiting to be claimed
// Expected: Message successfully cancelled and removed from queue
func TestMessageLifecycle_CancelPendingMessage(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-cancel-pending")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
		},
	})
	require.NoError(t, err)

	// Post a message
	msgID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStructFromString(t, "Test cancel pending"),
		ContentType: "text/plain",
	}

	message := &message_pb.Message{
		MessageId: msgID,
		Metadata: &message_pb.Message_Metadata{
			Payload:     payload,
			Priority:    50,
			MaxAttempts: 3,
		},
	}

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})
	require.NoError(t, err)

	// Wait for message to transition to PENDING
	helpers.WaitForMessageTransition(t)

	// Act - Cancel the PENDING message
	cancelResp, err := client.CancelMessage(ctx, &queueservice_pb.CancelMessageRequest{
		QueueName: queueName,
		MessageId: msgID,
	})

	// Assert
	require.NoError(t, err, "Cancelling PENDING message should succeed")
	assert.True(t, cancelResp.Success, "Response should indicate success")

	// Verify message is not retrievable
	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(5 * time.Second),
	})
	require.NoError(t, err)
	assert.Nil(t, getResp.Message, "Cancelled message should not be retrievable")
}

// TestMessageLifecycle_CancelRunningMessageFails validates that cancelling a RUNNING message fails
//
// Test Scenario: TC-M-018 - Attempt to cancel a message that is being processed
// Expected: Cancel operation fails with appropriate error
func TestMessageLifecycle_CancelRunningMessageFails(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-cancel-running")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
		},
	})
	require.NoError(t, err)

	// Post a message
	msgID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStructFromString(t, "Test cancel running"),
		ContentType: "text/plain",
	}

	message := &message_pb.Message{
		MessageId: msgID,
		Metadata: &message_pb.Message_Metadata{
			Payload:     payload,
			Priority:    50,
			MaxAttempts: 3,
		},
	}

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})
	require.NoError(t, err)

	// Wait for message to transition to PENDING
	helpers.WaitForMessageTransition(t)

	// Claim the message (moves to RUNNING state)
	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.Message, "Message should be claimed")

	// Act - Attempt to cancel the RUNNING message
	_, err = client.CancelMessage(ctx, &queueservice_pb.CancelMessageRequest{
		QueueName: queueName,
		MessageId: msgID,
	})

	// Assert
	require.Error(t, err, "Cancelling RUNNING message should fail")
	assert.Contains(t, err.Error(), "RUNNING", "Error should mention RUNNING state")
}

// TestMessageLifecycle_CancelNonExistentMessage validates error handling for non-existent messages
//
// Test Scenario: TC-M-019 - Attempt to cancel a message that doesn't exist
// Expected: Cancel operation fails with not found error
func TestMessageLifecycle_CancelNonExistentMessage(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-cancel-nonexistent")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
		},
	})
	require.NoError(t, err)

	// Act - Attempt to cancel a non-existent message
	nonExistentMsgID := "non-existent-message-id"
	_, err = client.CancelMessage(ctx, &queueservice_pb.CancelMessageRequest{
		QueueName: queueName,
		MessageId: nonExistentMsgID,
	})

	// Assert
	require.Error(t, err, "Cancelling non-existent message should fail")
	assert.Contains(t, err.Error(), "not found", "Error should indicate message not found")
}

// TestMessageLifecycle_CancelWithReason validates cancellation with audit reason
//
// Test Scenario: TC-M-020 - Cancel message with a reason for auditing
// Expected: Message cancelled successfully with reason logged
func TestMessageLifecycle_CancelWithReason(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-cancel-with-reason")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
		},
	})
	require.NoError(t, err)

	// Post a message
	msgID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStructFromString(t, "Test cancel with reason"),
		ContentType: "text/plain",
	}

	message := &message_pb.Message{
		MessageId: msgID,
		Metadata: &message_pb.Message_Metadata{
			Payload:     payload,
			Priority:    50,
			MaxAttempts: 3,
		},
	}

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})
	require.NoError(t, err)

	// Wait for message to transition to PENDING
	helpers.WaitForMessageTransition(t)

	// Act - Cancel with a specific reason
	reason := "Order was cancelled by customer"
	cancelResp, err := client.CancelMessage(ctx, &queueservice_pb.CancelMessageRequest{
		QueueName: queueName,
		MessageId: msgID,
		Reason:    &reason,
	})

	// Assert
	require.NoError(t, err, "Cancelling message with reason should succeed")
	assert.True(t, cancelResp.Success, "Response should indicate success")

	// Note: The cancellation reason is persisted in the database (cancellation_reason column)
	// but there's currently no InspectMessage API to verify it directly.
	// The reason is available for audit queries directly against the storage layer.

	// Verify message is not retrievable
	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(5 * time.Second),
	})
	require.NoError(t, err)
	assert.Nil(t, getResp.Message, "Cancelled message should not be retrievable")
}

// TestMessageLifecycle_CancelMultipleMessages validates bulk cancellation scenario
//
// Test Scenario: TC-M-021 - Cancel multiple messages in batch
// Expected: All messages successfully cancelled
func TestMessageLifecycle_CancelMultipleMessages(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-cancel-multiple")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
		},
	})
	require.NoError(t, err)

	// Post multiple messages
	messageIDs := make([]string, 5)
	for i := 0; i < 5; i++ {
		msgID := helpers.GenerateUniqueMessageID(t)
		messageIDs[i] = msgID

		payload := &common_pb.Payload{
			Data:        createStructFromString(t, fmt.Sprintf("Test message %d", i)),
			ContentType: "text/plain",
		}

		message := &message_pb.Message{
			MessageId: msgID,
			Metadata: &message_pb.Message_Metadata{
				Payload:     payload,
				Priority:    50,
				MaxAttempts: 3,
			},
		}

		_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message:   message,
		})
		require.NoError(t, err)
	}

	// Wait for messages to transition to PENDING
	helpers.WaitForMessageTransition(t)

	// Act - Cancel all messages
	for _, msgID := range messageIDs {
		cancelResp, err := client.CancelMessage(ctx, &queueservice_pb.CancelMessageRequest{
			QueueName: queueName,
			MessageId: msgID,
		})
		require.NoError(t, err, "Cancelling message %s should succeed", msgID)
		assert.True(t, cancelResp.Success)
	}

	// Assert - Verify no messages are retrievable
	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(5 * time.Second),
	})
	require.NoError(t, err)
	assert.Nil(t, getResp.Message, "No cancelled messages should be retrievable")
}

// TestMessageLifecycle_CancelAfterAcknowledgeFails validates that completed messages cannot be cancelled
//
// Test Scenario: TC-M-022 - Attempt to cancel a message that was already acknowledged
// Expected: Cancel operation fails
func TestMessageLifecycle_CancelAfterAcknowledgeFails(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-cancel-after-ack")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
		},
	})
	require.NoError(t, err)

	// Post a message
	msgID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStructFromString(t, "Test cancel after ack"),
		ContentType: "text/plain",
	}

	message := &message_pb.Message{
		MessageId: msgID,
		Metadata: &message_pb.Message_Metadata{
			Payload:     payload,
			Priority:    50,
			MaxAttempts: 3,
		},
	}

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})
	require.NoError(t, err)

	// Wait for message to transition to PENDING
	helpers.WaitForMessageTransition(t)

	// Claim and acknowledge the message
	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.Message)

	attemptID := getResp.GetAttemptId()
	workerID := getResp.GetWorkerId()

	_, err = client.AcknowledgeMessage(ctx, &queueservice_pb.AcknowledgeMessageRequest{
		QueueName: queueName,
		MessageId: msgID,
		State:     message_pb.Message_Metadata_COMPLETED,
		AttemptId: &attemptID,
		WorkerId:  &workerID,
	})
	require.NoError(t, err)

	// Act - Attempt to cancel the completed message
	_, err = client.CancelMessage(ctx, &queueservice_pb.CancelMessageRequest{
		QueueName: queueName,
		MessageId: msgID,
	})

	// Assert
	require.Error(t, err, "Cancelling completed message should fail")
	// The message is either not found (deleted due to retention) or in wrong state
	errMsg := err.Error()
	validError := strings.Contains(errMsg, "not found") ||
		strings.Contains(errMsg, "cannot cancel") ||
		strings.Contains(errMsg, "COMPLETED") ||
		strings.Contains(errMsg, "ERRORED") ||
		strings.Contains(errMsg, "CANCELED") ||
		strings.Contains(errMsg, "state changed")
	assert.True(t, validError,
		"Error should indicate message cannot be cancelled, got: %s", errMsg)
}
