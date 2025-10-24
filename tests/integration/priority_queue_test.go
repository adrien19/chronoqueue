package integration

// Package integration provides priority queue ordering tests for ChronoQueue.
//
// These tests validate:
// - Priority-based message ordering
// - Multiple priority levels (0-100)
// - FIFO behavior within same priority
// - Mixed priority scenarios
//
// Run with: go test -v ./tests/integration/ -run TestPriorityQueue

import (
	"context"
	"fmt"
	"testing"
	"time"

	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/tests/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestPriorityQueue_HighToLowOrdering validates priority-based message ordering
//
// Test Scenario: Priority queue with messages across full priority range (0-100)
// Expected: Messages retrieved in strict priority order (highest first)
func TestPriorityQueue_HighToLowOrdering(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer conn.Close()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-priority-ordering")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Post messages with various priorities (intentionally out of order)
	priorities := []int64{25, 95, 10, 75, 50, 100, 5, 80, 30, 60}

	for i, priority := range priorities {
		payload := &common_pb.Payload{
			Data:        createStruct(t, map[string]interface{}{"index": i, "priority": priority}),
			ContentType: "application/json",
		}

		message := &message_pb.Message{
			MessageId: fmt.Sprintf("priority-msg-%d", i),
			Metadata: &message_pb.Message_Metadata{
				Payload:              payload,
				Priority:             priority,
				MaxAttempts:          1,                 // Set max attempts to 1 for simplicity
				InvisibilityDuration: durationpb.New(0), // Message available immediately
			},
		}

		_, err := client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message:   message,
		})
		require.NoError(t, err)
	}

	// Wait for background worker to process message state transitions (INVISIBLE -> PENDING)
	helpers.WaitForMessageTransition(t)

	// Act - Retrieve all messages
	var retrievedPriorities []int64
	for i := 0; i < len(priorities); i++ {
		getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
			QueueName:     queueName,
			LeaseDuration: durationpb.New(30 * time.Second),
		})
		require.NoError(t, err)
		require.NotNil(t, getResp.Message)
		retrievedPriorities = append(retrievedPriorities, getResp.Message.Metadata.Priority)
	}

	// Assert - Verify descending order (highest priority first)
	for i := 1; i < len(retrievedPriorities); i++ {
		assert.GreaterOrEqual(t, retrievedPriorities[i-1], retrievedPriorities[i],
			"Priority at position %d (%d) should be >= priority at position %d (%d)",
			i-1, retrievedPriorities[i-1], i, retrievedPriorities[i])
	}

	t.Logf("Retrieved priorities in order: %v", retrievedPriorities)
}

// TestPriorityQueue_SamePriorityFIFO validates FIFO ordering within same priority
//
// Test Scenario: Multiple messages with same priority
// Expected: Messages retrieved in FIFO order within that priority level
func TestPriorityQueue_SamePriorityFIFO(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer conn.Close()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-priority-fifo")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Post 10 messages with same priority
	const samePriority = int64(50)
	messageIDs := make([]string, 10)

	for i := 0; i < 10; i++ {
		msgID := fmt.Sprintf("fifo-priority-msg-%d", i)
		messageIDs[i] = msgID

		payload := &common_pb.Payload{
			Data:        createStruct(t, map[string]interface{}{"sequence": i}),
			ContentType: "application/json",
		}

		message := &message_pb.Message{
			MessageId: msgID,
			Metadata: &message_pb.Message_Metadata{
				Payload:              payload,
				Priority:             samePriority,
				MaxAttempts:          1,                 // Set max attempts to 1 for simplicity
				InvisibilityDuration: durationpb.New(0), // Message available immediately
			},
		}

		_, err := client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message:   message,
		})
		require.NoError(t, err)

		// Small delay to ensure distinct timestamps
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for background worker to process message state transitions (INVISIBLE -> PENDING)
	helpers.WaitForMessageTransition(t)

	// Act - Retrieve all messages
	retrievedIDs := make([]string, 10)
	for i := 0; i < 10; i++ {
		getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
			QueueName:     queueName,
			LeaseDuration: durationpb.New(30 * time.Second),
		})
		require.NoError(t, err)
		require.NotNil(t, getResp.Message)
		retrievedIDs[i] = getResp.Message.MessageId
	}

	// Assert - Should be in FIFO order
	assert.Equal(t, messageIDs, retrievedIDs, "Messages with same priority should be retrieved in FIFO order")
}

// TestPriorityQueue_MixedPriorities validates complex priority scenarios
//
// Test Scenario: Mix of high, medium, and low priority messages
// Expected: Correct interleaving based on priority
func TestPriorityQueue_MixedPriorities(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer conn.Close()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-mixed-priorities")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Post messages: 10 low, 10 medium, 10 high priority
	testData := []struct {
		count    int
		priority int64
		label    string
	}{
		{10, 10, "low"},
		{10, 50, "medium"},
		{10, 90, "high"},
	}

	totalMessages := 0
	for _, td := range testData {
		for i := 0; i < td.count; i++ {
			payload := &common_pb.Payload{
				Data:        createStruct(t, map[string]interface{}{"label": td.label, "index": i}),
				ContentType: "application/json",
			}

			message := &message_pb.Message{
				MessageId: fmt.Sprintf("%s-msg-%d", td.label, i),
				Metadata: &message_pb.Message_Metadata{
					Payload:              payload,
					Priority:             td.priority,
					MaxAttempts:          1,                 // Set max attempts to 1 for simplicity
					InvisibilityDuration: durationpb.New(0), // Message available immediately
				},
			}

			_, err := client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
				QueueName: queueName,
				Message:   message,
			})
			require.NoError(t, err)
			totalMessages++
		}
	}

	// Wait for background worker to process message state transitions (INVISIBLE -> PENDING)
	helpers.WaitForMessageTransition(t)

	// Act - Retrieve first 15 messages
	retrievedPriorities := make([]int64, 15)
	for i := 0; i < 15; i++ {
		getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
			QueueName:     queueName,
			LeaseDuration: durationpb.New(30 * time.Second),
		})
		require.NoError(t, err)
		require.NotNil(t, getResp.Message)
		retrievedPriorities[i] = getResp.Message.Metadata.Priority
	}

	// Assert - First 10 should all be high priority (90), next 5 should be medium (50)
	for i := 0; i < 10; i++ {
		assert.Equal(t, int64(90), retrievedPriorities[i],
			"First 10 messages should be high priority")
	}
	for i := 10; i < 15; i++ {
		assert.Equal(t, int64(50), retrievedPriorities[i],
			"Next 5 messages should be medium priority")
	}

	t.Logf("Retrieved priorities: %v", retrievedPriorities)
}

// TestPriorityQueue_PeekWithPriorityRange validates peeking within priority range
//
// Test Scenario: Peek messages within specific priority range
// Expected: Only messages in specified range returned
func TestPriorityQueue_PeekWithPriorityRange(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer conn.Close()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-peek-priority-range")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Post messages with priorities: 10, 30, 50, 70, 90
	priorities := []int64{10, 30, 50, 70, 90}
	for i, priority := range priorities {
		payload := &common_pb.Payload{
			Data:        createStruct(t, map[string]interface{}{"priority": priority}),
			ContentType: "application/json",
		}

		message := &message_pb.Message{
			MessageId: fmt.Sprintf("range-msg-%d", i),
			Metadata: &message_pb.Message_Metadata{
				Payload:     payload,
				Priority:    priority,
				MaxAttempts: 1, // Set max attempts to 1 for simplicity
			},
		}

		_, err := client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message:   message,
		})
		require.NoError(t, err)
	}

	// Act - Peek messages with priority range 40-80
	peekResp, err := client.PeekQueueMessages(ctx, &queueservice_pb.PeekQueueMessagesRequest{
		QueueName: queueName,
		Limit:     10,
		PriorityRange: &queueservice_pb.PeekQueueMessagesRequest_PriorityRange{
			Min: 40,
			Max: 80,
		},
	})

	// Assert
	require.NoError(t, err, "Peek with priority range should succeed")

	// Should return messages with priority 50 and 70
	assert.LessOrEqual(t, len(peekResp.Messages), 2, "Should return at most 2 messages in range 40-80")

	for _, msg := range peekResp.Messages {
		priority := msg.Metadata.Priority
		assert.GreaterOrEqual(t, priority, int64(40), "Priority should be >= 40")
		assert.LessOrEqual(t, priority, int64(80), "Priority should be <= 80")
	}

	t.Logf("Peeked %d messages in priority range 40-80", len(peekResp.Messages))
}

// TestPriorityQueue_BoundaryValues validates priority boundary conditions
//
// Test Scenario: Messages with priority 0, 1, 99, 100
// Expected: All priorities handled correctly, extreme values work
func TestPriorityQueue_BoundaryValues(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer conn.Close()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-priority-boundaries")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Post messages with boundary priorities
	boundaryPriorities := []int64{0, 1, 99, 100}
	for i, priority := range boundaryPriorities {
		payload := &common_pb.Payload{
			Data:        createStruct(t, map[string]interface{}{"priority": priority}),
			ContentType: "application/json",
		}

		message := &message_pb.Message{
			MessageId: fmt.Sprintf("boundary-msg-%d", i),
			Metadata: &message_pb.Message_Metadata{
				Payload:              payload,
				Priority:             priority,
				MaxAttempts:          1,                 // Set max attempts to 1 for simplicity
				InvisibilityDuration: durationpb.New(0), // Message available immediately
			},
		}

		_, err := client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message:   message,
		})
		require.NoError(t, err, "Should accept priority %d", priority)
	}

	// Wait for background worker to process message state transitions (INVISIBLE -> PENDING)
	helpers.WaitForMessageTransition(t)

	// Act - Retrieve all messages
	var retrievedPriorities []int64
	for i := 0; i < len(boundaryPriorities); i++ {
		getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
			QueueName:     queueName,
			LeaseDuration: durationpb.New(30 * time.Second),
		})
		require.NoError(t, err)
		require.NotNil(t, getResp.Message)
		retrievedPriorities = append(retrievedPriorities, getResp.Message.Metadata.Priority)
	}

	// Assert - Should be in descending order: 100, 99, 1, 0
	expectedOrder := []int64{100, 99, 1, 0}
	assert.Equal(t, expectedOrder, retrievedPriorities, "Boundary priorities should be ordered correctly")
}

// // Helper function to create protobuf Struct
// func createStruct(t *testing.T, data map[string]interface{}) *structpb.Struct {
// 	t.Helper()

// 	s, err := structpb.NewStruct(data)
// 	require.NoError(t, err, "Failed to create protobuf Struct")

// 	return s
// }
