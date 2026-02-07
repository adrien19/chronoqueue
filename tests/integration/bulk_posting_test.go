//go:build integration

package integration

// Package integration provides comprehensive bulk posting tests for ChronoQueue.
//
// These tests validate:
// - Bulk message posting with ALL_OR_NOTHING transaction mode
// - Bulk message posting with BEST_EFFORT transaction mode
// - Error handling for duplicate messages
// - Validation of batch limits
// - Schema validation in bulk operations
//
// Run with: go test -v -tags integration ./tests/integration/ -run TestBulkPosting

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

// TestBulkPosting_AllOrNothing_Success validates successful bulk posting with atomic mode
func TestBulkPosting_AllOrNothing_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "bulk-all-or-nothing")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:               queue_pb.QueueType_SIMPLE,
			LeaseDuration:      durationpb.New(30 * time.Second),
			DefaultMaxAttempts: 3,
		},
	})
	require.NoError(t, err)

	// Create 10 messages
	messages := make([]*message_pb.Message, 10)
	for i := 0; i < 10; i++ {
		payload, err := structpb.NewStruct(map[string]interface{}{
			"task":    fmt.Sprintf("process-item-%d", i),
			"orderId": fmt.Sprintf("order-%d", i),
			"index":   i,
		})
		require.NoError(t, err)

		messages[i] = &message_pb.Message{
			MessageId: helpers.GenerateUniqueMessageID(t),
			Metadata: &message_pb.Message_Metadata{
				Payload: &common_pb.Payload{
					Data:        payload,
					ContentType: "application/json",
				},
				Priority: 5,
			},
		}
	}

	// Post messages in bulk with ALL_OR_NOTHING mode
	response, err := client.PostMessagesBulk(ctx, &queueservice_pb.PostMessagesBulkRequest{
		QueueName:       queueName,
		Messages:        messages,
		TransactionMode: queueservice_pb.PostMessagesBulkRequest_ALL_OR_NOTHING,
	})
	require.NoError(t, err)

	// Validate response
	assert.True(t, response.Success)
	assert.Equal(t, int32(10), response.SuccessfulCount)
	assert.Equal(t, int32(0), response.FailedCount)
	assert.Len(t, response.Results, 10)

	for i, result := range response.Results {
		assert.True(t, result.Success, "Message %d should succeed", i)
		assert.Equal(t, queueservice_pb.PostMessagesBulkResponse_MessagePostResult_SUCCESS, result.ErrorCode)
		assert.Equal(t, messages[i].MessageId, result.MessageId)
	}

	// Wait for messages to be processed
	helpers.WaitForMessageTransition(t)

	// Verify we can retrieve a message from the queue
	msg, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName: queueName,
	})
	require.NoError(t, err)
	require.NotNil(t, msg.Message, "Should be able to retrieve a message from the queue")
}

// TestBulkPosting_BestEffort_PartialSuccess validates partial success with BEST_EFFORT mode
func TestBulkPosting_BestEffort_PartialSuccess(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "bulk-best-effort")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:               queue_pb.QueueType_SIMPLE,
			LeaseDuration:      durationpb.New(30 * time.Second),
			DefaultMaxAttempts: 3,
		},
	})
	require.NoError(t, err)

	// Create messages with one duplicate
	messages := make([]*message_pb.Message, 5)
	duplicateID := helpers.GenerateUniqueMessageID(t)

	for i := 0; i < 5; i++ {
		payload, err := structpb.NewStruct(map[string]interface{}{
			"task":  fmt.Sprintf("process-item-%d", i),
			"index": i,
		})
		require.NoError(t, err)

		messageID := helpers.GenerateUniqueMessageID(t)
		switch i {
		case 0:
			messageID = duplicateID
		case 2:
			// Make message 2 a duplicate of message 0
			messageID = duplicateID
		}

		messages[i] = &message_pb.Message{
			MessageId: messageID,
			Metadata: &message_pb.Message_Metadata{
				Payload: &common_pb.Payload{
					Data:        payload,
					ContentType: "application/json",
				},
				Priority: 5,
			},
		}
	}

	// Post messages in bulk with BEST_EFFORT mode
	response, err := client.PostMessagesBulk(ctx, &queueservice_pb.PostMessagesBulkRequest{
		QueueName:       queueName,
		Messages:        messages,
		TransactionMode: queueservice_pb.PostMessagesBulkRequest_BEST_EFFORT,
	})
	require.NoError(t, err)

	// Validate response - should have partial success
	assert.True(t, response.Success, "Should succeed with at least one successful message")
	assert.Equal(t, int32(4), response.SuccessfulCount, "4 messages should succeed")
	assert.Equal(t, int32(1), response.FailedCount, "1 duplicate should fail")
	assert.Len(t, response.Results, 5)

	// Verify the duplicate failed
	duplicateFailureCount := 0
	for _, result := range response.Results {
		if !result.Success {
			duplicateFailureCount++
			assert.Equal(t, queueservice_pb.PostMessagesBulkResponse_MessagePostResult_DUPLICATE_MESSAGE_ID, result.ErrorCode)
		}
	}
	assert.Equal(t, 1, duplicateFailureCount, "Exactly one duplicate should fail")
}

// TestBulkPosting_ExceedsBatchLimit validates rejection of oversized batches
func TestBulkPosting_ExceedsBatchLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "bulk-limit-check")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:               queue_pb.QueueType_SIMPLE,
			LeaseDuration:      durationpb.New(30 * time.Second),
			DefaultMaxAttempts: 3,
		},
	})
	require.NoError(t, err)

	// Create 1001 messages (over the limit)
	messages := make([]*message_pb.Message, 1001)
	for i := 0; i < 1001; i++ {
		payload, err := structpb.NewStruct(map[string]interface{}{
			"task":  fmt.Sprintf("process-item-%d", i),
			"index": i,
		})
		require.NoError(t, err)

		messages[i] = &message_pb.Message{
			MessageId: helpers.GenerateUniqueMessageID(t),
			Metadata: &message_pb.Message_Metadata{
				Payload: &common_pb.Payload{
					Data:        payload,
					ContentType: "application/json",
				},
				Priority: 5,
			},
		}
	}

	// Post messages in bulk - should fail
	_, err = client.PostMessagesBulk(ctx, &queueservice_pb.PostMessagesBulkRequest{
		QueueName:       queueName,
		Messages:        messages,
		TransactionMode: queueservice_pb.PostMessagesBulkRequest_ALL_OR_NOTHING,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too many messages")
}

// TestBulkPosting_EmptyBatch validates rejection of empty batches
func TestBulkPosting_EmptyBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "bulk-empty-check")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:               queue_pb.QueueType_SIMPLE,
			LeaseDuration:      durationpb.New(30 * time.Second),
			DefaultMaxAttempts: 3,
		},
	})
	require.NoError(t, err)

	// Post empty batch - should fail
	_, err = client.PostMessagesBulk(ctx, &queueservice_pb.PostMessagesBulkRequest{
		QueueName:       queueName,
		Messages:        []*message_pb.Message{},
		TransactionMode: queueservice_pb.PostMessagesBulkRequest_ALL_OR_NOTHING,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no messages")
}

// TestBulkPosting_LargeBatch validates posting a large batch (near the limit)
func TestBulkPosting_LargeBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "bulk-large-batch")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:               queue_pb.QueueType_SIMPLE,
			LeaseDuration:      durationpb.New(30 * time.Second),
			DefaultMaxAttempts: 3,
		},
	})
	require.NoError(t, err)

	// Create 1000 messages (at the limit)
	messages := make([]*message_pb.Message, 1000)
	for i := 0; i < 1000; i++ {
		payload, err := structpb.NewStruct(map[string]interface{}{
			"task":  fmt.Sprintf("process-item-%d", i),
			"index": i,
		})
		require.NoError(t, err)

		messages[i] = &message_pb.Message{
			MessageId: helpers.GenerateUniqueMessageID(t),
			Metadata: &message_pb.Message_Metadata{
				Payload: &common_pb.Payload{
					Data:        payload,
					ContentType: "application/json",
				},
				Priority: 5,
			},
		}
	}

	// Post messages in bulk
	response, err := client.PostMessagesBulk(ctx, &queueservice_pb.PostMessagesBulkRequest{
		QueueName:       queueName,
		Messages:        messages,
		TransactionMode: queueservice_pb.PostMessagesBulkRequest_ALL_OR_NOTHING,
	})
	require.NoError(t, err)

	// Validate response
	assert.True(t, response.Success)
	assert.Equal(t, int32(1000), response.SuccessfulCount)
	assert.Equal(t, int32(0), response.FailedCount)
	assert.Len(t, response.Results, 1000)

	// Wait for messages to be processed
	helpers.WaitForMessageTransition(t)

	// Verify we can retrieve messages from the queue
	for i := 0; i < 5; i++ {
		msg, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
			QueueName: queueName,
		})
		require.NoError(t, err)
		require.NotNil(t, msg.Message, "Should be able to retrieve message %d", i)
	}
}

// TestBulkPosting_PriorityPreservation validates that message priorities are preserved
func TestBulkPosting_PriorityPreservation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "bulk-priority")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:               queue_pb.QueueType_SIMPLE,
			LeaseDuration:      durationpb.New(30 * time.Second),
			DefaultMaxAttempts: 3,
		},
	})
	require.NoError(t, err)

	// Create messages with different priorities
	priorities := []int64{1, 5, 10, 3, 7}
	messages := make([]*message_pb.Message, len(priorities))

	for i, priority := range priorities {
		payload, err := structpb.NewStruct(map[string]interface{}{
			"task":     fmt.Sprintf("priority-task-%d", i),
			"priority": priority,
		})
		require.NoError(t, err)

		messages[i] = &message_pb.Message{
			MessageId: helpers.GenerateUniqueMessageID(t),
			Metadata: &message_pb.Message_Metadata{
				Payload: &common_pb.Payload{
					Data:        payload,
					ContentType: "application/json",
				},
				Priority: priority,
			},
		}
	}

	// Post messages in bulk
	response, err := client.PostMessagesBulk(ctx, &queueservice_pb.PostMessagesBulkRequest{
		QueueName:       queueName,
		Messages:        messages,
		TransactionMode: queueservice_pb.PostMessagesBulkRequest_ALL_OR_NOTHING,
	})
	require.NoError(t, err)
	assert.True(t, response.Success)
	assert.Equal(t, int32(5), response.SuccessfulCount)

	// Wait for messages to be processed
	helpers.WaitForMessageTransition(t)

	// Retrieve messages - should be in priority order (10, 7, 5, 3, 1)
	expectedOrder := []int64{10, 7, 5, 3, 1}
	for i, expectedPriority := range expectedOrder {
		msg, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
			QueueName: queueName,
		})
		require.NoError(t, err)
		require.NotNil(t, msg.Message, "Message %d should exist", i)
		assert.Equal(t, expectedPriority, msg.Message.Metadata.Priority, "Message %d should have priority %d", i, expectedPriority)
	}
}

// TestBulkPosting_QueueNotFound validates error handling for non-existent queue
func TestBulkPosting_QueueNotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	// Create a message
	payload, err := structpb.NewStruct(map[string]interface{}{
		"task": "test-task",
	})
	require.NoError(t, err)

	messages := []*message_pb.Message{
		{
			MessageId: helpers.GenerateUniqueMessageID(t),
			Metadata: &message_pb.Message_Metadata{
				Payload: &common_pb.Payload{
					Data:        payload,
					ContentType: "application/json",
				},
				Priority: 5,
			},
		},
	}

	// Post to non-existent queue
	_, err = client.PostMessagesBulk(ctx, &queueservice_pb.PostMessagesBulkRequest{
		QueueName:       "non-existent-queue-xyz",
		Messages:        messages,
		TransactionMode: queueservice_pb.PostMessagesBulkRequest_ALL_OR_NOTHING,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "queue metadata")
}
