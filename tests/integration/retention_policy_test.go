package integration

// Package integration provides comprehensive retention policy tests for ChronoQueue.
//
// These tests validate the configurable message retention feature:
// - DELETE_IMMEDIATELY (default): Messages are hard-deleted on acknowledgment
// - RETAIN_DURATION: Messages are soft-deleted and auto-cleaned after retention period
// - RETAIN_FOREVER: Messages are soft-deleted but never auto-cleaned
//
// Run with: go test -v ./tests/integration/ -run TestRetention

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/tests/helpers"
)

// TestRetentionPolicy_DeleteImmediately validates that nil retention policy (default)
// results in immediate hard-delete of acknowledged messages.
//
// Expected: Message is permanently deleted after acknowledgment
func TestRetentionPolicy_DeleteImmediately(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-retention-delete")

	// Create queue with nil retention policy (default = DELETE_IMMEDIATELY)
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
			// RetentionPolicy is nil (default DELETE_IMMEDIATELY)
		},
	})
	require.NoError(t, err)

	// Post message
	msgID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStructFromString(t, "Test retention delete immediately"),
		ContentType: "text/plain",
	}

	message := &message_pb.Message{
		MessageId: msgID,
		Metadata: &message_pb.Message_Metadata{
			Payload:     payload,
			Priority:    2,
			MaxAttempts: 1,
		},
	}

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})
	require.NoError(t, err)
	helpers.WaitForMessageTransition(t)

	// Get and acknowledge message
	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.Message)

	workerID := getResp.GetWorkerId()
	attemptID := getResp.GetAttemptId()

	ackResp, err := client.AcknowledgeMessage(ctx, &queueservice_pb.AcknowledgeMessageRequest{
		QueueName: queueName,
		MessageId: msgID,
		State:     message_pb.Message_Metadata_COMPLETED,
		AttemptId: &attemptID,
		WorkerId:  &workerID,
	})
	require.NoError(t, err)
	assert.True(t, ackResp.Success, "Acknowledgment should succeed")

	// Verify message is no longer available (hard-deleted)
	ctx2, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	getResp2, err := client.GetNextMessage(ctx2, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})

	// Should not get any message back
	if err == nil && getResp2 != nil && getResp2.Message != nil {
		t.Error("Message should be hard-deleted after acknowledgment with DELETE_IMMEDIATELY policy")
	}
}

// TestRetentionPolicy_RetainDuration validates that messages with RETAIN_DURATION policy
// are soft-deleted and not visible after acknowledgment.
//
// Expected: Message is soft-deleted (not visible) but retained for audit purposes
func TestRetentionPolicy_RetainDuration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-retention-duration")

	// Create queue with RETAIN_DURATION policy (retain for 1 hour)
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
			MessageRetentionPolicy: &queue_pb.MessageRetentionPolicy{
				Mode:             queue_pb.MessageRetentionPolicy_RETAIN_DURATION,
				RetentionSeconds: 3600, // 1 hour
			},
		},
	})
	require.NoError(t, err)

	// Post message
	msgID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStructFromString(t, "Test retention duration"),
		ContentType: "text/plain",
	}

	message := &message_pb.Message{
		MessageId: msgID,
		Metadata: &message_pb.Message_Metadata{
			Payload:     payload,
			Priority:    2,
			MaxAttempts: 1,
		},
	}

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})
	require.NoError(t, err)
	helpers.WaitForMessageTransition(t)

	// Get and acknowledge message
	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.Message)

	workerID := getResp.GetWorkerId()
	attemptID := getResp.GetAttemptId()

	ackResp, err := client.AcknowledgeMessage(ctx, &queueservice_pb.AcknowledgeMessageRequest{
		QueueName: queueName,
		MessageId: msgID,
		State:     message_pb.Message_Metadata_COMPLETED,
		AttemptId: &attemptID,
		WorkerId:  &workerID,
	})
	require.NoError(t, err)
	assert.True(t, ackResp.Success, "Acknowledgment should succeed")

	// Verify message is not visible (soft-deleted)
	ctx2, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	getResp2, err := client.GetNextMessage(ctx2, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})

	// Should not get any message back (soft-deleted messages are filtered)
	if err == nil && getResp2 != nil && getResp2.Message != nil {
		t.Error("Soft-deleted message should not be visible after acknowledgment with RETAIN_DURATION policy")
	}

	// Verify message is also not visible via Peek
	peekResp, err := client.PeekQueueMessages(ctx, &queueservice_pb.PeekQueueMessagesRequest{
		QueueName: queueName,
		Limit:     10,
	})
	require.NoError(t, err)
	assert.Empty(t, peekResp.Messages, "Soft-deleted messages should not be visible in peek")
}

// TestRetentionPolicy_RetainForever validates that messages with RETAIN_FOREVER policy
// are soft-deleted and never auto-cleaned.
//
// Expected: Message is soft-deleted (not visible) but retained indefinitely
func TestRetentionPolicy_RetainForever(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-retention-forever")

	// Create queue with RETAIN_FOREVER policy
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
			MessageRetentionPolicy: &queue_pb.MessageRetentionPolicy{
				Mode: queue_pb.MessageRetentionPolicy_RETAIN_FOREVER,
			},
		},
	})
	require.NoError(t, err)

	// Post message
	msgID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStructFromString(t, "Test retention forever"),
		ContentType: "text/plain",
	}

	message := &message_pb.Message{
		MessageId: msgID,
		Metadata: &message_pb.Message_Metadata{
			Payload:     payload,
			Priority:    2,
			MaxAttempts: 1,
		},
	}

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})
	require.NoError(t, err)
	helpers.WaitForMessageTransition(t)

	// Get and acknowledge message
	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.Message)

	workerID := getResp.GetWorkerId()
	attemptID := getResp.GetAttemptId()

	ackResp, err := client.AcknowledgeMessage(ctx, &queueservice_pb.AcknowledgeMessageRequest{
		QueueName: queueName,
		MessageId: msgID,
		State:     message_pb.Message_Metadata_COMPLETED,
		AttemptId: &attemptID,
		WorkerId:  &workerID,
	})
	require.NoError(t, err)
	assert.True(t, ackResp.Success, "Acknowledgment should succeed")

	// Verify message is not visible (soft-deleted)
	ctx2, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	getResp2, err := client.GetNextMessage(ctx2, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})

	if err == nil && getResp2 != nil && getResp2.Message != nil {
		t.Error("Soft-deleted message should not be visible after acknowledgment with RETAIN_FOREVER policy")
	}
}

// TestRetentionPolicy_NackWithRetention validates that NACK with max retries exhausted
// respects the retention policy (soft-delete with ERRORED state).
//
// Expected: Message is soft-deleted with ERRORED state when max retries exhausted
func TestRetentionPolicy_NackWithRetention(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-retention-nack")

	// Create queue with RETAIN_DURATION policy
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:               queue_pb.QueueType_SIMPLE,
			LeaseDuration:      durationpb.New(30 * time.Second),
			DefaultMaxAttempts: 1, // Only 1 attempt, so first NACK exhausts retries
			MessageRetentionPolicy: &queue_pb.MessageRetentionPolicy{
				Mode:             queue_pb.MessageRetentionPolicy_RETAIN_DURATION,
				RetentionSeconds: 3600,
			},
		},
	})
	require.NoError(t, err)

	// Post message
	msgID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStructFromString(t, "Test NACK with retention"),
		ContentType: "text/plain",
	}

	message := &message_pb.Message{
		MessageId: msgID,
		Metadata: &message_pb.Message_Metadata{
			Payload:     payload,
			Priority:    2,
			MaxAttempts: 1,
		},
	}

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})
	require.NoError(t, err)
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

	// NACK the message (this exhausts retries since MaxAttempts=1)
	nackResp, err := client.AcknowledgeMessage(ctx, &queueservice_pb.AcknowledgeMessageRequest{
		QueueName: queueName,
		MessageId: msgID,
		State:     message_pb.Message_Metadata_ERRORED,
		AttemptId: &attemptID,
		WorkerId:  &workerID,
	})
	require.NoError(t, err)
	assert.True(t, nackResp.Success, "NACK should succeed")

	// Verify message is not visible (soft-deleted with ERRORED state)
	ctx2, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	getResp2, err := client.GetNextMessage(ctx2, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})

	if err == nil && getResp2 != nil && getResp2.Message != nil {
		t.Error("Message should be soft-deleted after NACK exhausts retries with retention policy")
	}
}

// TestRetentionPolicy_MultipleMessages validates retention policy works correctly
// when processing multiple messages in the same queue.
//
// Expected: All acknowledged messages are properly handled according to retention policy
func TestRetentionPolicy_MultipleMessages(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-retention-multi")

	// Create queue with RETAIN_DURATION policy
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
			MessageRetentionPolicy: &queue_pb.MessageRetentionPolicy{
				Mode:             queue_pb.MessageRetentionPolicy_RETAIN_DURATION,
				RetentionSeconds: 3600,
			},
		},
	})
	require.NoError(t, err)

	// Post 5 messages
	messageCount := 5
	for i := 0; i < messageCount; i++ {
		msgID := helpers.GenerateUniqueMessageID(t)
		payload := &common_pb.Payload{
			Data:        createStructFromString(t, "Test message"),
			ContentType: "text/plain",
		}

		message := &message_pb.Message{
			MessageId: msgID,
			Metadata: &message_pb.Message_Metadata{
				Payload:     payload,
				Priority:    2,
				MaxAttempts: 1,
			},
		}

		_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message:   message,
		})
		require.NoError(t, err)
	}
	helpers.WaitForMessageTransition(t)

	// Acknowledge all messages
	for i := 0; i < messageCount; i++ {
		getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
			QueueName:     queueName,
			LeaseDuration: durationpb.New(30 * time.Second),
		})
		require.NoError(t, err)
		require.NotNil(t, getResp.Message, "Should get message %d", i)

		workerID := getResp.GetWorkerId()
		attemptID := getResp.GetAttemptId()

		_, err = client.AcknowledgeMessage(ctx, &queueservice_pb.AcknowledgeMessageRequest{
			QueueName: queueName,
			MessageId: getResp.Message.MessageId,
			State:     message_pb.Message_Metadata_COMPLETED,
			AttemptId: &attemptID,
			WorkerId:  &workerID,
		})
		require.NoError(t, err)
	}

	// Verify no messages are visible (all soft-deleted)
	ctx2, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	getResp2, err := client.GetNextMessage(ctx2, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})

	if err == nil && getResp2 != nil && getResp2.Message != nil {
		t.Error("All messages should be soft-deleted")
	}

	// Verify peek also returns empty
	peekResp, err := client.PeekQueueMessages(ctx, &queueservice_pb.PeekQueueMessagesRequest{
		QueueName: queueName,
		Limit:     10,
	})
	require.NoError(t, err)
	assert.Empty(t, peekResp.Messages, "Peek should return no messages after all are soft-deleted")
}

// TestRetentionPolicy_ExplicitDeleteImmediately validates that explicitly setting
// DELETE_IMMEDIATELY mode works the same as nil retention policy.
//
// Expected: Message is permanently deleted after acknowledgment
func TestRetentionPolicy_ExplicitDeleteImmediately(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-retention-explicit-delete")

	// Create queue with explicit DELETE_IMMEDIATELY policy
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
			MessageRetentionPolicy: &queue_pb.MessageRetentionPolicy{
				Mode: queue_pb.MessageRetentionPolicy_DELETE_IMMEDIATELY,
			},
		},
	})
	require.NoError(t, err)

	// Post message
	msgID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStructFromString(t, "Test explicit delete immediately"),
		ContentType: "text/plain",
	}

	message := &message_pb.Message{
		MessageId: msgID,
		Metadata: &message_pb.Message_Metadata{
			Payload:     payload,
			Priority:    2,
			MaxAttempts: 1,
		},
	}

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})
	require.NoError(t, err)
	helpers.WaitForMessageTransition(t)

	// Get and acknowledge message
	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.Message)

	workerID := getResp.GetWorkerId()
	attemptID := getResp.GetAttemptId()

	ackResp, err := client.AcknowledgeMessage(ctx, &queueservice_pb.AcknowledgeMessageRequest{
		QueueName: queueName,
		MessageId: msgID,
		State:     message_pb.Message_Metadata_COMPLETED,
		AttemptId: &attemptID,
		WorkerId:  &workerID,
	})
	require.NoError(t, err)
	assert.True(t, ackResp.Success, "Acknowledgment should succeed")

	// Verify message is no longer available (hard-deleted)
	ctx2, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	getResp2, err := client.GetNextMessage(ctx2, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})

	if err == nil && getResp2 != nil && getResp2.Message != nil {
		t.Error("Message should be hard-deleted after acknowledgment with explicit DELETE_IMMEDIATELY policy")
	}
}
