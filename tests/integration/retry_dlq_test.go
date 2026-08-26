package integration

// Package integration provides retry system and DLQ tests for ChronoQueue.
//
// These tests validate:
// - Immediate message retry
// - Maximum retry attempts
// - Dead Letter Queue (DLQ) operations
// - Message requeue from DLQ
// - DLQ statistics and monitoring
//
// Test Scenarios: TC-R-001 through TC-R-006, TC-D-001 through TC-D-008
//
// Run with: go test -v ./tests/integration/ -run Test(Retry|DLQ)

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

func waitForMessage(t *testing.T, ctx context.Context, client queueservice_pb.QueueServiceClient, queueName string) *queueservice_pb.GetNextMessageResponse {
	t.Helper()
	pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for pollCtx.Err() == nil {
		response, err := client.GetNextMessage(pollCtx, &queueservice_pb.GetNextMessageRequest{QueueName: queueName, LeaseDuration: durationpb.New(5 * time.Second)})
		if err != nil {
			if pollCtx.Err() != nil {
				break
			}
			require.NoError(t, err)
		}
		if response.GetMessage() != nil {
			return response
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-pollCtx.Done():
		}
	}
	require.FailNow(t, "message did not become claimable", "queue=%s", queueName)
	return nil
}

func failNextMessage(t *testing.T, ctx context.Context, client queueservice_pb.QueueServiceClient, queueName, messageID string) {
	t.Helper()
	claimed := waitForMessage(t, ctx, client, queueName)
	require.Equal(t, messageID, claimed.GetMessage().GetMessageId())
	_, err := client.AcknowledgeMessage(ctx, &queueservice_pb.AcknowledgeMessageRequest{
		QueueName: queueName,
		MessageId: messageID,
		State:     message_pb.Message_Metadata_ERRORED,
		WorkerId:  claimed.WorkerId,
		AttemptId: claimed.AttemptId,
	})
	require.NoError(t, err)
}

func postAndFailMessage(t *testing.T, ctx context.Context, client queueservice_pb.QueueServiceClient, queueName string) string {
	t.Helper()
	messageID := helpers.GenerateUniqueMessageID(t)
	_, err := client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message: &message_pb.Message{MessageId: messageID, Metadata: &message_pb.Message_Metadata{
			Payload:     &common_pb.Payload{Data: createStruct(t, map[string]interface{}{"test": "dlq_operation"}), ContentType: "application/json"},
			MaxAttempts: 1,
		}},
	})
	require.NoError(t, err)
	failNextMessage(t, ctx, client, queueName, messageID)
	return messageID
}

// TestRetrySystem_ImmediateRetry validates that a NACK makes a retriable message immediately claimable.
//
// Test Scenario: TC-R-002 from TESTING_GUIDE.md
// Data: Message that fails multiple times
// Expected: The next attempt is available without a configured backoff delay.
func TestRetrySystem_ImmediateRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping retry system test in short mode")
	}

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-immediate-retry")

	// Create queue with short retry settings for testing
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:               queue_pb.QueueType_SIMPLE,
			DefaultMaxAttempts: 5,
			LeaseDuration:      durationpb.New(5 * time.Second),
			AutoCreateDlq:      true,
		},
	})
	require.NoError(t, err)

	// Post message
	msgID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStruct(t, map[string]interface{}{"test": "immediate_retry"}),
		ContentType: "application/json",
	}

	message := &message_pb.Message{
		MessageId: msgID,
		Metadata: &message_pb.Message_Metadata{
			Payload:     payload,
			Priority:    2,
			MaxAttempts: 3, // Server requires >= 1
		},
	}

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})
	require.NoError(t, err)

	// Wait for background worker to process message state transition (INVISIBLE -> PENDING)
	helpers.WaitForMessageTransition(t)

	first, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{QueueName: queueName})
	require.NoError(t, err)
	require.NotNil(t, first.GetMessage())
	firstWorkerID := first.GetWorkerId()
	firstAttemptID := first.GetAttemptId()

	_, err = client.AcknowledgeMessage(ctx, &queueservice_pb.AcknowledgeMessageRequest{
		QueueName: queueName,
		MessageId: msgID,
		State:     message_pb.Message_Metadata_ERRORED,
		WorkerId:  &firstWorkerID,
		AttemptId: &firstAttemptID,
	})
	require.NoError(t, err)

	retried, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{QueueName: queueName})
	require.NoError(t, err)
	require.NotNil(t, retried.GetMessage())
	assert.Equal(t, msgID, retried.Message.GetMessageId())
	assert.NotEqual(t, first.GetAttemptId(), retried.GetAttemptId())
	assert.Equal(t, first.Message.Metadata.GetAttemptsLeft()-1, retried.Message.Metadata.GetAttemptsLeft())
}

// TestRetrySystem_MaxRetriesReached validates DLQ after max retries
//
// Test Scenario: TC-R-003 from TESTING_GUIDE.md
// Data: Message fails 3 times (max_attempts = 3)
// Expected: Message moved to DLQ after 3rd failure
func TestRetrySystem_MaxRetriesReached(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-max-retries")
	dlqName := queueName + "_dlq"

	// Create queue with max 3 attempts
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:               queue_pb.QueueType_SIMPLE,
			DefaultMaxAttempts: 3,
			LeaseDuration:      durationpb.New(3 * time.Second),
			AutoCreateDlq:      true,
		},
	})
	require.NoError(t, err)

	// Post message
	msgID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStruct(t, map[string]interface{}{"test": "max_retries"}),
		ContentType: "application/json",
	}

	message := &message_pb.Message{
		MessageId: msgID,
		Metadata: &message_pb.Message_Metadata{
			Payload:     payload,
			Priority:    2,
			MaxAttempts: 3,
		},
	}

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})
	require.NoError(t, err)

	// Wait for background worker to process message state transition (INVISIBLE -> PENDING)
	helpers.WaitForMessageTransition(t)

	// Act - Fail message 3 times
	for attempt := 0; attempt < 3; attempt++ {
		failNextMessage(t, ctx, client, queueName, msgID)
	}

	// Assert - Check DLQ for the failed message
	dlqResp, err := client.GetDLQMessages(ctx, &queueservice_pb.GetDLQMessagesRequest{
		DlqName: dlqName,
		Limit:   10,
	})

	require.NoError(t, err)
	require.Len(t, dlqResp.Messages, 1)
	require.Equal(t, msgID, dlqResp.Messages[0].GetMessageId())
}

// TestDLQ_AutomaticCreation validates automatic DLQ creation
//
// Test Scenario: TC-D-001 from TESTING_GUIDE.md
// Data: Queue with auto_create_dlq=true
// Expected: DLQ created automatically with name {queue}_dlq
func TestDLQ_AutomaticCreation(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-auto-dlq")
	expectedDLQName := queueName + "_dlq"

	// Act - Create queue with auto_create_dlq=true
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			AutoCreateDlq: true,
		},
	})
	require.NoError(t, err)

	// Assert - Verify DLQ exists by checking queue state directly
	// (more resilient than ListQueues which can see other tests' queues)
	mainQueueState, err := client.GetQueueState(ctx, &queueservice_pb.GetQueueStateRequest{
		QueueName: queueName,
	})
	require.NoError(t, err, "Main queue should exist")
	assert.NotNil(t, mainQueueState, "Main queue state should be returned")

	dlqState, err := client.GetQueueState(ctx, &queueservice_pb.GetQueueStateRequest{
		QueueName: expectedDLQName,
	})
	require.NoError(t, err, "DLQ should be created automatically")
	assert.NotNil(t, dlqState, "DLQ state should be returned")
}

// TestDLQ_RequeueMessage validates requeuing messages from DLQ
//
// Test Scenario: TC-D-004 from TESTING_GUIDE.md
// Data: Message in DLQ
// Expected: Message requeued to original queue, retry count reset
func TestDLQ_RequeueMessage(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-dlq-requeue")
	dlqName := queueName + "_dlq"

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:               queue_pb.QueueType_SIMPLE,
			DefaultMaxAttempts: 1, // Fail immediately to DLQ
			LeaseDuration:      durationpb.New(2 * time.Second),
			AutoCreateDlq:      true,
		},
	})
	require.NoError(t, err)

	// Post message that will go to DLQ
	msgID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStruct(t, map[string]interface{}{"test": "requeue"}),
		ContentType: "application/json",
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

	failNextMessage(t, ctx, client, queueName, msgID)

	// Get message from DLQ
	dlqResp, err := client.GetDLQMessages(ctx, &queueservice_pb.GetDLQMessagesRequest{
		DlqName: dlqName,
		Limit:   10,
	})

	require.NoError(t, err)
	require.Len(t, dlqResp.Messages, 1)

	dlqMessage := dlqResp.Messages[0]
	t.Logf("Found message in DLQ: %s", dlqMessage.MessageId)

	// Act - Requeue message from DLQ
	requeueResp, err := client.RequeueFromDLQ(ctx, &queueservice_pb.RequeueFromDLQRequest{
		DlqName:     dlqName,
		MessageId:   dlqMessage.MessageId,
		TargetQueue: queueName,
	})

	require.NoError(t, err)
	require.True(t, requeueResp.GetSuccess())
	getResp2 := waitForMessage(t, ctx, client, queueName)
	require.Equal(t, msgID, getResp2.GetMessage().GetMessageId())
	require.Equal(t, int32(1), getResp2.GetMessage().GetMetadata().GetAttemptsLeft())
}

// TestDLQ_DeleteMessage validates permanent message deletion from DLQ
//
// Test Scenario: TC-D-005 from TESTING_GUIDE.md
// Data: Message in DLQ
// Expected: Message permanently deleted, not recoverable
func TestDLQ_DeleteMessage(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-dlq-delete")
	dlqName := queueName + "_dlq"

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:               queue_pb.QueueType_SIMPLE,
			DefaultMaxAttempts: 1,
			LeaseDuration:      durationpb.New(2 * time.Second),
			AutoCreateDlq:      true,
		},
	})
	require.NoError(t, err)

	messageID := postAndFailMessage(t, ctx, client, queueName)

	// Act - Attempt to delete from DLQ
	deleteResp, err := client.DeleteFromDLQ(ctx, &queueservice_pb.DeleteFromDLQRequest{
		DlqName:   dlqName,
		MessageId: messageID,
	})
	require.NoError(t, err)
	require.True(t, deleteResp.GetSuccess())
	messages, err := client.GetDLQMessages(ctx, &queueservice_pb.GetDLQMessagesRequest{DlqName: dlqName, Limit: 10})
	require.NoError(t, err)
	require.Empty(t, messages.GetMessages())
}

// TestDLQ_PurgeAll validates bulk DLQ purge operation
//
// Test Scenario: TC-D-006 from TESTING_GUIDE.md
// Data: DLQ with multiple messages
// Expected: All messages deleted
func TestDLQ_PurgeAll(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-dlq-purge")
	dlqName := queueName + "_dlq"

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			AutoCreateDlq: true,
		},
	})
	require.NoError(t, err)
	postAndFailMessage(t, ctx, client, queueName)
	postAndFailMessage(t, ctx, client, queueName)

	// Act - Purge DLQ
	purgeResp, err := client.PurgeDLQ(ctx, &queueservice_pb.PurgeDLQRequest{
		DlqName: dlqName,
	})

	// Assert
	require.NoError(t, err, "Purge DLQ should succeed")
	require.True(t, purgeResp.GetSuccess())
	messages, err := client.GetDLQMessages(ctx, &queueservice_pb.GetDLQMessagesRequest{DlqName: dlqName, Limit: 10})
	require.NoError(t, err)
	require.Empty(t, messages.GetMessages())
}

// TestDLQ_GetStatistics validates DLQ statistics retrieval
//
// Test Scenario: TC-D-007 from TESTING_GUIDE.md
// Data: DLQ with known state
// Expected: Accurate statistics returned
func TestDLQ_GetStatistics(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-dlq-stats")
	dlqName := queueName + "_dlq"

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			AutoCreateDlq: true,
		},
	})
	require.NoError(t, err)
	postAndFailMessage(t, ctx, client, queueName)

	// Act - Get DLQ statistics
	statsResp, err := client.GetDLQStats(ctx, &queueservice_pb.GetDLQStatsRequest{
		DlqName: dlqName,
	})

	require.NoError(t, err)
	require.Equal(t, dlqName, statsResp.GetName())
	require.EqualValues(t, 1, statsResp.GetMessageCount())
	require.Positive(t, statsResp.GetCreatedAt())
	require.Positive(t, statsResp.GetUpdatedAt())
}

// Helper functions

// func createStruct(t *testing.T, data map[string]interface{}) *structpb.Struct {
// 	t.Helper()
// 	s, err := structpb.NewStruct(data)
// 	require.NoError(t, err)
// 	return s
// }

func extractQueueNames(queues []*queue_pb.Queue) []string {
	names := make([]string, len(queues))
	for i, q := range queues {
		names[i] = q.Name
	}
	return names
}
