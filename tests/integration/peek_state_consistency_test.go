package integration

import (
	"context"
	"fmt"
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

// TestPeekStateConsistency_LeasedMessageReportedAsRunning verifies end-to-end
// consistency between GetNextMessage, GetQueueState, and PeekQueueMessages.
//
// Regression coverage:
//   - Peek must reflect runtime state (RUNNING) for leased messages.
//   - Reclaim service must not immediately requeue RUNNING messages when
//     heartbeat_expiry is unset/zero.
func TestPeekStateConsistency_LeasedMessageReportedAsRunning(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()

	client := queueservice_pb.NewQueueServiceClient(conn)
	queueName := helpers.GenerateUniqueQueueName(t, "test-peek-state-consistency")
	messageID := helpers.GenerateUniqueMessageID(t)

	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	payload := &common_pb.Payload{
		Data:        createStructFromString(t, fmt.Sprintf("consistency payload %s", messageID)),
		ContentType: "text/plain",
	}

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message: &message_pb.Message{
			MessageId: messageID,
			Metadata: &message_pb.Message_Metadata{
				Payload:     payload,
				Priority:    1,
				MaxAttempts: 2,
			},
		},
	})
	require.NoError(t, err)

	helpers.WaitForMessageTransition(t)

	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.GetMessage(), "expected leased message")
	require.NotNil(t, getResp.GetAttemptId(), "attempt_id should be set for leased messages")

	assert.Equal(t, messageID, getResp.GetMessage().GetMessageId())
	assert.Equal(t, message_pb.Message_Metadata_RUNNING, getResp.GetMessage().GetMetadata().GetState())

	stateResp, err := client.GetQueueState(ctx, &queueservice_pb.GetQueueStateRequest{QueueName: queueName})
	require.NoError(t, err)
	assert.Equal(t, int32(0), stateResp.GetStateCounts()["PENDING"])
	assert.Equal(t, int32(1), stateResp.GetStateCounts()["RUNNING"])

	peekResp, err := client.PeekQueueMessages(ctx, &queueservice_pb.PeekQueueMessagesRequest{
		QueueName: queueName,
		Limit:     10,
	})
	require.NoError(t, err)
	require.Len(t, peekResp.GetMessages(), 1)

	peekMsg := peekResp.GetMessages()[0]
	assert.Equal(t, messageID, peekMsg.GetMessageId())
	assert.Equal(t, message_pb.Message_Metadata_RUNNING, peekMsg.GetMetadata().GetState())
	assert.Equal(t, getResp.GetAttemptId(), peekMsg.GetMetadata().GetCurrentAttempt().GetAttemptId())

	// Wait longer than reclaim interval (2s in shared env) to ensure reclaim does
	// not incorrectly move RUNNING -> PENDING when heartbeat_expiry is unset.
	time.Sleep(3 * time.Second)

	stateAfterWait, err := client.GetQueueState(ctx, &queueservice_pb.GetQueueStateRequest{QueueName: queueName})
	require.NoError(t, err)
	assert.Equal(t, int32(0), stateAfterWait.GetStateCounts()["PENDING"])
	assert.Equal(t, int32(1), stateAfterWait.GetStateCounts()["RUNNING"])

	peekAfterWait, err := client.PeekQueueMessages(ctx, &queueservice_pb.PeekQueueMessagesRequest{
		QueueName: queueName,
		Limit:     10,
	})
	require.NoError(t, err)
	require.Len(t, peekAfterWait.GetMessages(), 1)
	assert.Equal(t, message_pb.Message_Metadata_RUNNING, peekAfterWait.GetMessages()[0].GetMetadata().GetState())

	attemptID := getResp.GetAttemptId()
	workerID := getResp.GetWorkerId()

	_, err = client.AcknowledgeMessage(ctx, &queueservice_pb.AcknowledgeMessageRequest{
		QueueName: queueName,
		MessageId: messageID,
		State:     message_pb.Message_Metadata_COMPLETED,
		AttemptId: &attemptID,
		WorkerId:  &workerID,
	})
	require.NoError(t, err)

	finalState, err := client.GetQueueState(ctx, &queueservice_pb.GetQueueStateRequest{QueueName: queueName})
	require.NoError(t, err)
	assert.Equal(t, int32(0), finalState.GetStateCounts()["RUNNING"])
	assert.Equal(t, int32(0), finalState.GetStateCounts()["PENDING"])
}
