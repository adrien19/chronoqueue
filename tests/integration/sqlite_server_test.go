//go:build integration && sqlite && cgo
// +build integration,sqlite,cgo

package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservicepb "github.com/adrien19/chronoqueue/api/queueservice/v1"
)

func TestSQLiteServerIntegration(t *testing.T) {
	h := startSQLiteServer(t, "test.db")
	defer h.shutdown(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(h.grpcTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := queueservicepb.NewQueueServiceClient(conn)
	queueName := "test-queue"
	messageID := "msg-1"

	t.Run("CreateQueue", func(t *testing.T) {
		createResp, err := client.CreateQueue(ctx, &queueservicepb.CreateQueueRequest{
			Name: queueName,
			Metadata: &queuepb.QueueMetadata{
				Type: queuepb.QueueType_SIMPLE,
			},
		})
		require.NoError(t, err)
		assert.True(t, createResp.GetSuccess())
	})

	t.Run("PostMessage", func(t *testing.T) {
		payloadData, err := structpb.NewStruct(map[string]interface{}{
			"test": "data",
		})
		require.NoError(t, err)

		postResp, err := client.PostMessage(ctx, &queueservicepb.PostMessageRequest{
			QueueName: queueName,
			Message: &messagepb.Message{
				MessageId: messageID,
				Metadata: &messagepb.Message_Metadata{
					Payload: &commonpb.Payload{
						Data:        payloadData,
						ContentType: "application/json",
					},
					Priority:    1,
					MaxAttempts: 2,
				},
			},
		})
		require.NoError(t, err)
		assert.True(t, postResp.GetSuccess())
	})

	var attemptID string
	t.Run("GetMessage", func(t *testing.T) {
		getResp, err := client.GetNextMessage(ctx, &queueservicepb.GetNextMessageRequest{
			QueueName:     queueName,
			LeaseDuration: durationpb.New(30 * time.Second),
		})
		require.NoError(t, err)
		require.NotNil(t, getResp.GetMessage())
		assert.Equal(t, messageID, getResp.GetMessage().GetMessageId())
		assert.Equal(t, messagepb.Message_Metadata_RUNNING, getResp.GetMessage().GetMetadata().GetState())
		assert.NotNil(t, getResp.GetMessage().GetMetadata().GetPayload())
		attemptID = getResp.GetAttemptId()
	})

	t.Run("ListQueues", func(t *testing.T) {
		listResp, err := client.ListQueues(ctx, &queueservicepb.ListQueuesRequest{})
		require.NoError(t, err)
		require.NotNil(t, listResp)

		found := false
		for _, q := range listResp.GetQueues() {
			if q.GetName() == queueName {
				found = true
				break
			}
		}
		assert.True(t, found, "created queue should appear in list")
	})

	t.Run("GetQueueState", func(t *testing.T) {
		stateResp, err := client.GetQueueState(ctx, &queueservicepb.GetQueueStateRequest{
			QueueName: queueName,
		})
		require.NoError(t, err)
		require.NotNil(t, stateResp)
		assert.Equal(t, int32(0), stateResp.GetStateCounts()["PENDING"])
		assert.Equal(t, int32(1), stateResp.GetStateCounts()["RUNNING"])
	})

	t.Run("AcknowledgeMessage", func(t *testing.T) {
		ackReq := &queueservicepb.AcknowledgeMessageRequest{
			QueueName: queueName,
			MessageId: messageID,
			State:     messagepb.Message_Metadata_COMPLETED,
		}
		if attemptID != "" {
			ackReq.AttemptId = &attemptID
		}

		ackResp, err := client.AcknowledgeMessage(ctx, ackReq)
		require.NoError(t, err)
		assert.True(t, ackResp.GetSuccess())

		stateResp, err := client.GetQueueState(ctx, &queueservicepb.GetQueueStateRequest{QueueName: queueName})
		require.NoError(t, err)
		assert.Equal(t, int32(0), stateResp.GetStateCounts()["RUNNING"])
		assert.Equal(t, int32(0), stateResp.GetStateCounts()["PENDING"])
	})

	_, err = os.Stat(h.dbPath)
	require.NoError(t, err, "SQLite database file should exist")
}
