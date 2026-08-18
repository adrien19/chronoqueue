package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservicepb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/tests/helpers"
)

func TestExclusiveQueue_GRPCAndHTTP(t *testing.T) {
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { require.NoError(t, conn.Close()) }()
	client := queueservicepb.NewQueueServiceClient(conn)
	key := "orders"

	_, err := client.CreateQueue(ctx, &queueservicepb.CreateQueueRequest{
		Name: helpers.GenerateUniqueQueueName(t, "exclusive-invalid"),
		Metadata: &queuepb.QueueMetadata{
			Type: queuepb.QueueType_EXCLUSIVE,
		},
	})
	require.Error(t, err)

	for _, transport := range []string{"grpc", "http"} {
		t.Run(transport, func(t *testing.T) {
			queueName := helpers.GenerateUniqueQueueName(t, "exclusive-"+transport)
			_, err := client.CreateQueue(ctx, &queueservicepb.CreateQueueRequest{
				Name: queueName,
				Metadata: &queuepb.QueueMetadata{
					Type: queuepb.QueueType_EXCLUSIVE, ExclusivityKey: key,
				},
			})
			require.NoError(t, err)
			for index := range 2 {
				postExclusiveTestMessage(t, ctx, client, queueName, fmt.Sprintf("%s-%d-%s", transport, index, helpers.GenerateUniqueMessageID(t)))
			}
			helpers.WaitForMessageTransition(t)

			if transport == "grpc" {
				_, err = client.GetNextMessage(ctx, &queueservicepb.GetNextMessageRequest{QueueName: queueName})
				require.Error(t, err)
				_, err = client.GetNextMessage(ctx, &queueservicepb.GetNextMessageRequest{QueueName: queueName, ExclusivityKey: "wrong"})
				require.Error(t, err)
				claim, claimErr := client.GetNextMessage(ctx, &queueservicepb.GetNextMessageRequest{QueueName: queueName, ExclusivityKey: key})
				require.NoError(t, claimErr)
				require.NotNil(t, claim.GetMessage())
				blocked, claimErr := client.GetNextMessage(ctx, &queueservicepb.GetNextMessageRequest{QueueName: queueName, ExclusivityKey: key})
				require.NoError(t, claimErr)
				require.Nil(t, blocked.GetMessage())
				ackExclusiveTestMessage(t, ctx, client, queueName, claim)
				next, claimErr := client.GetNextMessage(ctx, &queueservicepb.GetNextMessageRequest{QueueName: queueName, ExclusivityKey: key})
				require.NoError(t, claimErr)
				require.NotNil(t, next.GetMessage())
				return
			}

			path := fmt.Sprintf("%s/v1/queues/%s/messages:getNext", env.HTTPAddr, queueName)
			require.NotEqual(t, http.StatusOK, exclusiveHTTPClaim(t, ctx, path, `{}`, nil))
			require.NotEqual(t, http.StatusOK, exclusiveHTTPClaim(t, ctx, path, `{"exclusivityKey":"wrong"}`, nil))
			claim := &queueservicepb.GetNextMessageResponse{}
			require.Equal(t, http.StatusOK, exclusiveHTTPClaim(t, ctx, path, `{"exclusivityKey":"orders","workerId":"http-worker"}`, claim))
			require.NotNil(t, claim.GetMessage())
			blocked := &queueservicepb.GetNextMessageResponse{}
			require.Equal(t, http.StatusOK, exclusiveHTTPClaim(t, ctx, path, `{"exclusivityKey":"orders","workerId":"other-worker"}`, blocked))
			require.Nil(t, blocked.GetMessage())
			ackExclusiveTestMessage(t, ctx, client, queueName, claim)
			next := &queueservicepb.GetNextMessageResponse{}
			require.Equal(t, http.StatusOK, exclusiveHTTPClaim(t, ctx, path, `{"exclusivityKey":"orders","workerId":"other-worker"}`, next))
			require.NotNil(t, next.GetMessage())
		})
	}
}

func postExclusiveTestMessage(t *testing.T, ctx context.Context, client queueservicepb.QueueServiceClient, queueName, messageID string) {
	t.Helper()
	payload, err := structpb.NewStruct(map[string]any{"message": messageID})
	require.NoError(t, err)
	_, err = client.PostMessage(ctx, &queueservicepb.PostMessageRequest{
		QueueName: queueName,
		Message: &messagepb.Message{MessageId: messageID, Metadata: &messagepb.Message_Metadata{
			Payload: &commonpb.Payload{Data: payload, ContentType: "application/json"}, MaxAttempts: 2,
		}},
	})
	require.NoError(t, err)
}

func ackExclusiveTestMessage(t *testing.T, ctx context.Context, client queueservicepb.QueueServiceClient, queueName string, claim *queueservicepb.GetNextMessageResponse) {
	t.Helper()
	_, err := client.AcknowledgeMessage(ctx, &queueservicepb.AcknowledgeMessageRequest{
		QueueName: queueName, MessageId: claim.GetMessage().GetMessageId(), State: messagepb.Message_Metadata_COMPLETED,
		AttemptId: claim.AttemptId, WorkerId: claim.WorkerId,
	})
	require.NoError(t, err)
}

func exclusiveHTTPClaim(t *testing.T, ctx context.Context, url, body string, response *queueservicepb.GetNextMessageResponse) int {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("api-key", helpers.TestAPIKey)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	responseBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	if response != nil && resp.StatusCode == http.StatusOK {
		require.NoError(t, protojson.Unmarshal(responseBody, response))
	}
	return resp.StatusCode
}
