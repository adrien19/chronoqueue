package integration

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"sync"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservicepb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/tests/helpers"
)

func TestAttemptOwnership_GRPCAndHTTP(t *testing.T) {
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { require.NoError(t, conn.Close()) }()
	client := queueservicepb.NewQueueServiceClient(conn)
	queueName := helpers.GenerateUniqueQueueName(t, "ownership")
	otherQueue := helpers.GenerateUniqueQueueName(t, "ownership-other")
	for _, name := range []string{queueName, otherQueue} {
		_, err := client.CreateQueue(ctx, &queueservicepb.CreateQueueRequest{Name: name, Metadata: &queuepb.QueueMetadata{Type: queuepb.QueueType_SIMPLE}})
		require.NoError(t, err)
	}

	t.Run("grpc rejects stale and wrong-queue owners and permits one terminal transition", func(t *testing.T) {
		claim := postAndClaimOwnedMessage(t, ctx, client, queueName, "grpc-owner")
		attemptID := claim.GetAttemptId()
		workerID := claim.GetWorkerId()
		staleWorker := "stale-worker"
		_, err := client.AcknowledgeMessage(ctx, &queueservicepb.AcknowledgeMessageRequest{
			QueueName: queueName, MessageId: claim.GetMessage().GetMessageId(), State: messagepb.Message_Metadata_COMPLETED,
			AttemptId: &attemptID, WorkerId: &staleWorker,
		})
		require.Error(t, err)
		_, err = client.AcknowledgeMessage(ctx, &queueservicepb.AcknowledgeMessageRequest{
			QueueName: otherQueue, MessageId: claim.GetMessage().GetMessageId(), State: messagepb.Message_Metadata_COMPLETED,
			AttemptId: &attemptID, WorkerId: &workerID,
		})
		require.Error(t, err)

		start := make(chan struct{})
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, ackErr := client.AcknowledgeMessage(ctx, &queueservicepb.AcknowledgeMessageRequest{
					QueueName: queueName, MessageId: claim.GetMessage().GetMessageId(), State: messagepb.Message_Metadata_COMPLETED,
					AttemptId: &attemptID, WorkerId: &workerID,
				})
				errs <- ackErr
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		var successes int
		for err := range errs {
			if err == nil {
				successes++
			}
		}
		require.Equal(t, 1, successes)
		_, err = client.AcknowledgeMessage(ctx, &queueservicepb.AcknowledgeMessageRequest{
			QueueName: queueName, MessageId: claim.GetMessage().GetMessageId(), State: messagepb.Message_Metadata_COMPLETED,
			AttemptId: &attemptID, WorkerId: &workerID,
		})
		require.Error(t, err)
	})

	t.Run("grpc rejects an expired lease", func(t *testing.T) {
		claim := postAndClaimOwnedMessage(t, ctx, client, queueName, "grpc-expired")
		db, err := sql.Open("postgres", env.PostgresConnStr+"sslmode=disable")
		require.NoError(t, err)
		defer func() { require.NoError(t, db.Close()) }()
		_, err = db.ExecContext(ctx, `UPDATE cq_messages SET lease_expiry = 0 WHERE message_id = $1`, claim.GetMessage().GetMessageId())
		require.NoError(t, err)
		_, err = client.AcknowledgeMessage(ctx, &queueservicepb.AcknowledgeMessageRequest{
			QueueName: queueName, MessageId: claim.GetMessage().GetMessageId(), State: messagepb.Message_Metadata_COMPLETED,
			AttemptId: claim.AttemptId, WorkerId: claim.WorkerId,
		})
		require.Error(t, err)
	})

	t.Run("http requires ownership and rejects duplicate acknowledgment", func(t *testing.T) {
		claim := postAndClaimOwnedMessage(t, ctx, client, queueName, "http-owner")
		path := fmt.Sprintf("%s/v1/queues/%s/messages/%s:acknowledge", env.HTTPAddr, queueName, claim.GetMessage().GetMessageId())
		missing := doOwnershipHTTPRequest(t, ctx, path, `{"state":"COMPLETED"}`)
		require.NotEqual(t, http.StatusOK, missing)
		staleWorkerBody := fmt.Sprintf(`{"state":"COMPLETED","attemptId":%q,"workerId":"stale-worker"}`, claim.GetAttemptId())
		require.NotEqual(t, http.StatusOK, doOwnershipHTTPRequest(t, ctx, path, staleWorkerBody))
		wrongQueuePath := fmt.Sprintf("%s/v1/queues/%s/messages/%s:acknowledge", env.HTTPAddr, otherQueue, claim.GetMessage().GetMessageId())
		wrongQueueBody := fmt.Sprintf(`{"state":"COMPLETED","attemptId":%q,"workerId":%q}`, claim.GetAttemptId(), claim.GetWorkerId())
		require.NotEqual(t, http.StatusOK, doOwnershipHTTPRequest(t, ctx, wrongQueuePath, wrongQueueBody))
		body := fmt.Sprintf(`{"state":"COMPLETED","attemptId":%q,"workerId":%q}`, claim.GetAttemptId(), claim.GetWorkerId())
		require.Equal(t, http.StatusOK, doOwnershipHTTPRequest(t, ctx, path, body))
		require.NotEqual(t, http.StatusOK, doOwnershipHTTPRequest(t, ctx, path, body))
	})

	t.Run("http rejects an expired lease", func(t *testing.T) {
		claim := postAndClaimOwnedMessage(t, ctx, client, queueName, "http-expired")
		db, err := sql.Open("postgres", env.PostgresConnStr+"sslmode=disable")
		require.NoError(t, err)
		defer func() { require.NoError(t, db.Close()) }()
		_, err = db.ExecContext(ctx, `UPDATE cq_messages SET lease_expiry = 0 WHERE message_id = $1`, claim.GetMessage().GetMessageId())
		require.NoError(t, err)
		path := fmt.Sprintf("%s/v1/queues/%s/messages/%s:acknowledge", env.HTTPAddr, queueName, claim.GetMessage().GetMessageId())
		body := fmt.Sprintf(`{"state":"COMPLETED","attemptId":%q,"workerId":%q}`, claim.GetAttemptId(), claim.GetWorkerId())
		require.NotEqual(t, http.StatusOK, doOwnershipHTTPRequest(t, ctx, path, body))
	})

	t.Run("http permits one concurrent terminal transition", func(t *testing.T) {
		claim := postAndClaimOwnedMessage(t, ctx, client, queueName, "http-concurrent")
		path := fmt.Sprintf("%s/v1/queues/%s/messages/%s:acknowledge", env.HTTPAddr, queueName, claim.GetMessage().GetMessageId())
		body := fmt.Sprintf(`{"state":"COMPLETED","attemptId":%q,"workerId":%q}`, claim.GetAttemptId(), claim.GetWorkerId())
		start := make(chan struct{})
		statuses := make(chan int, 2)
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				statusCode, requestErr := ownershipHTTPStatus(ctx, path, body)
				statuses <- statusCode
				errs <- requestErr
			}()
		}
		close(start)
		wg.Wait()
		close(statuses)
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		var successes int
		for statusCode := range statuses {
			if statusCode == http.StatusOK {
				successes++
			}
		}
		require.Equal(t, 1, successes)
	})
}

func postAndClaimOwnedMessage(t *testing.T, ctx context.Context, client queueservicepb.QueueServiceClient, queueName, prefix string) *queueservicepb.GetNextMessageResponse {
	t.Helper()
	messageID := prefix + "-" + helpers.GenerateUniqueMessageID(t)
	payload, err := structpb.NewStruct(map[string]any{"test": prefix})
	require.NoError(t, err)
	_, err = client.PostMessage(ctx, &queueservicepb.PostMessageRequest{
		QueueName: queueName,
		Message: &messagepb.Message{MessageId: messageID, Metadata: &messagepb.Message_Metadata{
			Payload: &commonpb.Payload{Data: payload, ContentType: "application/json"}, MaxAttempts: 1,
		}},
	})
	require.NoError(t, err)
	helpers.WaitForMessageTransition(t)
	workerID := "worker-" + prefix
	claim, err := client.GetNextMessage(ctx, &queueservicepb.GetNextMessageRequest{QueueName: queueName, WorkerId: &workerID})
	require.NoError(t, err)
	require.NotNil(t, claim.GetMessage())
	require.Equal(t, messageID, claim.GetMessage().GetMessageId())
	require.NotEmpty(t, claim.GetAttemptId())
	require.NotEmpty(t, claim.GetWorkerId())
	return claim
}

func doOwnershipHTTPRequest(t *testing.T, ctx context.Context, url, body string) int {
	t.Helper()
	statusCode, err := ownershipHTTPStatus(ctx, url, body)
	require.NoError(t, err)
	return statusCode
}

func ownershipHTTPStatus(ctx context.Context, url, body string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBufferString(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("api-key", helpers.TestAPIKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	if err := resp.Body.Close(); err != nil {
		return 0, err
	}
	return resp.StatusCode, nil
}
