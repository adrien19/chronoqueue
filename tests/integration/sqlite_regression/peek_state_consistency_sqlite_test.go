//go:build integration && sqlite && cgo
// +build integration,sqlite,cgo

package sqlite_regression

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
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
	"github.com/adrien19/chronoqueue/internal/server"
)

func TestSQLitePeekStateConsistency_LeasedMessageReportedAsRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcPort := freePort(t)
	httpPort := freePort(t)
	grpcAddr := fmt.Sprintf(":%d", grpcPort)
	httpAddr := fmt.Sprintf(":%d", httpPort)
	httpURLAddr := fmt.Sprintf("127.0.0.1:%d", httpPort)

	cfg := server.DefaultConfig()
	cfg.StorageType = "sqlite"
	cfg.SQLiteDBPath = filepath.Join(t.TempDir(), "chronoqueue-test.db")
	cfg.GRPCAddr = grpcAddr
	cfg.HTTPAddr = httpAddr
	cfg.IsDevelopment = true
	cfg.SchedulerIntervalMs = 300
	cfg.ReclaimIntervalMs = 2000

	srv, err := server.New(cfg)
	require.NoError(t, err)

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- srv.Start(ctx)
	}()

	require.NoError(t, waitForHTTPHealth(httpURLAddr, 10*time.Second, serverDone))

	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			t.Errorf("failed to close grpc connection: %v", closeErr)
		}
	}()

	client := queueservicepb.NewQueueServiceClient(conn)

	queueName := fmt.Sprintf("sqlite-peek-state-%d", time.Now().UnixNano())
	messageID := fmt.Sprintf("msg-%d", time.Now().UnixNano())

	_, err = client.CreateQueue(ctx, &queueservicepb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queuepb.QueueMetadata{
			Type: queuepb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	payloadData, err := structpb.NewStruct(map[string]interface{}{
		"kind": "sqlite-regression",
		"id":   messageID,
	})
	require.NoError(t, err)

	_, err = client.PostMessage(ctx, &queueservicepb.PostMessageRequest{
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

	getResp, err := client.GetNextMessage(ctx, &queueservicepb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.GetMessage())
	require.NotEmpty(t, getResp.GetAttemptId())
	assert.Equal(t, messagepb.Message_Metadata_RUNNING, getResp.GetMessage().GetMetadata().GetState())

	stateResp, err := client.GetQueueState(ctx, &queueservicepb.GetQueueStateRequest{QueueName: queueName})
	require.NoError(t, err)
	assert.Equal(t, int32(0), stateResp.GetStateCounts()["PENDING"])
	assert.Equal(t, int32(1), stateResp.GetStateCounts()["RUNNING"])

	peekResp, err := client.PeekQueueMessages(ctx, &queueservicepb.PeekQueueMessagesRequest{
		QueueName: queueName,
		Limit:     10,
	})
	require.NoError(t, err)
	require.Len(t, peekResp.GetMessages(), 1)
	assert.Equal(t, messagepb.Message_Metadata_RUNNING, peekResp.GetMessages()[0].GetMetadata().GetState())
	assert.Equal(t, getResp.GetAttemptId(), peekResp.GetMessages()[0].GetMetadata().GetCurrentAttempt().GetAttemptId())

	// Ensure reclaim loop does not incorrectly move RUNNING to PENDING when heartbeat_expiry is unset.
	time.Sleep(3 * time.Second)

	stateAfterWait, err := client.GetQueueState(ctx, &queueservicepb.GetQueueStateRequest{QueueName: queueName})
	require.NoError(t, err)
	assert.Equal(t, int32(0), stateAfterWait.GetStateCounts()["PENDING"])
	assert.Equal(t, int32(1), stateAfterWait.GetStateCounts()["RUNNING"])

	peekAfterWait, err := client.PeekQueueMessages(ctx, &queueservicepb.PeekQueueMessagesRequest{
		QueueName: queueName,
		Limit:     10,
	})
	require.NoError(t, err)
	require.Len(t, peekAfterWait.GetMessages(), 1)
	assert.Equal(t, messagepb.Message_Metadata_RUNNING, peekAfterWait.GetMessages()[0].GetMetadata().GetState())

	attemptID := getResp.GetAttemptId()
	workerID := getResp.GetWorkerId()
	_, err = client.AcknowledgeMessage(ctx, &queueservicepb.AcknowledgeMessageRequest{
		QueueName: queueName,
		MessageId: messageID,
		State:     messagepb.Message_Metadata_COMPLETED,
		AttemptId: &attemptID,
		WorkerId:  &workerID,
	})
	require.NoError(t, err)

	finalState, err := client.GetQueueState(ctx, &queueservicepb.GetQueueStateRequest{QueueName: queueName})
	require.NoError(t, err)
	assert.Equal(t, int32(0), finalState.GetStateCounts()["RUNNING"])
	assert.Equal(t, int32(0), finalState.GetStateCounts()["PENDING"])

	cancel()
	select {
	case err := <-serverDone:
		if err != nil {
			t.Logf("server shutdown returned: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Log("server shutdown still in progress; continuing test teardown")
	}
}

func freePort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		if closeErr := listener.Close(); closeErr != nil {
			t.Errorf("failed to close listener: %v", closeErr)
		}
	}()

	return listener.Addr().(*net.TCPAddr).Port
}

func waitForHTTPHealth(addr string, timeout time.Duration, serverDone <-chan error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	url := fmt.Sprintf("http://%s/health", addr)
	httpClient := &http.Client{}

	for {
		select {
		case err := <-serverDone:
			if err != nil {
				return fmt.Errorf("server exited before becoming ready: %w", err)
			}
			return fmt.Errorf("server exited before becoming ready")
		default:
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("failed to create health request: %w", err)
		}

		resp, err := httpClient.Do(req)
		if err == nil {
			if closeErr := resp.Body.Close(); closeErr != nil {
				return fmt.Errorf("failed to close health response body: %w", closeErr)
			}
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("HTTP health endpoint not reachable at %s within %s", url, timeout)
		case <-ticker.C:
		}
	}
}
