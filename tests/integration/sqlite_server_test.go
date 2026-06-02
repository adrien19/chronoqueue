//go:build integration && sqlite
// +build integration,sqlite

package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/adrien19/chronoqueue/internal/server"

	queueservicepb "github.com/adrien19/chronoqueue/api/queueservice/v1"
)

func TestSQLiteServerIntegration(t *testing.T) {
	// Create temporary directory for test database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Configure server with SQLite
	config := server.DefaultConfig()
	config.StorageType = "sqlite"
	config.SQLiteDBPath = dbPath
	config.GRPCAddr = ":19000" // Use different port to avoid conflicts
	config.HTTPAddr = ":18080"
	config.IsDevelopment = true

	// Create and start server
	srv, err := server.New(config)
	require.NoError(t, err)

	// Start server in background
	serverDone := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		serverDone <- srv.Start(ctx)
	}()

	// Give server time to start
	time.Sleep(500 * time.Millisecond)

	// Connect to gRPC server
	conn, err := grpc.Dial(
		"localhost:19000",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := queueservicepb.NewQueueServiceClient(conn)

	// Test 1: Create a queue
	t.Run("CreateQueue", func(t *testing.T) {
		createResp, err := client.CreateQueue(ctx, &queueservicepb.CreateQueueRequest{
			Name: "test-queue",
			Type: "simple",
		})
		require.NoError(t, err)
		assert.NotNil(t, createResp)
		assert.Equal(t, "test-queue", createResp.Queue.Name)
	})

	// Test 2: Post a message
	t.Run("PostMessage", func(t *testing.T) {
		postResp, err := client.PostMessage(ctx, &queueservicepb.PostMessageRequest{
			QueueName: "test-queue",
			MessageId: "msg-1",
			Payload:   []byte(`{"test": "data"}`),
			Priority:  5,
		})
		require.NoError(t, err)
		assert.NotNil(t, postResp)
		assert.Equal(t, "msg-1", postResp.MessageId)
	})

	// Test 3: Get a message
	t.Run("GetMessage", func(t *testing.T) {
		getResp, err := client.GetNextMessage(ctx, &queueservicepb.GetNextMessageRequest{
			QueueName: "test-queue",
		})
		require.NoError(t, err)
		assert.NotNil(t, getResp)
		assert.Equal(t, "msg-1", getResp.Message.MessageId)
		assert.Equal(t, []byte(`{"test": "data"}`), getResp.Message.Metadata.Payload)
	})

	// Test 4: List queues
	t.Run("ListQueues", func(t *testing.T) {
		listResp, err := client.ListQueues(ctx, &queueservicepb.ListQueuesRequest{})
		require.NoError(t, err)
		assert.NotNil(t, listResp)
		assert.Len(t, listResp.Queues, 1)
		assert.Equal(t, "test-queue", listResp.Queues[0].Name)
	})

	// Test 5: Get queue state
	t.Run("GetQueueState", func(t *testing.T) {
		stateResp, err := client.GetQueueState(ctx, &queueservicepb.GetQueueStateRequest{
			QueueName: "test-queue",
		})
		require.NoError(t, err)
		assert.NotNil(t, stateResp)
		assert.Equal(t, "test-queue", stateResp.QueueName)
		// Message is in RUNNING state after GetNextMessage
		assert.Equal(t, int64(1), stateResp.RunningCount)
	})

	// Shutdown server
	cancel()
	select {
	case err := <-serverDone:
		if err != nil {
			t.Logf("Server shutdown with: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Server did not shutdown in time")
	}

	// Verify database file was created
	_, err = os.Stat(dbPath)
	require.NoError(t, err, "SQLite database file should exist")
}
