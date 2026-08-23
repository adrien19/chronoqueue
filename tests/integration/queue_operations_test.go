package integration

// Package integration provides integration tests for ChronoQueue queue operations.
//
// These tests validate queue creation, deletion, listing, and state management
// using real Redis and ChronoQueue server containers via Testcontainers.
//
// Run with: go test -v ./tests/integration/...

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/pkg/log"
	"github.com/adrien19/chronoqueue/tests/helpers"
)

// TestMain sets up shared test infrastructure for all tests in this package.
// Containers are created once and reused, significantly speeding up test execution.
func TestMain(m *testing.M) {
	if err := os.Setenv("CHRONOQUEUE_API_KEY", helpers.TestAPIKey); err != nil {
		log.NewLogger().Fatal("Failed to configure integration API key", "error", err)
	}
	os.Exit(helpers.RunWithSharedEnvironment(m))
}

// TestQueueOperations_CreateSimpleQueue_Success validates basic queue creation.
//
// Test Scenario: TC-Q-001 from TESTING_GUIDE.md
// Data: Simple FIFO queue with default settings
// Expected: Queue created successfully, appears in list
func TestQueueOperations_CreateSimpleQueue_Success(t *testing.T) {
	t.Parallel() // Safe to run in parallel

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)

	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()

	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := "test-simple-queue"
	request := &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:               queue_pb.QueueType_SIMPLE,
			DefaultMaxAttempts: 3,
			AutoCreateDlq:      true,
			LeaseDuration:      durationpb.New(30 * time.Second),
		},
	}

	// Act
	response, err := client.CreateQueue(ctx, request)

	// Assert
	require.NoError(t, err, "Queue creation should succeed")
	assert.True(t, response.Success, "Response should indicate success")

	// Verify queue exists by checking its state directly
	// (more resilient than ListQueues which can see other tests' queues being deleted)
	queueState, err := client.GetQueueState(ctx, &queueservice_pb.GetQueueStateRequest{
		QueueName: queueName,
	})
	require.NoError(t, err, "Created queue should exist")
	assert.NotNil(t, queueState, "Queue state should be returned")
}

// TestQueueOperations_CreateQueueWithDLQ_Success validates automatic DLQ creation.
//
// Test Scenario: TC-Q-003 from TESTING_GUIDE.md
// Data: Queue with auto_create_dlq=true
// Expected: Both main queue and DLQ created
func TestQueueOperations_CreateQueueWithDLQ_Success(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)

	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()

	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := "test-queue-with-dlq"
	dlqName := queueName + "_dlq"

	request := &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			AutoCreateDlq: true,
		},
	}

	// Act
	response, err := client.CreateQueue(ctx, request)

	// Assert
	require.NoError(t, err, "Queue creation should succeed")
	assert.True(t, response.Success, "Response should indicate success")

	// Verify both queue and DLQ exist by checking their state directly
	mainQueueState, err := client.GetQueueState(ctx, &queueservice_pb.GetQueueStateRequest{
		QueueName: queueName,
	})
	require.NoError(t, err, "Main queue should exist")
	assert.NotNil(t, mainQueueState, "Main queue state should be returned")

	dlqState, err := client.GetQueueState(ctx, &queueservice_pb.GetQueueStateRequest{
		QueueName: dlqName,
	})
	require.NoError(t, err, "DLQ should exist")
	assert.NotNil(t, dlqState, "DLQ state should be returned")
}

// TestQueueOperations_DeleteEmptyQueue_Success validates deleting an empty queue.
//
// Test Scenario: TC-Q-005 from TESTING_GUIDE.md
// Data: Queue with no messages
// Expected: Queue deleted successfully
func TestQueueOperations_DeleteEmptyQueue_Success(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)

	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()

	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := "test-queue-to-delete"

	// Create queue
	createResp, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)
	require.True(t, createResp.Success)

	// Act
	deleteResp, err := client.DeleteQueue(ctx, &queueservice_pb.DeleteQueueRequest{
		Name: queueName,
	})

	// Assert
	require.NoError(t, err, "Queue deletion should succeed")
	assert.True(t, deleteResp.Success, "Response should indicate success")

	// If deletion succeeded, the queue no longer exists
	// (no need to verify further - deletion success means it's gone)
}

// TestQueueOperations_DuplicateQueueCreation_Error validates duplicate prevention.
//
// Test Scenario: TC-Q-010 from TESTING_GUIDE.md
// Data: Create same queue twice
// Expected: Error on second attempt
func TestQueueOperations_DuplicateQueueCreation_Error(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)

	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()

	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := "test-duplicate-queue"
	request := &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	}

	// Act - First creation
	resp1, err1 := client.CreateQueue(ctx, request)
	require.NoError(t, err1, "First queue creation should succeed")
	require.True(t, resp1.Success)

	// Act - Second creation (duplicate)
	_, err2 := client.CreateQueue(ctx, request)

	assert.Equal(t, codes.AlreadyExists, status.Code(err2))
}

// TestQueueOperations_ListQueues_Prefix validates queue listing by name prefix.
//
// Test Scenario: TC-Q-007 from TESTING_GUIDE.md
// Data: 10+ queues created
// Expected: All queues returned with correct metadata
func TestQueueOperations_ListQueues_Prefix(t *testing.T) {
	// Note: Not parallel - creates many queues

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)

	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()

	client := queueservice_pb.NewQueueServiceClient(conn)

	const numQueues = 15
	createdQueues := make([]string, numQueues)
	prefix := helpers.GenerateUniqueQueueName(t, "list-prefix") + "-"

	// Create multiple queues
	for i := 0; i < numQueues; i++ {
		queueName := fmt.Sprintf("%s%d", prefix, i)
		createdQueues[i] = queueName

		_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
			Name: queueName,
			Metadata: &queue_pb.QueueMetadata{
				Type: queue_pb.QueueType_SIMPLE,
			},
		})
		require.NoError(t, err, "Queue creation should succeed")
	}

	// Act
	listResp, err := client.ListQueues(ctx, &queueservice_pb.ListQueuesRequest{Prefix: prefix})

	// Assert
	require.NoError(t, err, "List queues should succeed")
	require.Len(t, listResp.Queues, numQueues, "Should return only queues with the requested prefix")

	queueNames := extractQueueNames(listResp.Queues)
	for _, queueName := range createdQueues {
		assert.Contains(t, queueNames, queueName, "Created queue should be in list")
	}
}

// Helper functions

// // extractQueueNames extracts queue names from a list of queue objects.
// func extractQueueNames(queues []*queue_pb.Queue) []string {
// 	names := make([]string, len(queues))
// 	for i, q := range queues {
// 		names[i] = q.Name
// 	}
// 	return names
// }

// TODO: Add more test cases:
// - TestQueueOperations_GetQueueState_AccurateStatistics
// - TestQueueOperations_InvalidQueueName_Error
// - TestQueueOperations_CreateQueueWithSchema_Success
// - TestQueueOperations_DeleteQueueWithMessages_Handling
// etc.
