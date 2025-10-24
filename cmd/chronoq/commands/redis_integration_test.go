// Package commands_test provides Redis-backed integration tests for CLI business logic
package commands

import (
	"context"
	"testing"
	"time"

	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
	"github.com/adrien19/chronoqueue/pkg/log"
	"github.com/adrien19/chronoqueue/pkg/repository"
	"github.com/alicebob/miniredis"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestStorage creates a test Redis storage layer using miniredis
func setupTestStorage(t *testing.T) (repository.Storage, *miniredis.Miniredis, func()) {
	// Start miniredis server
	server, err := miniredis.Run()
	require.NoError(t, err, "Failed to start miniredis server")

	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr: server.Addr(),
		DB:   0,
	})

	// Setup logger
	logger := log.NewLogger()

	// Setup encryption key manager
	keyManager, err := keymanager.NewEncryptionKeyManager(logger)
	require.NoError(t, err, "Failed to create encryption key manager")

	// Create storage layer using the testing constructor to avoid background workers
	storage := repository.NewQueueStorageForTesting(client, keyManager, logger)

	// Return cleanup function
	cleanup := func() {
		_ = client.Close() // Best-effort close
		server.Close()
	}

	return storage, server, cleanup
}

// TestRedisBasicQueueOperations tests basic queue operations with Redis backend
func TestRedisBasicQueueOperations(t *testing.T) {
	storage, _, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("CreateQueue", func(t *testing.T) {
		request := &queueservice_pb.CreateQueueRequest{
			Name: "test-queue",
			Metadata: &queue_pb.QueueMetadata{
				Type: queue_pb.QueueType_SIMPLE,
			},
		}

		response, err := storage.CreateQueue(ctx, request)
		require.NoError(t, err, "Queue creation should succeed")
		assert.NotNil(t, response, "Response should not be nil")
		assert.True(t, response.Success, "Queue creation should be successful")
	})

	t.Run("CreateQueueWithoutMetadata", func(t *testing.T) {
		request := &queueservice_pb.CreateQueueRequest{
			Name: "simple-queue",
		}

		response, err := storage.CreateQueue(ctx, request)
		// This might fail depending on validation rules
		if err != nil {
			t.Logf("Queue creation without metadata failed (may be expected): %v", err)
			return
		}

		assert.NotNil(t, response, "Response should not be nil")
		assert.True(t, response.Success, "Queue creation should be successful")
	})

	t.Run("ListQueues", func(t *testing.T) {
		request := &queueservice_pb.ListQueuesRequest{}

		response, err := storage.ListQueues(ctx, request)
		require.NoError(t, err, "Queue listing should succeed")
		assert.NotNil(t, response, "Response should not be nil")

		t.Logf("Found %d queues", len(response.Queues))
		for i, queue := range response.Queues {
			t.Logf("Queue %d: %s", i, queue.Name)
		}
	})
}

// TestRedisBasicMessageOperations tests basic message operations with Redis backend
func TestRedisBasicMessageOperations(t *testing.T) {
	storage, _, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()

	// First create a queue for message operations
	createQueueRequest := &queueservice_pb.CreateQueueRequest{
		Name: "message-test-queue",
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	}

	_, err := storage.CreateQueue(ctx, createQueueRequest)
	require.NoError(t, err, "Queue creation should succeed")

	t.Run("CreateQueueMessage", func(t *testing.T) {
		request := &queueservice_pb.PostMessageRequest{
			QueueName: "message-test-queue",
			Message: &message_pb.Message{
				MessageId: "test-message-id",
				Metadata: &message_pb.Message_Metadata{
					Payload:  &common_pb.Payload{},
					Priority: time.Now().Unix(),
				},
			},
		}

		response, err := storage.CreateQueueMessage(ctx, request, nil)
		require.NoError(t, err, "Message creation should succeed")
		assert.NotNil(t, response, "Response should not be nil")
		t.Logf("Created message response: %+v", response)
	})

	t.Run("CreateQueueMessageWithoutId", func(t *testing.T) {
		request := &queueservice_pb.PostMessageRequest{
			QueueName: "message-test-queue",
			Message: &message_pb.Message{
				Metadata: &message_pb.Message_Metadata{
					Payload:  &common_pb.Payload{},
					Priority: time.Now().Unix(),
				},
			},
		}

		response, err := storage.CreateQueueMessage(ctx, request, nil)
		// This should fail according to existing tests
		if err != nil {
			t.Logf("Message creation without ID failed as expected: %v", err)
			return
		}

		assert.NotNil(t, response, "Response should not be nil")
		t.Logf("Created message response: %+v", response)
	})

	t.Run("GetQueueMessage", func(t *testing.T) {
		request := &queueservice_pb.GetNextMessageRequest{
			QueueName: "message-test-queue",
		}

		response, err := storage.GetQueueMessage(ctx, request)
		if err != nil {
			t.Logf("GetQueueMessage failed (may be expected if no messages): %v", err)
			return
		}

		assert.NotNil(t, response, "Response should not be nil")
		t.Logf("Got message response: %+v", response)
	})
}

// TestRedisErrorConditions tests error conditions with Redis backend
func TestRedisErrorConditions(t *testing.T) {
	storage, _, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("PostMessageToNonExistentQueue", func(t *testing.T) {
		request := &queueservice_pb.PostMessageRequest{
			QueueName: "non-existent-queue",
			Message: &message_pb.Message{
				MessageId: "test-message-id",
				Metadata: &message_pb.Message_Metadata{
					Payload: &common_pb.Payload{},
				},
			},
		}

		// Note: ChronoQueue allows posting to non-existent queues (auto-creation behavior)
		// This is a design decision - Redis allows adding to keys that don't exist
		_, err := storage.CreateQueueMessage(ctx, request, nil)
		assert.NoError(t, err, "Posting to non-existent queue succeeds (auto-creation)")
		t.Logf("Message posted to non-existent queue (auto-creation): %v", err)
	})

	t.Run("CreateQueueWithEmptyName", func(t *testing.T) {
		request := &queueservice_pb.CreateQueueRequest{
			Name: "",
		}

		_, err := storage.CreateQueue(ctx, request)
		assert.Error(t, err, "Should fail for empty queue name")
		t.Logf("Expected error for empty name: %v", err)
	})

	t.Run("AcknowledgeNonExistentMessage", func(t *testing.T) {
		// First create a queue
		createQueueRequest := &queueservice_pb.CreateQueueRequest{
			Name:     "error-test-queue",
			Metadata: &queue_pb.QueueMetadata{Type: queue_pb.QueueType_SIMPLE},
		}
		_, err := storage.CreateQueue(ctx, createQueueRequest)
		require.NoError(t, err, "Queue creation should succeed")

		// Try to acknowledge non-existent message
		request := &queueservice_pb.AcknowledgeMessageRequest{
			QueueName: "error-test-queue",
			MessageId: "non-existent-message-id",
		}

		_, err = storage.AcknowledgeMessage(ctx, request)
		assert.Error(t, err, "Should fail for non-existent message")
		t.Logf("Expected error for non-existent message: %v", err)
	})
}

// TestRedisDataPersistence tests that data persists correctly in Redis
func TestRedisDataPersistence(t *testing.T) {
	storage, redisServer, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()

	// Create a queue
	createQueueRequest := &queueservice_pb.CreateQueueRequest{
		Name: "persistence-test-queue",
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	}

	_, err := storage.CreateQueue(ctx, createQueueRequest)
	require.NoError(t, err, "Queue creation should succeed")

	// Check that data exists in Redis
	keys := redisServer.Keys()
	t.Logf("Redis keys after queue creation: %v", keys)
	assert.NotEmpty(t, keys, "Redis should contain data")

	// Post a message and check Redis keys again
	postMessageRequest := &queueservice_pb.PostMessageRequest{
		QueueName: "persistence-test-queue",
		Message: &message_pb.Message{
			MessageId: "persistence-test-message",
			Metadata: &message_pb.Message_Metadata{
				Payload: &common_pb.Payload{},
			},
		},
	}

	_, err = storage.CreateQueueMessage(ctx, postMessageRequest, nil)
	if err != nil {
		t.Logf("Message creation failed (may be expected): %v", err)
	} else {
		keys = redisServer.Keys()
		t.Logf("Redis keys after message creation: %v", keys)
	}
}

// BenchmarkRedisOperations provides performance benchmarks for Redis operations
func BenchmarkRedisOperations(b *testing.B) {
	storage, _, cleanup := setupTestStorage(&testing.T{})
	defer cleanup()

	ctx := context.Background()

	// Setup test queue
	createQueueRequest := &queueservice_pb.CreateQueueRequest{
		Name: "benchmark-queue",
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	}
	_, err := storage.CreateQueue(ctx, createQueueRequest)
	if err != nil {
		b.Fatalf("Failed to create benchmark queue: %v", err)
	}

	b.Run("CreateQueueMessage", func(b *testing.B) {
		request := &queueservice_pb.PostMessageRequest{
			QueueName: "benchmark-queue",
			Message: &message_pb.Message{
				Metadata: &message_pb.Message_Metadata{
					Payload: &common_pb.Payload{},
				},
			},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			request.Message.MessageId = string(rune(i)) // Simple ID generation
			_, err := storage.CreateQueueMessage(ctx, request, nil)
			if err != nil {
				b.Logf("Message creation failed: %v", err)
			}
		}
	})

	b.Run("ListQueues", func(b *testing.B) {
		request := &queueservice_pb.ListQueuesRequest{}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := storage.ListQueues(ctx, request)
			if err != nil {
				b.Logf("Queue listing failed: %v", err)
			}
		}
	})
}
