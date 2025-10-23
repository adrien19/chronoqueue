//go:build integration

package repository

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
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestDLQImplementation(t *testing.T) {
	// Setup test environment
	ctx := context.Background()
	redisClient := setupRedisForTest()
	defer redisClient.Close()

	logger := log.NewLogger()
	keyManager := &keymanager.EncryptionKeyManager{}
	storage := NewQueueStorageForTesting(redisClient, keyManager, logger)

	// Clean up any existing data
	redisClient.FlushAll(ctx)

	t.Run("DLQ Auto-Creation and Message Movement", func(t *testing.T) {
		queueName := "test-queue-dlq"
		dlqName := queueName + "_dlq"

		// Create test queue with DLQ configuration
		createQueueReq := &queueservice_pb.CreateQueueRequest{
			Name: queueName,
			Metadata: &queue_pb.QueueMetadata{
				Type:               queue_pb.QueueType_SIMPLE,
				DefaultMaxAttempts: 2,
				LeaseDuration:      durationpb.New(5 * time.Second),
				AutoCreateDlq:      true,
			},
		}
		_, err := storage.CreateQueue(ctx, createQueueReq)
		require.NoError(t, err)

		// Create a message that will exhaust retries
		messageReq := &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message: &message_pb.Message{
				MessageId: "test-msg-1",
				Metadata: &message_pb.Message_Metadata{
					Payload: &common_pb.Payload{
						Metadata: map[string]*structpb.Value{
							"contentType": structpb.NewStringValue("text/plain"),
						},
						Data: &structpb.Struct{
							Fields: map[string]*structpb.Value{
								"message": structpb.NewStringValue("test message for DLQ"),
							},
						},
					},
					InvisibilityDuration: durationpb.New(5 * time.Millisecond),  // Short
					LeaseDuration:        durationpb.New(50 * time.Millisecond), // Short
					Priority:             100,
				},
			},
		}
		_, err = storage.CreateQueueMessage(ctx, messageReq, nil)
		require.NoError(t, err)

		// Wait for message to become PENDING
		time.Sleep(20 * time.Millisecond)
		err = storage.ProcessExpiredInvisibleMessages(ctx)
		require.NoError(t, err)

		// Get the message to start processing (becomes RUNNING)
		getReq := &queueservice_pb.GetNextMessageRequest{
			QueueName: queueName,
		}
		response, err := storage.GetQueueMessage(ctx, getReq)
		require.NoError(t, err)
		require.NotNil(t, response.Message)

		// Let the lease expire to trigger retry logic
		time.Sleep(100 * time.Millisecond) // Longer wait to ensure lease expires
		err = storage.ProcessExpiredRunningMessages(ctx)
		require.NoError(t, err)

		// Get message again (second attempt)
		response, err = storage.GetQueueMessage(ctx, getReq)
		require.NoError(t, err)
		require.NotNil(t, response.Message)
		assert.Equal(t, int32(1), response.Message.Metadata.AttemptsLeft) // Should have 1 attempt left

		// Let lease expire again to exhaust retries
		time.Sleep(100 * time.Millisecond) // Longer wait to ensure lease expires
		err = storage.ProcessExpiredRunningMessages(ctx)
		require.NoError(t, err)

		// Process ERRORED messages for DLQ movement
		err = storage.ProcessErroredMessagesForDLQ(ctx)
		require.NoError(t, err)

		// Verify message was moved to DLQ
		dlqMessages, err := storage.GetDLQMessages(ctx, dlqName, 10)
		require.NoError(t, err)
		require.Len(t, dlqMessages, 1)
		assert.Equal(t, "test-msg-1", dlqMessages[0].MessageId)
		assert.Equal(t, message_pb.Message_Metadata_PENDING, dlqMessages[0].Metadata.State)

		// Verify original queue is empty
		originalQueueMessages, err := storage.PeekQueueMessages(ctx, &queueservice_pb.PeekQueueMessagesRequest{
			QueueName: queueName,
			Limit:     10,
		})
		require.NoError(t, err)
		assert.Len(t, originalQueueMessages.Messages, 0)
	})

	t.Run("DLQ with Custom Name", func(t *testing.T) {
		queueName := "test-queue-custom-dlq"
		customDLQName := "custom-dead-letter-queue"

		// Create test queue with custom DLQ name
		createQueueReq := &queueservice_pb.CreateQueueRequest{
			Name: queueName,
			Metadata: &queue_pb.QueueMetadata{
				Type:                queue_pb.QueueType_SIMPLE,
				DefaultMaxAttempts:  1,
				LeaseDuration:       durationpb.New(1 * time.Millisecond),
				DeadLetterQueueName: customDLQName,
				AutoCreateDlq:       true,
			},
		}
		_, err := storage.CreateQueue(ctx, createQueueReq)
		require.NoError(t, err)

		// Create and process a message to DLQ
		messageReq := &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message: &message_pb.Message{
				MessageId: "test-msg-custom-dlq",
				Metadata: &message_pb.Message_Metadata{
					Payload: &common_pb.Payload{
						Metadata: map[string]*structpb.Value{
							"contentType": structpb.NewStringValue("text/plain"),
						},
						Data: &structpb.Struct{
							Fields: map[string]*structpb.Value{
								"message": structpb.NewStringValue("test message for custom DLQ"),
							},
						},
					},
					InvisibilityDuration: durationpb.New(1 * time.Millisecond),
					LeaseDuration:        durationpb.New(1 * time.Millisecond),
					Priority:             100,
				},
			},
		}
		_, err = storage.CreateQueueMessage(ctx, messageReq, nil)
		require.NoError(t, err)

		// Wait and process to move to DLQ
		time.Sleep(10 * time.Millisecond)
		err = storage.ProcessExpiredInvisibleMessages(ctx)
		require.NoError(t, err)

		// Get and let expire
		getReq := &queueservice_pb.GetNextMessageRequest{QueueName: queueName}
		_, err = storage.GetQueueMessage(ctx, getReq)
		require.NoError(t, err)

		time.Sleep(10 * time.Millisecond)
		err = storage.ProcessExpiredRunningMessages(ctx)
		require.NoError(t, err)

		err = storage.ProcessErroredMessagesForDLQ(ctx)
		require.NoError(t, err)

		// Verify message is in custom DLQ
		dlqMessages, err := storage.GetDLQMessages(ctx, customDLQName, 10)
		require.NoError(t, err)
		require.Len(t, dlqMessages, 1)
		assert.Equal(t, "test-msg-custom-dlq", dlqMessages[0].MessageId)
	})

	t.Run("Infinite Retry Messages Not Moved to DLQ", func(t *testing.T) {
		queueName := "test-queue-infinite"
		dlqName := queueName + "_dlq"

		// Create queue with infinite retry default
		createQueueReq := &queueservice_pb.CreateQueueRequest{
			Name: queueName,
			Metadata: &queue_pb.QueueMetadata{
				Type:               queue_pb.QueueType_SIMPLE,
				DefaultMaxAttempts: -1, // Infinite retries
				LeaseDuration:      durationpb.New(1 * time.Millisecond),
				AutoCreateDlq:      true,
			},
		}
		_, err := storage.CreateQueue(ctx, createQueueReq)
		require.NoError(t, err)

		// Create message with infinite retries
		messageReq := &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message: &message_pb.Message{
				MessageId: "test-msg-infinite",
				Metadata: &message_pb.Message_Metadata{
					Payload: &common_pb.Payload{
						Metadata: map[string]*structpb.Value{
							"contentType": structpb.NewStringValue("text/plain"),
						},
						Data: &structpb.Struct{
							Fields: map[string]*structpb.Value{
								"message": structpb.NewStringValue("infinite retry message"),
							},
						},
					},
					InvisibilityDuration: durationpb.New(1 * time.Millisecond),
					LeaseDuration:        durationpb.New(1 * time.Millisecond),
					MaxAttempts:          -1, // Explicit infinite retry
					Priority:             100,
				},
			},
		}
		_, err = storage.CreateQueueMessage(ctx, messageReq, nil)
		require.NoError(t, err)

		// Process multiple times (should keep retrying)
		for i := 0; i < 5; i++ {
			time.Sleep(10 * time.Millisecond)
			err = storage.ProcessExpiredInvisibleMessages(ctx)
			require.NoError(t, err)

			_, err = storage.GetQueueMessage(ctx, &queueservice_pb.GetNextMessageRequest{QueueName: queueName})
			require.NoError(t, err)

			time.Sleep(10 * time.Millisecond)
			err = storage.ProcessExpiredRunningMessages(ctx)
			require.NoError(t, err)
		}

		// Process DLQ - should not move infinite retry messages
		err = storage.ProcessErroredMessagesForDLQ(ctx)
		require.NoError(t, err)

		// Verify DLQ is empty (message should still be retrying)
		dlqMessages, err := storage.GetDLQMessages(ctx, dlqName, 10)
		require.NoError(t, err)
		assert.Len(t, dlqMessages, 0)
	})

	t.Run("DLQ Management Operations", func(t *testing.T) {
		queueName := "test-queue-mgmt"
		dlqName := queueName + "_dlq"

		// Create queue and add message to DLQ (setup)
		createQueueReq := &queueservice_pb.CreateQueueRequest{
			Name: queueName,
			Metadata: &queue_pb.QueueMetadata{
				Type:               queue_pb.QueueType_SIMPLE,
				DefaultMaxAttempts: 1,
				LeaseDuration:      durationpb.New(1 * time.Millisecond),
				AutoCreateDlq:      true,
			},
		}
		_, err := storage.CreateQueue(ctx, createQueueReq)
		require.NoError(t, err)

		// Add a message that will go to DLQ
		messageReq := &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message: &message_pb.Message{
				MessageId: "test-msg-mgmt",
				Metadata: &message_pb.Message_Metadata{
					Payload: &common_pb.Payload{
						Metadata: map[string]*structpb.Value{
							"contentType": structpb.NewStringValue("text/plain"),
						},
						Data: &structpb.Struct{
							Fields: map[string]*structpb.Value{
								"message": structpb.NewStringValue("management test message"),
							},
						},
					},
					InvisibilityDuration: durationpb.New(1 * time.Millisecond),
					LeaseDuration:        durationpb.New(1 * time.Millisecond),
					Priority:             100,
				},
			},
		}
		_, err = storage.CreateQueueMessage(ctx, messageReq, nil)
		require.NoError(t, err)

		// Process to DLQ
		time.Sleep(10 * time.Millisecond)
		err = storage.ProcessExpiredInvisibleMessages(ctx)
		require.NoError(t, err)
		_, err = storage.GetQueueMessage(ctx, &queueservice_pb.GetNextMessageRequest{QueueName: queueName})
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
		err = storage.ProcessExpiredRunningMessages(ctx)
		require.NoError(t, err)
		err = storage.ProcessErroredMessagesForDLQ(ctx)
		require.NoError(t, err)

		// Test DLQ stats
		stats, err := storage.GetDLQStats(ctx, dlqName)
		require.NoError(t, err)
		assert.Equal(t, dlqName, stats.Name)
		assert.Equal(t, int64(1), stats.MessageCount)

		// Test requeue from DLQ
		err = storage.RequeueFromDLQ(ctx, dlqName, "test-msg-mgmt", queueName, true)
		require.NoError(t, err)

		// Verify message is back in original queue
		originalMessages, err := storage.PeekQueueMessages(ctx, &queueservice_pb.PeekQueueMessagesRequest{
			QueueName: queueName,
			Limit:     10,
		})
		require.NoError(t, err)
		require.Len(t, originalMessages.Messages, 1)
		assert.Equal(t, "test-msg-mgmt", originalMessages.Messages[0].MessageId)

		// Verify DLQ is now empty
		dlqMessages, err := storage.GetDLQMessages(ctx, dlqName, 10)
		require.NoError(t, err)
		assert.Len(t, dlqMessages, 0)
	})

	t.Run("DLQ Auto-Create Disabled", func(t *testing.T) {
		queueName := "test-queue-no-auto-dlq"

		// Create queue with auto-create DLQ disabled
		createQueueReq := &queueservice_pb.CreateQueueRequest{
			Name: queueName,
			Metadata: &queue_pb.QueueMetadata{
				Type:               queue_pb.QueueType_SIMPLE,
				DefaultMaxAttempts: 1,
				LeaseDuration:      durationpb.New(1 * time.Millisecond),
				AutoCreateDlq:      false, // Disabled
			},
		}
		_, err := storage.CreateQueue(ctx, createQueueReq)
		require.NoError(t, err)

		// Create and process message
		messageReq := &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message: &message_pb.Message{
				MessageId: "test-msg-no-auto",
				Metadata: &message_pb.Message_Metadata{
					Payload: &common_pb.Payload{
						Metadata: map[string]*structpb.Value{
							"contentType": structpb.NewStringValue("text/plain"),
						},
						Data: &structpb.Struct{
							Fields: map[string]*structpb.Value{
								"message": structpb.NewStringValue("no auto DLQ message"),
							},
						},
					},
					InvisibilityDuration: durationpb.New(1 * time.Millisecond),
					LeaseDuration:        durationpb.New(1 * time.Millisecond),
					Priority:             100,
				},
			},
		}
		_, err = storage.CreateQueueMessage(ctx, messageReq, nil)
		require.NoError(t, err)

		// Process to exhaustion
		time.Sleep(10 * time.Millisecond)
		err = storage.ProcessExpiredInvisibleMessages(ctx)
		require.NoError(t, err)
		_, err = storage.GetQueueMessage(ctx, &queueservice_pb.GetNextMessageRequest{QueueName: queueName})
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
		err = storage.ProcessExpiredRunningMessages(ctx)
		require.NoError(t, err)

		// Process DLQ - should not create DLQ
		err = storage.ProcessErroredMessagesForDLQ(ctx)
		require.NoError(t, err)

		// Verify default DLQ was not created and contains no messages
		dlqName := queueName + "_dlq"
		dlqMessages, err := storage.GetDLQMessages(ctx, dlqName, 10)
		require.Error(t, err) // Should error because DLQ doesn't exist
		assert.Contains(t, err.Error(), "does not exist")
		assert.Nil(t, dlqMessages)
	})
}

func setupRedisForTest() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   1, // Use test database
	})
}
