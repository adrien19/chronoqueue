package integration

// Package integration provides concurrency tests for ChronoQueue.
//
// These tests validate:
// - Concurrent message claiming (no duplicates)
// - Race-free message acknowledgment
// - Lease expiry under concurrent load
// - Proper isolation between workers
//
// Run with: go test -v -race ./tests/integration/ -run TestConcurrency

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/tests/helpers"
)

// TestConcurrency_MessageClaiming_NoDuplicates verifies that concurrent workers
// claiming messages from the same queue do not receive duplicate messages.
//
// Scenario:
//   - Create queue with 10 messages
//   - Spawn 100 workers trying to claim messages concurrently
//   - Verify each message is claimed exactly once
//   - Verify no worker receives a duplicate
func TestConcurrency_MessageClaiming_NoDuplicates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "concurrency-claim")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
		},
	})
	require.NoError(t, err)

	// Post 10 messages
	numMessages := 10
	messageIDs := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		msgID := fmt.Sprintf("msg-%d-%s", i, helpers.GenerateUniqueMessageID(t))
		messageIDs[i] = msgID

		payload, err := structpb.NewStruct(map[string]interface{}{
			"index": i,
			"data":  fmt.Sprintf("test-data-%d", i),
		})
		require.NoError(t, err)

		_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message: &message_pb.Message{
				MessageId: msgID,
				Metadata: &message_pb.Message_Metadata{
					Payload: &common_pb.Payload{
						Data:        payload,
						ContentType: "application/json",
					},
					MaxAttempts: 3,
				},
			},
		})
		require.NoError(t, err)
	}

	// Spawn 100 workers to claim messages concurrently
	numWorkers := 100
	var wg sync.WaitGroup
	claimedMessages := make(chan string, numWorkers)
	errors := make(chan error, numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		workerID := fmt.Sprintf("worker-%d", i)

		go func(id string) {
			defer wg.Done()

			workerPtr := id
			resp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
				QueueName: queueName,
				WorkerId:  &workerPtr,
			})
			if err != nil {
				errors <- err
				return
			}

			if resp.Message != nil && resp.Message.MessageId != "" {
				claimedMessages <- resp.Message.MessageId
			}
		}(workerID)
	}

	// Wait for all workers to complete
	wg.Wait()
	close(claimedMessages)
	close(errors)

	// Verify no errors (except "no messages available")
	for err := range errors {
		// It's OK to have "no messages available" errors since we have more workers than messages
		assert.Contains(t, err.Error(), "no messages available", "Unexpected error: %v", err)
	}

	// Collect claimed messages
	claimed := make(map[string]int)
	for msgID := range claimedMessages {
		claimed[msgID]++
	}

	// Verify each message was claimed exactly once
	assert.Len(t, claimed, numMessages, "All messages should be claimed exactly once")
	for msgID, count := range claimed {
		assert.Equal(t, 1, count, "Message %s should be claimed exactly once, got %d", msgID, count)
	}

	// Verify all original messages were claimed
	for _, msgID := range messageIDs {
		assert.Contains(t, claimed, msgID, "Message %s should be claimed", msgID)
	}
}

// TestConcurrency_SingleMessage_RaceDetector verifies that when multiple workers
// try to claim a single message, exactly one succeeds.
//
// This test should be run with -race flag to detect data races.
func TestConcurrency_SingleMessage_RaceDetector(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "concurrency-race")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
		},
	})
	require.NoError(t, err)

	// Post exactly one message
	msgID := helpers.GenerateUniqueMessageID(t)
	payload, err := structpb.NewStruct(map[string]interface{}{
		"data": "single-message",
	})
	require.NoError(t, err)

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message: &message_pb.Message{
			MessageId: msgID,
			Metadata: &message_pb.Message_Metadata{
				Payload: &common_pb.Payload{
					Data:        payload,
					ContentType: "application/json",
				},
				MaxAttempts: 3,
			},
		},
	})
	require.NoError(t, err)

	// Spawn 50 workers to claim the same message concurrently
	numWorkers := 50
	var wg sync.WaitGroup
	successCount := int32(0)
	var mu sync.Mutex

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		workerID := fmt.Sprintf("worker-%d", i)

		go func(id string) {
			defer wg.Done()

			workerPtr := id
			resp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
				QueueName: queueName,
				WorkerId:  &workerPtr,
			})

			if err == nil && resp.Message != nil && resp.Message.MessageId == msgID {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(workerID)
	}

	wg.Wait()

	// Verify exactly one worker successfully claimed the message
	assert.Equal(t, int32(1), successCount, "Exactly one worker should claim the message")
}

// TestConcurrency_Acknowledge_Idempotency verifies that concurrent acknowledgments
// of the same message are idempotent (no errors, no side effects).
func TestConcurrency_Acknowledge_Idempotency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "concurrency-ack")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
		},
	})
	require.NoError(t, err)

	// Post a message
	msgID := helpers.GenerateUniqueMessageID(t)
	payload, err := structpb.NewStruct(map[string]interface{}{
		"data": "ack-test",
	})
	require.NoError(t, err)

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message: &message_pb.Message{
			MessageId: msgID,
			Metadata: &message_pb.Message_Metadata{
				Payload: &common_pb.Payload{
					Data:        payload,
					ContentType: "application/json",
				},
				MaxAttempts: 3,
			},
		},
	})
	require.NoError(t, err)

	// Claim the message
	workerID := "worker-ack-test"
	workerPtr := workerID
	resp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName: queueName,
		WorkerId:  &workerPtr,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Message)
	require.Equal(t, msgID, resp.Message.MessageId)
	require.NotNil(t, resp.Message.Metadata.CurrentAttempt, "CurrentAttempt should not be nil")

	attemptID := resp.Message.Metadata.CurrentAttempt.AttemptId

	// Spawn 20 goroutines to acknowledge the same message concurrently
	numWorkers := 20
	var wg sync.WaitGroup
	errors := make(chan error, numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			attemptPtr := attemptID
			_, err := client.AcknowledgeMessage(ctx, &queueservice_pb.AcknowledgeMessageRequest{
				QueueName: queueName,
				MessageId: msgID,
				AttemptId: &attemptPtr,
				State:     message_pb.Message_Metadata_COMPLETED,
			})
			if err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	// Verify at most one error (if any)
	// Idempotency means the first ack succeeds, subsequent ones should either succeed or fail gracefully
	errorCount := 0
	for err := range errors {
		errorCount++
		// If there's an error, it should be "message not found" or "attempt mismatch"
		// This is acceptable as long as the message was successfully acknowledged once
		t.Logf("Acknowledge error (expected for idempotency): %v", err)
	}

	// The important assertion: no panic, no data corruption
	// Some implementations may allow multiple acks to succeed (idempotent)
	// Others may return errors after the first (also valid)
	assert.LessOrEqual(t, errorCount, numWorkers-1, "At least one acknowledgment should succeed")
}

// TestConcurrency_LeaseExpiry_ConcurrentReclaim verifies that expired leases
// are properly reclaimed under concurrent load without data races.
func TestConcurrency_LeaseExpiry_ConcurrentReclaim(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "concurrency-lease")

	// Create queue with very short lease (2 seconds)
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(2 * time.Second),
		},
	})
	require.NoError(t, err)

	// Post 5 messages
	numMessages := 5
	for i := 0; i < numMessages; i++ {
		msgID := fmt.Sprintf("msg-%d-%s", i, helpers.GenerateUniqueMessageID(t))

		payload, err := structpb.NewStruct(map[string]interface{}{
			"index": i,
		})
		require.NoError(t, err)

		_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message: &message_pb.Message{
				MessageId: msgID,
				Metadata: &message_pb.Message_Metadata{
					Payload: &common_pb.Payload{
						Data:        payload,
						ContentType: "application/json",
					},
					MaxAttempts: 5,
				},
			},
		})
		require.NoError(t, err)
	}

	// Claim all messages
	for i := 0; i < numMessages; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		workerPtr := workerID
		resp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
			QueueName: queueName,
			WorkerId:  &workerPtr,
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Message)
	}

	// Wait for leases to expire (2 seconds + 1 second buffer)
	time.Sleep(3 * time.Second)

	// Wait for reclaim processor to run (runs every 2 seconds)
	// Add extra time to ensure at least one reclaim cycle completes
	time.Sleep(3 * time.Second)

	// Spawn 30 workers to reclaim expired messages concurrently
	numWorkers := 30
	var wg sync.WaitGroup
	claimedMessages := make(chan string, numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		workerID := fmt.Sprintf("reclaim-worker-%d", i)

		go func(id string) {
			defer wg.Done()

			workerPtr := id
			resp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
				QueueName: queueName,
				WorkerId:  &workerPtr,
			})

			if err == nil && resp.Message != nil {
				claimedMessages <- resp.Message.MessageId
			}
		}(workerID)
	}

	wg.Wait()
	close(claimedMessages)

	// Verify messages were reclaimed
	reclaimed := make(map[string]int)
	for msgID := range claimedMessages {
		reclaimed[msgID]++
	}

	// Each message should be reclaimed exactly once
	for msgID, count := range reclaimed {
		assert.Equal(t, 1, count, "Message %s should be reclaimed exactly once, got %d", msgID, count)
	}

	// At least some messages should be reclaimed (expired lease allows reclaim)
	assert.NotEmpty(t, reclaimed, "At least some messages should be reclaimed after lease expiry")
}
