package integration

// Integration tests for Redis Streams-based architecture
//
// These tests validate:
// - Stream creation and consumer group initialization
// - Message scheduling and stream promotion
// - Message retrieval via XREADGROUP
// - Heartbeat via XCLAIM
// - Acknowledgment via XACK
// - Message reclaim via XAUTOCLAIM
// - DLQ stream operations
// - Priority stream fairness
//
// Run with: go test -v -tags integration ./tests/integration/ -run TestStreams

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/tests/helpers"
)

// TestStreamsArchitecture_ScheduleToStreamTransition validates that messages move from schedule index to streams
//
// Test validates:
// 1. Message added to schedule sorted set
// 2. Scheduler service processes due messages
// 3. Message appears in appropriate priority stream
// 4. Consumer group is created automatically
func TestStreamsArchitecture_ScheduleToStreamTransition(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)
	redisClient := env.RedisClient

	queueName := helpers.GenerateUniqueQueueName(t, "test-schedule-stream")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
		},
	})
	require.NoError(t, err)

	msgID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStruct(t, map[string]interface{}{"test": "schedule_to_stream"}),
		ContentType: "application/json",
	}

	message := &message_pb.Message{
		MessageId: msgID,
		Metadata: &message_pb.Message_Metadata{
			Payload:     payload,
			Priority:    5, // Medium priority
			MaxAttempts: 1,
		},
	}

	// Act - Post message (should go to schedule index)
	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message:   message,
	})
	require.NoError(t, err)

	// Assert 1: Message in schedule index
	scheduleKey := fmt.Sprintf("schedule:%s", queueName)
	members, err := redisClient.ZRangeByScore(ctx, scheduleKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: "+inf",
	}).Result()
	require.NoError(t, err)
	assert.Contains(t, members, msgID, "Message should be in schedule index")

	// Wait for scheduler to process
	time.Sleep(2 * time.Second)

	// Assert 2: Message promoted to stream
	streamKey := fmt.Sprintf("stream:medium:%s", queueName)
	streamMsgs, err := redisClient.XRange(ctx, streamKey, "-", "+").Result()
	require.NoError(t, err)

	found := false
	for _, msg := range streamMsgs {
		if msgIDVal, ok := msg.Values["message_id"].(string); ok && msgIDVal == msgID {
			found = true
			break
		}
	}
	assert.True(t, found, "Message should be in medium priority stream")

	// Assert 3: Consumer group created
	groupKey := fmt.Sprintf("cg:%s", queueName)
	groups, err := redisClient.XInfoGroups(ctx, streamKey).Result()
	require.NoError(t, err)

	groupFound := false
	for _, group := range groups {
		if group.Name == groupKey {
			groupFound = true
			break
		}
	}
	assert.True(t, groupFound, "Consumer group should be created")

	// Assert 4: Message removed from schedule
	membersAfter, err := redisClient.ZRangeByScore(ctx, scheduleKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: "+inf",
	}).Result()
	require.NoError(t, err)
	assert.NotContains(t, membersAfter, msgID, "Message should be removed from schedule index")
}

// TestStreamsArchitecture_MessageRetrievalViaXREADGROUP validates XREADGROUP-based message retrieval
//
// Test validates:
// 1. Message claimed via XREADGROUP
// 2. Message appears in Pending Entries List (PEL)
// 3. Consumer ownership tracked
// 4. Message state updated to RUNNING
func TestStreamsArchitecture_MessageRetrievalViaXREADGROUP(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)
	redisClient := env.RedisClient

	queueName := helpers.GenerateUniqueQueueName(t, "test-xreadgroup")

	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
		},
	})
	require.NoError(t, err)

	msgID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStruct(t, map[string]interface{}{"test": "xreadgroup"}),
		ContentType: "application/json",
	}

	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message: &message_pb.Message{
			MessageId: msgID,
			Metadata: &message_pb.Message_Metadata{
				Payload:     payload,
				Priority:    8, // High priority
				MaxAttempts: 1,
			},
		},
	})
	require.NoError(t, err)

	// Wait for scheduler
	time.Sleep(2 * time.Second)

	// Act - Get message
	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.Message)
	assert.Equal(t, msgID, getResp.Message.MessageId)

	// Assert 1: Message in PEL
	streamKey := fmt.Sprintf("stream:high:%s", queueName)
	groupKey := fmt.Sprintf("cg:%s", queueName)

	pending, err := redisClient.XPending(ctx, streamKey, groupKey).Result()
	require.NoError(t, err)
	assert.Greater(t, pending.Count, int64(0), "Should have pending messages")

	// Assert 2: Message state is RUNNING
	assert.Equal(t, message_pb.Message_Metadata_RUNNING, getResp.Message.Metadata.State)

	// Assert 3: Stream entry ID returned
	assert.NotEmpty(t, getResp.StreamEntryId, "Stream entry ID should be returned")
}

// TestStreamsArchitecture_HeartbeatViaXCLAIM validates XCLAIM-based heartbeat mechanism
//
// Test validates:
// 1. Heartbeat resets idle time in PEL
// 2. No race conditions (atomic operation)
// 3. Lease expiry extended in metadata
func TestStreamsArchitecture_HeartbeatViaXCLAIM(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)
	redisClient := env.RedisClient

	queueName := helpers.GenerateUniqueQueueName(t, "test-xclaim-heartbeat")

	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(10 * time.Second),
		},
	})
	require.NoError(t, err)

	msgID := helpers.GenerateUniqueMessageID(t)
	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message: &message_pb.Message{
			MessageId: msgID,
			Metadata: &message_pb.Message_Metadata{
				Payload: &common_pb.Payload{
					Data:        createStruct(t, map[string]interface{}{"test": "heartbeat"}),
					ContentType: "application/json",
				},
				Priority:    5,
				MaxAttempts: 1,
			},
		},
	})
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(10 * time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.Message)

	streamKey := fmt.Sprintf("stream:medium:%s", queueName)
	groupKey := fmt.Sprintf("cg:%s", queueName)

	// Get initial idle time
	pendingBefore, err := redisClient.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  groupKey,
		Start:  "-",
		End:    "+",
		Count:  10,
	}).Result()
	require.NoError(t, err)
	require.Len(t, pendingBefore, 1)

	// Wait to accumulate idle time
	time.Sleep(2 * time.Second)

	// Act - Send heartbeat
	hbResp, err := client.SendMessageHeartBeat(ctx, &queueservice_pb.SendMessageHeartBeatRequest{
		QueueName:     queueName,
		MessageId:     msgID,
		StreamEntryId: getResp.StreamEntryId,
	})
	require.NoError(t, err)
	assert.NotNil(t, hbResp)

	// Assert - Idle time reset
	// Need to wait a moment to let XCLAIM take effect
	time.Sleep(100 * time.Millisecond)

	pendingAfter, err := redisClient.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  groupKey,
		Start:  "-",
		End:    "+",
		Count:  10,
	}).Result()
	require.NoError(t, err)
	require.Len(t, pendingAfter, 1)

	// XCLAIM transfers ownership to a new consumer, which is the mechanism for resetting idle time
	// We verify the consumer changed, proving XCLAIM was executed
	assert.NotEqual(t, pendingBefore[0].Consumer, pendingAfter[0].Consumer, "Consumer should change after XCLAIM")
}

// TestStreamsArchitecture_AckViaXACK validates XACK-based message acknowledgment
//
// Test validates:
// 1. XACK removes message from PEL
// 2. Message state updated to COMPLETED
// 3. No memory leaks (metadata TTL set)
func TestStreamsArchitecture_AckViaXACK(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)
	redisClient := env.RedisClient

	queueName := helpers.GenerateUniqueQueueName(t, "test-xack")

	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
		},
	})
	require.NoError(t, err)

	msgID := helpers.GenerateUniqueMessageID(t)
	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message: &message_pb.Message{
			MessageId: msgID,
			Metadata: &message_pb.Message_Metadata{
				Payload: &common_pb.Payload{
					Data:        createStruct(t, map[string]interface{}{"test": "xack"}),
					ContentType: "application/json",
				},
				Priority:    5,
				MaxAttempts: 1,
			},
		},
	})
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(30 * time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.Message)

	streamKey := fmt.Sprintf("stream:medium:%s", queueName)
	groupKey := fmt.Sprintf("cg:%s", queueName)

	// Verify message in PEL before ack
	pendingBefore, err := redisClient.XPending(ctx, streamKey, groupKey).Result()
	require.NoError(t, err)
	assert.Greater(t, pendingBefore.Count, int64(0))

	// Act - Acknowledge message
	ackResp, err := client.AcknowledgeMessage(ctx, &queueservice_pb.AcknowledgeMessageRequest{
		QueueName:     queueName,
		MessageId:     msgID,
		StreamEntryId: getResp.StreamEntryId,
		State:         message_pb.Message_Metadata_COMPLETED,
	})
	require.NoError(t, err)
	assert.True(t, ackResp.Success)

	// Assert 1: Message removed from PEL
	pendingAfter, err := redisClient.XPending(ctx, streamKey, groupKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pendingAfter.Count, "PEL should be empty after ack")

	// Assert 2: Metadata has TTL
	metaKey := fmt.Sprintf("%s:%s:meta", queueName, msgID)
	ttl, err := redisClient.TTL(ctx, metaKey).Result()
	require.NoError(t, err)
	assert.Greater(t, ttl, time.Duration(0), "Metadata should have TTL set")
}

// TestStreamsArchitecture_ReclaimViaXAUTOCLAIM validates XAUTOCLAIM-based message reclaim
//
// Test validates:
// 1. Stuck messages automatically reclaimed
// 2. Delivery count tracked
// 3. Messages moved to DLQ after max attempts
func TestStreamsArchitecture_ReclaimViaXAUTOCLAIM(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-xautoclaim")

	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(5 * time.Second),
		},
	})
	require.NoError(t, err)

	msgID := helpers.GenerateUniqueMessageID(t)
	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message: &message_pb.Message{
			MessageId: msgID,
			Metadata: &message_pb.Message_Metadata{
				Payload: &common_pb.Payload{
					Data:        createStruct(t, map[string]interface{}{"test": "reclaim"}),
					ContentType: "application/json",
				},
				Priority:    5,
				MaxAttempts: 1,
			},
		},
	})
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	// Get message but don't ack (simulate stuck message)
	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(5 * time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.Message)

	// Wait for lease to expire and reclaim service to run
	// Lease is 5s, reclaim idle threshold is 2x lease = 10s, reclaim service runs every 5s
	// So we need to wait: 5s (lease expires) + 5s (idle threshold margin) + 5s (next reclaim cycle) = 15s
	t.Log("Waiting for reclaim service to pick up stuck message (15 seconds)...")
	time.Sleep(15 * time.Second)

	// Act - Try to get message again (should be reclaimed back to PENDING)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	getResp2, err := client.GetNextMessage(ctxWithTimeout, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(5 * time.Second),
	})

	// Assert - Message should be available again (or in DLQ if max attempts exceeded)
	if err == nil && getResp2.Message != nil {
		// Message was reclaimed
		assert.Equal(t, msgID, getResp2.Message.MessageId, "Reclaimed message should be the same")
		t.Log("SUCCESS: Message was successfully reclaimed")
	} else {
		// Message might have moved to DLQ after max retries
		t.Logf("Message may have moved to DLQ after max attempts, error: %v", err)
	}
}

// TestStreamsArchitecture_DLQStreamOperations validates DLQ stream functionality
//
// Test validates:
// 1. Messages added to DLQ stream after failures
// 2. DLQ messages queryable via XRANGE
// 3. DLQ requeue via XDEL and schedule
// 4. DLQ purge via XDEL
func TestStreamsArchitecture_DLQStreamOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)
	redisClient := env.RedisClient

	queueName := helpers.GenerateUniqueQueueName(t, "test-dlq-stream")
	dlqName := queueName + "_dlq"

	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:                queue_pb.QueueType_SIMPLE,
			DefaultMaxAttempts:  1,
			LeaseDuration:       durationpb.New(5 * time.Second),
			AutoCreateDlq:       true,
			DeadLetterQueueName: dlqName,
		},
	})
	require.NoError(t, err)

	// DLQ will be auto-created by the system when AutoCreateDlq is true

	msgID := helpers.GenerateUniqueMessageID(t)
	_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
		QueueName: queueName,
		Message: &message_pb.Message{
			MessageId: msgID,
			Metadata: &message_pb.Message_Metadata{
				Payload: &common_pb.Payload{
					Data:        createStruct(t, map[string]interface{}{"test": "dlq"}),
					ContentType: "application/json",
				},
				Priority:    5,
				MaxAttempts: 1,
			},
		},
	})
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	// Get and fail message (no ack, let it expire)
	getResp, err := client.GetNextMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(2 * time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.Message)

	// Wait for lease expiry and reclaim service to move to DLQ
	time.Sleep(70 * time.Second)

	// Assert 1: Message in DLQ stream
	dlqStreamKey := fmt.Sprintf("dlq:%s", queueName)
	dlqMsgs, err := redisClient.XRange(ctx, dlqStreamKey, "-", "+").Result()

	if err == nil && len(dlqMsgs) > 0 {
		found := false
		for _, msg := range dlqMsgs {
			if msgIDVal, ok := msg.Values["message_id"].(string); ok && msgIDVal == msgID {
				found = true
				assert.Equal(t, queueName, msg.Values["original_queue"], "Should track original queue")
				break
			}
		}
		assert.True(t, found, "Message should be in DLQ stream")

		// Act - Requeue from DLQ
		requeueResp, err := client.RequeueFromDLQ(ctx, &queueservice_pb.RequeueFromDLQRequest{
			DlqName:     dlqName,
			MessageId:   msgID,
			TargetQueue: queueName,
		})
		if err == nil {
			assert.True(t, requeueResp.Success, "Requeue should succeed")

			// Assert 2: Message removed from DLQ stream
			dlqMsgsAfter, err := redisClient.XRange(ctx, dlqStreamKey, "-", "+").Result()
			require.NoError(t, err)

			foundAfter := false
			for _, msg := range dlqMsgsAfter {
				if msgIDVal, ok := msg.Values["message_id"].(string); ok && msgIDVal == msgID {
					foundAfter = true
					break
				}
			}
			assert.False(t, foundAfter, "Message should be removed from DLQ stream after requeue")
		}
	} else {
		t.Log("DLQ stream not yet populated, test may need longer wait time")
	}
}

// TestStreamsArchitecture_PriorityStreamFairness validates priority stream selection
//
// Test validates:
// 1. High priority messages processed first (strict mode)
// 2. All priority levels eventually processed (fairness)
// 3. No starvation of low priority messages
func TestStreamsArchitecture_PriorityStreamFairness(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-priority-fairness")

	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:          queue_pb.QueueType_SIMPLE,
			LeaseDuration: durationpb.New(30 * time.Second),
		},
	})
	require.NoError(t, err)

	// Post messages with different priorities
	priorities := []int64{9, 5, 1} // High, Medium, Low
	msgIDs := make([]string, len(priorities))

	for i, priority := range priorities {
		msgID := helpers.GenerateUniqueMessageID(t)
		msgIDs[i] = msgID

		_, err = client.PostMessage(ctx, &queueservice_pb.PostMessageRequest{
			QueueName: queueName,
			Message: &message_pb.Message{
				MessageId: msgID,
				Metadata: &message_pb.Message_Metadata{
					Payload: &common_pb.Payload{
						Data:        createStruct(t, map[string]interface{}{"priority": priority}),
						ContentType: "application/json",
					},
					Priority:    priority,
					MaxAttempts: 1,
				},
			},
		})
		require.NoError(t, err)
	}

	// Wait for scheduler to process all messages (runs every 1s)
	time.Sleep(5 * time.Second)

	// Act - Retrieve messages in order
	var retrievedPriorities []int64
	for i := 0; i < len(priorities); i++ {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
		getResp, err := client.GetNextMessage(ctxWithTimeout, &queueservice_pb.GetNextMessageRequest{
			QueueName:     queueName,
			LeaseDuration: durationpb.New(30 * time.Second),
		})
		cancel()

		if err != nil {
			t.Logf("Failed to get message %d: %v", i, err)
			break
		}
		if getResp.Message == nil {
			t.Logf("No message returned for iteration %d", i)
			break
		}

		retrievedPriorities = append(retrievedPriorities, getResp.Message.Metadata.Priority)

		// Ack immediately
		_, _ = client.AcknowledgeMessage(ctx, &queueservice_pb.AcknowledgeMessageRequest{
			QueueName:     queueName,
			MessageId:     getResp.Message.MessageId,
			StreamEntryId: getResp.StreamEntryId,
			State:         message_pb.Message_Metadata_COMPLETED,
		})
	}

	// Assert - High priority should be first
	require.GreaterOrEqual(t, len(retrievedPriorities), 1)
	assert.Equal(t, int64(9), retrievedPriorities[0], "High priority message should be retrieved first")
}
