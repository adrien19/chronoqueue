//go:build test_dep

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"

	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/pkg/log"
)

// setupRedisForTest creates a miniredis instance for testing
func setupRedisForTest() (*miniredis.Miniredis, *redis.Client) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	client := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	return s, client
}

// TestHeartbeatUpdatesRunningMessagesScore validates that heartbeat updates
// the sorted set score atomically (Fix #1)
func TestHeartbeatUpdatesRunningMessagesScore(t *testing.T) {
	ctx := context.Background()
	miniRedis, redisClient := setupRedisForTest()
	defer miniRedis.Close()
	defer redisClient.Close()

	logger := log.NewLogger()
	storageInterface := NewQueueStorageForTesting(redisClient, nil, logger)
	storageImpl := storageInterface.(*storage)

	redisClient.FlushAll(ctx)

	queueName := "test-heartbeat-queue"
	messageID := "test-heartbeat-msg"

	// Create queue
	queueMeta := &queue_pb.QueueMetadata{
		Type:               queue_pb.QueueType_SIMPLE,
		DefaultMaxAttempts: 3,
		LeaseDuration:      durationpb.New(5 * time.Second),
	}
	_, err := storageInterface.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name:     queueName,
		Metadata: queueMeta,
	})
	require.NoError(t, err)

	// Manually create message in PENDING state to avoid INVISIBLE state complexity
	testMessageKey := queueName + ":" + messageID + ":meta"
	testMessage := &message_pb.Message{
		MessageId: messageID,
		Metadata: &message_pb.Message_Metadata{
			Payload:  &common_pb.Payload{},
			State:    message_pb.Message_Metadata_PENDING,
			Priority: 0,
		},
	}

	// Serialize and store the message metadata
	metadataJSON, err := protojson.Marshal(testMessage.Metadata)
	require.NoError(t, err)
	err = redisClient.HSet(ctx, testMessageKey, "metadata", metadataJSON).Err()
	require.NoError(t, err)

	// Add message to the queue sorted set with priority score
	priorityScore := float64(100 * 1e10)
	err = redisClient.ZAdd(ctx, "queue:"+queueName, redis.Z{
		Score:  priorityScore,
		Member: messageID,
	}).Err()
	require.NoError(t, err)

	// Get the message (transitions to RUNNING and adds to running_messages)
	getResp, err := storageInterface.GetQueueMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(5 * time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.Message)

	initialLeaseExpiry := getResp.Message.Metadata.LeaseExpiry

	// Get initial score from running_messages using internal storage type
	messageKey := queueName + ":" + messageID + ":meta"
	initialScore, err := storageImpl.redisClient.ZScore(ctx, "running_messages", messageKey).Result()
	require.NoError(t, err)
	assert.Equal(t, float64(initialLeaseExpiry), initialScore, "Initial score should match lease expiry")

	// Wait to ensure lease is near expiration threshold
	time.Sleep(4500 * time.Millisecond) // Wait 4.5 seconds (90% of 5s lease)

	// Send heartbeat (should renew lease and UPDATE sorted set score)
	heartbeatResp, err := storageInterface.SendMessageHeartBeat(ctx, &queueservice_pb.SendMessageHeartBeatRequest{
		QueueName: queueName,
		MessageId: messageID,
	})
	require.NoError(t, err)
	assert.NotNil(t, heartbeatResp)

	// Fetch updated metadata
	updatedMeta, err := storageImpl.fetchMessageMetadata(ctx, queueName, messageID)
	require.NoError(t, err)
	assert.Greater(t, updatedMeta.LeaseExpiry, initialLeaseExpiry, "Lease should be extended")
	assert.Equal(t, int32(1), updatedMeta.LeaseRenewalCount, "Renewal count should increment")

	// CRITICAL TEST: Verify sorted set score was updated
	updatedScore, err := storageImpl.redisClient.ZScore(ctx, "running_messages", messageKey).Result()
	require.NoError(t, err)
	assert.Equal(t, float64(updatedMeta.LeaseExpiry), updatedScore, "Score should be updated to new lease expiry")
	assert.Greater(t, updatedScore, initialScore, "New score should be greater than initial score")

	// Verify message is still in running_messages
	exists, err := storageImpl.redisClient.ZRank(ctx, "running_messages", messageKey).Result()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, exists, int64(0), "Message should still be in running_messages")
}

// TestHeartbeatWithMultipleRenewals validates that repeated heartbeats
// keep updating the score correctly
func TestHeartbeatWithMultipleRenewals(t *testing.T) {
	ctx := context.Background()
	miniRedis, redisClient := setupRedisForTest()
	defer miniRedis.Close()
	defer redisClient.Close()

	logger := log.NewLogger()
	storageInterface := NewQueueStorageForTesting(redisClient, nil, logger)
	storageImpl := storageInterface.(*storage)

	redisClient.FlushAll(ctx)

	queueName := "test-multi-heartbeat-queue"
	messageID := "test-multi-heartbeat-msg"

	// Create queue with short lease for faster testing
	queueMeta := &queue_pb.QueueMetadata{
		Type:               queue_pb.QueueType_SIMPLE,
		DefaultMaxAttempts: 3,
		LeaseDuration:      durationpb.New(2 * time.Second),
	}
	_, err := storageInterface.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name:     queueName,
		Metadata: queueMeta,
	})
	require.NoError(t, err)

	// Manually create message in PENDING state
	testMessageKey := queueName + ":" + messageID + ":meta"
	testMessage := &message_pb.Message{
		MessageId: messageID,
		Metadata: &message_pb.Message_Metadata{
			Payload:  &common_pb.Payload{},
			State:    message_pb.Message_Metadata_PENDING,
			Priority: 0,
		},
	}

	metadataJSON, err := protojson.Marshal(testMessage.Metadata)
	require.NoError(t, err)
	err = redisClient.HSet(ctx, testMessageKey, "metadata", metadataJSON).Err()
	require.NoError(t, err)

	priorityScore := float64(100 * 1e10)
	err = redisClient.ZAdd(ctx, "queue:"+queueName, redis.Z{
		Score:  priorityScore,
		Member: messageID,
	}).Err()
	require.NoError(t, err)

	getResp, err := storageInterface.GetQueueMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(2 * time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.Message)

	messageKey := queueName + ":" + messageID + ":meta"
	previousScore := float64(0)

	// Send multiple heartbeats and verify score updates each time
	for i := 0; i < 3; i++ {
		time.Sleep(1800 * time.Millisecond) // Wait 1.8s (90% of 2s lease)

		_, err := storageInterface.SendMessageHeartBeat(ctx, &queueservice_pb.SendMessageHeartBeatRequest{
			QueueName: queueName,
			MessageId: messageID,
		})
		require.NoError(t, err, "Heartbeat %d should succeed", i+1)

		currentScore, err := storageImpl.redisClient.ZScore(ctx, "running_messages", messageKey).Result()
		require.NoError(t, err, "Should get score for heartbeat %d", i+1)

		if i > 0 {
			assert.Greater(t, currentScore, previousScore, "Score should increase with each heartbeat (iteration %d)", i+1)
		}

		previousScore = currentScore

		// Verify metadata matches score
		meta, err := storageImpl.fetchMessageMetadata(ctx, queueName, messageID)
		require.NoError(t, err)
		assert.Equal(t, float64(meta.LeaseExpiry), currentScore, "Metadata and score should match (iteration %d)", i+1)
		assert.Equal(t, int32(i+1), meta.LeaseRenewalCount, "Renewal count should match iteration")
	}
}

// TestRunningToPendingRespectsUpdatedScore validates that the Lua script
// runningToPending respects the updated score from heartbeat
func TestRunningToPendingRespectsUpdatedScore(t *testing.T) {
	ctx := context.Background()
	miniRedis, redisClient := setupRedisForTest()
	defer miniRedis.Close()
	defer redisClient.Close()
	logger := log.NewLogger()
	storageInterface := NewQueueStorageForTesting(redisClient, nil, logger)
	storageImpl := storageInterface.(*storage)

	redisClient.FlushAll(ctx)

	queueName := "test-lua-score-queue"
	messageID := "test-lua-score-msg"

	// Create queue with very short lease
	queueMeta := &queue_pb.QueueMetadata{
		Type:               queue_pb.QueueType_SIMPLE,
		DefaultMaxAttempts: 3,
		LeaseDuration:      durationpb.New(1 * time.Second),
	}
	_, err := storageInterface.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name:     queueName,
		Metadata: queueMeta,
	})
	require.NoError(t, err)

	// Manually create message in PENDING state
	testMessageKey := queueName + ":" + messageID + ":meta"
	testMessage := &message_pb.Message{
		MessageId: messageID,
		Metadata: &message_pb.Message_Metadata{
			Payload:  &common_pb.Payload{},
			State:    message_pb.Message_Metadata_PENDING,
			Priority: 0,
		},
	}

	metadataJSON, err := protojson.Marshal(testMessage.Metadata)
	require.NoError(t, err)
	err = redisClient.HSet(ctx, testMessageKey, "metadata", metadataJSON).Err()
	require.NoError(t, err)

	priorityScore := float64(100 * 1e10)
	err = redisClient.ZAdd(ctx, "queue:"+queueName, redis.Z{
		Score:  priorityScore,
		Member: messageID,
	}).Err()
	require.NoError(t, err)

	getResp, err := storageInterface.GetQueueMessage(ctx, &queueservice_pb.GetNextMessageRequest{
		QueueName:     queueName,
		LeaseDuration: durationpb.New(1 * time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, getResp.Message)

	// Send heartbeat before original lease expires (extends lease)
	time.Sleep(900 * time.Millisecond) // 90% of 1s
	_, err = storageInterface.SendMessageHeartBeat(ctx, &queueservice_pb.SendMessageHeartBeatRequest{
		QueueName: queueName,
		MessageId: messageID,
	})
	require.NoError(t, err)

	// Original lease would have expired by now, but heartbeat extended it
	time.Sleep(500 * time.Millisecond)

	// Run ProcessExpiredRunningMessages (Lua script)
	err = storageInterface.ProcessExpiredRunningMessages(ctx)
	require.NoError(t, err)

	// Message should STILL be RUNNING (not transitioned to PENDING)
	meta, err := storageImpl.fetchMessageMetadata(ctx, queueName, messageID)
	require.NoError(t, err)
	assert.Equal(t, message_pb.Message_Metadata_RUNNING, meta.State, "Message should still be RUNNING after heartbeat extension")

	// Message should still be in running_messages
	messageKey := queueName + ":" + messageID + ":meta"
	exists, err := storageImpl.redisClient.ZRank(ctx, "running_messages", messageKey).Result()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, exists, int64(0), "Message should still be in running_messages")
}
