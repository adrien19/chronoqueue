package repository

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"

	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
)

func (as *storage) streamKey(queueName string, priority int32) string {
	level := as.priorityLevel(priority)
	return fmt.Sprintf("chronoqueue:stream:%s:%s", level, urlEncode(queueName))
}

func (as *storage) priorityLevel(priority int32) string {
	if priority >= 70 {
		return "high"
	} else if priority >= 30 {
		return "medium"
	}
	return "low"
}

func (as *storage) scheduleKey(queueName string) string {
	return fmt.Sprintf("chronoqueue:schedule:%s", urlEncode(queueName))
}

func (as *storage) groupKey(queueName string) string {
	return fmt.Sprintf("chronoqueue:cg:%s", urlEncode(queueName))
}

func (as *storage) dlqStreamKey(queueName string) string {
	return fmt.Sprintf("chronoqueue:dlq:%s", urlEncode(queueName))
}

func (as *storage) addToSchedule(ctx context.Context, queueName, messageID string, scheduledTime int64) error {
	return as.redisClient.ZAdd(ctx, as.scheduleKey(queueName), redis.Z{
		Score:  float64(scheduledTime),
		Member: messageID,
	}).Err()
}

func (as *storage) ensureConsumerGroup(ctx context.Context, streamKey, groupKey string) error {
	err := as.redisClient.XGroupCreateMkStream(ctx, streamKey, groupKey, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

func (as *storage) ackMessage(ctx context.Context, queueName, streamEntryID string) error {
	priorities := []int32{100, 50, 10} // high, medium, low
	groupKey := as.groupKey(queueName)

	for _, priority := range priorities {
		streamKey := as.streamKey(queueName, priority)

		// XACK removes message from PEL (marks as processed by consumer group)
		ackCount, err := as.redisClient.XAck(ctx, streamKey, groupKey, streamEntryID).Result()

		// Only delete from the stream if XACK succeeded (message was in this stream)
		if err == nil && ackCount > 0 {
			// XDEL actually removes the message from the stream
			if err := as.redisClient.XDel(ctx, streamKey, streamEntryID).Err(); err != nil {
				as.logger.ErrorWithFields("Failed to delete message from stream", "streamKey", streamKey, "streamEntryID", streamEntryID, "error", err)
			}
			return nil
		}
	}

	return nil
}

func (as *storage) sendHeartbeat(ctx context.Context, queueName, consumerName, streamEntryID string) error {
	priorities := []int32{100, 50, 10} // high, medium, low
	groupKey := as.groupKey(queueName)

	for _, priority := range priorities {
		streamKey := as.streamKey(queueName, priority)

		_, err := as.redisClient.XClaim(ctx, &redis.XClaimArgs{
			Stream:   streamKey,
			Group:    groupKey,
			Consumer: consumerName,
			MinIdle:  0,
			Messages: []string{streamEntryID},
		}).Result()

		if err == nil {
			return nil
		}
	}

	return fmt.Errorf("failed to send heartbeat for entry %s", streamEntryID)
}

func (as *storage) claimStrict(ctx context.Context, queueName, consumerName string) (*redis.XMessage, error) {
	priorities := []int32{100, 50, 10} // high, medium, low

	for _, priority := range priorities {
		streamKey := as.streamKey(queueName, priority)
		groupKey := as.groupKey(queueName)

		// Ensure consumer group exists before attempting to read
		if err := as.ensureConsumerGroup(ctx, streamKey, groupKey); err != nil {
			as.logger.ErrorWithFields("Failed to ensure consumer group in claimStrict", "streamKey", streamKey, "error", err)
			continue
		}

		as.logger.InfoWithFields("Attempting XREADGROUP", "streamKey", streamKey, "groupKey", groupKey, "consumer", consumerName)

		// Check consumer group info to see if there are undelivered messages
		groups, err := as.redisClient.XInfoGroups(ctx, streamKey).Result()
		if err != nil {
			as.logger.InfoWithFields("XInfoGroups error (stream might not exist yet)", "streamKey", streamKey, "error", err.Error())
			continue
		}

		// Find our consumer group and check lag
		var hasUndelivered bool
		for _, group := range groups {
			if group.Name == groupKey {
				// Lag > 0 means there are messages that haven't been delivered to any consumer yet
				as.logger.InfoWithFields("Consumer group info", "streamKey", streamKey, "lag", group.Lag, "pending", group.Pending)
				if group.Lag > 0 {
					hasUndelivered = true
				}
				break
			}
		}

		if !hasUndelivered {
			as.logger.InfoWithFields("No undelivered messages in stream, skipping", "streamKey", streamKey)
			continue
		}

		messages, err := as.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupKey,
			Consumer: consumerName,
			Streams:  []string{streamKey, ">"},
			Count:    1,
			Block:    0, // Non-blocking
		}).Result()

		// Log result
		if err != nil {
			as.logger.InfoWithFields("XREADGROUP error", "streamKey", streamKey, "error", err.Error())
		} else if len(messages) == 0 {
			as.logger.InfoWithFields("XREADGROUP returned no stream results", "streamKey", streamKey)
		} else if len(messages[0].Messages) == 0 {
			as.logger.InfoWithFields("XREADGROUP returned empty messages array", "streamKey", streamKey)
		} else {
			as.logger.InfoWithFields("XREADGROUP success", "streamKey", streamKey, "messageID", messages[0].Messages[0].ID)
		}

		if err == nil && len(messages) > 0 && len(messages[0].Messages) > 0 {
			return &messages[0].Messages[0], nil
		}
	}

	as.logger.InfoWithFields("claimStrict returning nil - no messages found in any priority stream", "queueName", queueName)
	return nil, nil
}

func (as *storage) claimWeightedWithAging(ctx context.Context, queueName, consumerName string, config *queue_pb.PriorityConfig) (*redis.XMessage, error) {
	effectiveWeights := make(map[int32]int32)

	prioritiesMap := map[string]int32{"high": 100, "medium": 50, "low": 10}
	for priorityStr, priorityVal := range prioritiesMap {
		baseWeight := as.getBaseWeight(priorityStr, config)
		hasAgedMessages := as.hasAgedMessages(ctx, queueName, priorityStr, config.AgeBoostThreshold.AsDuration())

		if hasAgedMessages {
			effectiveWeights[priorityVal] = baseWeight * config.AgeBoostMultiplier
		} else {
			effectiveWeights[priorityVal] = baseWeight
		}
	}

	selectedPriority := as.weightedRandomSelect(effectiveWeights)

	streamKey := as.streamKey(queueName, selectedPriority)
	groupKey := as.groupKey(queueName)

	messages, err := as.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupKey,
		Consumer: consumerName,
		Streams:  []string{streamKey, ">"},
		Count:    1,
		Block:    0,
	}).Result()

	if err == nil && len(messages) > 0 && len(messages[0].Messages) > 0 {
		return &messages[0].Messages[0], nil
	}

	allPriorities := []int32{100, 50, 10} // high, medium, low
	for _, priority := range allPriorities {
		if priority == selectedPriority {
			continue
		}

		streamKey := as.streamKey(queueName, priority)
		messages, err := as.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupKey,
			Consumer: consumerName,
			Streams:  []string{streamKey, ">"},
			Count:    1,
			Block:    0,
		}).Result()

		if err == nil && len(messages) > 0 && len(messages[0].Messages) > 0 {
			return &messages[0].Messages[0], nil
		}
	}

	return nil, nil
}

func (as *storage) getBaseWeight(priority string, config *queue_pb.PriorityConfig) int32 {
	if config == nil || config.PriorityWeights == nil {
		switch priority {
		case "high":
			return 10
		case "medium":
			return 5
		default:
			return 1
		}
	}

	priorityLevel := int32(5)
	switch priority {
	case "high":
		priorityLevel = 9
	case "medium":
		priorityLevel = 5
	case "low":
		priorityLevel = 1
	}

	if weight, ok := config.PriorityWeights[priorityLevel]; ok {
		return weight
	}

	switch priority {
	case "high":
		return 10
	case "medium":
		return 5
	default:
		return 1
	}
}

func (as *storage) hasAgedMessages(ctx context.Context, queueName, priority string, threshold time.Duration) bool {
	priorityMap := map[string]int32{"high": 100, "medium": 50, "low": 10}
	priorityVal := priorityMap[priority]
	streamKey := as.streamKey(queueName, priorityVal)
	groupKey := as.groupKey(queueName)

	pending, err := as.redisClient.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  groupKey,
		Start:  "-",
		End:    "+",
		Count:  1,
	}).Result()

	if err != nil || len(pending) == 0 {
		return false
	}

	return pending[0].Idle >= threshold
}

func (as *storage) weightedRandomSelect(weights map[int32]int32) int32 {
	if len(weights) == 0 {
		return 50 // medium
	}

	var totalWeight int32
	for _, weight := range weights {
		totalWeight += weight
	}

	if totalWeight == 0 {
		return 100 // high
	}

	randValue := rand.Int31n(totalWeight)

	var cumulative int32
	for _, item := range []struct {
		priority int32
		weight   int32
	}{
		{100, weights[100]}, // high
		{50, weights[50]},   // medium
		{10, weights[10]},   // low
	} {
		cumulative += item.weight
		if randValue < cumulative {
			return item.priority
		}
	}

	return 100 // high
}
