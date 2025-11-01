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
	return fmt.Sprintf("stream:%s:%s", level, queueName)
}

func (as *storage) priorityLevel(priority int32) string {
	if priority >= 8 {
		return "high"
	} else if priority >= 4 {
		return "medium"
	}
	return "low"
}

func (as *storage) scheduleKey(queueName string) string {
	return fmt.Sprintf("schedule:%s", queueName)
}

func (as *storage) groupKey(queueName string) string {
	return fmt.Sprintf("cg:%s", queueName)
}

func (as *storage) dlqStreamKey(queueName string) string {
	return fmt.Sprintf("dlq:%s", queueName)
}

func (as *storage) addToSchedule(ctx context.Context, queueName, messageID string, scheduledTime int64) error {
	return as.redisClient.ZAdd(ctx, as.scheduleKey(queueName), redis.Z{
		Score:  float64(scheduledTime),
		Member: messageID,
	}).Err()
}

func (as *storage) addToStream(ctx context.Context, queueName string, priority int32, messageID string, payload map[string]interface{}) (string, error) {
	streamKey := as.streamKey(queueName, priority)
	groupKey := as.groupKey(queueName)

	if err := as.ensureConsumerGroup(ctx, streamKey, groupKey); err != nil {
		return "", err
	}

	result := as.redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: payload,
	})

	return result.Val(), result.Err()
}

func (as *storage) ensureConsumerGroup(ctx context.Context, streamKey, groupKey string) error {
	err := as.redisClient.XGroupCreateMkStream(ctx, streamKey, groupKey, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

func (as *storage) claimFromStream(ctx context.Context, queueName, consumerName string, count int64) ([]redis.XMessage, error) {
	priorities := []string{"high", "medium", "low"}

	for _, priority := range priorities {
		streamKey := fmt.Sprintf("stream:%s:%s", priority, queueName)
		groupKey := as.groupKey(queueName)

		messages, err := as.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupKey,
			Consumer: consumerName,
			Streams:  []string{streamKey, ">"},
			Count:    count,
			Block:    0,
		}).Result()

		if err != nil && err != redis.Nil {
			return nil, err
		}

		if len(messages) > 0 && len(messages[0].Messages) > 0 {
			return messages[0].Messages, nil
		}
	}

	return nil, nil
}

func (as *storage) ackMessage(ctx context.Context, queueName, streamEntryID string) error {
	priorities := []string{"high", "medium", "low"}
	groupKey := as.groupKey(queueName)

	for _, priority := range priorities {
		streamKey := fmt.Sprintf("stream:%s:%s", priority, queueName)
		_ = as.redisClient.XAck(ctx, streamKey, groupKey, streamEntryID)
	}

	return nil
}

func (as *storage) sendHeartbeat(ctx context.Context, queueName, consumerName, streamEntryID string) error {
	priorities := []string{"high", "medium", "low"}
	groupKey := as.groupKey(queueName)

	for _, priority := range priorities {
		streamKey := fmt.Sprintf("stream:%s:%s", priority, queueName)

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
	priorities := []string{"high", "medium", "low"}

	for _, priority := range priorities {
		streamKey := fmt.Sprintf("stream:%s:%s", priority, queueName)
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
	}

	return nil, nil
}

func (as *storage) claimWeightedWithAging(ctx context.Context, queueName, consumerName string, config *queue_pb.PriorityConfig) (*redis.XMessage, error) {
	effectiveWeights := make(map[string]int32)

	priorities := []string{"high", "medium", "low"}
	for _, priority := range priorities {
		baseWeight := as.getBaseWeight(priority, config)
		hasAgedMessages := as.hasAgedMessages(ctx, queueName, priority, config.AgeBoostThreshold.AsDuration())

		if hasAgedMessages {
			effectiveWeights[priority] = baseWeight * config.AgeBoostMultiplier
		} else {
			effectiveWeights[priority] = baseWeight
		}
	}

	selectedPriority := as.weightedRandomSelect(effectiveWeights)

	streamKey := fmt.Sprintf("stream:%s:%s", selectedPriority, queueName)
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

	for _, priority := range priorities {
		if priority == selectedPriority {
			continue
		}

		streamKey := fmt.Sprintf("stream:%s:%s", priority, queueName)
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
	streamKey := fmt.Sprintf("stream:%s:%s", priority, queueName)
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

func (as *storage) weightedRandomSelect(weights map[string]int32) string {
	if len(weights) == 0 {
		return "medium"
	}

	var totalWeight int32
	for _, weight := range weights {
		totalWeight += weight
	}

	if totalWeight == 0 {
		return "high"
	}

	randValue := rand.Int31n(totalWeight)

	var cumulative int32
	for _, weight := range []struct {
		name   string
		weight int32
	}{
		{"high", weights["high"]},
		{"medium", weights["medium"]},
		{"low", weights["low"]},
	} {
		cumulative += weight.weight
		if randValue < cumulative {
			return weight.name
		}
	}

	return "high"
}

func (as *storage) moveToDLQ(ctx context.Context, queueName, messageID, streamEntryID, reason string) error {
	dlqStream := as.dlqStreamKey(queueName)

	meta, err := as.fetchMessageMetadata(ctx, queueName, messageID)
	if err != nil {
		return fmt.Errorf("failed to fetch message metadata: %w", err)
	}

	payload := map[string]interface{}{
		"message_id":       messageID,
		"original_queue":   queueName,
		"failure_reason":   reason,
		"delivery_count":   meta.MaxAttempts - meta.AttemptsLeft,
		"original_payload": meta.Payload,
		"timestamp":        time.Now().UnixMilli(),
	}

	_, err = as.redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: dlqStream,
		Values: payload,
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to add message to DLQ stream: %w", err)
	}

	meta.State = 2
	if err := as.saveMessageMetadata(ctx, queueName, messageID, meta); err != nil {
		return fmt.Errorf("failed to update message metadata: %w", err)
	}

	if streamEntryID != "" {
		if err := as.ackMessage(ctx, queueName, streamEntryID); err != nil {
			as.logger.ErrorWithFields("Failed to ack message after moving to DLQ", "error", err, "messageID", messageID)
		}
	}

	as.logger.InfoWithFields("Message moved to DLQ", "queueName", queueName, "messageID", messageID, "reason", reason)
	return nil
}
