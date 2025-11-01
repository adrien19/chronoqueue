package repository

import (
	"context"
	"fmt"
	"time"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
)

// GetDLQMessages retrieves messages from a Dead Letter Queue (stream-based)
func (as *storage) GetDLQMessages(ctx context.Context, dlqName string, limit int32) ([]*message_pb.Message, error) {
	dlqStream := as.dlqStreamKey(dlqName)

	streamMessages, err := as.redisClient.XRange(ctx, dlqStream, "-", "+").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch DLQ messages: %w", err)
	}

	if limit > 0 && int32(len(streamMessages)) > limit {
		streamMessages = streamMessages[:limit]
	}

	messages := make([]*message_pb.Message, 0, len(streamMessages))
	for _, streamMsg := range streamMessages {
		messageID, ok := streamMsg.Values["message_id"].(string)
		if !ok {
			continue
		}

		originalQueue, _ := streamMsg.Values["original_queue"].(string)
		failureReason, _ := streamMsg.Values["failure_reason"].(string)
		deliveryCountStr, _ := streamMsg.Values["delivery_count"].(string)

		meta := &message_pb.Message_Metadata{
			State:        2,
			MaxAttempts:  0,
			AttemptsLeft: 0,
		}

		message := &message_pb.Message{
			MessageId: messageID,
			Metadata:  meta,
		}

		as.logger.DebugWithFields("DLQ message", "messageID", messageID, "originalQueue", originalQueue, "reason", failureReason, "deliveryCount", deliveryCountStr)
		messages = append(messages, message)
	}

	return messages, nil
}

// RequeueFromDLQ moves a message from DLQ back to its original queue or specified target queue (stream-based)
func (as *storage) RequeueFromDLQ(ctx context.Context, dlqName string, messageID string, targetQueueName string, resetRetries bool) error {
	dlqStream := as.dlqStreamKey(dlqName)

	streamMessages, err := as.redisClient.XRange(ctx, dlqStream, "-", "+").Result()
	if err != nil {
		return fmt.Errorf("failed to read DLQ stream: %w", err)
	}

	var entryID string
	var found bool
	for _, msg := range streamMessages {
		if msgID, ok := msg.Values["message_id"].(string); ok && msgID == messageID {
			entryID = msg.ID
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("message '%s' not found in DLQ '%s'", messageID, dlqName)
	}

	targetExists, err := as.checkQueueExistence(ctx, targetQueueName)
	if err != nil {
		return fmt.Errorf("failed to check target queue existence: %w", err)
	}
	if !targetExists {
		return fmt.Errorf("target queue '%s' does not exist", targetQueueName)
	}

	targetQueueMeta, err := as.GetQueueMetadata(ctx, targetQueueName)
	if err != nil {
		return fmt.Errorf("failed to get target queue metadata: %w", err)
	}

	metadata := &message_pb.Message_Metadata{
		State:        0,
		MaxAttempts:  targetQueueMeta.DefaultMaxAttempts,
		AttemptsLeft: targetQueueMeta.DefaultMaxAttempts,
		Priority:     5,
	}

	if resetRetries {
		metadata.AttemptsLeft = metadata.MaxAttempts
	}

	scheduledTime := time.Now().UnixMilli()
	if err := as.addToSchedule(ctx, targetQueueName, messageID, scheduledTime); err != nil {
		return fmt.Errorf("failed to add message to schedule: %w", err)
	}

	if err := as.saveMessageMetadata(ctx, targetQueueName, messageID, metadata); err != nil {
		return fmt.Errorf("failed to save message metadata: %w", err)
	}

	if err := as.redisClient.XDel(ctx, dlqStream, entryID).Err(); err != nil {
		return fmt.Errorf("failed to delete from DLQ stream: %w", err)
	}

	as.logger.InfoWithFields("Message requeued from DLQ", "dlqName", dlqName, "targetQueue", targetQueueName, "messageID", messageID)
	return nil
}

// DeleteFromDLQ permanently deletes a message from a DLQ (stream-based)
func (as *storage) DeleteFromDLQ(ctx context.Context, dlqName string, messageID string) error {
	dlqStream := as.dlqStreamKey(dlqName)

	streamMessages, err := as.redisClient.XRange(ctx, dlqStream, "-", "+").Result()
	if err != nil {
		return fmt.Errorf("failed to read DLQ stream: %w", err)
	}

	var entryID string
	var found bool
	for _, msg := range streamMessages {
		if msgID, ok := msg.Values["message_id"].(string); ok && msgID == messageID {
			entryID = msg.ID
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("message '%s' not found in DLQ '%s'", messageID, dlqName)
	}

	if err := as.redisClient.XDel(ctx, dlqStream, entryID).Err(); err != nil {
		return fmt.Errorf("failed to delete from DLQ stream: %w", err)
	}

	as.logger.InfoWithFields("Message deleted from DLQ", "dlqName", dlqName, "messageID", messageID)
	return nil
}

// PurgeDLQ removes all messages from a DLQ (stream-based)
func (as *storage) PurgeDLQ(ctx context.Context, dlqName string) error {
	dlqStream := as.dlqStreamKey(dlqName)

	streamMessages, err := as.redisClient.XRange(ctx, dlqStream, "-", "+").Result()
	if err != nil {
		return fmt.Errorf("failed to read DLQ stream: %w", err)
	}

	deletedCount := 0
	for _, msg := range streamMessages {
		if err := as.redisClient.XDel(ctx, dlqStream, msg.ID).Err(); err != nil {
			as.logger.ErrorWithFields("Failed to delete message during DLQ purge", "dlq", dlqName, "entryID", msg.ID, "error", err)
			continue
		}
		deletedCount++
	}

	as.logger.InfoWithFields("Purged DLQ", "dlq", dlqName, "deletedCount", deletedCount, "totalMessages", len(streamMessages))
	return nil
}

// GetDLQStats returns statistics about a DLQ (stream-based)
func (as *storage) GetDLQStats(ctx context.Context, dlqName string) (*DLQStats, error) {
	dlqStream := as.dlqStreamKey(dlqName)

	info, err := as.redisClient.XInfoStream(ctx, dlqStream).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get DLQ stream info: %w", err)
	}

	stats := &DLQStats{
		Name:         dlqName,
		MessageCount: info.Length,
		CreatedAt:    0,
		UpdatedAt:    0,
	}

	return stats, nil
}
