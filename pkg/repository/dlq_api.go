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
	var originalQueue string
	var found bool
	for _, msg := range streamMessages {
		if msgID, ok := msg.Values["message_id"].(string); ok && msgID == messageID {
			entryID = msg.ID
			originalQueue, _ = msg.Values["original_queue"].(string)
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("message '%s' not found in DLQ '%s'", messageID, dlqName)
	}

	if originalQueue == "" {
		return fmt.Errorf("original queue not found for message '%s' in DLQ '%s'", messageID, dlqName)
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

	// Fetch existing metadata from original queue (contains payload)
	existingMetadata, err := as.fetchMessageMetadata(ctx, originalQueue, messageID)
	if err != nil {
		return fmt.Errorf("failed to fetch existing message metadata from original queue: %w", err)
	}

	// Reset state and retry counters for requeue
	existingMetadata.State = message_pb.Message_Metadata_PENDING
	existingMetadata.MaxAttempts = targetQueueMeta.DefaultMaxAttempts
	existingMetadata.AttemptsLeft = targetQueueMeta.DefaultMaxAttempts

	if resetRetries {
		existingMetadata.AttemptsLeft = existingMetadata.MaxAttempts
	}

	// Clear all attempt runtime state for fresh start
	// Note: CurrentAttempt contains new fields (attempt_id, worker_id, lease_started_at, etc.)
	// while LeaseExpiry/LeaseRenewalCount are legacy top-level fields for backward compatibility
	existingMetadata.CurrentAttempt = nil
	existingMetadata.LeaseExpiry = 0
	existingMetadata.LeaseRenewalCount = 0

	// Preserve LeasePolicy (configuration, not runtime state)
	// LeasePolicy defines how leases work (base_lease, max_extension, heartbeat_timeout)
	// and should be retained when requeuing

	scheduledTime := time.Now().UnixMilli()
	if err := as.addToSchedule(ctx, targetQueueName, messageID, scheduledTime); err != nil {
		return fmt.Errorf("failed to add message to schedule: %w", err)
	}

	// Save to target queue (may be different from original queue)
	if err := as.saveMessageMetadata(ctx, targetQueueName, messageID, existingMetadata); err != nil {
		return fmt.Errorf("failed to save message metadata: %w", err)
	}

	// Delete from original queue if target is different
	if targetQueueName != originalQueue {
		originalMetaKey := as.messageMetaKey(originalQueue, messageID)
		if err := as.redisClient.Del(ctx, originalMetaKey).Err(); err != nil {
			as.logger.WarnWithFields("Failed to delete original queue metadata", "queue", originalQueue, "messageID", messageID, "error", err)
		}
	}

	if err := as.redisClient.XDel(ctx, dlqStream, entryID).Err(); err != nil {
		return fmt.Errorf("failed to delete from DLQ stream: %w", err)
	}

	as.logger.InfoWithFields("Message requeued from DLQ", "dlqName", dlqName, "originalQueue", originalQueue, "targetQueue", targetQueueName, "messageID", messageID)
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
