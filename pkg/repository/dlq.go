package repository

import (
	"context"
	"fmt"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
)

const (
	DLQ_MESSAGE_STATE_KEY = "dlq_messages"
)

// requeueFromDLQ moves a message from DLQ back to its original queue or a specified queue
func (as *storage) requeueFromDLQ(ctx context.Context, dlqName string, messageID string, targetQueueName string, resetRetries bool) error {
	// Get the message from DLQ
	dlqMessageKey := fmt.Sprintf("%s:%s:meta", dlqName, messageID)
	metadata, err := as.fetchMessageMetadata(ctx, dlqName, messageID)
	if err != nil {
		return fmt.Errorf("failed to fetch DLQ message metadata: %w", err)
	}

	// Prepare message for requeue
	if resetRetries {
		// Get target queue metadata to reset retry counts
		targetQueueMeta, err := as.GetQueueMetadata(ctx, targetQueueName)
		if err != nil {
			return fmt.Errorf("failed to get target queue metadata: %w", err)
		}

		// Reset retry attempts
		if metadata.MaxAttempts == 0 {
			metadata.MaxAttempts = targetQueueMeta.DefaultMaxAttempts
		}
		metadata.AttemptsLeft = metadata.MaxAttempts
	}

	// Reset state for requeue
	metadata.State = message_pb.Message_Metadata_PENDING
	metadata.LeaseExpiry = 0
	metadata.LeaseRenewalCount = 0

	// Create the message in target queue
	message := &message_pb.Message{
		MessageId: messageID,
		Metadata:  metadata,
	}

	requeueRequest := &queueservice_pb.PostMessageRequest{
		QueueName: targetQueueName,
		Message:   message,
	}

	_, err = as.CreateQueueMessage(ctx, requeueRequest, nil)
	if err != nil {
		return fmt.Errorf("failed to requeue message to %s: %w", targetQueueName, err)
	}

	// Remove from DLQ
	txPipeline := as.redisClient.TxPipeline()

	// Remove from DLQ queue
	prefixedDLQName := "queue:" + dlqName
	_, err = txPipeline.ZRem(ctx, prefixedDLQName, messageID).Result()
	if err != nil {
		return fmt.Errorf("failed to remove from DLQ queue: %w", err)
	}

	// Remove from DLQ state index
	_, err = txPipeline.ZRem(ctx, DLQ_MESSAGE_STATE_KEY, dlqMessageKey).Result()
	if err != nil {
		return fmt.Errorf("failed to remove from DLQ state index: %w", err)
	}

	// Remove DLQ message metadata
	_, err = txPipeline.Del(ctx, dlqMessageKey).Result()
	if err != nil {
		return fmt.Errorf("failed to delete DLQ message metadata: %w", err)
	}

	// Execute transaction
	_, err = txPipeline.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute requeue transaction: %w", err)
	}

	as.logger.InfoWithFields(
		"Message requeued from DLQ",
		"dlqName", dlqName,
		"targetQueue", targetQueueName,
		"messageId", messageID,
		"resetRetries", resetRetries,
	)

	return nil
}
