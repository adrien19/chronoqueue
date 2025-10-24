package repository

import (
	"context"
	"fmt"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
)

// GetDLQMessages retrieves messages from a Dead Letter Queue
func (as *storage) GetDLQMessages(ctx context.Context, dlqName string, limit int32) ([]*message_pb.Message, error) {
	// Check if DLQ exists
	exists, err := as.checkQueueExistence(ctx, dlqName)
	if err != nil {
		return nil, fmt.Errorf("failed to check DLQ existence: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("DLQ '%s' does not exist", dlqName)
	}

	// Fetch message IDs from DLQ
	messageIDs, err := as.fetchQueueMembersByPriority(ctx, dlqName, int64(limit))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch DLQ messages: %w", err)
	}

	// Fetch message metadata for each ID
	messages := make([]*message_pb.Message, 0, len(messageIDs))
	for _, messageID := range messageIDs {
		if messageID == "" {
			continue // Skip empty message IDs
		}

		metadata, err := as.fetchMessageMetadata(ctx, dlqName, messageID)
		if err != nil {
			as.logger.ErrorWithFields("Failed to fetch DLQ message metadata", "dlq", dlqName, "messageId", messageID, "error", err)
			continue // Skip this message but continue with others
		}

		if metadata != nil {
			message := &message_pb.Message{
				MessageId: messageID,
				Metadata:  metadata,
			}
			messages = append(messages, message)
		}
	}

	return messages, nil
}

// RequeueFromDLQ moves a message from DLQ back to its original queue or specified target queue
func (as *storage) RequeueFromDLQ(ctx context.Context, dlqName string, messageID string, targetQueueName string, resetRetries bool) error {
	// Check if message exists in DLQ
	metadata, err := as.fetchMessageMetadata(ctx, dlqName, messageID)
	if err != nil {
		return fmt.Errorf("failed to fetch DLQ message: %w", err)
	}
	if metadata == nil {
		return fmt.Errorf("message '%s' not found in DLQ '%s'", messageID, dlqName)
	}

	// Check if target queue exists
	targetExists, err := as.checkQueueExistence(ctx, targetQueueName)
	if err != nil {
		return fmt.Errorf("failed to check target queue existence: %w", err)
	}
	if !targetExists {
		return fmt.Errorf("target queue '%s' does not exist", targetQueueName)
	}

	// Use the requeue helper function
	return as.requeueFromDLQ(ctx, dlqName, messageID, targetQueueName, resetRetries)
}

// DeleteFromDLQ permanently deletes a message from a DLQ
func (as *storage) DeleteFromDLQ(ctx context.Context, dlqName string, messageID string) error {
	// Check if message exists in DLQ
	metadata, err := as.fetchMessageMetadata(ctx, dlqName, messageID)
	if err != nil {
		return fmt.Errorf("failed to fetch DLQ message: %w", err)
	}
	if metadata == nil {
		return fmt.Errorf("message '%s' not found in DLQ '%s'", messageID, dlqName)
	}

	// Use existing delete functionality
	return as.DeleteQueueMessage(ctx, dlqName, messageID)
}

// PurgeDLQ removes all messages from a DLQ
func (as *storage) PurgeDLQ(ctx context.Context, dlqName string) error {
	// Check if DLQ exists
	exists, err := as.checkQueueExistence(ctx, dlqName)
	if err != nil {
		return fmt.Errorf("failed to check DLQ existence: %w", err)
	}
	if !exists {
		return fmt.Errorf("DLQ '%s' does not exist", dlqName)
	}

	// Get all message IDs from DLQ
	messageIDs, err := as.fetchQueueMembersByPriority(ctx, dlqName, 10000) // Large limit to get all
	if err != nil {
		return fmt.Errorf("failed to fetch DLQ messages for purging: %w", err)
	}

	// Delete each message
	deletedCount := 0
	deletedMembers := make([]string, 0, len(messageIDs))
	for _, messageID := range messageIDs {
		if messageID == "" {
			continue
		}
		prefixedMessageID := dlqName + ":" + messageID + ":meta"
		err := as.DeleteQueueMessage(ctx, dlqName, prefixedMessageID)
		if err != nil {
			as.logger.ErrorWithFields("Failed to delete message during DLQ purge", "dlq", dlqName, "messageId", messageID, "error", err)
			continue // Continue purging other messages
		}
		deletedMembers = append(deletedMembers, messageID)
		deletedCount++
	}

	// Remove all deleted members from the DLQ sorted set
	if len(deletedMembers) > 0 {
		err = as.removeQueueMembersByPriority(ctx, dlqName, deletedMembers)
		if err != nil {
			return fmt.Errorf("failed to remove messages from DLQ sorted set: %w", err)
		}
	}

	as.logger.InfoWithFields(
		"Purged DLQ",
		"dlq", dlqName,
		"deletedCount", deletedCount,
		"totalMessages", len(messageIDs),
	)

	return nil
}

// GetDLQStats returns statistics about a DLQ
func (as *storage) GetDLQStats(ctx context.Context, dlqName string) (*DLQStats, error) {
	// Check if DLQ exists
	exists, err := as.checkQueueExistence(ctx, dlqName)
	if err != nil {
		return nil, fmt.Errorf("failed to check DLQ existence: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("DLQ '%s' does not exist", dlqName)
	}

	// Get queue state
	stateRequest := &queueservice_pb.GetQueueStateRequest{
		QueueName: dlqName,
	}
	stateResponse, err := as.GetQueueState(ctx, stateRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to get DLQ state: %w", err)
	}

	// Calculate total message count from state counts
	var totalMessages int32
	for _, count := range stateResponse.StateCounts {
		totalMessages += count
	}

	stats := &DLQStats{
		Name:         dlqName,
		MessageCount: int64(totalMessages),
		CreatedAt:    0, // Not available in current API
		UpdatedAt:    0, // Not available in current API
	}

	return stats, nil
}
