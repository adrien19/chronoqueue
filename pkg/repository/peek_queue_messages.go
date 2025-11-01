package repository

import (
	"context"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/util"
)

// fetchMessageIDsFromStreams retrieves message IDs from Redis Streams based on priority range
func (as *storage) fetchMessageIDsFromStreams(ctx context.Context, queueName string, priorityRange *queueservice_pb.PeekQueueMessagesRequest_PriorityRange, limit int64) ([]string, error) {
	var messageIDs []string

	// Query priority streams - use priority levels (high, medium, low) instead of individual priorities
	priorityLevels := []string{"high", "medium", "low"}
	remaining := limit

	// Query streams in priority order (high to low)
	for _, level := range priorityLevels {
		if remaining <= 0 {
			break
		}

		streamKey := "stream:" + level + ":" + queueName

		// Use XRANGE to peek at messages without consuming
		entries, err := as.redisClient.XRange(ctx, streamKey, "-", "+").Result()
		if err != nil && err != redis.Nil {
			continue
		}

		for _, entry := range entries {
			if remaining <= 0 {
				break
			}
			if msgID, ok := entry.Values["message_id"].(string); ok {
				messageIDs = append(messageIDs, msgID)
				remaining--
			}
		}
	}

	// Also check scheduled messages if there's remaining capacity
	if remaining > 0 {
		scheduleKey := as.scheduleKey(queueName)
		scheduledMsgIDs, err := as.redisClient.ZRange(ctx, scheduleKey, 0, remaining-1).Result()
		if err == nil {
			messageIDs = append(messageIDs, scheduledMsgIDs...)
		}
	}

	return messageIDs, nil
}

func (as *storage) PeekQueueMessages(ctx context.Context, request *queueservice_pb.PeekQueueMessagesRequest) (*queueservice_pb.PeekQueueMessagesResponse, error) {
	queueName := request.GetQueueName()
	limit := request.GetLimit()
	if limit <= 0 {
		limit = 10 // Default limit
	}

	messageIDs, err := as.fetchMessageIDsFromStreams(ctx, queueName, request.PriorityRange, limit)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error while fetching message IDs")
		return nil, chronoErr.GRPCStatus()
	}
	messageIDs = util.FilterEmptyStrings(messageIDs)

	messages := make([]*message_pb.Message, 0, len(messageIDs))
	for _, messageID := range messageIDs {
		metadata, err := as.fetchMessageMetadata(ctx, queueName, messageID)
		if err != nil {
			as.logger.DebugWithFields("Failed to fetch metadata for message", "messageID", messageID, "error", err)
			continue
		}

		messages = append(messages, &message_pb.Message{
			MessageId: messageID,
			Metadata:  metadata,
		})
	}

	return &queueservice_pb.PeekQueueMessagesResponse{Messages: messages}, nil
}
