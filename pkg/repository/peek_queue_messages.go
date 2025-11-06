package repository

import (
	"context"
	"fmt"

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
	priorities := []int32{100, 50, 10} // high, medium, low
	remaining := limit

	// Query streams in priority order (high to low)
	for _, priority := range priorities {
		if remaining <= 0 {
			break
		}

		streamKey := as.streamKey(queueName, priority)

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
				// Filter by priority range if specified
				if priorityRange != nil {
					// Try to get priority value from stream entry
					// Redis stores numbers as strings in stream entries
					var priority int64
					priorityFound := false
					switch v := entry.Values["priority"].(type) {
					case string:
						if _, err := fmt.Sscanf(v, "%d", &priority); err == nil {
							priorityFound = true
						}
					case int64:
						priority = v
						priorityFound = true
					case int:
						priority = int64(v)
						priorityFound = true
					case float64:
						priority = int64(v)
						priorityFound = true
					}

					if priorityFound {
						// Check if priority falls within the requested range
						if priority < priorityRange.Min || priority > priorityRange.Max {
							continue
						}
					}
				}
				messageIDs = append(messageIDs, msgID)
				remaining--
			}
		}
	}

	// Also check scheduled messages if there's remaining capacity
	if remaining > 0 {
		scheduleKey := as.scheduleKey(queueName)
		// Get more messages than needed in case some are filtered out
		scheduledMsgIDs, err := as.redisClient.ZRange(ctx, scheduleKey, 0, -1).Result()
		if err == nil {
			// Filter scheduled messages by priority range if specified
			for _, msgID := range scheduledMsgIDs {
				if remaining <= 0 {
					break
				}

				// If priority range filtering is enabled, check the message metadata
				if priorityRange != nil {
					metadata, err := as.fetchMessageMetadata(ctx, queueName, msgID)
					if err != nil {
						continue
					}

					priority := metadata.Priority
					if priority < priorityRange.Min || priority > priorityRange.Max {
						continue
					}
				}

				messageIDs = append(messageIDs, msgID)
				remaining--
			}
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
