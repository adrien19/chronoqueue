package repository

import (
	"context"
	"fmt"
	"strconv"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/redis/go-redis/v9"
)

// Fetches message IDs from the sorted set in Redis based on the priority range.
func (as *storage) fetchMessageIDs(ctx context.Context, queueName string, priorityRange *chronoqueue.PeekQueueMessagesRequest_PriorityRange, limit int64) ([]string, error) {
	min := "-inf"
	max := "+inf"
	if priorityRange != nil {
		min = strconv.FormatInt(priorityRange.GetMin(), 10)
		max = strconv.FormatInt(priorityRange.GetMax(), 10)
	}
	return as.redisClient.ZRangeByScore(ctx, queueName, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: 0,
		Count:  limit,
	}).Result()
}

func (as *storage) PeekQueueMessages(ctx context.Context, request *chronoqueue.PeekQueueMessagesRequest) (*chronoqueue.PeekQueueMessagesResponse, error) {
	queueName := request.GetQueueName()
	messageIDs, err := as.fetchMessageIDs(ctx, queueName, request.PriorityRange, request.GetLimit())
	if err != nil {
		return nil, fmt.Errorf("error fetching message IDs: %v", err)
	}

	messages := make([]*chronoqueue.Message, len(messageIDs))
	for i, messageID := range messageIDs {
		metadata, err := as.fetchMessageMetadata(ctx, queueName, messageID)
		if err != nil {
			return nil, fmt.Errorf("error fetching metadata for message %s: %v", messageID, err)
		}

		messages[i] = &chronoqueue.Message{
			MessageId: messageID,
			Priority:  0, // TODO: This seems to be hardcoded for now, should be updated.
			Metadata:  metadata,
		}
	}

	return &chronoqueue.PeekQueueMessagesResponse{Messages: messages}, nil
}
