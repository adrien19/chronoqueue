package repository

import (
	"context"
	"fmt"
	"strconv"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
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
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error while fetching message IDs")
		return nil, chronoErr.GRPCStatus()
	}

	messages := make([]*chronoqueue.Message, len(messageIDs))
	for i, messageID := range messageIDs {
		metadata, err := as.fetchMessageMetadata(ctx, queueName, messageID)
		if err != nil {
			msg := fmt.Sprintf("error fetching metadata for message %s", messageID)
			chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, msg)
			return nil, chronoErr.GRPCStatus()
		}

		messages[i] = &chronoqueue.Message{
			MessageId: messageID,
			Priority:  0, // TODO: This seems to be hardcoded for now, should be updated.
			Metadata:  metadata,
		}
	}

	return &chronoqueue.PeekQueueMessagesResponse{Messages: messages}, nil
}
