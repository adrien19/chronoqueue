package repository

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/util"
)

// Fetches message IDs from the sorted set in Redis based on the priority range.
// With the new scoring system: score = (MaxPriority - priority) * 1e10 + timestamp
func (as *storage) fetchMessageIDs(ctx context.Context, queueName string, priorityRange *queueservice_pb.PeekQueueMessagesRequest_PriorityRange, limit int64) ([]string, error) {
	prefixedQueueName := "queue:" + queueName

	if priorityRange != nil {
		// Convert priority range to score range
		// Higher priority (e.g., 100) -> Lower score (e.g., 0 * 1e10)
		// Lower priority (e.g., 0) -> Higher score (e.g., 100 * 1e10)
		minPriority := priorityRange.GetMin()
		maxPriority := priorityRange.GetMax()

		// Convert to score bounds (note the inversion)
		minScore := strconv.FormatInt((MaxPriority-maxPriority)*1e10, 10)
		maxScore := strconv.FormatInt((MaxPriority-minPriority)*1e10+1e10-1, 10) // Include all timestamps for this priority

		return as.redisClient.ZRangeByScore(ctx, prefixedQueueName, &redis.ZRangeBy{
			Min:    minScore,
			Max:    maxScore,
			Offset: 0,
			Count:  limit,
		}).Result()
	}

	// No priority filter - get all messages ordered by priority
	return as.redisClient.ZRange(ctx, prefixedQueueName, 0, limit-1).Result()
}

func (as *storage) PeekQueueMessages(ctx context.Context, request *queueservice_pb.PeekQueueMessagesRequest) (*queueservice_pb.PeekQueueMessagesResponse, error) {
	queueName := request.GetQueueName()
	messageIDs, err := as.fetchMessageIDs(ctx, queueName, request.PriorityRange, request.GetLimit())
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error while fetching message IDs")
		return nil, chronoErr.GRPCStatus()
	}
	messageIDs = util.FilterEmptyStrings(messageIDs)

	messages := make([]*message_pb.Message, len(messageIDs))
	for i, messageID := range messageIDs {
		metadata, err := as.fetchMessageMetadata(ctx, queueName, messageID)
		if err != nil {
			msg := fmt.Sprintf("error fetching metadata for message %s", messageID)
			chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, msg)
			return nil, chronoErr.GRPCStatus()
		}

		messages[i] = &message_pb.Message{
			MessageId: messageID,
			Metadata:  metadata,
		}
	}

	return &queueservice_pb.PeekQueueMessagesResponse{Messages: messages}, nil
}
