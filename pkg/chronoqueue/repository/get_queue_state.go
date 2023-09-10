package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (as *storage) GetQueueState(ctx context.Context, request *chronoqueue.GetQueueStateRequest) (*chronoqueue.GetQueueStateResponse, error) {
	// Create or fetch the mutex for this specific queue
	mutex := as.rs.NewMutex("mutex:" + request.GetQueueName())

	// Try to acquire the lock
	if err := mutex.Lock(); err != nil {
		return nil, fmt.Errorf("Failed to acquire lock: %v", err)
	}
	defer mutex.Unlock()

	// Now we can safely compute the queue state as before
	membersWithScores, err := as.redisClient.ZRangeByScoreWithScores(ctx, request.GetQueueName(), &redis.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: 0,
	}).Result()
	if err != nil {
		return &chronoqueue.GetQueueStateResponse{}, err
	}
	if len(membersWithScores) <= 1 {
		return &chronoqueue.GetQueueStateResponse{}, nil
	}

	// Assuming the first element of array is an empty string member
	earliestDeadline := time.Unix(0, int64(membersWithScores[1].Score)*int64(time.Millisecond))

	stateCounts := make(map[chronoqueue.Message_Metadata_State]int)

	for _, member := range membersWithScores {
		if len(member.Member.(string)) == 0 {
			continue
		}

		metadata, err := as.fetchMessageMetadata(ctx, request.GetQueueName(), member.Member.(string))
		if err != nil {
			return nil, fmt.Errorf("error fetching metadata for message %s: %v", member.Member.(string), err)
		}

		stateCounts[metadata.GetState()] += 1
	}

	return &chronoqueue.GetQueueStateResponse{
		InvisibleMessagesCount: int32(stateCounts[chronoqueue.Message_Metadata_INVISIBLE]),
		PendingMessagesCount:   int32(stateCounts[chronoqueue.Message_Metadata_PENDING]),
		RunningMessagesCount:   int32(stateCounts[chronoqueue.Message_Metadata_RUNNING]),
		CompletedMessagesCount: int32(stateCounts[chronoqueue.Message_Metadata_COMPLETED]),
		CanceledMessagesCount:  int32(stateCounts[chronoqueue.Message_Metadata_CANCELED]),
		ErroredMessagesCount:   int32(stateCounts[chronoqueue.Message_Metadata_ERRORED]),
		EarliestDeadline:       timestamppb.New(earliestDeadline),
	}, nil
}
