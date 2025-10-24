package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/util"
)

func (as *storage) GetQueueState(ctx context.Context, request *queueservice_pb.GetQueueStateRequest) (*queueservice_pb.GetQueueStateResponse, error) {
	// Create or fetch the mutex for this specific queue
	queueMutex := as.rs.NewMutex("mutex:" + request.GetQueueName())

	// Try to acquire the lock
	if err := queueMutex.Lock(); err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected while acquiring lock")
		// chronoErr := util.NewServiceError("LOCK_ERROR", codes.Internal, err)
		return nil, chronoErr.GRPCStatus()
	}

	defer func() {
		// Release the message lock
		if ok, err := queueMutex.Unlock(); !ok || err != nil {
			as.logger.Error("Failed to release queue lock", err)
		}
	}()

	prefixedQueueName := "queue:" + request.GetQueueName()
	// Now we can safely compute the queue state as before
	membersWithScores, err := as.redisClient.ZRangeByScoreWithScores(ctx, prefixedQueueName, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: 0,
	}).Result()
	if err != nil {
		return &queueservice_pb.GetQueueStateResponse{}, err
	}
	if len(membersWithScores) <= 1 {
		return &queueservice_pb.GetQueueStateResponse{}, nil
	}

	stateCounts := make(map[message_pb.Message_Metadata_State]int32)
	var earliestDeadline time.Time

	for _, member := range membersWithScores {
		if len(member.Member.(string)) == 0 {
			continue
		}

		metadata, err := as.fetchMessageMetadata(ctx, request.GetQueueName(), member.Member.(string))
		if err != nil {
			msg := fmt.Sprintf("Unexpected error occured while fetching metadata for message %s", member.Member.(string))
			chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, msg)
			as.logger.DebugWithFields(msg, "err", chronoErr.Error())

			return nil, chronoErr.GRPCStatus()
		}

		stateCounts[metadata.GetState()] += 1

		// Calculate earliest deadline from message metadata
		if metadata.LeaseExpiry > 0 {
			leaseDeadline := time.Unix(0, metadata.LeaseExpiry*int64(time.Millisecond))
			if earliestDeadline.IsZero() || leaseDeadline.Before(earliestDeadline) {
				earliestDeadline = leaseDeadline
			}
		}
	}

	return &queueservice_pb.GetQueueStateResponse{
		StateCounts: map[string]int32{
			"INVISIBLE": stateCounts[message_pb.Message_Metadata_INVISIBLE],
			"PENDING":   stateCounts[message_pb.Message_Metadata_PENDING],
			"RUNNING":   stateCounts[message_pb.Message_Metadata_RUNNING],
			"COMPLETED": stateCounts[message_pb.Message_Metadata_COMPLETED],
			"CANCELED":  stateCounts[message_pb.Message_Metadata_CANCELED],
			"ERRORED":   stateCounts[message_pb.Message_Metadata_ERRORED],
		},
		EarliestDeadline: timestamppb.New(earliestDeadline),
	}, nil
}
