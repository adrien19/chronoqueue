package repository

import (
	"context"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (as *storage) SendMessageHeartBeat(ctx context.Context, request *chronoqueue.SendMessageHeartBeatRequest) (*chronoqueue.SendMessageHeartBeatResponse, error) {
	meta, err := as.fetchMessageMetadata(ctx, request.GetQueueName(), request.GetMessageId())
	if err != nil {
		return nil, err
	}
	if meta == nil {
		// ignore heartbeat requests for non-existing messages.
		return nil, nil
	}
	key := "heartbeat:" + request.GetQueueName() + ":" + request.GetMessageId()
	// Set expiration time to 3x the lease duration
	expiration := time.Duration(3 * meta.GetLeaseDuration().AsDuration())
	if expiration < time.Millisecond {
		expiration = time.Millisecond
	}

	// remaining_time := meta.GetLeaseExpiry() - time.Now().Unix()
	// Calculate the remaining time
	remainingSeconds := meta.GetLeaseExpiry() - time.Now().Unix()
	remainingTimeDuration := durationpb.New(time.Duration(remainingSeconds) * time.Second)

	_, err = as.redisClient.Set(context.Background(), key, time.Now().Unix(), expiration).Result()
	return &chronoqueue.SendMessageHeartBeatResponse{
		RemainingTime: remainingTimeDuration,
	}, err
}
