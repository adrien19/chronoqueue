package repository

import (
	"context"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
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
	expiration := time.Duration(3 * meta.GetLeaseDuration())
	if expiration < time.Millisecond {
		expiration = time.Millisecond
	}

	remaining_time := meta.GetLeaseExpiry() - time.Now().Unix()

	_, err = as.redisClient.Set(context.Background(), key, time.Now().Unix(), expiration).Result()
	return &chronoqueue.SendMessageHeartBeatResponse{
		RemainingTime: remaining_time,
	}, err
}

// func (as *storage) lastHeartbeat(queueName, messageID string) (time.Time, error) {
// 	key := "heartbeat:" + queueName + ":" + messageID
// 	result, err := as.redisClient.Get(context.Background(), key).Int64()
// 	if err != nil {
// 		return time.Time{}, err
// 	}
// 	return time.Unix(result, 0), nil
// }

// func (as *storage) StartLeaseExpiryChecker() {
// 	ticker := time.NewTicker(1 * time.Minute)
// 	defer ticker.Stop()

// 	for range ticker.C {
// 		// Check for each message if its last heartbeat + lease duration > current time
// 		for _, msg := range allMessagesWithActiveLease {
// 			lastHeartbeat, err := as.lastHeartbeat(msg.QueueName, msg.MessageId)
// 			if err != nil {
// 				// Handle error
// 				continue
// 			}
// 			if time.Since(lastHeartbeat) > msg.LeaseDuration {
// 				// This message's lease has expired due to lack of heartbeat.
// 				// Handle lease expiry logic here.
// 			}
// 		}
// 	}
// }
