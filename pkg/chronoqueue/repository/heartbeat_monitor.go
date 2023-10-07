package repository

import (
	"context"
	"errors"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	DefaultThresholdPercentage = 0.1 // Default threshold (x seconds) to renew the lease
	DefaultLeaseRenewal        = 3   // Default lease renewal time (y seconds)
)

func (as *storage) SendMessageHeartBeat(ctx context.Context, request *chronoqueue.SendMessageHeartBeatRequest) (*chronoqueue.SendMessageHeartBeatResponse, error) {
	meta, err := as.fetchMessageMetadata(ctx, request.GetQueueName(), request.GetMessageId())
	if err != nil {
		return nil, err
	}

	if meta == nil || meta.State != chronoqueue.Message_Metadata_RUNNING {
		return &chronoqueue.SendMessageHeartBeatResponse{
			RemainingTime: nil,
			State:         meta.State,
		}, nil
	}

	key := "heartbeat:" + request.GetQueueName() + ":" + request.GetMessageId()

	maxRenewal := 10 * time.Second
	minRenewal := 3 * time.Second
	fractionOfOriginal := 0.25

	renewalDuration := meta.GetLeaseDuration().AsDuration()

	if meta.LeaseRenewalCount > 1 {
		renewalDuration = time.Duration(renewalDuration.Seconds() * fractionOfOriginal)
		if renewalDuration > maxRenewal {
			renewalDuration = maxRenewal
		} else if renewalDuration < minRenewal {
			renewalDuration = minRenewal
		}
	}

	// Calculate the remaining time
	remainingMilliseconds := meta.GetLeaseExpiry() - time.Now().UnixNano()/int64(time.Millisecond)
	thresholdMilliseconds := int64(float64(renewalDuration.Milliseconds()) * DefaultThresholdPercentage)

	if remainingMilliseconds <= thresholdMilliseconds {
		// If lease is about to expire within the threshold, renew the lease for another y seconds
		newExpiry := time.Now().Add(renewalDuration)
		meta.LeaseExpiry = newExpiry.UnixNano() / int64(time.Millisecond)
		meta.LeaseRenewalCount = meta.GetLeaseRenewalCount() + 1 // increase the renewal count
		if err = as.saveMessageMetadata(ctx, request.GetQueueName(), request.GetMessageId(), meta); err != nil {
			return nil, errors.New("failed to renew message lease")
		}
	}

	remainingTimeDuration := durationpb.New(renewalDuration)

	// Set expiration time to 60x the lease duration to ensure heartbeat key persistence
	expiration := 60 * renewalDuration
	_, err = as.redisClient.Set(ctx, key, time.Now().Unix(), expiration).Result()

	return &chronoqueue.SendMessageHeartBeatResponse{
		RemainingTime: remainingTimeDuration,
		State:         meta.State,
	}, err
}
