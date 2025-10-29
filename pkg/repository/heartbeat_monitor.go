package repository

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/util"
)

const (
	DefaultThresholdPercentage = 0.1 // Default threshold (x seconds) to renew the lease
	DefaultLeaseRenewal        = 3   // Default lease renewal time (y seconds)
)

func (as *storage) SendMessageHeartBeat(ctx context.Context, request *queueservice_pb.SendMessageHeartBeatRequest) (*queueservice_pb.SendMessageHeartBeatResponse, error) {
	as.logger.InfoWithFields("Processing heartbeat for message",
		"queue name", request.GetQueueName(),
		"request message id", request.GetMessageId(),
	)
	meta, err := as.fetchMessageMetadata(ctx, request.GetQueueName(), request.GetMessageId())
	if err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to fetch metadata for heartbeat.").GRPCStatus()
	}

	if meta == nil || meta.State != message_pb.Message_Metadata_RUNNING {
		return &queueservice_pb.SendMessageHeartBeatResponse{
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
		oldState := meta.State // Capture old state (should be RUNNING)
		meta.LeaseExpiry = newExpiry.UnixNano() / int64(time.Millisecond)
		meta.LeaseRenewalCount = meta.GetLeaseRenewalCount() + 1 // increase the renewal count
		if err = as.saveMessageMetadataWithOldState(ctx, request.GetQueueName(), request.GetMessageId(), meta, oldState); err != nil {
			return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to save metadata for heartbeat.").GRPCStatus()
		}
	}

	remainingTimeDuration := durationpb.New(renewalDuration)

	// Set expiration time to 60x the lease duration to ensure heartbeat key persistence
	expiration := 60 * renewalDuration
	_, err = as.redisClient.Set(ctx, key, time.Now().Unix(), expiration).Result()

	return &queueservice_pb.SendMessageHeartBeatResponse{
		RemainingTime: remainingTimeDuration,
		State:         meta.State,
	}, err
}
