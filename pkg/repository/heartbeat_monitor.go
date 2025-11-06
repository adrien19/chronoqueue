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

	queueName := request.GetQueueName()
	messageID := request.GetMessageId()
	streamEntryID := request.GetStreamEntryId()

	meta, err := as.fetchMessageMetadata(ctx, queueName, messageID)
	if err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to fetch metadata for heartbeat.").GRPCStatus()
	}

	if meta == nil || meta.State != message_pb.Message_Metadata_RUNNING {
		return &queueservice_pb.SendMessageHeartBeatResponse{
			RemainingTime: nil,
			State:         meta.State,
		}, nil
	}

	consumerName := as.generateConsumerName()

	if err := as.sendHeartbeat(ctx, queueName, consumerName, streamEntryID); err != nil {
		as.logger.ErrorWithFields("Failed to send heartbeat via XCLAIM", "error", err, "messageID", messageID)
	}

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

	remainingMilliseconds := meta.GetLeaseExpiry() - time.Now().UnixNano()/int64(time.Millisecond)
	thresholdMilliseconds := int64(float64(renewalDuration.Milliseconds()) * DefaultThresholdPercentage)

	if remainingMilliseconds <= thresholdMilliseconds {
		newExpiry := time.Now().Add(renewalDuration)
		oldState := meta.State
		meta.LeaseExpiry = newExpiry.UnixNano() / int64(time.Millisecond)
		meta.LeaseRenewalCount = meta.GetLeaseRenewalCount() + 1
		if err = as.saveMessageMetadataWithOldState(ctx, queueName, messageID, meta, oldState); err != nil {
			return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to save metadata for heartbeat.").GRPCStatus()
		}

		metaKey := as.messageMetaKey(queueName, messageID)
		as.redisClient.Expire(ctx, metaKey, 2*renewalDuration)
	}

	remainingTimeDuration := durationpb.New(renewalDuration)

	return &queueservice_pb.SendMessageHeartBeatResponse{
		RemainingTime: remainingTimeDuration,
		State:         meta.State,
	}, nil
}
