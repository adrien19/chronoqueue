package repository

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/util"
)

func (as *storage) validateExclusivity(queueMeta *queue_pb.QueueMetadata, exclusivityKey string) error {
	if queueMeta.GetType() == queue_pb.QueueType_EXCLUSIVE && exclusivityKey == "" {
		return errors.New("error: queue requires an exclusive key")
	}

	if queueMeta.GetExclusivityKey() != exclusivityKey {
		return errors.New("error: provided exclusive key is not valid for the request queue")
	}
	return nil
}

func (as *storage) GetQueueMessage(ctx context.Context, request *queueservice_pb.GetNextMessageRequest) (*queueservice_pb.GetNextMessageResponse, error) {
	queueName := request.GetQueueName()
	queueMeta, err := as.GetQueueMetadata(ctx, queueName)
	if err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, err, "Failed to get queue's metadata.").GRPCStatus()
	}

	if err := as.validateExclusivity(queueMeta, request.GetExclusivityKey()); err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Error occured while validating the exclusivity key.").GRPCStatus()
	}

	consumerName := as.generateConsumerName()
	var streamEntry *redis.XMessage
	if queueMeta.PriorityConfig == nil || queueMeta.PriorityConfig.Policy == queue_pb.FairnessPolicy_STRICT {
		streamEntry, err = as.claimStrict(ctx, queueName, consumerName)
	} else {
		streamEntry, err = as.claimWeightedWithAging(ctx, queueName, consumerName, queueMeta.PriorityConfig)
	}
	if err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to claim message from stream.").GRPCStatus()
	}
	if streamEntry == nil {
		return &queueservice_pb.GetNextMessageResponse{}, nil
	}

	messageID, ok := streamEntry.Values["message_id"].(string)
	if !ok || messageID == "" {
		return &queueservice_pb.GetNextMessageResponse{}, nil
	}

	meta, err := as.fetchMessageMetadata(ctx, queueName, messageID)
	if err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to fetch message metadata.").GRPCStatus()
	}

	meta.State = message_pb.Message_Metadata_RUNNING
	meta.LeaseExpiry = time.Now().Add(request.LeaseDuration.AsDuration()).UnixMilli()

	if err := as.saveMessageMetadata(ctx, queueName, messageID, meta); err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to save message's metadata.").GRPCStatus()
	}

	err = as.decryptMessageMetadataPayload(meta)
	if err != nil {
		return nil, err
	}

	as.logger.InfoWithFields(
		"Successfully leased the message:",
		"lease expiry", meta.GetLeaseExpiry(),
		"message Id", messageID,
		"state", meta.State.String(),
	)
	return &queueservice_pb.GetNextMessageResponse{
		Message: &message_pb.Message{
			MessageId: messageID,
			Metadata:  meta,
		},
		StreamEntryId: streamEntry.ID,
	}, nil
}
