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
	"github.com/adrien19/chronoqueue/internal/lease"
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

	// Determine worker/consumer identity: use provided worker_id or generate one
	workerID := request.GetWorkerId()
	if workerID == "" {
		workerID = as.generateWorkerID()
	}
	consumerName := as.generateConsumerName(workerID)

	// Claim a message from the stream based on the queue's priority policy
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

	// fetchMessageMetadata returns already decrypted metadata
	meta, err := as.fetchMessageMetadata(ctx, queueName, messageID)
	if err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to fetch message metadata.").GRPCStatus()
	}

	now := time.Now()
	workerHint := time.Duration(0)
	if request.GetLeaseDuration() != nil {
		workerHint = request.GetLeaseDuration().AsDuration()
	}

	policy := lease.MergeLeasePolicy(queueMeta, meta, workerHint)
	attemptID := lease.GenerateAttemptID(messageID, workerID, now)

	rt := policy.InitRuntime(now, attemptID, workerID)

	if meta.CurrentAttempt == nil {
		meta.CurrentAttempt = &message_pb.Message_Metadata_AttemptRuntime{}
	}

	lease.ApplyRuntimeToProto(rt, meta.CurrentAttempt)

	meta.State = message_pb.Message_Metadata_RUNNING
	meta.LeaseExpiry = now.Add(policy.BaseTimeout).UnixMilli()

	// saveMessageMetadata will re-encrypt before saving to Redis
	if err := as.saveMessageMetadata(ctx, queueName, messageID, meta); err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to save message's metadata.").GRPCStatus()
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
		WorkerId:      &workerID,
		AttemptId:     &attemptID,
	}, nil
}
