package repository

import (
	"context"
	"errors"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	"google.golang.org/grpc/codes"
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

func (as *storage) getNextPendingMessage(ctx context.Context, queueName string, members []string) (*message_pb.Message, error) {
	for _, member := range members {
		if len(member) == 0 {
			continue
		}
		meta, err := as.fetchMessageMetadata(ctx, queueName, member)
		if err != nil {
			as.logger.ErrorWithFields("Failed to fetch message metadata", "error", err)
			return nil, err
		}
		if meta.State == message_pb.Message_Metadata_PENDING {
			return &message_pb.Message{
				MessageId: member,
				Metadata:  meta,
			}, nil
		}
	}
	return nil, nil
}

func (as *storage) GetQueueMessage(ctx context.Context, request *queueservice_pb.GetNextMessageRequest) (*queueservice_pb.GetNextMessageResponse, error) {
	queueMeta, err := as.GetQueueMetadata(ctx, request.GetQueueName())
	if err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, err, "Failed to get queue's metadata.").GRPCStatus()
	}

	if err := as.validateExclusivity(queueMeta, request.GetExclusivityKey()); err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Error occured while validating the exclusivity key.").GRPCStatus()
	}

	members, err := as.fetchQueueMembersByPriority(ctx, request.GetQueueName(), 50)
	if err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, err, "Failed to get queue members.").GRPCStatus()
	}
	if len(members) == 0 {
		as.logger.Info("No messages found with a deadline before now")
		return &queueservice_pb.GetNextMessageResponse{}, nil
	}

	message, err := as.getNextPendingMessage(ctx, request.GetQueueName(), members)
	if err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to get next pending message.").GRPCStatus()
	}
	if message == nil {
		// util.Info("No pending messages found with a deadline before now")
		return &queueservice_pb.GetNextMessageResponse{}, nil
	}

	// Update the message's state to "Running" and restore the message
	oldState := message.Metadata.State // Capture old state before update (should be PENDING)
	as.logger.InfoWithFields(
		"Leasing message:",
		"message Id", message.GetMessageId(),
		"old state", oldState,
	)
	as.updateMessageStateAndLease(message, request, queueMeta)

	if err := as.saveMessageWithMetadataAndOldState(ctx, request.GetQueueName(), message, oldState); err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to save message's metadata.").GRPCStatus()
	}

	err = as.decryptMessageMetadataPayload(message.Metadata)
	if err != nil {
		return nil, err
	}

	as.logger.InfoWithFields(
		"Successfully leased the message:",
		"lease expiry", message.Metadata.GetLeaseExpiry(),
		"message Id", message.GetMessageId(),
	)
	return &queueservice_pb.GetNextMessageResponse{
		Message: message,
	}, nil
}
