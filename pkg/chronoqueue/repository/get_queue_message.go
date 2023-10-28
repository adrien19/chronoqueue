package repository

import (
	"context"
	"errors"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	"google.golang.org/grpc/codes"
)

func (as *storage) validateExclusivity(queueMeta *chronoqueue.Queue_Options, exclusivityKey string) error {
	if queueMeta.GetType() == chronoqueue.Queue_Options_EXCLUSIVE && exclusivityKey == "" {
		return errors.New("error: queue requires an exclusive key")
	}

	if queueMeta.GetExclusivityKey() != exclusivityKey {
		return errors.New("error: provided exclusive key is not valid for the request queue")
	}
	return nil
}

func (as *storage) getNextPendingMessage(ctx context.Context, queueName string, members []string) (*chronoqueue.Message, error) {
	for _, member := range members {
		if len(member) == 0 {
			continue
		}
		meta, err := as.fetchMessageMetadata(ctx, queueName, member)
		if err != nil {
			util.ErrorWithFields("Failed to fetch message metadata", map[string]interface{}{
				"error": err,
			})
			return nil, err
		}
		if meta.State == chronoqueue.Message_Metadata_PENDING {
			return &chronoqueue.Message{
				MessageId: member,
				Metadata:  meta,
			}, nil
		}
	}
	return nil, nil
}

func (as *storage) GetQueueMessage(ctx context.Context, request *chronoqueue.GetNextMessageRequest) (*chronoqueue.GetNextMessageResponse, error) {
	queueMeta, err := as.getQueueMetadata(ctx, request.GetQueueName())
	if err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, err, "Failed to get queue's metadata.").GRPCStatus()
	}

	if err := as.validateExclusivity(queueMeta, request.GetExclusivityKey()); err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Error occured while validating the exclusivity key.").GRPCStatus()
	}

	members, err := as.fetchQueueMembersBeforeNow(ctx, request.GetQueueName())
	if err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, err, "Failed to get queue members.").GRPCStatus()
	}
	if len(members) == 0 {
		util.Info("No messages found with a deadline before now")
		return &chronoqueue.GetNextMessageResponse{}, nil
	}

	message, err := as.getNextPendingMessage(ctx, request.GetQueueName(), members)
	if err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to get next pending message.").GRPCStatus()
	}
	if message == nil {
		// util.Info("No pending messages found with a deadline before now")
		return &chronoqueue.GetNextMessageResponse{}, nil
	}

	// Update the message's state to "Running" and restore the message
	as.updateMessageStateAndLease(message, request, queueMeta)

	if err := as.saveMessageWithMetadata(ctx, request.GetQueueName(), message); err != nil {
		return nil, util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to save message's metadata.").GRPCStatus()
	}

	err = as.decryptMessageMetadataPayload(message.Metadata)
	if err != nil {
		return nil, err
	}

	util.InfoWithFields("Successfully leased the message:", map[string]interface{}{
		"lease expiry": message.Metadata.GetLeaseExpiry(),
		"message Id":   message.GetMessageId(),
	})
	return &chronoqueue.GetNextMessageResponse{
		Message: message,
	}, nil
}
