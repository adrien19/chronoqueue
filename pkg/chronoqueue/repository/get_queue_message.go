package repository

import (
	"context"
	"errors"
	"log"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
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
			log.Println("===>> Error occurred: ", err)
			return nil, err
		}
		if meta.State == chronoqueue.Message_Metadata_PENDING {
			return &chronoqueue.Message{
				MessageId: member,
				Priority:  0,
				Metadata:  meta,
			}, nil
		}
	}
	return nil, nil
}

func (as *storage) GetQueueMessage(ctx context.Context, request *chronoqueue.GetNextMessageRequest) (*chronoqueue.GetNextMessageResponse, error) {
	queueMeta, err := as.getQueueMetadata(ctx, request.GetQueueName())
	if err != nil {
		return handleError(err, "Failed to get queue's metadata. Err: ")
	}

	// if err := as.validateExclusivity(queueMeta, request.GetExclusivityKey()); err != nil {
	// 	return handleError(err, "Failed to validate exclusivity. Err: ")
	// }

	members, err := as.fetchQueueMembersBeforeNow(ctx, request.GetQueueName())
	if err != nil {
		return handleError(err, "Failed to get queue members. Err: ")
	}
	if len(members) == 0 {
		log.Println("No messages found with a deadline before now")
		return &chronoqueue.GetNextMessageResponse{}, nil
	}

	message, err := as.getNextPendingMessage(ctx, request.GetQueueName(), members)
	if err != nil {
		log.Println("Error getting Next Pending Messsage for: ", queueMeta.Type)
		return handleError(err, "Failed to get next pending message. Err: ")
	}
	if message == nil {
		log.Println("No pending messages found with a deadline before now")
		return &chronoqueue.GetNextMessageResponse{}, nil
	}

	// Update the message's state to "Running" and restore the message
	as.updateMessageStateAndLease(message, request, queueMeta)

	if err := as.saveMessageWithMetadata(ctx, request.GetQueueName(), message); err != nil {
		return handleError(err, "Failed to save message's metadata. Err: ")
	}

	log.Println("Successfully leased the message until: ", message.Metadata.GetLeaseExpiry())
	return &chronoqueue.GetNextMessageResponse{
		Message: message,
	}, nil
}
