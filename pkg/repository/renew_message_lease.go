package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (as *storage) RenewMessageLease(ctx context.Context, request *queueservice_pb.RenewMessageLeaseRequest) (*queueservice_pb.RenewMessageLeaseResponse, error) {
	queueName := request.GetQueueName()
	messageID := request.GetMessageId()

	// Basic validation
	if queueName == "" || messageID == "" {
		return nil, errors.New("invalid input: missing required fields")
	}

	metadata, err := as.fetchMessageMetadata(ctx, queueName, messageID)
	if err != nil {
		return nil, fmt.Errorf("error fetching message metadata: %v", err)
	}

	// Update the lease duration and expiration time
	metadata.LeaseDuration = request.GetLeaseDuration()
	expireDate := time.Now().Add(time.Duration(metadata.GetLeaseDuration().AsDuration()))
	metadata.LeaseExpiry = expireDate.UnixNano() / int64(time.Millisecond)

	// Save the updated metadata
	err = as.saveMessageMetadata(ctx, queueName, messageID, metadata)
	if err != nil {
		return nil, fmt.Errorf("error saving updated message metadata: %v", err)
	}
	as.logger.InfoWithFields(
		"Message successfully renewed lease",
		"message_id", messageID,
		"new_expiry", metadata.GetLeaseExpiry(),
	)
	remainingTimeDuration := durationpb.New(time.Duration(metadata.GetLeaseExpiry()-time.Now().UnixNano()/int64(time.Millisecond)) * time.Millisecond)

	return &queueservice_pb.RenewMessageLeaseResponse{
		RemainingTime: remainingTimeDuration,
		State:         metadata.State,
	}, nil
}
