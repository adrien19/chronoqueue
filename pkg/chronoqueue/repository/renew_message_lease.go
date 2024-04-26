package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/adrien19/chronoqueue/api-deplicated/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (as *storage) RenewMessageLease(ctx context.Context, request *chronoqueue.RenewMessageLeaseRequest) (*chronoqueue.RenewMessageLeaseResponse, error) {
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
	util.InfoWithFields("Message successfully renewed lease", map[string]interface{}{
		"message_id": messageID,
		"new_expiry": metadata.GetLeaseExpiry(),
	})
	remainingTimeDuration := durationpb.New(time.Duration(metadata.GetLeaseExpiry()-time.Now().UnixNano()/int64(time.Millisecond)) * time.Millisecond)

	return &chronoqueue.RenewMessageLeaseResponse{
		RemainingTime: remainingTimeDuration,
		State:         metadata.State,
	}, nil
}
