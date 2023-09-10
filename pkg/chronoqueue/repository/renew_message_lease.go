package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
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
	leaseDuration := request.GetLeaseDuration()
	metadata.LeaseDuration = &leaseDuration
	expireDate := time.Now().Add(time.Duration(metadata.GetLeaseDuration())).UnixNano() / int64(time.Millisecond)
	metadata.LeaseExpiry = &expireDate

	// Save the updated metadata
	err = as.saveMessageMetadata(ctx, queueName, messageID, metadata)
	if err != nil {
		return nil, fmt.Errorf("error saving updated message metadata: %v", err)
	}

	return &chronoqueue.RenewMessageLeaseResponse{}, nil
}
