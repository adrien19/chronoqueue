package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// Updates and saves the message metadata in Redis.
func (as *storage) saveMessageMetadata(ctx context.Context, queueName string, messageID string, metadata *chronoqueue.Message_Metadata) error {
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	messageMetadataByte, err := m.Marshal(metadata)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s:%s:meta", queueName, messageID)
	_, err = as.redisClient.HSet(ctx, key, "metadata", string(messageMetadataByte)).Result()
	return err
}

func (as *storage) AcknowledgeMessage(ctx context.Context, request *chronoqueue.AcknowledgeMessageRequest) (*chronoqueue.AcknowledgeMessageResponse, error) {
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

	// Update the message state
	metadata.State = request.State

	// Save the updated metadata
	err = as.saveMessageMetadata(ctx, queueName, messageID, metadata)
	if err != nil {
		return nil, fmt.Errorf("error saving updated message metadata: %v", err)
	}

	return &chronoqueue.AcknowledgeMessageResponse{}, nil
}
