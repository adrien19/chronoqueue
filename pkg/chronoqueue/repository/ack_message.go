package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	"google.golang.org/grpc/codes"
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
		err := errors.New("invalid input: queue name and message ID required")
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, err, "Invalid input provided")
		return nil, chronoErr.GRPCStatus()
	}

	metadata, err := as.fetchMessageMetadata(ctx, queueName, messageID)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while fetching message metadata")
		return nil, chronoErr.GRPCStatus()
	}

	// Update the message state
	metadata.State = request.State

	// Save the updated metadata
	err = as.saveMessageMetadata(ctx, queueName, messageID, metadata)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while saving updated message metadata")
		return nil, chronoErr.GRPCStatus()
	}

	return &chronoqueue.AcknowledgeMessageResponse{}, nil
}
