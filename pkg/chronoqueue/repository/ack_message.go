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

type transitionState int32

const (
	UNDEFINED transitionState = iota
	INVISIBLE
	PENDING
	RUNNING
	COMPLETED
	CANCELED
	ERRORED
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

// IsValidTransition checks if transitioning from the current state to a new state is valid based on the defined rules.
func isValidTransition(currentState, newState transitionState) bool {
	switch currentState {
	case INVISIBLE:
		return newState == PENDING
	case PENDING:
		return newState == RUNNING || newState == CANCELED
	case RUNNING:
		return newState == PENDING || newState == COMPLETED || newState == CANCELED || newState == ERRORED
	case COMPLETED, CANCELED, ERRORED:
		return false
	default:
		// For UNDEFINED or any other state not explicitly handled
		return false
	}
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

	// Check if the state transition is allowed
	if !isValidTransition(transitionState(metadata.State), transitionState(request.State)) {
		err := errors.New("invalid input: requested state transition is not allowed")
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while changing message state")
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
