package repository

import (
	"context"
	"errors"
	"fmt"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
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

// // Updates and saves the message metadata in Redis.
// func (as *storage) saveMessageMetadata(ctx context.Context, queueName string, messageID string, metadata *message_pb.Message_Metadata) error {
// 	return as.saveMessageMetadataWithOldState(ctx, queueName, messageID, metadata, message_pb.Message_Metadata_State(-1))
// }

func (as *storage) saveMessageMetadataWithOldState(ctx context.Context, queueName string, messageID string, metadata *message_pb.Message_Metadata, oldState message_pb.Message_Metadata_State) error {

	messageMutex := as.rs.NewMutex("mutex:" + messageID)
	// Try to acquire the lock
	if err := messageMutex.Lock(); err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected while acquiring lock")
		return chronoErr.GRPCStatus()
	}

	defer func() {
		// Release the message lock
		if ok, err := messageMutex.Unlock(); !ok || err != nil {
			as.logger.Error("Failed to release message lock", err)
		}
	}()

	key := fmt.Sprintf("%s:%s:meta", queueName, messageID)

	// If oldState is -1, we need to fetch the old state
	if oldState == message_pb.Message_Metadata_State(-1) {
		if oldMetadata, err := as.fetchMessageMetadata(ctx, queueName, messageID); err == nil {
			oldState = oldMetadata.State
		} else {
			// If we can't fetch old metadata, assume this is a new message
			oldState = message_pb.Message_Metadata_State(-1)
		}
	}

	// Begin transaction pipeline for atomic updates
	txPipeline := as.redisClient.TxPipeline()

	err := as.encryptMetadataPayload(metadata)
	if err != nil {
		return err
	}

	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	messageMetadataBytes, err := m.Marshal(metadata)
	if err != nil {
		return err
	}

	// Update message metadata
	_, err = txPipeline.HSet(ctx, key, "metadata", string(messageMetadataBytes)).Result()
	if err != nil {
		return err
	}

	// Handle state index transitions
	newState := metadata.State
	if oldState != newState {
		var newScore float64
		switch newState {
		case message_pb.Message_Metadata_RUNNING:
			newScore = float64(metadata.LeaseExpiry)
		case message_pb.Message_Metadata_INVISIBLE:
			newScore = float64(metadata.InvisibilityExpiry)
		}

		if err := as.transitionStateIndex(ctx, txPipeline, key, oldState, newState, newScore); err != nil {
			return fmt.Errorf("failed to update state indexes: %w", err)
		}
	}

	// Commit the transaction
	_, err = txPipeline.Exec(ctx)
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

func (as *storage) AcknowledgeMessage(ctx context.Context, request *queueservice_pb.AcknowledgeMessageRequest) (*queueservice_pb.AcknowledgeMessageResponse, error) {
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
	oldState := metadata.State // Capture old state before update
	if !isValidTransition(transitionState(metadata.State), transitionState(request.State)) {
		err := fmt.Errorf("invalid input: requested state %v transition to %v is not allowed for message: %s", request.State, metadata.State, messageID)
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while changing message state")
		return nil, chronoErr.GRPCStatus()
	}
	// Update the message state
	metadata.State = request.State

	// Save the updated metadata with the known old state to avoid redundant fetch
	err = as.saveMessageMetadataWithOldState(ctx, queueName, messageID, metadata, oldState)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while saving updated message metadata")
		return nil, chronoErr.GRPCStatus()
	}

	return &queueservice_pb.AcknowledgeMessageResponse{Success: true}, nil
}
