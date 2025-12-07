package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/redis/go-redis/v9"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/util"
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

	key := as.messageMetaKey(queueName, messageID)

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
		}

		if err := as.transitionStateIndex(ctx, txPipeline, key, oldState, newState, newScore); err != nil {
			return fmt.Errorf("failed to update state indexes: %w", err)
		}
	} else if newState == message_pb.Message_Metadata_RUNNING {
		// If state didn't change and is RUNNING, we may need to update the score
		_, err = txPipeline.ZAdd(ctx, "running_messages", redis.Z{
			Score:  float64(metadata.LeaseExpiry),
			Member: key,
		}).Result()
		if err != nil {
			return fmt.Errorf("failed to add message to running_messages: %w", err)
		}
	}

	// Commit the transaction
	_, err = txPipeline.Exec(ctx)
	return err
}

// // IsValidTransition checks if transitioning from the current state to a new state is valid based on the defined rules.
// func isValidTransition(currentState, newState transitionState) bool {
// 	switch currentState {
// 	case INVISIBLE:
// 		return newState == PENDING
// 	case PENDING:
// 		return newState == RUNNING || newState == CANCELED
// 	case RUNNING:
// 		return newState == PENDING || newState == COMPLETED || newState == CANCELED || newState == ERRORED
// 	case COMPLETED, CANCELED, ERRORED:
// 		return false
// 	default:
// 		// For UNDEFINED or any other state not explicitly handled
// 		return false
// 	}
// }

func isValidProtoStateTransition(current, next message_pb.Message_Metadata_State) bool {
	switch current {
	case message_pb.Message_Metadata_INVISIBLE:
		return next == message_pb.Message_Metadata_PENDING
	case message_pb.Message_Metadata_PENDING:
		return next == message_pb.Message_Metadata_RUNNING || next == message_pb.Message_Metadata_CANCELED
	case message_pb.Message_Metadata_RUNNING:
		return next == message_pb.Message_Metadata_PENDING ||
			next == message_pb.Message_Metadata_COMPLETED ||
			next == message_pb.Message_Metadata_CANCELED ||
			next == message_pb.Message_Metadata_ERRORED
	case message_pb.Message_Metadata_COMPLETED, message_pb.Message_Metadata_CANCELED, message_pb.Message_Metadata_ERRORED:
		return false
	default:
		return false
	}
}

func (as *storage) AcknowledgeMessage(
	ctx context.Context,
	request *queueservice_pb.AcknowledgeMessageRequest,
) (*queueservice_pb.AcknowledgeMessageResponse, error) {
	queueName := request.GetQueueName()
	messageID := request.GetMessageId()
	streamEntryID := request.GetStreamEntryId()
	attemptID := request.GetAttemptId()
	workerID := request.GetWorkerId()

	if queueName == "" || messageID == "" {
		err := errors.New("invalid input: queue name and message ID required")
		chronoErr := util.NewChronoError(
			util.ERROR_LEVEL_ERROR,
			codes.InvalidArgument,
			err,
			"Invalid input provided",
		)
		return nil, chronoErr.GRPCStatus()
	}

	// 1) Load message metadata
	metadata, err := as.fetchMessageMetadata(ctx, queueName, messageID)
	if err != nil {
		chronoErr := util.NewChronoError(
			util.ERROR_LEVEL_ERROR,
			codes.Internal,
			err,
			"Unexpected error occurred while fetching message metadata",
		)
		return nil, chronoErr.GRPCStatus()
	}
	if metadata == nil {
		// Message is gone or already cleaned up.
		return &queueservice_pb.AcknowledgeMessageResponse{Success: false}, nil
	}

	// 2) Validate we are ACKing a RUNNING message
	if metadata.State != message_pb.Message_Metadata_RUNNING {
		err := fmt.Errorf(
			"cannot acknowledge message in state %v (expected RUNNING), message: %s",
			metadata.State, messageID,
		)
		chronoErr := util.NewChronoError(
			util.ERROR_LEVEL_ERROR,
			codes.FailedPrecondition,
			err,
			"Invalid message state for acknowledgment",
		)
		return nil, chronoErr.GRPCStatus()
	}

	// 3) Validate ownership via attempt_id
	currentAttempt := metadata.GetCurrentAttempt()
	if currentAttempt == nil || currentAttempt.GetAttemptId() == "" {
		err := fmt.Errorf(
			"message %s has no current attempt; cannot acknowledge",
			messageID,
		)
		chronoErr := util.NewChronoError(
			util.ERROR_LEVEL_ERROR,
			codes.FailedPrecondition,
			err,
			"No current attempt found for message",
		)
		return nil, chronoErr.GRPCStatus()
	}

	// Backward compatibility: if attempt_id is absent, default to current attempt
	if attemptID == "" {
		attemptID = currentAttempt.GetAttemptId()
	}
	if attemptID != currentAttempt.GetAttemptId() {
		// This ACK is stale or from a different attempt; reject.
		as.logger.WarnWithFields("Stale or mismatched ACK attempt_id",
			"queue_name", queueName,
			"message_id", messageID,
			"expected_attempt_id", currentAttempt.GetAttemptId(),
			"got_attempt_id", attemptID,
		)

		err := fmt.Errorf("attempt ownership mismatch for message %s", messageID)
		chronoErr := util.NewChronoError(
			util.ERROR_LEVEL_ERROR,
			codes.FailedPrecondition,
			err,
			"Attempt ownership mismatch for acknowledgment",
		)
		return nil, chronoErr.GRPCStatus()
	}

	// Optional: soft check worker_id for observability
	if workerID != "" && currentAttempt.GetWorkerId() != "" &&
		workerID != currentAttempt.GetWorkerId() {

		as.logger.WarnWithFields("ACK worker_id mismatch",
			"queue_name", queueName,
			"message_id", messageID,
			"attempt_id", attemptID,
			"expected_worker_id", currentAttempt.GetWorkerId(),
			"got_worker_id", workerID,
		)
		// TODO: Should this fail here or just log.
	}

	// 4) Validate state transition (RUNNING -> COMPLETED/ERRORED/CANCELED)
	oldState := metadata.State
	requestedState := request.GetState()

	if !isValidProtoStateTransition(oldState, requestedState) {
		err := fmt.Errorf(
			"invalid state transition from %v to %v for message: %s",
			oldState, requestedState, messageID,
		)
		chronoErr := util.NewChronoError(
			util.ERROR_LEVEL_ERROR,
			codes.FailedPrecondition,
			err,
			"Unexpected error occurred while changing message state",
		)
		return nil, chronoErr.GRPCStatus()
	}

	metadata.State = requestedState

	// 5) Persist metadata state change
	if err := as.saveMessageMetadataWithOldState(ctx, queueName, messageID, metadata, oldState); err != nil {
		chronoErr := util.NewChronoError(
			util.ERROR_LEVEL_ERROR,
			codes.Internal,
			err,
			"Unexpected error occurred while saving updated message metadata",
		)
		return nil, chronoErr.GRPCStatus()
	}

	// 6) Update state counters (best-effort)
	if err := as.updateStateCounters(ctx, queueName, oldState, requestedState); err != nil {
		as.logger.ErrorWithFields("Failed to update state counters", "error", err, "messageID", messageID)
		// Do not fail the ACK because of metrics issues
	}

	// 7) XACK from Redis stream, if we have a stream entry id
	if streamEntryID != "" {
		if err := as.ackMessage(ctx, queueName, streamEntryID); err != nil {
			as.logger.ErrorWithFields("Failed to XACK message from stream", "error", err, "messageID", messageID)
		}

		// Optionally expire metadata as a cleanup hint
		metaKey := as.messageMetaKey(queueName, messageID)
		as.redisClient.Expire(ctx, metaKey, 1*time.Hour)
	}

	return &queueservice_pb.AcknowledgeMessageResponse{Success: true}, nil
}

// updateStateCounters updates Redis hash counters for state transitions
// This provides O(1) lookup for GetQueueState instead of scanning all message keys
func (as *storage) updateStateCounters(ctx context.Context, queueName string, oldState, newState message_pb.Message_Metadata_State) error {
	statsKey := as.statsKey(queueName)

	// Use pipelined operations for atomicity
	pipe := as.redisClient.Pipeline()

	// Decrement old state counter
	oldStateStr := oldState.String()
	if oldStateStr != "" && oldStateStr != "UNDEFINED" {
		pipe.HIncrBy(ctx, statsKey, oldStateStr, -1)
	}

	// Increment new state counter
	newStateStr := newState.String()
	if newStateStr != "" && newStateStr != "UNDEFINED" {
		pipe.HIncrBy(ctx, statsKey, newStateStr, 1)
	}

	// Set TTL on stats key to auto-cleanup (match message metadata TTL)
	pipe.Expire(ctx, statsKey, 2*time.Hour)

	_, err := pipe.Exec(ctx)
	return err
}
