package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal/encryption"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

// Serialize the message metadata into JSON
func (as *storage) serializeMessageMetadata(metadata *chronoqueue.Message_Metadata) ([]byte, error) {
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	return m.Marshal(metadata)
}

// Serialize the metadata payload into JSON
func (as *storage) serializeMetadataPayload(payload *chronoqueue.Payload) ([]byte, error) {
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	return m.Marshal(payload)
}

// Serialize the metadata payload into JSON
func (as *storage) encryptMetadataPayload(metadata *chronoqueue.Message_Metadata) error {
	if !as.encryptionKeyManager.Enabled {
		return nil
	}
	// Get the payload data from the message
	payloadData, err := as.serializeMetadataPayload(metadata.Payload)
	if err != nil {
		return err
	}

	// Encrypt the payload data
	encryptedPayload, nonce, err := encryption.EncryptPayload(payloadData, as.encryptionKeyManager)
	if err != nil {
		return err
	}

	if encryptedPayload != "" && nonce != "" {
		metadata.Payload = &chronoqueue.Payload{}
		metadata.Payload.Metadata = make(map[string]*structpb.Value)
	}

	// Update to the metadata field of Payload
	metadata.Payload.Metadata["encryptedPayload"] = structpb.NewStringValue(encryptedPayload)
	metadata.Payload.Metadata["nonce"] = structpb.NewStringValue(nonce)

	if metadata.Payload.Metadata["encryptedPayload"].GetStringValue() == "" || metadata.Payload.Metadata["nonce"].GetStringValue() == "" {
		return errors.New("failed to updated encryptedPayload or nonce in metadata")
	}
	return nil
}

func (as *storage) CreateQueueMessage(ctx context.Context, request *chronoqueue.PostMessageRequest) (*chronoqueue.PostMessageResponse, error) {
	queueName := request.GetQueueName()
	message := request.GetMessage()

	// Basic input validation
	if queueName == "" || message == nil || message.GetMessageId() == "" {
		err := errors.New("invalid input: queue name and message ID required")
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, err, "Invalid input provided")
		return nil, chronoErr.GRPCStatus()
	}

	exists, err := as.checkQueueExistence(ctx, queueName)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while checking queue existence")
		return nil, chronoErr.GRPCStatus()
	}
	if !exists {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, err, "Message's queue does not exist")
		return nil, chronoErr.GRPCStatus()
	}

	err = as.encryptMetadataPayload(message.Metadata)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while encrypting message payload")
		return nil, chronoErr.GRPCStatus()
	}

	// Set the message state if InvisibilityDuration is zero
	if message.GetMetadata().GetInvisibilityDuration().AsDuration() == 0 {
		message.Metadata.State = chronoqueue.Message_Metadata_PENDING
	}

	// Set the message invisibility expiry
	invisibity_expiry := time.Now().Add(message.Metadata.InvisibilityDuration.AsDuration())
	message.Metadata.InvisibilityExpiry = invisibity_expiry.UnixNano() / int64(time.Millisecond)

	// Calculate the message's priority score
	// The score is calculated as the current time plus the message's priority
	// This ensures that messages with a higher priority are processed first
	priorityScore := time.Now().UnixNano() + int64(message.Metadata.GetPriority())

	// Generate a unique score for the message to ensure correct ordering for messages with the same priority
	// We use the current time in nanoseconds as a tie-breaker
	uniqueScore := float64(priorityScore) + float64(time.Now().UnixNano())/float64(time.Second)

	// Begin transaction pipeline
	txPipeline := as.redisClient.TxPipeline()

	// Add the message to the queue
	_, err = txPipeline.ZAdd(ctx, queueName, redis.Z{
		Score:  float64(uniqueScore),
		Member: message.GetMessageId(),
	}).Result()
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while adding message to queue")
		return nil, chronoErr.GRPCStatus()
	}

	// Serialize the message metadata and store
	messageMetadataByte, err := as.serializeMessageMetadata(message.Metadata)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while serializing message metadata")
		return nil, chronoErr.GRPCStatus()
	}

	_, err = txPipeline.HSet(ctx, fmt.Sprintf("%s:%s:meta", queueName, message.MessageId), "metadata", string(messageMetadataByte)).Result()
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while saving message metadata")
		return nil, chronoErr.GRPCStatus()
	}

	// Commit the transaction
	_, err = txPipeline.Exec(ctx)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while executing redis pipeline")
		return nil, chronoErr.GRPCStatus()
	}

	return &chronoqueue.PostMessageResponse{}, nil
}
