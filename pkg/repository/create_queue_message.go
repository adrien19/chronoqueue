package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/encryption"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/adrien19/chronoqueue/pkg/validator"
)

const (
	MaxPriority = 100
	MinPriority = 0
)

// Serialize the message metadata into JSON
func (as *storage) serializeMessageMetadata(metadata *message_pb.Message_Metadata) ([]byte, error) {
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	return m.Marshal(metadata)
}

// Serialize the metadata payload into JSON
func (as *storage) serializeMetadataPayload(payload *common_pb.Payload) ([]byte, error) {
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	return m.Marshal(payload)
}

// Serialize the metadata payload into JSON
func (as *storage) encryptMetadataPayload(metadata *message_pb.Message_Metadata) error {
	// Handle nil encryption key manager (used in testing)
	if as.encryptionKeyManager == nil || !as.encryptionKeyManager.Enabled {
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
		metadata.Payload = &common_pb.Payload{}
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

func (as *storage) CreateQueueMessage(ctx context.Context, request *queueservice_pb.PostMessageRequest, validator validator.Validator) (*queueservice_pb.PostMessageResponse, error) {
	queueName := request.GetQueueName()
	message := request.GetMessage()

	// Basic input validation
	if queueName == "" || message == nil || message.GetMessageId() == "" {
		err := errors.New("invalid input: queue name and message ID required")
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, err, "Invalid input provided")
		return nil, chronoErr.GRPCStatus()
	}

	if validator != nil {
		// NEW: Comprehensive payload validation with schema registry support
		validationResult := validator.Validate(ctx, message)

		if !validationResult.Valid {
			// Build detailed error message
			errorDetails := "Message validation failed:"
			for _, valErr := range validationResult.Errors {
				errorDetails += fmt.Sprintf("\n  - %s: %s", valErr.Field, valErr.Message)
			}
			err := errors.New(errorDetails)
			chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, err, "Message validation failed")
			return nil, chronoErr.GRPCStatus()
		}
	}

	scheduledTime := as.calculateScheduledTime(message.Metadata)

	message.Metadata.State = message_pb.Message_Metadata_PENDING

	priority := message.Metadata.GetPriority()
	if priority > MaxPriority {
		priority = MaxPriority
	} else if priority < 0 {
		priority = MinPriority
	}
	message.Metadata.PriorityLevel = int32(priority)

	if err := as.encryptMetadataPayload(message.Metadata); err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to encrypt message payload")
		return nil, chronoErr.GRPCStatus()
	}

	messageMetadataByte, err := as.serializeMessageMetadata(message.Metadata)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while serializing message metadata")
		return nil, chronoErr.GRPCStatus()
	}

	_, err = as.redisClient.HSet(ctx, as.messageMetaKey(queueName, message.MessageId), "metadata", string(messageMetadataByte)).Result()
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while saving message metadata")
		return nil, chronoErr.GRPCStatus()
	}

	if err := as.addToSchedule(ctx, queueName, message.MessageId, scheduledTime); err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to add message to schedule index")
		return nil, chronoErr.GRPCStatus()
	}

	metaKey := as.messageMetaKey(queueName, message.MessageId)
	as.redisClient.Expire(ctx, metaKey, 24*time.Hour)

	return &queueservice_pb.PostMessageResponse{Success: true}, nil
}

func (as *storage) calculateScheduledTime(meta *message_pb.Message_Metadata) int64 {
	if meta.ScheduledTime != nil {
		return meta.ScheduledTime.AsTime().UnixMilli()
	}

	return time.Now().UnixMilli()
}
