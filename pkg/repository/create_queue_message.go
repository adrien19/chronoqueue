package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/encryption"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
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

func (as *storage) CreateQueueMessage(ctx context.Context, request *queueservice_pb.PostMessageRequest) (*queueservice_pb.PostMessageResponse, error) {
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

	// // avoid double encryption for schedule messages.
	// if message.Metadata.GetCronSchedule() == "" {
	// 	err = as.encryptMetadataPayload(message.Metadata)
	// 	if err != nil {
	// 		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while encrypting message payload")
	// 		return nil, chronoErr.GRPCStatus()
	// 	}
	// }

	// Set the message invisibility expiry
	invisibity_expiry := time.Now().Add(message.Metadata.InvisibilityDuration.AsDuration())
	message.Metadata.InvisibilityExpiry = invisibity_expiry.UnixMilli()
	message.Metadata.State = message_pb.Message_Metadata_INVISIBLE

	priority := message.Metadata.GetPriority()
	if priority > MaxPriority {
		priority = MaxPriority
	} else if priority < 0 {
		priority = MinPriority
	}
	// Calculate the message's priority score
	// The score is calculated as the current time plus the message's priority
	// This ensures that messages with a higher priority are processed first
	priorityScore := time.Now().Add(time.Duration(int64(MaxPriority-priority))).UnixNano() / int64(time.Millisecond)

	// Begin transaction pipeline
	txPipeline := as.redisClient.TxPipeline()

	// Add the message to the queue
	prefixedQueueName := "queue:" + queueName
	_, err = txPipeline.ZAdd(ctx, prefixedQueueName, redis.Z{
		Score:  float64(priorityScore),
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

	// Add message to state-based index (INVISIBLE state)
	messageKey := fmt.Sprintf("%s:%s:meta", queueName, message.MessageId)
	_, err = txPipeline.ZAdd(ctx, "invisible_messages", redis.Z{
		Score:  float64(message.Metadata.InvisibilityExpiry),
		Member: messageKey,
	}).Result()
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while adding message to invisible index")
		return nil, chronoErr.GRPCStatus()
	}

	// Commit the transaction
	_, err = txPipeline.Exec(ctx)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while executing redis pipeline")
		return nil, chronoErr.GRPCStatus()
	}

	return &queueservice_pb.PostMessageResponse{Success: true}, nil
}
