package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
)

// Serialize the message metadata into JSON
func (as *storage) serializeMessageMetadata(metadata *chronoqueue.Message_Metadata) ([]byte, error) {
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	return m.Marshal(metadata)
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

	// Set the message state if InvisibilityDuration is zero
	if message.GetMetadata().GetInvisibilityDuration() == 0 {
		message.Metadata.State = chronoqueue.Message_Metadata_PENDING
	}

	// Calculate the message's deadline
	deadline := time.Now().Add(time.Duration(message.Priority)).UnixNano() / int64(time.Millisecond)

	// Begin transaction pipeline
	txPipeline := as.redisClient.TxPipeline()

	// Add the message to the queue
	_, err = txPipeline.ZAdd(ctx, queueName, redis.Z{
		Score:  float64(deadline),
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
