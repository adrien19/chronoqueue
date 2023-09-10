package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/redis/go-redis/v9"
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
		return nil, errors.New("invalid input: missing required fields")
	}

	exists, err := as.checkQueueExistence(ctx, queueName)
	if err != nil {
		return nil, fmt.Errorf("error checking queue existence: %v", err)
	}
	if !exists {
		return nil, errors.New("message's queue does not exist")
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
		return nil, fmt.Errorf("error adding message to queue: %v", err)
	}

	// Serialize the message metadata and store
	messageMetadataByte, err := as.serializeMessageMetadata(message.Metadata)
	if err != nil {
		return nil, fmt.Errorf("error serializing message metadata: %v", err)
	}

	_, err = txPipeline.HSet(ctx, fmt.Sprintf("%s:%s:meta", queueName, message.MessageId), "metadata", string(messageMetadataByte)).Result()
	if err != nil {
		return nil, fmt.Errorf("error storing message metadata: %v", err)
	}

	// Commit the transaction
	_, err = txPipeline.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("error executing redis pipeline: %v", err)
	}

	return &chronoqueue.PostMessageResponse{}, nil
}
