package repository

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
)

type ChronoHandlerFunc func(ctx context.Context, req interface{}) (interface{}, error)

func ErrorHandler(defaultResp interface{}, msg string) ChronoHandlerFunc {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		resp, err := defaultResp, errors.New(msg)
		log.Println(msg, err)
		return resp, err
	}
}

// Fetches and deserializes the message metadata from Redis.
func (as *storage) fetchMessageMetadata(ctx context.Context, queueName string, messageID string) (*chronoqueue.Message_Metadata, error) {
	key := fmt.Sprintf("%s:%s:meta", queueName, messageID)
	result, err := as.redisClient.HGet(ctx, key, "metadata").Result()
	if err != nil {
		return nil, err
	}

	var meta chronoqueue.Message_Metadata
	err = protojson.Unmarshal([]byte(result), &meta)
	if err != nil {
		return nil, err
	}

	return &meta, nil
}

func (as *storage) getQueueMetadata(ctx context.Context, queueName string) (*chronoqueue.Queue_Options, error) {
	queueMetaKey := fmt.Sprintf("%s:meta", queueName)
	queueMetaResult, err := as.redisClient.HGet(ctx, queueMetaKey, "metadata").Result()
	if err != nil {
		return nil, err
	}
	var queueMeta chronoqueue.Queue_Options
	if err = protojson.Unmarshal([]byte(queueMetaResult), &queueMeta); err != nil {
		return nil, err
	}
	return &queueMeta, nil
}

func (as *storage) updateMessageStateAndLease(message *chronoqueue.Message, request *chronoqueue.GetNextMessageRequest, queueMeta *chronoqueue.Queue_Options) {
	message.Metadata.State = chronoqueue.Message_Metadata_RUNNING
	if message.Metadata.GetLeaseDuration() <= 0 {
		if request.LeaseDuration > 0 {
			message_lease := request.GetLeaseDuration()
			message.Metadata.LeaseDuration = &message_lease
		} else {
			message.Metadata.LeaseDuration = &queueMeta.LeaseDuration
		}
	}

	// Add lease expiry data to the message metadata
	expireDate := time.Now().Add(time.Duration(message.Metadata.GetLeaseDuration())).UnixNano() / int64(time.Millisecond)
	message.Metadata.LeaseExpiry = &expireDate
}

func (as *storage) saveMessageWithMetadata(ctx context.Context, queueName string, message *chronoqueue.Message) error {
	// Create a proto message's metadata marshaller
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	messageMetadataByte, err := m.Marshal(message.Metadata)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s:%s:meta", queueName, message.MessageId)
	return as.redisClient.HSet(ctx, key, "metadata", string(messageMetadataByte)).Err()
}

func (as *storage) fetchQueueMembersBeforeNow(ctx context.Context, queueName string) ([]string, error) {
	max := strconv.FormatInt(time.Now().UnixMilli(), 10)
	return as.redisClient.ZRangeByScore(ctx, queueName, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    max,
		Offset: 0,
		Count:  10,
	}).Result()
}
