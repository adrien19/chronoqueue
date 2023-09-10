package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
)

func (as *storage) checkQueueExistence(ctx context.Context, queueName string) (bool, error) {
	exists, err := as.redisClient.Exists(ctx, queueName, fmt.Sprintf("%s:meta", queueName)).Result()
	return exists >= 2, err
}

func (as *storage) setQueueMetadata(ctx context.Context, queueInfo *chronoqueue.Queue) error {
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	queueMetadataByte, err := m.Marshal(queueInfo.GetMetadata())
	if err != nil {
		return err
	}

	_, err = as.redisClient.HSet(ctx, fmt.Sprintf("%s:meta", queueInfo.GetName()), "metadata", string(queueMetadataByte)).Result()
	return err
}

func (as *storage) CreateQueue(ctx context.Context, request *chronoqueue.CreateQueueRequest) (*chronoqueue.CreateQueueResponse, error) {
	queueInfo := request.GetQueue()
	if queueInfo == nil || queueInfo.GetName() == "" {
		return nil, errors.New("error: queue information missing")
	}

	exists, err := as.checkQueueExistence(ctx, queueInfo.GetName())
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, errors.New("queue already exists")
	}

	txPipeline := as.redisClient.TxPipeline()
	_, err = txPipeline.ZAdd(ctx, queueInfo.GetName(), redis.Z{}).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to create queue. error: %v", err)
	}

	if queueInfo.Metadata.GetType() == chronoqueue.Queue_Options_EXCLUSIVE && queueInfo.Metadata.GetExclusivityKey() == "" {
		return nil, errors.New("exclusivity key missing for an EXCLUSIVE queue type")
	}

	err = as.setQueueMetadata(ctx, queueInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to set queue metadata. error: %v", err)
	}

	_, err = txPipeline.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue in transaction. error: %v", err)
	}

	return &chronoqueue.CreateQueueResponse{}, nil
}
