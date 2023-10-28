package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
)

func (as *storage) checkQueueExistence(ctx context.Context, queueName string) (bool, error) {
	exists, err := as.redisClient.Exists(ctx, queueName, fmt.Sprintf("%s:meta", queueName)).Result()
	return exists >= 2, err
}

func (as *storage) setQueueMetadata(ctx context.Context, queueInfo *chronoqueue.Queue, txPipeline redis.Pipeliner) error {
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	queueMetadataByte, err := m.Marshal(queueInfo.GetMetadata())
	if err != nil {
		return err
	}

	_, err = txPipeline.HSet(ctx, fmt.Sprintf("%s:meta", queueInfo.GetName()), "metadata", string(queueMetadataByte)).Result()
	return err
}

func (as *storage) CreateQueue(ctx context.Context, request *chronoqueue.CreateQueueRequest) (*chronoqueue.CreateQueueResponse, error) {
	queueInfo := request.GetQueue()
	if queueInfo == nil || queueInfo.GetName() == "" {
		err := errors.New("invalid input: queue name required")
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, err, "Invalid input provided")
		return nil, chronoErr.GRPCStatus()
	}

	exists, err := as.checkQueueExistence(ctx, queueInfo.GetName())
	if err != nil {
		return nil, err
	}
	if exists {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.AlreadyExists, err, "Queue already exists")
		return nil, chronoErr.GRPCStatus()
	}

	txPipeline := as.redisClient.TxPipeline()
	_, err = txPipeline.ZAdd(ctx, queueInfo.GetName(), redis.Z{}).Result()
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while creating queue")
		return nil, chronoErr.GRPCStatus()
	}

	if queueInfo.Metadata.GetType() == chronoqueue.Queue_Options_EXCLUSIVE && queueInfo.Metadata.GetExclusivityKey() == "" {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, err, "Exclusivity key missing for an EXCLUSIVE queue type")
		return nil, chronoErr.GRPCStatus()
	}

	err = as.setQueueMetadata(ctx, queueInfo, txPipeline)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while creating queue metadata")
		return nil, chronoErr.GRPCStatus()
	}

	_, err = txPipeline.Exec(ctx)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while creating executing pipeline transaction")
		return nil, chronoErr.GRPCStatus()
	}

	return &chronoqueue.CreateQueueResponse{}, nil
}
