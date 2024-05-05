package repository

import (
	"context"
	"errors"
	"fmt"

	queue "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
)

func (as *storage) checkQueueExistence(ctx context.Context, queueName string) (bool, error) {
	exists, err := as.redisClient.Exists(ctx, queueName, fmt.Sprintf("%s:meta", queueName)).Result()
	return exists >= 2, err
}

func (as *storage) setQueueMetadata(ctx context.Context, request *queueservice_pb.CreateQueueRequest, txPipeline redis.Pipeliner) error {
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	queueMetadataByte, err := m.Marshal(request.GetMetadata())
	if err != nil {
		return err
	}

	_, err = txPipeline.HSet(ctx, fmt.Sprintf("%s:meta", request.GetName()), "metadata", string(queueMetadataByte)).Result()
	return err
}

func (as *storage) CreateQueue(ctx context.Context, request *queueservice_pb.CreateQueueRequest) (*queueservice_pb.CreateQueueResponse, error) {
	if request.GetMetadata() == nil || request.GetName() == "" {
		err := errors.New("invalid input: queue name required")
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, err, "Invalid input provided")
		return &queueservice_pb.CreateQueueResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	exists, err := as.checkQueueExistence(ctx, request.GetName())
	if err != nil {
		return &queueservice_pb.CreateQueueResponse{
			Success: false,
		}, err
	}
	if exists {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.AlreadyExists, err, "Queue already exists")
		return &queueservice_pb.CreateQueueResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	txPipeline := as.redisClient.TxPipeline()
	_, err = txPipeline.ZAdd(ctx, request.GetName(), redis.Z{}).Result()
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while creating queue")
		return &queueservice_pb.CreateQueueResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	if request.Metadata.GetType() == queue.QueueType_EXCLUSIVE && request.Metadata.GetExclusivityKey() == "" {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, err, "Exclusivity key missing for an EXCLUSIVE queue type")
		return &queueservice_pb.CreateQueueResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	err = as.setQueueMetadata(ctx, request, txPipeline)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while creating queue metadata")
		return &queueservice_pb.CreateQueueResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	_, err = txPipeline.Exec(ctx)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while creating executing pipeline transaction")
		return &queueservice_pb.CreateQueueResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	return &queueservice_pb.CreateQueueResponse{
		Success: true,
	}, nil
}
