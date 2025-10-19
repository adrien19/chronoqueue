package repository

import (
	"context"
	"errors"
	"fmt"

	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
)

func (as *storage) checkQueueExistence(ctx context.Context, queueName string) (bool, error) {
	exists, err := as.redisClient.Exists(ctx, queueName, fmt.Sprintf("queue:%s:meta", queueName)).Result()
	return exists >= 1, err
}

func (as *storage) setQueueMetadata(ctx context.Context, request *queueservice_pb.CreateQueueRequest, txPipeline redis.Pipeliner) error {
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	queueMetadataByte, err := m.Marshal(request.GetMetadata())
	if err != nil {
		return err
	}

	_, err = txPipeline.HSet(ctx, fmt.Sprintf("queue:%s:meta", request.GetName()), "metadata", string(queueMetadataByte)).Result()
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
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occurred while checking queue existence")
		return &queueservice_pb.CreateQueueResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}
	if exists {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.AlreadyExists, err, "Queue already exists")
		return &queueservice_pb.CreateQueueResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	txPipeline := as.redisClient.TxPipeline()
	queueID := "queue:" + request.GetName() // add prefix to avoid key collisions
	_, err = txPipeline.ZAdd(ctx, queueID, redis.Z{}).Result()
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while creating queue")
		return &queueservice_pb.CreateQueueResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	if request.Metadata.GetType() == queue_pb.QueueType_EXCLUSIVE && request.Metadata.GetExclusivityKey() == "" {
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

	// create dlq if auto dlq is enabled
	if request.GetMetadata().GetAutoCreateDlq() {
		dlqName := fmt.Sprintf("%s_dlq", request.GetName())
		if request.GetMetadata().GetDeadLetterQueueName() != "" {
			dlqName = request.GetMetadata().GetDeadLetterQueueName()
		}
		dlqRequest := &queueservice_pb.CreateQueueRequest{
			Name: dlqName,
			Metadata: &queue_pb.QueueMetadata{
				Type:                 request.GetMetadata().GetType(),
				AutoCreateDlq:        false,
				ExclusivityKey:       request.GetMetadata().GetExclusivityKey(),
				LeaseDuration:        request.GetMetadata().GetLeaseDuration(),
				InvisibilityDuration: request.GetMetadata().GetInvisibilityDuration(),
				DefaultMaxAttempts:   request.GetMetadata().GetDefaultMaxAttempts(),
			},
		}
		err = as.setQueueMetadata(ctx, dlqRequest, txPipeline)
		if err != nil {
			chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while creating DLQ metadata")
			return &queueservice_pb.CreateQueueResponse{
				Success: false,
			}, chronoErr.GRPCStatus()
		}
		_, err = txPipeline.ZAdd(ctx, "queue:"+dlqName, redis.Z{}).Result()
		if err != nil {
			chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while creating DLQ")
			return &queueservice_pb.CreateQueueResponse{
				Success: false,
			}, chronoErr.GRPCStatus()
		}
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
