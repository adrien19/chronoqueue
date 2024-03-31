package repository

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal/encryption"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

// Serialize the metadata payload into JSON
func (as *storage) encryptScheduleMetadataPayload(metadata *chronoqueue.Schedule_Metadata) error {
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

func (as *storage) CreateSchedule(ctx context.Context, request *chronoqueue.CreateScheduleRequest) (*chronoqueue.CreateScheduleResponse, error) {
	scheduleInfo := request.GetSchedule()
	if scheduleInfo == nil || scheduleInfo.GetScheduleId() == "" {
		err := errors.New("creating schedule: schedule ID required")
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, err, "Invalid input provided")
		return &chronoqueue.CreateScheduleResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	exists, err := as.checkScheduleExistence(ctx, scheduleInfo.GetScheduleId())
	if err != nil {
		return &chronoqueue.CreateScheduleResponse{
			Success: false,
		}, err
	}
	if exists {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.AlreadyExists, err, "Schedule already exists")
		return &chronoqueue.CreateScheduleResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	txPipeline := as.redisClient.TxPipeline()
	_, err = txPipeline.ZAdd(ctx, scheduleInfo.GetScheduleId(), redis.Z{}).Result()
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while creating schedule")
		return &chronoqueue.CreateScheduleResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	err = as.setScheduleMetadata(ctx, scheduleInfo, txPipeline)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while creating schedule")
		return &chronoqueue.CreateScheduleResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	_, err = txPipeline.Exec(ctx)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while creating schedule")
		return &chronoqueue.CreateScheduleResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	return &chronoqueue.CreateScheduleResponse{
		Success: true,
	}, nil
}

func (as *storage) checkScheduleExistence(ctx context.Context, scheduleId string) (bool, error) {
	exists, err := as.redisClient.Exists(ctx, scheduleId, fmt.Sprintf("schedule:%s:meta", scheduleId)).Result()
	return exists >= 2, err
}

func (as *storage) setScheduleMetadata(ctx context.Context, scheduleInfo *chronoqueue.Schedule, txPipeline redis.Pipeliner) error {
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}

	err := as.encryptScheduleMetadataPayload(scheduleInfo.Metadata)
	if err != nil {
		return err
	}

	scheduleMetadataByte, err := m.Marshal(scheduleInfo.GetMetadata())
	if err != nil {
		return err
	}

	_, err = txPipeline.HSet(ctx, fmt.Sprintf("schedule:%s:meta", scheduleInfo.GetScheduleId()), "metadata", string(scheduleMetadataByte)).Result()
	return err
}

func (as *storage) DeleteSchedule(ctx context.Context, request *chronoqueue.DeleteScheduleRequest) (*chronoqueue.DeleteScheduleResponse, error) {

	// Create or fetch the mutex for this specific queue
	queueMutex := as.rs.NewMutex("mutex:" + request.GetScheduleId())

	// Try to acquire the lock
	if err := queueMutex.Lock(); err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected while acquiring lock")
		return &chronoqueue.DeleteScheduleResponse{Success: false}, chronoErr.GRPCStatus()
	}

	defer func() {
		// Release the message lock
		if ok, err := queueMutex.Unlock(); !ok || err != nil {
			util.Error("Failed to release schedule lock", err)
		}
	}()

	if request == nil || request.GetScheduleId() == "" {
		return &chronoqueue.DeleteScheduleResponse{Success: false}, errors.New("error: schedule information missing")
	}
	checker := NewKeyChecker(as.redisClient, 100)

	start := time.Now()
	checker.Start(ctx)

	iter := as.redisClient.Scan(ctx, 0, fmt.Sprintf("%s*", request.GetScheduleId()), 0).Iterator()
	for iter.Next(ctx) {
		checker.Add(iter.Val())
	}
	if err := iter.Err(); err != nil {
		return &chronoqueue.DeleteScheduleResponse{Success: false}, err
	}

	deleted := checker.Stop()
	log.Println("deleted", deleted, "keys", "in", time.Since(start))

	return &chronoqueue.DeleteScheduleResponse{Success: true}, nil
}

func (as *storage) GetSchedule(ctx context.Context, request *chronoqueue.GetScheduleRequest) (*chronoqueue.GetScheduleResponse, error) {
	if request == nil || request.GetScheduleId() == "" {
		return nil, errors.New("error: schedule information missing")
	}

	scheduleMetadata, err := as.redisClient.HGet(ctx, fmt.Sprintf("schedule:%s:meta", request.GetScheduleId()), "metadata").Result()
	if err != nil {
		return nil, err
	}

	var metadata chronoqueue.Schedule_Metadata
	err = protojson.Unmarshal([]byte(scheduleMetadata), &metadata)
	if err != nil {
		return nil, err
	}

	return &chronoqueue.GetScheduleResponse{
		Schedule: &chronoqueue.Schedule{
			ScheduleId: request.GetScheduleId(),
			Metadata:   &metadata,
		},
	}, nil
}

func (as *storage) ListSchedules(ctx context.Context, request *chronoqueue.ListSchedulesRequest) (*chronoqueue.ListSchedulesResponse, error) {
	scheduleMetadataIDs, err := as.listMetadataIDs(ctx, "schedule", request.GetPrefix(), 10)
	if err != nil {
		return nil, err
	}

	schedules := make([]*chronoqueue.Schedule, len(scheduleMetadataIDs))
	for i, scheduleMetadataID := range scheduleMetadataIDs {
		scheduleID := strings.Split(scheduleMetadataID, ":")[0]
		metadata, err := as.getScheduleMetadata(ctx, scheduleID)
		if err != nil {
			msg := fmt.Sprintf("error fetching metadata for schedule %s", scheduleID)
			chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, msg)
			return nil, chronoErr.GRPCStatus()
		}

		schedules[i] = &chronoqueue.Schedule{
			ScheduleId: scheduleID,
			Metadata:   metadata,
		}
	}

	return &chronoqueue.ListSchedulesResponse{
		Schedules: schedules,
	}, nil
}

func (as *storage) GetScheduleHistory(ctx context.Context, request *chronoqueue.GetScheduleHistoryRequest) (*chronoqueue.GetScheduleHistoryResponse, error) {
	if request == nil || request.GetScheduleId() == "" {
		return nil, errors.New("error: schedule information missing")
	}

	scheduleMetadata, err := as.redisClient.HGet(ctx, fmt.Sprintf("%s:meta", request.GetScheduleId()), "metadata").Result()
	if err != nil {
		return nil, err
	}

	var metadata chronoqueue.Schedule_Metadata
	err = protojson.Unmarshal([]byte(scheduleMetadata), &metadata)
	if err != nil {
		return nil, err
	}

	return &chronoqueue.GetScheduleHistoryResponse{
		ScheduleHistory: &chronoqueue.ScheduleHistory{
			ScheduleId: request.GetScheduleId(),
			Messages:   []*chronoqueue.Message{},
			NextRun:    metadata.GetNextRun(),
			LastRun:    metadata.GetLastRun(),
			CreatedAt:  metadata.GetCreatedAt(),
			UpdatedAt:  metadata.GetUpdatedAt(),
		},
	}, nil
}

func (as *storage) PauseSchedule(ctx context.Context, request *chronoqueue.PauseScheduleRequest) (*chronoqueue.PauseScheduleResponse, error) {
	if request == nil || request.GetScheduleId() == "" {
		return nil, errors.New("error: schedule information missing")
	}

	scheduleMetadata, err := as.getScheduleMetadata(ctx, request.GetScheduleId())
	if err != nil {
		msg := fmt.Sprintf("error fetching metadata for schedule %s", request.GetScheduleId())
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, msg)
		return nil, chronoErr.GRPCStatus()
	}

	scheduleMetadata.State = chronoqueue.Schedule_Metadata_PAUSED

	err = as.setScheduleMetadata(ctx, &chronoqueue.Schedule{
		ScheduleId: request.GetScheduleId(),
		Metadata:   scheduleMetadata,
	}, as.redisClient.Pipeline())
	if err != nil {
		return nil, err
	}

	return &chronoqueue.PauseScheduleResponse{
		Success: true,
	}, nil
}

func (as *storage) ResumeSchedule(ctx context.Context, request *chronoqueue.ResumeScheduleRequest) (*chronoqueue.ResumeScheduleResponse, error) {
	if request == nil || request.GetScheduleId() == "" {
		return nil, errors.New("error: schedule information missing")
	}

	scheduleMetadata, err := as.getScheduleMetadata(ctx, request.GetScheduleId())
	if err != nil {
		msg := fmt.Sprintf("error fetching metadata for schedule %s", request.GetScheduleId())
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, msg)
		return nil, chronoErr.GRPCStatus()
	}

	scheduleMetadata.State = chronoqueue.Schedule_Metadata_SCHEDULED

	err = as.setScheduleMetadata(ctx, &chronoqueue.Schedule{
		ScheduleId: request.GetScheduleId(),
		Metadata:   scheduleMetadata,
	}, as.redisClient.Pipeline())
	if err != nil {
		return nil, err
	}

	return &chronoqueue.ResumeScheduleResponse{
		Success: true,
	}, nil
}
