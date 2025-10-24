package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	schedule_pb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/internal/encryption"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

// Serialize the metadata payload into JSON
func (as *storage) encryptScheduleMetadataPayload(metadata *schedule_pb.Schedule_Metadata) error {
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

func (as *storage) CreateSchedule(ctx context.Context, request *queueservice_pb.CreateScheduleRequest) (*queueservice_pb.CreateScheduleResponse, error) {
	scheduleInfo := request.GetSchedule()
	if scheduleInfo == nil || scheduleInfo.GetScheduleId() == "" {
		err := errors.New("creating schedule: schedule ID required")
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, err, "Invalid input provided")
		return &queueservice_pb.CreateScheduleResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	// Validate calendar schedule if present
	if scheduleInfo.GetMetadata().GetCalendarSchedule() != nil {
		if err := as.ValidateCalendarSchedule(ctx, scheduleInfo.GetMetadata().GetCalendarSchedule()); err != nil {
			chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, err, "Invalid calendar schedule configuration")
			return &queueservice_pb.CreateScheduleResponse{
				Success: false,
			}, chronoErr.GRPCStatus()
		}
		as.logger.InfoWithFields("Calendar schedule validated successfully", "scheduleID", scheduleInfo.GetScheduleId())
	}

	exists, err := as.checkScheduleExistence(ctx, scheduleInfo.GetScheduleId())
	if err != nil {
		return &queueservice_pb.CreateScheduleResponse{
			Success: false,
		}, err
	}
	if exists {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.AlreadyExists, err, "Schedule already exists")
		return &queueservice_pb.CreateScheduleResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	txPipeline := as.redisClient.TxPipeline()
	_, err = txPipeline.ZAdd(ctx, scheduleInfo.GetScheduleId(), redis.Z{}).Result()
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while creating schedule")
		return &queueservice_pb.CreateScheduleResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	err = as.setScheduleMetadata(ctx, scheduleInfo, txPipeline)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while creating schedule")
		return &queueservice_pb.CreateScheduleResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	_, err = txPipeline.Exec(ctx)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while creating schedule")
		return &queueservice_pb.CreateScheduleResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	return &queueservice_pb.CreateScheduleResponse{
		Success: true,
	}, nil
}

func (as *storage) checkScheduleExistence(ctx context.Context, scheduleId string) (bool, error) {
	exists, err := as.redisClient.Exists(ctx, scheduleId, fmt.Sprintf("schedule:%s:meta", scheduleId)).Result()
	return exists >= 2, err
}

func (as *storage) setScheduleMetadata(ctx context.Context, scheduleInfo *schedule_pb.Schedule, txPipeline redis.Pipeliner) error {
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}

	if scheduleInfo.Metadata.Payload.Metadata["encryptedPayload"] == nil && scheduleInfo.Metadata.Payload.Metadata["nonce"] == nil {
		as.logger.InfoWithFields("Encrypting schedule metadata payload", "scheduleID", scheduleInfo.GetScheduleId())
		err := as.encryptScheduleMetadataPayload(scheduleInfo.Metadata)
		if err != nil {
			return err
		}
	}

	scheduleMetadataByte, err := m.Marshal(scheduleInfo.GetMetadata())
	if err != nil {
		return err
	}

	as.logger.DebugWithFields("Updating schedule metadata payload", "scheduleID", scheduleInfo.GetScheduleId(), "metadata", scheduleInfo.GetMetadata())
	_, err = txPipeline.HSet(ctx, fmt.Sprintf("schedule:%s:meta", scheduleInfo.GetScheduleId()), "metadata", string(scheduleMetadataByte)).Result()
	return err
}

func (as *storage) setPausedScheduleMetadata(ctx context.Context, scheduleInfo *schedule_pb.Schedule) error {
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}

	scheduleMetadataByte, err := m.Marshal(scheduleInfo.GetMetadata())
	if err != nil {
		return err
	}

	as.logger.InfoWithFields(
		"Updating schedule metadata payload",
		"scheduleID", scheduleInfo.GetScheduleId(),
		"metadata", scheduleInfo.GetMetadata(),
	)
	_, err = as.redisClient.HSet(ctx, fmt.Sprintf("schedule:%s:meta", scheduleInfo.GetScheduleId()), "metadata", string(scheduleMetadataByte)).Result()
	return err
}

func (as *storage) DeleteSchedule(ctx context.Context, request *queueservice_pb.DeleteScheduleRequest) (*queueservice_pb.DeleteScheduleResponse, error) {

	// Create or fetch the mutex for this specific schedule
	scheduleMutex := as.rs.NewMutex("mutex:" + request.GetScheduleId())

	// Try to acquire the lock
	if err := scheduleMutex.Lock(); err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected while acquiring lock")
		return &queueservice_pb.DeleteScheduleResponse{Success: false}, chronoErr.GRPCStatus()
	}

	defer func() {
		// Release the message lock
		if ok, err := scheduleMutex.Unlock(); !ok || err != nil {
			as.logger.Error("Failed to release schedule lock", err)
		}
	}()

	if request == nil || request.GetScheduleId() == "" {
		return &queueservice_pb.DeleteScheduleResponse{Success: false}, errors.New("error: schedule information missing")
	}
	checker := NewKeyChecker(as.redisClient, 100)

	start := time.Now()
	checker.Start(ctx)

	iter := as.redisClient.Scan(ctx, 0, fmt.Sprintf("*%s*", request.GetScheduleId()), 0).Iterator()
	for iter.Next(ctx) {
		checker.Add(iter.Val())
	}
	if err := iter.Err(); err != nil {
		return &queueservice_pb.DeleteScheduleResponse{Success: false}, err
	}

	deleted := checker.Stop()
	as.logger.DebugWithFields(
		"Deleted keys associated with schedule",
		"total", deleted,
		"scheduleID", request.GetScheduleId(),
		"took", time.Since(start),
	)

	return &queueservice_pb.DeleteScheduleResponse{Success: true}, nil
}

func (as *storage) GetSchedule(ctx context.Context, request *queueservice_pb.GetScheduleRequest) (*queueservice_pb.GetScheduleResponse, error) {
	if request == nil || request.GetScheduleId() == "" {
		return nil, errors.New("error: schedule information missing")
	}

	scheduleMetadata, err := as.redisClient.HGet(ctx, fmt.Sprintf("schedule:%s:meta", request.GetScheduleId()), "metadata").Result()
	if err != nil {
		return nil, err
	}

	var metadata schedule_pb.Schedule_Metadata
	err = protojson.Unmarshal([]byte(scheduleMetadata), &metadata)
	if err != nil {
		return nil, err
	}

	return &queueservice_pb.GetScheduleResponse{
		Schedule: &schedule_pb.Schedule{
			ScheduleId: request.GetScheduleId(),
			Metadata:   &metadata,
		},
	}, nil
}

func (as *storage) ListSchedules(ctx context.Context, request *queueservice_pb.ListSchedulesRequest) (*queueservice_pb.ListSchedulesResponse, error) {
	scheduleMetadataIDs, err := as.listMetadataIDs(ctx, "schedule", request.GetPrefix(), 100)
	if err != nil {
		return nil, err
	}

	schedules := make([]*schedule_pb.Schedule, len(scheduleMetadataIDs))
	for i, scheduleMetadataID := range scheduleMetadataIDs {
		scheduleID := strings.Split(scheduleMetadataID, ":")[1]
		metadata, err := as.getScheduleMetadata(ctx, scheduleID)
		if err != nil {
			msg := fmt.Sprintf("error fetching metadata for schedule %s", scheduleID)
			chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, msg)
			return nil, chronoErr.GRPCStatus()
		}

		schedules[i] = &schedule_pb.Schedule{
			ScheduleId: scheduleID,
			Metadata:   metadata,
		}
	}

	return &queueservice_pb.ListSchedulesResponse{
		Schedules: schedules,
	}, nil
}

func (as *storage) GetScheduleMessages(ctx context.Context, scheduleId string) ([]*message_pb.Message, error) {
	if scheduleId == "" {
		return nil, errors.New("error: must provide scheduleId")
	}

	// Retrieve messageIds from sorted list set in ascending order of timestamp
	messageIds, err := as.redisClient.ZRange(ctx, scheduleId, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	// Remove the first index if it contains an empty member at score 0
	if len(messageIds) > 0 && messageIds[0] == "" {
		messageIds = messageIds[1:]
	}

	// Retrieve associated message's metadata for each messageId
	messages := make([]*message_pb.Message, len(messageIds))
	for i, messageInfo := range messageIds {
		queueName := strings.Split(messageInfo, ":")[0]
		messageId := strings.Split(messageInfo, ":")[1]
		messageMetadata, err := as.redisClient.HGet(ctx, fmt.Sprintf("%s:%s:meta", queueName, messageId), "metadata").Result()
		if err != nil {
			return nil, err
		}

		var metadata message_pb.Message_Metadata
		err = protojson.Unmarshal([]byte(messageMetadata), &metadata)
		if err != nil {
			return nil, err
		}

		messages[i] = &message_pb.Message{
			MessageId: messageId,
			Metadata:  &metadata,
		}
	}

	return messages, nil
}

func (as *storage) GetScheduleHistory(ctx context.Context, request *queueservice_pb.GetScheduleHistoryRequest) (*queueservice_pb.GetScheduleHistoryResponse, error) {
	if request == nil || request.GetScheduleId() == "" {
		return nil, errors.New("error: schedule information missing")
	}

	scheduleMetadata, err := as.redisClient.HGet(ctx, fmt.Sprintf("schedule:%s:meta", request.GetScheduleId()), "metadata").Result()
	if err != nil {
		return nil, err
	}

	var metadata schedule_pb.Schedule_Metadata
	err = protojson.Unmarshal([]byte(scheduleMetadata), &metadata)
	if err != nil {
		return nil, err
	}

	scheduleMessages, err := as.GetScheduleMessages(ctx, request.GetScheduleId())
	if err != nil {
		return nil, err
	}

	return &queueservice_pb.GetScheduleHistoryResponse{
		ScheduleHistory: &schedule_pb.ScheduleHistory{
			ScheduleId: request.GetScheduleId(),
			Messages:   scheduleMessages,
			NextRun:    metadata.GetNextRun(),
			LastRun:    metadata.GetLastRun(),
			CreatedAt:  metadata.GetCreatedAt(),
			UpdatedAt:  metadata.GetUpdatedAt(),
		},
	}, nil
}

func (as *storage) PauseSchedule(ctx context.Context, request *queueservice_pb.PauseScheduleRequest) (*queueservice_pb.PauseScheduleResponse, error) {
	if request == nil || request.GetScheduleId() == "" {
		return nil, errors.New("error: schedule information missing")
	}
	// Create or fetch the mutex for this specific schedule
	scheduleMutex := as.rs.NewMutex("mutex:" + "schedule" + request.GetScheduleId() + ":meta")
	if err := scheduleMutex.Lock(); err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected while acquiring lock")
		return &queueservice_pb.PauseScheduleResponse{Success: false}, chronoErr.GRPCStatus()
	}

	defer func() {
		// Release the message lock
		if ok, err := scheduleMutex.Unlock(); !ok || err != nil {
			as.logger.Error("Failed to release schedule lock", err)
		}
	}()

	scheduleMetadata, err := as.getScheduleMetadata(ctx, request.GetScheduleId())
	if err != nil {
		msg := fmt.Sprintf("error fetching metadata for schedule %s", request.GetScheduleId())
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, msg)
		return nil, chronoErr.GRPCStatus()
	}

	scheduleMetadata.State = schedule_pb.Schedule_Metadata_PAUSED

	err = as.setPausedScheduleMetadata(ctx, &schedule_pb.Schedule{
		ScheduleId: request.GetScheduleId(),
		Metadata:   scheduleMetadata,
	})
	if err != nil {
		return nil, err
	}

	return &queueservice_pb.PauseScheduleResponse{
		Success: true,
	}, nil
}

func (as *storage) ResumeSchedule(ctx context.Context, request *queueservice_pb.ResumeScheduleRequest) (*queueservice_pb.ResumeScheduleResponse, error) {
	if request == nil || request.GetScheduleId() == "" {
		return nil, errors.New("error: schedule information missing")
	}

	// Create or fetch the mutex for this specific schedule
	scheduleMutex := as.rs.NewMutex("mutex:" + "schedule" + request.GetScheduleId() + ":meta")
	if err := scheduleMutex.Lock(); err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected while acquiring lock")
		return &queueservice_pb.ResumeScheduleResponse{Success: false}, chronoErr.GRPCStatus()
	}

	defer func() {
		// Release the message lock
		if ok, err := scheduleMutex.Unlock(); !ok || err != nil {
			as.logger.Error("Failed to release schedule lock", err)
		}
	}()

	scheduleMetadata, err := as.getScheduleMetadata(ctx, request.GetScheduleId())
	if err != nil {
		msg := fmt.Sprintf("error fetching metadata for schedule %s", request.GetScheduleId())
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, msg)
		return nil, chronoErr.GRPCStatus()
	}

	scheduleMetadata.State = schedule_pb.Schedule_Metadata_SCHEDULED

	err = as.setPausedScheduleMetadata(ctx, &schedule_pb.Schedule{
		ScheduleId: request.GetScheduleId(),
		Metadata:   scheduleMetadata,
	})
	if err != nil {
		return nil, err
	}

	return &queueservice_pb.ResumeScheduleResponse{
		Success: true,
	}, nil
}
