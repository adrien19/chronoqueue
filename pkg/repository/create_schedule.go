package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	schedule_pb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	schedule "github.com/adrien19/chronoqueue/pkg/schedule"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
)

func (as *storage) CreateSchedule(ctx context.Context, scheduleInfo *schedule.Schedule) (*queueservice_pb.CreateScheduleResponse, error) {
	if scheduleInfo == nil || scheduleInfo.ScheduleId == "" {
		err := errors.New("creating schedule: schedule ID required")
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, err, "Invalid input provided")
		return &queueservice_pb.CreateScheduleResponse{
			Success: false,
		}, chronoErr.GRPCStatus()
	}

	exists, err := as.checkScheduleExistence(ctx, scheduleInfo.ScheduleId)
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
	_, err = txPipeline.ZAdd(ctx, scheduleInfo.ScheduleId, redis.Z{}).Result()
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

	err = as.PushToQueue(ctx, scheduleInfo.Metadata.QueueName, scheduleInfo.ScheduleId, 0)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected error occured while pushing schedule to queue")
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

func (as *storage) setScheduleMetadata(ctx context.Context, scheduleInfo *schedule.Schedule, txPipeline redis.Pipeliner) error {
	err := scheduleInfo.Metadata.EncryptPayload(as.encryptionKeyManager)
	if err != nil {
		return err
	}
	scheduleMetadataByte, err := scheduleInfo.Metadata.ToBytes()
	if err != nil {
		return err
	}

	as.logger.DebugWithFields("Updating schedule metadata payload", "scheduleID", scheduleInfo.ScheduleId, "metadata", scheduleInfo.Metadata)
	_, err = txPipeline.HSet(ctx, fmt.Sprintf("schedule:%s:meta", scheduleInfo.ScheduleId), "metadata", string(scheduleMetadataByte)).Result()
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

func (as *storage) DeleteSchedule(ctx context.Context, scheduleId string) (*queueservice_pb.DeleteScheduleResponse, error) {
	if scheduleId == "" {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.InvalidArgument, errors.New("error: scheduleId must be provided"), "Failed to delete schedule")
		return &queueservice_pb.DeleteScheduleResponse{Success: false}, chronoErr.GRPCStatus()
	}
	// Create or fetch the mutex for this specific schedule
	scheduleMutex := as.rs.NewMutex("mutex:" + scheduleId)

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

	metadata, err := as.getScheduleMetadata(ctx, scheduleId)
	if err != nil {
		msg := fmt.Sprintf("error fetching metadata for schedule %s", scheduleId)
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, msg)
		return &queueservice_pb.DeleteScheduleResponse{Success: false}, chronoErr.GRPCStatus()
	}

	// Delete the schedule metadata
	_, err = as.redisClient.Del(ctx, fmt.Sprintf("schedule:%s:meta", scheduleId)).Result()
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to delete schedule's metadata")
		return &queueservice_pb.DeleteScheduleResponse{Success: false}, chronoErr.GRPCStatus()
	}

	// Delete the schedule sorted set
	_, err = as.redisClient.Del(ctx, scheduleId).Result()
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to delete schedule's sorted set")
		return &queueservice_pb.DeleteScheduleResponse{Success: false}, chronoErr.GRPCStatus()
	}

	err = as.RemoveFromQueue(ctx, metadata.QueueName, scheduleId)
	if err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to delete scheduleId from queue")
		return &queueservice_pb.DeleteScheduleResponse{Success: false}, chronoErr.GRPCStatus()
	}

	return &queueservice_pb.DeleteScheduleResponse{Success: true}, nil
}

func (as *storage) GetSchedule(ctx context.Context, scheduleId string) (*queueservice_pb.GetScheduleResponse, error) {
	if scheduleId == "" {
		return nil, errors.New("error: schedule information missing")
	}

	scheduleMetadata, err := as.redisClient.HGet(ctx, fmt.Sprintf("schedule:%s:meta", scheduleId), "metadata").Result()
	if err != nil {
		return nil, err
	}

	var metadata schedule_pb.Schedule_Metadata
	err = protojson.Unmarshal([]byte(scheduleMetadata), &metadata)
	if err != nil {
		as.logger.WarnWithFields("Failed to unmarshal schedule metadata", "scheduleID", scheduleId, "error", err)
		return nil, err
	}

	return &queueservice_pb.GetScheduleResponse{
		Schedule: &schedule_pb.Schedule{
			ScheduleId: scheduleId,
			Metadata:   &metadata,
		},
	}, nil
}

func (as *storage) ListSchedules(ctx context.Context, prefix string) (*queueservice_pb.ListSchedulesResponse, error) {
	scheduleMetadataIDs, err := as.listMetadataIDs(ctx, "schedule", prefix, 100)
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

func (as *storage) GetScheduleMessages(ctx context.Context, scheduleId string, limit int64) ([]*message_pb.Message, error) {
	if scheduleId == "" {
		return nil, errors.New("error: must provide scheduleId")
	}
	if limit == 0 {
		limit = -1
	}

	// Retrieve messageIds from sorted list set in ascending order of timestamp
	messageIds, err := as.redisClient.ZRange(ctx, scheduleId, 0, limit).Result()
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

func (as *storage) GetScheduleHistory(ctx context.Context, scheduleId string, limit int64) (*queueservice_pb.GetScheduleHistoryResponse, error) {
	if scheduleId == "" {
		return nil, errors.New("error: schedule information missing")
	}

	scheduleMetadata, err := as.redisClient.HGet(ctx, fmt.Sprintf("schedule:%s:meta", scheduleId), "metadata").Result()
	if err != nil {
		return nil, err
	}

	var metadata schedule_pb.Schedule_Metadata
	err = protojson.Unmarshal([]byte(scheduleMetadata), &metadata)
	if err != nil {
		return nil, err
	}

	scheduleMessages, err := as.GetScheduleMessages(ctx, scheduleId, limit)
	if err != nil {
		return nil, err
	}

	return &queueservice_pb.GetScheduleHistoryResponse{
		ScheduleHistory: &schedule_pb.ScheduleHistory{
			ScheduleId: scheduleId,
			Messages:   scheduleMessages,
			NextRun:    metadata.GetNextRun(),
			LastRun:    metadata.GetLastRun(),
			CreatedAt:  metadata.GetCreatedAt(),
			UpdatedAt:  metadata.GetUpdatedAt(),
		},
	}, nil
}

func (as *storage) PauseSchedule(ctx context.Context, scheduleId string) (*queueservice_pb.PauseScheduleResponse, error) {
	if scheduleId == "" {
		return nil, errors.New("error: schedule information missing")
	}
	// Create or fetch the mutex for this specific schedule
	scheduleMutex := as.rs.NewMutex("mutex:" + "schedule" + scheduleId + ":meta")
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

	scheduleMetadata, err := as.getScheduleMetadata(ctx, scheduleId)
	if err != nil {
		msg := fmt.Sprintf("error fetching metadata for schedule %s", scheduleId)
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, msg)
		return nil, chronoErr.GRPCStatus()
	}

	scheduleMetadata.State = schedule_pb.Schedule_Metadata_PAUSED

	err = as.setPausedScheduleMetadata(ctx, &schedule_pb.Schedule{
		ScheduleId: scheduleId,
		Metadata:   scheduleMetadata,
	})
	if err != nil {
		return nil, err
	}

	return &queueservice_pb.PauseScheduleResponse{
		Success: true,
	}, nil
}

func (as *storage) ResumeSchedule(ctx context.Context, scheduleId string) (*queueservice_pb.ResumeScheduleResponse, error) {
	if scheduleId == "" {
		return nil, errors.New("error: schedule information missing")
	}

	// Create or fetch the mutex for this specific schedule
	scheduleMutex := as.rs.NewMutex("mutex:" + "schedule" + scheduleId + ":meta")
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

	scheduleMetadata, err := as.getScheduleMetadata(ctx, scheduleId)
	if err != nil {
		msg := fmt.Sprintf("error fetching metadata for schedule %s", scheduleId)
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, msg)
		return nil, chronoErr.GRPCStatus()
	}

	scheduleMetadata.State = schedule_pb.Schedule_Metadata_SCHEDULED

	err = as.setPausedScheduleMetadata(ctx, &schedule_pb.Schedule{
		ScheduleId: scheduleId,
		Metadata:   scheduleMetadata,
	})
	if err != nil {
		return nil, err
	}

	return &queueservice_pb.ResumeScheduleResponse{
		Success: true,
	}, nil
}

func (s *storage) RecordExecution(ctx context.Context, execution *schedule.Execution) error {
	executionJSON, err := json.Marshal(execution)
	if err != nil {
		return err
	}

	err = s.redisClient.Set(ctx, fmt.Sprintf("execution:%s", execution.ID), executionJSON, 0).Err()
	if err != nil {
		return err
	}

	err = s.redisClient.SAdd(ctx, fmt.Sprintf("executions:%s", execution.ScheduleID), execution.ID).Err()
	return err
}

func (s *storage) ListExecutions(ctx context.Context, scheduleID string) ([]*schedule.Execution, error) {
	executionIDs, err := s.redisClient.SMembers(ctx, fmt.Sprintf("executions:%s", scheduleID)).Result()
	if err != nil {
		return nil, err
	}

	var executions []*schedule.Execution
	for _, execID := range executionIDs {
		executionJSON, err := s.redisClient.Get(ctx, fmt.Sprintf("execution:%s", execID)).Result()
		if err != nil {
			return nil, err
		}

		var execution schedule.Execution
		err = json.Unmarshal([]byte(executionJSON), &execution)
		if err != nil {
			return nil, err
		}

		executions = append(executions, &execution)
	}

	return executions, nil
}

func (s *storage) PushToQueue(ctx context.Context, queueName string, scheduleID string, priority float64) error {
	lockKey := "lock:queue:" + queueName
	lockID, err := s.lock(ctx, lockKey, 5*time.Second)
	if err != nil {
		return err
	}
	defer s.unlock(ctx, lockKey, lockID)
	return s.redisClient.ZAdd(ctx, "queue:"+queueName, redis.Z{
		Score:  priority,
		Member: scheduleID,
	}).Err()
}

func (s *storage) PopFromQueue(ctx context.Context, queueName string) (string, error) {
	lockKey := "lock:queue:" + queueName
	lockID, err := s.lock(ctx, lockKey, 5*time.Second)
	if err != nil {
		return "", err
	}
	defer s.unlock(ctx, lockKey, lockID)

	result, err := s.redisClient.ZPopMin(ctx, "queue:"+queueName).Result()
	if err != nil {
		return "", err
	}
	if len(result) == 0 {
		return "", fmt.Errorf("no items in queue")
	}
	return result[0].Member.(string), nil
}

func (s *storage) RemoveFromQueue(ctx context.Context, queueName, memberID string) error {
	lockKey := "lock:queue:" + queueName
	lockID, err := s.lock(ctx, lockKey, 5*time.Second)
	if err != nil {
		return err
	}
	defer s.unlock(ctx, lockKey, lockID)

	result, err := s.redisClient.ZRem(ctx, "queue:"+queueName, memberID).Result()
	if err != nil {
		return err
	}
	s.logger.DebugWithFields("Removed item from queue", "queueName", queueName, "memberID", memberID, "total", result)
	return nil
}

func (s *storage) lock(ctx context.Context, key string, timeout time.Duration) (string, error) {
	lockID := uuid.New().String()
	ok, err := s.redisClient.SetNX(ctx, key, lockID, timeout).Result()
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("unable to acquire lock")
	}
	return lockID, nil
}

func (s *storage) unlock(ctx context.Context, key, lockID string) error {
	currentLockID, err := s.redisClient.Get(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	if currentLockID == lockID {
		_, err = s.redisClient.Del(ctx, key).Result()
	}
	return err
}

func (s *storage) LockSchedule(ctx context.Context, scheduleID string, workerID string, ttl time.Duration) (bool, error) {
	lockKey := "lock:schedule:" + scheduleID
	ok, err := s.redisClient.SetNX(ctx, lockKey, workerID, ttl).Result()
	return ok, err
}

func (s *storage) UnlockSchedule(ctx context.Context, scheduleID string, workerID string) error {
	lockKey := "lock:schedule:" + scheduleID
	currentWorkerID, err := s.redisClient.Get(ctx, lockKey).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	if currentWorkerID == workerID {
		_, err = s.redisClient.Del(ctx, lockKey).Result()
	}
	return err
}
