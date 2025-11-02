package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	schedule_pb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
	"github.com/adrien19/chronoqueue/internal/util"
	log "github.com/adrien19/chronoqueue/pkg/log"
	"github.com/adrien19/chronoqueue/pkg/validator"

	"github.com/go-redsync/redsync/v4"
	goredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// DLQStats represents statistics about a Dead Letter Queue
type DLQStats struct {
	Name         string `json:"name"`
	MessageCount int64  `json:"message_count"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

type Storage interface {
	CreateQueue(ctx context.Context, request *queueservice_pb.CreateQueueRequest) (*queueservice_pb.CreateQueueResponse, error)
	GetQueueMetadata(ctx context.Context, queueName string) (*queue_pb.QueueMetadata, error)
	DeleteQueue(ctx context.Context, request *queueservice_pb.DeleteQueueRequest) (*queueservice_pb.DeleteQueueResponse, error)
	CreateQueueMessage(ctx context.Context, request *queueservice_pb.PostMessageRequest, validator validator.Validator) (*queueservice_pb.PostMessageResponse, error)
	GetQueueMessage(ctx context.Context, request *queueservice_pb.GetNextMessageRequest) (*queueservice_pb.GetNextMessageResponse, error)
	DeleteQueueMessage(ctx context.Context, queueName string, messageID string) error
	AcknowledgeMessage(ctx context.Context, request *queueservice_pb.AcknowledgeMessageRequest) (*queueservice_pb.AcknowledgeMessageResponse, error)
	RenewMessageLease(ctx context.Context, request *queueservice_pb.RenewMessageLeaseRequest) (*queueservice_pb.RenewMessageLeaseResponse, error)
	PeekQueueMessages(ctx context.Context, request *queueservice_pb.PeekQueueMessagesRequest) (*queueservice_pb.PeekQueueMessagesResponse, error)
	GetQueueState(ctx context.Context, request *queueservice_pb.GetQueueStateRequest) (*queueservice_pb.GetQueueStateResponse, error)
	SendMessageHeartBeat(ctx context.Context, request *queueservice_pb.SendMessageHeartBeatRequest) (*queueservice_pb.SendMessageHeartBeatResponse, error)
	ListQueues(ctx context.Context, request *queueservice_pb.ListQueuesRequest) (*queueservice_pb.ListQueuesResponse, error)
	CreateSchedule(ctx context.Context, request *queueservice_pb.CreateScheduleRequest) (*queueservice_pb.CreateScheduleResponse, error)
	DeleteSchedule(ctx context.Context, request *queueservice_pb.DeleteScheduleRequest) (*queueservice_pb.DeleteScheduleResponse, error)
	GetSchedule(ctx context.Context, request *queueservice_pb.GetScheduleRequest) (*queueservice_pb.GetScheduleResponse, error)
	ListSchedules(ctx context.Context, request *queueservice_pb.ListSchedulesRequest) (*queueservice_pb.ListSchedulesResponse, error)
	GetScheduleHistory(ctx context.Context, request *queueservice_pb.GetScheduleHistoryRequest) (*queueservice_pb.GetScheduleHistoryResponse, error)
	PauseSchedule(ctx context.Context, request *queueservice_pb.PauseScheduleRequest) (*queueservice_pb.PauseScheduleResponse, error)
	ResumeSchedule(ctx context.Context, request *queueservice_pb.ResumeScheduleRequest) (*queueservice_pb.ResumeScheduleResponse, error)

	// Calendar schedule operations
	ValidateCalendarSchedule(ctx context.Context, calendarSchedule *schedule_pb.CalendarSchedule) error
	GetCalendarSchedulePreview(ctx context.Context, calendarSchedule *schedule_pb.CalendarSchedule, count int) (*queueservice_pb.PreviewCalendarScheduleResponse, error)

	// DLQ management methods
	GetDLQMessages(ctx context.Context, dlqName string, limit int32) ([]*message_pb.Message, error)
	RequeueFromDLQ(ctx context.Context, dlqName string, messageID string, targetQueueName string, resetRetries bool) error
	DeleteFromDLQ(ctx context.Context, dlqName string, messageID string) error
	PurgeDLQ(ctx context.Context, dlqName string) error
	GetDLQStats(ctx context.Context, dlqName string) (*DLQStats, error)
}

type storage struct {
	redisClient          *redis.Client
	rs                   *redsync.Redsync
	encryptionKeyManager *keymanager.EncryptionKeyManager
	logger               *log.Logger
	scheduler            *Scheduler
	reclaimService       *ReclaimService
}

func NewQueueStorage(ctx context.Context, redisClient *redis.Client, encryptionKeyManager *keymanager.EncryptionKeyManager, logger *log.Logger) Storage {
	return NewQueueStorageWithIntervals(ctx, redisClient, encryptionKeyManager, logger, time.Second, 5*time.Second)
}

// NewQueueStorageWithIntervals creates a storage instance with custom scheduler and reclaim intervals
func NewQueueStorageWithIntervals(ctx context.Context, redisClient *redis.Client, encryptionKeyManager *keymanager.EncryptionKeyManager, logger *log.Logger, schedulerInterval, reclaimInterval time.Duration) Storage {
	pool := goredis.NewPool(redisClient)
	rs := redsync.New(pool)
	storage := &storage{
		redisClient:          redisClient,
		rs:                   rs,
		encryptionKeyManager: encryptionKeyManager,
		logger:               logger,
	}

	scheduler := NewScheduler(storage, schedulerInterval)
	storage.scheduler = scheduler
	go scheduler.Start(ctx)

	// Start the reclaim service for XAUTOCLAIM-based stuck message recovery
	reclaimService := NewReclaimService(storage, reclaimInterval)
	storage.reclaimService = reclaimService
	go reclaimService.Start(ctx)

	// Start background task for cron schedule updates
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := storage.updateAllCronSchedules(ctx); err != nil {
					storage.logger.ErrorWithFields("Failed to update cron schedules", "error", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return storage
}

// NewQueueStorageForTesting creates a storage instance without background workers for testing
func NewQueueStorageForTesting(redisClient *redis.Client, encryptionKeyManager *keymanager.EncryptionKeyManager, logger *log.Logger) Storage {
	pool := goredis.NewPool(redisClient)
	rs := redsync.New(pool)
	return &storage{
		redisClient:          redisClient,
		rs:                   rs,
		encryptionKeyManager: encryptionKeyManager,
		logger:               logger,
	}
}

func (as *storage) DeleteQueue(ctx context.Context, request *queueservice_pb.DeleteQueueRequest) (*queueservice_pb.DeleteQueueResponse, error) {
	// Create or fetch the mutex for this specific queue
	queueMutex := as.rs.NewMutex("mutex:" + request.GetName())

	// Try to acquire the lock
	if err := queueMutex.Lock(); err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected while acquiring lock")
		return &queueservice_pb.DeleteQueueResponse{Success: false}, chronoErr.GRPCStatus()
	}

	defer func() {
		// Release the message lock
		if ok, err := queueMutex.Unlock(); !ok || err != nil {
			as.logger.ErrorWithFields("Failed to release queue lock", "error", err)
		}
	}()

	if request == nil || request.GetName() == "" {
		return &queueservice_pb.DeleteQueueResponse{Success: false}, errors.New("error: queue information missing")
	}

	queueName := request.GetName()
	checker := NewKeyChecker(as.redisClient, 100)

	start := time.Now()
	checker.Start(ctx)

	// Delete legacy queue sorted set and metadata
	iter := as.redisClient.Scan(ctx, 0, fmt.Sprintf("queue:%s*", queueName), 0).Iterator()
	for iter.Next(ctx) {
		checker.Add(iter.Val())
	}
	if err := iter.Err(); err != nil {
		return &queueservice_pb.DeleteQueueResponse{Success: false}, err
	}

	// Delete all priority streams for this queue
	priorityLevels := []string{"high", "medium", "low"}
	groupKey := as.groupKey(queueName)

	for _, level := range priorityLevels {
		streamKey := fmt.Sprintf("stream:%s:%s", level, queueName)

		// Destroy consumer group first if it exists
		err := as.redisClient.XGroupDestroy(ctx, streamKey, groupKey).Err()
		if err != nil && err != redis.Nil {
			as.logger.DebugWithFields("Failed to destroy consumer group", "stream", streamKey, "group", groupKey, "error", err)
		}

		// Delete the stream
		checker.Add(streamKey)
	}

	// Delete scheduled messages sorted set
	scheduleKey := as.scheduleKey(queueName)
	checker.Add(scheduleKey)

	// Delete message metadata keys
	metaIter := as.redisClient.Scan(ctx, 0, fmt.Sprintf("%s:*:meta", queueName), 0).Iterator()
	for metaIter.Next(ctx) {
		checker.Add(metaIter.Val())
	}
	if err := metaIter.Err(); err != nil {
		as.logger.DebugWithFields("Error scanning message metadata keys", "error", err)
	}

	// Delete DLQ stream if exists
	dlqStream := as.dlqStreamKey(queueName)
	err := as.redisClient.XGroupDestroy(ctx, dlqStream, groupKey).Err()
	if err != nil && err != redis.Nil {
		as.logger.DebugWithFields("Failed to destroy DLQ consumer group", "stream", dlqStream, "error", err)
	}
	checker.Add(dlqStream)

	deleted := checker.Stop()
	as.logger.DebugWithFields(
		"Deleted keys associated with queue",
		"total", deleted,
		"queue", queueName,
		"took", time.Since(start),
	)

	return &queueservice_pb.DeleteQueueResponse{Success: true}, nil
}

func (as *storage) DeleteQueueMessage(ctx context.Context, queueName string, messageID string) error {
	_, err := as.redisClient.Del(ctx, messageID).Result()
	if err != nil {
		return err
	}
	return nil
}

func calculateNextRunTime(cronSchedule string) (int64, error) {
	schedule, err := cron.ParseStandard(cronSchedule)
	if err != nil {
		return 0, err
	}
	nextRun := schedule.Next(time.Now())
	return nextRun.UnixMilli(), nil
}

func (as *storage) updateMessageCronSchedule(ctx context.Context, key string, metadata *schedule_pb.Schedule_Metadata) error {
	scheduleID := strings.Split(key, ":")[1]
	queueID := metadata.GetQueueName()

	// Create or fetch the mutex for this specific queue
	scheduleMutex := as.rs.NewMutex("mutex:" + key)
	// Try to acquire the lock with timeout
	if err := scheduleMutex.Lock(); err != nil {
		return err
	}
	defer func() {
		// Release the message lock
		if ok, err := scheduleMutex.Unlock(); !ok || err != nil {
			as.logger.Error("Failed to release message lock", err)
		}
	}()

	cronSchedule := metadata.GetCronSchedule()
	if cronSchedule != "" && (metadata.LastRun == nil || (metadata.LastRun.AsTime().Before(time.Now()) && metadata.NextRun.AsTime().Before(time.Now()))) {
		// Calculate the next run time for this message
		nextRunTime, err := calculateNextRunTime(cronSchedule)
		if err != nil {
			return err
		}

		// Update the next run time in Redis for the new message instance
		metadata.LastRun = metadata.NextRun
		metadata.NextRun = timestamppb.New(time.Unix(0, nextRunTime*int64(time.Millisecond)))

		// Create a proto message's metadata marshaller
		m := protojson.MarshalOptions{
			EmitUnpopulated: true,
		}

		runMessageInstanceMetadata := message_pb.Message_Metadata{
			Priority:      metadata.Priority,
			LeaseDuration: metadata.LeaseDuration,
			LeaseExpiry:   0,
			InvisibilityDuration: &durationpb.Duration{
				Seconds: nextRunTime / int64(time.Millisecond),
			},
			AttemptsLeft:      1,
			State:             message_pb.Message_Metadata_INVISIBLE,
			Payload:           metadata.Payload,
			LeaseRenewalCount: 0,
		}

		randomID, err := util.GenerateID()
		if err != nil {
			return err
		}

		_, err = as.CreateQueueMessage(ctx, &queueservice_pb.PostMessageRequest{
			Message: &message_pb.Message{
				MessageId: randomID,
				Metadata:  &runMessageInstanceMetadata,
			},
			QueueName: queueID,
		}, nil)
		if err != nil {
			return err
		}

		// Log the update for the schedule
		as.logger.InfoWithFields(
			"Updating cron schedule for schedule",
			"scheduleMetadataID", key,
			"state", metadata.State,
		)

		// Update the schedule with the run times
		scheduleMetadataByte, err := m.Marshal(metadata)
		if err != nil {
			return err
		}

		_, err = as.redisClient.HSet(ctx, key, "metadata", string(scheduleMetadataByte)).Result()
		if err != nil {
			return err
		}

		messageInfo := fmt.Sprintf("%s:%s", queueID, randomID)
		// Add the message to the schedule sorted set using the next run time as score
		_, err = as.redisClient.ZAdd(ctx, scheduleID, redis.Z{
			Score:  float64(nextRunTime),
			Member: messageInfo,
		}).Result()
		if err != nil {
			return err
		}
	}

	return nil
}

func (as *storage) updateAllCronSchedules(ctx context.Context) error {
	// Fetch all cron schedules
	var cursor uint64
	var err error
	var keys []string

	for {
		keys, cursor, err = as.redisClient.Scan(ctx, cursor, "schedule:*:meta", 10).Result()
		if err != nil {
			return err
		}

		for _, key := range keys {
			metadata, err := as.getScheduleMetadata(ctx, key)
			if err != nil {
				return err
			}
			if metadata.State == schedule_pb.Schedule_Metadata_SCHEDULED {
				// Process cron schedules
				if metadata.GetCronSchedule() != "" {
					err = as.updateMessageCronSchedule(ctx, key, metadata)
					if err != nil {
						return err
					}
				}
				// Process calendar schedules
				if metadata.GetCalendarSchedule() != nil {
					err = as.updateMessageCalendarSchedule(ctx, key, metadata)
					if err != nil {
						as.logger.ErrorWithFields("Failed to update calendar schedule", err, "key", key)
						// Continue processing other schedules even if one fails
						continue
					}
				}
			}
		}

		if cursor == 0 {
			break
		}
	}

	return nil
}
