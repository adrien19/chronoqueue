package repository

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Storage interface {
	CreateQueue(ctx context.Context, request *chronoqueue.CreateQueueRequest) (*chronoqueue.CreateQueueResponse, error)
	DeleteQueue(ctx context.Context, request *chronoqueue.DeleteQueueRequest) (*chronoqueue.DeleteQueueResponse, error)
	CreateQueueMessage(ctx context.Context, request *chronoqueue.PostMessageRequest) (*chronoqueue.PostMessageResponse, error)
	GetQueueMessage(ctx context.Context, request *chronoqueue.GetNextMessageRequest) (*chronoqueue.GetNextMessageResponse, error)
	DeleteQueueMessage(ctx context.Context, queueName string, messageID string) error
	AcknowledgeMessage(ctx context.Context, request *chronoqueue.AcknowledgeMessageRequest) (*chronoqueue.AcknowledgeMessageResponse, error)
	RenewMessageLease(ctx context.Context, request *chronoqueue.RenewMessageLeaseRequest) (*chronoqueue.RenewMessageLeaseResponse, error)
	PeekQueueMessages(ctx context.Context, request *chronoqueue.PeekQueueMessagesRequest) (*chronoqueue.PeekQueueMessagesResponse, error)
	GetQueueState(ctx context.Context, request *chronoqueue.GetQueueStateRequest) (*chronoqueue.GetQueueStateResponse, error)
	SendMessageHeartBeat(ctx context.Context, request *chronoqueue.SendMessageHeartBeatRequest) (*chronoqueue.SendMessageHeartBeatResponse, error)
	ListQueues(ctx context.Context, request *chronoqueue.ListQueuesRequest) (*chronoqueue.ListQueuesResponse, error)
	CreateSchedule(ctx context.Context, request *chronoqueue.CreateScheduleRequest) (*chronoqueue.CreateScheduleResponse, error)
	DeleteSchedule(ctx context.Context, request *chronoqueue.DeleteScheduleRequest) (*chronoqueue.DeleteScheduleResponse, error)
	GetSchedule(ctx context.Context, request *chronoqueue.GetScheduleRequest) (*chronoqueue.GetScheduleResponse, error)
	ListSchedules(ctx context.Context, request *chronoqueue.ListSchedulesRequest) (*chronoqueue.ListSchedulesResponse, error)
	GetScheduleHistory(ctx context.Context, request *chronoqueue.GetScheduleHistoryRequest) (*chronoqueue.GetScheduleHistoryResponse, error)
	PauseSchedule(ctx context.Context, request *chronoqueue.PauseScheduleRequest) (*chronoqueue.PauseScheduleResponse, error)
	ResumeSchedule(ctx context.Context, request *chronoqueue.ResumeScheduleRequest) (*chronoqueue.ResumeScheduleResponse, error)
}

type storage struct {
	redisClient          *redis.Client
	rs                   *redsync.Redsync
	encryptionKeyManager *keymanager.EncryptionKeyManager
}

func NewQueueStorage(ctx context.Context, redisClient *redis.Client, encryptionKeyManager *keymanager.EncryptionKeyManager) Storage {
	pool := goredis.NewPool(redisClient)
	rs := redsync.New(pool)
	storage := &storage{
		redisClient:          redisClient,
		rs:                   rs,
		encryptionKeyManager: encryptionKeyManager,
	}

	// Create a buffered channel for tasks
	tasks := make(chan Task, 10)

	// Start worker goroutines
	// for i := 0; i < 5; i++ { // 5 workers
	go storage.worker(ctx, tasks)
	// }

	// Schedule the initial tasks
	tasks <- Task{Name: "invisibleToPending", Script: invisibleToPending, GoFunc: nil, Interval: 2 * time.Second}
	tasks <- Task{Name: "runningToPending", Script: runningToPending, GoFunc: nil, Interval: 2 * time.Second}
	tasks <- Task{Name: "updateCronSchedules", Script: nil, GoFunc: storage.updateAllCronSchedules, Interval: time.Second}

	return storage
}

func (as *storage) DeleteQueue(ctx context.Context, request *chronoqueue.DeleteQueueRequest) (*chronoqueue.DeleteQueueResponse, error) {

	// Create or fetch the mutex for this specific queue
	queueMutex := as.rs.NewMutex("mutex:" + request.GetName())

	// Try to acquire the lock
	if err := queueMutex.Lock(); err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected while acquiring lock")
		return &chronoqueue.DeleteQueueResponse{Success: false}, chronoErr.GRPCStatus()
	}

	defer func() {
		// Release the message lock
		if ok, err := queueMutex.Unlock(); !ok || err != nil {
			util.Error("Failed to release queue lock", err)
		}
	}()

	if request == nil || request.GetName() == "" {
		return &chronoqueue.DeleteQueueResponse{Success: false}, errors.New("error: queue information missing")
	}
	checker := NewKeyChecker(as.redisClient, 100)

	start := time.Now()
	checker.Start(ctx)

	iter := as.redisClient.Scan(ctx, 0, fmt.Sprintf("%s*", request.GetName()), 0).Iterator()
	for iter.Next(ctx) {
		checker.Add(iter.Val())
	}
	if err := iter.Err(); err != nil {
		return &chronoqueue.DeleteQueueResponse{Success: false}, err
	}

	deleted := checker.Stop()
	log.Println("deleted", deleted, "keys", "in", time.Since(start))

	return &chronoqueue.DeleteQueueResponse{Success: true}, nil
}

func (as *storage) DeleteQueueMessage(ctx context.Context, queueName string, messageID string) error {
	_, err := as.redisClient.Del(ctx, messageID).Result()
	if err != nil {
		return err
	}
	return nil
}

type Task struct {
	Name     string
	Script   *redis.Script
	GoFunc   func(ctx context.Context) error
	Interval time.Duration
}

func (as *storage) worker(ctx context.Context, tasks chan Task) {
	for {
		select {
		case task := <-tasks:
			now := time.Now().UnixNano() / int64(time.Millisecond)
			if task.Script != nil {
				err := task.Script.Run(ctx, as.redisClient, nil, now).Err()
				if err != nil && err.Error() != "redis: nil" {
					util.ErrorWithFields("Failed to run the script", map[string]interface{}{
						"task":  task.Name,
						"error": err,
					})
				}
			}
			if task.GoFunc != nil {
				err := task.GoFunc(ctx)
				if err != nil {
					util.ErrorWithFields("Failed to run the go routine", map[string]interface{}{
						"task":  task.Name,
						"error": err,
					})
				}
			}

			// Re-schedule the task
			time.AfterFunc(task.Interval, func() { tasks <- task })
		case <-ctx.Done():
			return
		}
	}
}

func calculateNextRunTime(cronSchedule string) (int64, error) {
	schedule, err := cron.ParseStandard(cronSchedule)
	if err != nil {
		return 0, err
	}
	nextRun := schedule.Next(time.Now())
	return nextRun.UnixMilli(), nil
}

func (as *storage) updateMessageCronSchedule(ctx context.Context, key string, metadata *chronoqueue.Schedule_Metadata) error {
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
			util.Error("Failed to release message lock", err)
		}
	}()

	if metadata.CronSchedule != "" && (metadata.LastRun == nil || (metadata.LastRun.AsTime().Before(time.Now()) && metadata.NextRun.AsTime().Before(time.Now()))) {
		// Calculate the next run time for this message
		nextRunTime, err := calculateNextRunTime(metadata.CronSchedule)
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

		runMessageInstanceMetadata := chronoqueue.Message_Metadata{
			Priority:      metadata.Priority,
			LeaseDuration: metadata.LeaseDuration,
			LeaseExpiry:   0,
			InvisibilityDuration: &durationpb.Duration{
				Seconds: nextRunTime / int64(time.Millisecond),
			},
			AttemptsLeft:      1,
			State:             chronoqueue.Message_Metadata_INVISIBLE,
			CronSchedule:      scheduleID,
			Payload:           metadata.Payload,
			LeaseRenewalCount: 0,
		}

		priorityScore := time.Now().Add(time.Duration(int64(MaxPriority-runMessageInstanceMetadata.GetPriority()))).UnixNano() / int64(time.Millisecond)

		randomID, err := util.GenerateID()
		if err != nil {
			return err
		}

		_, err = as.CreateQueueMessage(ctx, &chronoqueue.PostMessageRequest{
			Message: &chronoqueue.Message{
				MessageId: randomID,
				Metadata:  &runMessageInstanceMetadata,
			},
			QueueName: queueID,
		})
		if err != nil {
			return err
		}

		// Log the update for the schedule
		util.InfoWithFields("Updating cron schedule for schedule", map[string]interface{}{
			"scheduleMetadataID": key,
			"state":              metadata.State,
		})

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
		// Add the message to the queue
		_, err = as.redisClient.ZAdd(ctx, scheduleID, redis.Z{
			Score:  float64(priorityScore),
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
			if metadata.State == chronoqueue.Schedule_Metadata_SCHEDULED {
				err = as.updateMessageCronSchedule(ctx, key, metadata)
				if err != nil {
					return err
				}
			}
		}

		if cursor == 0 {
			break
		}
	}

	return nil
}
