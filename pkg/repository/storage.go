package repository

import (
	"context"
	"encoding/json"
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
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
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

	// Message processing methods
	ProcessExpiredInvisibleMessages(ctx context.Context) error
	ProcessExpiredRunningMessages(ctx context.Context) error
	ProcessErroredMessagesForDLQ(ctx context.Context) error

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
}

func NewQueueStorage(ctx context.Context, redisClient *redis.Client, encryptionKeyManager *keymanager.EncryptionKeyManager, logger *log.Logger) Storage {
	pool := goredis.NewPool(redisClient)
	rs := redsync.New(pool)
	storage := &storage{
		redisClient:          redisClient,
		rs:                   rs,
		encryptionKeyManager: encryptionKeyManager,
		logger:               logger,
	}

	// Create a buffered channel for tasks
	tasks := make(chan Task, 10)

	// Start worker goroutines
	// for i := 0; i < 5; i++ { // 5 workers
	go storage.worker(ctx, tasks)
	// }

	// Schedule the initial tasks
	// tasks <- Task{Name: "invisibleToPending", Script: invisibleToPending, GoFunc: nil, Interval: time.Second}
	// tasks <- Task{Name: "runningToPending", Script: runningToPending, GoFunc: nil, Interval: time.Second}
	tasks <- Task{Name: "updateCronSchedules", Script: nil, GoFunc: storage.updateAllCronSchedules, Interval: time.Second}
	tasks <- Task{Name: "processErroredMessages", Script: processErroredMessages, GoFunc: nil, Interval: time.Second}
	tasks <- Task{Name: "invisibleToPending", Script: invisibleToPending, GoFunc: nil, Interval: time.Second} // process expired invisible messages
	tasks <- Task{Name: "runningToPending", Script: runningToPending, GoFunc: nil, Interval: time.Second}     // process expired running messages

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
	checker := NewKeyChecker(as.redisClient, 100)

	start := time.Now()
	checker.Start(ctx)

	iter := as.redisClient.Scan(ctx, 0, fmt.Sprintf("queue:%s*", request.GetName()), 0).Iterator()
	for iter.Next(ctx) {
		checker.Add(iter.Val())
	}
	if err := iter.Err(); err != nil {
		return &queueservice_pb.DeleteQueueResponse{Success: false}, err
	}

	deleted := checker.Stop()
	as.logger.DebugWithFields(
		"Deleted keys associated with queue",
		"total", deleted,
		"queue", request.GetName(),
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
				result := task.Script.Run(ctx, as.redisClient, nil, now)
				err := result.Err()
				if err != nil && err.Error() != "redis: nil" {
					as.logger.ErrorWithFields(
						"Failed to run the script",
						"task", task.Name,
						"error", err,
					)
				} else {
					// Parse and log structured response from script
					if resultStr := result.String(); resultStr != "" && resultStr != "[]" {
						// Extract JSON from Redis response
						// Look for the pattern ": {" which indicates the start of our JSON response
						jsonPattern := ": {"
						jsonStart := strings.Index(resultStr, jsonPattern)
						if jsonStart != -1 {
							jsonStr := resultStr[jsonStart+2:] // Skip ": " to get to the JSON

							// Try to parse as JSON to check for errors
							var scriptResult map[string]interface{}
							if jsonErr := json.Unmarshal([]byte(jsonStr), &scriptResult); jsonErr == nil {
								if success, ok := scriptResult["success"].(bool); ok && !success {
									// Script returned an error
									as.logger.ErrorWithFields(
										"Lua script execution error",
										"task", task.Name,
										"error", scriptResult["error"],
										"type", scriptResult["type"],
									)
								} else {
									// Script executed successfully, log summary
									processed := 0
									errors := 0
									transitions := 0
									totalKeys := 0

									if p, ok := scriptResult["processed"].(float64); ok {
										processed = int(p)
									}
									if e, ok := scriptResult["errors"].(float64); ok {
										errors = int(e)
									}
									if t, ok := scriptResult["transitions"].(float64); ok {
										transitions = int(t)
									}
									if tk, ok := scriptResult["total_keys"].(float64); ok {
										totalKeys = int(tk)
									}

									if processed > 0 || errors > 0 || transitions > 0 {
										as.logger.InfoWithFields(
											"Script execution summary",
											"task", task.Name,
											"processed", processed,
											"errors", errors,
											"transitions", transitions,
											"total_keys", totalKeys,
										)
									}

									// Log full debug info only if verbose logging or errors occurred
									if errors > 0 {
										as.logger.ErrorWithFields(
											"Script execution details",
											"task", task.Name,
											"full_response", jsonStr,
										)
									}
								}
							} else {
								// Fallback for non-JSON responses
								as.logger.InfoWithFields(
									"Script execution debug",
									"task", task.Name,
									"debug_info", resultStr,
								)
							}
						} else {
							// No JSON found, log as debug
							as.logger.InfoWithFields(
								"Script execution debug",
								"task", task.Name,
								"debug_info", resultStr,
							)
						}
					}
				}
			}
			if task.GoFunc != nil {
				err := task.GoFunc(ctx)
				if err != nil {
					as.logger.ErrorWithFields(
						"Failed to run the go routine",
						"task", task.Name,
						"error", err,
					)
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
