package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	schedule_pb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/adrien19/chronoqueue/pkg/calendar"
)

// // updateAllCalendarSchedules processes all calendar-based schedules.
// // used by a background worker to ensure schedules are up-to-date.
// func (as *storage) updateAllCalendarSchedules(ctx context.Context) error {
// 	// Fetch all schedules
// 	var cursor uint64
// 	var err error
// 	var keys []string

// 	for {
// 		keys, cursor, err = as.redisClient.Scan(ctx, cursor, "schedule:*:meta", 10).Result()
// 		if err != nil {
// 			return err
// 		}

// 		for _, key := range keys {
// 			metadata, err := as.getScheduleMetadata(ctx, key)
// 			if err != nil {
// 				as.logger.ErrorWithFields("Failed to get schedule metadata", err, "key", key)
// 				continue
// 			}

// 			// Only process scheduled (active) schedules with calendar configuration
// 			if metadata.State == schedule_pb.Schedule_Metadata_SCHEDULED && metadata.GetCalendarSchedule() != nil {
// 				err = as.updateMessageCalendarSchedule(ctx, key, metadata)
// 				if err != nil {
// 					scheduleID := strings.Split(key, ":")[1]
// 					as.logger.ErrorWithFields("Failed to update calendar schedule", err, "key", key, "scheduleID", scheduleID)
// 					continue
// 				}
// 			}
// 		}

// 		if cursor == 0 {
// 			break
// 		}
// 	}

// 	return nil
// }

// updateMessageCalendarSchedule handles calendar-based schedule updates
func (as *storage) updateMessageCalendarSchedule(ctx context.Context, key string, metadata *schedule_pb.Schedule_Metadata) error {
	// Key format: chronoqueue:schedule:{encoded_id}:meta
	parts := strings.Split(key, ":")
	encodedScheduleID := parts[2]
	scheduleID, err := urlDecode(encodedScheduleID)
	if err != nil {
		return fmt.Errorf("failed to decode schedule ID: %w", err)
	}
	queueID := metadata.GetQueueName()

	// Log the queue name to help debug message key issues
	as.logger.DebugWithFields("Processing calendar schedule",
		"scheduleID", scheduleID,
		"queueName", queueID,
		"key", key)

	// Create or fetch the mutex for this specific schedule
	scheduleMutex := as.rs.NewMutex("mutex:" + key)

	// Try to acquire the lock with timeout
	if err := scheduleMutex.Lock(); err != nil {
		return fmt.Errorf("failed to acquire lock for schedule %s: %w", scheduleID, err)
	}
	defer func() {
		// Release the schedule lock
		if ok, err := scheduleMutex.Unlock(); !ok || err != nil {
			as.logger.ErrorWithFields("Failed to release schedule lock", err, "scheduleID", scheduleID)
		}
	}()

	// CRITICAL: Re-fetch metadata after acquiring lock to prevent duplicate message creation
	// The background goroutine runs every second and could have already processed this execution
	metadata, err = as.getScheduleMetadata(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to re-fetch schedule metadata after lock: %w", err)
	}

	// Verify schedule is still in SCHEDULED state after re-fetch
	if metadata.State != schedule_pb.Schedule_Metadata_SCHEDULED {
		as.logger.DebugWithFields("Schedule no longer in SCHEDULED state after lock acquisition",
			"scheduleID", scheduleID,
			"state", metadata.State.String())
		return nil
	}

	calendarSchedule := metadata.GetCalendarSchedule()
	if calendarSchedule == nil {
		return fmt.Errorf("calendar schedule is nil for schedule %s", scheduleID)
	}

	// Check if we need to schedule the next message
	now := time.Now()
	shouldSchedule := false

	if metadata.LastRun == nil && metadata.NextRun == nil {
		// First time scheduling
		shouldSchedule = true
	} else if metadata.NextRun != nil && metadata.NextRun.AsTime().Before(now) {
		// Next run time has passed
		shouldSchedule = true
	}

	if !shouldSchedule {
		return nil
	}

	// Initialize calendar engine
	engine, err := as.getCalendarEngine(ctx)
	if err != nil {
		return fmt.Errorf("failed to initialize calendar engine: %w", err)
	}

	// Calculate the next run time using the calendar engine
	fromTime := now
	if metadata.NextRun != nil {
		fromTime = metadata.NextRun.AsTime()
	}

	nextRunTimePtr, err := engine.CalculateNextRun(ctx, calendarSchedule, fromTime)
	if err != nil {
		return fmt.Errorf("failed to calculate next run time for schedule %s: %w", scheduleID, err)
	}

	if nextRunTimePtr == nil {
		as.logger.InfoWithFields("No more runs scheduled for calendar schedule", "scheduleID", scheduleID)
		// Update state to paused if schedule has ended (or we could add a new state)
		metadata.State = schedule_pb.Schedule_Metadata_PAUSED
		metadata.StateMessage = "No more scheduled runs"
		if err := as.updateScheduleMetadata(ctx, key, metadata); err != nil {
			return fmt.Errorf("failed to update schedule state: %w", err)
		}
		return nil
	}

	nextRunTime := *nextRunTimePtr
	nextRunMillis := nextRunTime.UnixMilli()

	// Update the last run and next run times
	if metadata.NextRun != nil {
		metadata.LastRun = metadata.NextRun
	}
	metadata.NextRun = timestamppb.New(nextRunTime)

	// Create a message instance for this scheduled run
	randomID, err := util.GenerateID()
	if err != nil {
		return fmt.Errorf("failed to generate message ID: %w", err)
	}

	runMessageInstanceMetadata := message_pb.Message_Metadata{
		Priority:          metadata.Priority,
		LeaseDuration:     metadata.LeaseDuration,
		LeaseExpiry:       0,
		ScheduledTime:     timestamppb.New(nextRunTime),
		AttemptsLeft:      1,
		State:             message_pb.Message_Metadata_INVISIBLE,
		Payload:           metadata.Payload,
		LeaseRenewalCount: 0,
	}

	// Create the calendar schedule message directly (bypass queue scheduling)
	// This prevents duplicate sorted sets - calendar messages go directly to queue schedule
	err = as.createCalendarScheduleMessage(ctx, queueID, randomID, &runMessageInstanceMetadata)
	if err != nil {
		return fmt.Errorf("failed to create queue message for schedule %s: %w", scheduleID, err)
	}

	// Log the successful scheduling
	as.logger.InfoWithFields(
		"Scheduled calendar-based message",
		"scheduleID", scheduleID,
		"messageID", randomID,
		"queueName", queueID,
		"nextRunTime", nextRunTime.Format(time.RFC3339),
		"lastRunTime", func() string {
			if metadata.LastRun != nil {
				return metadata.LastRun.AsTime().Format(time.RFC3339)
			}
			return "never"
		}(),
	)

	// Update the schedule metadata with new run times
	if err := as.updateScheduleMetadata(ctx, key, metadata); err != nil {
		return fmt.Errorf("failed to update schedule metadata: %w", err)
	}

	// Track this execution in the calendar schedule history
	// This sorted set is ONLY for tracking/history - not for scheduling
	// The actual scheduling happens via scheduleKey(queueName) in createCalendarScheduleMessage
	_, err = as.redisClient.ZAdd(ctx, as.scheduleSetKey(scheduleID), redis.Z{
		Score:  float64(nextRunMillis),
		Member: randomID,
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to add message to schedule history: %w", err)
	}

	return nil
}

// updateScheduleMetadata updates the schedule metadata in Redis
func (as *storage) updateScheduleMetadata(ctx context.Context, key string, metadata *schedule_pb.Schedule_Metadata) error {
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}

	scheduleMetadataByte, err := m.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal schedule metadata: %w", err)
	}

	_, err = as.redisClient.HSet(ctx, key, "metadata", string(scheduleMetadataByte)).Result()
	if err != nil {
		return fmt.Errorf("failed to update schedule metadata in Redis: %w", err)
	}

	return nil
}

// getCalendarEngine initializes and returns a calendar engine instance
func (as *storage) getCalendarEngine(ctx context.Context) (calendar.Engine, error) {
	// Initialize the engine with default configuration and providers
	// In a production environment, you might want to use custom providers
	// that integrate with your business calendar storage, etc.
	engine := calendar.NewDefaultEngine()

	return engine, nil
}

// ValidateCalendarSchedule validates a calendar schedule configuration
func (as *storage) ValidateCalendarSchedule(ctx context.Context, calendarSchedule *schedule_pb.CalendarSchedule) error {
	if calendarSchedule == nil {
		return fmt.Errorf("calendar schedule cannot be nil")
	}

	engine, err := as.getCalendarEngine(ctx)
	if err != nil {
		return fmt.Errorf("failed to initialize calendar engine: %w", err)
	}

	// Use the engine's validation method
	if err := engine.ValidateSchedule(ctx, calendarSchedule); err != nil {
		return fmt.Errorf("invalid calendar schedule: %w", err)
	}

	return nil
}

// GetCalendarSchedulePreview generates a preview of upcoming execution times
func (as *storage) GetCalendarSchedulePreview(ctx context.Context, calendarSchedule *schedule_pb.CalendarSchedule, count int) (*queueservice_pb.PreviewCalendarScheduleResponse, error) {
	if calendarSchedule == nil {
		return nil, fmt.Errorf("calendar schedule cannot be nil")
	}

	engine, err := as.getCalendarEngine(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize calendar engine: %w", err)
	}

	// Generate preview from now
	now := time.Now()
	preview, err := engine.PreviewSchedule(ctx, calendarSchedule, now, count)
	if err != nil {
		return nil, fmt.Errorf("failed to generate schedule preview: %w", err)
	}

	// Convert to protobuf response
	executionTimes := make([]*timestamppb.Timestamp, len(preview.ExecutionTimes))
	for i, et := range preview.ExecutionTimes {
		executionTimes[i] = timestamppb.New(et.Time)
	}

	return &queueservice_pb.PreviewCalendarScheduleResponse{
		ExecutionTimes: executionTimes,
		Timezone:       preview.Timezone,
		PreviewStart:   timestamppb.New(now),
		TotalCount:     int32(len(executionTimes)),
	}, nil
}

// createCalendarScheduleMessage creates a message for calendar schedule execution
// without going through the queue scheduling system (which is for user-posted messages).
// This prevents duplicate sorted sets and ensures messages are only scheduled once.
func (as *storage) createCalendarScheduleMessage(ctx context.Context, queueName, messageID string, metadata *message_pb.Message_Metadata) error {
	// Validate inputs
	if queueName == "" || messageID == "" || metadata == nil {
		return fmt.Errorf("invalid parameters: queueName, messageID, and metadata are required")
	}

	// Set message state to INVISIBLE (will be visible at scheduled time)
	metadata.State = message_pb.Message_Metadata_INVISIBLE

	// Encrypt payload if encryption is enabled
	if err := as.encryptMetadataPayload(metadata); err != nil {
		return fmt.Errorf("failed to encrypt message payload: %w", err)
	}

	// Serialize message metadata
	messageMetadataByte, err := as.serializeMessageMetadata(metadata)
	if err != nil {
		return fmt.Errorf("failed to serialize message metadata: %w", err)
	}

	// Save message metadata to Redis
	metaKey := as.messageMetaKey(queueName, messageID)
	if err := as.redisClient.HSet(ctx, metaKey, "metadata", string(messageMetadataByte)).Err(); err != nil {
		return fmt.Errorf("failed to save message metadata: %w", err)
	}

	// Set expiration on metadata (24 hours)
	if err := as.redisClient.Expire(ctx, metaKey, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("failed to set metadata expiration: %w", err)
	}

	// Add to the queue's schedule sorted set (not calendar schedule set)
	// The scheduler in scheduler.go will move it to stream at the right time
	scheduledTime := metadata.GetScheduledTime().AsTime()
	scheduleKey := as.scheduleKey(queueName)

	if err := as.redisClient.ZAdd(ctx, scheduleKey, redis.Z{
		Score:  float64(scheduledTime.UnixMilli()),
		Member: messageID,
	}).Err(); err != nil {
		return fmt.Errorf("failed to add message to queue schedule: %w", err)
	}

	return nil
}
