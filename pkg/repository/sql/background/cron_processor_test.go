package background

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/pkg/log"
	"github.com/adrien19/chronoqueue/pkg/repository/sqlite"
)

func TestParseCronExpressionValid(t *testing.T) {
	svc := &CronProcessorService{}
	schedule, err := svc.parseCronExpression("0 9 * * 1")
	require.NoError(t, err)
	require.NotNil(t, schedule)
}

func TestParseCronExpressionInvalid(t *testing.T) {
	svc := &CronProcessorService{}
	_, err := svc.parseCronExpression("invalid")
	require.Error(t, err)
}

func TestCalculateNextCronRunIntervals(t *testing.T) {
	svc := &CronProcessorService{}
	parsed, err := svc.parseCronExpression("*/5 * * * *")
	require.NoError(t, err)

	start := time.Date(2025, time.January, 1, 10, 2, 0, 0, time.UTC)
	next := svc.calculateNextCronRun(parsed, start)
	require.Equal(t, start.Add(3*time.Minute), next)
}

func TestCalculateNextCronRunMondayNineAM(t *testing.T) {
	svc := &CronProcessorService{}
	parsed, err := svc.parseCronExpression("0 9 * * 1")
	require.NoError(t, err)

	sunday := time.Date(2025, time.January, 5, 12, 0, 0, 0, time.UTC) // Sunday
	monday := svc.calculateNextCronRun(parsed, sunday)
	expected := time.Date(2025, time.January, 6, 9, 0, 0, 0, time.UTC)
	require.Equal(t, expected, monday)
}

func TestCronProcessorExecutesSchedule(t *testing.T) {
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("Skipping test because CGO is disabled (required for SQLite)")
	}

	ctx := context.Background()
	logger := log.NewLogger(log.WithLevel(logrus.ErrorLevel))
	storage, err := sqlite.NewStorage(ctx, &sqlite.Config{Path: ":memory:", Logger: logger})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	queue := &queuepb.Queue{Name: "cron-q", Metadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 1}}
	require.NoError(t, storage.CreateQueue(ctx, queue))

	baseTime := time.Date(2025, time.January, 5, 10, 0, 0, 0, time.UTC)
	currentTime := baseTime

	payloadData, _ := structpb.NewStruct(map[string]interface{}{"hello": "world"})
	schedule := &schedulepb.Schedule{
		ScheduleId: "cron-s1",
		Metadata: &schedulepb.Schedule_Metadata{
			Headers: []*messagepb.Message_Metadata_Header{
				{Key: "trace-id", Value: []byte{0x00, 0xff}},
				{Key: "trace-id", Value: []byte("second")},
			},
			State:          schedulepb.Schedule_Metadata_SCHEDULED,
			QueueName:      queue.Name,
			NextRun:        timestamppb.New(baseTime),
			Priority:       4,
			Payload:        &commonpb.Payload{Data: payloadData, ContentType: "application/json"},
			ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{CronSchedule: "*/2 * * * *"},
		},
	}
	require.NoError(t, storage.CreateSchedule(ctx, schedule))

	svc := NewCronProcessorService(storage.BaseSQL, time.Second)
	svc.nowFn = func() time.Time {
		return currentTime
	}

	for i := 0; i < 3; i++ {
		require.NoError(t, svc.RunOnce(ctx))
		currentTime = currentTime.Add(2 * time.Minute)
	}

	var count int
	require.NoError(t, storage.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM cq_messages WHERE queue_name = ?", queue.Name).Scan(&count))
	require.Equal(t, 3, count)

	// Ensure stored message is valid and parsable by scheduler/claim paths
	var metadataBytes []byte
	require.NoError(t, storage.DB.QueryRowContext(ctx, "SELECT metadata_pb FROM cq_messages LIMIT 1").Scan(&metadataBytes))
	msg, err := storage.Serializer.UnmarshalMessage(metadataBytes)
	require.NoError(t, err)
	require.True(t, proto.Equal(schedule.Metadata.GetPayload(), msg.GetMetadata().GetPayload()), "payloads should be equal")
	require.True(t, proto.Equal(schedule.Metadata.GetHeaders()[0], msg.GetMetadata().GetHeaders()[0]))
	require.True(t, proto.Equal(schedule.Metadata.GetHeaders()[1], msg.GetMetadata().GetHeaders()[1]))
	require.True(t, proto.Equal(schedule.Metadata.GetHeaders()[0], msg.GetMetadata().GetHeaders()[0]))
	require.True(t, proto.Equal(schedule.Metadata.GetHeaders()[1], msg.GetMetadata().GetHeaders()[1]))

	var executionCount int64
	var nextRunMs, lastRunMs int64
	require.NoError(t, storage.DB.QueryRowContext(ctx, "SELECT execution_count, next_run, last_run FROM cq_schedules WHERE id = ?", schedule.ScheduleId).Scan(&executionCount, &nextRunMs, &lastRunMs))
	require.Equal(t, int64(3), executionCount)
	require.Equal(t, baseTime.Add(6*time.Minute).UnixMilli(), nextRunMs)
	require.Equal(t, baseTime.Add(4*time.Minute).UnixMilli(), lastRunMs)
}

func TestCronProcessorEnforcesMaxMessagesAndResumeResetsLimit(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	queue := &queuepb.Queue{Name: "cron-limited", Metadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 1}}
	require.NoError(t, storage.CreateQueue(ctx, queue))
	currentTime := time.Date(2026, time.August, 27, 10, 0, 0, 0, time.UTC)
	payload, err := structpb.NewStruct(map[string]any{"ok": true})
	require.NoError(t, err)
	schedule := &schedulepb.Schedule{ScheduleId: "cron-limited", Metadata: &schedulepb.Schedule_Metadata{
		State: schedulepb.Schedule_Metadata_SCHEDULED, QueueName: queue.Name, NextRun: timestamppb.New(currentTime),
		HasMaxMessages: true, MaxMessages: 2, Payload: &commonpb.Payload{Data: payload},
		ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{CronSchedule: "* * * * *"},
	}}
	require.NoError(t, storage.CreateSchedule(ctx, schedule))
	service := NewCronProcessorService(storage.BaseSQL, time.Second)
	service.nowFn = func() time.Time { return currentTime }
	require.NoError(t, service.RunOnce(ctx))
	currentTime = currentTime.Add(time.Minute)
	require.NoError(t, service.RunOnce(ctx))
	currentTime = currentTime.Add(time.Minute)
	require.NoError(t, service.RunOnce(ctx))

	var messageCount, executionCount int64
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_messages WHERE queue_name = ?`, queue.Name).Scan(&messageCount))
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT execution_count FROM cq_schedules WHERE id = ?`, schedule.ScheduleId).Scan(&executionCount))
	require.Equal(t, int64(2), messageCount)
	require.Equal(t, int64(2), executionCount)
	updated, err := storage.GetSchedule(ctx, schedule.ScheduleId)
	require.NoError(t, err)
	require.Equal(t, schedulepb.Schedule_Metadata_PAUSED, updated.GetMetadata().GetState())

	require.NoError(t, storage.ResumeSchedule(ctx, schedule.ScheduleId))
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT execution_count FROM cq_schedules WHERE id = ?`, schedule.ScheduleId).Scan(&executionCount))
	require.Zero(t, executionCount)
}

func TestCronProcessorClearsNextRunWhenAlreadyAtMessageLimit(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	queue := &queuepb.Queue{Name: "cron-already-limited", Metadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 1}}
	require.NoError(t, storage.CreateQueue(ctx, queue))
	due := time.Now().Add(-time.Minute)
	schedule := &schedulepb.Schedule{ScheduleId: "cron-already-limited", Metadata: &schedulepb.Schedule_Metadata{
		State: schedulepb.Schedule_Metadata_SCHEDULED, QueueName: queue.Name, NextRun: timestamppb.New(due),
		HasMaxMessages: true, MaxMessages: 1,
		ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{CronSchedule: "* * * * *"},
	}}
	require.NoError(t, storage.CreateSchedule(ctx, schedule))
	_, err := storage.DB.ExecContext(ctx, `UPDATE cq_schedules SET execution_count = 1 WHERE id = ?`, schedule.ScheduleId)
	require.NoError(t, err)

	service := NewCronProcessorService(storage.BaseSQL, time.Second)
	service.nowFn = time.Now
	require.NoError(t, service.RunOnce(ctx))

	var nextRun sql.NullInt64
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT next_run FROM cq_schedules WHERE id = ?`, schedule.ScheduleId).Scan(&nextRun))
	require.False(t, nextRun.Valid)
	var messageCount, executionCount int64
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_messages WHERE queue_name = ?`, queue.Name).Scan(&messageCount))
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT execution_count FROM cq_schedules WHERE id = ?`, schedule.ScheduleId).Scan(&executionCount))
	require.Zero(t, messageCount)
	require.Equal(t, int64(1), executionCount)
	updated, err := storage.GetSchedule(ctx, schedule.ScheduleId)
	require.NoError(t, err)
	require.Equal(t, schedulepb.Schedule_Metadata_PAUSED, updated.GetMetadata().GetState())
	require.Nil(t, updated.GetMetadata().GetNextRun())
}

func TestCronProcessorRejectsMessageOutsideQueueAdmissionContract(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	queue := &queuepb.Queue{Name: "cron-validation", Metadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 1, MaxPayloadSize: 1}}
	require.NoError(t, storage.CreateQueue(ctx, queue))
	now := time.Date(2026, time.August, 27, 10, 0, 0, 0, time.UTC)
	payload, err := structpb.NewStruct(map[string]any{"too": "large"})
	require.NoError(t, err)
	schedule := &schedulepb.Schedule{ScheduleId: "cron-validation", Metadata: &schedulepb.Schedule_Metadata{
		State: schedulepb.Schedule_Metadata_SCHEDULED, QueueName: queue.Name, NextRun: timestamppb.New(now),
		Payload:        &commonpb.Payload{Data: payload, ContentType: "application/json"},
		ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{CronSchedule: "* * * * *"},
	}}
	require.NoError(t, storage.CreateSchedule(ctx, schedule))
	service := NewCronProcessorService(storage.BaseSQL, time.Second)
	service.nowFn = func() time.Time { return now }
	require.NoError(t, service.RunOnce(ctx))

	var messageCount int
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_messages WHERE queue_name = ?`, queue.Name).Scan(&messageCount))
	require.Zero(t, messageCount)
	updated, err := storage.GetSchedule(ctx, schedule.ScheduleId)
	require.NoError(t, err)
	require.Equal(t, schedulepb.Schedule_Metadata_ERRORED, updated.GetMetadata().GetState())
	require.Contains(t, updated.GetMetadata().GetStateMessage(), "scheduled message validation failed")
}

func TestCronProcessorMarksInvalidExpression(t *testing.T) {
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("Skipping test because CGO is disabled (required for SQLite)")
	}

	ctx := context.Background()
	logger := log.NewLogger(log.WithLevel(logrus.ErrorLevel))
	storage, err := sqlite.NewStorage(ctx, &sqlite.Config{Path: ":memory:", Logger: logger})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	queue := &queuepb.Queue{Name: "cron-q2", Metadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 1}}
	require.NoError(t, storage.CreateQueue(ctx, queue))

	now := time.Date(2025, time.January, 5, 10, 0, 0, 0, time.UTC)
	schedule := &schedulepb.Schedule{
		ScheduleId: "cron-s2",
		Metadata: &schedulepb.Schedule_Metadata{
			State:          schedulepb.Schedule_Metadata_SCHEDULED,
			QueueName:      queue.Name,
			NextRun:        timestamppb.New(now),
			ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{CronSchedule: "invalid"},
		},
	}
	require.NoError(t, storage.CreateSchedule(ctx, schedule))

	svc := NewCronProcessorService(storage.BaseSQL, time.Second)
	svc.nowFn = func() time.Time { return now }

	require.NoError(t, svc.RunOnce(ctx))

	updated, err := storage.GetSchedule(ctx, schedule.ScheduleId)
	require.NoError(t, err)
	require.Equal(t, schedulepb.Schedule_Metadata_ERRORED, updated.Metadata.State)
	require.Contains(t, updated.Metadata.StateMessage, "invalid cron")
	require.Nil(t, updated.Metadata.NextRun)
	var success int
	var errorMessage string
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT success, error_message FROM cq_schedule_history WHERE schedule_id = ?`, schedule.ScheduleId).Scan(&success, &errorMessage))
	require.Zero(t, success)
	require.Contains(t, errorMessage, "invalid cron")
}

func TestCronProcessorCoalescesMissedRunAndRecoversAfterRestart(t *testing.T) {
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("Skipping test because CGO is disabled (required for SQLite)")
	}

	ctx := context.Background()
	logger := log.NewLogger(log.WithLevel(logrus.ErrorLevel))
	storage, err := sqlite.NewStorage(ctx, &sqlite.Config{Path: ":memory:", Logger: logger})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	queue := &queuepb.Queue{Name: "cron-recovery", Metadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 1}}
	require.NoError(t, storage.CreateQueue(ctx, queue))
	firstDue := time.Date(2025, time.January, 5, 10, 0, 0, 0, time.UTC)
	schedule := &schedulepb.Schedule{
		ScheduleId: "cron-recovery",
		Metadata: &schedulepb.Schedule_Metadata{
			State:          schedulepb.Schedule_Metadata_SCHEDULED,
			QueueName:      queue.Name,
			NextRun:        timestamppb.New(firstDue),
			Payload:        &commonpb.Payload{Data: &structpb.Struct{}, ContentType: "application/json"},
			ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{CronSchedule: "*/2 * * * *"},
		},
	}
	require.NoError(t, storage.CreateSchedule(ctx, schedule))

	missedRunProcessor := NewCronProcessorService(storage.BaseSQL, time.Second)
	missedRunProcessor.nowFn = func() time.Time { return firstDue.Add(10 * time.Minute) }
	require.NoError(t, missedRunProcessor.RunOnce(ctx))

	var count int
	require.NoError(t, storage.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM cq_messages WHERE queue_name = ?", queue.Name).Scan(&count))
	require.Equal(t, 1, count, "missed occurrences are coalesced into one execution")

	restartedProcessor := NewCronProcessorService(storage.BaseSQL, time.Second)
	restartedProcessor.nowFn = func() time.Time { return firstDue.Add(12 * time.Minute) }
	require.NoError(t, restartedProcessor.RunOnce(ctx))
	require.NoError(t, storage.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM cq_messages WHERE queue_name = ?", queue.Name).Scan(&count))
	require.Equal(t, 2, count, "a restarted processor must continue from persisted next_run")
}

func TestCronProcessorDoesNotRecordConflictingMessage(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { _ = storage.Close() })

	queue := &queuepb.Queue{Name: "cron-conflict", Metadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 1}}
	require.NoError(t, storage.CreateQueue(ctx, queue))
	require.NoError(t, storage.EnqueueMessage(ctx, queue.Name, &messagepb.Message{
		MessageId: "duplicate-id",
		Metadata:  &messagepb.Message_Metadata{State: messagepb.Message_Metadata_PENDING, AttemptsLeft: 1, MaxAttempts: 1},
	}))
	countsBefore, err := storage.StateManager.GetStateCounts(ctx, storage.DB, queue.Name)
	require.NoError(t, err)

	now := time.Date(2025, time.January, 5, 10, 0, 0, 0, time.UTC)
	schedule := &schedulepb.Schedule{
		ScheduleId: "cron-conflict",
		Metadata: &schedulepb.Schedule_Metadata{
			State:          schedulepb.Schedule_Metadata_SCHEDULED,
			QueueName:      queue.Name,
			NextRun:        timestamppb.New(now),
			Payload:        &commonpb.Payload{Data: &structpb.Struct{}, ContentType: "application/json"},
			ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{CronSchedule: "*/2 * * * *"},
		},
	}
	require.NoError(t, storage.CreateSchedule(ctx, schedule))

	service := NewCronProcessorService(storage.BaseSQL, time.Second)
	service.nowFn = func() time.Time { return now }
	service.generateID = func() (string, error) { return "duplicate-id", nil }
	require.ErrorContains(t, service.processSchedule(ctx, schedule.ScheduleId), "insert message")

	var messageCount, historyCount, executionCount int
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_messages WHERE queue_name = ?`, queue.Name).Scan(&messageCount))
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_schedule_history WHERE schedule_id = ?`, schedule.ScheduleId).Scan(&historyCount))
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT execution_count FROM cq_schedules WHERE id = ?`, schedule.ScheduleId).Scan(&executionCount))
	require.Equal(t, 1, messageCount)
	require.Zero(t, historyCount)
	require.Zero(t, executionCount)

	counts, err := storage.StateManager.GetStateCounts(ctx, storage.DB, queue.Name)
	require.NoError(t, err)
	require.Equal(t, countsBefore, counts)
}
