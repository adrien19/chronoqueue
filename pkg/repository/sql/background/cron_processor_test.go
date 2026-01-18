package background

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
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
	t.Cleanup(func() { _ = storage.Close() })

	queue := &queuepb.Queue{Name: "cron-q", Metadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 1}}
	require.NoError(t, storage.CreateQueue(ctx, queue))

	baseTime := time.Date(2025, time.January, 5, 10, 0, 0, 0, time.UTC)
	currentTime := baseTime

	payloadData, _ := structpb.NewStruct(map[string]interface{}{"hello": "world"})
	schedule := &schedulepb.Schedule{
		ScheduleId: "cron-s1",
		Metadata: &schedulepb.Schedule_Metadata{
			State:          schedulepb.Schedule_Metadata_SCHEDULED,
			QueueName:      queue.Name,
			NextRun:        timestamppb.New(baseTime),
			Priority:       5,
			Payload:        &commonpb.Payload{Data: payloadData},
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

	var executionCount int64
	var nextRunMs, lastRunMs int64
	require.NoError(t, storage.DB.QueryRowContext(ctx, "SELECT execution_count, next_run, last_run FROM cq_schedules WHERE id = ?", schedule.ScheduleId).Scan(&executionCount, &nextRunMs, &lastRunMs))
	require.Equal(t, int64(3), executionCount)
	require.Equal(t, baseTime.Add(6*time.Minute).UnixMilli(), nextRunMs)
	require.Equal(t, baseTime.Add(4*time.Minute).UnixMilli(), lastRunMs)
}

func TestCronProcessorMarksInvalidExpression(t *testing.T) {
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("Skipping test because CGO is disabled (required for SQLite)")
	}

	ctx := context.Background()
	logger := log.NewLogger(log.WithLevel(logrus.ErrorLevel))
	storage, err := sqlite.NewStorage(ctx, &sqlite.Config{Path: ":memory:", Logger: logger})
	require.NoError(t, err)
	t.Cleanup(func() { _ = storage.Close() })

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
}
