package background

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/pkg/calendar"
	"github.com/adrien19/chronoqueue/pkg/log"
	"github.com/adrien19/chronoqueue/pkg/repository/sqlite"
)

type stubCalendarEngine struct {
	nextRun *time.Time
	err     error
}

func (s *stubCalendarEngine) CalculateNextRun(ctx context.Context, calendarSchedule *schedulepb.CalendarSchedule, from time.Time) (*time.Time, error) {
	return s.nextRun, s.err
}

func (s *stubCalendarEngine) CalculateNextRuns(ctx context.Context, calendarSchedule *schedulepb.CalendarSchedule, from time.Time, count int) ([]time.Time, error) {
	return nil, s.err
}

func (s *stubCalendarEngine) ValidateSchedule(ctx context.Context, calendarSchedule *schedulepb.CalendarSchedule) error {
	return s.err
}

func (s *stubCalendarEngine) PreviewSchedule(ctx context.Context, calendarSchedule *schedulepb.CalendarSchedule, from time.Time, count int) (*calendar.SchedulePreview, error) {
	return &calendar.SchedulePreview{}, s.err
}

func (s *stubCalendarEngine) IsBusinessDay(ctx context.Context, date time.Time, businessCalendar *schedulepb.BusinessCalendar) (bool, error) {
	return true, s.err
}

func (s *stubCalendarEngine) GetHolidays(ctx context.Context, businessCalendar *schedulepb.BusinessCalendar, from, to time.Time) ([]calendar.Holiday, error) {
	return nil, s.err
}

func newTestStorage(t *testing.T) *sqlite.Storage {
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("Skipping test because CGO is disabled (required for SQLite)")
	}

	logger := log.NewLogger(log.WithLevel(logrus.ErrorLevel))
	storage, err := sqlite.NewStorage(context.Background(), &sqlite.Config{Path: ":memory:", Logger: logger})
	require.NoError(t, err)
	return storage
}

func TestCalendarServiceProcessesDueSchedule(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { _ = storage.Close() })

	queue := &queuepb.Queue{
		Name:     "q1",
		Metadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 2},
	}
	require.NoError(t, storage.CreateQueue(ctx, queue))

	now := time.Now().Add(-1 * time.Minute)
	next := now.Add(1 * time.Hour)

	payloadData, _ := structpb.NewStruct(map[string]interface{}{"hello": "world"})
	schedule := &schedulepb.Schedule{
		ScheduleId: "s1",
		Metadata: &schedulepb.Schedule_Metadata{
			State:          schedulepb.Schedule_Metadata_SCHEDULED,
			QueueName:      "q1",
			NextRun:        timestamppb.New(now),
			Priority:       0,
			Payload:        &commonpb.Payload{Data: payloadData},
			ScheduleConfig: &schedulepb.Schedule_Metadata_CalendarSchedule{CalendarSchedule: &schedulepb.CalendarSchedule{}},
			Timezone:       "UTC",
		},
	}
	require.NoError(t, storage.CreateSchedule(ctx, schedule))

	engine := &stubCalendarEngine{nextRun: &next}
	service := NewCalendarService(storage.BaseSQL, engine, time.Second)
	require.NoError(t, service.RunOnce(ctx))

	var count int
	require.NoError(t, storage.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM cq_messages WHERE queue_name = ?", "q1").Scan(&count))
	require.Equal(t, 1, count)

	updated, err := storage.GetSchedule(ctx, "s1")
	require.NoError(t, err)
	require.NotNil(t, updated.Metadata.LastRun)
	require.Equal(t, now.UnixMilli(), updated.Metadata.LastRun.AsTime().UnixMilli())
	require.NotNil(t, updated.Metadata.NextRun)
	require.Equal(t, next.UnixMilli(), updated.Metadata.NextRun.AsTime().UnixMilli())
	require.Equal(t, schedulepb.Schedule_Metadata_SCHEDULED, updated.Metadata.State)
}

func TestCalendarServicePausesWhenNoFutureRuns(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { _ = storage.Close() })

	queue := &queuepb.Queue{Name: "q2", Metadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 1}}
	require.NoError(t, storage.CreateQueue(ctx, queue))

	now := time.Now().Add(-2 * time.Minute)
	schedule := &schedulepb.Schedule{
		ScheduleId: "s2",
		Metadata: &schedulepb.Schedule_Metadata{
			State:          schedulepb.Schedule_Metadata_SCHEDULED,
			QueueName:      "q2",
			NextRun:        timestamppb.New(now),
			ScheduleConfig: &schedulepb.Schedule_Metadata_CalendarSchedule{CalendarSchedule: &schedulepb.CalendarSchedule{}},
		},
	}
	require.NoError(t, storage.CreateSchedule(ctx, schedule))

	engine := &stubCalendarEngine{nextRun: nil}
	service := NewCalendarService(storage.BaseSQL, engine, time.Second)
	require.NoError(t, service.RunOnce(ctx))

	updated, err := storage.GetSchedule(ctx, "s2")
	require.NoError(t, err)
	require.Equal(t, schedulepb.Schedule_Metadata_PAUSED, updated.Metadata.State)
	require.Nil(t, updated.Metadata.NextRun)

	var count int
	require.NoError(t, storage.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM cq_messages WHERE queue_name = ?", "q2").Scan(&count))
	require.Equal(t, 1, count)
}

func TestCalendarServiceSkipsCronOnlySchedules(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { _ = storage.Close() })

	queue := &queuepb.Queue{Name: "q-cron", Metadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 1}}
	require.NoError(t, storage.CreateQueue(ctx, queue))

	now := time.Now().Add(-1 * time.Minute)
	schedule := &schedulepb.Schedule{
		ScheduleId: "s-cron",
		Metadata: &schedulepb.Schedule_Metadata{
			State:          schedulepb.Schedule_Metadata_SCHEDULED,
			QueueName:      queue.Name,
			NextRun:        timestamppb.New(now),
			ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{CronSchedule: "* * * * *"},
		},
	}
	require.NoError(t, storage.CreateSchedule(ctx, schedule))

	engine := &stubCalendarEngine{}
	service := NewCalendarService(storage.BaseSQL, engine, time.Second)
	require.NoError(t, service.RunOnce(ctx))

	updated, err := storage.GetSchedule(ctx, schedule.ScheduleId)
	require.NoError(t, err)
	require.Equal(t, schedulepb.Schedule_Metadata_SCHEDULED, updated.Metadata.State)

	var count int
	require.NoError(t, storage.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM cq_messages WHERE queue_name = ?", queue.Name).Scan(&count))
	require.Equal(t, 0, count)
}
