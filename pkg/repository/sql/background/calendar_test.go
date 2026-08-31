package background

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/pkg/calendar"
	"github.com/adrien19/chronoqueue/pkg/log"
	repositorycommon "github.com/adrien19/chronoqueue/pkg/repository/common"
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
			MessageIds: []string{"legacy-message"},
			Headers: []*messagepb.Message_Metadata_Header{
				{Key: "trace-id", Value: []byte{0x00, 0xff}},
				{Key: "trace-id", Value: []byte("second")},
			},
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
	var messageBytes []byte
	require.NoError(t, storage.DB.QueryRowContext(ctx, "SELECT metadata_pb FROM cq_messages WHERE queue_name = ?", "q1").Scan(&messageBytes))
	createdMessage, err := storage.Serializer.UnmarshalMessage(messageBytes)
	require.NoError(t, err)
	headers := createdMessage.GetMetadata().GetHeaders()
	require.Len(t, headers, 2)
	require.Equal(t, "trace-id", headers[0].GetKey())
	require.Equal(t, []byte{0x00, 0xff}, headers[0].GetValue())
	require.Equal(t, "trace-id", headers[1].GetKey())
	require.Equal(t, []byte("second"), headers[1].GetValue())

	updated, err := storage.GetSchedule(ctx, "s1")
	require.NoError(t, err)
	require.NotNil(t, updated.Metadata.LastRun)
	require.Equal(t, now.UnixMilli(), updated.Metadata.LastRun.AsTime().UnixMilli())
	require.NotNil(t, updated.Metadata.NextRun)
	require.Equal(t, next.UnixMilli(), updated.Metadata.NextRun.AsTime().UnixMilli())
	require.Equal(t, schedulepb.Schedule_Metadata_SCHEDULED, updated.Metadata.State)
	require.False(t, repositorycommon.HasLegacyScheduleMessageIDs(updated.GetMetadata()))
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
			Payload:        &commonpb.Payload{Data: &structpb.Struct{}, ContentType: "application/json"},
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

func TestCalendarServiceEnforcesMaxMessages(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	queue := &queuepb.Queue{Name: "calendar-limited", Metadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 1}}
	require.NoError(t, storage.CreateQueue(ctx, queue))
	due := time.Now().Add(-time.Minute)
	next := due.Add(time.Second)
	payload, err := structpb.NewStruct(map[string]any{"ok": true})
	require.NoError(t, err)
	schedule := &schedulepb.Schedule{ScheduleId: "calendar-limited", Metadata: &schedulepb.Schedule_Metadata{
		State: schedulepb.Schedule_Metadata_SCHEDULED, QueueName: queue.Name, NextRun: timestamppb.New(due),
		HasMaxMessages: true, MaxMessages: 2, Payload: &commonpb.Payload{Data: payload},
		ScheduleConfig: &schedulepb.Schedule_Metadata_CalendarSchedule{CalendarSchedule: &schedulepb.CalendarSchedule{}},
	}}
	require.NoError(t, storage.CreateSchedule(ctx, schedule))
	service := NewCalendarService(storage.BaseSQL, &stubCalendarEngine{nextRun: &next}, time.Second)
	require.NoError(t, service.RunOnce(ctx))
	require.NoError(t, service.RunOnce(ctx))
	require.NoError(t, service.RunOnce(ctx))

	var messageCount, executionCount int64
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_messages WHERE queue_name = ?`, queue.Name).Scan(&messageCount))
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT execution_count FROM cq_schedules WHERE id = ?`, schedule.ScheduleId).Scan(&executionCount))
	require.Equal(t, int64(2), messageCount)
	require.Equal(t, int64(2), executionCount)
	updated, err := storage.GetSchedule(ctx, schedule.ScheduleId)
	require.NoError(t, err)
	require.Equal(t, schedulepb.Schedule_Metadata_PAUSED, updated.GetMetadata().GetState())
	require.Contains(t, updated.GetMetadata().GetStateMessage(), "maximum message count")
}

func TestCalendarServiceRetainsNextRunAndExecutesAfterResume(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	queue := &queuepb.Queue{Name: "calendar-already-limited", Metadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 1}}
	require.NoError(t, storage.CreateQueue(ctx, queue))
	due := time.Now().Add(-time.Minute)
	schedule := &schedulepb.Schedule{ScheduleId: "calendar-already-limited", Metadata: &schedulepb.Schedule_Metadata{
		State: schedulepb.Schedule_Metadata_SCHEDULED, QueueName: queue.Name, NextRun: timestamppb.New(due),
		HasMaxMessages: true, MaxMessages: 1,
		Payload:        &commonpb.Payload{Data: &structpb.Struct{}, ContentType: "application/json"},
		ScheduleConfig: &schedulepb.Schedule_Metadata_CalendarSchedule{CalendarSchedule: &schedulepb.CalendarSchedule{}},
	}}
	require.NoError(t, storage.CreateSchedule(ctx, schedule))
	_, err := storage.DB.ExecContext(ctx, `UPDATE cq_schedules SET execution_count = 1 WHERE id = ?`, schedule.ScheduleId)
	require.NoError(t, err)

	require.NoError(t, NewCalendarService(storage.BaseSQL, &stubCalendarEngine{}, time.Second).RunOnce(ctx))

	var nextRun sql.NullInt64
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT next_run FROM cq_schedules WHERE id = ?`, schedule.ScheduleId).Scan(&nextRun))
	require.True(t, nextRun.Valid)
	updated, err := storage.GetSchedule(ctx, schedule.ScheduleId)
	require.NoError(t, err)
	require.Equal(t, schedulepb.Schedule_Metadata_PAUSED, updated.GetMetadata().GetState())
	require.NotNil(t, updated.GetMetadata().GetNextRun())

	require.NoError(t, storage.ResumeSchedule(ctx, schedule.ScheduleId))
	require.NoError(t, NewCalendarService(storage.BaseSQL, &stubCalendarEngine{}, time.Second).RunOnce(ctx))
	var messageCount int
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_messages WHERE queue_name = ?`, queue.Name).Scan(&messageCount))
	require.Equal(t, 1, messageCount)
}

func TestCalendarServiceRejectsMessageOutsideQueueAdmissionContract(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	queue := &queuepb.Queue{Name: "calendar-validation", Metadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 1, MaxPayloadSize: 1}}
	require.NoError(t, storage.CreateQueue(ctx, queue))
	due := time.Now().Add(-time.Minute)
	next := due.Add(time.Hour)
	payload, err := structpb.NewStruct(map[string]any{"too": "large"})
	require.NoError(t, err)
	schedule := &schedulepb.Schedule{ScheduleId: "calendar-validation", Metadata: &schedulepb.Schedule_Metadata{
		State: schedulepb.Schedule_Metadata_SCHEDULED, QueueName: queue.Name, NextRun: timestamppb.New(due),
		Payload:        &commonpb.Payload{Data: payload, ContentType: "application/json"},
		ScheduleConfig: &schedulepb.Schedule_Metadata_CalendarSchedule{CalendarSchedule: &schedulepb.CalendarSchedule{}},
	}}
	require.NoError(t, storage.CreateSchedule(ctx, schedule))
	require.NoError(t, NewCalendarService(storage.BaseSQL, &stubCalendarEngine{nextRun: &next}, time.Second).RunOnce(ctx))

	var messageCount int
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_messages WHERE queue_name = ?`, queue.Name).Scan(&messageCount))
	require.Zero(t, messageCount)
	updated, err := storage.GetSchedule(ctx, schedule.ScheduleId)
	require.NoError(t, err)
	require.Equal(t, schedulepb.Schedule_Metadata_ERRORED, updated.GetMetadata().GetState())
	require.Contains(t, updated.GetMetadata().GetStateMessage(), "scheduled message validation failed")
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

func TestCalendarServiceDoesNotRecordConflictingMessage(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	queue := &queuepb.Queue{Name: "calendar-conflict", Metadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 1}}
	require.NoError(t, storage.CreateQueue(ctx, queue))
	require.NoError(t, storage.EnqueueMessage(ctx, queue.Name, &messagepb.Message{
		MessageId: "duplicate-id",
		Metadata:  &messagepb.Message_Metadata{State: messagepb.Message_Metadata_PENDING, AttemptsLeft: 1, MaxAttempts: 1},
	}))
	countsBefore, err := storage.StateManager.GetStateCounts(ctx, storage.DB, queue.Name)
	require.NoError(t, err)

	now := time.Now().Add(-time.Minute)
	next := now.Add(time.Hour)
	schedule := &schedulepb.Schedule{
		ScheduleId: "calendar-conflict",
		Metadata: &schedulepb.Schedule_Metadata{
			MessageIds:     []string{"legacy-message"},
			State:          schedulepb.Schedule_Metadata_SCHEDULED,
			QueueName:      queue.Name,
			NextRun:        timestamppb.New(now),
			Payload:        &commonpb.Payload{Data: &structpb.Struct{}, ContentType: "application/json"},
			ScheduleConfig: &schedulepb.Schedule_Metadata_CalendarSchedule{CalendarSchedule: &schedulepb.CalendarSchedule{}},
		},
	}
	require.NoError(t, storage.CreateSchedule(ctx, schedule))

	service := NewCalendarService(storage.BaseSQL, &stubCalendarEngine{nextRun: &next}, time.Second)
	service.generateID = func() (string, error) { return "duplicate-id", nil }
	require.NoError(t, service.RunOnce(ctx))

	var messageCount, historyCount int
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_messages WHERE queue_name = ?`, queue.Name).Scan(&messageCount))
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_schedule_history WHERE schedule_id = ?`, schedule.ScheduleId).Scan(&historyCount))
	require.Equal(t, 1, messageCount)
	require.Equal(t, 1, historyCount)
	var success int
	var errorMessage string
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT success, error_message FROM cq_schedule_history WHERE schedule_id = ?`, schedule.ScheduleId).Scan(&success, &errorMessage))
	require.Zero(t, success)
	require.Contains(t, errorMessage, "insert message")

	counts, err := storage.StateManager.GetStateCounts(ctx, storage.DB, queue.Name)
	require.NoError(t, err)
	require.Equal(t, countsBefore, counts)

	updated, err := storage.GetSchedule(ctx, schedule.ScheduleId)
	require.NoError(t, err)
	require.Equal(t, schedulepb.Schedule_Metadata_ERRORED, updated.GetMetadata().GetState())
	require.Contains(t, updated.GetMetadata().GetStateMessage(), "insert message")
	require.True(t, repositorycommon.HasLegacyScheduleMessageIDs(updated.GetMetadata()))
}
