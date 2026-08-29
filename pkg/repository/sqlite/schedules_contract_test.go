//go:build sqlite && cgo

package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/internal/domainerror"
)

func TestScheduleHistorySurvivesMessageAndScheduleDeletion(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "durable-schedule-history.db"))
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: "jobs", Metadata: &queuepb.QueueMetadata{}}))
	schedule := &schedulepb.Schedule{ScheduleId: "durable-history", Metadata: &schedulepb.Schedule_Metadata{
		State: schedulepb.Schedule_Metadata_SCHEDULED, QueueName: "jobs",
		ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{CronSchedule: "*/5 * * * *"},
	}}
	require.NoError(t, storage.CreateSchedule(ctx, schedule))

	payload, err := structpb.NewStruct(map[string]any{"job": "snapshot"})
	require.NoError(t, err)
	message := &messagepb.Message{MessageId: "scheduled-message", Metadata: &messagepb.Message_Metadata{
		State: messagepb.Message_Metadata_PENDING, AttemptsLeft: 3, MaxAttempts: 3,
		Payload: &commonpb.Payload{Data: payload, ContentType: "application/json"},
	}}
	require.NoError(t, storage.EnqueueMessage(ctx, "jobs", message))
	executedAt := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, storage.RecordScheduleExecution(ctx, schedule.ScheduleId, message.MessageId, executedAt.UnixMilli()))

	_, err = storage.DB.ExecContext(ctx, `DELETE FROM cq_messages WHERE queue_name = ? AND message_id = ?`, "jobs", message.MessageId)
	require.NoError(t, err)
	history, err := storage.GetScheduleHistory(ctx, schedule.ScheduleId, 10)
	require.NoError(t, err)
	require.Len(t, history.GetMessages(), 1)
	require.Len(t, history.GetExecutions(), 1)
	require.Equal(t, message.MessageId, history.GetExecutions()[0].GetMessageId())
	require.Equal(t, executedAt, history.GetExecutions()[0].GetExecutedAt().AsTime())
	require.True(t, history.GetExecutions()[0].GetSuccess())
	require.Equal(t, "snapshot", history.GetExecutions()[0].GetMessage().GetMetadata().GetPayload().GetData().GetFields()["job"].GetStringValue())

	require.NoError(t, storage.DeleteSchedule(ctx, schedule.ScheduleId))
	_, err = storage.GetSchedule(ctx, schedule.ScheduleId)
	require.Equal(t, codes.NotFound, status.Code(domainerror.ToGRPC(err)))
	history, err = storage.GetScheduleHistory(ctx, schedule.ScheduleId, 10)
	require.NoError(t, err)
	require.Equal(t, schedule.ScheduleId, history.GetScheduleId())
	require.Len(t, history.GetExecutions(), 1)
	require.Len(t, history.GetMessages(), 1)
}

func TestPauseScheduleContract(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "schedule-contract.db"))
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: "jobs", Metadata: &queuepb.QueueMetadata{}}))
	oldUpdatedAt := timestamppb.New(time.Unix(1, 0))
	schedule := &schedulepb.Schedule{ScheduleId: "pause-contract", Metadata: &schedulepb.Schedule_Metadata{
		State: schedulepb.Schedule_Metadata_SCHEDULED, QueueName: "jobs", UpdatedAt: oldUpdatedAt,
		ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{CronSchedule: "*/5 * * * *"},
	}}
	require.NoError(t, storage.CreateSchedule(ctx, schedule))
	_, err := storage.DB.ExecContext(ctx, `UPDATE cq_schedules SET execution_count = 3 WHERE id = ?`, schedule.ScheduleId)
	require.NoError(t, err)

	require.NoError(t, storage.PauseSchedule(ctx, schedule.ScheduleId))
	paused, err := storage.GetSchedule(ctx, schedule.ScheduleId)
	require.NoError(t, err)
	require.True(t, paused.GetMetadata().GetUpdatedAt().AsTime().After(oldUpdatedAt.AsTime()))
	var executionCount, updatedAt int64
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT execution_count, updated_at FROM cq_schedules WHERE id = ?`, schedule.ScheduleId).Scan(&executionCount, &updatedAt))
	require.Equal(t, int64(3), executionCount)
	require.Equal(t, paused.GetMetadata().GetUpdatedAt().AsTime().UnixMilli(), updatedAt)

	err = storage.PauseSchedule(ctx, schedule.ScheduleId)
	require.Equal(t, codes.FailedPrecondition, status.Code(domainerror.ToGRPC(err)))
}
