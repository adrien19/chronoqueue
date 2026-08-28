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
	"google.golang.org/protobuf/types/known/timestamppb"

	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/internal/domainerror"
)

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
