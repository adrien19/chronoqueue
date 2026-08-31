//go:build integration

package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/internal/domainerror"
	"github.com/adrien19/chronoqueue/pkg/repository/sql/background"
)

func TestScheduleHistorySurvivesMessageAndScheduleDeletion(t *testing.T) {
	ctx := context.Background()
	container, err := postgrescontainer.Run(
		ctx,
		"postgres:17-alpine",
		postgrescontainer.WithDatabase("chronoqueue"),
		postgrescontainer.WithUsername("chronoqueue"),
		postgrescontainer.WithPassword("chronoqueue"),
		postgrescontainer.BasicWaitStrategies(),
		testcontainers.WithTmpfs(map[string]string{"/var/lib/postgresql/data": "rw"}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(ctx)) })
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	storage := newPostgresReclaimTestStorage(t, ctx, dsn)

	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: "history-jobs", Metadata: &queuepb.QueueMetadata{}}))
	schedule := &schedulepb.Schedule{ScheduleId: "durable-history", Metadata: &schedulepb.Schedule_Metadata{
		State: schedulepb.Schedule_Metadata_SCHEDULED, QueueName: "history-jobs",
		ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{CronSchedule: "*/5 * * * *"},
	}}
	require.NoError(t, storage.CreateSchedule(ctx, schedule))
	payload, err := structpb.NewStruct(map[string]any{"job": "snapshot"})
	require.NoError(t, err)
	message := &messagepb.Message{MessageId: "scheduled-message", Metadata: &messagepb.Message_Metadata{
		State: messagepb.Message_Metadata_PENDING, AttemptsLeft: 3, MaxAttempts: 3,
		Payload: &commonpb.Payload{Data: payload, ContentType: "application/json"},
	}}
	require.NoError(t, storage.EnqueueMessage(ctx, "history-jobs", message))
	executedAt := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, storage.RecordScheduleExecution(ctx, schedule.ScheduleId, message.MessageId, executedAt.UnixMilli()))
	_, err = storage.DB.ExecContext(ctx, `DELETE FROM cq_messages WHERE queue_name = $1 AND message_id = $2`, "history-jobs", message.MessageId)
	require.NoError(t, err)

	history, err := storage.GetScheduleHistory(ctx, schedule.ScheduleId, 10)
	require.NoError(t, err)
	require.Len(t, history.GetMessages(), 1)
	require.Len(t, history.GetExecutions(), 1)
	require.Equal(t, executedAt, history.GetExecutions()[0].GetExecutedAt().AsTime())
	require.Equal(t, "snapshot", history.GetExecutions()[0].GetMessage().GetMetadata().GetPayload().GetData().GetFields()["job"].GetStringValue())

	require.NoError(t, storage.DeleteSchedule(ctx, schedule.ScheduleId))
	_, err = storage.GetSchedule(ctx, schedule.ScheduleId)
	require.Equal(t, codes.NotFound, status.Code(domainerror.ToGRPC(err)))
	history, err = storage.GetScheduleHistory(ctx, schedule.ScheduleId, 10)
	require.NoError(t, err)
	require.Len(t, history.GetExecutions(), 1)
}

func TestCronSchedule_ExecutesOnceAcrossPostgresReplicas(t *testing.T) {
	ctx := context.Background()
	container, err := postgrescontainer.Run(
		ctx,
		"postgres:17-alpine",
		postgrescontainer.WithDatabase("chronoqueue"),
		postgrescontainer.WithUsername("chronoqueue"),
		postgrescontainer.WithPassword("chronoqueue"),
		postgrescontainer.BasicWaitStrategies(),
		testcontainers.WithTmpfs(map[string]string{"/var/lib/postgresql/data": "rw"}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(ctx)) })
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	first := newPostgresReclaimTestStorage(t, ctx, dsn)
	second := newPostgresReclaimTestStorage(t, ctx, dsn)
	queue := &queuepb.Queue{Name: "cron-replicas", Metadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 1}}
	require.NoError(t, first.CreateQueue(ctx, queue))
	now := time.Now().UTC().Add(-time.Second)
	payload, err := structpb.NewStruct(map[string]any{"source": "cron-replicas"})
	require.NoError(t, err)
	require.NoError(t, first.CreateSchedule(ctx, &schedulepb.Schedule{
		ScheduleId: "cron-replicas",
		Metadata: &schedulepb.Schedule_Metadata{
			State:          schedulepb.Schedule_Metadata_SCHEDULED,
			QueueName:      queue.Name,
			NextRun:        timestamppb.New(now),
			Payload:        &commonpb.Payload{Data: payload, ContentType: "application/json"},
			ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{CronSchedule: "0 0 1 1 *"},
		},
	}))

	processors := []*background.CronProcessorService{
		background.NewCronProcessorService(first.BaseSQL, time.Second),
		background.NewCronProcessorService(second.BaseSQL, time.Second),
	}
	start := make(chan struct{})
	errs := make(chan error, len(processors))
	var wg sync.WaitGroup
	for _, processor := range processors {
		wg.Add(1)
		go func(processor *background.CronProcessorService) {
			defer wg.Done()
			<-start
			errs <- processor.RunOnce(ctx)
		}(processor)
	}
	close(start)
	wg.Wait()
	close(errs)
	for runErr := range errs {
		require.NoError(t, runErr)
	}

	var count int
	require.NoError(t, first.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_messages WHERE queue_name = $1`, queue.Name).Scan(&count))
	require.Equal(t, 1, count)

	_, err = first.DB.ExecContext(ctx, `UPDATE cq_schedules SET execution_count = 4 WHERE id = $1`, "cron-replicas")
	require.NoError(t, err)
	require.NoError(t, first.PauseSchedule(ctx, "cron-replicas"))
	var executionCount int64
	require.NoError(t, first.DB.QueryRowContext(ctx, `SELECT execution_count FROM cq_schedules WHERE id = $1`, "cron-replicas").Scan(&executionCount))
	require.Equal(t, int64(4), executionCount)
	err = first.PauseSchedule(ctx, "cron-replicas")
	require.Equal(t, codes.FailedPrecondition, status.Code(domainerror.ToGRPC(err)))
	require.NoError(t, first.ResumeSchedule(ctx, "cron-replicas"))
	require.NoError(t, first.DB.QueryRowContext(ctx, `SELECT execution_count FROM cq_schedules WHERE id = $1`, "cron-replicas").Scan(&executionCount))
	require.Zero(t, executionCount)
}
