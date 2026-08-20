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
	"google.golang.org/protobuf/types/known/timestamppb"

	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/pkg/repository/sql/background"
)

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
	now := time.Date(2025, time.January, 5, 10, 0, 0, 0, time.UTC)
	require.NoError(t, first.CreateSchedule(ctx, &schedulepb.Schedule{
		ScheduleId: "cron-replicas",
		Metadata: &schedulepb.Schedule_Metadata{
			State:          schedulepb.Schedule_Metadata_SCHEDULED,
			QueueName:      queue.Name,
			NextRun:        timestamppb.New(now),
			ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{CronSchedule: "*/2 * * * *"},
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
}
