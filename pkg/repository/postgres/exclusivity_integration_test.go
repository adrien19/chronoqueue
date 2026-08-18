//go:build integration

package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
)

func TestExclusiveQueue_SerializesClaimsAcrossPostgresInstancesAndRecoversAfterReclaim(t *testing.T) {
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
	queueName := "exclusive"
	key := "orders"
	require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{
		Type: queuepb.QueueType_EXCLUSIVE, ExclusivityKey: key,
	}}))
	for _, messageID := range []string{"first", "second"} {
		require.NoError(t, first.EnqueueMessage(ctx, queueName, postgresReclaimTestMessage(messageID, 2, 2)))
	}

	_, err = first.ClaimMessage(ctx, queueName, "worker", "attempt", "")
	require.Error(t, err)
	_, err = first.ClaimMessage(ctx, queueName, "worker", "attempt", "wrong")
	require.Error(t, err)

	type claimResult struct {
		message *messagepb.Message
		err     error
	}
	start := make(chan struct{})
	results := make(chan claimResult, 2)
	var wg sync.WaitGroup
	for index, storage := range []*Storage{first, second} {
		worker := fmt.Sprintf("worker-%d", index+1)
		attempt := fmt.Sprintf("attempt-%d", index+1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			message, claimErr := storage.ClaimMessage(ctx, queueName, worker, attempt, key)
			results <- claimResult{message: message, err: claimErr}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var owner claimResult
	var claimed int
	for result := range results {
		require.NoError(t, result.err)
		if result.message != nil {
			owner = result
			claimed++
		}
	}
	require.Equal(t, 1, claimed)
	blocked, err := second.ClaimMessage(ctx, queueName, "blocked", "blocked", key)
	require.NoError(t, err)
	require.Nil(t, blocked)

	expirePostgresReclaimTestMessage(t, ctx, first, owner.message.GetMessageId())
	expired, err := first.FindExpiredMessages(ctx, queueName, 10)
	require.NoError(t, err)
	require.Len(t, expired, 1)
	require.NoError(t, first.ReclaimExpiredMessage(ctx, queueName, expired[0]))

	reclaimed, err := second.ClaimMessage(ctx, queueName, "worker-3", "attempt-3", key)
	require.NoError(t, err)
	require.NotNil(t, reclaimed)
	require.Equal(t, owner.message.GetMessageId(), reclaimed.GetMessageId())
	require.NoError(t, second.AcknowledgeMessage(ctx, queueName, reclaimed.GetMessageId(), "attempt-3", "worker-3"))

	next, err := first.ClaimMessage(ctx, queueName, "worker-4", "attempt-4", key)
	require.NoError(t, err)
	require.NotNil(t, next)
	require.NotEqual(t, reclaimed.GetMessageId(), next.GetMessageId())
}

func TestExclusiveQueue_IgnoresExpiredRunningLease(t *testing.T) {
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
	queueName := "exclusive-expired"
	key := "orders"
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{
		Type: queuepb.QueueType_EXCLUSIVE, ExclusivityKey: key,
	}}))
	for _, messageID := range []string{"first", "second"} {
		require.NoError(t, storage.EnqueueMessage(ctx, queueName, postgresReclaimTestMessage(messageID, 2, 2)))
	}

	first, err := storage.ClaimMessage(ctx, queueName, "worker-1", "attempt-1", key)
	require.NoError(t, err)
	require.NotNil(t, first)
	expirePostgresReclaimTestMessage(t, ctx, storage, first.GetMessageId())

	second, err := storage.ClaimMessage(ctx, queueName, "worker-2", "attempt-2", key)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.NotEqual(t, first.GetMessageId(), second.GetMessageId())
}
