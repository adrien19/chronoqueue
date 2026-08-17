//go:build integration

package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
	"github.com/adrien19/chronoqueue/pkg/log"
)

func TestReclaimExpiredMessage_AtomicAcrossPostgresInstances(t *testing.T) {
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
	queueName := "reclaim-fencing"
	require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
	require.NoError(t, first.EnqueueMessage(ctx, queueName, postgresReclaimTestMessage("finite", 2, 2)))

	claimed, err := first.ClaimMessage(ctx, queueName, "worker-1", "attempt-1", "")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.EqualValues(t, 2, claimed.GetMetadata().GetAttemptsLeft())
	expirePostgresReclaimTestMessage(t, ctx, first, claimed.GetMessageId())
	expired, err := first.FindExpiredMessages(ctx, queueName, 10)
	require.NoError(t, err)
	require.Len(t, expired, 1)
	require.EqualValues(t, 2, expired[0].GetMetadata().GetAttemptsLeft())
	require.Equal(t, "attempt-1", expired[0].GetMetadata().GetCurrentAttempt().GetAttemptId())

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, storage := range []*Storage{first, second} {
		candidate := proto.Clone(expired[0]).(*messagepb.Message)
		wg.Add(1)
		go func(storage *Storage, message *messagepb.Message) {
			defer wg.Done()
			<-start
			errs <- storage.ReclaimExpiredMessage(ctx, queueName, message)
		}(storage, candidate)
	}
	close(start)
	wg.Wait()
	close(errs)

	var successes int
	for reclaimErr := range errs {
		if reclaimErr == nil {
			successes++
			continue
		}
		assert.Contains(t, reclaimErr.Error(), "message is no longer expired")
	}
	require.Equal(t, 1, successes)

	claimed, err = first.ClaimMessage(ctx, queueName, "worker-2", "attempt-2", "")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.EqualValues(t, 1, claimed.GetMetadata().GetAttemptsLeft())
	expirePostgresReclaimTestMessage(t, ctx, first, claimed.GetMessageId())
	expired, err = first.FindExpiredMessages(ctx, queueName, 10)
	require.NoError(t, err)
	require.Len(t, expired, 1)
	require.NoError(t, first.ReclaimExpiredMessage(ctx, queueName, expired[0]))

	peeked, err := first.PeekMessages(ctx, queueName, 10)
	require.NoError(t, err)
	require.Len(t, peeked, 1)
	assert.Equal(t, messagepb.Message_Metadata_ERRORED, peeked[0].GetMetadata().GetState())
	assert.EqualValues(t, 0, peeked[0].GetMetadata().GetAttemptsLeft())
}

func TestWorkerMutations_RequireActiveOwnershipAcrossPostgresInstances(t *testing.T) {
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
	require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: "owned", Metadata: &queuepb.QueueMetadata{}}))
	require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: "other", Metadata: &queuepb.QueueMetadata{}}))

	mutations := map[string]func(context.Context, *Storage, string, string, string) error{
		"ack": func(ctx context.Context, storage *Storage, queue, attempt, worker string) error {
			return storage.AcknowledgeMessage(ctx, queue, "ack", attempt, worker)
		},
		"nack": func(ctx context.Context, storage *Storage, queue, attempt, worker string) error {
			return storage.NackMessage(ctx, queue, "nack", attempt, worker)
		},
		"heartbeat": func(ctx context.Context, storage *Storage, queue, attempt, worker string) error {
			_, _, err := storage.HeartbeatMessage(ctx, queue, "heartbeat", attempt, worker)
			return err
		},
		"renew": func(ctx context.Context, storage *Storage, queue, attempt, worker string) error {
			return storage.ExtendMessageLease(ctx, queue, "renew", attempt, worker, 1_000)
		},
	}

	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, first.EnqueueMessage(ctx, "owned", postgresReclaimTestMessage(name, 1, 1)))
			claimed, err := first.ClaimMessage(ctx, "owned", "worker", "attempt", "")
			require.NoError(t, err)
			require.NotNil(t, claimed)
			require.Error(t, mutate(ctx, first, "other", "attempt", "worker"))
			require.Error(t, mutate(ctx, first, "owned", "stale-attempt", "worker"))
			require.Error(t, mutate(ctx, first, "owned", "attempt", "stale-worker"))
			expirePostgresReclaimTestMessage(t, ctx, first, name)
			require.Error(t, mutate(ctx, first, "owned", "attempt", "worker"))

			var state messagepb.Message_Metadata_State
			require.NoError(t, first.DB.QueryRowContext(ctx, `SELECT state FROM cq_messages WHERE message_id = $1`, name).Scan(&state))
			require.Equal(t, messagepb.Message_Metadata_RUNNING, state)
		})
	}

	require.NoError(t, first.EnqueueMessage(ctx, "owned", postgresReclaimTestMessage("terminal", 1, 1)))
	claimed, err := first.ClaimMessage(ctx, "owned", "worker", "terminal-attempt", "")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, mutate := range []func() error{
		func() error { return first.AcknowledgeMessage(ctx, "owned", "terminal", "terminal-attempt", "worker") },
		func() error { return second.NackMessage(ctx, "owned", "terminal", "terminal-attempt", "worker") },
	} {
		wg.Add(1)
		go func(mutate func() error) {
			defer wg.Done()
			<-start
			errs <- mutate()
		}(mutate)
	}
	close(start)
	wg.Wait()
	close(errs)
	var successes int
	for err := range errs {
		if err == nil {
			successes++
		}
	}
	require.Equal(t, 1, successes)
}

func newPostgresReclaimTestStorage(t *testing.T, ctx context.Context, dsn string) *Storage {
	t.Helper()
	logger := log.NewLogger()
	manager, err := keymanager.NewEncryptionKeyManagerWithConfig(logger, keymanager.Config{Enabled: false})
	require.NoError(t, err)
	storage, err := NewStorage(ctx, &Config{Conn: ConnectionConfig{DSN: dsn}, Logger: logger, KeyManager: manager})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	return storage
}

func postgresReclaimTestMessage(id string, attemptsLeft, maxAttempts int32) *messagepb.Message {
	return &messagepb.Message{
		MessageId: id,
		Metadata: &messagepb.Message_Metadata{
			State:        messagepb.Message_Metadata_PENDING,
			AttemptsLeft: attemptsLeft,
			MaxAttempts:  maxAttempts,
			LeasePolicy: &commonpb.LeasePolicy{
				BaseLease: durationpb.New(time.Hour),
			},
		},
	}
}

func expirePostgresReclaimTestMessage(t *testing.T, ctx context.Context, storage *Storage, messageID string) {
	t.Helper()
	result, err := storage.DB.ExecContext(ctx, `UPDATE cq_messages SET lease_expiry = 0 WHERE message_id = $1`, messageID)
	require.NoError(t, err)
	rows, err := result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, rows)
}
