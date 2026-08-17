//go:build sqlite && cgo

package sqlite

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
)

func TestWorkerMutations_RequireActiveOwnership(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "ownership.db"))
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: "owned", Metadata: &queuepb.QueueMetadata{}}))
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: "other", Metadata: &queuepb.QueueMetadata{}}))

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
			require.NoError(t, storage.EnqueueMessage(ctx, "owned", reclaimTestMessage(name, 1, 1)))
			claimed, err := storage.ClaimMessage(ctx, "owned", "worker", "attempt")
			require.NoError(t, err)
			require.NotNil(t, claimed)

			require.Error(t, mutate(ctx, storage, "other", "attempt", "worker"))
			require.Error(t, mutate(ctx, storage, "owned", "stale-attempt", "worker"))
			require.Error(t, mutate(ctx, storage, "owned", "attempt", "stale-worker"))
			expireReclaimTestMessage(t, ctx, storage, name)
			require.Error(t, mutate(ctx, storage, "owned", "attempt", "worker"))

			var state messagepb.Message_Metadata_State
			require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT state FROM cq_messages WHERE message_id = ?`, name).Scan(&state))
			require.Equal(t, messagepb.Message_Metadata_RUNNING, state)
		})
	}
}

func TestWorkerMutations_OnlyOneConcurrentTerminalTransitionWins(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ownership-race.db")
	first := newReclaimTestStorage(t, ctx, path)
	second := newReclaimTestStorage(t, ctx, path)
	require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: "owned", Metadata: &queuepb.QueueMetadata{}}))
	require.NoError(t, first.EnqueueMessage(ctx, "owned", reclaimTestMessage("terminal", 1, 1)))
	claimed, err := first.ClaimMessage(ctx, "owned", "worker", "attempt")
	require.NoError(t, err)
	require.NotNil(t, claimed)

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, mutate := range []func() error{
		func() error { return first.AcknowledgeMessage(ctx, "owned", "terminal", "attempt", "worker") },
		func() error { return second.NackMessage(ctx, "owned", "terminal", "attempt", "worker") },
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
