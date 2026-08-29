//go:build sqlite && cgo

package sqlite

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	"github.com/adrien19/chronoqueue/internal/domainerror"
)

func TestExtendMessageLease_ReturnsPolicyCappedRemainingTime(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "renew-remaining.db"))
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: "owned", Metadata: &queuepb.QueueMetadata{}}))
	message := reclaimTestMessage("renew-capped", 1, 1)
	message.Metadata.LeasePolicy = &commonpb.LeasePolicy{
		BaseLease:    durationpb.New(time.Minute),
		MaxExtension: durationpb.New(5 * time.Second),
		ExtendStep:   durationpb.New(time.Second),
	}
	require.NoError(t, storage.EnqueueMessage(ctx, "owned", message))
	claimed, err := storage.ClaimMessage(ctx, "owned", "worker", "attempt", "")
	require.NoError(t, err)
	require.NotNil(t, claimed)

	remainingMs, err := storage.ExtendMessageLease(ctx, "owned", message.GetMessageId(), "attempt", "worker", int64((10 * time.Second).Milliseconds()))
	require.NoError(t, err)
	require.Positive(t, remainingMs)
	require.LessOrEqual(t, remainingMs, int64((65 * time.Second).Milliseconds()))
	require.Greater(t, remainingMs, int64((64 * time.Second).Milliseconds()))
}

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
			_, err := storage.ExtendMessageLease(ctx, queue, "renew", attempt, worker, 1_000)
			return err
		},
	}

	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, storage.EnqueueMessage(ctx, "owned", reclaimTestMessage(name, 1, 1)))
			claimed, err := storage.ClaimMessage(ctx, "owned", "worker", "attempt", "")
			require.NoError(t, err)
			require.NotNil(t, claimed)

			require.Equal(t, codes.NotFound, status.Code(domainerror.ToGRPC(mutate(ctx, storage, "other", "attempt", "worker"))))
			require.Equal(t, codes.FailedPrecondition, status.Code(domainerror.ToGRPC(mutate(ctx, storage, "owned", "stale-attempt", "worker"))))
			require.Equal(t, codes.FailedPrecondition, status.Code(domainerror.ToGRPC(mutate(ctx, storage, "owned", "attempt", "stale-worker"))))
			expireReclaimTestMessage(t, ctx, storage, name)
			require.Equal(t, codes.DeadlineExceeded, status.Code(domainerror.ToGRPC(mutate(ctx, storage, "owned", "attempt", "worker"))))

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
	claimed, err := first.ClaimMessage(ctx, "owned", "worker", "attempt", "")
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
