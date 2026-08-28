//go:build sqlite && cgo

package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	"github.com/adrien19/chronoqueue/internal/domainerror"
)

func TestQueueErrorContract(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "errors.db"))
	queue := &queuepb.Queue{Name: "orders", Metadata: &queuepb.QueueMetadata{}}
	require.NoError(t, storage.CreateQueue(ctx, queue))

	err := storage.CreateQueue(ctx, queue)
	require.Equal(t, codes.AlreadyExists, status.Code(domainerror.ToGRPC(err)))

	_, err = storage.GetQueue(ctx, "missing")
	require.Equal(t, codes.NotFound, status.Code(domainerror.ToGRPC(err)))
}

func TestDLQErrorContract(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "dlq-errors.db"))

	err := storage.DeleteDLQMessage(ctx, "orders-dlq", "missing")
	require.Equal(t, codes.NotFound, status.Code(domainerror.ToGRPC(err)))
	err = storage.RetryDLQMessage(ctx, "orders-dlq", "missing", "orders", false)
	require.Equal(t, codes.NotFound, status.Code(domainerror.ToGRPC(err)))

	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: "orders", Metadata: &queuepb.QueueMetadata{}}))
	require.NoError(t, storage.EnqueueMessage(ctx, "orders", &messagepb.Message{
		MessageId: "pending", Metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_PENDING, AttemptsLeft: 1, MaxAttempts: 1},
	}))
	err = storage.DeleteDLQMessage(ctx, "orders", "pending")
	require.Equal(t, codes.FailedPrecondition, status.Code(domainerror.ToGRPC(err)))
	err = storage.RetryDLQMessage(ctx, "orders", "pending", "orders", false)
	require.Equal(t, codes.FailedPrecondition, status.Code(domainerror.ToGRPC(err)))
}
