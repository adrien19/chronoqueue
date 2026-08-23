//go:build sqlite && cgo

package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
