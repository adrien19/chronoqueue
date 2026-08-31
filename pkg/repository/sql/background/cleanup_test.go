package background

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
)

func TestCleanupDeletesAndUpdatesCountersAcrossBatches(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	queueName := "cleanup-batches"
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
	for index := range 5 {
		require.NoError(t, storage.EnqueueMessage(ctx, queueName, &messagepb.Message{
			MessageId: fmt.Sprintf("expired-%d", index),
			Metadata:  &messagepb.Message_Metadata{State: messagepb.Message_Metadata_COMPLETED},
		}))
	}
	_, err := storage.DB.ExecContext(ctx, `UPDATE cq_messages SET deleted_at = ? WHERE queue_name = ?`, storage.Clock.NowMs()-1, queueName)
	require.NoError(t, err)

	service := NewCleanupService(storage.BaseSQL, time.Hour)
	service.batchSize = 2
	service.maxDrainBatches = 3
	require.NoError(t, service.cleanupExpiredMessages(ctx))
	var remaining int
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_messages WHERE queue_name = ?`, queueName).Scan(&remaining))
	require.Zero(t, remaining)
	counts, err := storage.StateManager.GetStateCounts(ctx, storage.DB, queueName)
	require.NoError(t, err)
	require.Zero(t, counts["completed"])
}

func TestCleanupCanceledContextLeavesRowsUntouched(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	queueName := "cleanup-canceled"
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
	require.NoError(t, storage.EnqueueMessage(ctx, queueName, &messagepb.Message{MessageId: "expired", Metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_COMPLETED}}))
	_, err := storage.DB.ExecContext(ctx, `UPDATE cq_messages SET deleted_at = ? WHERE queue_name = ?`, storage.Clock.NowMs()-1, queueName)
	require.NoError(t, err)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	err = NewCleanupService(storage.BaseSQL, time.Hour).cleanupExpiredMessages(canceled)
	require.ErrorContains(t, err, "cleanup cycle canceled")
	var remaining int
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_messages WHERE queue_name = ?`, queueName).Scan(&remaining))
	require.Equal(t, 1, remaining)
}
