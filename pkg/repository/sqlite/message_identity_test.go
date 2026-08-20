//go:build sqlite && cgo

package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
)

func TestMessageID_IsUniqueWithinQueue(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "identity.db"))
	for _, queueName := range []string{"queue-a", "queue-b"} {
		require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
	}

	require.NoError(t, storage.EnqueueMessage(ctx, "queue-a", reclaimTestMessage("shared-id", 1, 1)))
	require.NoError(t, storage.EnqueueMessage(ctx, "queue-b", reclaimTestMessage("shared-id", 1, 1)))
	require.ErrorContains(t, storage.EnqueueMessage(ctx, "queue-a", reclaimTestMessage("shared-id", 1, 1)), "duplicate message_id")
}

func TestRetryDLQMessage_IsScopedToQueue(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "dlq-identity.db"))
	for _, queueName := range []string{"queue-a", "queue-b"} {
		require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
		require.NoError(t, storage.EnqueueMessage(ctx, queueName, reclaimTestMessage("shared-id", 1, 1)))
		_, err := storage.DB.ExecContext(ctx, `UPDATE cq_messages SET state = ? WHERE queue_name = ? AND message_id = ?`, messagepb.Message_Metadata_ERRORED, queueName, "shared-id")
		require.NoError(t, err)
	}

	require.NoError(t, storage.RetryDLQMessage(ctx, "queue-b", "shared-id"))
	var queueAState, queueBState messagepb.Message_Metadata_State
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT state FROM cq_messages WHERE queue_name = ? AND message_id = ?`, "queue-a", "shared-id").Scan(&queueAState))
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT state FROM cq_messages WHERE queue_name = ? AND message_id = ?`, "queue-b", "shared-id").Scan(&queueBState))
	require.Equal(t, messagepb.Message_Metadata_ERRORED, queueAState)
	require.Equal(t, messagepb.Message_Metadata_PENDING, queueBState)
}

func TestRetryDLQMessage_RejectsNonDLQMessage(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "dlq-retry-state.db"))
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: "queue-a", Metadata: &queuepb.QueueMetadata{}}))
	require.NoError(t, storage.EnqueueMessage(ctx, "queue-a", reclaimTestMessage("pending-id", 1, 1)))

	err := storage.RetryDLQMessage(ctx, "queue-a", "pending-id")
	require.ErrorContains(t, err, "message is not in DLQ state")

	var state messagepb.Message_Metadata_State
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT state FROM cq_messages WHERE queue_name = ? AND message_id = ?`, "queue-a", "pending-id").Scan(&state))
	require.Equal(t, messagepb.Message_Metadata_PENDING, state)
}

func TestDeleteDLQMessage_IsScopedToQueue(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "dlq-delete-scope.db"))
	for _, queueName := range []string{"queue-a", "queue-b"} {
		require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
		require.NoError(t, storage.EnqueueMessage(ctx, queueName, reclaimTestMessage("shared-id", 1, 1)))
		_, err := storage.DB.ExecContext(ctx, `UPDATE cq_messages SET state = ? WHERE queue_name = ? AND message_id = ?`, messagepb.Message_Metadata_ERRORED, queueName, "shared-id")
		require.NoError(t, err)
	}

	require.NoError(t, storage.DeleteDLQMessage(ctx, "queue-b", "shared-id"))
	var queueACount, queueBCount int
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_messages WHERE queue_name = ? AND message_id = ?`, "queue-a", "shared-id").Scan(&queueACount))
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_messages WHERE queue_name = ? AND message_id = ?`, "queue-b", "shared-id").Scan(&queueBCount))
	require.Equal(t, 1, queueACount)
	require.Zero(t, queueBCount)
}
