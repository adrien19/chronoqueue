//go:build sqlite && cgo

package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	repositorysql "github.com/adrien19/chronoqueue/pkg/repository/sql"
)

func TestClaimMessage_UsesRequestedLeaseDuration(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "lease-duration.db"))
	queueName := "lease-duration"
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
	require.NoError(t, storage.EnqueueMessage(ctx, queueName, reclaimTestMessage("message", 1, 1)))

	message, err := storage.ClaimMessageWithLeaseDuration(ctx, queueName, "worker", "attempt", "", 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, message)

	var leaseStartedAt, leaseExpiry int64
	require.NoError(t, storage.DB.QueryRowContext(ctx,
		`SELECT lease_started_at, lease_expiry FROM cq_messages WHERE message_id = ?`, message.GetMessageId(),
	).Scan(&leaseStartedAt, &leaseExpiry))
	assert.EqualValues(t, 5*time.Second/time.Millisecond, leaseExpiry-leaseStartedAt)
}

func TestPeekMessages_FiltersPriorityRange(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "priority-range.db"))
	queueName := "priority-range"
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
	for priority := int64(0); priority <= 4; priority++ {
		message := reclaimTestMessage(string(rune('a'+priority)), 1, 1)
		message.Metadata.Priority = priority
		require.NoError(t, storage.EnqueueMessage(ctx, queueName, message))
	}

	messages, err := storage.PeekMessagesWithPriorityRange(ctx, queueName, 10, &repositorysql.PriorityRange{Min: 2, Max: 4})
	require.NoError(t, err)
	require.Len(t, messages, 3)
	for _, message := range messages {
		assert.GreaterOrEqual(t, message.GetMetadata().GetPriority(), int64(2))
		assert.LessOrEqual(t, message.GetMetadata().GetPriority(), int64(4))
	}
}

func TestListResources_FiltersLiteralPrefixes(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "list-prefix.db"))
	for _, name := range []string{"orders-eu", "orders-us", "payments"} {
		require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: name, Metadata: &queuepb.QueueMetadata{}}))
	}
	for _, id := range []string{"billing-daily", "billing-monthly", "cleanup"} {
		require.NoError(t, storage.CreateSchedule(ctx, &schedulepb.Schedule{
			ScheduleId: id,
			Metadata:   &schedulepb.Schedule_Metadata{QueueName: "orders-eu"},
		}))
	}

	queues, err := storage.ListQueuesWithPrefix(ctx, "orders-")
	require.NoError(t, err)
	require.Len(t, queues, 2)
	allQueues, err := storage.ListQueuesWithPrefix(ctx, "")
	require.NoError(t, err)
	require.Len(t, allQueues, 3)
	literalQueues, err := storage.ListQueuesWithPrefix(ctx, "%")
	require.NoError(t, err)
	require.Empty(t, literalQueues)

	schedules, err := storage.ListSchedulesWithPrefix(ctx, "billing-")
	require.NoError(t, err)
	require.Len(t, schedules, 2)
	allSchedules, err := storage.ListSchedulesWithPrefix(ctx, "")
	require.NoError(t, err)
	require.Len(t, allSchedules, 3)
	literalSchedules, err := storage.ListSchedulesWithPrefix(ctx, "%")
	require.NoError(t, err)
	require.Empty(t, literalSchedules)
}
