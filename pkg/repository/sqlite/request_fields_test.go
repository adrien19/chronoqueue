//go:build sqlite && cgo

package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	repositorysql "github.com/adrien19/chronoqueue/pkg/repository/sql"
)

func TestMessageQueriesPreserveOrderedBinaryHeaders(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "headers.db"))
	queueName := "headers"
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
	message := reclaimTestMessage("message", 2, 2)
	message.Metadata.Headers = []*messagepb.Message_Metadata_Header{
		{Key: "trace-id", Value: []byte{0x00, 0xff}},
		{Key: "trace-id", Value: []byte("second")},
	}
	require.NoError(t, storage.EnqueueMessage(ctx, queueName, message))

	peeked, err := storage.PeekMessagesWithPriorityRange(ctx, queueName, 1, nil)
	require.NoError(t, err)
	require.Len(t, peeked, 1)
	requireHeadersEqual(t, message.Metadata.Headers, peeked[0].GetMetadata().GetHeaders())

	claimed, err := storage.ClaimMessageWithLeaseDuration(ctx, queueName, "worker", "attempt", "", time.Second)
	require.NoError(t, err)
	requireHeadersEqual(t, message.Metadata.Headers, claimed.GetMetadata().GetHeaders())

	_, err = storage.DB.ExecContext(ctx, `UPDATE cq_messages SET state = ? WHERE queue_name = ? AND message_id = ?`, messagepb.Message_Metadata_ERRORED, queueName, message.GetMessageId())
	require.NoError(t, err)
	dlqMessages, err := storage.GetDLQMessages(ctx, queueName, 1)
	require.NoError(t, err)
	require.Len(t, dlqMessages, 1)
	requireHeadersEqual(t, message.Metadata.Headers, dlqMessages[0].GetMetadata().GetHeaders())
}

func requireHeadersEqual(t *testing.T, expected, actual []*messagepb.Message_Metadata_Header) {
	t.Helper()
	require.Len(t, actual, len(expected))
	for i := range expected {
		require.Equal(t, expected[i].GetKey(), actual[i].GetKey())
		require.Equal(t, expected[i].GetValue(), actual[i].GetValue())
	}
}

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
		`SELECT lease_started_at, lease_expiry FROM cq_messages WHERE queue_name = ? AND message_id = ?`, queueName, message.GetMessageId(),
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

func TestRequestFieldQueries_PropagateClosedDatabaseErrors(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "closed.db"))
	require.NoError(t, storage.DB.Close())

	_, err := storage.ClaimMessageWithLeaseDuration(ctx, "queue", "worker", "attempt", "", time.Second)
	require.Error(t, err)
	_, err = storage.PeekMessagesWithPriorityRange(ctx, "queue", 1, &repositorysql.PriorityRange{Min: 1, Max: 2})
	require.Error(t, err)
	_, err = storage.ListQueuesWithPrefix(ctx, "queue")
	require.Error(t, err)
	_, err = storage.ListSchedulesWithPrefix(ctx, "schedule")
	require.Error(t, err)
}
