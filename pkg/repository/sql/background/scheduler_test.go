package background

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
)

func TestSchedulerActivationIsCompareAndSet(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	queueName := "scheduled-cas"
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
	due := time.Now().Add(-time.Minute)
	message := &messagepb.Message{
		MessageId: "due-message",
		Metadata: &messagepb.Message_Metadata{
			State:         messagepb.Message_Metadata_INVISIBLE,
			ScheduledTime: timestamppb.New(due),
		},
	}
	require.NoError(t, storage.EnqueueMessage(ctx, queueName, message))
	_, err := storage.DB.ExecContext(ctx, `UPDATE cq_queues SET state_counts = '{"invisible":1}' WHERE name = ?`, queueName)
	require.NoError(t, err)

	service := NewSchedulerService(storage.BaseSQL, time.Second)
	candidates, err := service.collectScheduledMessages(ctx, storage.Clock.NowMs(), service.batchSize)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	candidate := candidates[0]
	candidate.message.Metadata.State = messagepb.Message_Metadata_PENDING

	activated, err := service.activateMessage(ctx, candidate.id, candidate.queueName, candidate.messageID, candidate.message, messagepb.Message_Metadata_INVISIBLE, storage.Clock.NowMs())
	require.NoError(t, err)
	require.True(t, activated)

	activated, err = service.activateMessage(ctx, candidate.id, candidate.queueName, candidate.messageID, candidate.message, messagepb.Message_Metadata_INVISIBLE, storage.Clock.NowMs())
	require.NoError(t, err)
	require.False(t, activated)

	counts, err := storage.StateManager.GetStateCounts(ctx, storage.DB, queueName)
	require.NoError(t, err)
	require.EqualValues(t, 0, counts["invisible"])
	require.EqualValues(t, 1, counts["pending"])
}

func TestSchedulerDrainsMultipleBatchesInOneCycle(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	queueName := "scheduled-drain"
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
	due := time.Now().Add(-time.Minute)
	for index := range 4 {
		require.NoError(t, storage.EnqueueMessage(ctx, queueName, &messagepb.Message{
			MessageId: fmt.Sprintf("due-%d", index),
			Metadata: &messagepb.Message_Metadata{
				State:         messagepb.Message_Metadata_INVISIBLE,
				ScheduledTime: timestamppb.New(due),
			},
		}))
	}
	service := NewSchedulerService(storage.BaseSQL, time.Second)
	service.batchSize = 1
	service.maxDrainBatches = 3
	require.NoError(t, service.RunOnce(ctx))
	counts, err := storage.StateManager.GetStateCounts(ctx, storage.DB, queueName)
	require.NoError(t, err)
	require.EqualValues(t, 3, counts["pending"])
	require.EqualValues(t, 1, counts["invisible"])
}
