//go:build sqlite && cgo

package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	"github.com/adrien19/chronoqueue/internal/domainerror"
)

func TestDeleteQueue_RejectsReferencedDLQ(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "referenced-dlq.db"))
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: "orders-dlq", Metadata: &queuepb.QueueMetadata{}}))
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{
		Name:     "orders",
		Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "orders-dlq"},
	}))
	isDLQ, err := storage.IsDLQ(ctx, "orders-dlq")
	require.NoError(t, err)
	require.True(t, isDLQ)

	err = storage.DeleteQueue(ctx, "orders-dlq")
	require.ErrorContains(t, err, "referenced as a dead letter queue")
	require.Equal(t, codes.FailedPrecondition, status.Code(domainerror.ToGRPC(err)))

	_, err = storage.GetQueue(ctx, "orders-dlq")
	require.NoError(t, err)
	require.NoError(t, storage.DeleteQueue(ctx, "orders"))
	isDLQ, err = storage.IsDLQ(ctx, "orders-dlq")
	require.NoError(t, err)
	require.False(t, isDLQ)
	require.NoError(t, storage.DeleteQueue(ctx, "orders-dlq"))
}

func TestCreateQueue_ValidatesDLQInsideDeleteIntegrityBoundary(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "concurrent-dlq.db")
	creator := newReclaimTestStorage(t, ctx, databasePath)
	deleter := newReclaimTestStorage(t, ctx, databasePath)

	err := creator.CreateQueue(ctx, &queuepb.Queue{
		Name:     "missing-source",
		Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "missing-dlq"},
	})
	require.ErrorContains(t, err, `dead letter queue "missing-dlq" not found`)
	require.Equal(t, codes.NotFound, status.Code(domainerror.ToGRPC(err)))
	_, err = creator.GetQueue(ctx, "missing-source")
	require.Error(t, err)

	for iteration := range 20 {
		dlqName := fmt.Sprintf("concurrent-dlq-%d", iteration)
		sourceName := fmt.Sprintf("concurrent-source-%d", iteration)
		require.NoError(t, creator.CreateQueue(ctx, &queuepb.Queue{Name: dlqName, Metadata: &queuepb.QueueMetadata{}}))

		start := make(chan struct{})
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			errs <- creator.CreateQueue(ctx, &queuepb.Queue{
				Name:     sourceName,
				Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: dlqName},
			})
		}()
		go func() {
			defer wg.Done()
			<-start
			errs <- deleter.DeleteQueue(ctx, dlqName)
		}()
		close(start)
		wg.Wait()
		close(errs)
		successes := 0
		for operationErr := range errs {
			if operationErr == nil {
				successes++
			}
		}
		require.Positive(t, successes)

		_, sourceErr := creator.GetQueue(ctx, sourceName)
		_, dlqErr := creator.GetQueue(ctx, dlqName)
		if sourceErr == nil {
			require.NoError(t, dlqErr, "dependent queue committed without its dead-letter queue")
		}
	}
}

func TestCreateQueueWithDLQ_IsAtomic(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "atomic-auto-dlq.db"))
	source := &queuepb.Queue{Name: "orders", Metadata: &queuepb.QueueMetadata{AutoCreateDlq: true, DeadLetterQueueName: "orders_dlq"}}
	dlq := &queuepb.Queue{Name: "orders_dlq", Metadata: &queuepb.QueueMetadata{}}

	require.NoError(t, storage.CreateQueueWithDLQ(ctx, source, dlq))
	_, err := storage.GetQueue(ctx, source.GetName())
	require.NoError(t, err)
	_, err = storage.GetQueue(ctx, dlq.GetName())
	require.NoError(t, err)

	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: "collision_dlq", Metadata: &queuepb.QueueMetadata{}}))
	err = storage.CreateQueueWithDLQ(ctx,
		&queuepb.Queue{Name: "collision", Metadata: &queuepb.QueueMetadata{AutoCreateDlq: true, DeadLetterQueueName: "collision_dlq"}},
		&queuepb.Queue{Name: "collision_dlq", Metadata: &queuepb.QueueMetadata{}},
	)
	require.Error(t, err)
	_, err = storage.GetQueue(ctx, "collision")
	require.Error(t, err)
}

func TestCreateQueueWithDLQ_RejectsInvalidRelationshipWithoutInserting(t *testing.T) {
	tests := []struct {
		name   string
		source *queuepb.Queue
		dlq    *queuepb.Queue
	}{
		{
			name:   "empty target",
			source: &queuepb.Queue{Name: "empty-target-source", Metadata: &queuepb.QueueMetadata{AutoCreateDlq: true}},
			dlq:    &queuepb.Queue{Name: "empty-target-dlq", Metadata: &queuepb.QueueMetadata{}},
		},
		{
			name:   "empty DLQ name",
			source: &queuepb.Queue{Name: "empty-name-source", Metadata: &queuepb.QueueMetadata{AutoCreateDlq: true, DeadLetterQueueName: "expected-dlq"}},
			dlq:    &queuepb.Queue{Metadata: &queuepb.QueueMetadata{}},
		},
		{
			name:   "mismatched target",
			source: &queuepb.Queue{Name: "mismatch-source", Metadata: &queuepb.QueueMetadata{AutoCreateDlq: true, DeadLetterQueueName: "expected-dlq"}},
			dlq:    &queuepb.Queue{Name: "created-dlq", Metadata: &queuepb.QueueMetadata{}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "invalid-auto-dlq.db"))

			err := storage.CreateQueueWithDLQ(ctx, test.source, test.dlq)
			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(domainerror.ToGRPC(err)))

			_, err = storage.GetQueue(ctx, test.source.GetName())
			require.Error(t, err)
			if test.dlq.GetName() != "" {
				_, err = storage.GetQueue(ctx, test.dlq.GetName())
				require.Error(t, err)
			}
		})
	}
}

func TestPurgeDLQDeletesAcrossBoundedBatchesAndUpdatesCounter(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "batched-purge.db"))
	queueName := "batched-dlq"
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
	for index := range dlqPurgeBatchSize*2 + 5 {
		require.NoError(t, storage.EnqueueMessage(ctx, queueName, &messagepb.Message{
			MessageId: fmt.Sprintf("errored-%03d", index),
			Metadata:  &messagepb.Message_Metadata{State: messagepb.Message_Metadata_ERRORED},
		}))
	}

	deleted, err := storage.PurgeDLQ(ctx, queueName)
	require.NoError(t, err)
	require.EqualValues(t, dlqPurgeBatchSize*2+5, deleted)
	counts, err := storage.StateManager.GetStateCounts(ctx, storage.DB, queueName)
	require.NoError(t, err)
	require.Zero(t, counts["errored"])
	var remaining int
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_messages WHERE queue_name = ?`, queueName).Scan(&remaining))
	require.Zero(t, remaining)
}

func TestPurgeDLQCanceledContextLeavesRowsForResume(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "canceled-purge.db"))
	queueName := "canceled-dlq"
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
	require.NoError(t, storage.EnqueueMessage(ctx, queueName, &messagepb.Message{MessageId: "errored", Metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_ERRORED}}))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	deleted, err := storage.PurgeDLQ(canceled, queueName)
	require.ErrorContains(t, err, "purge DLQ canceled")
	require.Zero(t, deleted)
	var remaining int
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_messages WHERE queue_name = ?`, queueName).Scan(&remaining))
	require.Equal(t, 1, remaining)
}

func TestPeekMessagesPageUsesStablePriorityKeyset(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "peek-keyset.db"))
	queueName := "peek-keyset"
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
	for _, message := range []*messagepb.Message{
		{MessageId: "high-a", Metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_PENDING, Priority: 3}},
		{MessageId: "high-b", Metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_PENDING, Priority: 3}},
		{MessageId: "normal", Metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_PENDING, Priority: 2}},
	} {
		require.NoError(t, storage.EnqueueMessage(ctx, queueName, message))
	}
	first, cursor, err := storage.PeekMessagesPage(ctx, queueName, 2, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, cursor)
	require.Equal(t, []string{"high-a", "high-b"}, []string{first[0].GetMessageId(), first[1].GetMessageId()})
	require.NoError(t, storage.CancelMessage(ctx, queueName, "high-a", "claimed before next page"))
	require.NoError(t, storage.EnqueueMessage(ctx, queueName, &messagepb.Message{MessageId: "urgent", Metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_PENDING, Priority: 4}}))
	require.NoError(t, storage.EnqueueMessage(ctx, queueName, &messagepb.Message{MessageId: "high-aa", Metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_PENDING, Priority: 3}}))
	require.NoError(t, storage.EnqueueMessage(ctx, queueName, &messagepb.Message{MessageId: "high-c", Metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_PENDING, Priority: 3}}))

	second, nextCursor, err := storage.PeekMessagesPage(ctx, queueName, 10, cursor, nil)
	require.NoError(t, err)
	require.Nil(t, nextCursor)
	require.Equal(t, []string{"high-aa", "high-c", "normal"}, []string{second[0].GetMessageId(), second[1].GetMessageId(), second[2].GetMessageId()})
}
