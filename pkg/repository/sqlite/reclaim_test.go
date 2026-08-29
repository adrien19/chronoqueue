//go:build sqlite && cgo

package sqlite

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
	"github.com/adrien19/chronoqueue/pkg/log"
)

func TestReclaimExpiredMessage_UsesAuthoritativeAttemptsAndFencesAttempt(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "reclaim.db")
	first := newReclaimTestStorage(t, ctx, path)
	second := newReclaimTestStorage(t, ctx, path)

	queueName := "reclaim-fencing"
	require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
	require.NoError(t, first.EnqueueMessage(ctx, queueName, reclaimTestMessage("finite", 2, 2)))

	claimed, err := first.ClaimMessage(ctx, queueName, "worker-1", "attempt-1", "")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.EqualValues(t, 2, claimed.GetMetadata().GetAttemptsLeft())
	expireReclaimTestMessage(t, ctx, first, claimed.GetMessageId())

	expired, err := first.FindExpiredMessages(ctx, queueName, 10)
	require.NoError(t, err)
	require.Len(t, expired, 1)
	require.EqualValues(t, 2, expired[0].GetMetadata().GetAttemptsLeft())
	require.Equal(t, "attempt-1", expired[0].GetMetadata().GetCurrentAttempt().GetAttemptId())

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, storage := range []*Storage{first, second} {
		candidate := proto.Clone(expired[0]).(*messagepb.Message)
		wg.Add(1)
		go func(storage *Storage, message *messagepb.Message) {
			defer wg.Done()
			<-start
			_, reclaimErr := storage.ReclaimExpiredMessage(ctx, queueName, message)
			errs <- reclaimErr
		}(storage, candidate)
	}
	close(start)
	wg.Wait()
	close(errs)

	var successes int
	for reclaimErr := range errs {
		if reclaimErr == nil {
			successes++
			continue
		}
		assert.Contains(t, reclaimErr.Error(), "message is no longer expired")
	}
	require.Equal(t, 1, successes)

	claimed, err = first.ClaimMessage(ctx, queueName, "worker-2", "attempt-2", "")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.EqualValues(t, 1, claimed.GetMetadata().GetAttemptsLeft())
	expireReclaimTestMessage(t, ctx, first, claimed.GetMessageId())

	expired, err = first.FindExpiredMessages(ctx, queueName, 10)
	require.NoError(t, err)
	require.Len(t, expired, 1)
	_, err = first.ReclaimExpiredMessage(ctx, queueName, expired[0])
	require.NoError(t, err)

	peeked, err := first.GetDLQMessages(ctx, queueName, 10)
	require.NoError(t, err)
	require.Len(t, peeked, 1)
	assert.Equal(t, messagepb.Message_Metadata_ERRORED, peeked[0].GetMetadata().GetState())
	assert.EqualValues(t, 0, peeked[0].GetMetadata().GetAttemptsLeft())

	require.NoError(t, first.EnqueueMessage(ctx, queueName, reclaimTestMessage("legacy-null-attempt", 2, 2)))
	claimed, err = first.ClaimMessage(ctx, queueName, "legacy-worker", "legacy-attempt", "")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	_, err = first.DB.ExecContext(ctx, `UPDATE cq_messages SET lease_expiry = 0, current_attempt_id = NULL WHERE message_id = ?`, claimed.GetMessageId())
	require.NoError(t, err)

	legacyMessage := &messagepb.Message{MessageId: claimed.GetMessageId()}
	_, err = first.ReclaimExpiredMessage(ctx, queueName, legacyMessage)
	require.NoError(t, err)
	var state messagepb.Message_Metadata_State
	var attemptsLeft int32
	require.NoError(t, first.DB.QueryRowContext(ctx, `SELECT state, attempts_left FROM cq_messages WHERE message_id = ?`, claimed.GetMessageId()).Scan(&state, &attemptsLeft))
	assert.Equal(t, messagepb.Message_Metadata_PENDING, state)
	assert.EqualValues(t, 1, attemptsLeft)
}

func TestReclaimExpiredMessage_PreservesInfiniteRetries(t *testing.T) {
	ctx := context.Background()
	storage := newReclaimTestStorage(t, ctx, filepath.Join(t.TempDir(), "reclaim-infinite.db"))
	queueName := "reclaim-infinite"
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
	require.NoError(t, storage.EnqueueMessage(ctx, queueName, reclaimTestMessage("infinite", -1, -1)))

	claimed, err := storage.ClaimMessage(ctx, queueName, "worker", "attempt", "")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	expireReclaimTestMessage(t, ctx, storage, claimed.GetMessageId())

	expired, err := storage.FindExpiredMessages(ctx, queueName, 10)
	require.NoError(t, err)
	require.Len(t, expired, 1)
	_, err = storage.ReclaimExpiredMessage(ctx, queueName, expired[0])
	require.NoError(t, err)

	peeked, err := storage.PeekMessages(ctx, queueName, 10)
	require.NoError(t, err)
	require.Len(t, peeked, 1)
	assert.Equal(t, messagepb.Message_Metadata_PENDING, peeked[0].GetMetadata().GetState())
	assert.EqualValues(t, -1, peeked[0].GetMetadata().GetAttemptsLeft())
}

func newReclaimTestStorage(t *testing.T, ctx context.Context, path string) *Storage {
	t.Helper()
	logger := log.NewLogger()
	manager, err := keymanager.NewEncryptionKeyManagerWithConfig(logger, keymanager.Config{Enabled: false})
	require.NoError(t, err)
	storage, err := NewStorage(ctx, &Config{Path: path, Logger: logger, KeyManager: manager})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	return storage
}

func reclaimTestMessage(id string, attemptsLeft, maxAttempts int32) *messagepb.Message {
	return &messagepb.Message{
		MessageId: id,
		Metadata: &messagepb.Message_Metadata{
			State:        messagepb.Message_Metadata_PENDING,
			AttemptsLeft: attemptsLeft,
			MaxAttempts:  maxAttempts,
			LeasePolicy: &commonpb.LeasePolicy{
				BaseLease: durationpb.New(time.Hour),
			},
		},
	}
}

func expireReclaimTestMessage(t *testing.T, ctx context.Context, storage *Storage, messageID string) {
	t.Helper()
	result, err := storage.DB.ExecContext(ctx, `UPDATE cq_messages SET lease_expiry = 0 WHERE message_id = ?`, messageID)
	require.NoError(t, err)
	rows, err := result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, rows)
}
