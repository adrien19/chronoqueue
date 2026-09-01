//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
	"github.com/adrien19/chronoqueue/pkg/log"
	"github.com/adrien19/chronoqueue/pkg/repository/sql/background"
)

func TestReclaimExpiredMessage_AtomicAcrossPostgresInstances(t *testing.T) {
	ctx := context.Background()
	container, err := postgrescontainer.Run(
		ctx,
		"postgres:17-alpine",
		postgrescontainer.WithDatabase("chronoqueue"),
		postgrescontainer.WithUsername("chronoqueue"),
		postgrescontainer.WithPassword("chronoqueue"),
		postgrescontainer.BasicWaitStrategies(),
		testcontainers.WithTmpfs(map[string]string{"/var/lib/postgresql/data": "rw"}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(ctx)) })
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	singleConnectionCtx, cancelSingleConnection := context.WithTimeout(ctx, 5*time.Second)
	singleConnectionStorage, err := NewStorage(singleConnectionCtx, &Config{
		Conn:   ConnectionConfig{DSN: dsn, MaxOpenConns: 1, MaxIdleConns: 1},
		Logger: log.NewLogger(),
	})
	cancelSingleConnection()
	require.NoError(t, err)
	require.NoError(t, singleConnectionStorage.Close())

	first := newPostgresReclaimTestStorage(t, ctx, dsn)
	second := newPostgresReclaimTestStorage(t, ctx, dsn)
	t.Run("preserves infinite retries after nack", func(t *testing.T) {
		require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: "nack-infinite", Metadata: &queuepb.QueueMetadata{}}))
		require.NoError(t, first.EnqueueMessage(ctx, "nack-infinite", postgresReclaimTestMessage("infinite", -1, -1)))
		for _, attempt := range []string{"attempt-1", "attempt-2"} {
			claimed, claimErr := first.ClaimMessage(ctx, "nack-infinite", "worker", attempt, "")
			require.NoError(t, claimErr)
			require.NotNil(t, claimed)
			require.NoError(t, first.NackMessage(ctx, "nack-infinite", "infinite", attempt, "worker"))
		}
		messages, listErr := first.PeekMessages(ctx, "nack-infinite", 1)
		require.NoError(t, listErr)
		require.Len(t, messages, 1)
		require.EqualValues(t, -1, messages[0].GetMetadata().GetAttemptsLeft())
	})

	t.Run("exhausts last finite attempt after nack", func(t *testing.T) {
		require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: "nack-finite", Metadata: &queuepb.QueueMetadata{
			MessageRetentionPolicy: &queuepb.MessageRetentionPolicy{Mode: queuepb.MessageRetentionPolicy_RETAIN_FOREVER},
		}}))
		const messageID = "nack-finite-message"
		require.NoError(t, first.EnqueueMessage(ctx, "nack-finite", postgresReclaimTestMessage(messageID, 1, 1)))
		claimed, claimErr := first.ClaimMessage(ctx, "nack-finite", "worker", "attempt", "")
		require.NoError(t, claimErr)
		require.NotNil(t, claimed)
		require.NoError(t, first.NackMessage(ctx, "nack-finite", messageID, "attempt", "worker"))

		var state messagepb.Message_Metadata_State
		var attemptsLeft int32
		require.NoError(t, first.DB.QueryRowContext(ctx, `SELECT state, attempts_left FROM cq_messages WHERE queue_name = $1 AND message_id = $2`, "nack-finite", messageID).Scan(&state, &attemptsLeft))
		require.Equal(t, messagepb.Message_Metadata_ERRORED, state)
		require.Zero(t, attemptsLeft)
	})

	t.Run("uses persisted attempts for reclaim retention timestamps", func(t *testing.T) {
		const queueName = "reclaim-persisted-attempts"
		const messageID = "reclaim-retryable-message"
		require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{
			MessageRetentionPolicy: &queuepb.MessageRetentionPolicy{Mode: queuepb.MessageRetentionPolicy_RETAIN_DURATION, RetentionSeconds: 60},
		}}))
		require.NoError(t, first.EnqueueMessage(ctx, queueName, postgresReclaimTestMessage(messageID, 2, 2)))
		claimed, claimErr := first.ClaimMessage(ctx, queueName, "worker", "attempt", "")
		require.NoError(t, claimErr)
		expirePostgresReclaimTestMessage(t, ctx, first, messageID)

		sparse := &messagepb.Message{MessageId: messageID, Metadata: &messagepb.Message_Metadata{
			CurrentAttempt: claimed.GetMetadata().GetCurrentAttempt(),
		}}
		result, reclaimErr := first.ReclaimExpiredMessage(ctx, queueName, sparse)
		require.NoError(t, reclaimErr)
		require.Equal(t, messagepb.Message_Metadata_PENDING, result.State)

		var state messagepb.Message_Metadata_State
		var attemptsLeft int32
		var completedAt, deletedAt sql.NullInt64
		require.NoError(t, first.DB.QueryRowContext(ctx, `SELECT state, attempts_left, completed_at, deleted_at FROM cq_messages WHERE queue_name = $1 AND message_id = $2`, queueName, messageID).Scan(&state, &attemptsLeft, &completedAt, &deletedAt))
		require.Equal(t, messagepb.Message_Metadata_PENDING, state)
		require.EqualValues(t, 1, attemptsLeft)
		require.False(t, completedAt.Valid)
		require.False(t, deletedAt.Valid)
	})

	t.Run("bounds heartbeat extensions", func(t *testing.T) {
		require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: "heartbeat-policy", Metadata: &queuepb.QueueMetadata{}}))
		maxRenewals := int32(1)
		message := postgresReclaimTestMessage("heartbeat", 1, 1)
		message.Metadata.LeasePolicy = &commonpb.LeasePolicy{
			BaseLease: durationpb.New(time.Minute), MaxExtension: durationpb.New(2 * time.Second),
			ExtendStep: durationpb.New(time.Second), MaxRenewals: &maxRenewals,
		}
		require.NoError(t, first.EnqueueMessage(ctx, "heartbeat-policy", message))
		_, err = first.ClaimMessage(ctx, "heartbeat-policy", "worker", "attempt", "")
		require.NoError(t, err)
		_, _, err = first.HeartbeatMessage(ctx, "heartbeat-policy", "heartbeat", "attempt", "worker")
		require.NoError(t, err)
		_, _, err = first.HeartbeatMessage(ctx, "heartbeat-policy", "heartbeat", "attempt", "worker")
		require.NoError(t, err)
		var heartbeatExpiry sql.NullInt64
		var extensionUsed int64
		var renewalCount int32
		require.NoError(t, first.DB.QueryRowContext(ctx, `SELECT heartbeat_expiry, lease_extension_used, lease_renewal_count FROM cq_messages WHERE message_id = $1`, "heartbeat").Scan(&heartbeatExpiry, &extensionUsed, &renewalCount))
		require.False(t, heartbeatExpiry.Valid)
		require.EqualValues(t, time.Second.Milliseconds(), extensionUsed)
		require.EqualValues(t, 1, renewalCount)
	})

	t.Run("zero effective renewal does not consume renewal", func(t *testing.T) {
		const queueName = "renew-zero"
		require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
		message := postgresReclaimTestMessage("renew-zero-message", 1, 1)
		message.Metadata.LeasePolicy = &commonpb.LeasePolicy{BaseLease: durationpb.New(time.Minute), MaxExtension: durationpb.New(5 * time.Second)}
		require.NoError(t, first.EnqueueMessage(ctx, queueName, message))
		_, claimErr := first.ClaimMessage(ctx, queueName, "worker", "attempt", "")
		require.NoError(t, claimErr)
		_, renewErr := first.ExtendMessageLease(ctx, queueName, message.GetMessageId(), "attempt", "worker", 0)
		require.ErrorContains(t, renewErr, "lease cannot be extended")
		var extensionUsed int64
		var renewalCount int32
		require.NoError(t, first.DB.QueryRowContext(ctx, `SELECT lease_extension_used, lease_renewal_count FROM cq_messages WHERE message_id = $1`, message.GetMessageId()).Scan(&extensionUsed, &renewalCount))
		require.Zero(t, extensionUsed)
		require.Zero(t, renewalCount)
	})

	t.Run("peek uses stable priority keyset", func(t *testing.T) {
		const queueName = "peek-keyset"
		require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
		for _, message := range []*messagepb.Message{
			{MessageId: "high-a", Metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_PENDING, Priority: 3}},
			{MessageId: "high-b", Metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_PENDING, Priority: 3}},
			{MessageId: "normal", Metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_PENDING, Priority: 2}},
		} {
			require.NoError(t, first.EnqueueMessage(ctx, queueName, message))
		}
		page, cursor, pageErr := first.PeekMessagesPage(ctx, queueName, 2, nil, nil)
		require.NoError(t, pageErr)
		require.NotNil(t, cursor)
		require.Equal(t, []string{"high-a", "high-b"}, []string{page[0].GetMessageId(), page[1].GetMessageId()})
		require.NoError(t, first.CancelMessage(ctx, queueName, "high-a", "removed before page two"))
		require.NoError(t, first.EnqueueMessage(ctx, queueName, &messagepb.Message{MessageId: "urgent", Metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_PENDING, Priority: 4}}))
		require.NoError(t, first.EnqueueMessage(ctx, queueName, &messagepb.Message{MessageId: "high-aa", Metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_PENDING, Priority: 3}}))
		require.NoError(t, first.EnqueueMessage(ctx, queueName, &messagepb.Message{MessageId: "high-c", Metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_PENDING, Priority: 3}}))
		page, cursor, pageErr = first.PeekMessagesPage(ctx, queueName, 10, cursor, nil)
		require.NoError(t, pageErr)
		require.Nil(t, cursor)
		require.Equal(t, []string{"high-aa", "high-c", "normal"}, []string{page[0].GetMessageId(), page[1].GetMessageId(), page[2].GetMessageId()})
	})

	t.Run("purges DLQ across bounded batches", func(t *testing.T) {
		const queueName = "batched-purge"
		require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
		for index := range dlqPurgeBatchSize*2 + 5 {
			require.NoError(t, first.EnqueueMessage(ctx, queueName, &messagepb.Message{
				MessageId: fmt.Sprintf("errored-%03d", index), Metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_ERRORED},
			}))
		}
		deleted, purgeErr := first.purgeDLQ(ctx, queueName, dlqPurgeBatchSize, 1)
		require.ErrorContains(t, purgeErr, "purge DLQ incomplete")
		require.EqualValues(t, dlqPurgeBatchSize, deleted)
		deleted, purgeErr = first.PurgeDLQ(ctx, queueName)
		require.NoError(t, purgeErr)
		require.EqualValues(t, dlqPurgeBatchSize+5, deleted)
		counts, countErr := first.StateManager.GetStateCounts(ctx, first.DB, queueName)
		require.NoError(t, countErr)
		require.Zero(t, counts["errored"])
	})

	t.Run("reports incomplete purge when an errored message is locked", func(t *testing.T) {
		const queueName = "locked-dlq-purge"
		require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
		for index := range 2 {
			require.NoError(t, first.EnqueueMessage(ctx, queueName, &messagepb.Message{
				MessageId: fmt.Sprintf("locked-errored-%d", index), Metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_ERRORED},
			}))
		}

		lockTx, txErr := first.DB.BeginTx(ctx, nil)
		require.NoError(t, txErr)
		lockReleased := false
		t.Cleanup(func() {
			if lockReleased {
				return
			}
			rollbackErr := lockTx.Rollback()
			if rollbackErr != nil && rollbackErr != sql.ErrTxDone {
				t.Errorf("rollback DLQ row lock transaction: %v", rollbackErr)
			}
		})
		var lockedMessageID string
		require.NoError(t, lockTx.QueryRowContext(ctx, `
			SELECT message_id FROM cq_messages
			WHERE queue_name = $1 AND state = $2
			ORDER BY id
			LIMIT 1
			FOR UPDATE
		`, queueName, messagepb.Message_Metadata_ERRORED).Scan(&lockedMessageID))

		deleted, purgeErr := second.PurgeDLQ(ctx, queueName)
		require.ErrorContains(t, purgeErr, "purge DLQ incomplete")
		require.EqualValues(t, 1, deleted)
		var remaining int
		require.NoError(t, first.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_messages WHERE queue_name = $1 AND state = $2`, queueName, messagepb.Message_Metadata_ERRORED).Scan(&remaining))
		require.Equal(t, 1, remaining)

		require.NoError(t, lockTx.Commit())
		lockReleased = true
		deleted, purgeErr = second.PurgeDLQ(ctx, queueName)
		require.NoError(t, purgeErr)
		require.EqualValues(t, 1, deleted)
	})

	t.Run("preserves state counters above int32", func(t *testing.T) {
		const queueName = "int64-state-counter"
		const initial = int64(math.MaxInt32) + 10
		require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
		_, err := first.DB.ExecContext(ctx, `UPDATE cq_queues SET state_counts = jsonb_build_object('pending', $1::bigint) WHERE name = $2`, initial, queueName)
		require.NoError(t, err)
		require.NoError(t, first.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
			return first.StateManager.InsertCounter(ctx, tx, queueName, messagepb.Message_Metadata_PENDING)
		}))
		counts, err := first.StateManager.GetStateCounts(ctx, first.DB, queueName)
		require.NoError(t, err)
		require.Equal(t, initial+1, counts["pending"])
		require.NoError(t, first.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
			return first.StateManager.RemoveCounters(ctx, tx, queueName, messagepb.Message_Metadata_PENDING, 2)
		}))
		counts, err = first.StateManager.GetStateCounts(ctx, first.DB, queueName)
		require.NoError(t, err)
		require.Equal(t, initial-1, counts["pending"])
	})

	t.Run("archives schedules when deleting queue", func(t *testing.T) {
		require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: "queue-history", Metadata: &queuepb.QueueMetadata{}}))
		schedule := &schedulepb.Schedule{ScheduleId: "queue-history-schedule", Metadata: &schedulepb.Schedule_Metadata{
			State: schedulepb.Schedule_Metadata_SCHEDULED, QueueName: "queue-history",
			ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{CronSchedule: "*/5 * * * *"},
		}}
		require.NoError(t, first.CreateSchedule(ctx, schedule))
		require.NoError(t, first.DeleteQueue(ctx, "queue-history"))
		history, historyErr := first.GetScheduleHistory(ctx, schedule.ScheduleId, 10)
		require.NoError(t, historyErr)
		require.Equal(t, schedule.ScheduleId, history.GetScheduleId())
	})

	t.Run("cancel removes the locked message state counter", func(t *testing.T) {
		const queueName = "cancel-counter-lock"
		const messageID = "cancel-counter-message"
		require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
		require.NoError(t, first.EnqueueMessage(ctx, queueName, postgresReclaimTestMessage(messageID, 1, 1)))

		tx, txErr := first.DB.BeginTx(ctx, nil)
		require.NoError(t, txErr)
		t.Cleanup(func() { _ = tx.Rollback() })
		var state messagepb.Message_Metadata_State
		require.NoError(t, tx.QueryRowContext(ctx, `SELECT state FROM cq_messages WHERE queue_name = $1 AND message_id = $2 FOR UPDATE`, queueName, messageID).Scan(&state))
		require.Equal(t, messagepb.Message_Metadata_PENDING, state)
		cancelErr := make(chan error, 1)
		go func() { cancelErr <- second.CancelMessage(ctx, queueName, messageID, "cancel") }()
		require.Eventually(t, func() bool {
			var blocked bool
			err := first.DB.QueryRowContext(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM pg_stat_activity
					WHERE state = 'active'
					  AND wait_event_type = 'Lock'
					  AND query LIKE '%SELECT state FROM cq_messages WHERE message_id%FOR UPDATE%'
				)`).Scan(&blocked)
			return err == nil && blocked
		}, 5*time.Second, 10*time.Millisecond, "cancellation query did not block on the message row")
		_, txErr = tx.ExecContext(ctx, `UPDATE cq_messages SET state = $1 WHERE queue_name = $2 AND message_id = $3`, messagepb.Message_Metadata_INVISIBLE, queueName, messageID)
		require.NoError(t, txErr)
		_, txErr = tx.ExecContext(ctx, `UPDATE cq_queues SET state_counts = '{"pending":0,"invisible":1}' WHERE name = $1`, queueName)
		require.NoError(t, txErr)
		require.NoError(t, tx.Commit())
		require.NoError(t, <-cancelErr)

		counts, countErr := first.StateManager.GetStateCounts(ctx, first.DB, queueName)
		require.NoError(t, countErr)
		require.Zero(t, counts["pending"])
		require.Zero(t, counts["invisible"])
		var remaining int
		require.NoError(t, first.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_messages WHERE queue_name = $1 AND message_id = $2`, queueName, messageID).Scan(&remaining))
		require.Zero(t, remaining)
	})
	t.Run("rejects deletion of referenced DLQ", func(t *testing.T) {
		require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: "protected-dlq", Metadata: &queuepb.QueueMetadata{}}))
		require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: "protected-source", Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "protected-dlq"}}))
		require.ErrorContains(t, first.DeleteQueue(ctx, "protected-dlq"), "referenced as a dead letter queue")
		require.NoError(t, first.DeleteQueue(ctx, "protected-source"))
		require.NoError(t, first.DeleteQueue(ctx, "protected-dlq"))
	})

	t.Run("serializes dependent creation with DLQ deletion", func(t *testing.T) {
		require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: "concurrent-dlq", Metadata: &queuepb.QueueMetadata{}}))
		deleteTx, err := first.DB.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = deleteTx.ExecContext(ctx, lockQueuesForRelationshipMutation)
		require.NoError(t, err)
		_, err = deleteTx.ExecContext(ctx, `DELETE FROM cq_queues WHERE name = $1`, "concurrent-dlq")
		require.NoError(t, err)
		createErr := make(chan error, 1)
		go func() {
			createErr <- second.CreateQueue(ctx, &queuepb.Queue{Name: "concurrent-source", Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "concurrent-dlq"}})
		}()
		require.NoError(t, deleteTx.Commit())
		require.ErrorContains(t, <-createErr, `dead letter queue "concurrent-dlq" not found`)
		_, err = first.GetQueue(ctx, "concurrent-source")
		require.ErrorContains(t, err, `queue "concurrent-source" not found`)
	})

	t.Run("creates source and automatic DLQ atomically", func(t *testing.T) {
		require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: "atomic-collision_dlq", Metadata: &queuepb.QueueMetadata{}}))
		err := first.CreateQueueWithDLQ(ctx,
			&queuepb.Queue{Name: "atomic-collision", Metadata: &queuepb.QueueMetadata{AutoCreateDlq: true, DeadLetterQueueName: "atomic-collision_dlq"}},
			&queuepb.Queue{Name: "atomic-collision_dlq", Metadata: &queuepb.QueueMetadata{}},
		)
		require.Error(t, err)
		_, err = first.GetQueue(ctx, "atomic-collision")
		require.ErrorContains(t, err, `queue "atomic-collision" not found`)
	})

	t.Run("rejects mismatched automatic DLQ without inserting", func(t *testing.T) {
		err := first.CreateQueueWithDLQ(ctx,
			&queuepb.Queue{Name: "mismatched-source", Metadata: &queuepb.QueueMetadata{AutoCreateDlq: true, DeadLetterQueueName: "missing-dlq"}},
			&queuepb.Queue{Name: "created-dlq", Metadata: &queuepb.QueueMetadata{}},
		)
		require.ErrorContains(t, err, `dead letter target "missing-dlq" does not match queue "created-dlq"`)
		_, err = first.GetQueue(ctx, "mismatched-source")
		require.ErrorContains(t, err, `queue "mismatched-source" not found`)
		_, err = first.GetQueue(ctx, "created-dlq")
		require.ErrorContains(t, err, `queue "created-dlq" not found`)
	})

	t.Run("promotes a scheduled message once across schedulers", func(t *testing.T) {
		require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: "scheduled-cas", Metadata: &queuepb.QueueMetadata{}}))
		dueMessage := postgresReclaimTestMessage("scheduled-once", 1, 1)
		dueMessage.Metadata.State = messagepb.Message_Metadata_INVISIBLE
		dueMessage.Metadata.ScheduledTime = timestamppb.New(time.Now().Add(-time.Minute))
		require.NoError(t, first.EnqueueMessage(ctx, "scheduled-cas", dueMessage))
		_, err := first.DB.ExecContext(ctx, `UPDATE cq_queues SET state_counts = '{"invisible":1}' WHERE name = $1`, "scheduled-cas")
		require.NoError(t, err)
		schedulers := []*background.SchedulerService{background.NewSchedulerService(first.BaseSQL, time.Second), background.NewSchedulerService(second.BaseSQL, time.Second)}
		startSchedulers := make(chan struct{})
		schedulerErrs := make(chan error, len(schedulers))
		var schedulerWG sync.WaitGroup
		for _, scheduler := range schedulers {
			schedulerWG.Add(1)
			go func(scheduler *background.SchedulerService) {
				defer schedulerWG.Done()
				<-startSchedulers
				schedulerErrs <- scheduler.RunOnce(ctx)
			}(scheduler)
		}
		close(startSchedulers)
		schedulerWG.Wait()
		close(schedulerErrs)
		for schedulerErr := range schedulerErrs {
			require.NoError(t, schedulerErr)
		}
		var invisibleCount, pendingCount int64
		err = first.DB.QueryRowContext(ctx, `SELECT COALESCE((state_counts->>'invisible')::BIGINT, 0), COALESCE((state_counts->>'pending')::BIGINT, 0) FROM cq_queues WHERE name = $1`, "scheduled-cas").Scan(&invisibleCount, &pendingCount)
		require.NoError(t, err)
		require.Zero(t, invisibleCount)
		require.EqualValues(t, 1, pendingCount)
	})

	queueName := "reclaim-fencing"
	require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{
		MessageRetentionPolicy: &queuepb.MessageRetentionPolicy{Mode: queuepb.MessageRetentionPolicy_RETAIN_FOREVER},
	}}))
	require.NoError(t, first.EnqueueMessage(ctx, queueName, postgresReclaimTestMessage("finite", 2, 2)))

	claimed, err := first.ClaimMessage(ctx, queueName, "worker-1", "attempt-1", "")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.EqualValues(t, 2, claimed.GetMetadata().GetAttemptsLeft())
	expirePostgresReclaimTestMessage(t, ctx, first, claimed.GetMessageId())
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
	expirePostgresReclaimTestMessage(t, ctx, first, claimed.GetMessageId())
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

	require.NoError(t, first.EnqueueMessage(ctx, queueName, postgresReclaimTestMessage("legacy-null-attempt", 2, 2)))
	claimed, err = first.ClaimMessage(ctx, queueName, "legacy-worker", "legacy-attempt", "")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	_, err = first.DB.ExecContext(ctx, `UPDATE cq_messages SET lease_expiry = 0, current_attempt_id = NULL WHERE message_id = $1`, claimed.GetMessageId())
	require.NoError(t, err)

	legacyMessage := &messagepb.Message{MessageId: claimed.GetMessageId()}
	_, err = first.ReclaimExpiredMessage(ctx, queueName, legacyMessage)
	require.NoError(t, err)
	var state messagepb.Message_Metadata_State
	var attemptsLeft int32
	require.NoError(t, first.DB.QueryRowContext(ctx, `SELECT state, attempts_left FROM cq_messages WHERE message_id = $1`, claimed.GetMessageId()).Scan(&state, &attemptsLeft))
	assert.Equal(t, messagepb.Message_Metadata_PENDING, state)
	assert.EqualValues(t, 1, attemptsLeft)
}

func TestWorkerMutations_RequireActiveOwnershipAcrossPostgresInstances(t *testing.T) {
	ctx := context.Background()
	container, err := postgrescontainer.Run(
		ctx,
		"postgres:17-alpine",
		postgrescontainer.WithDatabase("chronoqueue"),
		postgrescontainer.WithUsername("chronoqueue"),
		postgrescontainer.WithPassword("chronoqueue"),
		postgrescontainer.BasicWaitStrategies(),
		testcontainers.WithTmpfs(map[string]string{"/var/lib/postgresql/data": "rw"}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(ctx)) })
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	first := newPostgresReclaimTestStorage(t, ctx, dsn)
	second := newPostgresReclaimTestStorage(t, ctx, dsn)
	require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: "owned", Metadata: &queuepb.QueueMetadata{}}))
	require.NoError(t, first.CreateQueue(ctx, &queuepb.Queue{Name: "other", Metadata: &queuepb.QueueMetadata{}}))

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
			require.NoError(t, first.EnqueueMessage(ctx, "owned", postgresReclaimTestMessage(name, 1, 1)))
			claimed, err := first.ClaimMessage(ctx, "owned", "worker", "attempt", "")
			require.NoError(t, err)
			require.NotNil(t, claimed)
			require.Error(t, mutate(ctx, first, "other", "attempt", "worker"))
			require.Error(t, mutate(ctx, first, "owned", "stale-attempt", "worker"))
			require.Error(t, mutate(ctx, first, "owned", "attempt", "stale-worker"))
			expirePostgresReclaimTestMessage(t, ctx, first, name)
			require.Error(t, mutate(ctx, first, "owned", "attempt", "worker"))

			var state messagepb.Message_Metadata_State
			require.NoError(t, first.DB.QueryRowContext(ctx, `SELECT state FROM cq_messages WHERE message_id = $1`, name).Scan(&state))
			require.Equal(t, messagepb.Message_Metadata_RUNNING, state)
		})
	}

	require.NoError(t, first.EnqueueMessage(ctx, "owned", postgresReclaimTestMessage("terminal", 1, 1)))
	claimed, err := first.ClaimMessage(ctx, "owned", "worker", "terminal-attempt", "")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, mutate := range []func() error{
		func() error { return first.AcknowledgeMessage(ctx, "owned", "terminal", "terminal-attempt", "worker") },
		func() error { return second.NackMessage(ctx, "owned", "terminal", "terminal-attempt", "worker") },
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

func newPostgresReclaimTestStorage(t *testing.T, ctx context.Context, dsn string) *Storage {
	t.Helper()
	logger := log.NewLogger()
	manager, err := keymanager.NewEncryptionKeyManagerWithConfig(logger, keymanager.Config{Enabled: false})
	require.NoError(t, err)
	storage, err := NewStorage(ctx, &Config{Conn: ConnectionConfig{DSN: dsn}, Logger: logger, KeyManager: manager})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	return storage
}

func postgresReclaimTestMessage(id string, attemptsLeft, maxAttempts int32) *messagepb.Message {
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

func expirePostgresReclaimTestMessage(t *testing.T, ctx context.Context, storage *Storage, messageID string) {
	t.Helper()
	result, err := storage.DB.ExecContext(ctx, `UPDATE cq_messages SET lease_expiry = 0 WHERE message_id = $1`, messageID)
	require.NoError(t, err)
	rows, err := result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, rows)
}
