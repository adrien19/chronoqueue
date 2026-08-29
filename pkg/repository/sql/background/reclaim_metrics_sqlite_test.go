//go:build sqlite && cgo

package background

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	"github.com/adrien19/chronoqueue/pkg/metrics"
)

func TestReclaimMetricsReflectPersistedExpiryCause(t *testing.T) {
	tests := []struct {
		name                 string
		leaseExpired         bool
		transitionsToErrored bool
		dlqTarget            string
		wantLeaseDelta       float64
		wantHeartbeatDelta   float64
		reclamationFails     bool
	}{
		{name: "lease expiry", leaseExpired: true, wantLeaseDelta: 1},
		{name: "heartbeat expiry", wantHeartbeatDelta: 1},
		{name: "lease expiry to errored", leaseExpired: true, transitionsToErrored: true, wantLeaseDelta: 1},
		{name: "heartbeat expiry to configured dlq", transitionsToErrored: true, dlqTarget: "custom-dead-letters", wantHeartbeatDelta: 1},
		{name: "reclamation failure", leaseExpired: true, reclamationFails: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			storage := newTestStorage(t)
			t.Cleanup(func() { require.NoError(t, storage.Close()) })
			queueName := "reclaim-" + strings.ReplaceAll(tt.name, " ", "-")
			if tt.dlqTarget != "" {
				require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: tt.dlqTarget, Metadata: &queuepb.QueueMetadata{}}))
			}
			require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: tt.dlqTarget}}))
			attemptsLeft := int32(2)
			if tt.transitionsToErrored {
				attemptsLeft = 1
			}
			require.NoError(t, storage.EnqueueMessage(ctx, queueName, &messagepb.Message{
				MessageId: "message",
				Metadata: &messagepb.Message_Metadata{
					State:        messagepb.Message_Metadata_PENDING,
					AttemptsLeft: attemptsLeft,
					MaxAttempts:  2,
					LeasePolicy: &commonpb.LeasePolicy{
						BaseLease:        durationpb.New(time.Hour),
						HeartbeatTimeout: durationpb.New(time.Hour),
					},
				},
			}))
			claimed, err := storage.ClaimMessage(ctx, queueName, "worker", "attempt", "")
			require.NoError(t, err)
			require.NotNil(t, claimed)

			nowMs := storage.Clock.NowMs()
			leaseExpiry := nowMs + time.Hour.Milliseconds()
			heartbeatExpiry := nowMs - 1
			if tt.leaseExpired {
				leaseExpiry = nowMs - 1
				heartbeatExpiry = 0
			}
			_, err = storage.DB.ExecContext(ctx,
				"UPDATE cq_messages SET lease_expiry = ?, heartbeat_expiry = ? WHERE queue_name = ? AND message_id = ?",
				leaseExpiry, heartbeatExpiry, queueName, claimed.GetMessageId())
			require.NoError(t, err)

			registry := metrics.NewMetricsRegistry()
			leaseMetric := fmt.Sprintf(`chronoqueue_lease_expirations_total{expiry_type="lease",queue_name="%s"}`, queueName)
			heartbeatMetric := fmt.Sprintf(`chronoqueue_heartbeat_timeouts_total{queue_name="%s"}`, queueName)
			leaseBefore := metricValue(t, registry, leaseMetric)
			heartbeatBefore := metricValue(t, registry, heartbeatMetric)
			dlqReason := "heartbeat_timeout"
			if tt.leaseExpired {
				dlqReason = "lease_timeout"
			}
			metricDLQTarget := tt.dlqTarget
			if metricDLQTarget == "" {
				metricDLQTarget = queueName + "-dlq"
			}
			dlqMetric := fmt.Sprintf(`chronoqueue_dlq_ingestion_total{dlq_name="%s",reason="%s",source_queue="%s"}`, metricDLQTarget, dlqReason, queueName)
			dlqBefore := metricValue(t, registry, dlqMetric)
			transitionState := "PENDING"
			if tt.transitionsToErrored {
				transitionState = "ERRORED"
			}
			transitionMetric := fmt.Sprintf(`chronoqueue_message_state_transitions_total{from_state="RUNNING",queue_name="%s",to_state="%s"}`, queueName, transitionState)
			processedMetric := fmt.Sprintf(`chronoqueue_background_service_processed_messages_total{queue_name="%s",service="reclaim"}`, queueName)
			transitionBefore := metricValue(t, registry, transitionMetric)
			processedBefore := metricValue(t, registry, processedMetric)
			if tt.reclamationFails {
				_, err = storage.DB.ExecContext(ctx, `CREATE TRIGGER fail_reclamation BEFORE UPDATE ON cq_messages BEGIN SELECT RAISE(FAIL, 'forced reclamation failure'); END`)
				require.NoError(t, err)
			}
			service := NewReclaimService(storage, storage.BaseSQL, time.Second)

			err = service.reclaimQueueMessages(ctx, queueName)
			if tt.reclamationFails {
				require.ErrorContains(t, err, "forced reclamation failure")
				require.Equal(t, leaseBefore, metricValue(t, registry, leaseMetric))
				require.Equal(t, heartbeatBefore, metricValue(t, registry, heartbeatMetric))
				require.Equal(t, dlqBefore, metricValue(t, registry, dlqMetric))
				require.Equal(t, transitionBefore, metricValue(t, registry, transitionMetric))
				require.Equal(t, processedBefore, metricValue(t, registry, processedMetric))
				return
			}
			require.NoError(t, err)

			require.Equal(t, leaseBefore+tt.wantLeaseDelta, metricValue(t, registry, leaseMetric))
			require.Equal(t, heartbeatBefore+tt.wantHeartbeatDelta, metricValue(t, registry, heartbeatMetric))
			wantDLQDelta := float64(0)
			wantState := messagepb.Message_Metadata_PENDING
			stateQueue := queueName
			if tt.transitionsToErrored {
				wantState = messagepb.Message_Metadata_ERRORED
				if tt.dlqTarget != "" {
					wantDLQDelta = 1
					stateQueue = tt.dlqTarget
				}
			}
			require.Equal(t, dlqBefore+wantDLQDelta, metricValue(t, registry, dlqMetric))
			messages, err := storage.PeekMessages(ctx, stateQueue, 1)
			require.NoError(t, err)
			require.Equal(t, wantState, messages[0].GetMetadata().GetState())
		})
	}
}
