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
	}{
		{name: "lease expiry", leaseExpired: true, wantLeaseDelta: 1},
		{name: "heartbeat expiry", wantHeartbeatDelta: 1},
		{name: "lease expiry to errored", leaseExpired: true, transitionsToErrored: true, wantLeaseDelta: 1},
		{name: "heartbeat expiry to configured dlq", transitionsToErrored: true, dlqTarget: "custom-dead-letters", wantHeartbeatDelta: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			storage := newTestStorage(t)
			t.Cleanup(func() { require.NoError(t, storage.Close()) })
			queueName := "reclaim-" + strings.ReplaceAll(tt.name, " ", "-")
			require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: tt.dlqTarget}}))
			if tt.dlqTarget != "" {
				require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: tt.dlqTarget, Metadata: &queuepb.QueueMetadata{}}))
			}
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
			service := NewReclaimService(storage, storage.BaseSQL, time.Second)

			require.NoError(t, service.reclaimQueueMessages(ctx, queueName))

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
