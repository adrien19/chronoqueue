package validator

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
)

func TestValidateQueue(t *testing.T) {
	validLease := &commonpb.LeasePolicy{BaseLease: durationpb.New(30 * time.Second)}
	tests := []struct {
		name      string
		queueName string
		metadata  *queuepb.QueueMetadata
		wantError string
	}{
		{name: "defaults", queueName: "orders", metadata: &queuepb.QueueMetadata{LeasePolicy: validLease}},
		{name: "full configuration", queueName: "Orders_eu.v2", metadata: &queuepb.QueueMetadata{
			Type:               queuepb.QueueType_EXCLUSIVE,
			DefaultMaxAttempts: -1,
			ExclusivityKey:     "worker-key",
			SchemaId:           "order-schema",
			SchemaRequired:     true,
			MaxPayloadSize:     1024,
			AllowedContentTypes: []string{
				"application/json; charset=utf-8",
			},
			LeasePolicy: validLease,
			PriorityConfig: &queuepb.PriorityConfig{
				Policy:             queuepb.FairnessPolicy_HYBRID,
				PriorityWeights:    map[int32]int32{0: 10, 2: 20, 4: 70},
				AgeBoostThreshold:  durationpb.New(time.Minute),
				AgeBoostMultiplier: 2,
			},
			MessageRetentionPolicy: &queuepb.MessageRetentionPolicy{Mode: queuepb.MessageRetentionPolicy_RETAIN_DURATION, RetentionSeconds: 60},
		}},
		{name: "missing name", metadata: &queuepb.QueueMetadata{}, wantError: "queue name is required"},
		{name: "invalid name", queueName: "orders/eu", metadata: &queuepb.QueueMetadata{}, wantError: "queue name must start"},
		{name: "long name", queueName: strings.Repeat("a", MaxQueueNameLength+1), metadata: &queuepb.QueueMetadata{}, wantError: "must not exceed"},
		{name: "invalid type", queueName: "orders", metadata: &queuepb.QueueMetadata{Type: 99}, wantError: "queue type is invalid"},
		{name: "invalid attempts", queueName: "orders", metadata: &queuepb.QueueMetadata{DefaultMaxAttempts: -2}, wantError: "default_max_attempts"},
		{name: "invalid payload size", queueName: "orders", metadata: &queuepb.QueueMetadata{MaxPayloadSize: -1}, wantError: "max_payload_size"},
		{name: "required schema missing", queueName: "orders", metadata: &queuepb.QueueMetadata{SchemaRequired: true}, wantError: "schema_id is required"},
		{name: "conflicting leases", queueName: "orders", metadata: &queuepb.QueueMetadata{LeaseDuration: durationpb.New(time.Minute), LeasePolicy: validLease}, wantError: "must match"},
		{name: "invalid retention", queueName: "orders", metadata: &queuepb.QueueMetadata{MessageRetentionPolicy: &queuepb.MessageRetentionPolicy{Mode: queuepb.MessageRetentionPolicy_RETAIN_DURATION}}, wantError: "retention_seconds"},
		{name: "invalid priority key", queueName: "orders", metadata: &queuepb.QueueMetadata{PriorityConfig: &queuepb.PriorityConfig{Policy: queuepb.FairnessPolicy_WEIGHTED, PriorityWeights: map[int32]int32{1: 1}}}, wantError: "keys must be"},
		{name: "invalid priority weight", queueName: "orders", metadata: &queuepb.QueueMetadata{PriorityConfig: &queuepb.PriorityConfig{Policy: queuepb.FairnessPolicy_WEIGHTED, PriorityWeights: map[int32]int32{4: 0}}}, wantError: "values must be"},
		{name: "strict rejects weights", queueName: "orders", metadata: &queuepb.QueueMetadata{PriorityConfig: &queuepb.PriorityConfig{PriorityWeights: map[int32]int32{4: 1}}}, wantError: "STRICT"},
		{name: "weighted rejects aging", queueName: "orders", metadata: &queuepb.QueueMetadata{PriorityConfig: &queuepb.PriorityConfig{Policy: queuepb.FairnessPolicy_WEIGHTED, AgeBoostThreshold: durationpb.New(time.Minute)}}, wantError: "WEIGHTED"},
		{name: "aging rejects weights", queueName: "orders", metadata: &queuepb.QueueMetadata{PriorityConfig: &queuepb.PriorityConfig{Policy: queuepb.FairnessPolicy_AGING, PriorityWeights: map[int32]int32{4: 1}}}, wantError: "AGING"},
		{name: "invalid content type", queueName: "orders", metadata: &queuepb.QueueMetadata{AllowedContentTypes: []string{"not a mime"}}, wantError: "unsupported content type"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateQueue(tt.queueName, tt.metadata)
			if tt.wantError == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantError)
		})
	}
}
