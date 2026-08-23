package validator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
)

func TestLeasePolicyValidator(t *testing.T) {
	queuePolicy := &commonpb.LeasePolicy{
		BaseLease:    durationpb.New(30 * time.Second),
		MaxExtension: durationpb.New(5 * time.Minute),
	}
	tests := []struct {
		name          string
		metadata      *messagepb.Message_Metadata
		wantValid     bool
		wantBaseLease time.Duration
	}{
		{name: "inherits queue policy", metadata: &messagepb.Message_Metadata{}, wantValid: true, wantBaseLease: 30 * time.Second},
		{name: "lease policy override", metadata: &messagepb.Message_Metadata{LeasePolicy: &commonpb.LeasePolicy{BaseLease: durationpb.New(time.Minute)}}, wantValid: true, wantBaseLease: time.Minute},
		{name: "legacy override", metadata: &messagepb.Message_Metadata{LeaseDuration: durationpb.New(45 * time.Second)}, wantValid: true, wantBaseLease: 45 * time.Second},
		{name: "conflicting fields", metadata: &messagepb.Message_Metadata{LeaseDuration: durationpb.New(45 * time.Second), LeasePolicy: &commonpb.LeasePolicy{BaseLease: durationpb.New(time.Minute)}}, wantValid: false},
		{name: "invalid duration", metadata: &messagepb.Message_Metadata{LeasePolicy: &commonpb.LeasePolicy{BaseLease: &durationpb.Duration{Seconds: 315576000001}}}, wantValid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &messagepb.Message{Metadata: tt.metadata}
			result := NewLeasePolicyValidator(&queuepb.QueueMetadata{LeasePolicy: queuePolicy}).Validate(context.Background(), msg)
			assert.Equal(t, tt.wantValid, result.Valid)
			if tt.wantValid {
				require.NotNil(t, msg.GetMetadata().GetLeasePolicy())
				assert.Equal(t, tt.wantBaseLease, msg.GetMetadata().GetLeasePolicy().GetBaseLease().AsDuration())
			}
		})
	}
}
