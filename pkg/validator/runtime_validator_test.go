package validator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
)

func TestRuntimeValidator(t *testing.T) {
	valid := NewRuntimeValidator().Validate(context.Background(), &messagepb.Message{Metadata: &messagepb.Message_Metadata{}})
	assert.True(t, valid.Valid)

	invalid := NewRuntimeValidator().Validate(context.Background(), &messagepb.Message{Metadata: &messagepb.Message_Metadata{
		State:             messagepb.Message_Metadata_RUNNING,
		LeaseExpiry:       1,
		CurrentAttempt:    &messagepb.Message_Metadata_AttemptRuntime{AttemptId: "forged"},
		PriorityLevel:     2,
		LeaseRenewalCount: 1,
	}})
	assert.False(t, invalid.Valid)
	require.Len(t, invalid.Errors, 5)
}
