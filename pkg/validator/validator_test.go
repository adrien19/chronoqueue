package validator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
)

type validatorFunc func(context.Context, *messagepb.Message) *ValidationResult

func (f validatorFunc) Validate(ctx context.Context, message *messagepb.Message) *ValidationResult {
	return f(ctx, message)
}

func TestValidatorChainPreservesSchemaMismatch(t *testing.T) {
	chain := NewValidatorChain(validatorFunc(func(context.Context, *messagepb.Message) *ValidationResult {
		return &ValidationResult{Valid: false, SchemaMismatch: true, Errors: []*ValidationError{{Field: "payload"}}}
	}))

	result := chain.Validate(context.Background(), &messagepb.Message{})
	require.False(t, result.Valid)
	require.True(t, result.SchemaMismatch)
}
