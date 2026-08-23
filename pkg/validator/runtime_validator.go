package validator

import (
	"context"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	schemapb "github.com/adrien19/chronoqueue/api/schema/v1"
)

type RuntimeValidator struct{}

func NewRuntimeValidator() Validator {
	return &RuntimeValidator{}
}

func (v *RuntimeValidator) Validate(_ context.Context, msg *messagepb.Message) *ValidationResult {
	result := NewValidationResult()
	metadata := msg.GetMetadata()
	if metadata == nil {
		return result
	}
	serverManaged := []struct {
		field string
		set   bool
	}{
		{field: "metadata.state", set: metadata.GetState() != messagepb.Message_Metadata_INVISIBLE},
		{field: "metadata.lease_expiry", set: metadata.GetLeaseExpiry() != 0},
		{field: "metadata.lease_renewal_count", set: metadata.GetLeaseRenewalCount() != 0},
		{field: "metadata.current_attempt", set: metadata.GetCurrentAttempt() != nil},
		{field: "metadata.priority_level", set: metadata.GetPriorityLevel() != 0},
	}
	for _, runtimeField := range serverManaged {
		if runtimeField.set {
			result.AddError(NewValidationError(runtimeField.field, schemapb.ErrorCode_INVALID_FORMAT, "field is managed by the server and cannot be set when posting a message"))
		}
	}
	return result
}
