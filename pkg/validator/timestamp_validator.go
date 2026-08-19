package validator

import (
	"context"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	schemapb "github.com/adrien19/chronoqueue/api/schema/v1"
)

type TimestampValidator struct{}

func NewTimestampValidator() Validator {
	return &TimestampValidator{}
}

func (v *TimestampValidator) Validate(_ context.Context, msg *messagepb.Message) *ValidationResult {
	result := NewValidationResult()
	if scheduledTime := msg.GetMetadata().GetScheduledTime(); scheduledTime != nil {
		if err := scheduledTime.CheckValid(); err != nil {
			result.AddError(NewValidationError("metadata.scheduled_time", schemapb.ErrorCode_INVALID_FORMAT, "scheduled_time is invalid"))
		}
	}
	return result
}
