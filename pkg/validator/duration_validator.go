package validator

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	schema_pb "github.com/adrien19/chronoqueue/api/schema/v1"
)

const (
	// DefaultLeaseDuration is the default lease duration when not specified
	DefaultLeaseDuration = 30 * time.Second
	// MinLeaseDuration is the minimum lease duration (1 second)
	MinLeaseDuration = 1 * time.Second
)

// DurationValidator validates message duration fields
// Applies defaults for nil values and validates duration ranges
type DurationValidator struct {
	queueMeta *queue_pb.QueueMetadata
}

// NewDurationValidator creates a new duration validator
func NewDurationValidator(queueMeta *queue_pb.QueueMetadata) Validator {
	return &DurationValidator{
		queueMeta: queueMeta,
	}
}

// Validate validates the message duration fields
// Applies defaults for nil values and validates the final values
func (v *DurationValidator) Validate(ctx context.Context, msg *message_pb.Message) *ValidationResult {
	result := &ValidationResult{
		Valid:  true,
		Errors: []*ValidationError{},
	}

	if msg.Metadata == nil {
		result.Valid = false
		result.Errors = append(result.Errors, NewValidationError(
			"metadata",
			schema_pb.ErrorCode_REQUIRED_FIELD_MISSING,
			"Message metadata is required",
		))
		return result
	}

	// Apply defaults for lease_duration if not specified
	if msg.Metadata.LeaseDuration == nil {
		// Use queue default if available
		if v.queueMeta != nil && v.queueMeta.LeaseDuration != nil {
			msg.Metadata.LeaseDuration = v.queueMeta.LeaseDuration
		} else {
			// Apply system default
			msg.Metadata.LeaseDuration = durationpb.New(DefaultLeaseDuration)
		}
	}

	// Validate lease duration after defaults applied
	leaseDuration := msg.Metadata.LeaseDuration.AsDuration()
	if leaseDuration < MinLeaseDuration {
		result.Valid = false
		result.Errors = append(result.Errors, NewValidationError(
			"metadata.lease_duration",
			schema_pb.ErrorCode_VALUE_OUT_OF_RANGE,
			fmt.Sprintf("Lease duration %v is below minimum allowed value of %v",
				leaseDuration, MinLeaseDuration),
		))
	}

	return result
}
