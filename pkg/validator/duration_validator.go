package validator

import (
	"context"
	"fmt"
	"time"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
)

const (
	// MaxInvisibilityDuration is the maximum allowed invisibility duration (12 hours)
	MaxInvisibilityDuration = 12 * time.Hour
	// MinLeaseDuration is the minimum lease duration (1 second)
	MinLeaseDuration = 1 * time.Second
	// MaxLeaseDuration is the maximum lease duration (1 hour)
	MaxLeaseDuration = 1 * time.Hour
)

// DurationValidator validates message duration fields
// Checks invisibility and lease durations are within acceptable ranges
type DurationValidator struct {
	queueMeta               *queue_pb.QueueMetadata
	maxInvisibilityDuration time.Duration
	minLeaseDuration        time.Duration
	maxLeaseDuration        time.Duration
}

// NewDurationValidator creates a new duration validator
func NewDurationValidator(queueMeta *queue_pb.QueueMetadata) Validator {
	validator := &DurationValidator{
		queueMeta:               queueMeta,
		maxInvisibilityDuration: MaxInvisibilityDuration,
		minLeaseDuration:        MinLeaseDuration,
		maxLeaseDuration:        MaxLeaseDuration,
	}

	// Note: Queue-specific duration limits will be added when ValidationConfig is implemented

	return validator
}

// Validate validates the message duration fields
func (v *DurationValidator) Validate(ctx context.Context, msg *message_pb.Message) *ValidationResult {
	result := &ValidationResult{
		Valid:  true,
		Errors: []*ValidationError{},
	}

	if msg.Metadata == nil {
		result.Valid = false
		result.Errors = append(result.Errors, &ValidationError{
			Field:   "metadata",
			Message: "Message metadata is required",
		})
		return result
	}

	// Validate invisibility duration
	if msg.Metadata.InvisibilityDuration != nil {
		invisDuration := msg.Metadata.InvisibilityDuration.AsDuration()

		if invisDuration < 0 {
			result.Valid = false
			result.Errors = append(result.Errors, &ValidationError{
				Field:   "metadata.invisibility_duration",
				Message: "Invisibility duration cannot be negative",
			})
		}

		if invisDuration > v.maxInvisibilityDuration {
			result.Valid = false
			result.Errors = append(result.Errors, &ValidationError{
				Field: "metadata.invisibility_duration",
				Message: fmt.Sprintf("Invisibility duration %v exceeds maximum allowed value of %v",
					invisDuration, v.maxInvisibilityDuration),
			})
		}
	}

	// Validate lease duration if specified
	if msg.Metadata.LeaseDuration != nil {
		leaseDuration := msg.Metadata.LeaseDuration.AsDuration()

		if leaseDuration < v.minLeaseDuration {
			result.Valid = false
			result.Errors = append(result.Errors, &ValidationError{
				Field: "metadata.lease_duration",
				Message: fmt.Sprintf("Lease duration %v is below minimum allowed value of %v",
					leaseDuration, v.minLeaseDuration),
			})
		}

		if leaseDuration > v.maxLeaseDuration {
			result.Valid = false
			result.Errors = append(result.Errors, &ValidationError{
				Field: "metadata.lease_duration",
				Message: fmt.Sprintf("Lease duration %v exceeds maximum allowed value of %v",
					leaseDuration, v.maxLeaseDuration),
			})
		}
	}

	return result
}
