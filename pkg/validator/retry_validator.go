package validator

import (
	"context"
	"fmt"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
)

const (
	// DefaultMaxRetryAttempts is the default maximum retry attempts
	DefaultMaxRetryAttempts = 3
	// AbsoluteMaxRetryAttempts is the absolute maximum retry attempts allowed
	AbsoluteMaxRetryAttempts = 10
)

// RetryValidator validates retry configuration
// Ensures retry attempts are within reasonable limits
type RetryValidator struct {
	queueMeta   *queue_pb.QueueMetadata
	maxAttempts int32
}

// NewRetryValidator creates a new retry validator
func NewRetryValidator(queueMeta *queue_pb.QueueMetadata) Validator {
	validator := &RetryValidator{
		queueMeta:   queueMeta,
		maxAttempts: DefaultMaxRetryAttempts,
	}

	// Use queue-specific max attempts if configured
	if queueMeta != nil && queueMeta.DefaultMaxAttempts > 0 {
		validator.maxAttempts = queueMeta.DefaultMaxAttempts
	}

	// Cap at absolute maximum
	if validator.maxAttempts > AbsoluteMaxRetryAttempts {
		validator.maxAttempts = AbsoluteMaxRetryAttempts
	}

	return validator
}

// Validate validates the retry configuration
func (v *RetryValidator) Validate(ctx context.Context, msg *message_pb.Message) *ValidationResult {
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

	// Validate max attempts
	maxAttempts := msg.Metadata.GetMaxAttempts()
	if maxAttempts < 1 {
		result.Valid = false
		result.Errors = append(result.Errors, &ValidationError{
			Field:   "metadata.max_attempts",
			Message: "Max attempts must be at least 1",
		})
	}

	if maxAttempts > v.maxAttempts {
		result.Valid = false
		result.Errors = append(result.Errors, &ValidationError{
			Field: "metadata.max_attempts",
			Message: fmt.Sprintf("Max attempts %d exceeds queue maximum of %d",
				maxAttempts, v.maxAttempts),
		})
	}

	if maxAttempts > AbsoluteMaxRetryAttempts {
		result.Valid = false
		result.Errors = append(result.Errors, &ValidationError{
			Field: "metadata.max_attempts",
			Message: fmt.Sprintf("Max attempts %d exceeds absolute maximum of %d",
				maxAttempts, AbsoluteMaxRetryAttempts),
		})
	}

	// Validate retry count: check if attempts_left is reasonable
	attemptsLeft := msg.Metadata.GetAttemptsLeft()
	if attemptsLeft < 0 {
		result.Valid = false
		result.Errors = append(result.Errors, &ValidationError{
			Field:   "metadata.attempts_left",
			Message: "Attempts left cannot be negative",
		})
	}

	if attemptsLeft > maxAttempts {
		result.Valid = false
		result.Errors = append(result.Errors, &ValidationError{
			Field: "metadata.attempts_left",
			Message: fmt.Sprintf("Attempts left %d exceeds max attempts %d",
				attemptsLeft, maxAttempts),
		})
	}

	return result
}
