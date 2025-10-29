package validator

import (
	"context"
	"fmt"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	schema_pb "github.com/adrien19/chronoqueue/api/schema/v1"
)

const (
	// DefaultMaxRetryAttempts is the default maximum retry attempts when not specified
	DefaultMaxRetryAttempts = 3
	// InfiniteRetries indicates unlimited retry attempts
	InfiniteRetries = -1
)

// RetryValidator validates retry configuration
// Applies defaults and ensures retry attempts are valid
type RetryValidator struct {
	queueMeta *queue_pb.QueueMetadata
}

// NewRetryValidator creates a new retry validator
func NewRetryValidator(queueMeta *queue_pb.QueueMetadata) Validator {
	return &RetryValidator{
		queueMeta: queueMeta,
	}
}

// Validate validates the retry configuration
// Applies defaults for zero values and validates the final value
func (v *RetryValidator) Validate(ctx context.Context, msg *message_pb.Message) *ValidationResult {
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

	// Get max_attempts from message and queue
	maxAttempts := msg.Metadata.GetMaxAttempts()
	queueDefaultMaxAttempts := int32(0)
	if v.queueMeta != nil {
		queueDefaultMaxAttempts = v.queueMeta.GetDefaultMaxAttempts()
	}

	// Apply defaults if max_attempts is 0 (not set)
	if maxAttempts == 0 {
		// Use queue default if available
		if queueDefaultMaxAttempts != 0 {
			maxAttempts = queueDefaultMaxAttempts
		} else {
			// Use system default
			maxAttempts = DefaultMaxRetryAttempts
		}
		// Apply the default back to the message
		msg.Metadata.MaxAttempts = maxAttempts
	}

	// Validate max_attempts after defaults applied
	// Valid values: -1 (infinite retries) or >= 1 (specific count)
	if maxAttempts < InfiniteRetries || maxAttempts == 0 {
		result.Valid = false
		result.Errors = append(result.Errors, NewValidationError(
			"metadata.max_attempts",
			schema_pb.ErrorCode_VALUE_OUT_OF_RANGE,
			fmt.Sprintf("Max attempts must be -1 (infinite) or >= 1, got %d", maxAttempts),
		))
	}

	// Validate attempts_left is reasonable
	attemptsLeft := msg.Metadata.GetAttemptsLeft()

	// For infinite retries, attempts_left should be -1
	if maxAttempts == InfiniteRetries {
		if attemptsLeft != InfiniteRetries {
			// Auto-correct for infinite retries
			msg.Metadata.AttemptsLeft = InfiniteRetries
		}
	} else {
		// For finite retries, validate attempts_left
		if attemptsLeft < 0 && attemptsLeft != InfiniteRetries {
			result.Valid = false
			result.Errors = append(result.Errors, NewValidationError(
				"metadata.attempts_left",
				schema_pb.ErrorCode_VALUE_OUT_OF_RANGE,
				fmt.Sprintf("Attempts left must be >= 0 or -1 (infinite), got %d", attemptsLeft),
			))
		}

		if attemptsLeft > maxAttempts {
			result.Valid = false
			result.Errors = append(result.Errors, NewValidationError(
				"metadata.attempts_left",
				schema_pb.ErrorCode_VALUE_OUT_OF_RANGE,
				fmt.Sprintf("Attempts left %d exceeds max attempts %d", attemptsLeft, maxAttempts),
			))
		}
	}

	return result
}
