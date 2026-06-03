package validator

import (
	"context"
	"fmt"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
)

const (
	// DefaultMinPriority is the minimum allowed priority
	DefaultMinPriority = 0
	// DefaultMaxPriority is the maximum allowed priority
	DefaultMaxPriority = 4
	// DefaultPriority is used when no priority is specified
	DefaultPriority = 0
)

// PriorityValidator validates message priority
// Ensures priority is within configured bounds (queue-specific or global defaults)
type PriorityValidator struct {
	queueMeta   *queue_pb.QueueMetadata
	minPriority int64
	maxPriority int64
}

// NewPriorityValidator creates a new priority validator
func NewPriorityValidator(queueMeta *queue_pb.QueueMetadata) Validator {
	validator := &PriorityValidator{
		queueMeta:   queueMeta,
		minPriority: DefaultMinPriority,
		maxPriority: DefaultMaxPriority,
	}

	// Note: Queue-specific priority limits will be added in future when ValidationConfig is implemented
	// For now, using global defaults

	return validator
}

// Validate validates the message priority
func (v *PriorityValidator) Validate(ctx context.Context, msg *message_pb.Message) *ValidationResult {
	result := &ValidationResult{
		Valid:  true,
		Errors: []*ValidationError{},
	}

	if msg.Metadata == nil {
		// Metadata is required for priority check
		result.Valid = false
		result.Errors = append(result.Errors, &ValidationError{
			Field:   "metadata",
			Message: "Message metadata is required",
		})
		return result
	}

	priority := msg.Metadata.GetPriority()

	// Check if priority is within bounds
	if priority < v.minPriority {
		result.Valid = false
		result.Errors = append(result.Errors, &ValidationError{
			Field:   "metadata.priority",
			Message: fmt.Sprintf("Priority %d is below minimum allowed value of %d", priority, v.minPriority),
		})
	}

	if priority > v.maxPriority {
		result.Valid = false
		result.Errors = append(result.Errors, &ValidationError{
			Field:   "metadata.priority",
			Message: fmt.Sprintf("Priority %d exceeds maximum allowed value of %d", priority, v.maxPriority),
		})
	}

	return result
}
