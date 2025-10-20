package validator

import (
	"context"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	"github.com/adrien19/chronoqueue/pkg/schema"
)

// PayloadValidator combines multiple validators for comprehensive validation
type PayloadValidator struct {
	chain *ValidatorChain
}

// NewPayloadValidator creates a new payload validator with all sub-validators
// Validation order:
// 1. Metadata validators (protobuf-defined fields)
// 2. Payload validators (user content)
// 3. Schema validators (user payload structure)
func NewPayloadValidator(queueMeta *queue_pb.QueueMetadata, schemaRegistry schema.Registry) *PayloadValidator {
	validators := []Validator{
		// Phase 3: Metadata validators (validate protobuf-defined fields FIRST)
		NewMessageIDValidator(),         // Validate message ID format
		NewPriorityValidator(queueMeta), // Validate priority bounds
		NewDurationValidator(queueMeta), // Validate duration limits
		NewRetryValidator(queueMeta),    // Validate retry configuration
		NewHeadersValidator(),           // Validate headers (prepared for future)

		// Phase 1: Payload validators (validate user content)
		NewContentTypeValidator(queueMeta), // Validate MIME type
		NewSizeValidator(queueMeta),        // Validate size limits
		NewJSONStructureValidator(),        // Validate JSON structure if applicable
	}

	// Phase 2: Schema validator (validate against JSON Schema if configured)
	// This ONLY validates user payload data, not protobuf structure
	if schemaRegistry != nil {
		validators = append(validators, NewSchemaValidator(schemaRegistry, queueMeta))
	}

	return &PayloadValidator{
		chain: NewValidatorChain(validators...),
	}
}

// Validate runs all validators
func (v *PayloadValidator) Validate(ctx context.Context, msg *message_pb.Message) *ValidationResult {
	return v.chain.Validate(ctx, msg)
}
