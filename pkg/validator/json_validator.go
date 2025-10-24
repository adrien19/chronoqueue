package validator

import (
	"context"
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	schema_pb "github.com/adrien19/chronoqueue/api/schema/v1"
)

// JSONStructureValidator validates that JSON payloads have valid structure
type JSONStructureValidator struct{}

// NewJSONStructureValidator creates a new JSON structure validator
func NewJSONStructureValidator() *JSONStructureValidator {
	return &JSONStructureValidator{}
}

// Validate validates the JSON structure of a message payload
func (v *JSONStructureValidator) Validate(ctx context.Context, msg *message_pb.Message) *ValidationResult {
	result := NewValidationResult()

	if msg.Metadata == nil || msg.Metadata.Payload == nil {
		return result
	}

	payload := msg.Metadata.Payload

	// Only validate if content type is JSON
	contentType := normalizeContentType(payload.ContentType)
	if contentType != "application/json" && contentType != "application/x-json" {
		return result // Skip validation for non-JSON content
	}

	// Validate that payload.data is valid JSON if present
	if payload.Data != nil {
		// Serialize to JSON to validate structure
		marshaller := protojson.MarshalOptions{}
		jsonBytes, err := marshaller.Marshal(payload.Data)
		if err != nil {
			result.AddError(
				NewValidationError(
					"payload.data",
					schema_pb.ErrorCode_INVALID_FORMAT,
					"Failed to serialize payload data to JSON",
				).WithDetail("error", err.Error()),
			)
			return result
		}

		// Try to unmarshal to verify it's valid JSON
		var temp interface{}
		if err := json.Unmarshal(jsonBytes, &temp); err != nil {
			result.AddError(
				NewValidationError(
					"payload.data",
					schema_pb.ErrorCode_INVALID_FORMAT,
					"Payload data is not valid JSON",
				).WithDetail("error", err.Error()),
			)
		}
	}

	return result
}
