package validator

import (
	"context"
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	schema_pb "github.com/adrien19/chronoqueue/api/schema/v1"
	"github.com/adrien19/chronoqueue/pkg/schema"
)

// SchemaValidator validates messages against registered schemas
type SchemaValidator struct {
	registry  schema.Registry
	queueMeta *queue_pb.QueueMetadata
}

// NewSchemaValidator creates a new schema validator
func NewSchemaValidator(registry schema.Registry, queueMeta *queue_pb.QueueMetadata) *SchemaValidator {
	return &SchemaValidator{
		registry:  registry,
		queueMeta: queueMeta,
	}
}

// Validate validates a message against its schema
func (v *SchemaValidator) Validate(ctx context.Context, msg *message_pb.Message) *ValidationResult {
	result := NewValidationResult()

	if msg.Metadata == nil || msg.Metadata.Payload == nil {
		return result
	}

	payload := msg.Metadata.Payload

	// Determine which schema to use
	schemaID := payload.SchemaId
	schemaVersion := payload.SchemaVersion

	// If no schema specified in message, check queue default
	if schemaID == "" && v.queueMeta != nil {
		schemaID = v.queueMeta.SchemaId
	}

	// If queue requires schema but none specified, error
	if v.queueMeta != nil && v.queueMeta.SchemaRequired && schemaID == "" {
		result.AddError(
			NewValidationError(
				"payload.schema_id",
				schema_pb.ErrorCode_SCHEMA_NOT_FOUND,
				"Queue requires schema validation but no schema_id provided",
			),
		)
		return result
	}

	// If no schema specified and not required, skip validation
	if schemaID == "" {
		return result
	}

	// Convert payload data to JSON bytes
	payloadBytes, err := v.payloadToJSON(payload.Data)
	if err != nil {
		result.AddError(
			NewValidationError(
				"payload.data",
				schema_pb.ErrorCode_INVALID_FORMAT,
				"Failed to serialize payload data",
			).WithDetail("error", err.Error()),
		)
		return result
	}

	// Validate against schema
	schemaResult, err := v.registry.Validate(ctx, schemaID, schemaVersion, payloadBytes)
	if err != nil {
		result.AddError(
			NewValidationError(
				"payload",
				schema_pb.ErrorCode_SCHEMA_VALIDATION_FAILED,
				"Schema validation failed",
			).WithDetail("error", err.Error()),
		)
		return result
	}

	// Convert schema validation errors to validator errors
	if !schemaResult.Valid {
		result.Valid = false
		result.SchemaID = schemaResult.SchemaId
		result.SchemaVersion = schemaResult.SchemaVersion

		for _, schemaErr := range schemaResult.Errors {
			validationErr := &ValidationError{
				Field:     schemaErr.Field,
				ErrorCode: parseErrorCode(schemaErr.ErrorCode),
				Message:   schemaErr.Message,
				Details:   schemaErr.Details,
			}
			result.Errors = append(result.Errors, validationErr)
		}
	} else {
		result.SchemaID = schemaResult.SchemaId
		result.SchemaVersion = schemaResult.SchemaVersion
	}

	return result
}

// payloadToJSON converts payload data to JSON bytes
func (v *SchemaValidator) payloadToJSON(data interface{}) ([]byte, error) {
	if data == nil {
		return []byte("{}"), nil
	}

	// Use protojson to marshal the data
	marshaller := protojson.MarshalOptions{}
	jsonBytes, err := marshaller.Marshal(data.(proto.Message))
	if err != nil {
		// Fallback to standard JSON marshaling
		return json.Marshal(data)
	}

	// Unwrap if the data is wrapped in a "value" field
	// This happens when CLI wraps JSON data: {"value": {...actual data...}}
	var unwrapped map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &unwrapped); err == nil {
		if value, ok := unwrapped["value"]; ok {
			// Extract the "value" field and re-marshal it
			return json.Marshal(value)
		}
	}

	return jsonBytes, nil
}

// parseErrorCode converts error code string to ErrorCode enum
func parseErrorCode(codeStr string) schema_pb.ErrorCode {
	if code, ok := schema_pb.ErrorCode_value[codeStr]; ok {
		return schema_pb.ErrorCode(code)
	}
	return schema_pb.ErrorCode_UNKNOWN_ERROR
}
