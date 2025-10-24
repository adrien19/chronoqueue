package schema

import (
	"encoding/json"
	"fmt"

	"github.com/xeipuuv/gojsonschema"

	schema_pb "github.com/adrien19/chronoqueue/api/schema/v1"
)

// ValidateJSONSchema validates a JSON payload against a JSON Schema
func ValidateJSONSchema(schemaContent string, payload []byte) (*schema_pb.ValidationResult, error) {
	result := &schema_pb.ValidationResult{
		Valid:  true,
		Errors: make([]*schema_pb.ValidationError, 0),
	}

	// Parse payload as JSON
	var payloadData interface{}
	if err := json.Unmarshal(payload, &payloadData); err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, &schema_pb.ValidationError{
			Field:     "payload",
			ErrorCode: schema_pb.ErrorCode_INVALID_FORMAT.String(),
			Message:   "Payload is not valid JSON",
			Details:   map[string]string{"error": err.Error()},
		})
		return result, nil
	}

	// Load schema
	schemaLoader := gojsonschema.NewStringLoader(schemaContent)

	// Load document
	documentLoader := gojsonschema.NewGoLoader(payloadData)

	// Validate
	validationResult, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return nil, fmt.Errorf("schema validation failed: %w", err)
	}

	// Convert validation errors
	if !validationResult.Valid() {
		result.Valid = false

		for _, err := range validationResult.Errors() {
			errorCode := mapValidationErrorType(err.Type())

			validationError := &schema_pb.ValidationError{
				Field:     err.Field(),
				ErrorCode: errorCode.String(),
				Message:   err.Description(),
				Details: map[string]string{
					"context": err.Context().String(),
					"type":    err.Type(),
				},
			}

			// Add value if available
			if err.Value() != nil {
				validationError.Details["value"] = fmt.Sprintf("%v", err.Value())
			}

			result.Errors = append(result.Errors, validationError)
		}
	}

	return result, nil
}

// mapValidationErrorType maps gojsonschema error types to our error codes
func mapValidationErrorType(errorType string) schema_pb.ErrorCode {
	switch errorType {
	case "required":
		return schema_pb.ErrorCode_REQUIRED_FIELD_MISSING
	case "invalid_type":
		return schema_pb.ErrorCode_INVALID_TYPE
	case "format":
		return schema_pb.ErrorCode_INVALID_FORMAT
	case "pattern":
		return schema_pb.ErrorCode_PATTERN_MISMATCH
	case "minimum", "maximum", "minimum_exclusive", "maximum_exclusive":
		return schema_pb.ErrorCode_VALUE_OUT_OF_RANGE
	case "min_length", "max_length", "min_items", "max_items":
		return schema_pb.ErrorCode_ARRAY_LENGTH_INVALID
	case "min_properties", "max_properties":
		return schema_pb.ErrorCode_OBJECT_PROPERTIES_INVALID
	case "additional_properties_false":
		return schema_pb.ErrorCode_ADDITIONAL_PROPERTIES_NOT_ALLOWED
	case "enum":
		return schema_pb.ErrorCode_ENUM_VALUE_INVALID
	default:
		return schema_pb.ErrorCode_SCHEMA_VALIDATION_FAILED
	}
}
