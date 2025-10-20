package validator

import (
	"context"
	"fmt"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	schema_pb "github.com/adrien19/chronoqueue/api/schema/v1"
)

// Validator is the interface for message validation
type Validator interface {
	Validate(ctx context.Context, msg *message_pb.Message) *ValidationResult
}

// ValidationResult contains the outcome of validation
type ValidationResult struct {
	Valid         bool
	Errors        []*ValidationError
	SchemaID      string
	SchemaVersion int32
}

// ValidationError provides detailed error information
type ValidationError struct {
	Field     string
	ErrorCode schema_pb.ErrorCode
	Message   string
	Details   map[string]string
}

// NewValidationError creates a new validation error
func NewValidationError(field string, code schema_pb.ErrorCode, message string) *ValidationError {
	return &ValidationError{
		Field:     field,
		ErrorCode: code,
		Message:   message,
		Details:   make(map[string]string),
	}
}

// WithDetail adds a detail to the validation error
func (e *ValidationError) WithDetail(key, value string) *ValidationError {
	e.Details[key] = value
	return e
}

// Error implements the error interface
func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s (code: %s)", e.Field, e.Message, e.ErrorCode.String())
}

// NewValidationResult creates a new validation result
func NewValidationResult() *ValidationResult {
	return &ValidationResult{
		Valid:  true,
		Errors: make([]*ValidationError, 0),
	}
}

// AddError adds an error to the validation result
func (r *ValidationResult) AddError(err *ValidationError) {
	r.Valid = false
	r.Errors = append(r.Errors, err)
}

// HasErrors returns true if there are validation errors
func (r *ValidationResult) HasErrors() bool {
	return len(r.Errors) > 0
}

// ToProto converts ValidationResult to protobuf
func (r *ValidationResult) ToProto() *schema_pb.ValidationResult {
	protoErrors := make([]*schema_pb.ValidationError, len(r.Errors))
	for i, err := range r.Errors {
		protoErrors[i] = &schema_pb.ValidationError{
			Field:     err.Field,
			ErrorCode: err.ErrorCode.String(),
			Message:   err.Message,
			Details:   err.Details,
		}
	}

	return &schema_pb.ValidationResult{
		Valid:         r.Valid,
		Errors:        protoErrors,
		SchemaId:      r.SchemaID,
		SchemaVersion: r.SchemaVersion,
	}
}

// ValidatorChain chains multiple validators
type ValidatorChain struct {
	validators []Validator
}

// NewValidatorChain creates a new validator chain
func NewValidatorChain(validators ...Validator) *ValidatorChain {
	return &ValidatorChain{
		validators: validators,
	}
}

// Validate runs all validators in the chain
func (c *ValidatorChain) Validate(ctx context.Context, msg *message_pb.Message) *ValidationResult {
	result := NewValidationResult()

	for _, validator := range c.validators {
		vResult := validator.Validate(ctx, msg)
		if !vResult.Valid {
			result.Valid = false
			result.Errors = append(result.Errors, vResult.Errors...)
		}
	}

	return result
}
