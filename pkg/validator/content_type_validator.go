package validator

import (
	"context"
	"strings"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	schema_pb "github.com/adrien19/chronoqueue/api/schema/v1"
)

// SupportedContentTypes defines the MIME types we support
var SupportedContentTypes = map[string]bool{
	"application/json":         true,
	"application/x-json":       true,
	"text/plain":               true,
	"application/xml":          true,
	"application/octet-stream": true,
	"":                         true, // Empty defaults to application/json
}

// ContentTypeValidator validates message content types
type ContentTypeValidator struct {
	queueMeta *queue_pb.QueueMetadata
}

// NewContentTypeValidator creates a new content type validator
func NewContentTypeValidator(queueMeta *queue_pb.QueueMetadata) *ContentTypeValidator {
	return &ContentTypeValidator{
		queueMeta: queueMeta,
	}
}

// Validate validates the content type of a message
func (v *ContentTypeValidator) Validate(ctx context.Context, msg *message_pb.Message) *ValidationResult {
	result := NewValidationResult()

	if msg.Metadata == nil || msg.Metadata.Payload == nil {
		result.AddError(NewValidationError(
			"payload",
			schema_pb.ErrorCode_REQUIRED_FIELD_MISSING,
			"Message payload is required",
		))
		return result
	}

	contentType := msg.Metadata.Payload.ContentType

	// Default to application/json if not specified
	if contentType == "" {
		msg.Metadata.Payload.ContentType = "application/json"
		contentType = "application/json"
	}

	// Normalize content type (remove charset and other parameters)
	contentType = normalizeContentType(contentType)

	// Check if content type is supported
	if !SupportedContentTypes[contentType] {
		result.AddError(
			NewValidationError(
				"payload.content_type",
				schema_pb.ErrorCode_CONTENT_TYPE_INVALID,
				"Unsupported content type",
			).WithDetail("provided", contentType).
				WithDetail("supported", "application/json, text/plain, application/xml, application/octet-stream"),
		)
		return result
	}

	// If queue has allowed content types, check against that list
	if len(v.queueMeta.AllowedContentTypes) > 0 {
		allowed := false
		for _, allowedType := range v.queueMeta.AllowedContentTypes {
			if normalizeContentType(allowedType) == contentType {
				allowed = true
				break
			}
		}

		if !allowed {
			result.AddError(
				NewValidationError(
					"payload.content_type",
					schema_pb.ErrorCode_CONTENT_TYPE_INVALID,
					"Content type not allowed for this queue",
				).WithDetail("provided", contentType).
					WithDetail("allowed", strings.Join(v.queueMeta.AllowedContentTypes, ", ")),
			)
		}
	}

	return result
}

// normalizeContentType removes parameters from content type (e.g., charset)
func normalizeContentType(contentType string) string {
	// Split on semicolon to remove parameters like charset
	parts := strings.Split(contentType, ";")
	if len(parts) > 0 {
		return strings.TrimSpace(strings.ToLower(parts[0]))
	}
	return strings.TrimSpace(strings.ToLower(contentType))
}
