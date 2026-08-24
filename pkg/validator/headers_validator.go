package validator

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	schema_pb "github.com/adrien19/chronoqueue/api/schema/v1"
)

const (
	// MaxHeaderValueSize is the maximum size for a single header value (4KB)
	MaxHeaderValueSize = 4 * 1024
	// MaxTotalHeadersSize is the maximum total size for all headers (32KB)
	MaxTotalHeadersSize = 32 * 1024
)

// HeadersValidator validates message headers
// Ensures headers follow naming conventions and size limits
type HeadersValidator struct {
	keyPattern       *regexp.Regexp
	reservedPrefixes []string
	maxValueSize     int
	maxTotalSize     int
}

// NewHeadersValidator creates a new headers validator
func NewHeadersValidator() Validator {
	return &HeadersValidator{
		// Header keys: lowercase letters, numbers, hyphens
		keyPattern:       regexp.MustCompile(`^[a-z0-9-]+$`),
		reservedPrefixes: []string{"x-chronoqueue-", "x-internal-", "x-system-"},
		maxValueSize:     MaxHeaderValueSize,
		maxTotalSize:     MaxTotalHeadersSize,
	}
}

// Validate validates the message headers
func (v *HeadersValidator) Validate(_ context.Context, msg *message_pb.Message) *ValidationResult {
	result := NewValidationResult()
	if msg == nil || msg.GetMetadata() == nil {
		return result
	}

	totalSize := 0
	for i, header := range msg.GetMetadata().GetHeaders() {
		field := fmt.Sprintf("metadata.headers[%d]", i)
		if header == nil {
			result.AddError(NewValidationError(field, schema_pb.ErrorCode_REQUIRED_FIELD_MISSING, "Header is required"))
			continue
		}

		key := header.GetKey()
		if !v.keyPattern.MatchString(key) {
			result.AddError(NewValidationError(field+".key", schema_pb.ErrorCode_INVALID_FORMAT, "Header key must contain only lowercase letters, numbers, and hyphens"))
		}
		for _, prefix := range v.reservedPrefixes {
			if strings.HasPrefix(key, prefix) {
				result.AddError(
					NewValidationError(field+".key", schema_pb.ErrorCode_INVALID_FORMAT, "Header key uses a reserved prefix").
						WithDetail("reserved_prefix", prefix),
				)
				break
			}
		}

		valueSize := len(header.GetValue())
		if valueSize > v.maxValueSize {
			result.AddError(
				NewValidationError(field+".value", schema_pb.ErrorCode_PAYLOAD_SIZE_EXCEEDED, "Header value exceeds maximum size").
					WithDetail("size", fmt.Sprintf("%d", valueSize)).
					WithDetail("max_size", fmt.Sprintf("%d", v.maxValueSize)),
			)
		}
		totalSize += len(key) + valueSize
	}

	if totalSize > v.maxTotalSize {
		result.AddError(
			NewValidationError("metadata.headers", schema_pb.ErrorCode_PAYLOAD_SIZE_EXCEEDED, "Total headers size exceeds maximum").
				WithDetail("size", fmt.Sprintf("%d", totalSize)).
				WithDetail("max_size", fmt.Sprintf("%d", v.maxTotalSize)),
		)
	}

	return result
}
