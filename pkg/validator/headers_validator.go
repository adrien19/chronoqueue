package validator

import (
	"context"
	"regexp"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
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
func (v *HeadersValidator) Validate(ctx context.Context, msg *message_pb.Message) *ValidationResult {
	result := &ValidationResult{
		Valid:  true,
		Errors: []*ValidationError{},
	}

	// NOTE: Headers are not yet implemented in the Message protobuf definition
	// This validator is prepared for future use when headers are added
	// For now, it always returns valid

	// TODO: Uncomment when headers field is added to Message_Metadata
	/*
		if msg.Metadata == nil {
			return result // Headers are optional, no metadata is ok
		}

		headers := msg.Metadata.GetHeaders()
		if len(headers) == 0 {
			return result // No headers is valid
		}

		totalSize := 0

		for key, value := range headers {
			// Validate key format
			if !v.keyPattern.MatchString(key) {
				result.Valid = false
				result.Errors = append(result.Errors, &ValidationError{
					Field:   fmt.Sprintf("metadata.headers.%s", key),
					Message: "Header key must contain only lowercase letters, numbers, and hyphens",
				})
			}

			// Check for reserved prefixes
			for _, prefix := range v.reservedPrefixes {
				if strings.HasPrefix(key, prefix) {
					result.Valid = false
					result.Errors = append(result.Errors, &ValidationError{
						Field:   fmt.Sprintf("metadata.headers.%s", key),
						Message: fmt.Sprintf("Header key cannot start with reserved prefix: %s", prefix),
					})
					break
				}
			}

			// Check value size
			valueSize := len(value)
			if valueSize > v.maxValueSize {
				result.Valid = false
				result.Errors = append(result.Errors, &ValidationError{
					Field:   fmt.Sprintf("metadata.headers.%s", key),
					Message: fmt.Sprintf("Header value size %d bytes exceeds maximum of %d bytes",
						valueSize, v.maxValueSize),
				})
			}

			// Accumulate total size (key + value)
			totalSize += len(key) + valueSize
		}

		// Check total headers size
		if totalSize > v.maxTotalSize {
			result.Valid = false
			result.Errors = append(result.Errors, &ValidationError{
				Field:   "metadata.headers",
				Message: fmt.Sprintf("Total headers size %d bytes exceeds maximum of %d bytes",
					totalSize, v.maxTotalSize),
			})
		}
	*/

	return result
}
