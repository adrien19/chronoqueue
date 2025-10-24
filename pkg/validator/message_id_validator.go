package validator

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
)

// MessageIDValidator validates message ID format and constraints
// This validator ensures message IDs follow ChronoQueue conventions
type MessageIDValidator struct {
	pattern          *regexp.Regexp
	reservedPrefixes []string
	minLength        int
	maxLength        int
}

// NewMessageIDValidator creates a new message ID validator
func NewMessageIDValidator() Validator {
	return &MessageIDValidator{
		pattern:          regexp.MustCompile(`^[a-zA-Z0-9_-]+$`),
		reservedPrefixes: []string{"system:", "internal:", "chronoqueue:"},
		minLength:        1,
		maxLength:        256,
	}
}

// Validate validates the message ID
func (v *MessageIDValidator) Validate(ctx context.Context, msg *message_pb.Message) *ValidationResult {
	result := &ValidationResult{
		Valid:  true,
		Errors: []*ValidationError{},
	}

	messageID := msg.GetMessageId()

	// Check if message ID is empty
	if messageID == "" {
		result.Valid = false
		result.Errors = append(result.Errors, &ValidationError{
			Field:   "message_id",
			Message: "Message ID is required and cannot be empty",
		})
		return result
	}

	// Check length constraints
	if len(messageID) < v.minLength {
		result.Valid = false
		result.Errors = append(result.Errors, &ValidationError{
			Field:   "message_id",
			Message: fmt.Sprintf("Message ID too short (minimum %d characters)", v.minLength),
		})
	}

	if len(messageID) > v.maxLength {
		result.Valid = false
		result.Errors = append(result.Errors, &ValidationError{
			Field:   "message_id",
			Message: fmt.Sprintf("Message ID too long (maximum %d characters)", v.maxLength),
		})
	}

	// Check format pattern
	if !v.pattern.MatchString(messageID) {
		result.Valid = false
		result.Errors = append(result.Errors, &ValidationError{
			Field:   "message_id",
			Message: "Message ID contains invalid characters (allowed: alphanumeric, underscore, hyphen)",
		})
	}

	// Check for reserved prefixes
	for _, prefix := range v.reservedPrefixes {
		if strings.HasPrefix(strings.ToLower(messageID), prefix) {
			result.Valid = false
			result.Errors = append(result.Errors, &ValidationError{
				Field:   "message_id",
				Message: fmt.Sprintf("Message ID cannot start with reserved prefix: %s", prefix),
			})
			break
		}
	}

	return result
}
