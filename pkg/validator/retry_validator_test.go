package validator

import (
	"context"
	"testing"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	schema_pb "github.com/adrien19/chronoqueue/api/schema/v1"
)

func TestRetryValidator_AppliesDefaults(t *testing.T) {
	tests := []struct {
		name                string
		queueDefault        int32
		messageMaxAttempts  int32
		expectedMaxAttempts int32
		shouldBeValid       bool
	}{
		{
			name:                "Zero value uses queue default",
			queueDefault:        5,
			messageMaxAttempts:  0,
			expectedMaxAttempts: 5,
			shouldBeValid:       true,
		},
		{
			name:                "Zero value with zero queue default uses system default",
			queueDefault:        0,
			messageMaxAttempts:  0,
			expectedMaxAttempts: DefaultMaxRetryAttempts,
			shouldBeValid:       true,
		},
		{
			name:                "Explicit value is preserved",
			queueDefault:        5,
			messageMaxAttempts:  7,
			expectedMaxAttempts: 7,
			shouldBeValid:       true,
		},
		{
			name:                "Infinite retries is valid",
			queueDefault:        5,
			messageMaxAttempts:  InfiniteRetries,
			expectedMaxAttempts: InfiniteRetries,
			shouldBeValid:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queueMeta := &queue_pb.QueueMetadata{
				DefaultMaxAttempts: tt.queueDefault,
			}

			validator := NewRetryValidator(queueMeta)

			msg := &message_pb.Message{
				Metadata: &message_pb.Message_Metadata{
					MaxAttempts:  tt.messageMaxAttempts,
					AttemptsLeft: tt.messageMaxAttempts, // Start with same value
				},
			}

			result := validator.Validate(context.Background(), msg)

			if result.Valid != tt.shouldBeValid {
				t.Errorf("Expected valid=%v, got %v. Errors: %v", tt.shouldBeValid, result.Valid, result.Errors)
			}

			if msg.Metadata.MaxAttempts != tt.expectedMaxAttempts {
				t.Errorf("Expected max_attempts=%d, got %d", tt.expectedMaxAttempts, msg.Metadata.MaxAttempts)
			}
		})
	}
}

func TestRetryValidator_ValidationRules(t *testing.T) {
	tests := []struct {
		name          string
		maxAttempts   int32
		attemptsLeft  int32
		shouldBeValid bool
		errorField    string
	}{
		{
			name:          "Valid finite retries",
			maxAttempts:   5,
			attemptsLeft:  3,
			shouldBeValid: true,
		},
		{
			name:          "Valid infinite retries",
			maxAttempts:   InfiniteRetries,
			attemptsLeft:  InfiniteRetries,
			shouldBeValid: true,
		},
		{
			name:          "Infinite retries auto-corrects attempts_left",
			maxAttempts:   InfiniteRetries,
			attemptsLeft:  5, // Will be corrected to -1
			shouldBeValid: true,
		},
		{
			name:          "Invalid: attempts_left exceeds max_attempts",
			maxAttempts:   5,
			attemptsLeft:  10,
			shouldBeValid: false,
			errorField:    "metadata.attempts_left",
		},
		{
			name:          "Invalid: negative attempts_left (not infinite)",
			maxAttempts:   5,
			attemptsLeft:  -2,
			shouldBeValid: false,
			errorField:    "metadata.attempts_left",
		},
		{
			name:          "Invalid: max_attempts less than -1",
			maxAttempts:   -2,
			attemptsLeft:  0,
			shouldBeValid: false,
			errorField:    "metadata.max_attempts",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewRetryValidator(nil)

			msg := &message_pb.Message{
				Metadata: &message_pb.Message_Metadata{
					MaxAttempts:  tt.maxAttempts,
					AttemptsLeft: tt.attemptsLeft,
				},
			}

			result := validator.Validate(context.Background(), msg)

			if result.Valid != tt.shouldBeValid {
				t.Errorf("Expected valid=%v, got %v. Errors: %v", tt.shouldBeValid, result.Valid, result.Errors)
			}

			if !tt.shouldBeValid && tt.errorField != "" {
				found := false
				for _, err := range result.Errors {
					if err.Field == tt.errorField {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected error on field %s, but not found in errors: %v", tt.errorField, result.Errors)
				}
			}

			// Verify infinite retry auto-correction
			if tt.maxAttempts == InfiniteRetries && tt.shouldBeValid {
				if msg.Metadata.AttemptsLeft != InfiniteRetries {
					t.Errorf("Expected attempts_left to be corrected to %d, got %d", InfiniteRetries, msg.Metadata.AttemptsLeft)
				}
			}
		})
	}
}

func TestRetryValidator_MissingMetadata(t *testing.T) {
	validator := NewRetryValidator(nil)

	msg := &message_pb.Message{
		Metadata: nil,
	}

	result := validator.Validate(context.Background(), msg)

	if result.Valid {
		t.Error("Expected validation to fail for missing metadata")
	}

	if len(result.Errors) == 0 {
		t.Error("Expected at least one error")
	}

	if result.Errors[0].Field != "metadata" {
		t.Errorf("Expected error on 'metadata' field, got %s", result.Errors[0].Field)
	}

	if result.Errors[0].ErrorCode != schema_pb.ErrorCode_REQUIRED_FIELD_MISSING {
		t.Errorf("Expected error code REQUIRED_FIELD_MISSING, got %v", result.Errors[0].ErrorCode)
	}
}
