package validator

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	schema_pb "github.com/adrien19/chronoqueue/api/schema/v1"
)

func TestDurationValidator_AppliesLeaseDurationDefaults(t *testing.T) {
	tests := []struct {
		name                  string
		queueLeaseDuration    *durationpb.Duration
		messageLeaseDuration  *durationpb.Duration
		expectedLeaseDuration time.Duration
		shouldBeValid         bool
	}{
		{
			name:                  "Nil value uses queue default",
			queueLeaseDuration:    durationpb.New(45 * time.Second),
			messageLeaseDuration:  nil,
			expectedLeaseDuration: 45 * time.Second,
			shouldBeValid:         true,
		},
		{
			name:                  "Nil value with nil queue default uses system default",
			queueLeaseDuration:    nil,
			messageLeaseDuration:  nil,
			expectedLeaseDuration: DefaultLeaseDuration,
			shouldBeValid:         true,
		},
		{
			name:                  "Explicit value is preserved",
			queueLeaseDuration:    durationpb.New(45 * time.Second),
			messageLeaseDuration:  durationpb.New(2 * time.Minute),
			expectedLeaseDuration: 2 * time.Minute,
			shouldBeValid:         true,
		},
		{
			name:                  "Long lease duration is valid (no maximum)",
			queueLeaseDuration:    nil,
			messageLeaseDuration:  durationpb.New(5 * time.Hour), // Was invalid before, now valid
			expectedLeaseDuration: 5 * time.Hour,
			shouldBeValid:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var queueMeta *queue_pb.QueueMetadata
			if tt.queueLeaseDuration != nil {
				queueMeta = &queue_pb.QueueMetadata{
					LeaseDuration: tt.queueLeaseDuration,
				}
			}

			validator := NewDurationValidator(queueMeta)

			msg := &message_pb.Message{
				Metadata: &message_pb.Message_Metadata{
					LeaseDuration: tt.messageLeaseDuration,
				},
			}

			result := validator.Validate(context.Background(), msg)

			if result.Valid != tt.shouldBeValid {
				t.Errorf("Expected valid=%v, got %v. Errors: %v", tt.shouldBeValid, result.Valid, result.Errors)
			}

			if msg.Metadata.LeaseDuration.AsDuration() != tt.expectedLeaseDuration {
				t.Errorf("Expected lease_duration=%v, got %v", tt.expectedLeaseDuration, msg.Metadata.LeaseDuration.AsDuration())
			}
		})
	}
}

func TestDurationValidator_InvisibilityDurationNoMaximum(t *testing.T) {
	tests := []struct {
		name                 string
		invisibilityDuration *durationpb.Duration
		shouldBeValid        bool
		errorField           string
	}{
		{
			name:                 "Nil invisibility duration is valid (immediate visibility)",
			invisibilityDuration: nil,
			shouldBeValid:        true,
		},
		{
			name:                 "Zero invisibility duration is valid",
			invisibilityDuration: durationpb.New(0),
			shouldBeValid:        true,
		},
		{
			name:                 "Short invisibility duration is valid",
			invisibilityDuration: durationpb.New(5 * time.Second),
			shouldBeValid:        true,
		},
		{
			name:                 "Long invisibility duration is valid (58 hours - Friday to Monday)",
			invisibilityDuration: durationpb.New(58 * time.Hour), // Was invalid before, now valid
			shouldBeValid:        true,
		},
		{
			name:                 "Very long invisibility duration is valid (1 week)",
			invisibilityDuration: durationpb.New(7 * 24 * time.Hour), // Was invalid before, now valid
			shouldBeValid:        true,
		},
		{
			name:                 "Extremely long invisibility duration is valid (1 month)",
			invisibilityDuration: durationpb.New(30 * 24 * time.Hour), // Was invalid before, now valid
			shouldBeValid:        true,
		},
		{
			name:                 "Invalid: negative invisibility duration",
			invisibilityDuration: durationpb.New(-5 * time.Second),
			shouldBeValid:        false,
			errorField:           "metadata.invisibility_duration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewDurationValidator(nil)

			msg := &message_pb.Message{
				Metadata: &message_pb.Message_Metadata{
					InvisibilityDuration: tt.invisibilityDuration,
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
		})
	}
}

func TestDurationValidator_LeaseDurationValidation(t *testing.T) {
	tests := []struct {
		name          string
		leaseDuration *durationpb.Duration
		shouldBeValid bool
		errorField    string
	}{
		{
			name:          "Valid minimum lease duration (1 second)",
			leaseDuration: durationpb.New(MinLeaseDuration),
			shouldBeValid: true,
		},
		{
			name:          "Valid standard lease duration (30 seconds)",
			leaseDuration: durationpb.New(30 * time.Second),
			shouldBeValid: true,
		},
		{
			name:          "Valid long lease duration (2 hours - no maximum)",
			leaseDuration: durationpb.New(2 * time.Hour), // Was invalid before, now valid
			shouldBeValid: true,
		},
		{
			name:          "Valid very long lease duration (24 hours - no maximum)",
			leaseDuration: durationpb.New(24 * time.Hour), // Was invalid before, now valid
			shouldBeValid: true,
		},
		{
			name:          "Invalid: below minimum lease duration",
			leaseDuration: durationpb.New(500 * time.Millisecond),
			shouldBeValid: false,
			errorField:    "metadata.lease_duration",
		},
		{
			name:          "Invalid: zero lease duration",
			leaseDuration: durationpb.New(0),
			shouldBeValid: false,
			errorField:    "metadata.lease_duration",
		},
		{
			name:          "Invalid: negative lease duration",
			leaseDuration: durationpb.New(-5 * time.Second),
			shouldBeValid: false,
			errorField:    "metadata.lease_duration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewDurationValidator(nil)

			msg := &message_pb.Message{
				Metadata: &message_pb.Message_Metadata{
					LeaseDuration: tt.leaseDuration,
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
		})
	}
}

func TestDurationValidator_CalendarScheduleUseCases(t *testing.T) {
	tests := []struct {
		name                 string
		invisibilityDuration time.Duration
		leaseDuration        time.Duration
		description          string
	}{
		{
			name:                 "Friday night to Monday morning (58 hours)",
			invisibilityDuration: 58 * time.Hour,
			leaseDuration:        30 * time.Second,
			description:          "Real use case from interview platform",
		},
		{
			name:                 "Next business day (34 hours)",
			invisibilityDuration: 34 * time.Hour,
			leaseDuration:        30 * time.Second,
			description:          "Weekend scheduling",
		},
		{
			name:                 "Weekly schedule (7 days)",
			invisibilityDuration: 7 * 24 * time.Hour,
			leaseDuration:        1 * time.Minute,
			description:          "Weekly recurring task",
		},
		{
			name:                 "Monthly schedule (30 days)",
			invisibilityDuration: 30 * 24 * time.Hour,
			leaseDuration:        5 * time.Minute,
			description:          "Monthly billing run",
		},
		{
			name:                 "Long-running ML task (5 hours)",
			invisibilityDuration: 0,
			leaseDuration:        5 * time.Hour,
			description:          "ML model training with long lease",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewDurationValidator(nil)

			msg := &message_pb.Message{
				Metadata: &message_pb.Message_Metadata{
					InvisibilityDuration: durationpb.New(tt.invisibilityDuration),
					LeaseDuration:        durationpb.New(tt.leaseDuration),
				},
			}

			result := validator.Validate(context.Background(), msg)

			if !result.Valid {
				t.Errorf("%s: Expected valid=true for use case '%s', got errors: %v",
					tt.name, tt.description, result.Errors)
			}
		})
	}
}

func TestDurationValidator_MissingMetadata(t *testing.T) {
	validator := NewDurationValidator(nil)

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
