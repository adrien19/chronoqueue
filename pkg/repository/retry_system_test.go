package repository

import (
	"testing"
	"time"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestHybridRetrySystem(t *testing.T) {
	// Test the hybrid retry initialization logic
	tests := []struct {
		name                    string
		queueDefaultMaxAttempts int32
		messageMaxAttempts      int32
		messageAttemptsLeft     int32
		expectedMaxAttempts     int32
		expectedAttemptsLeft    int32
	}{
		{
			name:                    "Message with no retry config uses queue default",
			queueDefaultMaxAttempts: 5,
			messageMaxAttempts:      0,
			messageAttemptsLeft:     0,
			expectedMaxAttempts:     5,
			expectedAttemptsLeft:    5,
		},
		{
			name:                    "Message with max_attempts but no attempts_left gets initialized",
			queueDefaultMaxAttempts: 3,
			messageMaxAttempts:      7,
			messageAttemptsLeft:     0,
			expectedMaxAttempts:     7,
			expectedAttemptsLeft:    7,
		},
		{
			name:                    "Message with infinite retries (max_attempts = -1)",
			queueDefaultMaxAttempts: 3,
			messageMaxAttempts:      -1,
			messageAttemptsLeft:     0,
			expectedMaxAttempts:     -1,
			expectedAttemptsLeft:    -1,
		},
		{
			name:                    "Message with both values set remains unchanged",
			queueDefaultMaxAttempts: 3,
			messageMaxAttempts:      5,
			messageAttemptsLeft:     2,
			expectedMaxAttempts:     5,
			expectedAttemptsLeft:    2,
		},
		{
			name:                    "Queue default infinite retries",
			queueDefaultMaxAttempts: -1,
			messageMaxAttempts:      0,
			messageAttemptsLeft:     0,
			expectedMaxAttempts:     -1,
			expectedAttemptsLeft:    -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test message
			message := &message_pb.Message{
				MessageId: "test-message",
				Metadata: &message_pb.Message_Metadata{
					MaxAttempts:   tt.messageMaxAttempts,
					AttemptsLeft:  tt.messageAttemptsLeft,
					LeaseDuration: durationpb.New(30 * time.Second),
				},
			}

			// Create test queue metadata
			queueMeta := &queue_pb.QueueMetadata{
				DefaultMaxAttempts: tt.queueDefaultMaxAttempts,
				LeaseDuration:      durationpb.New(30 * time.Second),
			}

			// Create test request
			request := &queueservice_pb.GetNextMessageRequest{
				QueueName:     "test-queue",
				LeaseDuration: durationpb.New(30 * time.Second),
			}

			// Create storage instance (we only need the method)
			storage := &storage{}

			// Call the function
			storage.updateMessageStateAndLease(message, request, queueMeta)

			// Verify results
			if message.Metadata.MaxAttempts != tt.expectedMaxAttempts {
				t.Errorf("Expected MaxAttempts=%d, got %d", tt.expectedMaxAttempts, message.Metadata.MaxAttempts)
			}
			if message.Metadata.AttemptsLeft != tt.expectedAttemptsLeft {
				t.Errorf("Expected AttemptsLeft=%d, got %d", tt.expectedAttemptsLeft, message.Metadata.AttemptsLeft)
			}
			if message.Metadata.State != message_pb.Message_Metadata_RUNNING {
				t.Errorf("Expected state to be RUNNING, got %v", message.Metadata.State)
			}
			if message.Metadata.LeaseExpiry == 0 {
				t.Error("Expected LeaseExpiry to be set")
			}
		})
	}
}

func TestInfiniteRetryScenarios(t *testing.T) {
	tests := []struct {
		name         string
		maxAttempts  int32
		attemptsLeft int32
		description  string
		shouldRetry  bool
	}{
		{
			name:         "Infinite retries with -1",
			maxAttempts:  -1,
			attemptsLeft: -1,
			description:  "Messages with max_attempts=-1 should retry infinitely",
			shouldRetry:  true,
		},
		{
			name:         "Normal retries with attempts left",
			maxAttempts:  3,
			attemptsLeft: 2,
			description:  "Messages with attempts left should retry",
			shouldRetry:  true,
		},
		{
			name:         "No retries left",
			maxAttempts:  3,
			attemptsLeft: 0,
			description:  "Messages with no attempts left should not retry",
			shouldRetry:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This test verifies the conceptual logic
			// The actual Lua script implementation would be tested separately

			if tt.maxAttempts == -1 {
				// Infinite retries
				if !tt.shouldRetry {
					t.Error("Infinite retry messages should always retry")
				}
			} else if tt.attemptsLeft > 0 {
				// Normal retries
				if !tt.shouldRetry {
					t.Error("Messages with attempts left should retry")
				}
			} else {
				// No retries left
				if tt.shouldRetry {
					t.Error("Messages with no attempts left should not retry")
				}
			}
		})
	}
}
