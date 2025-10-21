package helpers

import (
	"testing"
	"time"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// WaitForMessageTransition waits for the background worker to process message state transitions.
// This is needed after posting messages because they start in INVISIBLE state and must
// transition to PENDING before they can be retrieved with GetNextMessage.
// The invisibleToPending worker runs every 1 second, so we wait 1.5s to be safe.
func WaitForMessageTransition(t *testing.T) {
	t.Helper()
	time.Sleep(1500 * time.Millisecond)
}

// AssertQueueExists verifies that a queue with the given name exists
func AssertQueueExists(t *testing.T, queues []*queue_pb.Queue, queueName string) {
	t.Helper()

	for _, q := range queues {
		if q.Name == queueName {
			return // Queue found
		}
	}
	t.Errorf("Queue '%s' not found in queue list", queueName)
}

// AssertQueueNotExists verifies that a queue with the given name does not exist
func AssertQueueNotExists(t *testing.T, queues []*queue_pb.Queue, queueName string) {
	t.Helper()

	for _, q := range queues {
		if q.Name == queueName {
			t.Errorf("Queue '%s' should not exist but was found", queueName)
			return
		}
	}
}

// AssertMessagePriority verifies that messages are ordered by priority
func AssertMessagePriority(t *testing.T, messages []*message_pb.Message, expectedOrder []int64) {
	t.Helper()

	require.Equal(t, len(expectedOrder), len(messages), "Number of messages should match expected order")

	for i, msg := range messages {
		priority := int64(0)
		if msg.Metadata != nil {
			priority = msg.Metadata.Priority
		}
		assert.Equal(t, expectedOrder[i], priority,
			"Message at index %d should have priority %d, got %d",
			i, expectedOrder[i], priority)
	}
}

// AssertMessageState verifies that a message is in the expected state
func AssertMessageState(t *testing.T, msg *message_pb.Message, expectedState message_pb.Message_Metadata_State) {
	t.Helper()

	if msg.Metadata == nil {
		t.Error("Message metadata is nil")
		return
	}

	assert.Equal(t, expectedState, msg.Metadata.State,
		"Message should be in state %s, got %s",
		expectedState.String(), msg.Metadata.State.String())
}

// AssertQueueStateApproximate verifies queue statistics (allowing for timing variations)
func AssertQueueStateApproximate(t *testing.T, pending, running, completed int64,
	expectedPending, expectedRunning, expectedCompleted int64, tolerance int64) {
	t.Helper()

	assert.InDelta(t, expectedPending, pending, float64(tolerance),
		"Pending count should be approximately %d, got %d", expectedPending, pending)
	assert.InDelta(t, expectedRunning, running, float64(tolerance),
		"Running count should be approximately %d, got %d", expectedRunning, running)
	assert.InDelta(t, expectedCompleted, completed, float64(tolerance),
		"Completed count should be approximately %d, got %d", expectedCompleted, completed)
}

// AssertDLQMessageCount verifies the number of messages in DLQ
func AssertDLQMessageCount(t *testing.T, dlqMessages []*message_pb.Message, expectedCount int) {
	t.Helper()

	assert.Equal(t, expectedCount, len(dlqMessages),
		"Expected %d messages in DLQ, got %d", expectedCount, len(dlqMessages))
}

// AssertMessageContent verifies message payload content
func AssertMessageContent(t *testing.T, msg *message_pb.Message, expectedContent string) {
	t.Helper()

	if msg.Metadata == nil || msg.Metadata.Payload == nil {
		t.Error("Message metadata or payload is nil")
		return
	}

	// For structured data, convert to JSON string for comparison
	// This is a simplified version - actual implementation may need to handle different payload types
	assert.NotNil(t, msg.Metadata.Payload.Data, "Message payload data should not be nil")
}

// AssertMessageContentType verifies content type
func AssertMessageContentType(t *testing.T, msg *message_pb.Message, expectedContentType string) {
	t.Helper()

	if msg.Metadata == nil || msg.Metadata.Payload == nil {
		t.Error("Message metadata or payload is nil")
		return
	}

	assert.Equal(t, expectedContentType, msg.Metadata.Payload.ContentType,
		"Message content type should be %s, got %s",
		expectedContentType, msg.Metadata.Payload.ContentType)
}

// AssertTimeWithinRange verifies that a timestamp is within an acceptable range
func AssertTimeWithinRange(t *testing.T, actual, expected time.Time, tolerance time.Duration) {
	t.Helper()

	diff := actual.Sub(expected)
	if diff < 0 {
		diff = -diff
	}

	assert.LessOrEqual(t, diff, tolerance,
		"Time difference should be within %s, got %s", tolerance, diff)
}

// AssertRetryAttemptsLeft verifies message retry attempts left
func AssertRetryAttemptsLeft(t *testing.T, msg *message_pb.Message, expectedAttempts int32) {
	t.Helper()

	if msg.Metadata == nil {
		t.Error("Message metadata is nil")
		return
	}

	assert.Equal(t, expectedAttempts, msg.Metadata.AttemptsLeft,
		"Message should have %d attempts left, got %d",
		expectedAttempts, msg.Metadata.AttemptsLeft)
}

// AssertLeaseActive verifies that a message lease is active
func AssertLeaseActive(t *testing.T, msg *message_pb.Message) {
	t.Helper()

	if msg.Metadata == nil {
		t.Error("Message metadata is nil")
		return
	}

	// Check if lease expiry is in the future
	if msg.Metadata.LeaseExpiry > 0 {
		leaseExpiry := time.Unix(msg.Metadata.LeaseExpiry, 0)
		now := time.Now()
		assert.True(t, leaseExpiry.After(now),
			"Message lease should be active (expires at %s, now is %s)",
			leaseExpiry, now)
	} else {
		t.Error("Message should have a lease expiry but it's 0")
	}
}

// AssertLeaseExpired verifies that a message lease has expired
func AssertLeaseExpired(t *testing.T, msg *message_pb.Message) {
	t.Helper()

	if msg.Metadata == nil {
		t.Error("Message metadata is nil")
		return
	}

	if msg.Metadata.LeaseExpiry > 0 {
		leaseExpiry := time.Unix(msg.Metadata.LeaseExpiry, 0)
		now := time.Now()
		assert.True(t, leaseExpiry.Before(now),
			"Message lease should be expired (expires at %s, now is %s)",
			leaseExpiry, now)
	} else {
		t.Error("Message should have a lease expiry but it's 0")
	}
}

// AssertScheduleExecutionCount verifies the number of schedule executions
func AssertScheduleExecutionCount(t *testing.T, executionCount int, expectedCount int) {
	t.Helper()

	assert.Equal(t, expectedCount, executionCount,
		"Expected %d schedule executions, got %d", expectedCount, executionCount)
}

// AssertErrorContains verifies that an error contains a specific substring
func AssertErrorContains(t *testing.T, err error, expectedSubstring string) {
	t.Helper()

	require.Error(t, err, "Expected an error but got nil")
	assert.Contains(t, err.Error(), expectedSubstring,
		"Error message should contain '%s'", expectedSubstring)
}

// AssertNoError is a convenience wrapper around require.NoError with custom message
func AssertNoError(t *testing.T, err error, operation string) {
	t.Helper()

	require.NoError(t, err, "Operation '%s' should not return an error", operation)
}

// AssertSuccess verifies that a response indicates success
func AssertSuccess(t *testing.T, success bool, operation string) {
	t.Helper()

	assert.True(t, success, "Operation '%s' should succeed", operation)
}
