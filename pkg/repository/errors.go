package repository

import "fmt"

// QueueNotFoundError is returned when a queue does not exist
type QueueNotFoundError struct {
	QueueName string
}

func (e *QueueNotFoundError) Error() string {
	return fmt.Sprintf("queue not found: %s", e.QueueName)
}

// MessageNotFoundError is returned when a message does not exist
type MessageNotFoundError struct {
	MessageID string
	QueueName string
}

func (e *MessageNotFoundError) Error() string {
	if e.QueueName != "" {
		return fmt.Sprintf("message %s not found in queue %s", e.MessageID, e.QueueName)
	}
	return fmt.Sprintf("message not found: %s", e.MessageID)
}

// AttemptMismatchError is returned when the attempt ID doesn't match
type AttemptMismatchError struct {
	MessageID       string
	QueueName       string
	ExpectedAttempt string
	ActualAttempt   string
}

func (e *AttemptMismatchError) Error() string {
	return fmt.Sprintf("attempt mismatch for message %s in queue %s: expected %s, got %s",
		e.MessageID, e.QueueName, e.ExpectedAttempt, e.ActualAttempt)
}

// ScheduleNotFoundError is returned when a schedule does not exist
type ScheduleNotFoundError struct {
	ScheduleID string
	QueueName  string
}

func (e *ScheduleNotFoundError) Error() string {
	if e.QueueName != "" {
		return fmt.Sprintf("schedule %s not found in queue %s", e.ScheduleID, e.QueueName)
	}
	return fmt.Sprintf("schedule not found: %s", e.ScheduleID)
}

// InvalidStateTransitionError is returned when a state transition is invalid
type InvalidStateTransitionError struct {
	Entity       string // "message", "queue", "schedule"
	ID           string
	CurrentState string
	TargetState  string
}

func (e *InvalidStateTransitionError) Error() string {
	return fmt.Sprintf("invalid state transition for %s %s: cannot transition from %s to %s",
		e.Entity, e.ID, e.CurrentState, e.TargetState)
}
