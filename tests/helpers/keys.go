package helpers

import (
	"fmt"
	"net/url"
)

// Test helper functions for generating Redis keys that match the production key format.
// These helpers ensure tests use the same key naming convention as the application.

// QueueKey returns the key for a queue sorted set.
func QueueKey(queueName string) string {
	return fmt.Sprintf("chronoqueue:queue:%s", urlEncode(queueName))
}

// QueueMetaKey returns the key for queue metadata hash.
func QueueMetaKey(queueName string) string {
	return fmt.Sprintf("chronoqueue:queue:%s:meta", urlEncode(queueName))
}

// MessageMetaKey returns the key for message metadata hash.
func MessageMetaKey(queueName, messageID string) string {
	return fmt.Sprintf("chronoqueue:%s:msg:%s:meta", urlEncode(queueName), urlEncode(messageID))
}

// StreamKey returns the key for a priority-based message stream.
func StreamKey(queueName, priority string) string {
	return fmt.Sprintf("chronoqueue:stream:%s:%s", priority, urlEncode(queueName))
}

// ScheduleKey returns the key for scheduled messages sorted set.
func ScheduleKey(queueName string) string {
	return fmt.Sprintf("chronoqueue:schedule:%s", urlEncode(queueName))
}

// ScheduleMetaKey returns the key for schedule metadata hash.
func ScheduleMetaKey(scheduleID string) string {
	return fmt.Sprintf("chronoqueue:schedule:%s:meta", urlEncode(scheduleID))
}

// ScheduleSetKey returns the key for schedule tracking sorted set.
func ScheduleSetKey(scheduleID string) string {
	return fmt.Sprintf("chronoqueue:schedule:%s:set", urlEncode(scheduleID))
}

// DLQStreamKey returns the key for dead letter queue stream.
func DLQStreamKey(queueName string) string {
	return fmt.Sprintf("chronoqueue:dlq:%s", urlEncode(queueName))
}

// GroupKey returns the key for consumer group identifier.
func GroupKey(queueName string) string {
	return fmt.Sprintf("chronoqueue:cg:%s", urlEncode(queueName))
}

// StatsKey returns the key for queue statistics hash.
func StatsKey(queueName string) string {
	return fmt.Sprintf("chronoqueue:stats:%s", urlEncode(queueName))
}

// urlEncode encodes a string to handle special characters in queue/message names.
func urlEncode(s string) string {
	return url.QueryEscape(s)
}
