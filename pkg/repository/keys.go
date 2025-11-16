package repository

import (
	"fmt"
	"net/url"
)

// Key generation helpers for Redis keys.
// All keys follow the format: chronoqueue:<entity_type>:<identifier>
// Message keys include :msg: identifier for easy filtering.

// queueKey returns the key for a queue sorted set (legacy minimal usage).
func (as *storage) queueKey(queueName string) string {
	return fmt.Sprintf("chronoqueue:queue:%s", urlEncode(queueName))
}

// queueMetaKey returns the key for queue metadata hash.
func (as *storage) queueMetaKey(queueName string) string {
	return fmt.Sprintf("chronoqueue:queue:%s:meta", urlEncode(queueName))
}

// messageMetaKey returns the key for message metadata hash.
// Includes :msg: identifier for easy message key identification.
func (as *storage) messageMetaKey(queueName, messageID string) string {
	return fmt.Sprintf("chronoqueue:%s:msg:%s:meta", urlEncode(queueName), urlEncode(messageID))
}

// scheduleMetaKey returns the key for schedule metadata hash.
func (as *storage) scheduleMetaKey(scheduleID string) string {
	return fmt.Sprintf("chronoqueue:schedule:%s:meta", urlEncode(scheduleID))
}

// scheduleSetKey returns the key for calendar schedule execution history tracking.
// This is ONLY used for calendar schedules to track which messages have been scheduled.
// This is NOT processed by the scheduler - it's for history/audit purposes.
// Do not confuse with scheduleKey() in streams.go which is for queue-level message scheduling.
func (as *storage) scheduleSetKey(scheduleID string) string {
	return fmt.Sprintf("chronoqueue:schedule:%s:set", urlEncode(scheduleID))
}

// statsKey returns the key for queue statistics hash.
func (as *storage) statsKey(queueName string) string {
	return fmt.Sprintf("chronoqueue:stats:%s", urlEncode(queueName))
}

// urlEncode encodes a string to handle special characters like : in queue/message names.
func urlEncode(s string) string {
	return url.QueryEscape(s)
}

// urlDecode decodes a URL-encoded string.
func urlDecode(s string) (string, error) {
	return url.QueryUnescape(s)
}
