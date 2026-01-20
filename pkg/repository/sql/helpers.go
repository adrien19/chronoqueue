package sql

import (
	"time"
)

// Clock provides timestamp operations for consistency across storage layer
type Clock struct{}

// NewClock creates a new Clock instance
func NewClock() *Clock {
	return &Clock{}
}

// NowMs returns the current Unix timestamp in milliseconds
func (c *Clock) NowMs() int64 {
	return time.Now().UnixMilli()
}

// Now returns the current time
func (c *Clock) Now() time.Time {
	return time.Now()
}

// ToMs converts a time.Time to Unix milliseconds
func (c *Clock) ToMs(t time.Time) int64 {
	return t.UnixMilli()
}

// FromMs converts Unix milliseconds to time.Time
func (c *Clock) FromMs(ms int64) time.Time {
	return time.UnixMilli(ms)
}
