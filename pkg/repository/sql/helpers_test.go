package sql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClock_Now(t *testing.T) {
	clock := NewClock()

	now := clock.Now()
	assert.NotZero(t, now)

	// Should be recent (within last minute)
	assert.True(t, time.Since(now) < time.Minute)
}

func TestClock_NowMs(t *testing.T) {
	clock := NewClock()

	nowMs := clock.NowMs()
	assert.Greater(t, nowMs, int64(0))

	// Should be a reasonable timestamp (after year 2020)
	assert.Greater(t, nowMs, int64(1577836800000)) // Jan 1, 2020 in milliseconds
}

func TestClock_ToMs(t *testing.T) {
	clock := NewClock()

	testTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	expectedMs := testTime.UnixMilli()

	result := clock.ToMs(testTime)
	assert.Equal(t, expectedMs, result)
}

func TestClock_FromMs(t *testing.T) {
	clock := NewClock()

	testMs := int64(1704067200000) // Jan 1, 2024, 00:00:00 UTC
	expectedTime := time.UnixMilli(testMs)

	result := clock.FromMs(testMs)
	assert.Equal(t, expectedTime.Unix(), result.Unix())
}

func TestClock_RoundTrip(t *testing.T) {
	clock := NewClock()

	originalTime := time.Now()
	ms := clock.ToMs(originalTime)
	reconstructed := clock.FromMs(ms)

	// Should be equal to millisecond precision
	assert.Equal(t, originalTime.Unix(), reconstructed.Unix())
}
