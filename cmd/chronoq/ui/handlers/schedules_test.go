package handlers

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schedule_pb "github.com/adrien19/chronoqueue/api/schedule/v1"
)

// TestParseExecutionTimes tests the execution time parsing logic
func TestParseExecutionTimes(t *testing.T) {
	handler := &SchedulesHandler{}

	tests := []struct {
		name          string
		input         string
		expectedLen   int
		expectedError bool
		expectedTimes []struct{ hour, minute int32 }
	}{
		{
			name:        "single valid time",
			input:       "09:30",
			expectedLen: 1,
			expectedTimes: []struct{ hour, minute int32 }{
				{9, 30},
			},
		},
		{
			name:        "multiple valid times",
			input:       "09:00,12:30,18:45",
			expectedLen: 3,
			expectedTimes: []struct{ hour, minute int32 }{
				{9, 0},
				{12, 30},
				{18, 45},
			},
		},
		{
			name:        "times with spaces",
			input:       " 08:00 , 14:30 , 20:00 ",
			expectedLen: 3,
			expectedTimes: []struct{ hour, minute int32 }{
				{8, 0},
				{14, 30},
				{20, 0},
			},
		},
		{
			name:        "empty string defaults to midnight",
			input:       "",
			expectedLen: 1,
			expectedTimes: []struct{ hour, minute int32 }{
				{0, 0},
			},
		},
		{
			name:        "midnight explicitly",
			input:       "00:00",
			expectedLen: 1,
			expectedTimes: []struct{ hour, minute int32 }{
				{0, 0},
			},
		},
		{
			name:          "invalid format - no colon",
			input:         "0900",
			expectedError: true,
		},
		{
			name:          "invalid hour - too high",
			input:         "25:00",
			expectedError: true,
		},
		{
			name:          "invalid hour - negative",
			input:         "-1:00",
			expectedError: true,
		},
		{
			name:          "invalid minute - too high",
			input:         "12:60",
			expectedError: true,
		},
		{
			name:          "invalid minute - negative",
			input:         "12:-5",
			expectedError: true,
		},
		{
			name:          "invalid format - letters",
			input:         "noon",
			expectedError: true,
		},
		{
			name:          "mixed valid and invalid",
			input:         "09:00,25:00",
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := handler.parseExecutionTimes(tt.input)

			if tt.expectedError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedLen, len(result))

			for i, expected := range tt.expectedTimes {
				assert.Equal(t, expected.hour, result[i].Hour, "hour mismatch at index %d", i)
				assert.Equal(t, expected.minute, result[i].Minute, "minute mismatch at index %d", i)
			}
		})
	}
}

// TestParseDaysOfWeek tests the days of week parsing logic
func TestParseDaysOfWeek(t *testing.T) {
	handler := &SchedulesHandler{}

	tests := []struct {
		name     string
		input    []string
		expected []int32
	}{
		{
			name:     "weekdays",
			input:    []string{"1", "2", "3", "4", "5"},
			expected: []int32{1, 2, 3, 4, 5},
		},
		{
			name:     "weekend",
			input:    []string{"6", "7"},
			expected: []int32{6, 7},
		},
		{
			name:     "single day",
			input:    []string{"3"},
			expected: []int32{3},
		},
		{
			name:     "empty defaults to weekdays",
			input:    []string{},
			expected: []int32{1, 2, 3, 4, 5},
		},
		{
			name:     "nil defaults to weekdays",
			input:    nil,
			expected: []int32{1, 2, 3, 4, 5},
		},
		{
			name:     "invalid entries are skipped",
			input:    []string{"1", "invalid", "3", "abc", "5"},
			expected: []int32{1, 3, 5},
		},
		{
			name:     "all invalid defaults to weekdays",
			input:    []string{"abc", "xyz"},
			expected: []int32{1, 2, 3, 4, 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.parseDaysOfWeek(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestParseDayOfMonth tests the day of month parsing logic
func TestParseDayOfMonth(t *testing.T) {
	handler := &SchedulesHandler{}

	tests := []struct {
		name     string
		input    string
		expected int32
	}{
		{
			name:     "valid day - first",
			input:    "1",
			expected: 1,
		},
		{
			name:     "valid day - middle",
			input:    "15",
			expected: 15,
		},
		{
			name:     "valid day - last",
			input:    "31",
			expected: 31,
		},
		{
			name:     "empty defaults to 1",
			input:    "",
			expected: 1,
		},
		{
			name:     "invalid text defaults to 1",
			input:    "abc",
			expected: 1,
		},
		{
			name:     "zero value defaults to 1",
			input:    "0",
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.parseDayOfMonth(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestMapCalendarTypeToEnum tests the calendar type string to enum mapping
func TestMapCalendarTypeToEnum(t *testing.T) {
	handler := &SchedulesHandler{}

	tests := []struct {
		name     string
		input    string
		expected schedule_pb.CalendarSchedule_ScheduleType
	}{
		{
			name:     "DAILY",
			input:    "DAILY",
			expected: schedule_pb.CalendarSchedule_DAILY,
		},
		{
			name:     "WEEKLY",
			input:    "WEEKLY",
			expected: schedule_pb.CalendarSchedule_WEEKLY,
		},
		{
			name:     "MONTHLY",
			input:    "MONTHLY",
			expected: schedule_pb.CalendarSchedule_MONTHLY,
		},
		{
			name:     "BUSINESS_DAYS",
			input:    "BUSINESS_DAYS",
			expected: schedule_pb.CalendarSchedule_BUSINESS_DAYS,
		},
		{
			name:     "invalid type defaults to DAILY",
			input:    "INVALID",
			expected: schedule_pb.CalendarSchedule_DAILY,
		},
		{
			name:     "empty defaults to DAILY",
			input:    "",
			expected: schedule_pb.CalendarSchedule_DAILY,
		},
		{
			name:     "lowercase daily defaults to DAILY",
			input:    "daily",
			expected: schedule_pb.CalendarSchedule_DAILY,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.mapCalendarTypeToEnum(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestBuildCalendarRule_Daily tests building a daily calendar rule
func TestBuildCalendarRule_Daily(t *testing.T) {
	handler := &SchedulesHandler{}

	form := url.Values{}
	form.Set("calendar_type", "DAILY")

	req := &http.Request{Form: form}

	executionTimes := []*schedule_pb.TimeOfDay{
		{Hour: 9, Minute: 0},
	}

	rule, err := handler.buildCalendarRule(req, "DAILY", executionTimes)

	require.NoError(t, err)
	require.NotNil(t, rule)
	assert.NotNil(t, rule.GetDaily())
	assert.Equal(t, int32(1), rule.GetDaily().DayInterval)
	assert.Equal(t, executionTimes, rule.ExecutionTimes)
}

// TestBuildCalendarRule_Weekly tests building a weekly calendar rule
func TestBuildCalendarRule_Weekly(t *testing.T) {
	handler := &SchedulesHandler{}

	form := url.Values{}
	form["days_of_week"] = []string{"1", "2", "3", "4", "5"}

	req := &http.Request{Form: form}

	executionTimes := []*schedule_pb.TimeOfDay{
		{Hour: 9, Minute: 0},
		{Hour: 17, Minute: 0},
	}

	rule, err := handler.buildCalendarRule(req, "WEEKLY", executionTimes)

	require.NoError(t, err)
	require.NotNil(t, rule)
	assert.NotNil(t, rule.GetWeekly())
	assert.Equal(t, []int32{1, 2, 3, 4, 5}, rule.GetWeekly().DaysOfWeek)
	assert.Equal(t, int32(1), rule.GetWeekly().WeekInterval)
	assert.Equal(t, executionTimes, rule.ExecutionTimes)
}

// TestBuildCalendarRule_Monthly tests building a monthly calendar rule
func TestBuildCalendarRule_Monthly(t *testing.T) {
	handler := &SchedulesHandler{}

	form := url.Values{}
	form.Set("day_of_month", "15")

	req := &http.Request{Form: form}

	executionTimes := []*schedule_pb.TimeOfDay{
		{Hour: 12, Minute: 0},
	}

	rule, err := handler.buildCalendarRule(req, "MONTHLY", executionTimes)

	require.NoError(t, err)
	require.NotNil(t, rule)
	assert.NotNil(t, rule.GetMonthly())
	assert.Equal(t, int32(15), rule.GetMonthly().DayValue)
	assert.Equal(t, schedule_pb.MonthlyRule_DAY_OF_MONTH, rule.GetMonthly().DayType)
	assert.Equal(t, executionTimes, rule.ExecutionTimes)
}

// TestBuildCalendarRule_BusinessDays tests building a business days calendar rule
func TestBuildCalendarRule_BusinessDays(t *testing.T) {
	handler := &SchedulesHandler{}

	form := url.Values{}
	req := &http.Request{Form: form}

	executionTimes := []*schedule_pb.TimeOfDay{
		{Hour: 8, Minute: 30},
	}

	rule, err := handler.buildCalendarRule(req, "BUSINESS_DAYS", executionTimes)

	require.NoError(t, err)
	require.NotNil(t, rule)
	assert.NotNil(t, rule.GetBusinessDays())
	assert.Equal(t, executionTimes, rule.ExecutionTimes)
}

// TestBuildCalendarRule_Invalid tests building with invalid calendar type
func TestBuildCalendarRule_Invalid(t *testing.T) {
	handler := &SchedulesHandler{}

	form := url.Values{}
	req := &http.Request{Form: form}

	executionTimes := []*schedule_pb.TimeOfDay{
		{Hour: 9, Minute: 0},
	}

	rule, err := handler.buildCalendarRule(req, "INVALID_TYPE", executionTimes)

	assert.Error(t, err)
	assert.Nil(t, rule)
	assert.Contains(t, err.Error(), "invalid calendar type")
}

// TestParseExecutionTimes_EdgeCases tests edge cases in time parsing
func TestParseExecutionTimes_EdgeCases(t *testing.T) {
	handler := &SchedulesHandler{}

	tests := []struct {
		name          string
		input         string
		expectedError bool
		description   string
	}{
		{
			name:          "boundary - 23:59",
			input:         "23:59",
			expectedError: false,
			description:   "Last valid minute of the day",
		},
		{
			name:          "boundary - 00:00",
			input:         "00:00",
			expectedError: false,
			description:   "First minute of the day",
		},
		{
			name:          "invalid - extra colon",
			input:         "12:30:45",
			expectedError: true,
			description:   "Time with seconds not supported",
		},
		{
			name:          "invalid - missing minute",
			input:         "12:",
			expectedError: true,
			description:   "Incomplete time format",
		},
		{
			name:          "invalid - missing hour",
			input:         ":30",
			expectedError: true,
			description:   "Incomplete time format",
		},
		{
			name:          "multiple times with one invalid",
			input:         "09:00,25:00,18:00",
			expectedError: true,
			description:   "Should fail if any time is invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := handler.parseExecutionTimes(tt.input)
			if tt.expectedError {
				assert.Error(t, err, tt.description)
			} else {
				assert.NoError(t, err, tt.description)
			}
		})
	}
}

// TestNewSchedulesHandler tests handler creation
func TestNewSchedulesHandler(t *testing.T) {
	handler := NewSchedulesHandler(nil, nil, nil)

	require.NotNil(t, handler)
	assert.IsType(t, &SchedulesHandler{}, handler)
}
