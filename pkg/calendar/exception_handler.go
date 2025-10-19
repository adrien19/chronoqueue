package calendar

import (
	"context"
	"fmt"
	"sort"
	"time"

	schedule "github.com/adrien19/chronoqueue/api/schedule/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// DefaultExceptionHandler is the default implementation of ExceptionHandler
type DefaultExceptionHandler struct{}

// NewDefaultExceptionHandler creates a new default exception handler
func NewDefaultExceptionHandler() *DefaultExceptionHandler {
	return &DefaultExceptionHandler{}
}

// ApplyExceptions applies calendar exceptions to a list of execution times
func (h *DefaultExceptionHandler) ApplyExceptions(ctx context.Context, times []time.Time, exceptions []*schedule.CalendarException, timezone *time.Location) ([]time.Time, error) {
	if len(exceptions) == 0 {
		return times, nil
	}

	// Create a map of exception dates for quick lookup
	exceptionMap := make(map[string]*schedule.CalendarException)
	for _, exception := range exceptions {
		dateKey := h.getDateKey(exception.Date.AsTime().In(timezone))
		exceptionMap[dateKey] = exception
	}

	var result []time.Time
	var extraTimes []time.Time

	// Process each execution time
	for _, execTime := range times {
		execTimeLocal := execTime.In(timezone)
		dateKey := h.getDateKey(execTimeLocal)

		if exception, hasException := exceptionMap[dateKey]; hasException {
			switch exception.Type {
			case schedule.CalendarException_SKIP:
				// Skip this execution time
				continue

			case schedule.CalendarException_RESCHEDULE:
				// Reschedule to a different time
				if exception.RescheduleTo != nil {
					rescheduledTime := exception.RescheduleTo.AsTime()

					// Preserve the original time of day if not specified in reschedule time
					if h.isDateOnly(exception.RescheduleTo.AsTime()) {
						originalTime := execTimeLocal
						rescheduledTime = time.Date(
							rescheduledTime.Year(), rescheduledTime.Month(), rescheduledTime.Day(),
							originalTime.Hour(), originalTime.Minute(), originalTime.Second(),
							originalTime.Nanosecond(), timezone)
					}

					result = append(result, rescheduledTime)
				}
				// If no reschedule time specified, effectively skip

			case schedule.CalendarException_EXTRA:
				// Keep the original time and add extra times
				result = append(result, execTime)

				// Add extra execution times
				for _, extraTimeOfDay := range exception.ExtraTimes {
					extraTime := time.Date(
						execTimeLocal.Year(), execTimeLocal.Month(), execTimeLocal.Day(),
						int(extraTimeOfDay.Hour), int(extraTimeOfDay.Minute), int(extraTimeOfDay.Second),
						0, timezone)
					extraTimes = append(extraTimes, extraTime)
				}

			default:
				// Unknown exception type, keep original time
				result = append(result, execTime)
			}
		} else {
			// No exception for this date, keep original time
			result = append(result, execTime)
		}
	}

	// Add all extra times
	result = append(result, extraTimes...)

	// Sort the final result
	sort.Slice(result, func(i, j int) bool {
		return result[i].Before(result[j])
	})

	return result, nil
}

// ValidateExceptions validates a list of calendar exceptions
func (h *DefaultExceptionHandler) ValidateExceptions(ctx context.Context, exceptions []*schedule.CalendarException) error {
	for i, exception := range exceptions {
		if err := h.validateException(exception); err != nil {
			return fmt.Errorf("exception %d validation failed: %w", i, err)
		}
	}

	return nil
}

// validateException validates a single calendar exception
func (h *DefaultExceptionHandler) validateException(exception *schedule.CalendarException) error {
	if exception == nil {
		return fmt.Errorf("exception cannot be nil")
	}

	// Validate date
	if exception.Date == nil {
		return fmt.Errorf("exception date is required")
	}

	// Validate type-specific requirements
	switch exception.Type {
	case schedule.CalendarException_SKIP:
		// No additional validation needed for SKIP

	case schedule.CalendarException_RESCHEDULE:
		// RESCHEDULE requires a reschedule_to date
		if exception.RescheduleTo == nil {
			return fmt.Errorf("reschedule_to is required for RESCHEDULE exception")
		}

	case schedule.CalendarException_EXTRA:
		// EXTRA requires at least one extra time
		if len(exception.ExtraTimes) == 0 {
			return fmt.Errorf("extra_times is required for EXTRA exception")
		}

		// Validate each extra time
		for j, extraTime := range exception.ExtraTimes {
			if err := h.validateTimeOfDay(extraTime); err != nil {
				return fmt.Errorf("extra_time %d validation failed: %w", j, err)
			}
		}

	default:
		return fmt.Errorf("unknown exception type: %v", exception.Type)
	}

	return nil
}

// validateTimeOfDay validates a time of day specification
func (h *DefaultExceptionHandler) validateTimeOfDay(timeOfDay *schedule.TimeOfDay) error {
	if timeOfDay == nil {
		return fmt.Errorf("time of day cannot be nil")
	}

	if timeOfDay.Hour < 0 || timeOfDay.Hour > 23 {
		return fmt.Errorf("invalid hour: %d (must be 0-23)", timeOfDay.Hour)
	}

	if timeOfDay.Minute < 0 || timeOfDay.Minute > 59 {
		return fmt.Errorf("invalid minute: %d (must be 0-59)", timeOfDay.Minute)
	}

	if timeOfDay.Second < 0 || timeOfDay.Second > 59 {
		return fmt.Errorf("invalid second: %d (must be 0-59)", timeOfDay.Second)
	}

	return nil
}

// Helper methods

// getDateKey returns a string key for a date (without time)
func (h *DefaultExceptionHandler) getDateKey(t time.Time) string {
	return t.Format("2006-01-02")
}

// isDateOnly checks if a time represents a date only (time components are zero)
func (h *DefaultExceptionHandler) isDateOnly(t time.Time) bool {
	return t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0
}

// CreateSkipException creates a SKIP exception for the given date
func CreateSkipException(date time.Time, reason string) *schedule.CalendarException {
	return &schedule.CalendarException{
		Date:   timestampFromTime(date),
		Type:   schedule.CalendarException_SKIP,
		Reason: reason,
	}
}

// CreateRescheduleException creates a RESCHEDULE exception
func CreateRescheduleException(fromDate, toDate time.Time, reason string) *schedule.CalendarException {
	return &schedule.CalendarException{
		Date:         timestampFromTime(fromDate),
		Type:         schedule.CalendarException_RESCHEDULE,
		RescheduleTo: timestampFromTime(toDate),
		Reason:       reason,
	}
}

// CreateExtraException creates an EXTRA exception with additional execution times
func CreateExtraException(date time.Time, extraTimes []*schedule.TimeOfDay, reason string) *schedule.CalendarException {
	return &schedule.CalendarException{
		Date:       timestampFromTime(date),
		Type:       schedule.CalendarException_EXTRA,
		ExtraTimes: extraTimes,
		Reason:     reason,
	}
}

// Utility functions for working with exceptions

// GetExceptionsForDateRange returns exceptions that fall within a date range
func (h *DefaultExceptionHandler) GetExceptionsForDateRange(exceptions []*schedule.CalendarException, from, to time.Time) []*schedule.CalendarException {
	var result []*schedule.CalendarException

	for _, exception := range exceptions {
		exceptionDate := exception.Date.AsTime()
		if !exceptionDate.Before(from) && !exceptionDate.After(to) {
			result = append(result, exception)
		}
	}

	return result
}

// GetExceptionsByType returns exceptions of a specific type
func (h *DefaultExceptionHandler) GetExceptionsByType(exceptions []*schedule.CalendarException, exceptionType schedule.CalendarException_ExceptionType) []*schedule.CalendarException {
	var result []*schedule.CalendarException

	for _, exception := range exceptions {
		if exception.Type == exceptionType {
			result = append(result, exception)
		}
	}

	return result
}

// HasExceptionOnDate checks if there's an exception on a specific date
func (h *DefaultExceptionHandler) HasExceptionOnDate(exceptions []*schedule.CalendarException, date time.Time) bool {
	dateKey := h.getDateKey(date)

	for _, exception := range exceptions {
		exceptionDateKey := h.getDateKey(exception.Date.AsTime())
		if exceptionDateKey == dateKey {
			return true
		}
	}

	return false
}

// GetExceptionOnDate returns the exception for a specific date, if any
func (h *DefaultExceptionHandler) GetExceptionOnDate(exceptions []*schedule.CalendarException, date time.Time) *schedule.CalendarException {
	dateKey := h.getDateKey(date)

	for _, exception := range exceptions {
		exceptionDateKey := h.getDateKey(exception.Date.AsTime())
		if exceptionDateKey == dateKey {
			return exception
		}
	}

	return nil
}

// CountExceptionsByType returns the count of exceptions by type
func (h *DefaultExceptionHandler) CountExceptionsByType(exceptions []*schedule.CalendarException) map[schedule.CalendarException_ExceptionType]int {
	counts := make(map[schedule.CalendarException_ExceptionType]int)

	for _, exception := range exceptions {
		counts[exception.Type]++
	}

	return counts
}

// Helper function to create timestamp from time.Time
func timestampFromTime(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}
