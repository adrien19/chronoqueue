package evaluators

import (
	"context"
	"fmt"
	"time"

	schedule "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/pkg/calendar/types"
)

// WeeklyEvaluator handles weekly calendar rules
type WeeklyEvaluator struct{}

// NewWeeklyEvaluator creates a new weekly rule evaluator
func NewWeeklyEvaluator() *WeeklyEvaluator {
	return &WeeklyEvaluator{}
}

// Evaluate returns the next execution time for a weekly rule after the given time
func (e *WeeklyEvaluator) Evaluate(ctx context.Context, rule *schedule.CalendarRule, from time.Time, timezone *time.Location) (*time.Time, error) {
	weeklyRule := rule.GetWeekly()
	if weeklyRule == nil {
		return nil, types.ErrInvalidRule.WithDetails("weekly rule is nil")
	}

	// Convert from time to the specified timezone
	fromLocal := from.In(timezone)

	// Get execution times for this rule
	executionTimes := rule.ExecutionTimes
	if len(executionTimes) == 0 {
		// Default to midnight if no execution times specified
		executionTimes = []*schedule.TimeOfDay{{Hour: 0, Minute: 0, Second: 0}}
	}

	// Check if rule is valid for the current time range
	if rule.ValidFrom != nil && from.Before(rule.ValidFrom.AsTime()) {
		fromLocal = rule.ValidFrom.AsTime().In(timezone)
	}
	if rule.ValidUntil != nil && from.After(rule.ValidUntil.AsTime()) {
		return nil, types.ErrNoExecutionTime.WithDetails("rule validity period has expired")
	}

	// Get week interval (default to 1)
	weekInterval := int(weeklyRule.WeekInterval)
	if weekInterval <= 0 {
		weekInterval = 1
	}

	// Get reference week for interval calculations
	var referenceWeek time.Time
	if weeklyRule.StartWeek != nil {
		referenceWeek = weeklyRule.StartWeek.AsTime().In(timezone)
	} else {
		// Use current week as reference
		referenceWeek = fromLocal
	}

	// Get days of week to execute (1=Monday, 7=Sunday)
	daysOfWeek := weeklyRule.DaysOfWeek
	if len(daysOfWeek) == 0 {
		return nil, types.ErrInvalidRule.WithDetails("no days of week specified")
	}

	return e.findNextExecution(fromLocal, daysOfWeek, weekInterval, referenceWeek, executionTimes)
}

// EvaluateMultiple returns the next N execution times for a weekly rule
func (e *WeeklyEvaluator) EvaluateMultiple(ctx context.Context, rule *schedule.CalendarRule, from time.Time, timezone *time.Location, count int) ([]time.Time, error) {
	var results []time.Time
	current := from

	for len(results) < count {
		next, err := e.Evaluate(ctx, rule, current, timezone)
		if err != nil {
			if err == types.ErrNoExecutionTime {
				break // No more execution times available
			}
			return nil, err
		}
		if next == nil {
			break
		}

		results = append(results, *next)
		current = next.Add(time.Minute) // Move past this execution time
	}

	return results, nil
}

// Validate checks if a weekly rule is valid
func (e *WeeklyEvaluator) Validate(ctx context.Context, rule *schedule.CalendarRule) error {
	weeklyRule := rule.GetWeekly()
	if weeklyRule == nil {
		return types.ErrInvalidRule.WithDetails("weekly rule is nil")
	}

	// Validate days of week
	if len(weeklyRule.DaysOfWeek) == 0 {
		return types.ErrInvalidRule.WithDetails("no days of week specified")
	}

	for _, day := range weeklyRule.DaysOfWeek {
		if day < 1 || day > 7 {
			return types.ErrInvalidRule.WithDetails(fmt.Sprintf("invalid day of week: %d (must be 1-7)", day))
		}
	}

	// Validate week interval
	if weeklyRule.WeekInterval < 0 {
		return types.ErrInvalidRule.WithDetails("week interval cannot be negative")
	}

	// Validate execution times
	for _, execTime := range rule.ExecutionTimes {
		if err := validateTimeOfDay(execTime); err != nil {
			return err
		}
	}

	return nil
}

// GetRuleType returns the type of rule this evaluator handles
func (e *WeeklyEvaluator) GetRuleType() types.RuleType {
	return types.RuleTypeWeekly
}

// findNextExecution finds the next execution time based on weekly rule parameters
func (e *WeeklyEvaluator) findNextExecution(from time.Time, daysOfWeek []int32, weekInterval int, referenceWeek time.Time, executionTimes []*schedule.TimeOfDay) (*time.Time, error) {
	// Convert Go weekday to our convention (1=Monday, 7=Sunday)
	goWeekdayToOurs := func(wd time.Weekday) int32 {
		if wd == time.Sunday {
			return 7
		}
		return int32(wd)
	}

	// Convert our weekday to Go weekday (currently unused but kept for potential future use)
	_ = func(day int32) time.Weekday {
		if day == 7 {
			return time.Sunday
		}
		return time.Weekday(day)
	}

	// Start from the current date
	currentDate := from.Truncate(24 * time.Hour) // Start of day
	maxAttempts := 14 * weekInterval             // Search for 14 intervals maximum

	for attempts := 0; attempts < maxAttempts; attempts++ {
		// Check if current date is in a valid week according to interval
		if e.isValidWeek(currentDate, referenceWeek, weekInterval) {
			currentWeekday := goWeekdayToOurs(currentDate.Weekday())

			// Check if current weekday is in the list of valid days
			for _, validDay := range daysOfWeek {
				if currentWeekday == validDay {
					// Check all execution times for this day
					for _, execTime := range executionTimes {
						candidate := time.Date(
							currentDate.Year(), currentDate.Month(), currentDate.Day(),
							int(execTime.Hour), int(execTime.Minute), int(execTime.Second),
							0, currentDate.Location(),
						)

						if candidate.After(from) {
							return &candidate, nil
						}
					}
				}
			}
		}

		// Move to next day
		currentDate = currentDate.Add(24 * time.Hour)
	}

	return nil, types.ErrNoExecutionTime.WithDetails("no valid execution time found within search window")
}

// isValidWeek checks if the given date falls in a valid week according to the interval
func (e *WeeklyEvaluator) isValidWeek(date, referenceWeek time.Time, weekInterval int) bool {
	if weekInterval <= 1 {
		return true // Every week is valid
	}

	// Calculate the start of the week for both dates (Monday)
	dateWeekStart := e.getWeekStart(date)
	refWeekStart := e.getWeekStart(referenceWeek)

	// Calculate the difference in weeks
	weeksDiff := int(dateWeekStart.Sub(refWeekStart).Hours() / (24 * 7))

	// Check if this week falls on the interval
	return weeksDiff%weekInterval == 0
}

// getWeekStart returns the start of the week (Monday) for the given date
func (e *WeeklyEvaluator) getWeekStart(date time.Time) time.Time {
	weekday := date.Weekday()
	daysToSubtract := int(weekday - time.Monday)
	if daysToSubtract < 0 {
		daysToSubtract += 7
	}

	weekStart := date.Add(-time.Duration(daysToSubtract) * 24 * time.Hour)
	return time.Date(weekStart.Year(), weekStart.Month(), weekStart.Day(), 0, 0, 0, 0, date.Location())
}
