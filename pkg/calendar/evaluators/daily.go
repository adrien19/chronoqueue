package evaluators

import (
	"context"
	"time"

	schedule "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/pkg/calendar/types"
)

// DailyEvaluator handles daily calendar rules
type DailyEvaluator struct{}

// NewDailyEvaluator creates a new daily rule evaluator
func NewDailyEvaluator() *DailyEvaluator {
	return &DailyEvaluator{}
}

// Evaluate returns the next execution time for a daily rule after the given time
func (e *DailyEvaluator) Evaluate(ctx context.Context, rule *schedule.CalendarRule, from time.Time, timezone *time.Location) (*time.Time, error) {
	dailyRule := rule.GetDaily()
	if dailyRule == nil {
		return nil, types.ErrInvalidRule.WithDetails("daily rule is nil")
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

	// Get day interval (default to 1)
	dayInterval := int(dailyRule.DayInterval)
	if dayInterval <= 0 {
		dayInterval = 1
	}

	// Get reference date for interval calculations
	var referenceDate time.Time
	if dailyRule.StartDate != nil {
		referenceDate = dailyRule.StartDate.AsTime().In(timezone)
	} else {
		// Use current date as reference
		referenceDate = fromLocal
	}

	return e.findNextExecution(fromLocal, dayInterval, referenceDate, dailyRule.WeekdaysOnly, executionTimes)
}

// EvaluateMultiple returns the next N execution times for a daily rule
func (e *DailyEvaluator) EvaluateMultiple(ctx context.Context, rule *schedule.CalendarRule, from time.Time, timezone *time.Location, count int) ([]time.Time, error) {
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

// Validate checks if a daily rule is valid
func (e *DailyEvaluator) Validate(ctx context.Context, rule *schedule.CalendarRule) error {
	dailyRule := rule.GetDaily()
	if dailyRule == nil {
		return types.ErrInvalidRule.WithDetails("daily rule is nil")
	}

	// Validate day interval
	if dailyRule.DayInterval < 0 {
		return types.ErrInvalidRule.WithDetails("day interval cannot be negative")
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
func (e *DailyEvaluator) GetRuleType() types.RuleType {
	return types.RuleTypeDaily
}

// findNextExecution finds the next execution time based on daily rule parameters
func (e *DailyEvaluator) findNextExecution(from time.Time, dayInterval int, referenceDate time.Time, weekdaysOnly bool, executionTimes []*schedule.TimeOfDay) (*time.Time, error) {
	// Start from the current date
	currentDate := from.Truncate(24 * time.Hour) // Start of day
	maxAttempts := 365                           // Search for up to 1 year

	for attempts := 0; attempts < maxAttempts; attempts++ {
		// Check if current date is valid according to interval
		if e.isValidDay(currentDate, referenceDate, dayInterval) {
			// Check weekdays only constraint
			if weekdaysOnly && e.isWeekend(currentDate) {
				currentDate = currentDate.Add(24 * time.Hour)
				continue
			}

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

		// Move to next day
		currentDate = currentDate.Add(24 * time.Hour)
	}

	return nil, types.ErrNoExecutionTime.WithDetails("no valid execution time found within 1 year")
}

// isValidDay checks if the given date falls on a valid day according to the interval
func (e *DailyEvaluator) isValidDay(date, referenceDate time.Time, dayInterval int) bool {
	if dayInterval <= 1 {
		return true // Every day is valid
	}

	// Calculate the difference in days
	daysDiff := int(date.Sub(referenceDate).Hours() / 24)

	// Check if this day falls on the interval
	return daysDiff%dayInterval == 0
}

// isWeekend checks if the given date is a weekend (Saturday or Sunday)
func (e *DailyEvaluator) isWeekend(date time.Time) bool {
	weekday := date.Weekday()
	return weekday == time.Saturday || weekday == time.Sunday
}
