package evaluators

import (
	"context"
	"fmt"
	"time"

	schedule "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/pkg/calendar"
)

// YearlyEvaluator handles yearly calendar rules
type YearlyEvaluator struct{}

// NewYearlyEvaluator creates a new yearly rule evaluator
func NewYearlyEvaluator() *YearlyEvaluator {
	return &YearlyEvaluator{}
}

// Evaluate returns the next execution time for a yearly rule after the given time
func (e *YearlyEvaluator) Evaluate(ctx context.Context, rule *schedule.CalendarRule, from time.Time, timezone *time.Location) (*time.Time, error) {
	yearlyRule := rule.GetYearly()
	if yearlyRule == nil {
		return nil, calendar.ErrInvalidRule.WithDetails("yearly rule is nil")
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
		return nil, calendar.ErrNoExecutionTime.WithDetails("rule validity period has expired")
	}

	return e.findNextExecution(fromLocal, yearlyRule, executionTimes)
}

// EvaluateMultiple returns the next N execution times for a yearly rule
func (e *YearlyEvaluator) EvaluateMultiple(ctx context.Context, rule *schedule.CalendarRule, from time.Time, timezone *time.Location, count int) ([]time.Time, error) {
	var results []time.Time
	current := from

	for len(results) < count {
		next, err := e.Evaluate(ctx, rule, current, timezone)
		if err != nil {
			if err == calendar.ErrNoExecutionTime {
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

// Validate checks if a yearly rule is valid
func (e *YearlyEvaluator) Validate(ctx context.Context, rule *schedule.CalendarRule) error {
	yearlyRule := rule.GetYearly()
	if yearlyRule == nil {
		return calendar.ErrInvalidRule.WithDetails("yearly rule is nil")
	}

	// Validate month
	if yearlyRule.Month < 1 || yearlyRule.Month > 12 {
		return calendar.ErrInvalidRule.WithDetails(fmt.Sprintf("invalid month: %d (must be 1-12)", yearlyRule.Month))
	}

	// Validate day
	if yearlyRule.Day < 1 || yearlyRule.Day > 31 {
		return calendar.ErrInvalidRule.WithDetails(fmt.Sprintf("invalid day: %d (must be 1-31)", yearlyRule.Day))
	}

	// Special validation for February 29
	if yearlyRule.Month == 2 && yearlyRule.Day == 29 && !yearlyRule.AdjustForLeapYear {
		return calendar.ErrInvalidRule.WithDetails("February 29 requires adjust_for_leap_year to be true")
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
func (e *YearlyEvaluator) GetRuleType() calendar.RuleType {
	return calendar.RuleTypeYearly
}

// findNextExecution finds the next execution time based on yearly rule parameters
func (e *YearlyEvaluator) findNextExecution(from time.Time, yearlyRule *schedule.YearlyRule, executionTimes []*schedule.TimeOfDay) (*time.Time, error) {
	month := time.Month(yearlyRule.Month)
	day := int(yearlyRule.Day)

	// Start from the current year
	currentYear := from.Year()
	maxAttempts := 10 // Search for up to 10 years

	for attempts := 0; attempts < maxAttempts; attempts++ {
		// Handle leap year adjustments for February 29
		targetDay := day
		if month == time.February && day == 29 {
			if !e.isLeapYear(currentYear) {
				if yearlyRule.AdjustForLeapYear {
					// Adjust to February 28 in non-leap years
					targetDay = 28
				} else {
					// Skip this year entirely
					currentYear++
					continue
				}
			}
		}

		// Check if the day exists in the specified month
		daysInMonth := e.daysInMonth(currentYear, month)
		if targetDay > daysInMonth {
			// Day doesn't exist in this month (e.g., April 31)
			currentYear++
			continue
		}

		// Check all execution times for this date
		for _, execTime := range executionTimes {
			candidate := time.Date(
				currentYear, month, targetDay,
				int(execTime.Hour), int(execTime.Minute), int(execTime.Second),
				0, from.Location())

			if candidate.After(from) {
				return &candidate, nil
			}
		}

		// Move to next year
		currentYear++
	}

	return nil, calendar.ErrNoExecutionTime.WithDetails("no valid execution time found within 10 years")
}

// isLeapYear checks if a year is a leap year
func (e *YearlyEvaluator) isLeapYear(year int) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}

// daysInMonth returns the number of days in a given month of a given year
func (e *YearlyEvaluator) daysInMonth(year int, month time.Month) int {
	// Get the first day of the next month and subtract one day
	firstOfNextMonth := time.Date(year, month+1, 1, 0, 0, 0, 0, time.UTC)
	lastOfMonth := firstOfNextMonth.Add(-24 * time.Hour)
	return lastOfMonth.Day()
}
