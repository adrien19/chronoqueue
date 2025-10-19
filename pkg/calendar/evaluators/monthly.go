package evaluators

import (
	"context"
	"fmt"
	"time"

	schedule "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/pkg/calendar"
)

// MonthlyEvaluator handles monthly calendar rules
type MonthlyEvaluator struct{}

// NewMonthlyEvaluator creates a new monthly rule evaluator
func NewMonthlyEvaluator() *MonthlyEvaluator {
	return &MonthlyEvaluator{}
}

// Evaluate returns the next execution time for a monthly rule after the given time
func (e *MonthlyEvaluator) Evaluate(ctx context.Context, rule *schedule.CalendarRule, from time.Time, timezone *time.Location) (*time.Time, error) {
	monthlyRule := rule.GetMonthly()
	if monthlyRule == nil {
		return nil, calendar.ErrInvalidRule.WithDetails("monthly rule is nil")
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

	// Find next execution time based on rule type
	switch monthlyRule.DayType {
	case schedule.MonthlyRule_DAY_OF_MONTH:
		return e.evaluateDayOfMonth(monthlyRule, fromLocal, executionTimes, timezone)
	case schedule.MonthlyRule_WEEKDAY_OF_MONTH:
		return e.evaluateWeekdayOfMonth(monthlyRule, fromLocal, executionTimes, timezone)
	case schedule.MonthlyRule_LAST_WEEKDAY:
		return e.evaluateLastWeekday(monthlyRule, fromLocal, executionTimes, timezone)
	case schedule.MonthlyRule_LAST_DAY:
		return e.evaluateLastDay(monthlyRule, fromLocal, executionTimes, timezone)
	default:
		return nil, calendar.ErrInvalidRule.WithDetails(fmt.Sprintf("unknown monthly rule day type: %v", monthlyRule.DayType))
	}
}

// EvaluateMultiple returns the next N execution times for a monthly rule
func (e *MonthlyEvaluator) EvaluateMultiple(ctx context.Context, rule *schedule.CalendarRule, from time.Time, timezone *time.Location, count int) ([]time.Time, error) {
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

// Validate checks if a monthly rule is valid
func (e *MonthlyEvaluator) Validate(ctx context.Context, rule *schedule.CalendarRule) error {
	monthlyRule := rule.GetMonthly()
	if monthlyRule == nil {
		return calendar.ErrInvalidRule.WithDetails("monthly rule is nil")
	}

	// Validate day type specific constraints
	switch monthlyRule.DayType {
	case schedule.MonthlyRule_DAY_OF_MONTH:
		if monthlyRule.DayValue < 1 || monthlyRule.DayValue > 31 {
			return calendar.ErrInvalidRule.WithDetails("day_value must be between 1 and 31 for DAY_OF_MONTH")
		}
	case schedule.MonthlyRule_WEEKDAY_OF_MONTH:
		if monthlyRule.DayValue < 1 || monthlyRule.DayValue > 7 {
			return calendar.ErrInvalidRule.WithDetails("day_value must be between 1 and 7 for WEEKDAY_OF_MONTH")
		}
		if monthlyRule.Occurrence < 1 || monthlyRule.Occurrence > 5 {
			return calendar.ErrInvalidRule.WithDetails("occurrence must be between 1 and 5 for WEEKDAY_OF_MONTH")
		}
	case schedule.MonthlyRule_LAST_WEEKDAY:
		if monthlyRule.DayValue < 1 || monthlyRule.DayValue > 7 {
			return calendar.ErrInvalidRule.WithDetails("day_value must be between 1 and 7 for LAST_WEEKDAY")
		}
	case schedule.MonthlyRule_LAST_DAY:
		// No additional validation needed for LAST_DAY
	default:
		return calendar.ErrInvalidRule.WithDetails(fmt.Sprintf("unknown day type: %v", monthlyRule.DayType))
	}

	// Validate months if specified
	for _, month := range monthlyRule.Months {
		if month < 1 || month > 12 {
			return calendar.ErrInvalidRule.WithDetails(fmt.Sprintf("invalid month: %d", month))
		}
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
func (e *MonthlyEvaluator) GetRuleType() calendar.RuleType {
	return calendar.RuleTypeMonthly
}

// evaluateDayOfMonth handles DAY_OF_MONTH rule type
func (e *MonthlyEvaluator) evaluateDayOfMonth(rule *schedule.MonthlyRule, from time.Time, executionTimes []*schedule.TimeOfDay, timezone *time.Location) (*time.Time, error) {
	dayOfMonth := int(rule.DayValue)

	// Start from the current month
	year, month, _ := from.Date()

	// If specific months are specified, filter to those months
	validMonths := rule.Months
	if len(validMonths) == 0 {
		// All months are valid
		for i := 1; i <= 12; i++ {
			validMonths = append(validMonths, int32(i))
		}
	}

	// Search for the next valid execution time
	for attempts := 0; attempts < 24; attempts++ { // Limit search to 2 years
		currentMonth := int32(month)

		// Check if current month is valid
		monthValid := false
		for _, validMonth := range validMonths {
			if validMonth == currentMonth {
				monthValid = true
				break
			}
		}

		if monthValid {
			// Check if the day exists in this month
			daysInMonth := time.Date(year, month, 0, 0, 0, 0, 0, timezone).Day()
			if dayOfMonth <= daysInMonth {
				// Find the next execution time on this day
				for _, execTime := range executionTimes {
					candidate := time.Date(year, month, dayOfMonth,
						int(execTime.Hour), int(execTime.Minute), int(execTime.Second), 0, timezone)

					if candidate.After(from) {
						return &candidate, nil
					}
				}
			}
		}

		// Move to next month
		month++
		if month > 12 {
			month = 1
			year++
		}
	}

	return nil, calendar.ErrNoExecutionTime.WithDetails("no valid execution time found within 2 years")
}

// evaluateWeekdayOfMonth handles WEEKDAY_OF_MONTH rule type
func (e *MonthlyEvaluator) evaluateWeekdayOfMonth(rule *schedule.MonthlyRule, from time.Time, executionTimes []*schedule.TimeOfDay, timezone *time.Location) (*time.Time, error) {
	weekday := time.Weekday(rule.DayValue - 1) // Convert to Go's weekday (0=Sunday)
	occurrence := int(rule.Occurrence)

	year, month, _ := from.Date()

	// If specific months are specified, filter to those months
	validMonths := rule.Months
	if len(validMonths) == 0 {
		for i := 1; i <= 12; i++ {
			validMonths = append(validMonths, int32(i))
		}
	}

	// Search for the next valid execution time
	for attempts := 0; attempts < 24; attempts++ {
		currentMonth := int32(month)

		// Check if current month is valid
		monthValid := false
		for _, validMonth := range validMonths {
			if validMonth == currentMonth {
				monthValid = true
				break
			}
		}

		if monthValid {
			// Find the Nth occurrence of the weekday in this month
			firstOfMonth := time.Date(year, month, 1, 0, 0, 0, 0, timezone)

			// Find first occurrence of the weekday
			daysUntilWeekday := int(weekday - firstOfMonth.Weekday())
			if daysUntilWeekday < 0 {
				daysUntilWeekday += 7
			}

			// Calculate the date of the Nth occurrence
			targetDay := 1 + daysUntilWeekday + (occurrence-1)*7

			// Check if this date exists in the month
			daysInMonth := time.Date(year, month, 0, 0, 0, 0, 0, timezone).Day()
			if targetDay <= daysInMonth {
				// Find the next execution time on this day
				for _, execTime := range executionTimes {
					candidate := time.Date(year, month, targetDay,
						int(execTime.Hour), int(execTime.Minute), int(execTime.Second), 0, timezone)

					if candidate.After(from) {
						return &candidate, nil
					}
				}
			}
		}

		// Move to next month
		month++
		if month > 12 {
			month = 1
			year++
		}
	}

	return nil, calendar.ErrNoExecutionTime.WithDetails("no valid execution time found within 2 years")
}

// evaluateLastWeekday handles LAST_WEEKDAY rule type
func (e *MonthlyEvaluator) evaluateLastWeekday(rule *schedule.MonthlyRule, from time.Time, executionTimes []*schedule.TimeOfDay, timezone *time.Location) (*time.Time, error) {
	weekday := time.Weekday(rule.DayValue - 1) // Convert to Go's weekday (0=Sunday)

	year, month, _ := from.Date()

	// If specific months are specified, filter to those months
	validMonths := rule.Months
	if len(validMonths) == 0 {
		for i := 1; i <= 12; i++ {
			validMonths = append(validMonths, int32(i))
		}
	}

	// Search for the next valid execution time
	for attempts := 0; attempts < 24; attempts++ {
		currentMonth := int32(month)

		// Check if current month is valid
		monthValid := false
		for _, validMonth := range validMonths {
			if validMonth == currentMonth {
				monthValid = true
				break
			}
		}

		if monthValid {
			// Find last occurrence of the weekday in this month
			lastOfMonth := time.Date(year, month+1, 0, 23, 59, 59, 0, timezone) // Last day of month

			// Work backwards to find the last occurrence of the weekday
			for day := lastOfMonth.Day(); day >= 1; day-- {
				candidate := time.Date(year, month, day, 0, 0, 0, 0, timezone)
				if candidate.Weekday() == weekday {
					// Found the last occurrence, check execution times
					for _, execTime := range executionTimes {
						execCandidate := time.Date(year, month, day,
							int(execTime.Hour), int(execTime.Minute), int(execTime.Second), 0, timezone)

						if execCandidate.After(from) {
							return &execCandidate, nil
						}
					}
					break
				}
			}
		}

		// Move to next month
		month++
		if month > 12 {
			month = 1
			year++
		}
	}

	return nil, calendar.ErrNoExecutionTime.WithDetails("no valid execution time found within 2 years")
}

// evaluateLastDay handles LAST_DAY rule type
func (e *MonthlyEvaluator) evaluateLastDay(rule *schedule.MonthlyRule, from time.Time, executionTimes []*schedule.TimeOfDay, timezone *time.Location) (*time.Time, error) {
	year, month, _ := from.Date()

	// If specific months are specified, filter to those months
	validMonths := rule.Months
	if len(validMonths) == 0 {
		for i := 1; i <= 12; i++ {
			validMonths = append(validMonths, int32(i))
		}
	}

	// Search for the next valid execution time
	for attempts := 0; attempts < 24; attempts++ {
		currentMonth := int32(month)

		// Check if current month is valid
		monthValid := false
		for _, validMonth := range validMonths {
			if validMonth == currentMonth {
				monthValid = true
				break
			}
		}

		if monthValid {
			// Get the last day of the month
			lastDay := time.Date(year, month+1, 0, 0, 0, 0, 0, timezone).Day()

			// Find the next execution time on the last day
			for _, execTime := range executionTimes {
				candidate := time.Date(year, month, lastDay,
					int(execTime.Hour), int(execTime.Minute), int(execTime.Second), 0, timezone)

				if candidate.After(from) {
					return &candidate, nil
				}
			}
		}

		// Move to next month
		month++
		if month > 12 {
			month = 1
			year++
		}
	}

	return nil, calendar.ErrNoExecutionTime.WithDetails("no valid execution time found within 2 years")
}

// validateTimeOfDay validates a time of day specification
func validateTimeOfDay(timeOfDay *schedule.TimeOfDay) error {
	if timeOfDay.Hour < 0 || timeOfDay.Hour > 23 {
		return calendar.ErrInvalidRule.WithDetails(fmt.Sprintf("invalid hour: %d", timeOfDay.Hour))
	}
	if timeOfDay.Minute < 0 || timeOfDay.Minute > 59 {
		return calendar.ErrInvalidRule.WithDetails(fmt.Sprintf("invalid minute: %d", timeOfDay.Minute))
	}
	if timeOfDay.Second < 0 || timeOfDay.Second > 59 {
		return calendar.ErrInvalidRule.WithDetails(fmt.Sprintf("invalid second: %d", timeOfDay.Second))
	}
	return nil
}
