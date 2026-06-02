package evaluators

import (
	"context"
	"fmt"
	"time"

	schedule "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/pkg/calendar/types"
)

// BusinessDaysEvaluator handles business days calendar rules
type BusinessDaysEvaluator struct {
	businessCalendarProvider types.BusinessCalendarProvider
}

// NewBusinessDaysEvaluator creates a new business days evaluator
func NewBusinessDaysEvaluator(provider types.BusinessCalendarProvider) *BusinessDaysEvaluator {
	return &BusinessDaysEvaluator{
		businessCalendarProvider: provider,
	}
}

// Evaluate returns the next execution time for a business days rule after the given time
func (e *BusinessDaysEvaluator) Evaluate(ctx context.Context, rule *schedule.CalendarRule, from time.Time, timezone *time.Location) (*time.Time, error) {
	businessRule := rule.GetBusinessDays()
	if businessRule == nil {
		return nil, types.ErrInvalidRule.WithDetails("business days rule is nil")
	}

	// Convert from time to the specified timezone
	fromLocal := from.In(timezone)

	// Get execution times for this rule
	executionTimes := rule.ExecutionTimes
	if len(executionTimes) == 0 {
		// Default to 9 AM for business days if no execution times specified
		executionTimes = []*schedule.TimeOfDay{{Hour: 9, Minute: 0, Second: 0}}
	}

	// Check if rule is valid for the current time range
	if rule.ValidFrom != nil && from.Before(rule.ValidFrom.AsTime()) {
		fromLocal = rule.ValidFrom.AsTime().In(timezone)
	}
	if rule.ValidUntil != nil && from.After(rule.ValidUntil.AsTime()) {
		return nil, types.ErrNoExecutionTime.WithDetails("rule validity period has expired")
	}

	// Get business calendar
	var businessCalendar *schedule.BusinessCalendar
	if businessRule.BusinessCalendarId != "" {
		if e.businessCalendarProvider != nil {
			var err error
			businessCalendar, err = e.businessCalendarProvider.GetBusinessCalendar(ctx, businessRule.BusinessCalendarId)
			if err != nil {
				return nil, fmt.Errorf("failed to get business calendar: %w", err)
			}
		} else {
			return nil, fmt.Errorf("business calendar provider not configured")
		}
	} else {
		// Use default business calendar (Monday-Friday, no holidays)
		businessCalendar = e.getDefaultBusinessCalendar()
	}

	return e.findNextExecution(ctx, fromLocal, businessRule, businessCalendar, executionTimes)
}

// EvaluateMultiple returns the next N execution times for a business days rule
func (e *BusinessDaysEvaluator) EvaluateMultiple(ctx context.Context, rule *schedule.CalendarRule, from time.Time, timezone *time.Location, count int) ([]time.Time, error) {
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
		current = next.Add(24 * time.Hour) // Move to next day
	}

	return results, nil
}

// Validate checks if a business days rule is valid
func (e *BusinessDaysEvaluator) Validate(ctx context.Context, rule *schedule.CalendarRule) error {
	businessRule := rule.GetBusinessDays()
	if businessRule == nil {
		return types.ErrInvalidRule.WithDetails("business days rule is nil")
	}

	// Validate day offset
	if businessRule.DayOffset < -365 || businessRule.DayOffset > 365 {
		return types.ErrInvalidRule.WithDetails(fmt.Sprintf("day offset out of range: %d", businessRule.DayOffset))
	}

	// Validate business calendar if specified
	if businessRule.BusinessCalendarId != "" {
		if e.businessCalendarProvider != nil {
			_, err := e.businessCalendarProvider.GetBusinessCalendar(ctx, businessRule.BusinessCalendarId)
			if err != nil {
				return types.ErrBusinessCalendarNotFound.WithDetails(fmt.Sprintf("business calendar not found: %s", businessRule.BusinessCalendarId))
			}
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
func (e *BusinessDaysEvaluator) GetRuleType() types.RuleType {
	return types.RuleTypeBusinessDays
}

// findNextExecution finds the next execution time based on business days rule parameters
func (e *BusinessDaysEvaluator) findNextExecution(ctx context.Context, from time.Time, businessRule *schedule.BusinessDaysRule, businessCalendar *schedule.BusinessCalendar, executionTimes []*schedule.TimeOfDay) (*time.Time, error) {
	// Start from the current date
	currentDate := from.Truncate(24 * time.Hour) // Start of day
	maxAttempts := 365                           // Search for up to 1 year

	for attempts := 0; attempts < maxAttempts; attempts++ {
		// Check if current date is a business day
		isBusinessDay, err := e.isBusinessDay(ctx, currentDate, businessCalendar)
		if err != nil {
			return nil, fmt.Errorf("failed to check business day: %w", err)
		}

		if isBusinessDay {
			// Apply day offset if specified
			targetDate := currentDate
			if businessRule.DayOffset != 0 {
				targetDate = e.applyDayOffset(currentDate, int(businessRule.DayOffset), businessCalendar)
			}

			// Check if target date is still a business day (after offset)
			if targetIsBusinessDay, err := e.isBusinessDay(ctx, targetDate, businessCalendar); err != nil {
				return nil, fmt.Errorf("failed to check target business day: %w", err)
			} else if !targetIsBusinessDay {
				// Skip if target date is not a business day
				currentDate = currentDate.Add(24 * time.Hour)
				continue
			}

			// Check all execution times for this business day
			for _, execTime := range executionTimes {
				candidate := time.Date(
					targetDate.Year(), targetDate.Month(), targetDate.Day(),
					int(execTime.Hour), int(execTime.Minute), int(execTime.Second),
					0, targetDate.Location(),
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

// isBusinessDay checks if a date is a business day according to the business calendar
func (e *BusinessDaysEvaluator) isBusinessDay(ctx context.Context, date time.Time, businessCalendar *schedule.BusinessCalendar) (bool, error) {
	// Check if it's a weekend
	weekday := int32(date.Weekday())
	if weekday == 0 { // Sunday
		weekday = 7
	}

	for _, weekendDay := range businessCalendar.WeekendDays {
		if weekday == weekendDay {
			return false, nil
		}
	}

	// Check if it's a holiday
	for _, holiday := range businessCalendar.Holidays {
		holidayDate := holiday.Date.AsTime().Truncate(24 * time.Hour)
		if holidayDate.Equal(date.Truncate(24 * time.Hour)) {
			return false, nil
		}

		// Check recurring yearly holidays
		if holiday.RecurringYearly {
			if holidayDate.Month() == date.Month() && holidayDate.Day() == date.Day() {
				return false, nil
			}
		}
	}

	return true, nil
}

// applyDayOffset applies a day offset to a date, considering business days
func (e *BusinessDaysEvaluator) applyDayOffset(date time.Time, offset int, businessCalendar *schedule.BusinessCalendar) time.Time {
	if offset == 0 {
		return date
	}

	current := date
	remaining := offset

	if offset > 0 {
		// Move forward
		for remaining > 0 {
			current = current.Add(24 * time.Hour)
			if isBusinessDay, _ := e.isBusinessDay(context.Background(), current, businessCalendar); isBusinessDay {
				remaining--
			}
		}
	} else {
		// Move backward
		remaining = -remaining
		for remaining > 0 {
			current = current.Add(-24 * time.Hour)
			if isBusinessDay, _ := e.isBusinessDay(context.Background(), current, businessCalendar); isBusinessDay {
				remaining--
			}
		}
	}

	return current
}

// getDefaultBusinessCalendar returns a default business calendar (Monday-Friday, no holidays)
func (e *BusinessDaysEvaluator) getDefaultBusinessCalendar() *schedule.BusinessCalendar {
	return &schedule.BusinessCalendar{
		CalendarId:  "default",
		Name:        "Default Business Calendar",
		Description: "Monday to Friday, no holidays",
		Holidays:    []*schedule.Holiday{},
		WeekendDays: []int32{6, 7}, // Saturday and Sunday
		Timezone:    "UTC",
	}
}
