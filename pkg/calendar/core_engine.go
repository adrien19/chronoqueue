package calendar

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	schedule "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/pkg/calendar/evaluators"
	"github.com/adrien19/chronoqueue/pkg/calendar/types"
)

// DefaultEngine is the default implementation of the Calendar Engine
type DefaultEngine struct {
	config                   *types.CalendarEngineConfig
	evaluatorRegistry        *evaluators.Registry
	businessCalendarProvider types.BusinessCalendarProvider
	timezoneProvider         types.TimezoneProvider
	exceptionHandler         types.ExceptionHandler
	cache                    *executionCache
	mu                       sync.RWMutex
}

// NewDefaultEngine creates a new default calendar engine with the given options
func NewDefaultEngine(options ...CalendarEngineOption) *DefaultEngine {
	config := DefaultCalendarEngineConfig()
	for _, option := range options {
		option(config)
	}

	engine := &DefaultEngine{
		config:                   config,
		evaluatorRegistry:        evaluators.NewRegistryWithBusinessCalendar(config.BusinessCalendarProvider),
		businessCalendarProvider: config.BusinessCalendarProvider,
		timezoneProvider:         config.TimezoneProvider,
		exceptionHandler:         config.ExceptionHandler,
	}

	// Initialize cache if enabled
	if config.EnableCaching {
		engine.cache = newExecutionCache(config.CacheTTL)
	}

	// Set up default providers if not provided
	if engine.businessCalendarProvider == nil {
		engine.businessCalendarProvider = NewMemoryBusinessCalendarProvider()
	}
	if engine.timezoneProvider == nil {
		engine.timezoneProvider = NewDefaultTimezoneProvider()
	}
	if engine.exceptionHandler == nil {
		engine.exceptionHandler = NewDefaultExceptionHandler()
	}

	return engine
}

// CalculateNextRun calculates the next execution time for a calendar schedule
func (e *DefaultEngine) CalculateNextRun(ctx context.Context, calendarSchedule *schedule.CalendarSchedule, from time.Time) (*time.Time, error) {
	// Validate the schedule first
	if err := e.ValidateSchedule(ctx, calendarSchedule); err != nil {
		return nil, fmt.Errorf("invalid calendar schedule: %w", err)
	}

	// Check cache first if enabled
	if e.cache != nil {
		cacheKey := e.generateCacheKey(calendarSchedule, from, 1)
		if cachedTimes := e.cache.get(cacheKey); len(cachedTimes) > 0 {
			return &cachedTimes[0], nil
		}
	}

	// Get timezone
	timezone, err := e.getTimezone(ctx, calendarSchedule.Timezone)
	if err != nil {
		return nil, fmt.Errorf("failed to get timezone: %w", err)
	}

	// Convert from time to the specified timezone
	fromLocal := from.In(timezone)

	// Collect next execution times from all rules
	var candidateTimes []time.Time
	for _, rule := range calendarSchedule.Rules {
		nextTime, err := e.evaluatorRegistry.EvaluateRule(ctx, rule, fromLocal, timezone)
		if err != nil {
			continue // Skip rules that error out
		}
		if nextTime != nil {
			candidateTimes = append(candidateTimes, *nextTime)
		}
	}

	if len(candidateTimes) == 0 {
		return nil, ErrNoExecutionTime.WithDetails("no rules produced valid execution times")
	}

	// Sort times and get the earliest
	sort.Slice(candidateTimes, func(i, j int) bool {
		return candidateTimes[i].Before(candidateTimes[j])
	})

	// Apply calendar exceptions
	finalTimes, err := e.applyExceptions(ctx, candidateTimes[:1], calendarSchedule.Exceptions, timezone)
	if err != nil {
		return nil, fmt.Errorf("failed to apply exceptions: %w", err)
	}

	if len(finalTimes) == 0 {
		return nil, ErrNoExecutionTime.WithDetails("all execution times were filtered out by exceptions")
	}

	result := finalTimes[0]

	// Cache the result if caching is enabled
	if e.cache != nil {
		cacheKey := e.generateCacheKey(calendarSchedule, from, 1)
		e.cache.set(cacheKey, finalTimes)
	}

	return &result, nil
}

// CalculateNextRuns calculates the next N execution times for a calendar schedule
func (e *DefaultEngine) CalculateNextRuns(ctx context.Context, calendarSchedule *schedule.CalendarSchedule, from time.Time, count int) ([]time.Time, error) {
	if count <= 0 {
		return nil, fmt.Errorf("count must be positive, got %d", count)
	}

	if count > e.config.MaxPreviewCount {
		return nil, fmt.Errorf("count %d exceeds maximum preview count %d", count, e.config.MaxPreviewCount)
	}

	// Validate the schedule first
	if err := e.ValidateSchedule(ctx, calendarSchedule); err != nil {
		return nil, fmt.Errorf("invalid calendar schedule: %w", err)
	}

	// Check cache first if enabled
	if e.cache != nil {
		cacheKey := e.generateCacheKey(calendarSchedule, from, count)
		if cachedTimes := e.cache.get(cacheKey); len(cachedTimes) >= count {
			return cachedTimes[:count], nil
		}
	}

	// Get timezone
	timezone, err := e.getTimezone(ctx, calendarSchedule.Timezone)
	if err != nil {
		return nil, fmt.Errorf("failed to get timezone: %w", err)
	}

	// Convert from time to the specified timezone
	fromLocal := from.In(timezone)

	// Collect candidate times from all rules with sufficient lookahead
	var allCandidates []time.Time
	searchWindow := e.config.MaxLookahead
	searchEnd := fromLocal.Add(searchWindow)

	for _, rule := range calendarSchedule.Rules {
		ruleTimes, err := e.evaluatorRegistry.EvaluateRuleMultiple(ctx, rule, fromLocal, timezone, count*5) // Get extra to account for exceptions
		if err != nil {
			continue // Skip rules that error out
		}

		// Filter times within search window
		for _, t := range ruleTimes {
			if t.Before(searchEnd) {
				allCandidates = append(allCandidates, t)
			}
		}
	}

	if len(allCandidates) == 0 {
		return nil, ErrNoExecutionTime.WithDetails("no rules produced valid execution times")
	}

	// Sort all candidate times
	sort.Slice(allCandidates, func(i, j int) bool {
		return allCandidates[i].Before(allCandidates[j])
	})

	// Remove duplicates and take more than needed to account for exceptions
	candidateTimes := e.removeDuplicateTimes(allCandidates)
	if len(candidateTimes) > count*2 {
		candidateTimes = candidateTimes[:count*2]
	}

	// Apply calendar exceptions
	finalTimes, err := e.applyExceptions(ctx, candidateTimes, calendarSchedule.Exceptions, timezone)
	if err != nil {
		return nil, fmt.Errorf("failed to apply exceptions: %w", err)
	}

	// Return requested count
	if len(finalTimes) > count {
		finalTimes = finalTimes[:count]
	}

	// Cache the result if caching is enabled
	if e.cache != nil {
		cacheKey := e.generateCacheKey(calendarSchedule, from, count)
		e.cache.set(cacheKey, finalTimes)
	}

	return finalTimes, nil
}

// ValidateSchedule validates a calendar schedule for correctness
func (e *DefaultEngine) ValidateSchedule(ctx context.Context, calendarSchedule *schedule.CalendarSchedule) error {
	if calendarSchedule == nil {
		return ErrInvalidSchedule.WithDetails("calendar schedule is nil")
	}

	// Validate timezone
	if calendarSchedule.Timezone != "" {
		if err := e.timezoneProvider.ValidateTimezone(ctx, calendarSchedule.Timezone); err != nil {
			return ErrInvalidTimezone.WithDetails(fmt.Sprintf("invalid timezone %s: %v", calendarSchedule.Timezone, err))
		}
	}

	// Validate rules
	if len(calendarSchedule.Rules) == 0 {
		return ErrInvalidSchedule.WithDetails("no rules specified")
	}

	for i, rule := range calendarSchedule.Rules {
		if err := e.evaluatorRegistry.ValidateRule(ctx, rule); err != nil {
			return fmt.Errorf("rule %d validation failed: %w", i, err)
		}
	}

	// Validate business calendar if specified
	if calendarSchedule.BusinessCalendar != nil {
		if err := e.validateBusinessCalendar(ctx, calendarSchedule.BusinessCalendar); err != nil {
			return fmt.Errorf("business calendar validation failed: %w", err)
		}
	}

	// Validate exceptions
	if len(calendarSchedule.Exceptions) > 0 {
		if err := e.exceptionHandler.ValidateExceptions(ctx, calendarSchedule.Exceptions); err != nil {
			return fmt.Errorf("exceptions validation failed: %w", err)
		}
	}

	return nil
}

// PreviewSchedule generates a preview of execution times for testing/debugging
func (e *DefaultEngine) PreviewSchedule(ctx context.Context, calendarSchedule *schedule.CalendarSchedule, from time.Time, count int) (*SchedulePreview, error) {
	fmt.Printf("DEBUG PreviewSchedule: received count=%d\n", count)

	// Default to 10 if count not specified
	if count <= 0 {
		count = 10
		fmt.Printf("DEBUG PreviewSchedule: defaulted count to 10\n")
	}

	if count > e.config.MaxPreviewCount {
		count = e.config.MaxPreviewCount
		fmt.Printf("DEBUG PreviewSchedule: capped count to MaxPreviewCount=%d\n", e.config.MaxPreviewCount)
	}

	fmt.Printf("DEBUG PreviewSchedule: calling CalculateNextRuns with count=%d\n", count)
	// Calculate execution times
	executionTimes, err := e.CalculateNextRuns(ctx, calendarSchedule, from, count)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate execution times: %w", err)
	}

	// Get timezone
	timezone, err := e.getTimezone(ctx, calendarSchedule.Timezone)
	if err != nil {
		return nil, fmt.Errorf("failed to get timezone: %w", err)
	}

	// Create preview
	preview := &SchedulePreview{
		ScheduleType: calendarSchedule.Type.String(),
		Timezone:     calendarSchedule.Timezone,
		GeneratedAt:  time.Now(),
		PreviewPeriod: PreviewPeriod{
			From:  from,
			To:    from.Add(e.config.MaxLookahead),
			Count: count,
		},
		ExecutionTimes: make([]ExecutionTime, len(executionTimes)),
	}

	// Fill execution times
	for i, execTime := range executionTimes {
		preview.ExecutionTimes[i] = ExecutionTime{
			Time:      execTime,
			LocalTime: execTime.In(timezone).Format(time.RFC3339),
			UTCTime:   execTime.UTC().Format(time.RFC3339),
		}
	}

	// Generate rule breakdown
	preview.RuleBreakdown = e.generateRuleBreakdown(ctx, calendarSchedule, from, timezone, count)

	// Generate exception information
	if len(calendarSchedule.Exceptions) > 0 {
		preview.Exceptions = e.generateExceptionPreview(calendarSchedule.Exceptions, timezone)
	}

	// Generate business day information if business calendar is specified
	if calendarSchedule.BusinessCalendar != nil {
		preview.BusinessDays = e.generateBusinessDayInfo(ctx, calendarSchedule.BusinessCalendar, from, count)
	}

	return preview, nil
}

// IsBusinessDay checks if a given date is a business day according to business calendar
func (e *DefaultEngine) IsBusinessDay(ctx context.Context, date time.Time, businessCalendar *schedule.BusinessCalendar) (bool, error) {
	if businessCalendar == nil {
		// Default behavior: Monday-Friday are business days
		weekday := date.Weekday()
		return weekday >= time.Monday && weekday <= time.Friday, nil
	}

	// Check weekend days
	weekday := int32(date.Weekday())
	if weekday == 0 { // Sunday
		weekday = 7
	}

	for _, weekendDay := range businessCalendar.WeekendDays {
		if weekday == weekendDay {
			return false, nil
		}
	}

	// Check holidays
	for _, holiday := range businessCalendar.Holidays {
		if e.isHolidayDate(date, holiday) {
			return false, nil
		}
	}

	return true, nil
}

// GetHolidays returns holidays for a date range from business calendar
func (e *DefaultEngine) GetHolidays(ctx context.Context, businessCalendar *schedule.BusinessCalendar, from, to time.Time) ([]Holiday, error) {
	if businessCalendar == nil {
		return []Holiday{}, nil
	}

	var holidays []Holiday
	for _, protoHoliday := range businessCalendar.Holidays {
		holiday := Holiday{
			Name:        protoHoliday.Name,
			Description: fmt.Sprintf("Holiday from calendar %s", businessCalendar.Name),
			IsRecurring: protoHoliday.RecurringYearly,
			CalendarID:  businessCalendar.CalendarId,
		}

		if protoHoliday.RecurringYearly {
			// Generate holiday dates for each year in the range
			startYear := from.Year()
			endYear := to.Year()
			for year := startYear; year <= endYear; year++ {
				holidayDate := e.calculateHolidayDate(protoHoliday, year)
				if holidayDate != nil && !holidayDate.Before(from) && !holidayDate.After(to) {
					holiday.Date = *holidayDate
					holidays = append(holidays, holiday)
				}
			}
		} else {
			holidayDate := protoHoliday.Date.AsTime()
			if !holidayDate.Before(from) && !holidayDate.After(to) {
				holiday.Date = holidayDate
				holidays = append(holidays, holiday)
			}
		}
	}

	return holidays, nil
}

// Helper methods

func (e *DefaultEngine) getTimezone(ctx context.Context, timezoneName string) (*time.Location, error) {
	if timezoneName == "" {
		timezoneName = e.config.DefaultTimezone
	}

	return e.timezoneProvider.GetTimezone(ctx, timezoneName)
}

func (e *DefaultEngine) applyExceptions(ctx context.Context, times []time.Time, exceptions []*schedule.CalendarException, timezone *time.Location) ([]time.Time, error) {
	if len(exceptions) == 0 {
		return times, nil
	}

	return e.exceptionHandler.ApplyExceptions(ctx, times, exceptions, timezone)
}

func (e *DefaultEngine) validateBusinessCalendar(ctx context.Context, businessCalendar *schedule.BusinessCalendar) error {
	// Validate calendar ID
	if businessCalendar.CalendarId == "" {
		return fmt.Errorf("business calendar ID is required")
	}

	// Validate weekend days
	for _, weekendDay := range businessCalendar.WeekendDays {
		if weekendDay < 1 || weekendDay > 7 {
			return fmt.Errorf("invalid weekend day: %d", weekendDay)
		}
	}

	// Validate timezone
	if businessCalendar.Timezone != "" {
		if err := e.timezoneProvider.ValidateTimezone(ctx, businessCalendar.Timezone); err != nil {
			return fmt.Errorf("invalid business calendar timezone: %w", err)
		}
	}

	return nil
}

func (e *DefaultEngine) removeDuplicateTimes(times []time.Time) []time.Time {
	if len(times) <= 1 {
		return times
	}

	var result []time.Time
	seen := make(map[int64]bool)

	for _, t := range times {
		key := t.Unix() // Use Unix timestamp as key to handle different timezones
		if !seen[key] {
			seen[key] = true
			result = append(result, t)
		}
	}

	return result
}

func (e *DefaultEngine) generateCacheKey(calendarSchedule *schedule.CalendarSchedule, from time.Time, count int) string {
	// Generate a simple cache key - in production, this would be more sophisticated
	return fmt.Sprintf("%s_%d_%d_%d", calendarSchedule.Timezone, from.Unix(), count, len(calendarSchedule.Rules))
}

func (e *DefaultEngine) generateRuleBreakdown(ctx context.Context, calendarSchedule *schedule.CalendarSchedule, from time.Time, timezone *time.Location, count int) []RulePreview {
	var breakdown []RulePreview

	for i, rule := range calendarSchedule.Rules {
		rulePreview := RulePreview{
			RuleIndex:   i,
			Description: e.describeRule(rule),
			IsActive:    true,
		}

		// Get rule type
		switch rule.Rule.(type) {
		case *schedule.CalendarRule_Monthly:
			rulePreview.RuleType = "monthly"
		case *schedule.CalendarRule_Weekly:
			rulePreview.RuleType = "weekly"
		case *schedule.CalendarRule_Daily:
			rulePreview.RuleType = "daily"
		case *schedule.CalendarRule_Yearly:
			rulePreview.RuleType = "yearly"
		case *schedule.CalendarRule_BusinessDays:
			rulePreview.RuleType = "business_days"
		case *schedule.CalendarRule_Custom:
			rulePreview.RuleType = "custom"
		default:
			rulePreview.RuleType = "unknown"
		}

		// Get next runs for this rule
		ruleTimes, err := e.evaluatorRegistry.EvaluateRuleMultiple(ctx, rule, from, timezone, count)
		if err == nil && len(ruleTimes) > 0 {
			rulePreview.NextRuns = ruleTimes
		}

		breakdown = append(breakdown, rulePreview)
	}

	return breakdown
}

func (e *DefaultEngine) generateExceptionPreview(exceptions []*schedule.CalendarException, timezone *time.Location) []ExceptionPreview {
	var preview []ExceptionPreview

	for _, exception := range exceptions {
		exceptionPreview := ExceptionPreview{
			Date:        exception.Date.AsTime().In(timezone),
			Description: exception.Reason,
		}

		switch exception.Type {
		case schedule.CalendarException_SKIP:
			exceptionPreview.Type = "skip"
			exceptionPreview.Impact = "Execution will be skipped on this date"
		case schedule.CalendarException_RESCHEDULE:
			exceptionPreview.Type = "reschedule"
			if exception.RescheduleTo != nil {
				exceptionPreview.Impact = fmt.Sprintf("Execution will be moved to %s", exception.RescheduleTo.AsTime().Format(time.RFC3339))
			}
		case schedule.CalendarException_EXTRA:
			exceptionPreview.Type = "extra"
			exceptionPreview.Impact = "Additional execution will be added on this date"
		}

		preview = append(preview, exceptionPreview)
	}

	return preview
}

func (e *DefaultEngine) generateBusinessDayInfo(ctx context.Context, businessCalendar *schedule.BusinessCalendar, from time.Time, count int) []BusinessDayInfo {
	var info []BusinessDayInfo
	current := from.Truncate(24 * time.Hour)

	for i := 0; i < count*7 && i < 100; i++ { // Limit to prevent excessive computation
		isBusinessDay, err := e.IsBusinessDay(ctx, current, businessCalendar)
		if err != nil {
			break
		}

		businessDayInfo := BusinessDayInfo{
			Date:          current,
			IsBusinessDay: isBusinessDay,
			IsWeekend:     e.isWeekend(current),
		}

		// Check if it's a holiday
		for _, holiday := range businessCalendar.Holidays {
			if e.isHolidayDate(current, holiday) {
				businessDayInfo.IsHoliday = true
				businessDayInfo.HolidayName = holiday.Name
				break
			}
		}

		info = append(info, businessDayInfo)
		current = current.Add(24 * time.Hour)
	}

	return info
}

func (e *DefaultEngine) describeRule(rule *schedule.CalendarRule) string {
	switch r := rule.Rule.(type) {
	case *schedule.CalendarRule_Monthly:
		return e.describeMonthlyRule(r.Monthly)
	case *schedule.CalendarRule_Weekly:
		return e.describeWeeklyRule(r.Weekly)
	case *schedule.CalendarRule_Daily:
		return e.describeDailyRule(r.Daily)
	case *schedule.CalendarRule_Yearly:
		return e.describeYearlyRule(r.Yearly)
	case *schedule.CalendarRule_BusinessDays:
		return e.describeBusinessDaysRule(r.BusinessDays)
	case *schedule.CalendarRule_Custom:
		return e.describeCustomRule(r.Custom)
	default:
		return "Unknown rule type"
	}
}

func (e *DefaultEngine) describeMonthlyRule(rule *schedule.MonthlyRule) string {
	switch rule.DayType {
	case schedule.MonthlyRule_DAY_OF_MONTH:
		return fmt.Sprintf("Day %d of every month", rule.DayValue)
	case schedule.MonthlyRule_WEEKDAY_OF_MONTH:
		weekdays := []string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}
		ordinals := []string{"", "First", "Second", "Third", "Fourth", "Fifth"}
		weekday := weekdays[rule.DayValue%7]
		ordinal := ordinals[rule.Occurrence]
		return fmt.Sprintf("%s %s of every month", ordinal, weekday)
	case schedule.MonthlyRule_LAST_WEEKDAY:
		weekdays := []string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}
		weekday := weekdays[rule.DayValue%7]
		return fmt.Sprintf("Last %s of every month", weekday)
	case schedule.MonthlyRule_LAST_DAY:
		return "Last day of every month"
	default:
		return "Monthly rule"
	}
}

func (e *DefaultEngine) describeWeeklyRule(rule *schedule.WeeklyRule) string {
	if len(rule.DaysOfWeek) == 0 {
		return "Weekly rule (no days specified)"
	}

	weekdays := []string{"", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"}
	var dayNames []string
	for _, day := range rule.DaysOfWeek {
		if day >= 1 && day <= 7 {
			dayNames = append(dayNames, weekdays[day])
		}
	}

	if rule.WeekInterval <= 1 {
		return fmt.Sprintf("Every %s", formatList(dayNames))
	}
	return fmt.Sprintf("Every %d weeks on %s", rule.WeekInterval, formatList(dayNames))
}

func (e *DefaultEngine) describeDailyRule(rule *schedule.DailyRule) string {
	if rule.DayInterval <= 1 {
		if rule.WeekdaysOnly {
			return "Every weekday"
		}
		return "Every day"
	}

	if rule.WeekdaysOnly {
		return fmt.Sprintf("Every %d weekdays", rule.DayInterval)
	}
	return fmt.Sprintf("Every %d days", rule.DayInterval)
}

func (e *DefaultEngine) describeYearlyRule(rule *schedule.YearlyRule) string {
	months := []string{"", "January", "February", "March", "April", "May", "June",
		"July", "August", "September", "October", "November", "December"}
	month := months[rule.Month]
	return fmt.Sprintf("Every %s %d", month, rule.Day)
}

func (e *DefaultEngine) describeBusinessDaysRule(rule *schedule.BusinessDaysRule) string {
	if rule.DayOffset == 0 {
		return "Every business day"
	} else if rule.DayOffset > 0 {
		return fmt.Sprintf("%d business days after each business day", rule.DayOffset)
	} else {
		return fmt.Sprintf("%d business days before each business day", -rule.DayOffset)
	}
}

func (e *DefaultEngine) describeCustomRule(rule *schedule.CustomRule) string {
	if rule.RuleType != "" {
		return fmt.Sprintf("Custom rule: %s", rule.RuleType)
	}
	return "Custom rule"
}

func (e *DefaultEngine) isWeekend(date time.Time) bool {
	weekday := date.Weekday()
	return weekday == time.Saturday || weekday == time.Sunday
}

func (e *DefaultEngine) isHolidayDate(date time.Time, holiday *schedule.Holiday) bool {
	holidayDate := holiday.Date.AsTime().Truncate(24 * time.Hour)
	dateToCheck := date.Truncate(24 * time.Hour)

	if holiday.RecurringYearly {
		// For recurring holidays, compare month and day
		return holidayDate.Month() == dateToCheck.Month() && holidayDate.Day() == dateToCheck.Day()
	}

	// For non-recurring holidays, compare exact dates
	return holidayDate.Equal(dateToCheck)
}

func (e *DefaultEngine) calculateHolidayDate(holiday *schedule.Holiday, year int) *time.Time {
	if holiday.Rule == nil {
		// Simple date-based holiday
		baseDate := holiday.Date.AsTime()
		result := time.Date(year, baseDate.Month(), baseDate.Day(), 0, 0, 0, 0, time.UTC)
		return &result
	}

	// Complex rule-based holiday calculation would go here
	// For now, return the simple date
	baseDate := holiday.Date.AsTime()
	result := time.Date(year, baseDate.Month(), baseDate.Day(), 0, 0, 0, 0, time.UTC)
	return &result
}

func formatList(items []string) string {
	if len(items) == 0 {
		return ""
	}
	if len(items) == 1 {
		return items[0]
	}
	if len(items) == 2 {
		return items[0] + " and " + items[1]
	}

	result := ""
	for i, item := range items {
		if i == len(items)-1 {
			result += "and " + item
		} else if i == 0 {
			result += item
		} else {
			result += ", " + item
		}
	}
	return result
}
