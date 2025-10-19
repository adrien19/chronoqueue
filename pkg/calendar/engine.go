package calendar

import (
	"context"
	"time"

	schedule "github.com/adrien19/chronoqueue/api/schedule/v1"
)

// Engine is the main interface for calendar-based scheduling operations
type Engine interface {
	// CalculateNextRun calculates the next execution time for a calendar schedule
	CalculateNextRun(ctx context.Context, calendarSchedule *schedule.CalendarSchedule, from time.Time) (*time.Time, error)

	// CalculateNextRuns calculates the next N execution times for a calendar schedule
	CalculateNextRuns(ctx context.Context, calendarSchedule *schedule.CalendarSchedule, from time.Time, count int) ([]time.Time, error)

	// ValidateSchedule validates a calendar schedule for correctness
	ValidateSchedule(ctx context.Context, calendarSchedule *schedule.CalendarSchedule) error

	// PreviewSchedule generates a preview of execution times for testing/debugging
	PreviewSchedule(ctx context.Context, calendarSchedule *schedule.CalendarSchedule, from time.Time, count int) (*SchedulePreview, error)

	// IsBusinessDay checks if a given date is a business day according to business calendar
	IsBusinessDay(ctx context.Context, date time.Time, businessCalendar *schedule.BusinessCalendar) (bool, error)

	// GetHolidays returns holidays for a date range from business calendar
	GetHolidays(ctx context.Context, businessCalendar *schedule.BusinessCalendar, from, to time.Time) ([]Holiday, error)
}

// RuleEvaluator interface for evaluating specific calendar rules
type RuleEvaluator interface {
	// Evaluate returns the next execution time for this rule after the given time
	Evaluate(ctx context.Context, rule *schedule.CalendarRule, from time.Time, timezone *time.Location) (*time.Time, error)

	// EvaluateMultiple returns the next N execution times for this rule
	EvaluateMultiple(ctx context.Context, rule *schedule.CalendarRule, from time.Time, timezone *time.Location, count int) ([]time.Time, error)

	// Validate checks if the rule is valid
	Validate(ctx context.Context, rule *schedule.CalendarRule) error

	// GetRuleType returns the type of rule this evaluator handles
	GetRuleType() RuleType
}

// BusinessCalendarProvider interface for business calendar operations
type BusinessCalendarProvider interface {
	// GetBusinessCalendar retrieves a business calendar by ID
	GetBusinessCalendar(ctx context.Context, calendarID string) (*schedule.BusinessCalendar, error)

	// CreateBusinessCalendar creates a new business calendar
	CreateBusinessCalendar(ctx context.Context, calendar *schedule.BusinessCalendar) error

	// UpdateBusinessCalendar updates an existing business calendar
	UpdateBusinessCalendar(ctx context.Context, calendar *schedule.BusinessCalendar) error

	// DeleteBusinessCalendar deletes a business calendar
	DeleteBusinessCalendar(ctx context.Context, calendarID string) error

	// ListBusinessCalendars lists all available business calendars
	ListBusinessCalendars(ctx context.Context) ([]*schedule.BusinessCalendar, error)
}

// TimezoneProvider interface for timezone operations
type TimezoneProvider interface {
	// GetTimezone returns a timezone by name
	GetTimezone(ctx context.Context, name string) (*time.Location, error)

	// ListTimezones returns available timezones
	ListTimezones(ctx context.Context) ([]string, error)

	// ValidateTimezone checks if a timezone name is valid
	ValidateTimezone(ctx context.Context, name string) error
}

// ExceptionHandler interface for handling calendar exceptions
type ExceptionHandler interface {
	// ApplyExceptions applies calendar exceptions to a list of execution times
	ApplyExceptions(ctx context.Context, times []time.Time, exceptions []*schedule.CalendarException, timezone *time.Location) ([]time.Time, error)

	// ValidateExceptions validates a list of calendar exceptions
	ValidateExceptions(ctx context.Context, exceptions []*schedule.CalendarException) error
}

// Core types and enums

// RuleType represents the type of calendar rule
type RuleType int

const (
	RuleTypeMonthly RuleType = iota
	RuleTypeWeekly
	RuleTypeDaily
	RuleTypeYearly
	RuleTypeBusinessDays
	RuleTypeCustom
)

// String returns the string representation of the rule type
func (r RuleType) String() string {
	switch r {
	case RuleTypeMonthly:
		return "monthly"
	case RuleTypeWeekly:
		return "weekly"
	case RuleTypeDaily:
		return "daily"
	case RuleTypeYearly:
		return "yearly"
	case RuleTypeBusinessDays:
		return "business_days"
	case RuleTypeCustom:
		return "custom"
	default:
		return "unknown"
	}
}

// SchedulePreview contains a preview of schedule execution times
type SchedulePreview struct {
	ScheduleID       string             `json:"schedule_id"`
	ScheduleType     string             `json:"schedule_type"`
	Timezone         string             `json:"timezone"`
	GeneratedAt      time.Time          `json:"generated_at"`
	PreviewPeriod    PreviewPeriod      `json:"preview_period"`
	ExecutionTimes   []ExecutionTime    `json:"execution_times"`
	RuleBreakdown    []RulePreview      `json:"rule_breakdown"`
	Exceptions       []ExceptionPreview `json:"exceptions"`
	BusinessDays     []BusinessDayInfo  `json:"business_days,omitempty"`
	ValidationIssues []ValidationIssue  `json:"validation_issues,omitempty"`
}

// PreviewPeriod defines the time range for the preview
type PreviewPeriod struct {
	From  time.Time `json:"from"`
	To    time.Time `json:"to"`
	Count int       `json:"count"`
}

// ExecutionTime represents a single execution time in the preview
type ExecutionTime struct {
	Time            time.Time `json:"time"`
	LocalTime       string    `json:"local_time"`
	UTCTime         string    `json:"utc_time"`
	RuleSource      string    `json:"rule_source"`
	IsException     bool      `json:"is_exception"`
	ExceptionReason string    `json:"exception_reason,omitempty"`
}

// RulePreview shows how each rule contributes to the schedule
type RulePreview struct {
	RuleType    string          `json:"rule_type"`
	RuleIndex   int             `json:"rule_index"`
	Description string          `json:"description"`
	NextRuns    []ExecutionTime `json:"next_runs"`
	IsActive    bool            `json:"is_active"`
}

// ExceptionPreview shows calendar exceptions
type ExceptionPreview struct {
	Date        time.Time `json:"date"`
	Type        string    `json:"type"`
	Description string    `json:"description"`
	Impact      string    `json:"impact"`
}

// BusinessDayInfo provides information about business days
type BusinessDayInfo struct {
	Date          time.Time `json:"date"`
	IsBusinessDay bool      `json:"is_business_day"`
	IsHoliday     bool      `json:"is_holiday"`
	HolidayName   string    `json:"holiday_name,omitempty"`
	IsWeekend     bool      `json:"is_weekend"`
}

// ValidationIssue represents a validation problem with the schedule
type ValidationIssue struct {
	Severity   string `json:"severity"`   // "error", "warning", "info"
	RuleIndex  int    `json:"rule_index"` // -1 for global issues
	Field      string `json:"field"`
	Message    string `json:"message"`
	Suggestion string `json:"suggestion,omitempty"`
}

// Holiday represents a holiday in a business calendar
type Holiday struct {
	Date        time.Time `json:"date"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	IsRecurring bool      `json:"is_recurring"`
	CalendarID  string    `json:"calendar_id"`
}

// CalendarEngineConfig contains configuration for the calendar engine
type CalendarEngineConfig struct {
	// DefaultTimezone is used when no timezone is specified
	DefaultTimezone string

	// MaxPreviewCount limits the number of execution times in previews
	MaxPreviewCount int

	// MaxLookahead limits how far in the future to calculate execution times
	MaxLookahead time.Duration

	// BusinessCalendarProvider for business calendar operations
	BusinessCalendarProvider BusinessCalendarProvider

	// TimezoneProvider for timezone operations
	TimezoneProvider TimezoneProvider

	// ExceptionHandler for calendar exceptions
	ExceptionHandler ExceptionHandler

	// EnableCaching enables caching of calculated execution times
	EnableCaching bool

	// CacheTTL defines how long to cache calculated execution times
	CacheTTL time.Duration
}

// CalendarEngineOption is a functional option for configuring the calendar engine
type CalendarEngineOption func(*CalendarEngineConfig)

// WithDefaultTimezone sets the default timezone
func WithDefaultTimezone(timezone string) CalendarEngineOption {
	return func(config *CalendarEngineConfig) {
		config.DefaultTimezone = timezone
	}
}

// WithMaxPreviewCount sets the maximum preview count
func WithMaxPreviewCount(count int) CalendarEngineOption {
	return func(config *CalendarEngineConfig) {
		config.MaxPreviewCount = count
	}
}

// WithMaxLookahead sets the maximum lookahead duration
func WithMaxLookahead(duration time.Duration) CalendarEngineOption {
	return func(config *CalendarEngineConfig) {
		config.MaxLookahead = duration
	}
}

// WithBusinessCalendarProvider sets the business calendar provider
func WithBusinessCalendarProvider(provider BusinessCalendarProvider) CalendarEngineOption {
	return func(config *CalendarEngineConfig) {
		config.BusinessCalendarProvider = provider
	}
}

// WithTimezoneProvider sets the timezone provider
func WithTimezoneProvider(provider TimezoneProvider) CalendarEngineOption {
	return func(config *CalendarEngineConfig) {
		config.TimezoneProvider = provider
	}
}

// WithExceptionHandler sets the exception handler
func WithExceptionHandler(handler ExceptionHandler) CalendarEngineOption {
	return func(config *CalendarEngineConfig) {
		config.ExceptionHandler = handler
	}
}

// WithCaching enables caching with the specified TTL
func WithCaching(enable bool, ttl time.Duration) CalendarEngineOption {
	return func(config *CalendarEngineConfig) {
		config.EnableCaching = enable
		config.CacheTTL = ttl
	}
}

// DefaultCalendarEngineConfig returns the default configuration
func DefaultCalendarEngineConfig() *CalendarEngineConfig {
	return &CalendarEngineConfig{
		DefaultTimezone: "UTC",
		MaxPreviewCount: 100,
		MaxLookahead:    time.Hour * 24 * 365, // 1 year
		EnableCaching:   true,
		CacheTTL:        time.Hour * 24, // 24 hours
	}
}

// Errors
var (
	ErrInvalidSchedule          = &CalendarError{Code: "INVALID_SCHEDULE", Message: "Invalid calendar schedule"}
	ErrInvalidRule              = &CalendarError{Code: "INVALID_RULE", Message: "Invalid calendar rule"}
	ErrInvalidTimezone          = &CalendarError{Code: "INVALID_TIMEZONE", Message: "Invalid timezone"}
	ErrNoExecutionTime          = &CalendarError{Code: "NO_EXECUTION_TIME", Message: "No execution time found"}
	ErrBusinessCalendarNotFound = &CalendarError{Code: "BUSINESS_CALENDAR_NOT_FOUND", Message: "Business calendar not found"}
	ErrRuleEvaluatorNotFound    = &CalendarError{Code: "RULE_EVALUATOR_NOT_FOUND", Message: "Rule evaluator not found"}
)

// CalendarError represents a calendar-specific error
type CalendarError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

// Error implements the error interface
func (e *CalendarError) Error() string {
	if e.Details != "" {
		return e.Message + ": " + e.Details
	}
	return e.Message
}

// WithDetails adds details to the error
func (e *CalendarError) WithDetails(details string) *CalendarError {
	return &CalendarError{
		Code:    e.Code,
		Message: e.Message,
		Details: details,
	}
}
