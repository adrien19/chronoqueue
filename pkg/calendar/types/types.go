package types

import (
	"context"
	"time"

	schedule "github.com/adrien19/chronoqueue/api/schedule/v1"
)

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

// Predefined errors
var (
	ErrInvalidSchedule          = &CalendarError{Code: "INVALID_SCHEDULE", Message: "Invalid calendar schedule"}
	ErrInvalidRule              = &CalendarError{Code: "INVALID_RULE", Message: "Invalid calendar rule"}
	ErrInvalidTimezone          = &CalendarError{Code: "INVALID_TIMEZONE", Message: "Invalid timezone"}
	ErrNoExecutionTime          = &CalendarError{Code: "NO_EXECUTION_TIME", Message: "No execution time found"}
	ErrBusinessCalendarNotFound = &CalendarError{Code: "BUSINESS_CALENDAR_NOT_FOUND", Message: "Business calendar not found"}
	ErrRuleEvaluatorNotFound    = &CalendarError{Code: "RULE_EVALUATOR_NOT_FOUND", Message: "Rule evaluator not found"}
)

// Holiday represents a calendar holiday
type Holiday struct {
	Date        time.Time `json:"date"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Type        string    `json:"type,omitempty"`
	Country     string    `json:"country,omitempty"`
	IsRecurring bool      `json:"is_recurring"`
	CalendarID  string    `json:"calendar_id"`
}

// SchedulePreview contains preview information for a calendar schedule
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

// ValidationIssue represents a validation problem with the schedule
type ValidationIssue struct {
	Severity   string `json:"severity"`   // "error", "warning", "info"
	RuleIndex  int    `json:"rule_index"` // -1 for global issues
	Field      string `json:"field"`
	Message    string `json:"message"`
	Suggestion string `json:"suggestion,omitempty"`
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

// BusinessCalendarProvider provides business calendar functionality
type BusinessCalendarProvider interface {
	GetBusinessCalendar(ctx context.Context, id string) (*schedule.BusinessCalendar, error)
	CreateBusinessCalendar(ctx context.Context, calendar *schedule.BusinessCalendar) error
	UpdateBusinessCalendar(ctx context.Context, calendar *schedule.BusinessCalendar) error
	DeleteBusinessCalendar(ctx context.Context, id string) error
	ListBusinessCalendars(ctx context.Context) ([]*schedule.BusinessCalendar, error)
	IsBusinessDay(ctx context.Context, calendarID string, date time.Time) (bool, error)
	GetNextBusinessDay(ctx context.Context, calendarID string, from time.Time) (time.Time, error)
	GetPreviousBusinessDay(ctx context.Context, calendarID string, from time.Time) (time.Time, error)
	AddBusinessDays(ctx context.Context, calendarID string, from time.Time, days int) (time.Time, error)
}

// TimezoneProvider interface for timezone operations
type TimezoneProvider interface {
	// GetTimezone returns a timezone by name
	GetTimezone(ctx context.Context, name string) (*time.Location, error)

	// ListTimezones returns available timezones
	ListTimezones(ctx context.Context) ([]string, error)

	// ValidateTimezone checks if a timezone name is valid
	ValidateTimezone(ctx context.Context, name string) error

	// GetTimezoneInfo returns detailed information about a timezone
	GetTimezoneInfo(ctx context.Context, name string, at time.Time) (*TimezoneInfo, error)

	// ConvertTime converts time from one timezone to another
	ConvertTime(ctx context.Context, t time.Time, fromTz, toTz string) (time.Time, error)
}

// TimezoneInfo contains detailed information about a timezone
type TimezoneInfo struct {
	Name         string        `json:"name"`
	Abbreviation string        `json:"abbreviation"`
	Offset       time.Duration `json:"offset"`
	IsDST        bool          `json:"is_dst"`
	DSTStart     *time.Time    `json:"dst_start,omitempty"`
	DSTEnd       *time.Time    `json:"dst_end,omitempty"`
}

// ExceptionHandler interface for handling calendar exceptions
type ExceptionHandler interface {
	// ApplyExceptions applies calendar exceptions to a list of execution times
	ApplyExceptions(ctx context.Context, times []time.Time, exceptions []*schedule.CalendarException, timezone *time.Location) ([]time.Time, error)

	// ValidateExceptions validates a list of calendar exceptions
	ValidateExceptions(ctx context.Context, exceptions []*schedule.CalendarException) error
}

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

// CalendarEngineConfig contains configuration for the calendar engine
type CalendarEngineConfig struct {
	BusinessCalendarProvider BusinessCalendarProvider
	TimezoneProvider         TimezoneProvider
	ExceptionHandler         ExceptionHandler
	CacheConfig              *CacheConfig
	DefaultTimezone          string
	MaxFutureCalculations    int
	EnablePerformanceMetrics bool
	MaxPreviewCount          int
	MaxLookahead             time.Duration
	EnableCaching            bool
	CacheTTL                 time.Duration
}

// CacheConfig contains configuration for the execution cache
type CacheConfig struct {
	Enabled         bool
	TTL             time.Duration
	MaxEntries      int
	CleanupInterval time.Duration
}

// RulePreview shows how each rule contributes to the schedule
type RulePreview struct {
	RuleType    string      `json:"rule_type"`
	RuleIndex   int         `json:"rule_index"`
	Description string      `json:"description"`
	NextRuns    []time.Time `json:"next_runs"`
	IsActive    bool        `json:"is_active"`
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
