package calendar

import (
	"time"

	"github.com/adrien19/chronoqueue/pkg/calendar/types"
)

// Type aliases for interfaces and types defined in types package
type Engine = types.Engine
type RuleEvaluator = types.RuleEvaluator
type BusinessCalendarProvider = types.BusinessCalendarProvider
type TimezoneProvider = types.TimezoneProvider
type ExceptionHandler = types.ExceptionHandler
type RuleType = types.RuleType
type SchedulePreview = types.SchedulePreview
type Holiday = types.Holiday
type CalendarEngineConfig = types.CalendarEngineConfig
type CalendarError = types.CalendarError
type PreviewPeriod = types.PreviewPeriod
type ExecutionTime = types.ExecutionTime
type RulePreview = types.RulePreview
type ExceptionPreview = types.ExceptionPreview
type BusinessDayInfo = types.BusinessDayInfo
type ValidationIssue = types.ValidationIssue

// Constants from types package
const (
	RuleTypeMonthly      = types.RuleTypeMonthly
	RuleTypeWeekly       = types.RuleTypeWeekly
	RuleTypeDaily        = types.RuleTypeDaily
	RuleTypeYearly       = types.RuleTypeYearly
	RuleTypeBusinessDays = types.RuleTypeBusinessDays
	RuleTypeCustom       = types.RuleTypeCustom
)

// Error aliases from types package
var (
	ErrInvalidSchedule          = types.ErrInvalidSchedule
	ErrInvalidRule              = types.ErrInvalidRule
	ErrInvalidTimezone          = types.ErrInvalidTimezone
	ErrNoExecutionTime          = types.ErrNoExecutionTime
	ErrBusinessCalendarNotFound = types.ErrBusinessCalendarNotFound
	ErrRuleEvaluatorNotFound    = types.ErrRuleEvaluatorNotFound
)

// CalendarEngineOption is a functional option for configuring the calendar engine
type CalendarEngineOption func(*CalendarEngineConfig)

// WithDefaultTimezone sets the default timezone
func WithDefaultTimezone(timezone string) CalendarEngineOption {
	return func(config *CalendarEngineConfig) {
		config.DefaultTimezone = timezone
	}
}

// WithMaxFutureCalculations sets the maximum future calculations
func WithMaxFutureCalculations(count int) CalendarEngineOption {
	return func(config *CalendarEngineConfig) {
		config.MaxFutureCalculations = count
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

// WithCaching enables caching with the specified configuration
func WithCaching(cacheConfig *types.CacheConfig) CalendarEngineOption {
	return func(config *CalendarEngineConfig) {
		config.CacheConfig = cacheConfig
	}
}

// WithPerformanceMetrics enables performance metrics
func WithPerformanceMetrics(enable bool) CalendarEngineOption {
	return func(config *CalendarEngineConfig) {
		config.EnablePerformanceMetrics = enable
	}
}

// DefaultCalendarEngineConfig returns the default configuration
func DefaultCalendarEngineConfig() *CalendarEngineConfig {
	return &CalendarEngineConfig{
		DefaultTimezone:          "UTC",
		MaxFutureCalculations:    1000,
		EnablePerformanceMetrics: false,
		CacheConfig: &types.CacheConfig{
			Enabled:         true,
			TTL:             time.Hour * 24, // 24 hours
			MaxEntries:      10000,
			CleanupInterval: time.Hour, // 1 hour
		},
	}
}
