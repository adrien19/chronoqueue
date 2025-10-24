package calendar

import (
	"time"

	"github.com/adrien19/chronoqueue/pkg/calendar/types"
)

// Type aliases for interfaces and types defined in types package
type (
	Engine                   = types.Engine
	RuleEvaluator            = types.RuleEvaluator
	BusinessCalendarProvider = types.BusinessCalendarProvider
	TimezoneProvider         = types.TimezoneProvider
	ExceptionHandler         = types.ExceptionHandler
	RuleType                 = types.RuleType
	SchedulePreview          = types.SchedulePreview
	Holiday                  = types.Holiday
	CalendarEngineConfig     = types.CalendarEngineConfig
	CalendarError            = types.CalendarError
	PreviewPeriod            = types.PreviewPeriod
	ExecutionTime            = types.ExecutionTime
	RulePreview              = types.RulePreview
	ExceptionPreview         = types.ExceptionPreview
	BusinessDayInfo          = types.BusinessDayInfo
	ValidationIssue          = types.ValidationIssue
)

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
		MaxPreviewCount:          100,                  // Maximum number of executions to preview
		MaxLookahead:             365 * 24 * time.Hour, // Look ahead 1 year
		EnableCaching:            true,
		CacheTTL:                 24 * time.Hour, // Cache for 24 hours
		CacheConfig: &types.CacheConfig{
			Enabled:         true,
			TTL:             time.Hour * 24, // 24 hours
			MaxEntries:      10000,
			CleanupInterval: time.Hour, // 1 hour
		},
	}
}
