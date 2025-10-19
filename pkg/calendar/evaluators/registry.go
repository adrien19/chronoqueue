package evaluators

import (
	"context"
	"fmt"
	"time"

	schedule "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/pkg/calendar"
)

// Registry manages all calendar rule evaluators
type Registry struct {
	evaluators map[calendar.RuleType]calendar.RuleEvaluator
}

// NewRegistry creates a new evaluator registry with default evaluators
func NewRegistry() *Registry {
	registry := &Registry{
		evaluators: make(map[calendar.RuleType]calendar.RuleEvaluator),
	}

	// Register default evaluators
	registry.RegisterEvaluator(NewMonthlyEvaluator())
	registry.RegisterEvaluator(NewWeeklyEvaluator())
	registry.RegisterEvaluator(NewDailyEvaluator())
	registry.RegisterEvaluator(NewYearlyEvaluator())
	registry.RegisterEvaluator(NewCustomEvaluator())

	return registry
}

// NewRegistryWithBusinessCalendar creates a new registry with business calendar support
func NewRegistryWithBusinessCalendar(provider calendar.BusinessCalendarProvider) *Registry {
	registry := NewRegistry()

	// Add business days evaluator with provider
	registry.RegisterEvaluator(NewBusinessDaysEvaluator(provider))

	return registry
}

// RegisterEvaluator registers a rule evaluator
func (r *Registry) RegisterEvaluator(evaluator calendar.RuleEvaluator) {
	r.evaluators[evaluator.GetRuleType()] = evaluator
}

// UnregisterEvaluator unregisters a rule evaluator
func (r *Registry) UnregisterEvaluator(ruleType calendar.RuleType) {
	delete(r.evaluators, ruleType)
}

// GetEvaluator returns the evaluator for a specific rule type
func (r *Registry) GetEvaluator(ruleType calendar.RuleType) (calendar.RuleEvaluator, error) {
	evaluator, exists := r.evaluators[ruleType]
	if !exists {
		return nil, calendar.ErrRuleEvaluatorNotFound.WithDetails(fmt.Sprintf("no evaluator found for rule type: %s", ruleType.String()))
	}
	return evaluator, nil
}

// GetRegisteredTypes returns all registered rule types
func (r *Registry) GetRegisteredTypes() []calendar.RuleType {
	var types []calendar.RuleType
	for ruleType := range r.evaluators {
		types = append(types, ruleType)
	}
	return types
}

// EvaluateRule evaluates a calendar rule using the appropriate evaluator
func (r *Registry) EvaluateRule(ctx context.Context, rule *schedule.CalendarRule, from time.Time, timezone *time.Location) (*time.Time, error) {
	ruleType := r.getRuleTypeFromCalendarRule(rule)
	evaluator, err := r.GetEvaluator(ruleType)
	if err != nil {
		return nil, err
	}

	return evaluator.Evaluate(ctx, rule, from, timezone)
}

// EvaluateRuleMultiple evaluates a calendar rule for multiple execution times
func (r *Registry) EvaluateRuleMultiple(ctx context.Context, rule *schedule.CalendarRule, from time.Time, timezone *time.Location, count int) ([]time.Time, error) {
	ruleType := r.getRuleTypeFromCalendarRule(rule)
	evaluator, err := r.GetEvaluator(ruleType)
	if err != nil {
		return nil, err
	}

	return evaluator.EvaluateMultiple(ctx, rule, from, timezone, count)
}

// ValidateRule validates a calendar rule using the appropriate evaluator
func (r *Registry) ValidateRule(ctx context.Context, rule *schedule.CalendarRule) error {
	ruleType := r.getRuleTypeFromCalendarRule(rule)
	evaluator, err := r.GetEvaluator(ruleType)
	if err != nil {
		return err
	}

	return evaluator.Validate(ctx, rule)
}

// ValidateAllRules validates all rules in a calendar schedule
func (r *Registry) ValidateAllRules(ctx context.Context, calendarSchedule *schedule.CalendarSchedule) error {
	for i, rule := range calendarSchedule.Rules {
		if err := r.ValidateRule(ctx, rule); err != nil {
			return fmt.Errorf("rule %d validation failed: %w", i, err)
		}
	}
	return nil
}

// getRuleTypeFromCalendarRule determines the rule type from a calendar rule
func (r *Registry) getRuleTypeFromCalendarRule(rule *schedule.CalendarRule) calendar.RuleType {
	switch rule.Rule.(type) {
	case *schedule.CalendarRule_Monthly:
		return calendar.RuleTypeMonthly
	case *schedule.CalendarRule_Weekly:
		return calendar.RuleTypeWeekly
	case *schedule.CalendarRule_Daily:
		return calendar.RuleTypeDaily
	case *schedule.CalendarRule_Yearly:
		return calendar.RuleTypeYearly
	case *schedule.CalendarRule_BusinessDays:
		return calendar.RuleTypeBusinessDays
	case *schedule.CalendarRule_Custom:
		return calendar.RuleTypeCustom
	default:
		return calendar.RuleTypeCustom // Default to custom for unknown types
	}
}

// GetEvaluatorInfo returns information about all registered evaluators
func (r *Registry) GetEvaluatorInfo() []EvaluatorInfo {
	var info []EvaluatorInfo
	for ruleType, evaluator := range r.evaluators {
		info = append(info, EvaluatorInfo{
			RuleType:    ruleType,
			Name:        ruleType.String(),
			Description: r.getEvaluatorDescription(ruleType),
			Evaluator:   evaluator,
		})
	}
	return info
}

// getEvaluatorDescription returns a description for each evaluator type
func (r *Registry) getEvaluatorDescription(ruleType calendar.RuleType) string {
	switch ruleType {
	case calendar.RuleTypeMonthly:
		return "Handles monthly scheduling rules including day of month, weekday of month, last weekday, and last day of month"
	case calendar.RuleTypeWeekly:
		return "Handles weekly scheduling rules with specific days of the week and interval support"
	case calendar.RuleTypeDaily:
		return "Handles daily scheduling rules with day intervals and weekdays-only options"
	case calendar.RuleTypeYearly:
		return "Handles yearly scheduling rules with month/day specifications and leap year handling"
	case calendar.RuleTypeBusinessDays:
		return "Handles business day scheduling with business calendar integration and day offsets"
	case calendar.RuleTypeCustom:
		return "Handles custom scheduling rules with pluggable processors"
	default:
		return "Unknown evaluator type"
	}
}

// EvaluatorInfo contains information about a registered evaluator
type EvaluatorInfo struct {
	RuleType    calendar.RuleType      `json:"rule_type"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Evaluator   calendar.RuleEvaluator `json:"-"` // Don't serialize the evaluator itself
}

// Clone creates a copy of the registry
func (r *Registry) Clone() *Registry {
	clone := &Registry{
		evaluators: make(map[calendar.RuleType]calendar.RuleEvaluator),
	}

	for ruleType, evaluator := range r.evaluators {
		clone.evaluators[ruleType] = evaluator
	}

	return clone
}

// DefaultRegistry returns a registry with all default evaluators
func DefaultRegistry() *Registry {
	return NewRegistry()
}

// DefaultRegistryWithBusinessCalendar returns a registry with business calendar support
func DefaultRegistryWithBusinessCalendar(provider calendar.BusinessCalendarProvider) *Registry {
	return NewRegistryWithBusinessCalendar(provider)
}
