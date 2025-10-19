package evaluators

import (
	"context"
	"fmt"
	"time"

	schedule "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/pkg/calendar"
)

// CustomEvaluator handles custom calendar rules
type CustomEvaluator struct {
	// processors maps rule types to their custom processors
	processors map[string]CustomRuleProcessor
}

// CustomRuleProcessor interface for processing custom rules
type CustomRuleProcessor interface {
	// Process evaluates a custom rule and returns the next execution time
	Process(ctx context.Context, rule *schedule.CustomRule, from time.Time, timezone *time.Location) (*time.Time, error)

	// ProcessMultiple evaluates a custom rule and returns multiple execution times
	ProcessMultiple(ctx context.Context, rule *schedule.CustomRule, from time.Time, timezone *time.Location, count int) ([]time.Time, error)

	// Validate validates a custom rule
	Validate(ctx context.Context, rule *schedule.CustomRule) error

	// GetDescription returns a description of what this processor handles
	GetDescription() string
}

// NewCustomEvaluator creates a new custom rule evaluator
func NewCustomEvaluator() *CustomEvaluator {
	return &CustomEvaluator{
		processors: make(map[string]CustomRuleProcessor),
	}
}

// RegisterProcessor registers a custom rule processor
func (e *CustomEvaluator) RegisterProcessor(ruleType string, processor CustomRuleProcessor) {
	e.processors[ruleType] = processor
}

// UnregisterProcessor unregisters a custom rule processor
func (e *CustomEvaluator) UnregisterProcessor(ruleType string) {
	delete(e.processors, ruleType)
}

// GetRegisteredProcessors returns all registered processor types
func (e *CustomEvaluator) GetRegisteredProcessors() []string {
	var types []string
	for ruleType := range e.processors {
		types = append(types, ruleType)
	}
	return types
}

// Evaluate returns the next execution time for a custom rule after the given time
func (e *CustomEvaluator) Evaluate(ctx context.Context, rule *schedule.CalendarRule, from time.Time, timezone *time.Location) (*time.Time, error) {
	customRule := rule.GetCustom()
	if customRule == nil {
		return nil, calendar.ErrInvalidRule.WithDetails("custom rule is nil")
	}

	// Check if rule is valid for the current time range
	if rule.ValidFrom != nil && from.Before(rule.ValidFrom.AsTime()) {
		from = rule.ValidFrom.AsTime()
	}
	if rule.ValidUntil != nil && from.After(rule.ValidUntil.AsTime()) {
		return nil, calendar.ErrNoExecutionTime.WithDetails("rule validity period has expired")
	}

	// Find the appropriate processor
	processor, exists := e.processors[customRule.RuleType]
	if !exists {
		return nil, calendar.ErrRuleEvaluatorNotFound.WithDetails(fmt.Sprintf("no processor found for custom rule type: %s", customRule.RuleType))
	}

	return processor.Process(ctx, customRule, from, timezone)
}

// EvaluateMultiple returns the next N execution times for a custom rule
func (e *CustomEvaluator) EvaluateMultiple(ctx context.Context, rule *schedule.CalendarRule, from time.Time, timezone *time.Location, count int) ([]time.Time, error) {
	customRule := rule.GetCustom()
	if customRule == nil {
		return nil, calendar.ErrInvalidRule.WithDetails("custom rule is nil")
	}

	// Check if rule is valid for the current time range
	if rule.ValidFrom != nil && from.Before(rule.ValidFrom.AsTime()) {
		from = rule.ValidFrom.AsTime()
	}
	if rule.ValidUntil != nil && from.After(rule.ValidUntil.AsTime()) {
		return nil, calendar.ErrNoExecutionTime.WithDetails("rule validity period has expired")
	}

	// Find the appropriate processor
	processor, exists := e.processors[customRule.RuleType]
	if !exists {
		return nil, calendar.ErrRuleEvaluatorNotFound.WithDetails(fmt.Sprintf("no processor found for custom rule type: %s", customRule.RuleType))
	}

	return processor.ProcessMultiple(ctx, customRule, from, timezone, count)
}

// Validate checks if a custom rule is valid
func (e *CustomEvaluator) Validate(ctx context.Context, rule *schedule.CalendarRule) error {
	customRule := rule.GetCustom()
	if customRule == nil {
		return calendar.ErrInvalidRule.WithDetails("custom rule is nil")
	}

	// Validate rule type
	if customRule.RuleType == "" {
		return calendar.ErrInvalidRule.WithDetails("custom rule type is required")
	}

	// Check if processor exists
	processor, exists := e.processors[customRule.RuleType]
	if !exists {
		return calendar.ErrRuleEvaluatorNotFound.WithDetails(fmt.Sprintf("no processor found for custom rule type: %s", customRule.RuleType))
	}

	// Validate with the specific processor
	return processor.Validate(ctx, customRule)
}

// GetRuleType returns the type of rule this evaluator handles
func (e *CustomEvaluator) GetRuleType() calendar.RuleType {
	return calendar.RuleTypeCustom
}

// Example custom rule processors

// CronExpressionProcessor handles cron-like expressions in custom rules
type CronExpressionProcessor struct{}

// NewCronExpressionProcessor creates a new cron expression processor
func NewCronExpressionProcessor() *CronExpressionProcessor {
	return &CronExpressionProcessor{}
}

// Process evaluates a cron expression custom rule
func (p *CronExpressionProcessor) Process(ctx context.Context, rule *schedule.CustomRule, from time.Time, timezone *time.Location) (*time.Time, error) {
	// This would integrate with a cron parser library
	// For now, return an error indicating it's not implemented
	return nil, fmt.Errorf("cron expression processor not yet implemented")
}

// ProcessMultiple evaluates a cron expression custom rule for multiple times
func (p *CronExpressionProcessor) ProcessMultiple(ctx context.Context, rule *schedule.CustomRule, from time.Time, timezone *time.Location, count int) ([]time.Time, error) {
	return nil, fmt.Errorf("cron expression processor not yet implemented")
}

// Validate validates a cron expression custom rule
func (p *CronExpressionProcessor) Validate(ctx context.Context, rule *schedule.CustomRule) error {
	if rule.Expression == "" {
		return calendar.ErrInvalidRule.WithDetails("cron expression is required")
	}
	// Additional cron expression validation would go here
	return nil
}

// GetDescription returns a description of the cron expression processor
func (p *CronExpressionProcessor) GetDescription() string {
	return "Processes cron-like expressions for custom scheduling rules"
}

// IntervalProcessor handles simple interval-based custom rules
type IntervalProcessor struct{}

// NewIntervalProcessor creates a new interval processor
func NewIntervalProcessor() *IntervalProcessor {
	return &IntervalProcessor{}
}

// Process evaluates an interval custom rule
func (p *IntervalProcessor) Process(ctx context.Context, rule *schedule.CustomRule, from time.Time, timezone *time.Location) (*time.Time, error) {
	intervalStr, exists := rule.Parameters["interval"]
	if !exists {
		return nil, calendar.ErrInvalidRule.WithDetails("interval parameter is required")
	}

	// Parse interval (e.g., "30m", "1h", "2d")
	// This is a simplified implementation
	switch intervalStr {
	case "30m":
		next := from.Add(30 * time.Minute)
		return &next, nil
	case "1h":
		next := from.Add(1 * time.Hour)
		return &next, nil
	case "2d":
		next := from.Add(48 * time.Hour)
		return &next, nil
	default:
		return nil, calendar.ErrInvalidRule.WithDetails(fmt.Sprintf("unsupported interval: %s", intervalStr))
	}
}

// ProcessMultiple evaluates an interval custom rule for multiple times
func (p *IntervalProcessor) ProcessMultiple(ctx context.Context, rule *schedule.CustomRule, from time.Time, timezone *time.Location, count int) ([]time.Time, error) {
	var results []time.Time
	current := from

	for len(results) < count {
		next, err := p.Process(ctx, rule, current, timezone)
		if err != nil {
			return nil, err
		}
		if next == nil {
			break
		}

		results = append(results, *next)
		current = *next
	}

	return results, nil
}

// Validate validates an interval custom rule
func (p *IntervalProcessor) Validate(ctx context.Context, rule *schedule.CustomRule) error {
	intervalStr, exists := rule.Parameters["interval"]
	if !exists {
		return calendar.ErrInvalidRule.WithDetails("interval parameter is required")
	}

	// Validate supported intervals
	supportedIntervals := map[string]bool{
		"30m": true,
		"1h":  true,
		"2d":  true,
	}

	if !supportedIntervals[intervalStr] {
		return calendar.ErrInvalidRule.WithDetails(fmt.Sprintf("unsupported interval: %s", intervalStr))
	}

	return nil
}

// GetDescription returns a description of the interval processor
func (p *IntervalProcessor) GetDescription() string {
	return "Processes simple interval-based scheduling rules (30m, 1h, 2d)"
}
