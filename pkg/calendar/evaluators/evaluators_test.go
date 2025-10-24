package evaluators

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schedule "github.com/adrien19/chronoqueue/api/schedule/v1"
)

// TestBasicRuleEvaluators tests the basic functionality of each rule evaluator
func TestBasicRuleEvaluators(t *testing.T) {
	ctx := context.Background()
	timezone, err := time.LoadLocation("UTC")
	require.NoError(t, err)

	tests := []struct {
		name     string
		rule     *schedule.CalendarRule
		from     time.Time
		expected bool // whether we expect a next execution time
	}{
		{
			name: "Monthly rule - 15th of month",
			rule: &schedule.CalendarRule{
				Rule: &schedule.CalendarRule_Monthly{
					Monthly: &schedule.MonthlyRule{
						DayType:  schedule.MonthlyRule_DAY_OF_MONTH,
						DayValue: 15,
					},
				},
				ExecutionTimes: []*schedule.TimeOfDay{
					{Hour: 12, Minute: 0, Second: 0},
				},
			},
			from:     time.Date(2025, 10, 19, 10, 0, 0, 0, time.UTC),
			expected: true,
		},
		{
			name: "Weekly rule - Monday and Friday",
			rule: &schedule.CalendarRule{
				Rule: &schedule.CalendarRule_Weekly{
					Weekly: &schedule.WeeklyRule{
						DaysOfWeek: []int32{1, 5}, // Monday and Friday
					},
				},
				ExecutionTimes: []*schedule.TimeOfDay{
					{Hour: 9, Minute: 0, Second: 0},
				},
			},
			from:     time.Date(2025, 10, 19, 10, 0, 0, 0, time.UTC), // Sunday
			expected: true,
		},
		{
			name: "Daily rule - every day",
			rule: &schedule.CalendarRule{
				Rule: &schedule.CalendarRule_Daily{
					Daily: &schedule.DailyRule{
						DayInterval: 1,
					},
				},
				ExecutionTimes: []*schedule.TimeOfDay{
					{Hour: 8, Minute: 30, Second: 0},
				},
			},
			from:     time.Date(2025, 10, 19, 10, 0, 0, 0, time.UTC),
			expected: true,
		},
		{
			name: "Yearly rule - December 25th",
			rule: &schedule.CalendarRule{
				Rule: &schedule.CalendarRule_Yearly{
					Yearly: &schedule.YearlyRule{
						Month: 12,
						Day:   25,
					},
				},
				ExecutionTimes: []*schedule.TimeOfDay{
					{Hour: 0, Minute: 0, Second: 0},
				},
			},
			from:     time.Date(2025, 10, 19, 10, 0, 0, 0, time.UTC),
			expected: true,
		},
	}

	registry := DefaultRegistry()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test validation
			err := registry.ValidateRule(ctx, tt.rule)
			assert.NoError(t, err, "Rule should be valid")

			// Test evaluation
			nextTime, err := registry.EvaluateRule(ctx, tt.rule, tt.from, timezone)
			assert.NoError(t, err, "Rule evaluation should not error")

			if tt.expected {
				assert.NotNil(t, nextTime, "Should have next execution time")
				if nextTime != nil {
					assert.True(t, nextTime.After(tt.from), "Next execution should be after from time")
				}
			}

			// Test multiple evaluations
			if tt.expected {
				times, err := registry.EvaluateRuleMultiple(ctx, tt.rule, tt.from, timezone, 3)
				assert.NoError(t, err, "Multiple rule evaluation should not error")
				assert.True(t, len(times) > 0, "Should have at least one execution time")

				// Verify times are in ascending order
				for i := 1; i < len(times); i++ {
					assert.True(t, times[i].After(times[i-1]), "Execution times should be in ascending order")
				}
			}
		})
	}
}

// TestRuleEvaluatorRegistry tests the rule evaluator registry functionality
func TestRuleEvaluatorRegistry(t *testing.T) {
	registry := DefaultRegistry()

	// Test that all expected evaluators are registered
	registeredTypes := registry.GetRegisteredTypes()
	expectedTypes := []string{"monthly", "weekly", "daily", "yearly", "custom"}

	for _, expectedType := range expectedTypes {
		found := false
		for _, registeredType := range registeredTypes {
			if registeredType.String() == expectedType {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected evaluator type %s should be registered", expectedType)
	}

	// Test evaluator info
	info := registry.GetEvaluatorInfo()
	assert.True(t, len(info) >= 5, "Should have at least 5 evaluators registered")

	for _, evaluatorInfo := range info {
		assert.NotEmpty(t, evaluatorInfo.Name, "Evaluator should have a name")
		assert.NotEmpty(t, evaluatorInfo.Description, "Evaluator should have a description")
		assert.NotNil(t, evaluatorInfo.Evaluator, "Evaluator should not be nil")
	}
}

// TestValidationRules tests various validation scenarios
func TestValidationRules(t *testing.T) {
	ctx := context.Background()
	registry := DefaultRegistry()

	tests := []struct {
		name          string
		rule          *schedule.CalendarRule
		shouldError   bool
		errorContains string
	}{
		{
			name: "Valid monthly rule",
			rule: &schedule.CalendarRule{
				Rule: &schedule.CalendarRule_Monthly{
					Monthly: &schedule.MonthlyRule{
						DayType:  schedule.MonthlyRule_DAY_OF_MONTH,
						DayValue: 15,
					},
				},
				ExecutionTimes: []*schedule.TimeOfDay{
					{Hour: 12, Minute: 0, Second: 0},
				},
			},
			shouldError: false,
		},
		{
			name: "Invalid monthly rule - day out of range",
			rule: &schedule.CalendarRule{
				Rule: &schedule.CalendarRule_Monthly{
					Monthly: &schedule.MonthlyRule{
						DayType:  schedule.MonthlyRule_DAY_OF_MONTH,
						DayValue: 32, // Invalid day
					},
				},
				ExecutionTimes: []*schedule.TimeOfDay{
					{Hour: 12, Minute: 0, Second: 0},
				},
			},
			shouldError:   true,
			errorContains: "day_value must be between 1 and 31",
		},
		{
			name: "Invalid execution time - hour out of range",
			rule: &schedule.CalendarRule{
				Rule: &schedule.CalendarRule_Daily{
					Daily: &schedule.DailyRule{
						DayInterval: 1,
					},
				},
				ExecutionTimes: []*schedule.TimeOfDay{
					{Hour: 25, Minute: 0, Second: 0}, // Invalid hour
				},
			},
			shouldError:   true,
			errorContains: "invalid hour",
		},
		{
			name: "Valid weekly rule",
			rule: &schedule.CalendarRule{
				Rule: &schedule.CalendarRule_Weekly{
					Weekly: &schedule.WeeklyRule{
						DaysOfWeek: []int32{1, 2, 3, 4, 5}, // Weekdays
					},
				},
				ExecutionTimes: []*schedule.TimeOfDay{
					{Hour: 9, Minute: 0, Second: 0},
				},
			},
			shouldError: false,
		},
		{
			name: "Invalid weekly rule - no days specified",
			rule: &schedule.CalendarRule{
				Rule: &schedule.CalendarRule_Weekly{
					Weekly: &schedule.WeeklyRule{
						DaysOfWeek: []int32{}, // No days specified
					},
				},
				ExecutionTimes: []*schedule.TimeOfDay{
					{Hour: 9, Minute: 0, Second: 0},
				},
			},
			shouldError:   true,
			errorContains: "no days of week specified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := registry.ValidateRule(ctx, tt.rule)

			if tt.shouldError {
				assert.Error(t, err, "Expected validation error")
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains, "Error should contain expected text")
				}
			} else {
				assert.NoError(t, err, "Expected no validation error")
			}
		})
	}
}

// BenchmarkRuleEvaluation benchmarks the performance of rule evaluation
func BenchmarkRuleEvaluation(b *testing.B) {
	ctx := context.Background()
	timezone, _ := time.LoadLocation("UTC")
	from := time.Date(2025, 10, 19, 10, 0, 0, 0, time.UTC)

	registry := DefaultRegistry()

	rule := &schedule.CalendarRule{
		Rule: &schedule.CalendarRule_Daily{
			Daily: &schedule.DailyRule{
				DayInterval: 1,
			},
		},
		ExecutionTimes: []*schedule.TimeOfDay{
			{Hour: 9, Minute: 0, Second: 0},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := registry.EvaluateRule(ctx, rule, from, timezone)
		if err != nil {
			b.Fatal(err)
		}
	}
}
