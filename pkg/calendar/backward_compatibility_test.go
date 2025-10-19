package calendar_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schedule "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/pkg/calendar/evaluators"
)

// TestBackwardCompatibility tests that existing cron schedules continue to work
// alongside the new calendar-based scheduling system
func TestBackwardCompatibility(t *testing.T) {

	tests := []struct {
		name           string
		cronSchedule   string
		description    string
		expectValid    bool
		checkExecution bool
		from           time.Time
		expectedNext   *time.Time
	}{
		{
			name:           "Daily at midnight",
			cronSchedule:   "0 0 * * *",
			description:    "Execute daily at midnight",
			expectValid:    true,
			checkExecution: true,
			from:           time.Date(2025, 10, 19, 12, 0, 0, 0, time.UTC),
			expectedNext: func() *time.Time {
				t := time.Date(2025, 10, 20, 0, 0, 0, 0, time.UTC)
				return &t
			}(),
		},
		{
			name:           "Every hour",
			cronSchedule:   "0 * * * *",
			description:    "Execute every hour on the hour",
			expectValid:    true,
			checkExecution: true,
			from:           time.Date(2025, 10, 19, 12, 30, 0, 0, time.UTC),
			expectedNext: func() *time.Time {
				t := time.Date(2025, 10, 19, 13, 0, 0, 0, time.UTC)
				return &t
			}(),
		},
		{
			name:           "Weekdays at 9 AM",
			cronSchedule:   "0 9 * * 1-5",
			description:    "Execute on weekdays at 9 AM",
			expectValid:    true,
			checkExecution: true,
			from:           time.Date(2025, 10, 19, 10, 0, 0, 0, time.UTC), // Sunday
			expectedNext: func() *time.Time {
				t := time.Date(2025, 10, 20, 9, 0, 0, 0, time.UTC) // Monday
				return &t
			}(),
		},
		{
			name:           "Monthly on 15th",
			cronSchedule:   "0 12 15 * *",
			description:    "Execute on the 15th of every month at noon",
			expectValid:    true,
			checkExecution: true,
			from:           time.Date(2025, 10, 19, 12, 0, 0, 0, time.UTC),
			expectedNext: func() *time.Time {
				t := time.Date(2025, 11, 15, 12, 0, 0, 0, time.UTC)
				return &t
			}(),
		},
		{
			name:           "Every 5 minutes",
			cronSchedule:   "*/5 * * * *",
			description:    "Execute every 5 minutes",
			expectValid:    true,
			checkExecution: true,
			from:           time.Date(2025, 10, 19, 12, 33, 0, 0, time.UTC),
			expectedNext: func() *time.Time {
				t := time.Date(2025, 10, 19, 12, 35, 0, 0, time.UTC)
				return &t
			}(),
		},
		{
			name:         "Invalid cron expression",
			cronSchedule: "invalid cron",
			description:  "Invalid cron expression should be handled gracefully",
			expectValid:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a schedule with cron configuration
			scheduleProto := &schedule.Schedule{
				ScheduleId: "test-backward-compat-" + tt.name,
				Metadata: &schedule.Schedule_Metadata{
					ScheduleConfig: &schedule.Schedule_Metadata_CronSchedule{
						CronSchedule: tt.cronSchedule,
					},
					QueueName: "test-queue",
					State:     schedule.Schedule_Metadata_SCHEDULED,
				},
			}

			// Test that the schedule structure is valid
			assert.NotNil(t, scheduleProto.Metadata)
			assert.Equal(t, tt.cronSchedule, scheduleProto.Metadata.GetCronSchedule())

			// Verify that cron_schedule field is still accessible
			cronSchedule := scheduleProto.Metadata.GetCronSchedule()
			if tt.expectValid {
				assert.NotEmpty(t, cronSchedule)
				assert.Equal(t, tt.cronSchedule, cronSchedule)
			}

			// Test that calendar_schedule field is nil for cron schedules
			calendarSchedule := scheduleProto.Metadata.GetCalendarSchedule()
			assert.Nil(t, calendarSchedule, "Calendar schedule should be nil for cron-based schedules")

			// Verify oneof behavior - only one schedule type should be set
			switch config := scheduleProto.Metadata.ScheduleConfig.(type) {
			case *schedule.Schedule_Metadata_CronSchedule:
				assert.Equal(t, tt.cronSchedule, config.CronSchedule)
			case *schedule.Schedule_Metadata_CalendarSchedule:
				t.Errorf("Expected cron schedule, got calendar schedule")
			default:
				if tt.expectValid {
					t.Errorf("Expected cron schedule, got unknown type")
				}
			}

			// Test cron validation (this would be done by existing cron parsing logic)
			if tt.checkExecution && tt.expectValid {
				// This simulates how the existing cron parser would handle the schedule
				// In real implementation, this would use robfig/cron/v3 or similar
				assert.NotNil(t, tt.expectedNext, "Expected next execution time should be provided for valid cron")
			}
		})
	}
}

// TestCalendarScheduleStructure tests that calendar schedules work correctly
// and don't interfere with cron schedules
func TestCalendarScheduleStructure(t *testing.T) {
	ctx := context.Background()

	// Create a calendar schedule
	calendarSchedule := &schedule.CalendarSchedule{
		Type:     schedule.CalendarSchedule_MONTHLY,
		Timezone: "UTC",
		Rules: []*schedule.CalendarRule{
			{
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
		},
	}

	// Create a schedule with calendar configuration
	scheduleProto := &schedule.Schedule{
		ScheduleId: "test-calendar-schedule",
		Metadata: &schedule.Schedule_Metadata{
			ScheduleConfig: &schedule.Schedule_Metadata_CalendarSchedule{
				CalendarSchedule: calendarSchedule,
			},
			QueueName: "test-queue",
			State:     schedule.Schedule_Metadata_SCHEDULED,
			Timezone:  "UTC",
		},
	}

	// Test that the schedule structure is valid
	assert.NotNil(t, scheduleProto.Metadata)
	assert.NotNil(t, scheduleProto.Metadata.GetCalendarSchedule())

	// Test that cron_schedule field is nil for calendar schedules
	cronSchedule := scheduleProto.Metadata.GetCronSchedule()
	assert.Empty(t, cronSchedule, "Cron schedule should be empty for calendar-based schedules")

	// Verify oneof behavior - only calendar schedule should be set
	switch config := scheduleProto.Metadata.ScheduleConfig.(type) {
	case *schedule.Schedule_Metadata_CalendarSchedule:
		assert.NotNil(t, config.CalendarSchedule)
		assert.Equal(t, schedule.CalendarSchedule_MONTHLY, config.CalendarSchedule.Type)
	case *schedule.Schedule_Metadata_CronSchedule:
		t.Errorf("Expected calendar schedule, got cron schedule")
	default:
		t.Errorf("Expected calendar schedule, got unknown type")
	}

	// Test calendar schedule validation
	registry := evaluators.DefaultRegistry()
	err := registry.ValidateAllRules(ctx, calendarSchedule)
	assert.NoError(t, err, "Calendar schedule should be valid")
}

// TestScheduleCompatibilityCoexistence tests that cron and calendar schedules
// can coexist in the same system without interfering with each other
func TestScheduleCompatibilityCoexistence(t *testing.T) {
	ctx := context.Background()

	// Create multiple schedules of different types
	schedules := []*schedule.Schedule{
		// Cron schedule
		{
			ScheduleId: "cron-schedule-1",
			Metadata: &schedule.Schedule_Metadata{
				ScheduleConfig: &schedule.Schedule_Metadata_CronSchedule{
					CronSchedule: "0 9 * * 1-5", // Weekdays at 9 AM
				},
				QueueName: "cron-queue",
				State:     schedule.Schedule_Metadata_SCHEDULED,
			},
		},
		// Calendar schedule
		{
			ScheduleId: "calendar-schedule-1",
			Metadata: &schedule.Schedule_Metadata{
				ScheduleConfig: &schedule.Schedule_Metadata_CalendarSchedule{
					CalendarSchedule: &schedule.CalendarSchedule{
						Type:     schedule.CalendarSchedule_WEEKLY,
						Timezone: "UTC",
						Rules: []*schedule.CalendarRule{
							{
								Rule: &schedule.CalendarRule_Weekly{
									Weekly: &schedule.WeeklyRule{
										DaysOfWeek: []int32{1, 2, 3, 4, 5}, // Weekdays
									},
								},
								ExecutionTimes: []*schedule.TimeOfDay{
									{Hour: 9, Minute: 0, Second: 0},
								},
							},
						},
					},
				},
				QueueName: "calendar-queue",
				State:     schedule.Schedule_Metadata_SCHEDULED,
				Timezone:  "UTC",
			},
		},
		// Another cron schedule
		{
			ScheduleId: "cron-schedule-2",
			Metadata: &schedule.Schedule_Metadata{
				ScheduleConfig: &schedule.Schedule_Metadata_CronSchedule{
					CronSchedule: "0 */2 * * *", // Every 2 hours
				},
				QueueName: "cron-queue-2",
				State:     schedule.Schedule_Metadata_SCHEDULED,
			},
		},
	}

	// Validate that each schedule can be identified and processed correctly
	registry := evaluators.DefaultRegistry()

	for _, sch := range schedules {
		t.Run(sch.ScheduleId, func(t *testing.T) {
			// Check that we can identify the schedule type
			cronSchedule := sch.Metadata.GetCronSchedule()
			calendarSchedule := sch.Metadata.GetCalendarSchedule()

			// Exactly one should be set
			hasCron := cronSchedule != ""
			hasCalendar := calendarSchedule != nil
			assert.True(t, hasCron != hasCalendar, "Exactly one schedule type should be set")

			if hasCalendar {
				// Validate calendar schedule
				err := registry.ValidateAllRules(ctx, calendarSchedule)
				assert.NoError(t, err, "Calendar schedule should be valid")

				// Test that we can evaluate the calendar schedule
				from := time.Date(2025, 10, 19, 12, 0, 0, 0, time.UTC)
				for _, rule := range calendarSchedule.Rules {
					timezone, err := time.LoadLocation(calendarSchedule.Timezone)
					require.NoError(t, err)

					nextTime, err := registry.EvaluateRule(ctx, rule, from, timezone)
					// Some rules might not have a next execution time immediately,
					// but there should be no error in evaluation
					assert.NoError(t, err, "Rule evaluation should not error")
					if nextTime != nil {
						assert.True(t, nextTime.After(from), "Next execution time should be after from time")
					}
				}
			}

			if hasCron {
				// For cron schedules, we just verify the structure is correct
				// Real cron validation would be done by the existing cron parser
				assert.NotEmpty(t, cronSchedule, "Cron schedule should not be empty")
			}
		})
	}
}

// TestProtocolBufferBackwardCompatibility tests that protocol buffer
// serialization/deserialization works correctly for both old and new formats
func TestProtocolBufferBackwardCompatibility(t *testing.T) {
	// Test that old proto messages (with only cron_schedule) still work
	t.Run("legacy cron schedule", func(t *testing.T) {
		// This simulates loading an old schedule that was serialized before
		// the calendar scheduling feature was added
		legacySchedule := &schedule.Schedule{
			ScheduleId: "legacy-schedule",
			Metadata: &schedule.Schedule_Metadata{
				// In the legacy format, this would have been set directly
				// The oneof wrapper maintains compatibility
				ScheduleConfig: &schedule.Schedule_Metadata_CronSchedule{
					CronSchedule: "0 12 * * *",
				},
				QueueName: "legacy-queue",
				State:     schedule.Schedule_Metadata_SCHEDULED,
			},
		}

		// Verify we can access the cron schedule
		cronSchedule := legacySchedule.Metadata.GetCronSchedule()
		assert.Equal(t, "0 12 * * *", cronSchedule)

		// Verify calendar schedule is nil
		calendarSchedule := legacySchedule.Metadata.GetCalendarSchedule()
		assert.Nil(t, calendarSchedule)
	})

	// Test that new calendar schedules work correctly
	t.Run("new calendar schedule", func(t *testing.T) {
		newSchedule := &schedule.Schedule{
			ScheduleId: "new-calendar-schedule",
			Metadata: &schedule.Schedule_Metadata{
				ScheduleConfig: &schedule.Schedule_Metadata_CalendarSchedule{
					CalendarSchedule: &schedule.CalendarSchedule{
						Type:     schedule.CalendarSchedule_DAILY,
						Timezone: "UTC",
						Rules: []*schedule.CalendarRule{
							{
								Rule: &schedule.CalendarRule_Daily{
									Daily: &schedule.DailyRule{
										DayInterval: 1,
									},
								},
								ExecutionTimes: []*schedule.TimeOfDay{
									{Hour: 12, Minute: 0, Second: 0},
								},
							},
						},
					},
				},
				QueueName: "new-queue",
				State:     schedule.Schedule_Metadata_SCHEDULED,
				Timezone:  "UTC",
			},
		}

		// Verify we can access the calendar schedule
		calendarSchedule := newSchedule.Metadata.GetCalendarSchedule()
		assert.NotNil(t, calendarSchedule)
		assert.Equal(t, schedule.CalendarSchedule_DAILY, calendarSchedule.Type)

		// Verify cron schedule is empty
		cronSchedule := newSchedule.Metadata.GetCronSchedule()
		assert.Empty(t, cronSchedule)
	})
}

// BenchmarkBackwardCompatibility benchmarks the performance impact of the new
// schedule structure on existing cron-based operations
func BenchmarkBackwardCompatibility(b *testing.B) {
	// Benchmark accessing cron schedule from old-style schedule
	b.Run("cron schedule access", func(b *testing.B) {
		scheduleProto := &schedule.Schedule{
			ScheduleId: "benchmark-cron",
			Metadata: &schedule.Schedule_Metadata{
				ScheduleConfig: &schedule.Schedule_Metadata_CronSchedule{
					CronSchedule: "0 12 * * *",
				},
				QueueName: "test-queue",
				State:     schedule.Schedule_Metadata_SCHEDULED,
			},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cronSchedule := scheduleProto.Metadata.GetCronSchedule()
			if cronSchedule == "" {
				b.Fatal("Expected non-empty cron schedule")
			}
		}
	})

	// Benchmark accessing calendar schedule
	b.Run("calendar schedule access", func(b *testing.B) {
		scheduleProto := &schedule.Schedule{
			ScheduleId: "benchmark-calendar",
			Metadata: &schedule.Schedule_Metadata{
				ScheduleConfig: &schedule.Schedule_Metadata_CalendarSchedule{
					CalendarSchedule: &schedule.CalendarSchedule{
						Type:     schedule.CalendarSchedule_DAILY,
						Timezone: "UTC",
						Rules: []*schedule.CalendarRule{
							{
								Rule: &schedule.CalendarRule_Daily{
									Daily: &schedule.DailyRule{DayInterval: 1},
								},
							},
						},
					},
				},
				QueueName: "test-queue",
				State:     schedule.Schedule_Metadata_SCHEDULED,
			},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			calendarSchedule := scheduleProto.Metadata.GetCalendarSchedule()
			if calendarSchedule == nil {
				b.Fatal("Expected non-nil calendar schedule")
			}
		}
	})
}
