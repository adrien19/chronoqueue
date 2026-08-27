package calendar

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
)

func TestValidateSchedule_RejectsUnsupportedCustomRules(t *testing.T) {
	engine := NewDefaultEngine()
	err := engine.ValidateSchedule(context.Background(), &schedulepb.CalendarSchedule{
		Type:     schedulepb.CalendarSchedule_CUSTOM,
		Timezone: "UTC",
		Rules: []*schedulepb.CalendarRule{{
			Rule: &schedulepb.CalendarRule_Custom{Custom: &schedulepb.CustomRule{RuleType: "expression"}},
		}},
	})
	require.ErrorContains(t, err, "custom calendar schedules are not supported")
}

func TestBusinessDaysScheduleUsesInlineCalendarByID(t *testing.T) {
	engine := NewDefaultEngine()
	schedule := &schedulepb.CalendarSchedule{
		Type:     schedulepb.CalendarSchedule_BUSINESS_DAYS,
		Timezone: "UTC",
		Rules: []*schedulepb.CalendarRule{{
			Rule:           &schedulepb.CalendarRule_BusinessDays{BusinessDays: &schedulepb.BusinessDaysRule{BusinessCalendarId: "company-calendar"}},
			ExecutionTimes: []*schedulepb.TimeOfDay{{Hour: 9}},
		}},
		BusinessCalendar: &schedulepb.BusinessCalendar{
			CalendarId:  "company-calendar",
			WeekendDays: []int32{6, 7},
			Timezone:    "UTC",
		},
	}

	require.NoError(t, engine.ValidateSchedule(context.Background(), schedule))
	next, err := engine.CalculateNextRun(context.Background(), schedule, time.Date(2026, time.August, 28, 10, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, time.August, 31, 9, 0, 0, 0, time.UTC), *next)
}

func TestValidateSchedule_RequiresMatchingRuleType(t *testing.T) {
	engine := NewDefaultEngine()
	err := engine.ValidateSchedule(context.Background(), &schedulepb.CalendarSchedule{
		Type:     schedulepb.CalendarSchedule_DAILY,
		Timezone: "UTC",
		Rules: []*schedulepb.CalendarRule{{
			Rule: &schedulepb.CalendarRule_Weekly{Weekly: &schedulepb.WeeklyRule{DaysOfWeek: []int32{1}}},
		}},
	})
	require.ErrorContains(t, err, "does not match calendar schedule type DAILY")
}

func TestValidateSchedule_AcceptsDailyRule(t *testing.T) {
	engine := NewDefaultEngine()
	err := engine.ValidateSchedule(context.Background(), &schedulepb.CalendarSchedule{
		Type:     schedulepb.CalendarSchedule_DAILY,
		Timezone: "UTC",
		Rules: []*schedulepb.CalendarRule{{
			Rule: &schedulepb.CalendarRule_Daily{Daily: &schedulepb.DailyRule{DayInterval: 1}},
		}},
	})
	require.NoError(t, err)
}
