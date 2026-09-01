package calendar

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

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

func TestCalculateNextRun_SearchesPastSkippedRecurrences(t *testing.T) {
	engine := NewDefaultEngine()
	from := time.Date(2026, time.August, 30, 8, 0, 0, 0, time.UTC)
	schedule := dailySchedule()
	schedule.Exceptions = []*schedulepb.CalendarException{
		CreateSkipException(time.Date(2026, time.August, 30, 0, 0, 0, 0, time.UTC), "skip"),
		CreateSkipException(time.Date(2026, time.August, 31, 0, 0, 0, 0, time.UTC), "skip"),
	}

	next, err := engine.CalculateNextRun(context.Background(), schedule, from)
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, time.September, 1, 9, 0, 0, 0, time.UTC), *next)
}

func TestCalculateNextRun_IncludesExtraOnNonRuleDate(t *testing.T) {
	engine := NewDefaultEngine()
	from := time.Date(2026, time.August, 30, 10, 0, 0, 0, time.UTC)
	schedule := dailySchedule()
	schedule.Rules[0].GetDaily().DayInterval = 7
	schedule.Rules[0].GetDaily().StartDate = timestamppb.New(time.Date(2026, time.August, 30, 0, 0, 0, 0, time.UTC))
	schedule.Exceptions = []*schedulepb.CalendarException{CreateExtraException(
		time.Date(2026, time.August, 31, 0, 0, 0, 0, time.UTC),
		[]*schedulepb.TimeOfDay{{Hour: 12}}, "extra",
	)}

	next, err := engine.CalculateNextRun(context.Background(), schedule, from)
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC), *next)
}

func dailySchedule() *schedulepb.CalendarSchedule {
	return &schedulepb.CalendarSchedule{
		Type: schedulepb.CalendarSchedule_DAILY, Timezone: "UTC",
		Rules: []*schedulepb.CalendarRule{{
			Rule:           &schedulepb.CalendarRule_Daily{Daily: &schedulepb.DailyRule{DayInterval: 1}},
			ExecutionTimes: []*schedulepb.TimeOfDay{{Hour: 9}},
		}},
	}
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
