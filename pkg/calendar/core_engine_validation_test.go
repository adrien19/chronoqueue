package calendar

import (
	"context"
	"testing"

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
