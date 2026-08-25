package handlers

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservicepb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
)

func TestParseQueueAdvancedOptions(t *testing.T) {
	t.Run("omitted defaults", func(t *testing.T) {
		r := formRequest(nil)
		if config, err := parsePriorityConfig(r); err != nil || config != nil {
			t.Fatalf("priority config = (%v, %v), want nil", config, err)
		}
		if policy, err := parseLeasePolicy(r.FormValue); err != nil || policy.BaseLease != "" {
			t.Fatalf("lease policy = (%v, %v)", policy, err)
		}
		if policy, err := parseRetentionPolicy(r); err != nil || policy != nil {
			t.Fatalf("retention policy = (%v, %v), want nil", policy, err)
		}
	})

	t.Run("full configuration", func(t *testing.T) {
		r := formRequest(url.Values{
			"priority_policy": {"HYBRID"}, "priority_weights": {`{"4":70,"2":20,"0":10}`},
			"age_boost_threshold": {"30m"}, "age_boost_multiplier": {"2"},
			"base_lease": {"30s"}, "max_extension": {"5m"}, "heartbeat_timeout": {"15s"}, "extend_step": {"10s"},
			"retention_mode": {"RETAIN_DURATION"}, "retention_seconds": {"3600"},
		})
		config, err := parsePriorityConfig(r)
		if err != nil || config.GetPolicy() != queuepb.FairnessPolicy_HYBRID || config.GetPriorityWeights()[4] != 70 {
			t.Fatalf("priority config = (%v, %v)", config, err)
		}
		lease, err := parseLeasePolicy(r.FormValue)
		if err != nil || lease.BaseLease != "30s" || lease.ExtendStep != "10s" {
			t.Fatalf("lease policy = (%v, %v)", lease, err)
		}
		retention, err := parseRetentionPolicy(r)
		if err != nil || retention.RetentionSeconds != 3600 {
			t.Fatalf("retention policy = (%v, %v)", retention, err)
		}
	})

	for _, test := range []struct {
		name   string
		values url.Values
		parse  func(*testing.T, url.Values) error
	}{
		{name: "malformed duration", values: url.Values{"base_lease": {"later"}}, parse: func(_ *testing.T, values url.Values) error {
			_, err := parseLeasePolicy(formRequest(values).FormValue)
			return err
		}},
		{name: "missing weights", values: url.Values{"priority_policy": {"WEIGHTED"}}, parse: func(_ *testing.T, values url.Values) error {
			_, err := parsePriorityConfig(formRequest(values))
			return err
		}},
		{name: "invalid weights", values: url.Values{"priority_policy": {"WEIGHTED"}, "priority_weights": {`{"9":0}`}}, parse: func(_ *testing.T, values url.Values) error {
			_, err := parsePriorityConfig(formRequest(values))
			return err
		}},
		{name: "invalid retention combination", values: url.Values{"retention_mode": {"RETAIN_FOREVER"}, "retention_seconds": {"10"}}, parse: func(_ *testing.T, values url.Values) error {
			_, err := parseRetentionPolicy(formRequest(values))
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.parse(t, test.values); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestParseContentTypes(t *testing.T) {
	got, err := parseContentTypes("application/json, application/xml; charset=utf-8")
	if err != nil || len(got) != 2 {
		t.Fatalf("content types = (%v, %v)", got, err)
	}
	if _, err := parseContentTypes("application/json,"); err == nil {
		t.Fatal("expected empty content-type validation error")
	}
	if _, err := parseContentTypes("not a mime"); err == nil {
		t.Fatal("expected malformed content-type validation error")
	}
}

func TestParseMessageHeaders(t *testing.T) {
	if headers, err := parseMessageHeaders(""); err != nil || headers != nil {
		t.Fatalf("absent headers = (%v, %v)", headers, err)
	}
	raw := `[{"key":"trace-id","value_base64":"AP8="},{"key":"trace-id","value_base64":"c2Vjb25k"}]`
	headers, err := parseMessageHeaders(raw)
	if err != nil || len(headers) != 2 || headers[0].Key != headers[1].Key || string(headers[0].Value) != string([]byte{0, 255}) {
		t.Fatalf("ordered headers = (%v, %v)", headers, err)
	}
	invalid := []string{
		`[{"key":"Trace_ID","value_base64":"YQ=="}]`,
		`[{"key":"x-system-token","value_base64":"YQ=="}]`,
		`[{"key":"trace-id","value_base64":"%%%"}]`,
		`[{"key":"trace-id","value_base64":"` + base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", 4097))) + `"}]`,
	}
	for _, raw := range invalid {
		if _, err := parseMessageHeaders(raw); err == nil {
			t.Fatalf("expected header validation error for %.40q", raw)
		}
	}
}

func TestParseBulkMessages(t *testing.T) {
	raw := `[{"message_id":"one","payload":{"value":1},"headers":[{"key":"trace-id","value_base64":"AP8="}],"scheduled_time":"2030-01-01T00:00:00Z","lease_policy":{"base_lease":"30s","max_extension":"5m"}},{"message_id":"two","payload":{"value":2}}]`
	messages, err := parseBulkMessages(raw)
	if err != nil || len(messages) != 2 || messages[0].MessageID != "one" || len(messages[0].Options.Headers) != 1 {
		t.Fatalf("bulk messages = (%v, %v)", messages, err)
	}
	if messages[0].Options.ScheduledTime == nil || messages[0].Options.LeasePolicy.BaseLease != "30s" {
		t.Fatalf("bulk producer options = %+v", messages[0].Options)
	}
	for _, raw := range []string{"[]", `[{}]`, `[{"message_id":"one","payload":[]}]`, `[{"message_id":"one","payload":{},"priority":5}]`} {
		if _, err := parseBulkMessages(raw); err == nil {
			t.Fatalf("expected bulk validation error for %s", raw)
		}
	}
}

func TestBuildBulkMessageResultsDoesNotExposeServerErrors(t *testing.T) {
	response := &queueservicepb.PostMessagesBulkResponse{SuccessfulCount: 1, FailedCount: 2, Results: []*queueservicepb.PostMessagesBulkResponse_MessagePostResult{
		{MessageId: "ok", Success: true},
		{MessageId: "duplicate", ErrorCode: queueservicepb.PostMessagesBulkResponse_MessagePostResult_DUPLICATE_MESSAGE_ID, Error: "database constraint detail"},
		{MessageId: "internal", ErrorCode: queueservicepb.PostMessagesBulkResponse_MessagePostResult_INTERNAL_ERROR, Error: "private stack trace"},
	}}
	view := buildBulkMessageResults(response)
	if view.SuccessfulCount != 1 || view.FailedCount != 2 || view.Results[1].Error != "Message ID already exists" || view.Results[2].Error != "Message could not be posted" {
		t.Fatalf("result view = %+v", view)
	}
}

func TestBuildSimpleYearlyCalendarRule(t *testing.T) {
	handler := &SchedulesHandler{}
	request := formRequest(url.Values{
		"timezone": {"America/New_York"}, "calendar_type": {"YEARLY"}, "yearly_month": {"2"}, "yearly_day": {"29"},
		"adjust_for_leap_year": {"true"}, "execution_times": {"09:30,17:00"},
	})
	schedule, err := handler.buildCalendarSchedule(request.Context(), request)
	if err != nil {
		t.Fatalf("build yearly schedule: %v", err)
	}
	if schedule.GetType() != schedulepb.CalendarSchedule_YEARLY || len(schedule.GetRules()) != 1 {
		t.Fatalf("yearly schedule = %v", schedule)
	}
	yearly := schedule.GetRules()[0].GetYearly()
	if yearly.GetMonth() != 2 || yearly.GetDay() != 29 || !yearly.GetAdjustForLeapYear() || len(schedule.GetRules()[0].GetExecutionTimes()) != 2 {
		t.Fatalf("yearly rule = %v", schedule.GetRules()[0])
	}
}

func TestBuildSimpleYearlyCalendarRuleRejectsInvalidDates(t *testing.T) {
	handler := &SchedulesHandler{}
	for _, test := range []struct {
		name  string
		month string
		day   string
	}{
		{name: "missing month", day: "1"},
		{name: "month below range", month: "0", day: "1"},
		{name: "month above range", month: "13", day: "1"},
		{name: "missing day", month: "1"},
		{name: "day below range", month: "1", day: "0"},
		{name: "day above range", month: "1", day: "32"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := formRequest(url.Values{"calendar_type": {"YEARLY"}, "yearly_month": {test.month}, "yearly_day": {test.day}})
			if _, err := handler.buildCalendarSchedule(request.Context(), request); err == nil {
				t.Fatal("expected yearly date validation error")
			}
		})
	}
}

func formRequest(values url.Values) *http.Request {
	request := httptest.NewRequest("POST", "/", strings.NewReader(values.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err := request.ParseForm(); err != nil {
		panic(err)
	}
	return request
}
