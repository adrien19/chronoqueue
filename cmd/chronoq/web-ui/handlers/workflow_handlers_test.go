package handlers

import (
	"context"
	"html/template"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservicepb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/client"
	clusterstore "github.com/adrien19/chronoqueue/cmd/chronoq/web-ui/cluster"
	"github.com/adrien19/chronoqueue/pkg/log"
)

func TestQueueCreateForwardsAdvancedConfiguration(t *testing.T) {
	service := &workflowQueueService{}
	handler := &QueuesHandler{BaseHandler: workflowBaseHandler(t, service)}
	values := url.Values{
		"name": {"orders"}, "type": {"simple"}, "lease_duration": {"30s"}, "default_max_attempts": {"3"},
		"max_payload_size": {"4096"}, "allowed_content_types": {"application/json,application/xml"},
		"priority_policy": {"HYBRID"}, "priority_weights": {`{"4":70,"2":20,"0":10}`}, "age_boost_threshold": {"30m"}, "age_boost_multiplier": {"2"},
		"base_lease": {"30s"}, "max_extension": {"5m"}, "heartbeat_timeout": {"15s"}, "extend_step": {"10s"},
		"max_renewals":   {"5"},
		"retention_mode": {"RETAIN_DURATION"}, "retention_seconds": {"3600"},
	}
	recorder := httptest.NewRecorder()
	handler.Create(recorder, htmxFormRequest("/api/queues/create", values))
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %q", recorder.Code, recorder.Body.String())
	}
	metadata := service.createQueueRequest.GetMetadata()
	if metadata.GetMaxPayloadSize() != 4096 || metadata.GetPriorityConfig().GetPolicy() != queuepb.FairnessPolicy_HYBRID || metadata.GetLeasePolicy().GetMaxRenewals() != 5 || metadata.GetMessageRetentionPolicy().GetRetentionSeconds() != 3600 {
		t.Fatalf("forwarded metadata = %v", metadata)
	}
}

func TestQueueCreateValidationAndServerFailure(t *testing.T) {
	t.Run("invalid options do not call server", func(t *testing.T) {
		service := &workflowQueueService{}
		handler := &QueuesHandler{BaseHandler: workflowBaseHandler(t, service)}
		recorder := httptest.NewRecorder()
		handler.Create(recorder, htmxFormRequest("/api/queues/create", url.Values{"name": {"orders"}, "type": {"simple"}, "lease_duration": {"30s"}, "priority_policy": {"WEIGHTED"}}))
		if recorder.Code != http.StatusOK || service.createQueueRequest != nil || !strings.Contains(recorder.Body.String(), "require priority weights") {
			t.Fatalf("response = (%d, %q), request = %v", recorder.Code, recorder.Body.String(), service.createQueueRequest)
		}
	})

	t.Run("server rejection", func(t *testing.T) {
		service := &workflowQueueService{createQueueErr: status.Error(codes.InvalidArgument, "private")}
		handler := &QueuesHandler{BaseHandler: workflowBaseHandler(t, service)}
		recorder := httptest.NewRecorder()
		handler.Create(recorder, htmxFormRequest("/api/queues/create", url.Values{"name": {"orders"}, "type": {"simple"}, "lease_duration": {"30s"}}))
		if recorder.Code != http.StatusOK || strings.Contains(recorder.Body.String(), "private") || !strings.Contains(recorder.Body.String(), "rejected") {
			t.Fatalf("response = (%d, %q)", recorder.Code, recorder.Body.String())
		}
	})
}

func TestQueueDeleteAndCancel(t *testing.T) {
	for _, test := range []struct {
		name      string
		serverErr error
		invoke    func(*QueuesHandler, http.ResponseWriter, *http.Request)
		path      string
		wantCall  string
		redirect  string
	}{
		{name: "delete success", invoke: (*QueuesHandler).Delete, path: "/queues/orders/delete", wantCall: "delete", redirect: "/queues"},
		{name: "delete failure", serverErr: status.Error(codes.FailedPrecondition, "not empty"), invoke: (*QueuesHandler).Delete, path: "/queues/orders/delete", wantCall: "delete"},
		{name: "cancel success", invoke: (*QueuesHandler).CancelMessage, path: "/api/queues/orders/messages/msg-1/cancel", wantCall: "cancel", redirect: "/queues/orders"},
		{name: "cancel concurrent state change", serverErr: status.Error(codes.FailedPrecondition, "now running"), invoke: (*QueuesHandler).CancelMessage, path: "/api/queues/orders/messages/msg-1/cancel", wantCall: "cancel"},
		{name: "cancel missing message", serverErr: status.Error(codes.NotFound, "missing"), invoke: (*QueuesHandler).CancelMessage, path: "/api/queues/orders/messages/msg-1/cancel", wantCall: "cancel"},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &workflowQueueService{}
			if test.wantCall == "delete" {
				service.deleteQueueErr = test.serverErr
			} else {
				service.cancelMessageErr = test.serverErr
			}
			handler := &QueuesHandler{BaseHandler: workflowBaseHandler(t, service)}
			recorder := httptest.NewRecorder()
			request := htmxFormRequest(test.path, url.Values{"reason": {"operator request"}})
			request.SetPathValue("name", "orders")
			request.SetPathValue("messageId", "msg-1")
			test.invoke(handler, recorder, request)
			if test.serverErr == nil && (recorder.Code != http.StatusOK || recorder.Header().Get("HX-Redirect") != test.redirect) {
				t.Fatalf("response = (%d, %q), headers = %v", recorder.Code, recorder.Body.String(), recorder.Header())
			}
			if test.serverErr != nil && recorder.Code != http.StatusOK {
				t.Fatalf("failure response = (%d, %q)", recorder.Code, recorder.Body.String())
			}
			if test.serverErr == nil && test.wantCall == "delete" && service.deleteQueueRequest.GetName() != "orders" {
				t.Fatalf("delete request = %v", service.deleteQueueRequest)
			}
			if test.serverErr == nil && test.wantCall == "cancel" && (service.cancelMessageRequest.GetQueueName() != "orders" || service.cancelMessageRequest.GetMessageId() != "msg-1" || service.cancelMessageRequest.GetReason() != "operator request") {
				t.Fatalf("cancel request = %v", service.cancelMessageRequest)
			}
		})
	}
}

func TestQueueDeleteAndCancelKeepHTTPRedirects(t *testing.T) {
	service := &workflowQueueService{}
	handler := &QueuesHandler{BaseHandler: workflowBaseHandler(t, service)}
	for _, test := range []struct {
		name     string
		path     string
		location string
		invoke   func(*QueuesHandler, http.ResponseWriter, *http.Request)
	}{
		{name: "delete", path: "/queues/orders/delete", location: "/queues", invoke: (*QueuesHandler).Delete},
		{name: "cancel", path: "/api/queues/orders/messages/msg-1/cancel", location: "/queues/orders", invoke: (*QueuesHandler).CancelMessage},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(url.Values{"reason": {"operator request"}}.Encode()))
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			request.SetPathValue("name", "orders")
			request.SetPathValue("messageId", "msg-1")
			test.invoke(handler, recorder, request)
			if recorder.Code != http.StatusSeeOther || recorder.Header().Get("Location") != test.location {
				t.Fatalf("response = (%d, %q), headers = %v", recorder.Code, recorder.Body.String(), recorder.Header())
			}
		})
	}
}

func TestQueueListAcceptsNormalizedHTMXHeader(t *testing.T) {
	service := &workflowQueueService{
		listQueuesResponse: &queueservicepb.ListQueuesResponse{Queues: []*queuepb.Queue{{Name: "orders"}}},
		getQueueStateErr:   status.Error(codes.Unavailable, "private"),
	}
	handler := &QueuesHandler{BaseHandler: workflowBaseHandler(t, service)}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/queues", nil)
	request.Header.Set("HX-Request", " TRUE ")
	handler.List(recorder, request)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "fragment warning") || !strings.Contains(recorder.Body.String(), "—") || strings.Contains(recorder.Body.String(), "full warning") {
		t.Fatalf("response = (%d, %q)", recorder.Code, recorder.Body.String())
	}
}

func TestQueueDetailKeepsPageOnDLQStatsFailure(t *testing.T) {
	service := &workflowQueueService{
		listQueuesResponse:    &queueservicepb.ListQueuesResponse{Queues: []*queuepb.Queue{{Name: "orders", Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "orders-dlq"}}}},
		getQueueStateResponse: &queueservicepb.GetQueueStateResponse{StateCounts: map[string]int32{}},
		peekQueueResponse:     &queueservicepb.PeekQueueMessagesResponse{},
		getDLQStatsErr:        status.Error(codes.Unavailable, "private"),
	}
	handler := &QueuesHandler{BaseHandler: workflowBaseHandler(t, service)}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/queues/orders", nil)
	request.SetPathValue("name", "orders")
	handler.Detail(recorder, request)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "partial warning") || !strings.Contains(recorder.Body.String(), "—") {
		t.Fatalf("response = (%d, %q)", recorder.Code, recorder.Body.String())
	}
}

func TestDashboardStatsUseTruthfulAggregateStates(t *testing.T) {
	tests := []struct {
		name          string
		stateResponse *queueservicepb.GetQueueStateResponse
		stateErr      error
		want          string
	}{
		{name: "complete", stateResponse: &queueservicepb.GetQueueStateResponse{StateCounts: map[string]int32{"PENDING": 3, "RUNNING": 2, "COMPLETED": 5}}, want: "Reachable|3|2|5|0|false"},
		{name: "partial", stateErr: status.Error(codes.Unavailable, "private"), want: "Partial data|—|—|—|—|true"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &workflowQueueService{
				listQueuesResponse:    &queueservicepb.ListQueuesResponse{Queues: []*queuepb.Queue{{Name: "orders"}}},
				getQueueStateResponse: test.stateResponse,
				getQueueStateErr:      test.stateErr,
			}
			handler := &DashboardHandler{BaseHandler: workflowBaseHandler(t, service)}
			recorder := httptest.NewRecorder()
			handler.DashboardStats(recorder, httptest.NewRequest(http.MethodGet, "/fragments/dashboard-stats", nil))
			if recorder.Code != http.StatusOK || strings.TrimSpace(recorder.Body.String()) != test.want {
				t.Fatalf("response = (%d, %q), want %q", recorder.Code, recorder.Body.String(), test.want)
			}
		})
	}

	t.Run("DLQ failure", func(t *testing.T) {
		service := &workflowQueueService{
			listQueuesResponse:    &queueservicepb.ListQueuesResponse{Queues: []*queuepb.Queue{{Name: "orders", Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "orders-dlq"}}}},
			getQueueStateResponse: &queueservicepb.GetQueueStateResponse{StateCounts: map[string]int32{"PENDING": 3}},
			getDLQStatsErr:        status.Error(codes.Unavailable, "private"),
		}
		handler := &DashboardHandler{BaseHandler: workflowBaseHandler(t, service)}
		recorder := httptest.NewRecorder()
		handler.DashboardStats(recorder, httptest.NewRequest(http.MethodGet, "/fragments/dashboard-stats", nil))
		if recorder.Code != http.StatusOK || strings.TrimSpace(recorder.Body.String()) != "Partial data|—|—|—|—|true" {
			t.Fatalf("response = (%d, %q)", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("queue list failure", func(t *testing.T) {
		service := &workflowQueueService{listQueuesErr: status.Error(codes.Unavailable, "private")}
		handler := &DashboardHandler{BaseHandler: workflowBaseHandler(t, service)}
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/fragments/dashboard-stats", nil)
		request.Header.Set("HX-Request", "true")
		handler.DashboardStats(recorder, request)
		if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "ChronoQueue is unavailable") || strings.Contains(recorder.Body.String(), "private") {
			t.Fatalf("response = (%d, %q)", recorder.Code, recorder.Body.String())
		}
	})
}

func TestSettingsNavigationOnlyExposesSupportedServerSettings(t *testing.T) {
	items := settingsNav()
	if len(items) != 1 || items[0].Key != "clusters" || items[0].Href != "/settings/clusters" {
		t.Fatalf("settings navigation = %+v", items)
	}
}

func TestLeaseMonitorRuntimeMetadataAndFailures(t *testing.T) {
	now := time.Date(2030, time.January, 1, 12, 0, 0, 0, time.UTC)
	message := &messagepb.Message{MessageId: "message-123456789", Metadata: &messagepb.Message_Metadata{
		State: messagepb.Message_Metadata_RUNNING,
		CurrentAttempt: &messagepb.Message_Metadata_AttemptRuntime{
			WorkerId:          "worker-1",
			LeaseStartedAt:    timestamppb.New(now.Add(-2 * time.Minute)),
			LeaseExpiry:       now.Add(30 * time.Second).UnixMilli(),
			LeaseRenewalCount: 3,
			LastHeartbeatAt:   timestamppb.New(now.Add(-10 * time.Second)),
		},
	}}
	row := buildLeaseRow(message, "orders", now)
	if row.Worker != "worker-1" || row.Renewals != "3" || row.Duration != "2m" || row.ExpiresIn != "30s" || row.LastHeartbeat != "10s ago" {
		t.Fatalf("lease row = %+v", row)
	}

	unknown := buildLeaseRow(&messagepb.Message{MessageId: "message", Metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_RUNNING}}, "orders", now)
	if unknown.Worker != "—" || unknown.Renewals != "—" || unknown.Duration != "—" || unknown.ExpiresIn != "—" || unknown.LastHeartbeat != "—" {
		t.Fatalf("unknown lease row = %+v", unknown)
	}

	t.Run("peek success", func(t *testing.T) {
		service := &workflowQueueService{
			listQueuesResponse:    &queueservicepb.ListQueuesResponse{Queues: []*queuepb.Queue{{Name: "orders"}}},
			getQueueStateResponse: &queueservicepb.GetQueueStateResponse{StateCounts: map[string]int32{"RUNNING": 1}},
			peekQueueResponse:     &queueservicepb.PeekQueueMessagesResponse{Messages: []*messagepb.Message{message}},
		}
		base := workflowBaseHandler(t, service)
		activeClient, err := base.clientProvider()
		if err != nil {
			t.Fatalf("active client: %v", err)
		}
		inflight, total, partial, err := (&LeaseMonitorHandler{BaseHandler: base}).collectInflight(context.Background(), activeClient)
		if err != nil || partial || total != 1 || len(inflight) != 1 || len(inflight[0].Rows) != 1 || inflight[0].Rows[0].Worker != "worker-1" {
			t.Fatalf("inflight = (%+v, %d, %t, %v)", inflight, total, partial, err)
		}
	})

	t.Run("list failure", func(t *testing.T) {
		service := &workflowQueueService{listQueuesErr: status.Error(codes.Unavailable, "private")}
		base := workflowBaseHandler(t, service)
		activeClient, err := base.clientProvider()
		if err != nil {
			t.Fatalf("active client: %v", err)
		}
		_, _, _, err = (&LeaseMonitorHandler{BaseHandler: base}).collectInflight(context.Background(), activeClient)
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("collect error = %v", err)
		}
	})

	t.Run("peek failure is partial", func(t *testing.T) {
		service := &workflowQueueService{
			listQueuesResponse:    &queueservicepb.ListQueuesResponse{Queues: []*queuepb.Queue{{Name: "orders"}}},
			getQueueStateResponse: &queueservicepb.GetQueueStateResponse{StateCounts: map[string]int32{"RUNNING": 1}},
			peekQueueErr:          status.Error(codes.Unavailable, "private"),
		}
		base := workflowBaseHandler(t, service)
		activeClient, err := base.clientProvider()
		if err != nil {
			t.Fatalf("active client: %v", err)
		}
		inflight, total, partial, err := (&LeaseMonitorHandler{BaseHandler: base}).collectInflight(context.Background(), activeClient)
		if err != nil || !partial || total != 1 || len(inflight) != 1 || len(inflight[0].Rows) != 0 {
			t.Fatalf("inflight = (%+v, %d, %t, %v)", inflight, total, partial, err)
		}
	})

	t.Run("state failure is partial", func(t *testing.T) {
		service := &workflowQueueService{
			listQueuesResponse: &queueservicepb.ListQueuesResponse{Queues: []*queuepb.Queue{{Name: "orders"}}},
			getQueueStateErr:   status.Error(codes.Unavailable, "private"),
		}
		base := workflowBaseHandler(t, service)
		activeClient, err := base.clientProvider()
		if err != nil {
			t.Fatalf("active client: %v", err)
		}
		inflight, total, partial, err := (&LeaseMonitorHandler{BaseHandler: base}).collectInflight(context.Background(), activeClient)
		if err != nil || !partial || total != 0 || len(inflight) != 0 {
			t.Fatalf("inflight = (%+v, %d, %t, %v)", inflight, total, partial, err)
		}
	})
}

func TestBulkPostModesPartialAndTransportFailure(t *testing.T) {
	for _, mode := range []string{"ALL_OR_NOTHING", "BEST_EFFORT"} {
		t.Run(mode, func(t *testing.T) {
			service := &workflowQueueService{bulkResponse: &queueservicepb.PostMessagesBulkResponse{Success: true, SuccessfulCount: 1, FailedCount: 1, Results: []*queueservicepb.PostMessagesBulkResponse_MessagePostResult{{MessageId: "one", Success: true}, {MessageId: "two", Error: "duplicate"}}}}
			handler := &QueuesHandler{BaseHandler: workflowBaseHandler(t, service)}
			recorder := httptest.NewRecorder()
			request := htmxFormRequest("/api/queues/orders/messages/bulk", url.Values{"transaction_mode": {mode}, "messages": {`[{"message_id":"one","payload":{}},{"message_id":"two","payload":{}}]`}})
			request.SetPathValue("name", "orders")
			handler.PostBulkMessages(recorder, request)
			if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "1 succeeded; 1 failed") {
				t.Fatalf("response = (%d, %q)", recorder.Code, recorder.Body.String())
			}
			if service.bulkRequest.GetTransactionMode().String() != mode {
				t.Fatalf("mode = %s", service.bulkRequest.GetTransactionMode())
			}
		})
	}

	service := &workflowQueueService{bulkErr: status.Error(codes.Unavailable, "private")}
	handler := &QueuesHandler{BaseHandler: workflowBaseHandler(t, service)}
	recorder := httptest.NewRecorder()
	request := htmxFormRequest("/api/queues/orders/messages/bulk", url.Values{"transaction_mode": {"BEST_EFFORT"}, "messages": {`[{"message_id":"one","payload":{}}]`}})
	request.SetPathValue("name", "orders")
	handler.PostBulkMessages(recorder, request)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "unavailable") || strings.Contains(recorder.Body.String(), "private") {
		t.Fatalf("transport failure response = (%d, %q)", recorder.Code, recorder.Body.String())
	}
}

func TestSchemaDeactivateAllUsesVersionZero(t *testing.T) {
	service := &workflowQueueService{}
	handler := &SchemasHandler{BaseHandler: workflowBaseHandler(t, service)}
	recorder := httptest.NewRecorder()
	request := htmxFormRequest("/api/schemas/orders/deactivate", nil)
	request.SetPathValue("schemaId", "orders")
	handler.DeactivateAll(recorder, request)
	if recorder.Code != http.StatusSeeOther || service.deleteSchemaRequest.GetVersion() != 0 {
		t.Fatalf("response = %d, request = %v", recorder.Code, service.deleteSchemaRequest)
	}

	service.deleteSchemaErr = status.Error(codes.NotFound, "private")
	recorder = httptest.NewRecorder()
	handler.DeactivateAll(recorder, request)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "not found") || strings.Contains(recorder.Body.String(), "private") {
		t.Fatalf("failure response = (%d, %q)", recorder.Code, recorder.Body.String())
	}
}

func TestSchemaDeleteKeepsPositiveVersionScope(t *testing.T) {
	service := &workflowQueueService{}
	handler := &SchemasHandler{BaseHandler: workflowBaseHandler(t, service)}
	recorder := httptest.NewRecorder()
	request := htmxFormRequest("/api/schemas/orders/versions/2/delete", nil)
	request.SetPathValue("schemaId", "orders")
	request.SetPathValue("version", "2")
	handler.Delete(recorder, request)
	if recorder.Code != http.StatusSeeOther || service.deleteSchemaRequest.GetVersion() != 2 {
		t.Fatalf("response = %d, request = %v", recorder.Code, service.deleteSchemaRequest)
	}

	service.deleteSchemaRequest = nil
	recorder = httptest.NewRecorder()
	request.SetPathValue("version", "0")
	handler.Delete(recorder, request)
	if recorder.Code != http.StatusBadRequest || service.deleteSchemaRequest != nil {
		t.Fatalf("version-zero delete response = %d, request = %v", recorder.Code, service.deleteSchemaRequest)
	}
}

func TestCalendarJSONSupportsCompleteServerContract(t *testing.T) {
	raw := `{"type":"YEARLY","timezone":"America/New_York","rules":[{"yearly":{"month":2,"day":29,"adjustForLeapYear":true},"executionTimes":[{"hour":9}],"validFrom":"2028-01-01T00:00:00Z","validUntil":"2032-12-31T23:59:59Z"},{"yearly":{"month":12,"day":31},"executionTimes":[{"hour":17}]}],"exceptions":[{"date":"2028-02-29T00:00:00Z","type":"SKIP","reason":"maintenance"}]}`
	request := formRequest(url.Values{"calendar_json": {raw}})
	handler := &SchedulesHandler{}
	schedule, err := handler.buildCalendarSchedule(context.Background(), request)
	if err != nil || schedule.GetType() != schedulepb.CalendarSchedule_YEARLY || len(schedule.GetRules()) != 2 || len(schedule.GetExceptions()) != 1 {
		t.Fatalf("calendar schedule = (%v, %v)", schedule, err)
	}
	if _, err := handler.buildCalendarSchedule(context.Background(), formRequest(url.Values{"calendar_json": {`{"type":"CUSTOM","timezone":"UTC"}`}})); err == nil {
		t.Fatal("expected CUSTOM schedule rejection")
	}
	businessRaw := `{"type":"BUSINESS_DAYS","timezone":"Europe/London","rules":[{"businessDays":{"businessCalendarId":"uk","dayOffset":0},"executionTimes":[{"hour":8},{"hour":17}]}],"businessCalendar":{"calendarId":"uk","weekendDays":[6,7],"timezone":"Europe/London"}}`
	business, err := handler.buildCalendarSchedule(context.Background(), formRequest(url.Values{"calendar_json": {businessRaw}}))
	if err != nil || business.GetBusinessCalendar().GetCalendarId() != "uk" || len(business.GetRules()[0].GetExecutionTimes()) != 2 {
		t.Fatalf("business calendar = (%v, %v)", business, err)
	}
}

func TestScheduleCreateValidatesOnServerAndForwardsOptions(t *testing.T) {
	service := &workflowQueueService{
		listQueuesResponse:       &queueservicepb.ListQueuesResponse{Queues: []*queuepb.Queue{{Name: "orders"}}},
		validateCalendarResponse: &queueservicepb.ValidateCalendarScheduleResponse{Valid: true},
	}
	handler := &SchedulesHandler{BaseHandler: workflowBaseHandler(t, service)}
	calendarJSON := `{"type":"DAILY","timezone":"UTC","rules":[{"daily":{"dayInterval":1},"executionTimes":[{"hour":9}]}]}`
	values := url.Values{
		"schedule_id": {"daily-orders"}, "schedule_type": {"calendar"}, "queue_name": {"orders"}, "payload_data": {`{"kind":"report"}`}, "calendar_json": {calendarJSON},
		"payload_metadata": {`{"tenant":"acme"}`}, "headers": {`[{"key":"trace-id","value_base64":"AP8="}]`}, "content_type": {"application/json"}, "schema_id": {"order-schema"}, "schema_version": {"2"}, "priority": {"4"}, "max_messages": {"25"}, "lease_duration": {"30s"},
	}
	recorder := httptest.NewRecorder()
	handler.Create(recorder, htmxFormRequest("/api/schedules/create", values))
	if recorder.Code != http.StatusCreated || service.validateCalendarRequest == nil || service.createScheduleRequest == nil {
		t.Fatalf("response = (%d, %q), validate = %v, create = %v", recorder.Code, recorder.Body.String(), service.validateCalendarRequest, service.createScheduleRequest)
	}
	metadata := service.createScheduleRequest.GetSchedule().GetMetadata()
	if metadata.GetPriority() != 4 || metadata.GetMaxMessages() != 25 || metadata.GetPayload().GetSchemaId() != "order-schema" || len(metadata.GetHeaders()) != 1 {
		t.Fatalf("schedule metadata = %v", metadata)
	}

	service.createScheduleErr = status.Error(codes.InvalidArgument, "private")
	recorder = httptest.NewRecorder()
	handler.Create(recorder, htmxFormRequest("/api/schedules/create", values))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "rejected") || strings.Contains(recorder.Body.String(), "private") {
		t.Fatalf("server rejection response = (%d, %q)", recorder.Code, recorder.Body.String())
	}
}

func TestScheduleCreateTrimsQueueNameBeforeExistenceCheck(t *testing.T) {
	service := &workflowQueueService{
		listQueuesResponse: &queueservicepb.ListQueuesResponse{Queues: []*queuepb.Queue{{Name: "financial-report"}}},
	}
	handler := &SchedulesHandler{BaseHandler: workflowBaseHandler(t, service)}
	recorder := httptest.NewRecorder()
	handler.Create(recorder, htmxFormRequest("/api/schedules/create", url.Values{
		"schedule_id":     {"daily-financial-report"},
		"schedule_type":   {"cron"},
		"cron_expression": {"0 9 * * *"},
		"queue_name":      {" financial-report "},
	}))

	if recorder.Code != http.StatusCreated || service.createScheduleRequest.GetSchedule().GetMetadata().GetQueueName() != "financial-report" {
		t.Fatalf("response = (%d, %q), request = %v", recorder.Code, recorder.Body.String(), service.createScheduleRequest)
	}
}

func TestScheduleCreateMissingQueueAction(t *testing.T) {
	values := url.Values{
		"schedule_id":     {"daily-financial-report"},
		"schedule_type":   {"cron"},
		"cron_expression": {"0 9 * * *"},
		"queue_name":      {"financial-report"},
	}
	service := &workflowQueueService{listQueuesResponse: &queueservicepb.ListQueuesResponse{}}
	handler := &SchedulesHandler{BaseHandler: workflowBaseHandler(t, service)}

	recorder := httptest.NewRecorder()
	handler.Create(recorder, htmxFormRequest("/api/schedules/create", values))
	body := recorder.Body.String()
	if recorder.Code != http.StatusOK || !strings.Contains(body, `hx-post="/api/schedules/create"`) || !strings.Contains(body, `hx-include="#schedule-form"`) || !strings.Contains(body, `hx-vals='{"auto_create_queue":"true"}'`) {
		t.Fatalf("missing queue response = (%d, %q)", recorder.Code, body)
	}

	values.Set("auto_create_queue", "true")
	recorder = httptest.NewRecorder()
	handler.Create(recorder, htmxFormRequest("/api/schedules/create", values))
	if recorder.Code != http.StatusCreated || service.createQueueRequest.GetName() != "financial-report" || service.createScheduleRequest == nil {
		t.Fatalf("auto-create response = (%d, %q), queue = %v, schedule = %v", recorder.Code, recorder.Body.String(), service.createQueueRequest, service.createScheduleRequest)
	}
}

func TestCalendarValidationPreviewSuccessAndFailures(t *testing.T) {
	calendarJSON := `{"type":"DAILY","timezone":"UTC","rules":[{"daily":{"dayInterval":1}}]}`
	t.Run("validation issues", func(t *testing.T) {
		service := &workflowQueueService{validateCalendarResponse: &queueservicepb.ValidateCalendarScheduleResponse{Valid: false, ErrorMessage: "invalid timezone"}}
		handler := &SchedulesHandler{BaseHandler: workflowBaseHandler(t, service)}
		recorder := httptest.NewRecorder()
		handler.ValidateCalendar(recorder, htmxFormRequest("/api/schedules/calendar/validate", url.Values{"calendar_json": {calendarJSON}}))
		if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "invalid timezone") {
			t.Fatalf("validation response = (%d, %q)", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("preview and limit", func(t *testing.T) {
		service := &workflowQueueService{previewCalendarResponse: &queueservicepb.PreviewCalendarScheduleResponse{Timezone: "UTC", TotalCount: 2}}
		handler := &SchedulesHandler{BaseHandler: workflowBaseHandler(t, service)}
		recorder := httptest.NewRecorder()
		handler.PreviewCalendar(recorder, htmxFormRequest("/api/schedules/calendar/preview", url.Values{"calendar_json": {calendarJSON}, "preview_count": {"2"}}))
		if recorder.Code != http.StatusOK || service.previewCalendarRequest.GetCount() != 2 {
			t.Fatalf("preview response = (%d, %q), request = %v", recorder.Code, recorder.Body.String(), service.previewCalendarRequest)
		}
		recorder = httptest.NewRecorder()
		handler.PreviewCalendar(recorder, htmxFormRequest("/api/schedules/calendar/preview", url.Values{"calendar_json": {calendarJSON}}))
		if recorder.Code != http.StatusOK || service.previewCalendarRequest.GetCount() != 10 {
			t.Fatalf("default preview response = (%d, %q), request = %v", recorder.Code, recorder.Body.String(), service.previewCalendarRequest)
		}
		recorder = httptest.NewRecorder()
		handler.PreviewCalendar(recorder, htmxFormRequest("/api/schedules/calendar/preview", url.Values{"calendar_json": {calendarJSON}, "preview_count": {"101"}}))
		if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "between 1 and 100") {
			t.Fatalf("invalid limit response = (%d, %q)", recorder.Code, recorder.Body.String())
		}
		recorder = httptest.NewRecorder()
		handler.PreviewCalendar(recorder, htmxFormRequest("/api/schedules/calendar/preview", url.Values{"calendar_json": {calendarJSON}, "preview_count": {"many"}}))
		if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "between 1 and 100") {
			t.Fatalf("malformed limit response = (%d, %q)", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("server failure", func(t *testing.T) {
		service := &workflowQueueService{validateCalendarErr: status.Error(codes.Unavailable, "private")}
		handler := &SchedulesHandler{BaseHandler: workflowBaseHandler(t, service)}
		recorder := httptest.NewRecorder()
		handler.ValidateCalendar(recorder, htmxFormRequest("/api/schedules/calendar/validate", url.Values{"calendar_json": {calendarJSON}}))
		if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "unavailable") || strings.Contains(recorder.Body.String(), "private") {
			t.Fatalf("failure response = (%d, %q)", recorder.Code, recorder.Body.String())
		}
	})
}

func TestCalendarValidationAndPreviewSupportSimpleFields(t *testing.T) {
	tests := []struct {
		name         string
		values       url.Values
		calendarType schedulepb.CalendarSchedule_ScheduleType
	}{
		{name: "daily", values: url.Values{"calendar_type": {"DAILY"}, "timezone": {"UTC"}, "execution_times": {"09:30"}}, calendarType: schedulepb.CalendarSchedule_DAILY},
		{name: "weekly", values: url.Values{"calendar_type": {"WEEKLY"}, "timezone": {"UTC"}, "execution_times": {"09:30"}, "days_of_week": {"1", "5"}}, calendarType: schedulepb.CalendarSchedule_WEEKLY},
		{name: "monthly", values: url.Values{"calendar_type": {"MONTHLY"}, "timezone": {"UTC"}, "execution_times": {"09:30"}, "day_of_month": {"15"}}, calendarType: schedulepb.CalendarSchedule_MONTHLY},
		{name: "yearly", values: url.Values{"calendar_type": {"YEARLY"}, "timezone": {"UTC"}, "execution_times": {"09:30"}, "yearly_month": {"6"}, "yearly_day": {"15"}}, calendarType: schedulepb.CalendarSchedule_YEARLY},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &workflowQueueService{
				validateCalendarResponse: &queueservicepb.ValidateCalendarScheduleResponse{Valid: true},
				previewCalendarResponse:  &queueservicepb.PreviewCalendarScheduleResponse{Timezone: "UTC", TotalCount: 2},
			}
			handler := &SchedulesHandler{BaseHandler: workflowBaseHandler(t, service)}

			recorder := httptest.NewRecorder()
			handler.ValidateCalendar(recorder, htmxFormRequest("/api/schedules/calendar/validate", test.values))
			validated := service.validateCalendarRequest.GetCalendarSchedule()
			if recorder.Code != http.StatusOK || validated.GetType() != test.calendarType || len(validated.GetRules()) != 1 {
				t.Fatalf("validation response = (%d, %q), request = %v", recorder.Code, recorder.Body.String(), service.validateCalendarRequest)
			}

			previewValues := make(url.Values, len(test.values)+1)
			for key, values := range test.values {
				previewValues[key] = append([]string(nil), values...)
			}
			previewValues.Set("preview_count", "2")
			recorder = httptest.NewRecorder()
			handler.PreviewCalendar(recorder, htmxFormRequest("/api/schedules/calendar/preview", previewValues))
			previewed := service.previewCalendarRequest.GetCalendarSchedule()
			if recorder.Code != http.StatusOK || previewed.GetType() != test.calendarType || len(previewed.GetRules()) != 1 || service.previewCalendarRequest.GetCount() != 2 {
				t.Fatalf("preview response = (%d, %q), request = %v", recorder.Code, recorder.Body.String(), service.previewCalendarRequest)
			}
		})
	}
}

func TestScheduleDetailLifecycleHistoryAndFailures(t *testing.T) {
	states := []schedulepb.Schedule_Metadata_State{schedulepb.Schedule_Metadata_SCHEDULED, schedulepb.Schedule_Metadata_PAUSED, schedulepb.Schedule_Metadata_CANCELED, schedulepb.Schedule_Metadata_ERRORED}
	for _, state := range states {
		t.Run(state.String(), func(t *testing.T) {
			service := &workflowQueueService{getScheduleResponse: &queueservicepb.GetScheduleResponse{Schedule: &schedulepb.Schedule{ScheduleId: "daily", Metadata: &schedulepb.Schedule_Metadata{State: state}}}, getHistoryResponse: &queueservicepb.GetScheduleHistoryResponse{ScheduleHistory: &schedulepb.ScheduleHistory{ScheduleId: "daily"}}}
			handler := &SchedulesHandler{BaseHandler: workflowBaseHandler(t, service)}
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/schedules/daily?history_limit=25", nil)
			request.SetPathValue("id", "daily")
			handler.Detail(recorder, request)
			if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), state.String()) || service.getHistoryRequest.GetLimit() != 25 {
				t.Fatalf("detail response = (%d, %q), history request = %v", recorder.Code, recorder.Body.String(), service.getHistoryRequest)
			}
		})
	}

	for _, failure := range []struct {
		name       string
		getErr     error
		historyErr error
	}{
		{name: "get failure", getErr: status.Error(codes.NotFound, "private")},
		{name: "history failure", historyErr: status.Error(codes.Unavailable, "private")},
	} {
		t.Run(failure.name, func(t *testing.T) {
			service := &workflowQueueService{getScheduleResponse: &queueservicepb.GetScheduleResponse{Schedule: &schedulepb.Schedule{ScheduleId: "daily", Metadata: &schedulepb.Schedule_Metadata{}}}, getScheduleErr: failure.getErr, getHistoryErr: failure.historyErr}
			handler := &SchedulesHandler{BaseHandler: workflowBaseHandler(t, service)}
			recorder := httptest.NewRecorder()
			request := htmxFormRequest("/schedules/daily", nil)
			request.SetPathValue("id", "daily")
			handler.Detail(recorder, request)
			if recorder.Code != http.StatusOK || strings.Contains(recorder.Body.String(), "private") {
				t.Fatalf("failure response = (%d, %q)", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func workflowBaseHandler(t *testing.T, service *workflowQueueService) BaseHandler {
	t.Helper()
	chronoClient, err := client.NewChronoQueueClient("unused", client.ClientOptions{MaxHeartBeatWorkers: 1, Connector: func(string, client.ClientOptions) (queueservicepb.QueueServiceClient, *grpc.ClientConn, error) {
		return service, nil, nil
	}})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	store := clusterstore.NewStore("")
	store.Seed("Local", "localhost:9000", true)
	templates := template.Must(template.New("test").Parse(`{{ define "base" }}{{ .ErrorMessage }}{{ with .Schedule }}{{ .Metadata.State }}{{ end }}{{ if eq .ContentTemplate "queue_detail_content" }}{{ template "queue_detail_content" . }}{{ end }}{{ end }}{{ define "bulk_message_results" }}{{ .SuccessfulCount }} succeeded; {{ .FailedCount }} failed{{ end }}{{ define "calendar_validation_result" }}{{ .ErrorMessage }}{{ end }}{{ define "calendar_preview_result" }}{{ .TotalCount }} runs{{ end }}{{ define "queue_table" }}{{ if .PartialDataFragmentWarning }}fragment warning{{ end }}{{ if .PartialDataWarning }}full warning{{ end }}{{ range .Rows }}{{ .Name }}{{ .Ready }}{{ end }}{{ end }}{{ define "queue_detail_content" }}{{ if .PartialDataWarning }}partial warning{{ end }} {{ .Queue.DLQ }}{{ end }}{{ define "dashboard_stats" }}{{ .BrokerStatus }}|{{ .TotalReady }}|{{ .TotalRunning }}|{{ .TotalCompleted }}|{{ .TotalDLQ }}|{{ .PartialData }}{{ end }}`))
	return BaseHandler{templates: templates, store: store, logger: log.NewLogger(), clientProvider: func() (*client.ChronoQueueClient, error) { return chronoClient, nil }}
}

func htmxFormRequest(path string, values url.Values) *http.Request {
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(values.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("HX-Request", "true")
	return request
}

type workflowQueueService struct {
	queueservicepb.QueueServiceClient
	createQueueRequest       *queueservicepb.CreateQueueRequest
	createQueueErr           error
	deleteQueueRequest       *queueservicepb.DeleteQueueRequest
	deleteQueueErr           error
	cancelMessageRequest     *queueservicepb.CancelMessageRequest
	cancelMessageErr         error
	bulkRequest              *queueservicepb.PostMessagesBulkRequest
	bulkResponse             *queueservicepb.PostMessagesBulkResponse
	bulkErr                  error
	deleteSchemaRequest      *queueservicepb.DeleteSchemaRequest
	deleteSchemaErr          error
	listQueuesResponse       *queueservicepb.ListQueuesResponse
	listQueuesErr            error
	getQueueStateResponse    *queueservicepb.GetQueueStateResponse
	getQueueStateErr         error
	peekQueueResponse        *queueservicepb.PeekQueueMessagesResponse
	peekQueueErr             error
	getDLQStatsResponse      *queueservicepb.GetDLQStatsResponse
	getDLQStatsErr           error
	validateCalendarRequest  *queueservicepb.ValidateCalendarScheduleRequest
	validateCalendarResponse *queueservicepb.ValidateCalendarScheduleResponse
	validateCalendarErr      error
	previewCalendarRequest   *queueservicepb.PreviewCalendarScheduleRequest
	previewCalendarResponse  *queueservicepb.PreviewCalendarScheduleResponse
	previewCalendarErr       error
	createScheduleRequest    *queueservicepb.CreateScheduleRequest
	createScheduleErr        error
	getScheduleResponse      *queueservicepb.GetScheduleResponse
	getScheduleErr           error
	getHistoryRequest        *queueservicepb.GetScheduleHistoryRequest
	getHistoryResponse       *queueservicepb.GetScheduleHistoryResponse
	getHistoryErr            error
}

func (s *workflowQueueService) CreateQueue(_ context.Context, request *queueservicepb.CreateQueueRequest, _ ...grpc.CallOption) (*queueservicepb.CreateQueueResponse, error) {
	s.createQueueRequest = request
	return &queueservicepb.CreateQueueResponse{Success: s.createQueueErr == nil}, s.createQueueErr
}

func (s *workflowQueueService) DeleteQueue(_ context.Context, request *queueservicepb.DeleteQueueRequest, _ ...grpc.CallOption) (*queueservicepb.DeleteQueueResponse, error) {
	s.deleteQueueRequest = request
	return &queueservicepb.DeleteQueueResponse{Success: s.deleteQueueErr == nil}, s.deleteQueueErr
}

func (s *workflowQueueService) CancelMessage(_ context.Context, request *queueservicepb.CancelMessageRequest, _ ...grpc.CallOption) (*queueservicepb.CancelMessageResponse, error) {
	s.cancelMessageRequest = request
	return &queueservicepb.CancelMessageResponse{Success: s.cancelMessageErr == nil}, s.cancelMessageErr
}

func (s *workflowQueueService) PostMessagesBulk(_ context.Context, request *queueservicepb.PostMessagesBulkRequest, _ ...grpc.CallOption) (*queueservicepb.PostMessagesBulkResponse, error) {
	s.bulkRequest = request
	return s.bulkResponse, s.bulkErr
}

func (s *workflowQueueService) DeleteSchema(_ context.Context, request *queueservicepb.DeleteSchemaRequest, _ ...grpc.CallOption) (*queueservicepb.DeleteSchemaResponse, error) {
	s.deleteSchemaRequest = request
	return &queueservicepb.DeleteSchemaResponse{Success: s.deleteSchemaErr == nil}, s.deleteSchemaErr
}

func (s *workflowQueueService) ListQueues(context.Context, *queueservicepb.ListQueuesRequest, ...grpc.CallOption) (*queueservicepb.ListQueuesResponse, error) {
	return s.listQueuesResponse, s.listQueuesErr
}

func (s *workflowQueueService) GetQueueState(context.Context, *queueservicepb.GetQueueStateRequest, ...grpc.CallOption) (*queueservicepb.GetQueueStateResponse, error) {
	return s.getQueueStateResponse, s.getQueueStateErr
}

func (s *workflowQueueService) PeekQueueMessages(context.Context, *queueservicepb.PeekQueueMessagesRequest, ...grpc.CallOption) (*queueservicepb.PeekQueueMessagesResponse, error) {
	return s.peekQueueResponse, s.peekQueueErr
}

func (s *workflowQueueService) GetDLQStats(context.Context, *queueservicepb.GetDLQStatsRequest, ...grpc.CallOption) (*queueservicepb.GetDLQStatsResponse, error) {
	return s.getDLQStatsResponse, s.getDLQStatsErr
}

func (s *workflowQueueService) ValidateCalendarSchedule(_ context.Context, request *queueservicepb.ValidateCalendarScheduleRequest, _ ...grpc.CallOption) (*queueservicepb.ValidateCalendarScheduleResponse, error) {
	s.validateCalendarRequest = request
	return s.validateCalendarResponse, s.validateCalendarErr
}

func (s *workflowQueueService) PreviewCalendarSchedule(_ context.Context, request *queueservicepb.PreviewCalendarScheduleRequest, _ ...grpc.CallOption) (*queueservicepb.PreviewCalendarScheduleResponse, error) {
	s.previewCalendarRequest = request
	return s.previewCalendarResponse, s.previewCalendarErr
}

func (s *workflowQueueService) CreateSchedule(_ context.Context, request *queueservicepb.CreateScheduleRequest, _ ...grpc.CallOption) (*queueservicepb.CreateScheduleResponse, error) {
	s.createScheduleRequest = request
	return &queueservicepb.CreateScheduleResponse{Success: s.createScheduleErr == nil}, s.createScheduleErr
}

func (s *workflowQueueService) GetSchedule(context.Context, *queueservicepb.GetScheduleRequest, ...grpc.CallOption) (*queueservicepb.GetScheduleResponse, error) {
	return s.getScheduleResponse, s.getScheduleErr
}

func (s *workflowQueueService) GetScheduleHistory(_ context.Context, request *queueservicepb.GetScheduleHistoryRequest, _ ...grpc.CallOption) (*queueservicepb.GetScheduleHistoryResponse, error) {
	s.getHistoryRequest = request
	return s.getHistoryResponse, s.getHistoryErr
}
