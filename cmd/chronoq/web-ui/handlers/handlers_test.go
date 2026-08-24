package handlers

import (
	"html/template"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	clusterstore "github.com/adrien19/chronoqueue/cmd/chronoq/web-ui/cluster"
	"github.com/adrien19/chronoqueue/pkg/log"
)

func TestStatusClass(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"COMPLETED", "cq-badge cq-badge-good"},
		{"ERRORED", "cq-badge border-red-500/25 bg-red-500/10 text-red-300"},
		{"RUNNING", "cq-badge border-sky-500/25 bg-sky-500/10 text-sky-300"},
		{"unknown", "cq-badge cq-badge-muted"},
	}
	for _, c := range cases {
		got := StatusClass(c.input)
		if got != c.want {
			t.Errorf("StatusClass(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

func TestShortenID(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"short", "short"},
		{"exactly12chars", "exactly12cha"},
		{"", ""},
	}
	for _, c := range cases {
		got := shortenID(c.input)
		if got != c.want {
			t.Errorf("shortenID(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

func TestBuildQueueAssociations(t *testing.T) {
	queues := []*queue_pb.Queue{
		{Name: "orders", Metadata: &queue_pb.QueueMetadata{DeadLetterQueueName: "orders-dlq"}},
		{Name: "billing", Metadata: &queue_pb.QueueMetadata{DeadLetterQueueName: "failed-payments"}},
		{Name: "refunds", Metadata: &queue_pb.QueueMetadata{DeadLetterQueueName: "failed-payments"}},
		{Name: "unrelated-dlq", Metadata: &queue_pb.QueueMetadata{}},
		{Name: "orders-dlq", Metadata: &queue_pb.QueueMetadata{}},
		{Name: "failed-payments", Metadata: &queue_pb.QueueMetadata{}},
		nil,
	}

	got := buildQueueAssociations(queues)
	if got.dlqBySource["orders"] != "orders-dlq" {
		t.Fatalf("conventional DLQ = %q, want orders-dlq", got.dlqBySource["orders"])
	}
	if got.dlqBySource["billing"] != "failed-payments" {
		t.Fatalf("custom DLQ = %q, want failed-payments", got.dlqBySource["billing"])
	}
	if sources := got.sourcesByDLQ["failed-payments"]; len(sources) != 2 || sources[0] != "billing" || sources[1] != "refunds" {
		t.Fatalf("shared DLQ sources = %v, want [billing refunds]", sources)
	}
	if _, exists := got.sourcesByDLQ["unrelated-dlq"]; exists {
		t.Fatal("unrelated suffix queue was classified as a DLQ")
	}
}

func TestSelectDLQTarget(t *testing.T) {
	tests := []struct {
		name      string
		dlq       string
		sources   []string
		requested string
		want      string
		wantError bool
	}{
		{name: "single source defaults", dlq: "orders-dlq", sources: []string{"orders"}, want: "orders"},
		{name: "shared source selected", dlq: "failures", sources: []string{"orders", "billing"}, requested: "billing", want: "billing"},
		{name: "shared source requires selection", dlq: "failures", sources: []string{"orders", "billing"}, wantError: true},
		{name: "rejects unrelated target", dlq: "failures", sources: []string{"orders"}, requested: "billing", wantError: true},
		{name: "rejects unconfigured DLQ", dlq: "suffix-dlq", wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := selectDLQTarget(test.dlq, test.sources, test.requested)
			if (err != nil) != test.wantError {
				t.Fatalf("selectDLQTarget() error = %v, wantError %v", err, test.wantError)
			}
			if got != test.want {
				t.Fatalf("selectDLQTarget() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestBuildMessageDisplaysPreservesStatesAndScheduledTimes(t *testing.T) {
	past := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	future := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	states := []message_pb.Message_Metadata_State{
		message_pb.Message_Metadata_INVISIBLE,
		message_pb.Message_Metadata_PENDING,
		message_pb.Message_Metadata_RUNNING,
		message_pb.Message_Metadata_COMPLETED,
		message_pb.Message_Metadata_CANCELED,
		message_pb.Message_Metadata_ERRORED,
	}
	raw := make([]*message_pb.Message, 0, len(states)+2)
	for i, state := range states {
		metadata := &message_pb.Message_Metadata{State: state, MaxAttempts: 3, AttemptsLeft: 2}
		if i == 0 {
			metadata.ScheduledTime = timestamppb.New(future)
		}
		if i == 1 {
			metadata.ScheduledTime = timestamppb.New(past)
		}
		raw = append(raw, &message_pb.Message{MessageId: state.String(), Metadata: metadata})
	}
	raw = append(raw, nil, &message_pb.Message{MessageId: "missing-metadata"})

	displays := buildMessageDisplays(raw)
	if len(displays) != len(states) {
		t.Fatalf("display count = %d, want %d", len(displays), len(states))
	}
	for i, state := range states {
		if displays[i].State != state.String() {
			t.Errorf("state[%d] = %q, want %q", i, displays[i].State, state)
		}
	}
	if displays[0].ScheduledAt == nil || !displays[0].ScheduledAt.Equal(future) {
		t.Fatalf("future scheduled time = %v, want %v", displays[0].ScheduledAt, future)
	}
	if displays[1].ScheduledAt == nil || !displays[1].ScheduledAt.Equal(past) {
		t.Fatalf("past scheduled time = %v, want %v", displays[1].ScheduledAt, past)
	}
	if displays[2].ScheduledAt != nil {
		t.Fatalf("absent scheduled time = %v, want nil", displays[2].ScheduledAt)
	}
}

func TestQueuesHandlerWithoutActiveClientReturnsServiceUnavailable(t *testing.T) {
	templates := template.Must(template.New("test").Parse(`{{ define "base" }}{{ .ErrorMessage }}{{ end }}`))
	handler := NewQueuesHandler(templates, clusterstore.NewStore(""), log.NewLogger(log.WithLevel(logrus.PanicLevel)))
	recorder := httptest.NewRecorder()

	handler.List(recorder, httptest.NewRequest(http.MethodGet, "/queues", nil))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	if recorder.Body.String() != "ChronoQueue backend is unavailable" {
		t.Fatalf("body = %q", recorder.Body.String())
	}
}

func TestFormatDuration(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{45 * time.Second, "45s"},
		{2 * time.Minute, "2m"},
		{90 * time.Second, "1m"},
		{2*time.Hour + 5*time.Minute, "2h 5m"},
	}
	for _, c := range cases {
		got := formatDuration(c.d)
		if got != c.want {
			t.Errorf("formatDuration(%v) = %q, want %q", c.d, got, c.want)
		}
	}
}
