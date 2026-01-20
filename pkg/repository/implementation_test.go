package repository

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservicepb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/pkg/calendar"
	"github.com/adrien19/chronoqueue/pkg/validator"
)

type stubValidator struct {
	result *validator.ValidationResult
	called bool
}

func (v *stubValidator) Validate(ctx context.Context, message *messagepb.Message) *validator.ValidationResult {
	v.called = true
	return v.result
}

type enqueuedCall struct {
	queue   string
	message *messagepb.Message
}

type stubBackend struct {
	queueMetadata *queuepb.QueueMetadata
	enqueued      []enqueuedCall
	enqueueErr    error
}

type stubEngine struct {
	preview     *calendar.SchedulePreview
	previewErr  error
	validateErr error
}

func (e *stubEngine) CalculateNextRun(ctx context.Context, calendarSchedule *schedulepb.CalendarSchedule, from time.Time) (*time.Time, error) {
	return nil, nil
}

func (e *stubEngine) CalculateNextRuns(ctx context.Context, calendarSchedule *schedulepb.CalendarSchedule, from time.Time, count int) ([]time.Time, error) {
	return nil, nil
}

func (e *stubEngine) ValidateSchedule(ctx context.Context, calendarSchedule *schedulepb.CalendarSchedule) error {
	return e.validateErr
}

func (e *stubEngine) PreviewSchedule(ctx context.Context, calendarSchedule *schedulepb.CalendarSchedule, from time.Time, count int) (*calendar.SchedulePreview, error) {
	return e.preview, e.previewErr
}

func (e *stubEngine) IsBusinessDay(ctx context.Context, date time.Time, businessCalendar *schedulepb.BusinessCalendar) (bool, error) {
	return true, nil
}

func (e *stubEngine) GetHolidays(ctx context.Context, businessCalendar *schedulepb.BusinessCalendar, from, to time.Time) ([]calendar.Holiday, error) {
	return nil, nil
}

// BackendStorage impl stubs
func (b *stubBackend) Close() error                                                { return nil }
func (b *stubBackend) CreateQueue(ctx context.Context, queue *queuepb.Queue) error { return nil }
func (b *stubBackend) GetQueue(ctx context.Context, name string) (*queuepb.Queue, error) {
	return &queuepb.Queue{Name: name, Metadata: b.queueMetadata}, nil
}

func (b *stubBackend) GetQueueMetadata(ctx context.Context, name string) (*queuepb.QueueMetadata, error) {
	if b.queueMetadata != nil {
		return b.queueMetadata, nil
	}
	return &queuepb.QueueMetadata{}, nil
}
func (b *stubBackend) ListQueues(ctx context.Context) ([]*queuepb.Queue, error) { return nil, nil }
func (b *stubBackend) DeleteQueue(ctx context.Context, name string) error       { return nil }
func (b *stubBackend) EnqueueMessage(ctx context.Context, queueName string, message *messagepb.Message) error {
	b.enqueued = append(b.enqueued, enqueuedCall{queue: queueName, message: message})
	return b.enqueueErr
}

func (b *stubBackend) ClaimMessage(ctx context.Context, queueName string, workerId string, attemptId string) (*messagepb.Message, error) {
	return nil, nil
}

func (b *stubBackend) AcknowledgeMessage(ctx context.Context, queueName string, messageId string, attemptId string) error {
	return nil
}

func (b *stubBackend) NackMessage(ctx context.Context, queueName string, messageId string, attemptId string) error {
	return nil
}

func (b *stubBackend) HeartbeatMessage(ctx context.Context, queueName string, messageId string, attemptId string) error {
	return nil
}

func (b *stubBackend) ExtendMessageLease(ctx context.Context, queueName string, messageId string, attemptId string, extensionMs int64) error {
	return nil
}

func (b *stubBackend) PeekMessages(ctx context.Context, queueName string, limit int32) ([]*messagepb.Message, error) {
	return nil, nil
}

func (b *stubBackend) CreateSchedule(ctx context.Context, schedule *schedulepb.Schedule) error {
	return nil
}

func (b *stubBackend) GetSchedule(ctx context.Context, scheduleId string) (*schedulepb.Schedule, error) {
	return nil, nil
}

func (b *stubBackend) ListSchedules(ctx context.Context, queueName string) ([]*schedulepb.Schedule, error) {
	return nil, nil
}
func (b *stubBackend) DeleteSchedule(ctx context.Context, scheduleId string) error { return nil }
func (b *stubBackend) PauseSchedule(ctx context.Context, scheduleId string) error  { return nil }
func (b *stubBackend) ResumeSchedule(ctx context.Context, scheduleId string) error { return nil }
func (b *stubBackend) RecordScheduleExecution(ctx context.Context, scheduleId string, messageId string, executionTime int64) error {
	return nil
}

func (b *stubBackend) GetScheduleHistory(ctx context.Context, scheduleId string, limit int64) (*schedulepb.ScheduleHistory, error) {
	return nil, nil
}

func (b *stubBackend) GetDLQMessages(ctx context.Context, dlqName string, limit int32) ([]*messagepb.Message, error) {
	return nil, nil
}

func (b *stubBackend) RetryDLQMessage(ctx context.Context, dlqName string, messageId string) error {
	return nil
}

func (b *stubBackend) DeleteDLQMessage(ctx context.Context, dlqName string, messageId string) error {
	return nil
}
func (b *stubBackend) PurgeDLQ(ctx context.Context, dlqName string) (int64, error) { return 0, nil }

func TestCreateQueueMessage_ValidatorNil(t *testing.T) {
	backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 2}}
	impl := &implementation{backend: backend}

	req := &queueservicepb.PostMessageRequest{
		QueueName: "queue-A",
		Message:   &messagepb.Message{MessageId: "msg-1"},
	}

	_, err := impl.CreateQueueMessage(context.Background(), req, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(backend.enqueued) != 1 {
		t.Fatalf("expected 1 enqueued message, got %d", len(backend.enqueued))
	}
	if backend.enqueued[0].queue != "queue-A" {
		t.Fatalf("expected queue queue-A, got %s", backend.enqueued[0].queue)
	}
	if backend.enqueued[0].message.GetMetadata().GetState() != messagepb.Message_Metadata_PENDING {
		t.Fatalf("expected message state to be PENDING")
	}
}

func TestCreateQueueMessage_ValidationFails(t *testing.T) {
	backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 3}}
	impl := &implementation{backend: backend}

	val := &stubValidator{
		result: &validator.ValidationResult{
			Valid: false,
			Errors: []*validator.ValidationError{
				{Field: "payload.task", Message: "required field missing"},
				{Field: "payload.priority", Message: "must be between 1 and 10"},
			},
		},
	}

	req := &queueservicepb.PostMessageRequest{
		QueueName: "queue-B",
		Message:   &messagepb.Message{MessageId: "msg-2"},
	}

	_, err := impl.CreateQueueMessage(context.Background(), req, val)
	if err == nil {
		t.Fatalf("expected validation error, got nil")
	}

	if !val.called {
		t.Fatalf("expected validator to be called")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "Message validation failed:") || !strings.Contains(errMsg, "payload.task") || !strings.Contains(errMsg, "payload.priority") {
		t.Fatalf("unexpected error message: %s", errMsg)
	}

	if len(backend.enqueued) != 0 {
		t.Fatalf("expected no enqueued messages, got %d", len(backend.enqueued))
	}
}

func TestCreateQueueMessage_ValidationPasses(t *testing.T) {
	backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 1}}
	impl := &implementation{backend: backend}

	val := &stubValidator{result: &validator.ValidationResult{Valid: true}}

	req := &queueservicepb.PostMessageRequest{
		QueueName: "queue-C",
		Message:   &messagepb.Message{MessageId: "msg-3"},
	}

	_, err := impl.CreateQueueMessage(context.Background(), req, val)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !val.called {
		t.Fatalf("expected validator to be called")
	}

	if len(backend.enqueued) != 1 {
		t.Fatalf("expected 1 enqueued message, got %d", len(backend.enqueued))
	}
	if backend.enqueued[0].queue != "queue-C" {
		t.Fatalf("expected queue queue-C, got %s", backend.enqueued[0].queue)
	}
}

func TestGetCalendarSchedulePreview_Success(t *testing.T) {
	now := time.Unix(1700000000, 0)
	preview := &calendar.SchedulePreview{
		Timezone: "UTC",
		ExecutionTimes: []calendar.ExecutionTime{
			{Time: now.Add(time.Hour)},
			{Time: now.Add(2 * time.Hour)},
		},
	}

	impl := &implementation{calendarEngine: &stubEngine{preview: preview}}

	resp, err := impl.GetCalendarSchedulePreview(context.Background(), &schedulepb.CalendarSchedule{}, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.GetExecutionTimes()) != len(preview.ExecutionTimes) {
		t.Fatalf("expected %d execution times, got %d", len(preview.ExecutionTimes), len(resp.GetExecutionTimes()))
	}

	for i, ts := range resp.GetExecutionTimes() {
		if got, want := ts.AsTime(), preview.ExecutionTimes[i].Time; !got.Equal(want) {
			t.Fatalf("execution time %d mismatch: got %v want %v", i, got, want)
		}
	}

	if resp.GetTimezone() != preview.Timezone {
		t.Fatalf("expected timezone %s, got %s", preview.Timezone, resp.GetTimezone())
	}

	if resp.GetTotalCount() != int32(len(preview.ExecutionTimes)) {
		t.Fatalf("expected total count %d, got %d", len(preview.ExecutionTimes), resp.GetTotalCount())
	}

	if resp.GetPreviewStart() == nil || resp.GetPreviewStart().AsTime().IsZero() {
		t.Fatalf("expected preview start to be set")
	}
}

func TestGetCalendarSchedulePreview_NilSchedule(t *testing.T) {
	impl := &implementation{calendarEngine: &stubEngine{}}

	_, err := impl.GetCalendarSchedulePreview(context.Background(), nil, 1)
	if err == nil || !strings.Contains(err.Error(), "cannot be nil") {
		t.Fatalf("expected nil schedule error, got %v", err)
	}
}

func TestGetCalendarSchedulePreview_EngineError(t *testing.T) {
	impl := &implementation{calendarEngine: &stubEngine{previewErr: fmt.Errorf("preview failed")}}

	_, err := impl.GetCalendarSchedulePreview(context.Background(), &schedulepb.CalendarSchedule{}, 1)
	if err == nil || !strings.Contains(err.Error(), "preview failed") {
		t.Fatalf("expected engine error, got %v", err)
	}
}
