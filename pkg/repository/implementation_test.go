package repository

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservicepb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/pkg/calendar"
	repositorycommon "github.com/adrien19/chronoqueue/pkg/repository/common"
	"github.com/adrien19/chronoqueue/pkg/validator"
)

type stubValidator struct {
	result       *validator.ValidationResult
	called       bool
	validateFunc func(context.Context, *messagepb.Message) *validator.ValidationResult
}

func (v *stubValidator) Validate(ctx context.Context, message *messagepb.Message) *validator.ValidationResult {
	v.called = true
	if v.validateFunc != nil {
		return v.validateFunc(ctx, message)
	}
	return v.result
}

type enqueuedCall struct {
	queue   string
	message *messagepb.Message
}

type cancelledCall struct {
	queueName string
	messageId string
	reason    string
}

type stubBackend struct {
	queueMetadata *queuepb.QueueMetadata
	enqueued      []enqueuedCall
	enqueueErr    error
	enqueueErrs   []error // per-message errors for bulk operations
	enqueueTxErr  error   // transaction-level error for bulk operations
	cancelled     []cancelledCall
	cancelErr     error
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

func (b *stubBackend) EnqueueMessagesBulk(ctx context.Context, queueName string, messages []*messagepb.Message, transactionMode queueservicepb.PostMessagesBulkRequest_TransactionMode) ([]error, error) {
	errors := make([]error, len(messages))
	for i, message := range messages {
		b.enqueued = append(b.enqueued, enqueuedCall{queue: queueName, message: message})
		// Use per-message error if available, otherwise fall back to enqueueErr
		if b.enqueueErrs != nil && i < len(b.enqueueErrs) {
			errors[i] = b.enqueueErrs[i]
		} else {
			errors[i] = b.enqueueErr
		}
	}

	// Mimic real backend behavior: if txErr is set in ALL_OR_NOTHING mode,
	// mark all messages as failed (like the real postgres/sqlite backends do)
	if b.enqueueTxErr != nil && transactionMode == queueservicepb.PostMessagesBulkRequest_ALL_OR_NOTHING {
		for i := range errors {
			if errors[i] == nil {
				errors[i] = fmt.Errorf("transaction failed: %w", b.enqueueTxErr)
			}
		}
	}

	// Return transaction error (not per-message error)
	return errors, b.enqueueTxErr
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

func (b *stubBackend) CancelMessage(ctx context.Context, queueName string, messageId string, reason string) error {
	b.cancelled = append(b.cancelled, cancelledCall{queueName: queueName, messageId: messageId, reason: reason})
	return b.cancelErr
}

func (b *stubBackend) HeartbeatMessage(ctx context.Context, queueName string, messageId string, attemptId string) (messagepb.Message_Metadata_State, int64, error) {
	return messagepb.Message_Metadata_RUNNING, 30000, nil
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

func TestCancelMessage_Success(t *testing.T) {
	backend := &stubBackend{}
	impl := &implementation{backend: backend}

	req := &queueservicepb.CancelMessageRequest{
		QueueName: "queue-A",
		MessageId: "msg-1",
	}

	resp, err := impl.CancelMessage(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !resp.Success {
		t.Fatalf("expected success to be true")
	}

	if len(backend.cancelled) != 1 {
		t.Fatalf("expected 1 cancelled message, got %d", len(backend.cancelled))
	}

	call := backend.cancelled[0]
	if call.queueName != "queue-A" {
		t.Fatalf("expected queue queue-A, got %s", call.queueName)
	}
	if call.messageId != "msg-1" {
		t.Fatalf("expected message ID msg-1, got %s", call.messageId)
	}
	if call.reason != "" {
		t.Fatalf("expected empty reason, got %s", call.reason)
	}
}

func TestCancelMessage_WithReason(t *testing.T) {
	backend := &stubBackend{}
	impl := &implementation{backend: backend}

	reason := "Order cancelled by customer"
	req := &queueservicepb.CancelMessageRequest{
		QueueName: "queue-B",
		MessageId: "msg-2",
		Reason:    &reason,
	}

	resp, err := impl.CancelMessage(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !resp.Success {
		t.Fatalf("expected success to be true")
	}

	if len(backend.cancelled) != 1 {
		t.Fatalf("expected 1 cancelled message, got %d", len(backend.cancelled))
	}

	call := backend.cancelled[0]
	if call.queueName != "queue-B" {
		t.Fatalf("expected queue queue-B, got %s", call.queueName)
	}
	if call.messageId != "msg-2" {
		t.Fatalf("expected message ID msg-2, got %s", call.messageId)
	}
	if call.reason != reason {
		t.Fatalf("expected reason %q, got %q", reason, call.reason)
	}
}

func TestCancelMessage_BackendError(t *testing.T) {
	backend := &stubBackend{cancelErr: fmt.Errorf("database connection failed")}
	impl := &implementation{backend: backend}

	req := &queueservicepb.CancelMessageRequest{
		QueueName: "queue-C",
		MessageId: "msg-3",
	}

	_, err := impl.CancelMessage(context.Background(), req)
	if err == nil {
		t.Fatalf("expected backend error, got nil")
	}

	if !strings.Contains(err.Error(), "database connection failed") {
		t.Fatalf("expected backend error message, got: %v", err)
	}

	if len(backend.cancelled) != 1 {
		t.Fatalf("expected 1 cancel attempt, got %d", len(backend.cancelled))
	}
}

func TestCancelMessage_NilRequest(t *testing.T) {
	backend := &stubBackend{}
	impl := &implementation{backend: backend}

	_, err := impl.CancelMessage(context.Background(), nil)
	if err == nil {
		t.Fatalf("expected error for nil request, got nil")
	}

	if len(backend.cancelled) != 0 {
		t.Fatalf("expected no cancel attempts for nil request, got %d", len(backend.cancelled))
	}
}

// ============================================================================
// Bulk Posting Tests
// ============================================================================

func TestCreateQueueMessagesBulk_Success_AllOrNothing(t *testing.T) {
	backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 3}}
	impl := &implementation{backend: backend}

	messages := []*messagepb.Message{
		{MessageId: "msg-1"},
		{MessageId: "msg-2"},
		{MessageId: "msg-3"},
	}

	req := &queueservicepb.PostMessagesBulkRequest{
		QueueName:       "test-queue",
		Messages:        messages,
		TransactionMode: queueservicepb.PostMessagesBulkRequest_ALL_OR_NOTHING,
	}

	resp, err := impl.CreateQueueMessagesBulk(context.Background(), req, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !resp.Success {
		t.Fatalf("expected success=true, got false")
	}

	if resp.SuccessfulCount != 3 {
		t.Fatalf("expected 3 successful messages, got %d", resp.SuccessfulCount)
	}

	if resp.FailedCount != 0 {
		t.Fatalf("expected 0 failed messages, got %d", resp.FailedCount)
	}

	if len(resp.Results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(resp.Results))
	}

	for i, result := range resp.Results {
		if !result.Success {
			t.Fatalf("expected result[%d] success=true, got false", i)
		}
		if result.ErrorCode != queueservicepb.PostMessagesBulkResponse_MessagePostResult_SUCCESS {
			t.Fatalf("expected result[%d] error_code=SUCCESS, got %v", i, result.ErrorCode)
		}
		if result.MessageId != messages[i].MessageId {
			t.Fatalf("expected result[%d] message_id=%s, got %s", i, messages[i].MessageId, result.MessageId)
		}
	}

	if len(backend.enqueued) != 3 {
		t.Fatalf("expected 3 enqueued messages, got %d", len(backend.enqueued))
	}
}

func TestCreateQueueMessagesBulk_Success_BestEffort(t *testing.T) {
	backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 2}}
	impl := &implementation{backend: backend}

	messages := []*messagepb.Message{
		{MessageId: "msg-1"},
		{MessageId: "msg-2"},
	}

	req := &queueservicepb.PostMessagesBulkRequest{
		QueueName:       "test-queue",
		Messages:        messages,
		TransactionMode: queueservicepb.PostMessagesBulkRequest_BEST_EFFORT,
	}

	resp, err := impl.CreateQueueMessagesBulk(context.Background(), req, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !resp.Success {
		t.Fatalf("expected success=true, got false")
	}

	if resp.SuccessfulCount != 2 {
		t.Fatalf("expected 2 successful messages, got %d", resp.SuccessfulCount)
	}

	if len(backend.enqueued) != 2 {
		t.Fatalf("expected 2 enqueued messages, got %d", len(backend.enqueued))
	}
}

func TestCreateQueueMessagesBulk_EmptyMessages(t *testing.T) {
	backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{}}
	impl := &implementation{backend: backend}

	req := &queueservicepb.PostMessagesBulkRequest{
		QueueName:       "test-queue",
		Messages:        []*messagepb.Message{},
		TransactionMode: queueservicepb.PostMessagesBulkRequest_ALL_OR_NOTHING,
	}

	_, err := impl.CreateQueueMessagesBulk(context.Background(), req, nil)
	if err == nil {
		t.Fatalf("expected error for empty messages, got nil")
	}

	if !strings.Contains(err.Error(), "no messages provided") {
		t.Fatalf("expected 'no messages provided' error, got: %v", err)
	}
}

func TestCreateQueueMessagesBulk_TooManyMessages(t *testing.T) {
	backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{}}
	impl := &implementation{backend: backend}

	// Create 1001 messages (over the limit)
	messages := make([]*messagepb.Message, 1001)
	for i := range messages {
		messages[i] = &messagepb.Message{MessageId: fmt.Sprintf("msg-%d", i)}
	}

	req := &queueservicepb.PostMessagesBulkRequest{
		QueueName:       "test-queue",
		Messages:        messages,
		TransactionMode: queueservicepb.PostMessagesBulkRequest_ALL_OR_NOTHING,
	}

	_, err := impl.CreateQueueMessagesBulk(context.Background(), req, nil)
	if err == nil {
		t.Fatalf("expected error for too many messages, got nil")
	}

	if !strings.Contains(err.Error(), "too many messages") {
		t.Fatalf("expected 'too many messages' error, got: %v", err)
	}
}

func TestCreateQueueMessagesBulk_PayloadTooLarge(t *testing.T) {
	backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{}}
	impl := &implementation{backend: backend}

	// Create a single message with a payload exceeding 1MB
	largeData, err := structpb.NewStruct(map[string]interface{}{
		"blob": strings.Repeat("x", 1_048_577),
	})
	if err != nil {
		t.Fatalf("build payload: %v", err)
	}
	messages := []*messagepb.Message{
		{
			MessageId: "msg-large",
			Metadata: &messagepb.Message_Metadata{
				Payload: &commonpb.Payload{Data: largeData},
			},
		},
	}

	req := &queueservicepb.PostMessagesBulkRequest{
		QueueName:       "test-queue",
		Messages:        messages,
		TransactionMode: queueservicepb.PostMessagesBulkRequest_ALL_OR_NOTHING,
	}

	_, err = impl.CreateQueueMessagesBulk(context.Background(), req, nil)
	if err == nil {
		t.Fatalf("expected error for payload exceeding 1MB, got nil")
	}

	if !strings.Contains(err.Error(), "exceeds 1MB limit") {
		t.Fatalf("expected '1MB limit' error, got: %v", err)
	}

	if len(backend.enqueued) != 0 {
		t.Fatalf("expected 0 enqueued messages, got %d", len(backend.enqueued))
	}
}

func TestCreateQueueMessagesBulk_ValidationFails_AllOrNothing(t *testing.T) {
	backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 3}}
	impl := &implementation{backend: backend}

	val := &stubValidator{
		result: &validator.ValidationResult{
			Valid: false,
			Errors: []*validator.ValidationError{
				{Field: "payload.task", Message: "required field missing"},
			},
		},
	}

	messages := []*messagepb.Message{
		{MessageId: "msg-1"},
		{MessageId: "msg-2"},
	}

	req := &queueservicepb.PostMessagesBulkRequest{
		QueueName:       "test-queue",
		Messages:        messages,
		TransactionMode: queueservicepb.PostMessagesBulkRequest_ALL_OR_NOTHING,
	}

	_, err := impl.CreateQueueMessagesBulk(context.Background(), req, val)
	if err == nil {
		t.Fatalf("expected validation error, got nil")
	}

	if !strings.Contains(err.Error(), "validation failed") {
		t.Fatalf("expected validation error message, got: %v", err)
	}

	// No messages should be enqueued in ALL_OR_NOTHING mode on validation failure
	if len(backend.enqueued) != 0 {
		t.Fatalf("expected 0 enqueued messages on validation failure, got %d", len(backend.enqueued))
	}
}

func TestCreateQueueMessagesBulk_InheritsQueueDefaults(t *testing.T) {
	backend := &stubBackend{
		queueMetadata: &queuepb.QueueMetadata{
			DefaultMaxAttempts: 5,
		},
	}
	impl := &implementation{backend: backend}

	messages := []*messagepb.Message{
		{MessageId: "msg-1"},
	}

	req := &queueservicepb.PostMessagesBulkRequest{
		QueueName:       "test-queue",
		Messages:        messages,
		TransactionMode: queueservicepb.PostMessagesBulkRequest_BEST_EFFORT,
	}

	_, err := impl.CreateQueueMessagesBulk(context.Background(), req, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(backend.enqueued) != 1 {
		t.Fatalf("expected 1 enqueued message, got %d", len(backend.enqueued))
	}

	enqueuedMsg := backend.enqueued[0].message
	if enqueuedMsg.GetMetadata().GetMaxAttempts() != 5 {
		t.Fatalf("expected max_attempts=5 from queue defaults, got %d", enqueuedMsg.GetMetadata().GetMaxAttempts())
	}

	if enqueuedMsg.GetMetadata().GetAttemptsLeft() != 5 {
		t.Fatalf("expected attempts_left=5, got %d", enqueuedMsg.GetMetadata().GetAttemptsLeft())
	}

	if enqueuedMsg.GetMetadata().GetPriority() != 0 {
		t.Fatalf("expected default priority=0, got %d", enqueuedMsg.GetMetadata().GetPriority())
	}

	if enqueuedMsg.GetMetadata().GetState() != messagepb.Message_Metadata_PENDING {
		t.Fatalf("expected state=PENDING, got %v", enqueuedMsg.GetMetadata().GetState())
	}
}

func TestCreateQueueMessagesBulk_NilRequest(t *testing.T) {
	backend := &stubBackend{}
	impl := &implementation{backend: backend}

	_, err := impl.CreateQueueMessagesBulk(context.Background(), nil, nil)
	if err == nil {
		t.Fatalf("expected error for nil request, got nil")
	}

	if !strings.Contains(err.Error(), "request is required") {
		t.Fatalf("expected 'request is required' error, got: %v", err)
	}
}

func TestCreateQueueMessagesBulk_BestEffort_MixedValidation(t *testing.T) {
	backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 3}}
	impl := &implementation{backend: backend}

	// Validator that fails for msg-2
	val := &stubValidator{
		validateFunc: func(ctx context.Context, message *messagepb.Message) *validator.ValidationResult {
			if message.MessageId == "msg-2" {
				return &validator.ValidationResult{
					Valid: false,
					Errors: []*validator.ValidationError{
						{Field: "payload.data", Message: "missing required field"},
					},
				}
			}
			return &validator.ValidationResult{Valid: true}
		},
	}

	messages := []*messagepb.Message{
		{MessageId: "msg-1"},
		{MessageId: "msg-2"},
		{MessageId: "msg-3"},
	}

	req := &queueservicepb.PostMessagesBulkRequest{
		QueueName:       "test-queue",
		Messages:        messages,
		TransactionMode: queueservicepb.PostMessagesBulkRequest_BEST_EFFORT,
	}

	resp, err := impl.CreateQueueMessagesBulk(context.Background(), req, val)
	if err != nil {
		t.Fatalf("unexpected error in BEST_EFFORT mode: %v", err)
	}

	if resp.SuccessfulCount != 2 {
		t.Fatalf("expected 2 successful messages, got %d", resp.SuccessfulCount)
	}

	if resp.FailedCount != 1 {
		t.Fatalf("expected 1 failed message, got %d", resp.FailedCount)
	}

	if !resp.Success {
		t.Fatalf("expected success=true in BEST_EFFORT mode when successfulCount > 0")
	}

	if len(resp.Results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(resp.Results))
	}

	// msg-1: success
	if !resp.Results[0].Success {
		t.Fatalf("expected msg-1 to succeed")
	}
	// msg-2: validation failed
	if resp.Results[1].Success {
		t.Fatalf("expected msg-2 to fail")
	}
	if resp.Results[1].ErrorCode != queueservicepb.PostMessagesBulkResponse_MessagePostResult_VALIDATION_FAILED {
		t.Fatalf("expected VALIDATION_FAILED for msg-2, got %v", resp.Results[1].ErrorCode)
	}
	// msg-3: success
	if !resp.Results[2].Success {
		t.Fatalf("expected msg-3 to succeed")
	}

	// Only 2 messages should be enqueued (msg-1 and msg-3)
	if len(backend.enqueued) != 2 {
		t.Fatalf("expected 2 enqueued messages, got %d", len(backend.enqueued))
	}
}

func TestCreateQueueMessagesBulk_BackendInternalError(t *testing.T) {
	backend := &stubBackend{
		queueMetadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 3},
		enqueueErrs: []error{
			nil,
			fmt.Errorf("database connection lost"),
			nil,
		},
	}
	impl := &implementation{backend: backend}

	messages := []*messagepb.Message{
		{MessageId: "msg-1"},
		{MessageId: "msg-2"},
		{MessageId: "msg-3"},
	}

	req := &queueservicepb.PostMessagesBulkRequest{
		QueueName:       "test-queue",
		Messages:        messages,
		TransactionMode: queueservicepb.PostMessagesBulkRequest_BEST_EFFORT,
	}

	resp, err := impl.CreateQueueMessagesBulk(context.Background(), req, nil)
	if err != nil {
		t.Fatalf("unexpected error in BEST_EFFORT mode: %v", err)
	}

	if resp.SuccessfulCount != 2 {
		t.Fatalf("expected 2 successful, got %d", resp.SuccessfulCount)
	}

	if resp.FailedCount != 1 {
		t.Fatalf("expected 1 failed, got %d", resp.FailedCount)
	}

	if resp.Results[1].ErrorCode != queueservicepb.PostMessagesBulkResponse_MessagePostResult_INTERNAL_ERROR {
		t.Fatalf("expected INTERNAL_ERROR for msg-2, got %v", resp.Results[1].ErrorCode)
	}

	if !strings.Contains(resp.Results[1].Error, "database connection lost") {
		t.Fatalf("expected error message to contain backend error")
	}
}

func TestCreateQueueMessagesBulk_DuplicateMessageID(t *testing.T) {
	backend := &stubBackend{
		queueMetadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 3},
		enqueueErrs: []error{
			nil,
			repositorycommon.ErrDuplicateMessageID,
			nil,
		},
	}
	impl := &implementation{backend: backend}

	messages := []*messagepb.Message{
		{MessageId: "msg-1"},
		{MessageId: "msg-2"},
		{MessageId: "msg-3"},
	}

	req := &queueservicepb.PostMessagesBulkRequest{
		QueueName:       "test-queue",
		Messages:        messages,
		TransactionMode: queueservicepb.PostMessagesBulkRequest_BEST_EFFORT,
	}

	resp, err := impl.CreateQueueMessagesBulk(context.Background(), req, nil)
	if err != nil {
		t.Fatalf("unexpected error in BEST_EFFORT mode: %v", err)
	}

	if resp.SuccessfulCount != 2 {
		t.Fatalf("expected 2 successful, got %d", resp.SuccessfulCount)
	}

	if resp.Results[1].ErrorCode != queueservicepb.PostMessagesBulkResponse_MessagePostResult_DUPLICATE_MESSAGE_ID {
		t.Fatalf("expected DUPLICATE_MESSAGE_ID for msg-2, got %v", resp.Results[1].ErrorCode)
	}
}

func TestCreateQueueMessagesBulk_AllOrNothing_TransactionFailure(t *testing.T) {
	backend := &stubBackend{
		queueMetadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 3},
		enqueueTxErr:  fmt.Errorf("transaction commit failed"),
	}
	impl := &implementation{backend: backend}

	messages := []*messagepb.Message{
		{MessageId: "msg-1"},
		{MessageId: "msg-2"},
	}

	req := &queueservicepb.PostMessagesBulkRequest{
		QueueName:       "test-queue",
		Messages:        messages,
		TransactionMode: queueservicepb.PostMessagesBulkRequest_ALL_OR_NOTHING,
	}

	resp, err := impl.CreateQueueMessagesBulk(context.Background(), req, nil)
	if err == nil {
		t.Fatalf("expected error for transaction failure in ALL_OR_NOTHING mode")
	}

	if !strings.Contains(err.Error(), "transaction failed") {
		t.Fatalf("expected 'transaction failed' in error, got: %v", err)
	}

	// Response should still be returned with failure details
	if resp == nil {
		t.Fatalf("expected response even on transaction failure")
	}

	if resp.SuccessfulCount != 0 {
		t.Fatalf("expected 0 successful on transaction failure, got %d", resp.SuccessfulCount)
	}

	if resp.Success {
		t.Fatalf("expected success=false on transaction failure")
	}
}

func TestCreateQueueMessagesBulk_BestEffort_PartialTransactionFailure(t *testing.T) {
	backend := &stubBackend{
		queueMetadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 3},
		enqueueErrs: []error{
			nil,
			fmt.Errorf("write failed"),
		},
		enqueueTxErr: fmt.Errorf("partial transaction error"),
	}
	impl := &implementation{backend: backend}

	messages := []*messagepb.Message{
		{MessageId: "msg-1"},
		{MessageId: "msg-2"},
	}

	req := &queueservicepb.PostMessagesBulkRequest{
		QueueName:       "test-queue",
		Messages:        messages,
		TransactionMode: queueservicepb.PostMessagesBulkRequest_BEST_EFFORT,
	}

	resp, err := impl.CreateQueueMessagesBulk(context.Background(), req, nil)
	if err == nil {
		t.Fatalf("expected error for transaction failure in BEST_EFFORT mode")
	}

	if !strings.Contains(err.Error(), "partial bulk enqueue failure") {
		t.Fatalf("expected 'partial bulk enqueue failure' in error, got: %v", err)
	}

	// Response should contain partial success details
	if resp == nil {
		t.Fatalf("expected response even with txErr")
	}

	if resp.SuccessfulCount != 1 {
		t.Fatalf("expected 1 successful, got %d", resp.SuccessfulCount)
	}

	if resp.FailedCount != 1 {
		t.Fatalf("expected 1 failed, got %d", resp.FailedCount)
	}

	// Both messages were "enqueued" (added to stub's list)
	if len(backend.enqueued) != 2 {
		t.Fatalf("expected 2 entries in backend.enqueued, got %d", len(backend.enqueued))
	}
}

func TestCreateQueueMessagesBulk_NilMessageInSlice_BestEffort(t *testing.T) {
	backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 3}}
	impl := &implementation{backend: backend}

	messages := []*messagepb.Message{
		{MessageId: "msg-1"},
		nil, // nil message should be treated as a validation error
		{MessageId: "msg-3"},
	}

	req := &queueservicepb.PostMessagesBulkRequest{
		QueueName:       "test-queue",
		Messages:        messages,
		TransactionMode: queueservicepb.PostMessagesBulkRequest_BEST_EFFORT,
	}

	resp, err := impl.CreateQueueMessagesBulk(context.Background(), req, nil)
	if err != nil {
		t.Fatalf("unexpected error in BEST_EFFORT mode: %v", err)
	}

	if resp.SuccessfulCount != 2 {
		t.Fatalf("expected 2 successful, got %d", resp.SuccessfulCount)
	}
	if resp.FailedCount != 1 {
		t.Fatalf("expected 1 failed, got %d", resp.FailedCount)
	}
	if resp.Results[1].ErrorCode != queueservicepb.PostMessagesBulkResponse_MessagePostResult_VALIDATION_FAILED {
		t.Fatalf("expected VALIDATION_FAILED for nil message, got %v", resp.Results[1].ErrorCode)
	}
	if resp.Results[1].MessageId != "" {
		t.Fatalf("expected empty MessageId for nil message, got %s", resp.Results[1].MessageId)
	}

	// Verify only the 2 non-nil messages were enqueued
	if len(backend.enqueued) != 2 {
		t.Fatalf("expected 2 messages enqueued, got %d", len(backend.enqueued))
	}
}
