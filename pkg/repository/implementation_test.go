package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservicepb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/internal/domainerror"
	"github.com/adrien19/chronoqueue/pkg/calendar"
	repositorycommon "github.com/adrien19/chronoqueue/pkg/repository/common"
	repositorysql "github.com/adrien19/chronoqueue/pkg/repository/sql"
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

type claimCall struct {
	queueName      string
	workerId       string
	attemptId      string
	exclusivityKey string
	leaseDuration  time.Duration
}

type peekCall struct {
	queueName     string
	limit         int32
	priorityRange *repositorysql.PriorityRange
}

type stubBackend struct {
	queueMetadata    *queuepb.QueueMetadata
	getQueueErr      error
	createdQueues    []*queuepb.Queue
	createQueueErrs  []error
	deletedQueues    []string
	claims           []claimCall
	peekCalls        []peekCall
	dlqLimits        []int32
	queues           []*queuepb.Queue
	schedules        []*schedulepb.Schedule
	messages         []*messagepb.Message
	history          *schedulepb.ScheduleHistory
	queuePrefix      string
	schedulePrefix   string
	enqueued         []enqueuedCall
	enqueueErr       error
	enqueueErrs      []error // per-message errors for bulk operations
	enqueueTxErr     error   // transaction-level error for bulk operations
	enqueueErrorsNil bool
	cancelled        []cancelledCall
	cancelErr        error
	extendRemaining  int64
	pingErr          error
}

type stubEngine struct {
	preview     *calendar.SchedulePreview
	previewErr  error
	validateErr error
	nextRunErr  error
}

func (e *stubEngine) CalculateNextRun(ctx context.Context, calendarSchedule *schedulepb.CalendarSchedule, from time.Time) (*time.Time, error) {
	return nil, e.nextRunErr
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
func (b *stubBackend) Close() error                   { return nil }
func (b *stubBackend) Ping(ctx context.Context) error { return b.pingErr }

func TestImplementationPing(t *testing.T) {
	backendFailure := errors.New("backend unavailable")
	tests := []struct {
		name      string
		backend   BackendStorage
		wantError error
		wantText  string
	}{
		{name: "backend success", backend: &stubBackend{}},
		{name: "backend failure", backend: &stubBackend{pingErr: backendFailure}, wantError: backendFailure},
		{name: "nil backend", wantText: "storage backend is not initialized"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			impl := &implementation{backend: tt.backend}
			err := impl.Ping(context.Background())

			if tt.wantError != nil {
				require.ErrorIs(t, err, tt.wantError)
				return
			}
			if tt.wantText != "" {
				require.ErrorContains(t, err, tt.wantText)
				return
			}
			require.NoError(t, err)
		})
	}
}

func (b *stubBackend) CreateQueue(ctx context.Context, queue *queuepb.Queue) error {
	b.createdQueues = append(b.createdQueues, queue)
	if call := len(b.createdQueues) - 1; call < len(b.createQueueErrs) {
		return b.createQueueErrs[call]
	}
	return nil
}

func (b *stubBackend) CreateQueueWithDLQ(ctx context.Context, queue *queuepb.Queue, dlq *queuepb.Queue) error {
	b.createdQueues = append(b.createdQueues, queue, dlq)
	for _, err := range b.createQueueErrs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *stubBackend) GetQueue(ctx context.Context, name string) (*queuepb.Queue, error) {
	if b.getQueueErr != nil {
		return nil, b.getQueueErr
	}
	return &queuepb.Queue{Name: name, Metadata: b.queueMetadata}, nil
}

func (b *stubBackend) GetQueueMetadata(ctx context.Context, name string) (*queuepb.QueueMetadata, error) {
	if b.queueMetadata != nil {
		return b.queueMetadata, nil
	}
	return &queuepb.QueueMetadata{}, nil
}

func (b *stubBackend) IsDLQ(ctx context.Context, name string) (bool, error) {
	for _, queue := range b.queues {
		if queue.GetMetadata().GetDeadLetterQueueName() == name {
			return true, nil
		}
	}
	return false, nil
}

func (b *stubBackend) ListQueuesWithPrefix(ctx context.Context, prefix string) ([]*queuepb.Queue, error) {
	b.queuePrefix = prefix
	return b.queues, nil
}

func (b *stubBackend) ListQueuesPage(ctx context.Context, prefix string, limit int32, cursor string) ([]*queuepb.Queue, string, error) {
	b.queuePrefix = prefix
	items, next := keysetPage(b.queues, limit, cursor, func(queue *queuepb.Queue) string { return queue.GetName() })
	return items, next, nil
}

func (b *stubBackend) DeleteQueue(ctx context.Context, name string) error {
	b.deletedQueues = append(b.deletedQueues, name)
	return nil
}

func (b *stubBackend) EnqueueMessage(ctx context.Context, queueName string, message *messagepb.Message) error {
	b.enqueued = append(b.enqueued, enqueuedCall{queue: queueName, message: message})
	return b.enqueueErr
}

func (b *stubBackend) EnqueueMessagesBulk(ctx context.Context, queueName string, messages []*messagepb.Message, transactionMode queueservicepb.PostMessagesBulkRequest_TransactionMode) ([]error, error) {
	if b.enqueueErrorsNil {
		return nil, b.enqueueTxErr
	}
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

func (b *stubBackend) ClaimMessageWithLeaseDuration(ctx context.Context, queueName string, workerId string, attemptId string, exclusivityKey string, leaseDuration time.Duration) (*messagepb.Message, error) {
	b.claims = append(b.claims, claimCall{queueName: queueName, workerId: workerId, attemptId: attemptId, exclusivityKey: exclusivityKey, leaseDuration: leaseDuration})
	return nil, nil
}

func (b *stubBackend) AcknowledgeMessage(ctx context.Context, queueName string, messageId string, attemptId string, workerId string) error {
	return nil
}

func (b *stubBackend) NackMessage(ctx context.Context, queueName string, messageId string, attemptId string, workerId string) error {
	return nil
}

func (b *stubBackend) CancelMessage(ctx context.Context, queueName string, messageId string, reason string) error {
	b.cancelled = append(b.cancelled, cancelledCall{queueName: queueName, messageId: messageId, reason: reason})
	return b.cancelErr
}

func (b *stubBackend) HeartbeatMessage(ctx context.Context, queueName string, messageId string, attemptId string, workerId string) (messagepb.Message_Metadata_State, int64, error) {
	return messagepb.Message_Metadata_RUNNING, 30000, nil
}

func (b *stubBackend) ExtendMessageLease(ctx context.Context, queueName string, messageId string, attemptId string, workerId string, extensionMs int64) (int64, error) {
	if b.extendRemaining != 0 {
		return b.extendRemaining, nil
	}
	return extensionMs, nil
}

func (b *stubBackend) PeekMessagesWithPriorityRange(ctx context.Context, queueName string, limit int32, priorityRange *repositorysql.PriorityRange) ([]*messagepb.Message, error) {
	b.peekCalls = append(b.peekCalls, peekCall{queueName: queueName, limit: limit, priorityRange: priorityRange})
	return nil, nil
}

func (b *stubBackend) PeekMessagesPage(ctx context.Context, queueName string, limit int32, offset int64, priorityRange *repositorysql.PriorityRange) ([]*messagepb.Message, error) {
	b.peekCalls = append(b.peekCalls, peekCall{queueName: queueName, limit: limit, priorityRange: priorityRange})
	return pageSlice(b.messages, limit, offset), nil
}

func (b *stubBackend) CreateSchedule(ctx context.Context, schedule *schedulepb.Schedule) error {
	b.schedules = append(b.schedules, schedule)
	return nil
}

func (b *stubBackend) GetSchedule(ctx context.Context, scheduleId string) (*schedulepb.Schedule, error) {
	return nil, nil
}

func (b *stubBackend) ListSchedules(ctx context.Context, queueName string) ([]*schedulepb.Schedule, error) {
	return b.schedules, nil
}

func (b *stubBackend) ListSchedulesWithPrefix(ctx context.Context, prefix string) ([]*schedulepb.Schedule, error) {
	b.schedulePrefix = prefix
	return b.schedules, nil
}

func (b *stubBackend) ListSchedulesPage(ctx context.Context, prefix string, limit int32, cursor string) ([]*schedulepb.Schedule, string, error) {
	b.schedulePrefix = prefix
	items, next := keysetPage(b.schedules, limit, cursor, func(schedule *schedulepb.Schedule) string { return schedule.GetScheduleId() })
	return items, next, nil
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

func (b *stubBackend) GetScheduleHistoryPage(ctx context.Context, scheduleId string, limit int32, cursor string) (*schedulepb.ScheduleHistory, string, error) {
	if b.history == nil {
		return &schedulepb.ScheduleHistory{}, "", nil
	}
	items, next := keysetPage(b.history.GetExecutions(), limit, cursor, func(execution *schedulepb.ScheduleHistory_Execution) string { return execution.GetMessageId() })
	return &schedulepb.ScheduleHistory{ScheduleId: b.history.GetScheduleId(), Executions: items}, next, nil
}

func (b *stubBackend) GetDLQMessages(ctx context.Context, dlqName string, limit int32) ([]*messagepb.Message, error) {
	b.dlqLimits = append(b.dlqLimits, limit)
	return nil, nil
}

func (b *stubBackend) GetDLQMessagesPage(ctx context.Context, dlqName string, limit int32, cursor string) ([]*messagepb.Message, string, error) {
	b.dlqLimits = append(b.dlqLimits, limit)
	items, next := keysetPage(b.messages, limit, cursor, func(message *messagepb.Message) string { return message.GetMessageId() })
	return items, next, nil
}

func keysetPage[T any](values []T, limit int32, cursor string, key func(T) string) ([]T, string) {
	start := 0
	if cursor != "" {
		start = len(values)
		for index, value := range values {
			if key(value) > cursor {
				start = index
				break
			}
		}
	}
	end := min(start+int(limit), len(values))
	items := values[start:end]
	if end == len(values) || len(items) == 0 {
		return items, ""
	}
	return items, key(items[len(items)-1])
}

func pageSlice[T any](values []T, limit int32, offset int64) []T {
	if offset >= int64(len(values)) {
		return nil
	}
	end := min(offset+int64(limit), int64(len(values)))
	return values[offset:end]
}

func (b *stubBackend) RetryDLQMessage(ctx context.Context, dlqName string, messageId string, targetQueueName string, resetRetries bool) error {
	return nil
}

func (b *stubBackend) DeleteDLQMessage(ctx context.Context, dlqName string, messageId string) error {
	return nil
}
func (b *stubBackend) PurgeDLQ(ctx context.Context, dlqName string) (int64, error) { return 0, nil }

func TestCreateSchedule_ValidatesCronExpression(t *testing.T) {
	backend := &stubBackend{}
	impl := &implementation{backend: backend}

	_, err := impl.CreateSchedule(context.Background(), &queueservicepb.CreateScheduleRequest{
		Schedule: &schedulepb.Schedule{
			ScheduleId: "invalid-cron",
			Metadata: &schedulepb.Schedule_Metadata{
				QueueName: "jobs",
				ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{
					CronSchedule: "* * * * * *",
				},
			},
		},
	})
	require.ErrorContains(t, err, "cron schedule is invalid")
	require.Empty(t, backend.schedules)

	_, err = impl.CreateSchedule(context.Background(), &queueservicepb.CreateScheduleRequest{
		Schedule: &schedulepb.Schedule{
			ScheduleId: "valid-cron",
			Metadata: &schedulepb.Schedule_Metadata{
				QueueName: "jobs",
				ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{
					CronSchedule: "*/5 * * * *",
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, backend.schedules, 1)
}

func TestCreateScheduleRequiresExistingTargetQueue(t *testing.T) {
	backend := &stubBackend{getQueueErr: domainerror.New(domainerror.NotFound, "queue not found", nil)}
	impl := &implementation{backend: backend}
	_, err := impl.CreateSchedule(context.Background(), &queueservicepb.CreateScheduleRequest{Schedule: &schedulepb.Schedule{
		ScheduleId: "missing-target",
		Metadata: &schedulepb.Schedule_Metadata{
			QueueName: "missing", ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{CronSchedule: "*/5 * * * *"},
		},
	}})
	require.Equal(t, codes.NotFound, status.Code(domainerror.ToGRPC(err)))
	require.Empty(t, backend.schedules)
}

func TestCreateScheduleRejectsServerManagedFields(t *testing.T) {
	backend := &stubBackend{}
	impl := &implementation{backend: backend}
	now := timestamppb.Now()

	_, err := impl.CreateSchedule(context.Background(), &queueservicepb.CreateScheduleRequest{Schedule: &schedulepb.Schedule{
		ScheduleId: "runtime-fields",
		Metadata: &schedulepb.Schedule_Metadata{
			QueueName:      "jobs",
			ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{CronSchedule: "*/5 * * * *"},
			State:          schedulepb.Schedule_Metadata_PAUSED,
			MessageIds:     []string{"fabricated"},
			NextRun:        now,
			LastRun:        now,
			CreatedAt:      now,
			UpdatedAt:      now,
			StateMessage:   "fabricated",
			NextRuns:       []*timestamppb.Timestamp{now},
		},
	}})
	require.Error(t, err)
	grpcStatus := status.Convert(domainerror.ToGRPC(err))
	require.Equal(t, codes.InvalidArgument, grpcStatus.Code())
	require.Empty(t, backend.schedules)
	require.Len(t, grpcStatus.Details(), 1)
	details, ok := grpcStatus.Details()[0].(*errdetails.BadRequest)
	require.True(t, ok)
	require.Len(t, details.GetFieldViolations(), 8)
	require.Equal(t, "schedule.metadata.state", details.GetFieldViolations()[0].GetField())
}

func TestCreateSchedulePersistsNoFutureCalendarScheduleAsPaused(t *testing.T) {
	backend := &stubBackend{}
	impl := &implementation{
		backend:        backend,
		calendarEngine: &stubEngine{nextRunErr: calendar.ErrNoExecutionTime.WithDetails("all execution times expired")},
	}

	_, err := impl.CreateSchedule(context.Background(), &queueservicepb.CreateScheduleRequest{Schedule: &schedulepb.Schedule{
		ScheduleId: "no-future-runs",
		Metadata: &schedulepb.Schedule_Metadata{
			QueueName: "jobs",
			ScheduleConfig: &schedulepb.Schedule_Metadata_CalendarSchedule{
				CalendarSchedule: &schedulepb.CalendarSchedule{},
			},
		},
	}})
	require.NoError(t, err)
	require.Len(t, backend.schedules, 1)
	metadata := backend.schedules[0].GetMetadata()
	require.Equal(t, schedulepb.Schedule_Metadata_PAUSED, metadata.GetState())
	require.Equal(t, "no future runs", metadata.GetStateMessage())
	require.Nil(t, metadata.GetNextRun())
}

func TestCreateScheduleReturnsUnexpectedNextRunError(t *testing.T) {
	backend := &stubBackend{}
	impl := &implementation{backend: backend, calendarEngine: &stubEngine{nextRunErr: errors.New("calculation failed")}}

	_, err := impl.CreateSchedule(context.Background(), &queueservicepb.CreateScheduleRequest{Schedule: &schedulepb.Schedule{
		ScheduleId: "calculation-error",
		Metadata: &schedulepb.Schedule_Metadata{
			QueueName: "jobs",
			ScheduleConfig: &schedulepb.Schedule_Metadata_CalendarSchedule{
				CalendarSchedule: &schedulepb.CalendarSchedule{},
			},
		},
	}})
	require.ErrorContains(t, err, "calculate next run")
	require.Empty(t, backend.schedules)
}

func TestCreateScheduleRejectsConflictingTimezoneFields(t *testing.T) {
	backend := &stubBackend{}
	impl := &implementation{backend: backend, calendarEngine: &stubEngine{}}

	_, err := impl.CreateSchedule(context.Background(), &queueservicepb.CreateScheduleRequest{Schedule: &schedulepb.Schedule{
		ScheduleId: "conflicting-timezones",
		Metadata: &schedulepb.Schedule_Metadata{
			QueueName: "jobs",
			Timezone:  "Europe/London",
			ScheduleConfig: &schedulepb.Schedule_Metadata_CalendarSchedule{CalendarSchedule: &schedulepb.CalendarSchedule{
				Timezone: "America/New_York",
			}},
		},
	}})
	require.Equal(t, codes.InvalidArgument, status.Code(domainerror.ToGRPC(err)))
	require.Empty(t, backend.schedules)
}

func TestCreateScheduleAllowsOmittedDeprecatedTimezone(t *testing.T) {
	backend := &stubBackend{}
	impl := &implementation{backend: backend, calendarEngine: &stubEngine{}}

	_, err := impl.CreateSchedule(context.Background(), &queueservicepb.CreateScheduleRequest{Schedule: &schedulepb.Schedule{
		ScheduleId: "canonical-timezone",
		Metadata: &schedulepb.Schedule_Metadata{
			QueueName: "jobs",
			ScheduleConfig: &schedulepb.Schedule_Metadata_CalendarSchedule{CalendarSchedule: &schedulepb.CalendarSchedule{
				Timezone: "America/New_York",
			}},
		},
	}})
	require.NoError(t, err)
	require.Len(t, backend.schedules, 1)
}

func TestMissingQueueStateAndDLQStatsReturnNotFound(t *testing.T) {
	backend := &stubBackend{getQueueErr: domainerror.New(domainerror.NotFound, "queue not found", nil)}
	impl := &implementation{backend: backend}
	_, err := impl.GetQueueState(context.Background(), &queueservicepb.GetQueueStateRequest{QueueName: "missing"})
	require.Equal(t, codes.NotFound, status.Code(domainerror.ToGRPC(err)))
	_, err = impl.GetDLQStats(context.Background(), "missing-dlq")
	require.Equal(t, codes.NotFound, status.Code(domainerror.ToGRPC(err)))
	_, err = impl.GetDLQMessages(context.Background(), "missing-dlq", 10)
	require.Equal(t, codes.NotFound, status.Code(domainerror.ToGRPC(err)))
	err = impl.PurgeDLQ(context.Background(), "missing-dlq")
	require.Equal(t, codes.NotFound, status.Code(domainerror.ToGRPC(err)))
	err = impl.RequeueFromDLQ(context.Background(), "missing-dlq", "message", "target", false)
	require.Equal(t, codes.NotFound, status.Code(domainerror.ToGRPC(err)))
	err = impl.DeleteFromDLQ(context.Background(), "missing-dlq", "message")
	require.Equal(t, codes.NotFound, status.Code(domainerror.ToGRPC(err)))
}

func TestRequeueFromDLQRequiresTargetQueue(t *testing.T) {
	backend := &stubBackend{}
	impl := &implementation{backend: backend}

	err := impl.RequeueFromDLQ(context.Background(), "orders-dlq", "message", "", false)

	require.Equal(t, codes.InvalidArgument, status.Code(domainerror.ToGRPC(err)))
	require.ErrorContains(t, err, "target queue name is required")
}

func TestCreateSchedule_ValidatesHeaders(t *testing.T) {
	backend := &stubBackend{}
	impl := &implementation{backend: backend}
	request := func(id string, headers ...*messagepb.Message_Metadata_Header) *queueservicepb.CreateScheduleRequest {
		return &queueservicepb.CreateScheduleRequest{Schedule: &schedulepb.Schedule{
			ScheduleId: id,
			Metadata: &schedulepb.Schedule_Metadata{
				QueueName:      "jobs",
				Headers:        headers,
				ScheduleConfig: &schedulepb.Schedule_Metadata_CronSchedule{CronSchedule: "*/5 * * * *"},
			},
		}}
	}

	_, err := impl.CreateSchedule(context.Background(), request("invalid", &messagepb.Message_Metadata_Header{Key: "Invalid_Key"}))
	require.ErrorContains(t, err, "metadata.headers[0].key")
	require.Empty(t, backend.schedules)

	_, err = impl.CreateSchedule(context.Background(), request("valid",
		&messagepb.Message_Metadata_Header{Key: "trace-id", Value: []byte{0x00, 0xff}},
		&messagepb.Message_Metadata_Header{Key: "trace-id", Value: []byte("second")},
	))
	require.NoError(t, err)
	require.Len(t, backend.schedules, 1)
}

func TestCreateQueue_RequiresExclusiveKey(t *testing.T) {
	backend := &stubBackend{}
	impl := &implementation{backend: backend}

	_, err := impl.CreateQueue(context.Background(), &queueservicepb.CreateQueueRequest{
		Name: "exclusive",
		Metadata: &queuepb.QueueMetadata{
			Type: queuepb.QueueType_EXCLUSIVE,
		},
	})
	require.Error(t, err)
	require.Empty(t, backend.createdQueues)

	_, err = impl.CreateQueue(context.Background(), &queueservicepb.CreateQueueRequest{
		Name: "exclusive",
		Metadata: &queuepb.QueueMetadata{
			Type:           queuepb.QueueType_EXCLUSIVE,
			ExclusivityKey: "orders",
		},
	})
	require.NoError(t, err)
	require.Len(t, backend.createdQueues, 1)
}

func TestCreateQueue_AutoCreatesComputedDLQName(t *testing.T) {
	request := &queueservicepb.CreateQueueRequest{
		Name: "orders",
		Metadata: &queuepb.QueueMetadata{
			AutoCreateDlq:       true,
			DeadLetterQueueName: "ignored-user-name",
		},
	}

	t.Run("success", func(t *testing.T) {
		backend := &stubBackend{}
		impl := &implementation{backend: backend}

		_, err := impl.CreateQueue(context.Background(), request)
		require.NoError(t, err)
		require.Len(t, backend.createdQueues, 2)
		require.Equal(t, "orders_dlq", backend.createdQueues[0].GetMetadata().GetDeadLetterQueueName())
		require.Equal(t, "orders_dlq", backend.createdQueues[1].GetName())
		require.Empty(t, backend.deletedQueues)
	})

	t.Run("atomic pair creation failure needs no compensation", func(t *testing.T) {
		createDLQErr := errors.New("create DLQ")
		backend := &stubBackend{createQueueErrs: []error{nil, createDLQErr}}
		impl := &implementation{backend: backend}

		_, err := impl.CreateQueue(context.Background(), request)
		require.ErrorIs(t, err, createDLQErr)
		require.Len(t, backend.createdQueues, 2)
		require.Equal(t, "orders_dlq", backend.createdQueues[0].GetMetadata().GetDeadLetterQueueName())
		require.Equal(t, "orders_dlq", backend.createdQueues[1].GetName())
		require.Empty(t, backend.deletedQueues)
	})
}

func TestCreateQueue_PreservesOptionalUserDLQName(t *testing.T) {
	backend := &stubBackend{queues: []*queuepb.Queue{{Name: "jobs", Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "jobs-dlq"}}}}
	impl := &implementation{backend: backend}

	_, err := impl.CreateQueue(context.Background(), &queueservicepb.CreateQueueRequest{
		Name:     "orders",
		Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "failed-orders"},
	})
	require.NoError(t, err)
	require.Len(t, backend.createdQueues, 1)
	require.Equal(t, "failed-orders", backend.createdQueues[0].GetMetadata().GetDeadLetterQueueName())
}

func TestCreateQueue_RequiresConfiguredDLQToExist(t *testing.T) {
	backend := &stubBackend{getQueueErr: domainerror.New(domainerror.NotFound, "queue not found", nil)}
	impl := &implementation{backend: backend}

	_, err := impl.CreateQueue(context.Background(), &queueservicepb.CreateQueueRequest{
		Name:     "orders",
		Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "missing-dlq"},
	})
	require.ErrorContains(t, err, "get configured dead letter queue")
	require.Empty(t, backend.createdQueues)
}

func TestRenewMessageLease_ReturnsBackendRemainingTime(t *testing.T) {
	backend := &stubBackend{extendRemaining: 1_250}
	impl := &implementation{backend: backend}
	attemptID := "attempt"
	workerID := "worker"

	response, err := impl.RenewMessageLease(context.Background(), &queueservicepb.RenewMessageLeaseRequest{
		QueueName:     "orders",
		MessageId:     "message",
		LeaseDuration: durationpb.New(45 * time.Second),
		AttemptId:     &attemptID,
		WorkerId:      &workerID,
	})
	require.NoError(t, err)
	require.Equal(t, 1250*time.Millisecond, response.GetRemainingTime().AsDuration())
	require.Equal(t, messagepb.Message_Metadata_RUNNING, response.GetState())
}

func TestGetQueueMessage_ForwardsExclusivityKey(t *testing.T) {
	backend := &stubBackend{}
	impl := &implementation{backend: backend}
	workerID := "worker"
	attemptID := "attempt"

	_, err := impl.GetQueueMessage(context.Background(), &queueservicepb.GetNextMessageRequest{
		QueueName:      "exclusive",
		WorkerId:       &workerID,
		AttemptId:      &attemptID,
		ExclusivityKey: "orders",
	})
	require.NoError(t, err)
	require.Equal(t, []claimCall{{queueName: "exclusive", workerId: workerID, attemptId: attemptID, exclusivityKey: "orders"}}, backend.claims)
}

func TestGetQueueMessage_LeaseDuration(t *testing.T) {
	tests := []struct {
		name          string
		leaseDuration *durationpb.Duration
		wantDuration  time.Duration
		wantError     string
	}{
		{name: "request override", leaseDuration: durationpb.New(45 * time.Second), wantDuration: 45 * time.Second},
		{name: "queue default", leaseDuration: nil, wantDuration: 0},
		{name: "zero duration", leaseDuration: durationpb.New(0), wantError: "greater than zero"},
		{name: "negative duration", leaseDuration: durationpb.New(-time.Second), wantError: "greater than zero"},
		{name: "invalid protobuf duration", leaseDuration: &durationpb.Duration{Seconds: 315576000001}, wantError: "invalid lease duration"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := &stubBackend{}
			impl := &implementation{backend: backend}
			_, err := impl.GetQueueMessage(context.Background(), &queueservicepb.GetNextMessageRequest{
				QueueName: "queue", LeaseDuration: tt.leaseDuration,
			})
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				require.Empty(t, backend.claims)
				return
			}
			require.NoError(t, err)
			require.Len(t, backend.claims, 1)
			require.Equal(t, tt.wantDuration, backend.claims[0].leaseDuration)
		})
	}
}

func TestPeekQueueMessages_PriorityRange(t *testing.T) {
	tests := []struct {
		name      string
		rangeReq  *queueservicepb.PeekQueueMessagesRequest_PriorityRange
		wantRange *repositorysql.PriorityRange
		wantError string
	}{
		{name: "filtered", rangeReq: &queueservicepb.PeekQueueMessagesRequest_PriorityRange{Min: 2, Max: 4}, wantRange: &repositorysql.PriorityRange{Min: 2, Max: 4}},
		{name: "full boundary", rangeReq: &queueservicepb.PeekQueueMessagesRequest_PriorityRange{Min: 0, Max: 4}, wantRange: &repositorysql.PriorityRange{Min: 0, Max: 4}},
		{name: "unfiltered"},
		{name: "minimum exceeds maximum", rangeReq: &queueservicepb.PeekQueueMessagesRequest_PriorityRange{Min: 3, Max: 2}, wantError: "minimum must not exceed maximum"},
		{name: "below minimum", rangeReq: &queueservicepb.PeekQueueMessagesRequest_PriorityRange{Min: -1, Max: 2}, wantError: "between 0 and 4"},
		{name: "above maximum", rangeReq: &queueservicepb.PeekQueueMessagesRequest_PriorityRange{Min: 2, Max: 5}, wantError: "between 0 and 4"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := &stubBackend{}
			impl := &implementation{backend: backend}
			_, err := impl.PeekQueueMessages(context.Background(), &queueservicepb.PeekQueueMessagesRequest{
				QueueName: "queue", PageSize: 10, PriorityRange: tt.rangeReq,
			})
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				require.Empty(t, backend.peekCalls)
				return
			}
			require.NoError(t, err)
			require.Len(t, backend.peekCalls, 1)
			require.Equal(t, tt.wantRange, backend.peekCalls[0].priorityRange)
		})
	}
}

func TestListQueues_FiltersByPrefix(t *testing.T) {
	backend := &stubBackend{queues: []*queuepb.Queue{{Name: "orders-eu"}, {Name: "orders-us"}}}
	impl := &implementation{backend: backend}

	filtered, err := impl.ListQueues(context.Background(), &queueservicepb.ListQueuesRequest{Prefix: "orders-"})
	require.NoError(t, err)
	require.Equal(t, "orders-", backend.queuePrefix)
	require.Equal(t, []string{"orders-eu", "orders-us"}, []string{filtered.Queues[0].GetName(), filtered.Queues[1].GetName()})

	backend.queues = []*queuepb.Queue{{Name: "orders-eu"}, {Name: "orders-us"}, {Name: "payments"}}
	all, err := impl.ListQueues(context.Background(), &queueservicepb.ListQueuesRequest{})
	require.NoError(t, err)
	require.Empty(t, backend.queuePrefix)
	require.Len(t, all.GetQueues(), 3)

	backend.queues = nil
	none, err := impl.ListQueues(context.Background(), &queueservicepb.ListQueuesRequest{Prefix: "missing"})
	require.NoError(t, err)
	require.Equal(t, "missing", backend.queuePrefix)
	require.Empty(t, none.GetQueues())
}

func TestListQueues_PaginatesAndValidatesTokens(t *testing.T) {
	backend := &stubBackend{queues: []*queuepb.Queue{{Name: "queue-a"}, {Name: "queue-b"}, {Name: "queue-c"}}}
	impl := &implementation{backend: backend}

	first, err := impl.ListQueues(context.Background(), &queueservicepb.ListQueuesRequest{Prefix: "queue-", PageSize: 2})
	require.NoError(t, err)
	require.Equal(t, []string{"queue-a", "queue-b"}, []string{first.Queues[0].GetName(), first.Queues[1].GetName()})
	require.NotEmpty(t, first.GetNextPageToken())

	second, err := impl.ListQueues(context.Background(), &queueservicepb.ListQueuesRequest{Prefix: "queue-", PageSize: 2, PageToken: first.GetNextPageToken()})
	require.NoError(t, err)
	require.Len(t, second.GetQueues(), 1)
	require.Equal(t, "queue-c", second.GetQueues()[0].GetName())
	require.Empty(t, second.GetNextPageToken())

	for name, request := range map[string]*queueservicepb.ListQueuesRequest{
		"malformed token":  {Prefix: "queue-", PageSize: 2, PageToken: "%%%"},
		"mismatched token": {Prefix: "other-", PageSize: 2, PageToken: first.GetNextPageToken()},
		"negative size":    {PageSize: -1},
		"oversized page":   {PageSize: 1001},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := impl.ListQueues(context.Background(), request)
			require.Equal(t, codes.InvalidArgument, status.Code(domainerror.ToGRPC(err)))
		})
	}
}

func TestPeekScheduleHistoryAndDLQPagination(t *testing.T) {
	messages := []*messagepb.Message{{MessageId: "message-a"}, {MessageId: "message-b"}, {MessageId: "message-c"}}
	executions := []*schedulepb.ScheduleHistory_Execution{{MessageId: "message-a"}, {MessageId: "message-b"}, {MessageId: "message-c"}}
	backend := &stubBackend{
		queues:   []*queuepb.Queue{{Name: "source", Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "source-dlq"}}},
		messages: messages,
		history:  &schedulepb.ScheduleHistory{ScheduleId: "schedule", Executions: executions},
	}
	impl := &implementation{backend: backend}
	ctx := context.Background()
	backend.schedules = []*schedulepb.Schedule{{ScheduleId: "schedule-a"}, {ScheduleId: "schedule-b"}, {ScheduleId: "schedule-c"}}
	schedulesFirst, err := impl.ListSchedules(ctx, &queueservicepb.ListSchedulesRequest{Prefix: "schedule-", PageSize: 2})
	require.NoError(t, err)
	require.Len(t, schedulesFirst.GetSchedules(), 2)
	require.NotEmpty(t, schedulesFirst.GetNextPageToken())
	schedulesLast, err := impl.ListSchedules(ctx, &queueservicepb.ListSchedulesRequest{Prefix: "schedule-", PageSize: 2, PageToken: schedulesFirst.GetNextPageToken()})
	require.NoError(t, err)
	require.Len(t, schedulesLast.GetSchedules(), 1)
	require.Equal(t, "schedule-c", schedulesLast.GetSchedules()[0].GetScheduleId())
	require.Empty(t, schedulesLast.GetNextPageToken())
	for name, request := range map[string]*queueservicepb.ListSchedulesRequest{
		"malformed token":  {Prefix: "schedule-", PageSize: 2, PageToken: "%%%"},
		"mismatched token": {Prefix: "other-", PageSize: 2, PageToken: schedulesFirst.GetNextPageToken()},
	} {
		t.Run("schedules "+name, func(t *testing.T) {
			_, err := impl.ListSchedules(ctx, request)
			require.Equal(t, codes.InvalidArgument, status.Code(domainerror.ToGRPC(err)))
		})
	}

	peekFirst, err := impl.PeekQueueMessages(ctx, &queueservicepb.PeekQueueMessagesRequest{QueueName: "queue", PageSize: 2})
	require.NoError(t, err)
	require.Len(t, peekFirst.GetMessages(), 2)
	require.NotEmpty(t, peekFirst.GetNextPageToken())
	peekLast, err := impl.PeekQueueMessages(ctx, &queueservicepb.PeekQueueMessagesRequest{QueueName: "queue", PageSize: 2, PageToken: peekFirst.GetNextPageToken()})
	require.NoError(t, err)
	require.Len(t, peekLast.GetMessages(), 1)
	require.Empty(t, peekLast.GetNextPageToken())
	_, err = impl.PeekQueueMessages(ctx, &queueservicepb.PeekQueueMessagesRequest{QueueName: "other", PageSize: 2, PageToken: peekFirst.GetNextPageToken()})
	require.Equal(t, codes.InvalidArgument, status.Code(domainerror.ToGRPC(err)))

	historyFirst, err := impl.GetScheduleHistory(ctx, &queueservicepb.GetScheduleHistoryRequest{ScheduleId: "schedule", PageSize: 2})
	require.NoError(t, err)
	require.Len(t, historyFirst.GetScheduleHistory().GetExecutions(), 2)
	require.NotEmpty(t, historyFirst.GetNextPageToken())
	historyLast, err := impl.GetScheduleHistory(ctx, &queueservicepb.GetScheduleHistoryRequest{ScheduleId: "schedule", PageSize: 2, PageToken: historyFirst.GetNextPageToken()})
	require.NoError(t, err)
	require.Len(t, historyLast.GetScheduleHistory().GetExecutions(), 1)
	require.Empty(t, historyLast.GetNextPageToken())
	for name, request := range map[string]*queueservicepb.GetScheduleHistoryRequest{
		"malformed token":  {ScheduleId: "schedule", PageSize: 2, PageToken: "%%%"},
		"mismatched token": {ScheduleId: "other-schedule", PageSize: 2, PageToken: historyFirst.GetNextPageToken()},
	} {
		t.Run("schedule history "+name, func(t *testing.T) {
			_, err := impl.GetScheduleHistory(ctx, request)
			require.Equal(t, codes.InvalidArgument, status.Code(domainerror.ToGRPC(err)))
		})
	}

	dlqFirst, err := impl.GetDLQMessagesPage(ctx, &queueservicepb.GetDLQMessagesRequest{DlqName: "source-dlq", PageSize: 2})
	require.NoError(t, err)
	require.Len(t, dlqFirst.GetMessages(), 2)
	require.NotEmpty(t, dlqFirst.GetNextPageToken())
	dlqLast, err := impl.GetDLQMessagesPage(ctx, &queueservicepb.GetDLQMessagesRequest{DlqName: "source-dlq", PageSize: 2, PageToken: dlqFirst.GetNextPageToken()})
	require.NoError(t, err)
	require.Len(t, dlqLast.GetMessages(), 1)
	require.Empty(t, dlqLast.GetNextPageToken())
	for name, request := range map[string]*queueservicepb.GetDLQMessagesRequest{
		"malformed token":  {DlqName: "source-dlq", PageSize: 2, PageToken: "%%%"},
		"mismatched token": {DlqName: "other-dlq", PageSize: 2, PageToken: dlqFirst.GetNextPageToken()},
	} {
		t.Run("DLQ "+name, func(t *testing.T) {
			_, err := impl.GetDLQMessagesPage(ctx, request)
			require.Equal(t, codes.InvalidArgument, status.Code(domainerror.ToGRPC(err)))
		})
	}
}

func TestListSchedules_FiltersByPrefix(t *testing.T) {
	backend := &stubBackend{schedules: []*schedulepb.Schedule{{ScheduleId: "billing-daily"}, {ScheduleId: "billing-monthly"}}}
	impl := &implementation{backend: backend}

	filtered, err := impl.ListSchedules(context.Background(), &queueservicepb.ListSchedulesRequest{Prefix: "billing-"})
	require.NoError(t, err)
	require.Equal(t, "billing-", backend.schedulePrefix)
	require.Equal(t, []string{"billing-daily", "billing-monthly"}, []string{filtered.Schedules[0].GetScheduleId(), filtered.Schedules[1].GetScheduleId()})

	backend.schedules = []*schedulepb.Schedule{{ScheduleId: "billing-daily"}, {ScheduleId: "billing-monthly"}, {ScheduleId: "cleanup"}}
	all, err := impl.ListSchedules(context.Background(), &queueservicepb.ListSchedulesRequest{})
	require.NoError(t, err)
	require.Empty(t, backend.schedulePrefix)
	require.Len(t, all.GetSchedules(), 3)

	backend.schedules = nil
	none, err := impl.ListSchedules(context.Background(), &queueservicepb.ListSchedulesRequest{Prefix: "missing"})
	require.NoError(t, err)
	require.Equal(t, "missing", backend.schedulePrefix)
	require.Empty(t, none.GetSchedules())
}

func TestCreateQueueMessage_ValidatorNil(t *testing.T) {
	backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 2}}
	impl := &implementation{backend: backend}

	req := &queueservicepb.PostMessageRequest{
		QueueName: "queue-A",
		Message:   &messagepb.Message{MessageId: "msg-1", Metadata: &messagepb.Message_Metadata{}},
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

func TestMessageSchedulingUsesRepositoryClock(t *testing.T) {
	fixedNow := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, tt := range []struct {
		name  string
		at    time.Time
		state messagepb.Message_Metadata_State
	}{
		{name: "past", at: fixedNow.Add(-time.Nanosecond), state: messagepb.Message_Metadata_PENDING},
		{name: "exact", at: fixedNow, state: messagepb.Message_Metadata_PENDING},
		{name: "future", at: fixedNow.Add(time.Nanosecond), state: messagepb.Message_Metadata_INVISIBLE},
	} {
		t.Run(tt.name, func(t *testing.T) {
			backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{}}
			impl := &implementation{backend: backend, clock: repositorysql.NewClockWithNow(func() time.Time { return fixedNow })}
			message := &messagepb.Message{MessageId: tt.name, Metadata: &messagepb.Message_Metadata{ScheduledTime: timestamppb.New(tt.at)}}

			_, err := impl.CreateQueueMessage(context.Background(), &queueservicepb.PostMessageRequest{QueueName: "queue", Message: message}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.state, backend.enqueued[0].message.GetMetadata().GetState())

			bulkMessage := &messagepb.Message{MessageId: tt.name + "-bulk", Metadata: &messagepb.Message_Metadata{ScheduledTime: timestamppb.New(tt.at)}}
			_, err = impl.CreateQueueMessagesBulk(context.Background(), &queueservicepb.PostMessagesBulkRequest{
				QueueName: "queue", Messages: []*messagepb.Message{bulkMessage}, TransactionMode: queueservicepb.PostMessagesBulkRequest_BEST_EFFORT,
			}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.state, bulkMessage.GetMetadata().GetState())
		})
	}
}

func TestCreateQueueMessage_InheritsUnlimitedRetriesThroughAdmissionValidator(t *testing.T) {
	payload, err := structpb.NewStruct(map[string]any{"task": "run"})
	require.NoError(t, err)
	queueMetadata := &queuepb.QueueMetadata{DefaultMaxAttempts: validator.InfiniteRetries}
	backend := &stubBackend{queueMetadata: queueMetadata}
	impl := &implementation{backend: backend}
	message := &messagepb.Message{
		MessageId: "unlimited-retries",
		Metadata: &messagepb.Message_Metadata{
			Payload: &commonpb.Payload{Data: payload, ContentType: "application/json"},
		},
	}

	_, err = impl.CreateQueueMessage(context.Background(), &queueservicepb.PostMessageRequest{
		QueueName: "queue-unlimited",
		Message:   message,
	}, validator.NewPayloadValidator(queueMetadata, nil))
	require.NoError(t, err)
	require.Equal(t, int32(validator.InfiniteRetries), backend.enqueued[0].message.GetMetadata().GetMaxAttempts())
	require.Equal(t, int32(validator.InfiniteRetries), backend.enqueued[0].message.GetMetadata().GetAttemptsLeft())
}

func TestCreateQueueMessage_RequiresMetadata(t *testing.T) {
	backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{}}
	impl := &implementation{backend: backend}

	_, err := impl.CreateQueueMessage(context.Background(), &queueservicepb.PostMessageRequest{
		QueueName: "queue-A",
		Message:   &messagepb.Message{MessageId: "msg-1"},
	}, nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(domainerror.ToGRPC(err)))
	require.Empty(t, backend.enqueued)
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
		Message:   &messagepb.Message{MessageId: "msg-2", Metadata: &messagepb.Message_Metadata{}},
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
		Message:   &messagepb.Message{MessageId: "msg-3", Metadata: &messagepb.Message_Metadata{}},
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
		{MessageId: "msg-1", Metadata: &messagepb.Message_Metadata{}},
		{MessageId: "msg-2", Metadata: &messagepb.Message_Metadata{}},
		{MessageId: "msg-3", Metadata: &messagepb.Message_Metadata{}},
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
		{MessageId: "msg-1", Metadata: &messagepb.Message_Metadata{}},
		{MessageId: "msg-2", Metadata: &messagepb.Message_Metadata{}},
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

func TestCreateQueueMessagesBulk_BestEffortRejectsMissingMetadata(t *testing.T) {
	backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{}}
	impl := &implementation{backend: backend}

	resp, err := impl.CreateQueueMessagesBulk(context.Background(), &queueservicepb.PostMessagesBulkRequest{
		QueueName: "test-queue",
		Messages: []*messagepb.Message{
			{MessageId: "missing"},
			{MessageId: "valid", Metadata: &messagepb.Message_Metadata{}},
		},
		TransactionMode: queueservicepb.PostMessagesBulkRequest_BEST_EFFORT,
	}, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, resp.GetSuccessfulCount())
	require.EqualValues(t, 1, resp.GetFailedCount())
	require.Equal(t, queueservicepb.PostMessagesBulkResponse_MessagePostResult_VALIDATION_FAILED, resp.GetResults()[0].GetErrorCode())
	require.Len(t, backend.enqueued, 1)
	require.Equal(t, "valid", backend.enqueued[0].message.GetMessageId())
}

func TestCreateQueueMessagesBulk_AllOrNothingReportsIndexedMetadata(t *testing.T) {
	backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{}}
	impl := &implementation{backend: backend}

	_, err := impl.CreateQueueMessagesBulk(context.Background(), &queueservicepb.PostMessagesBulkRequest{
		QueueName: "test-queue",
		Messages: []*messagepb.Message{
			{MessageId: "valid", Metadata: &messagepb.Message_Metadata{}},
			{MessageId: "missing"},
		},
		TransactionMode: queueservicepb.PostMessagesBulkRequest_ALL_OR_NOTHING,
	}, nil)
	require.Error(t, err)
	grpcStatus := status.Convert(domainerror.ToGRPC(err))
	require.Equal(t, codes.FailedPrecondition, grpcStatus.Code())
	require.Empty(t, backend.enqueued)
	require.Len(t, grpcStatus.Details(), 1)
	details, ok := grpcStatus.Details()[0].(*errdetails.BadRequest)
	require.True(t, ok)
	require.Equal(t, "messages[1].metadata", details.GetFieldViolations()[0].GetField())
}

func TestCreateQueueMessagesBulk_AllOrNothingRejectsNilMessageAsFailedPrecondition(t *testing.T) {
	backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{}}
	impl := &implementation{backend: backend}

	_, err := impl.CreateQueueMessagesBulk(context.Background(), &queueservicepb.PostMessagesBulkRequest{
		QueueName:       "test-queue",
		Messages:        []*messagepb.Message{nil},
		TransactionMode: queueservicepb.PostMessagesBulkRequest_ALL_OR_NOTHING,
	}, nil)
	require.Equal(t, codes.FailedPrecondition, status.Code(domainerror.ToGRPC(err)))
	require.Empty(t, backend.enqueued)
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
		{MessageId: "msg-1", Metadata: &messagepb.Message_Metadata{}},
		{MessageId: "msg-2", Metadata: &messagepb.Message_Metadata{}},
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
	require.Equal(t, codes.FailedPrecondition, status.Code(domainerror.ToGRPC(err)))

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
		{MessageId: "msg-1", Metadata: &messagepb.Message_Metadata{}},
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
		{MessageId: "msg-1", Metadata: &messagepb.Message_Metadata{}},
		{MessageId: "msg-2", Metadata: &messagepb.Message_Metadata{}},
		{MessageId: "msg-3", Metadata: &messagepb.Message_Metadata{}},
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
		{MessageId: "msg-1", Metadata: &messagepb.Message_Metadata{}},
		{MessageId: "msg-2", Metadata: &messagepb.Message_Metadata{}},
		{MessageId: "msg-3", Metadata: &messagepb.Message_Metadata{}},
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

	require.Equal(t, "internal server error", resp.Results[1].Error)
	require.NotContains(t, resp.Results[1].Error, "database connection lost")
}

func TestCreateQueueMessagesBulk_ReportsSchemaMismatch(t *testing.T) {
	backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{}}
	impl := &implementation{backend: backend}
	validation := &stubValidator{result: &validator.ValidationResult{
		Valid:          false,
		SchemaMismatch: true,
		Errors: []*validator.ValidationError{{
			Field: "payload.name", Message: "required field missing",
		}},
	}}

	response, err := impl.CreateQueueMessagesBulk(context.Background(), &queueservicepb.PostMessagesBulkRequest{
		QueueName:       "test-queue",
		Messages:        []*messagepb.Message{{MessageId: "schema-invalid", Metadata: &messagepb.Message_Metadata{}}},
		TransactionMode: queueservicepb.PostMessagesBulkRequest_BEST_EFFORT,
	}, validation)
	require.NoError(t, err)
	require.Equal(t, queueservicepb.PostMessagesBulkResponse_MessagePostResult_SCHEMA_MISMATCH, response.GetResults()[0].GetErrorCode())
}

func TestGetDLQMessagesNormalizesAndValidatesLimit(t *testing.T) {
	backend := &stubBackend{queues: []*queuepb.Queue{{Name: "jobs", Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "jobs-dlq"}}}}
	impl := &implementation{backend: backend}

	_, err := impl.GetDLQMessages(context.Background(), "jobs-dlq", 0)
	require.NoError(t, err)
	require.Equal(t, []int32{100}, backend.dlqLimits)

	_, err = impl.GetDLQMessages(context.Background(), "jobs-dlq", 1000)
	require.NoError(t, err)
	require.Equal(t, []int32{100, 1000}, backend.dlqLimits)

	for _, limit := range []int32{-1, 1001} {
		_, err = impl.GetDLQMessages(context.Background(), "jobs-dlq", limit)
		require.Equal(t, codes.InvalidArgument, status.Code(domainerror.ToGRPC(err)))
	}
	require.Equal(t, []int32{100, 1000}, backend.dlqLimits)
}

func TestDLQOperationsRejectOrdinaryQueue(t *testing.T) {
	impl := &implementation{backend: &stubBackend{queues: []*queuepb.Queue{{Name: "ordinary", Metadata: &queuepb.QueueMetadata{}}}}}
	operations := map[string]func() error{
		"get": func() error { _, err := impl.GetDLQMessages(context.Background(), "ordinary", 10); return err },
		"requeue": func() error {
			return impl.RequeueFromDLQ(context.Background(), "ordinary", "message", "target", true)
		},
		"delete": func() error { return impl.DeleteFromDLQ(context.Background(), "ordinary", "message") },
		"purge":  func() error { return impl.PurgeDLQ(context.Background(), "ordinary") },
		"stats":  func() error { _, err := impl.GetDLQStats(context.Background(), "ordinary"); return err },
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, codes.FailedPrecondition, status.Code(domainerror.ToGRPC(operation())))
		})
	}
}

func TestDLQRejectsProducerAndConsumerOperations(t *testing.T) {
	backend := &stubBackend{
		queueMetadata: &queuepb.QueueMetadata{},
		queues:        []*queuepb.Queue{{Name: "source", Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "source-dlq"}}},
	}
	impl := &implementation{backend: backend}
	_, postErr := impl.CreateQueueMessage(context.Background(), &queueservicepb.PostMessageRequest{
		QueueName: "source-dlq", Message: &messagepb.Message{MessageId: "message", Metadata: &messagepb.Message_Metadata{}},
	}, nil)
	_, bulkErr := impl.CreateQueueMessagesBulk(context.Background(), &queueservicepb.PostMessagesBulkRequest{
		QueueName: "source-dlq", Messages: []*messagepb.Message{{MessageId: "message", Metadata: &messagepb.Message_Metadata{}}},
	}, nil)
	_, claimErr := impl.GetQueueMessage(context.Background(), &queueservicepb.GetNextMessageRequest{QueueName: "source-dlq"})
	for _, err := range []error{postErr, bulkErr, claimErr} {
		require.Equal(t, codes.FailedPrecondition, status.Code(domainerror.ToGRPC(err)))
	}
	require.Empty(t, backend.enqueued)
	require.Empty(t, backend.claims)
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
		{MessageId: "msg-1", Metadata: &messagepb.Message_Metadata{}},
		{MessageId: "msg-2", Metadata: &messagepb.Message_Metadata{}},
		{MessageId: "msg-3", Metadata: &messagepb.Message_Metadata{}},
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
		{MessageId: "msg-1", Metadata: &messagepb.Message_Metadata{}},
		{MessageId: "msg-2", Metadata: &messagepb.Message_Metadata{}},
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
		{MessageId: "msg-1", Metadata: &messagepb.Message_Metadata{}},
		{MessageId: "msg-2", Metadata: &messagepb.Message_Metadata{}},
	}

	req := &queueservicepb.PostMessagesBulkRequest{
		QueueName:       "test-queue",
		Messages:        messages,
		TransactionMode: queueservicepb.PostMessagesBulkRequest_BEST_EFFORT,
	}

	resp, err := impl.CreateQueueMessagesBulk(context.Background(), req, nil)
	require.NoError(t, err)

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

func TestCreateQueueMessagesBulk_BestEffort_UnknownOutcomesAreInternalErrors(t *testing.T) {
	backend := &stubBackend{
		queueMetadata:    &queuepb.QueueMetadata{DefaultMaxAttempts: 3},
		enqueueErrorsNil: true,
		enqueueTxErr:     errors.New("backend outcome unavailable"),
	}
	impl := &implementation{backend: backend}
	response, err := impl.CreateQueueMessagesBulk(context.Background(), &queueservicepb.PostMessagesBulkRequest{
		QueueName: "test-queue",
		Messages: []*messagepb.Message{
			{MessageId: "msg-1", Metadata: &messagepb.Message_Metadata{}},
			{MessageId: "msg-2", Metadata: &messagepb.Message_Metadata{}},
		},
		TransactionMode: queueservicepb.PostMessagesBulkRequest_BEST_EFFORT,
	}, nil)

	require.NoError(t, err)
	require.False(t, response.GetSuccess())
	require.Zero(t, response.GetSuccessfulCount())
	require.EqualValues(t, 2, response.GetFailedCount())
	require.Len(t, response.GetResults(), 2)
	for _, result := range response.GetResults() {
		require.False(t, result.GetSuccess())
		require.Equal(t, queueservicepb.PostMessagesBulkResponse_MessagePostResult_INTERNAL_ERROR, result.GetErrorCode())
		require.Equal(t, "internal server error", result.GetError())
	}
}

func TestCreateQueueMessagesBulk_NilMessageInSlice_BestEffort(t *testing.T) {
	backend := &stubBackend{queueMetadata: &queuepb.QueueMetadata{DefaultMaxAttempts: 3}}
	impl := &implementation{backend: backend}

	messages := []*messagepb.Message{
		{MessageId: "msg-1", Metadata: &messagepb.Message_Metadata{}},
		nil, // nil message should be treated as a validation error
		{MessageId: "msg-3", Metadata: &messagepb.Message_Metadata{}},
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
