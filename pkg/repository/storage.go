package repository

import (
	"context"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservicepb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/pkg/validator"
)

// DLQStats represents statistics about a Dead Letter Queue
type DLQStats struct {
	Name         string `json:"name"`
	MessageCount int64  `json:"message_count"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

// Storage defines the interface for ChronoQueue persistence layer.
// All storage backends (SQLite, Postgres, Redis) must implement this interface.
type Storage interface {
	// Queue Operations
	CreateQueue(ctx context.Context, request *queueservicepb.CreateQueueRequest) (*queueservicepb.CreateQueueResponse, error)
	GetQueueMetadata(ctx context.Context, queueName string) (*queuepb.QueueMetadata, error)
	DeleteQueue(ctx context.Context, request *queueservicepb.DeleteQueueRequest) (*queueservicepb.DeleteQueueResponse, error)
	ListQueues(ctx context.Context, request *queueservicepb.ListQueuesRequest) (*queueservicepb.ListQueuesResponse, error)
	GetQueueState(ctx context.Context, request *queueservicepb.GetQueueStateRequest) (*queueservicepb.GetQueueStateResponse, error)

	// Message Lifecycle
	CreateQueueMessage(ctx context.Context, request *queueservicepb.PostMessageRequest, validator validator.Validator) (*queueservicepb.PostMessageResponse, error)
	GetQueueMessage(ctx context.Context, request *queueservicepb.GetNextMessageRequest) (*queueservicepb.GetNextMessageResponse, error)
	AcknowledgeMessage(ctx context.Context, request *queueservicepb.AcknowledgeMessageRequest) (*queueservicepb.AcknowledgeMessageResponse, error)
	CancelMessage(ctx context.Context, request *queueservicepb.CancelMessageRequest) (*queueservicepb.CancelMessageResponse, error)
	SendMessageHeartBeat(ctx context.Context, request *queueservicepb.SendMessageHeartBeatRequest) (*queueservicepb.SendMessageHeartBeatResponse, error)
	RenewMessageLease(ctx context.Context, request *queueservicepb.RenewMessageLeaseRequest) (*queueservicepb.RenewMessageLeaseResponse, error)
	PeekQueueMessages(ctx context.Context, request *queueservicepb.PeekQueueMessagesRequest) (*queueservicepb.PeekQueueMessagesResponse, error)

	// Schedule Operations
	CreateSchedule(ctx context.Context, request *queueservicepb.CreateScheduleRequest) (*queueservicepb.CreateScheduleResponse, error)
	DeleteSchedule(ctx context.Context, request *queueservicepb.DeleteScheduleRequest) (*queueservicepb.DeleteScheduleResponse, error)
	GetSchedule(ctx context.Context, request *queueservicepb.GetScheduleRequest) (*queueservicepb.GetScheduleResponse, error)
	ListSchedules(ctx context.Context, request *queueservicepb.ListSchedulesRequest) (*queueservicepb.ListSchedulesResponse, error)
	GetScheduleHistory(ctx context.Context, request *queueservicepb.GetScheduleHistoryRequest) (*queueservicepb.GetScheduleHistoryResponse, error)
	PauseSchedule(ctx context.Context, request *queueservicepb.PauseScheduleRequest) (*queueservicepb.PauseScheduleResponse, error)
	ResumeSchedule(ctx context.Context, request *queueservicepb.ResumeScheduleRequest) (*queueservicepb.ResumeScheduleResponse, error)

	// Calendar Schedule Operations
	ValidateCalendarSchedule(ctx context.Context, calendarSchedule *schedulepb.CalendarSchedule) error
	GetCalendarSchedulePreview(ctx context.Context, calendarSchedule *schedulepb.CalendarSchedule, count int) (*queueservicepb.PreviewCalendarScheduleResponse, error)

	// DLQ Management
	GetDLQMessages(ctx context.Context, dlqName string, limit int32) ([]*messagepb.Message, error)
	RequeueFromDLQ(ctx context.Context, dlqName string, messageID string, targetQueueName string, resetRetries bool) error
	DeleteFromDLQ(ctx context.Context, dlqName string, messageID string) error
	PurgeDLQ(ctx context.Context, dlqName string) error
	GetDLQStats(ctx context.Context, dlqName string) (*DLQStats, error)
}
