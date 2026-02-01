package repository

import (
	"context"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
)

// BackendStorage defines the low-level interface that storage backends must implement.
// This interface uses simple method signatures (unlike the high-level Storage interface
// which matches gRPC service signatures). Both sqlite.Storage and postgres.Storage
// implement this interface.
type BackendStorage interface {
	// Lifecycle
	Close() error

	// Queue Operations
	CreateQueue(ctx context.Context, queue *queuepb.Queue) error
	GetQueue(ctx context.Context, name string) (*queuepb.Queue, error)
	GetQueueMetadata(ctx context.Context, name string) (*queuepb.QueueMetadata, error)
	ListQueues(ctx context.Context) ([]*queuepb.Queue, error)
	DeleteQueue(ctx context.Context, name string) error

	// Message Operations
	EnqueueMessage(ctx context.Context, queueName string, message *messagepb.Message) error
	ClaimMessage(ctx context.Context, queueName string, workerId string, attemptId string) (*messagepb.Message, error)
	AcknowledgeMessage(ctx context.Context, queueName string, messageId string, attemptId string) error
	CancelMessage(ctx context.Context, queueName string, messageId string, reason string) error
	NackMessage(ctx context.Context, queueName string, messageId string, attemptId string) error
	HeartbeatMessage(ctx context.Context, queueName string, messageId string, attemptId string) (messagepb.Message_Metadata_State, int64, error)
	ExtendMessageLease(ctx context.Context, queueName string, messageId string, attemptId string, extensionMs int64) error
	PeekMessages(ctx context.Context, queueName string, limit int32) ([]*messagepb.Message, error)

	// Schedule Operations
	CreateSchedule(ctx context.Context, schedule *schedulepb.Schedule) error
	GetSchedule(ctx context.Context, scheduleId string) (*schedulepb.Schedule, error)
	ListSchedules(ctx context.Context, queueName string) ([]*schedulepb.Schedule, error)
	DeleteSchedule(ctx context.Context, scheduleId string) error
	PauseSchedule(ctx context.Context, scheduleId string) error
	ResumeSchedule(ctx context.Context, scheduleId string) error
	RecordScheduleExecution(ctx context.Context, scheduleId string, messageId string, executionTime int64) error
	GetScheduleHistory(ctx context.Context, scheduleId string, limit int64) (*schedulepb.ScheduleHistory, error)

	// DLQ Operations
	GetDLQMessages(ctx context.Context, queueName string, limit int32) ([]*messagepb.Message, error)
	RetryDLQMessage(ctx context.Context, queueName string, messageId string) error
	DeleteDLQMessage(ctx context.Context, queueName string, messageId string) error
	PurgeDLQ(ctx context.Context, queueName string) (int64, error)
}
