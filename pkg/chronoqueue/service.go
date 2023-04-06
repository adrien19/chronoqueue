package chronoqueue

import (
	"context"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal"
)

type Service interface {
	CreateQueue(ctx context.Context, queueInfo *chronoqueue.Queue) error
	DeleteQueue(ctx context.Context, queueName string) error
	PostMessage(ctx context.Context, queueName string, message internal.QueueMessageInfo) error
	GetNextMessage(ctx context.Context, queueName string, leaseDuration int64) (internal.QueueMessageInfo, error)
	AcknowledgeMessage(ctx context.Context, queueName string, messageID string, state internal.State) error
	RenewMessageLease(ctx context.Context, queueName string, leaseDuration int64, messageID string) error
	PeekQueueMessages(ctx context.Context, queueName string, limit int64, priorityRange internal.PriorityRange) ([]internal.QueueMessageInfo, error)
	GetQueueState(ctx context.Context, queueName string) (internal.QueueStateInfo, error)
}
