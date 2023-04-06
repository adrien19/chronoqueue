package chronoqueue

import (
	"context"
	"os"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue/repository"
	"github.com/go-kit/log"
)

type chronoqueueService struct {
	storage repository.Storage
}

func NewChronoqueueService(storage repository.Storage) Service {
	return &chronoqueueService{storage: storage}
}

func (cs *chronoqueueService) CreateQueue(ctx context.Context, queueInfo *chronoqueue.Queue) error {
	return cs.storage.CreateQueue(ctx, queueInfo)
}

func (cs *chronoqueueService) DeleteQueue(ctx context.Context, queueName string) error {
	return cs.storage.DeleteQueue(ctx, queueName)
}

func (cs *chronoqueueService) PostMessage(ctx context.Context, queueName string, message *chronoqueue.Message) error {
	return cs.storage.CreateQueueMessage(ctx, queueName, message)
}

func (cs *chronoqueueService) GetNextMessage(ctx context.Context, queueName string, leaseDuration int64) (*chronoqueue.Message, error) {
	return cs.storage.GetQueueMessage(ctx, queueName, leaseDuration)
}

func (cs *chronoqueueService) AcknowledgeMessage(ctx context.Context, queueName string, messageID string, state internal.State) error {
	return cs.storage.AcknowledgeMessage(ctx, queueName, messageID, state)
}
func (cs *chronoqueueService) RenewMessageLease(ctx context.Context, queueName string, leaseDuration int64, messageID string) error {
	return cs.storage.RenewMessageLease(ctx, queueName, leaseDuration, messageID)
}
func (cs *chronoqueueService) PeekQueueMessages(ctx context.Context, requestData *chronoqueue.PeekQueueMessagesRequest) (*chronoqueue.PeekQueueMessagesResponse, error) {
	return cs.storage.PeekQueueMessages(ctx, requestData)
}
func (cs *chronoqueueService) GetQueueState(ctx context.Context, request *chronoqueue.GetQueueStateRequest) (*chronoqueue.GetQueueStateResponse, error) {
	return cs.storage.GetQueueState(ctx, request)
}

var logger log.Logger

func init() {
	logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
}
