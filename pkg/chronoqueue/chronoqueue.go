package chronoqueue

import (
	"context"
	"os"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue/repository"
	"github.com/go-kit/log"
)

type chronoqueueService struct {
	storage repository.Storage
}

func NewChronoqueueService(storage repository.Storage) Service {
	return &chronoqueueService{storage: storage}
}

func (cs *chronoqueueService) CreateQueue(ctx context.Context, request *chronoqueue.CreateQueueRequest) (*chronoqueue.CreateQueueResponse, error) {
	return cs.storage.CreateQueue(ctx, request)
}

func (cs *chronoqueueService) DeleteQueue(ctx context.Context, request *chronoqueue.DeleteQueueRequest) (*chronoqueue.DeleteQueueResponse, error) {
	return cs.storage.DeleteQueue(ctx, request)
}

func (cs *chronoqueueService) PostMessage(ctx context.Context, request *chronoqueue.PostMessageRequest) (*chronoqueue.PostMessageResponse, error) {
	return cs.storage.CreateQueueMessage(ctx, request)
}

func (cs *chronoqueueService) GetNextMessage(ctx context.Context, request *chronoqueue.GetNextMessageRequest) (*chronoqueue.GetNextMessageResponse, error) {
	return cs.storage.GetQueueMessage(ctx, request)
}

func (cs *chronoqueueService) AcknowledgeMessage(ctx context.Context, request *chronoqueue.AcknowledgeMessageRequest) (*chronoqueue.AcknowledgeMessageResponse, error) {
	return cs.storage.AcknowledgeMessage(ctx, request)
}
func (cs *chronoqueueService) RenewMessageLease(ctx context.Context, request *chronoqueue.RenewMessageLeaseRequest) (*chronoqueue.RenewMessageLeaseResponse, error) {
	return cs.storage.RenewMessageLease(ctx, request)
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
