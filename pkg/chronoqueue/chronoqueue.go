package chronoqueue

import (
	"context"
	"errors"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue/repository"
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
	// Create an adapter around GetQueueMessage
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		// Type assert the request to its specific type
		specificReq, ok := req.(*chronoqueue.GetNextMessageRequest)
		if !ok {
			return nil, errors.New("invalid request type")
		}

		// Call the specific function
		return cs.storage.GetQueueMessage(ctx, specificReq)
	}
	// Wrap the handler with ErrorHandler
	wrappedHandler := util.ErrorHandler(adapterFunc, &chronoqueue.GetNextMessageResponse{})
	// return cs.storage.GetQueueMessage(ctx, request)
	// Now use the wrapped function
	resp, err := wrappedHandler(ctx, request)
	return resp.(*chronoqueue.GetNextMessageResponse), err
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
