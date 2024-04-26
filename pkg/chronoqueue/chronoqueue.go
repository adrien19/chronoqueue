package chronoqueue

import (
	"context"
	"errors"

	"github.com/adrien19/chronoqueue/api-deplicated/chronoqueue/v1"
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
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		specificReq, ok := req.(*chronoqueue.CreateQueueRequest)
		if !ok {
			return &chronoqueue.CreateQueueResponse{Success: false}, errors.New("invalid request type for creating a queue")
		}
		return cs.storage.CreateQueue(ctx, specificReq)
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &chronoqueue.CreateQueueResponse{Success: false})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*chronoqueue.CreateQueueResponse), err
}

func (cs *chronoqueueService) DeleteQueue(ctx context.Context, request *chronoqueue.DeleteQueueRequest) (*chronoqueue.DeleteQueueResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		specificReq, ok := req.(*chronoqueue.DeleteQueueRequest)
		if !ok {
			return &chronoqueue.DeleteQueueResponse{Success: false}, errors.New("invalid request type for removing a queue")
		}
		return cs.storage.DeleteQueue(ctx, specificReq)
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &chronoqueue.DeleteQueueResponse{Success: false})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*chronoqueue.DeleteQueueResponse), err
}

func (cs *chronoqueueService) PostMessage(ctx context.Context, request *chronoqueue.PostMessageRequest) (*chronoqueue.PostMessageResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		specificReq, ok := req.(*chronoqueue.PostMessageRequest)
		if !ok {
			return &chronoqueue.PostMessageResponse{Success: false}, errors.New("invalid request type for peeking queue's messages")
		}
		return cs.storage.CreateQueueMessage(ctx, specificReq)
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &chronoqueue.PostMessageResponse{Success: false})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*chronoqueue.PostMessageResponse), err
}

func (cs *chronoqueueService) GetNextMessage(ctx context.Context, request *chronoqueue.GetNextMessageRequest) (*chronoqueue.GetNextMessageResponse, error) {
	// Create an adapter around GetQueueMessage
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		// Type assert the request to its specific type
		specificReq, ok := req.(*chronoqueue.GetNextMessageRequest)
		if !ok {
			return nil, errors.New("invalid request type for getting next message")
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
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		specificReq, ok := req.(*chronoqueue.AcknowledgeMessageRequest)
		if !ok {
			return &chronoqueue.AcknowledgeMessageResponse{Success: false}, errors.New("invalid request type for acknowledging message request")
		}
		return cs.storage.AcknowledgeMessage(ctx, specificReq)
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &chronoqueue.AcknowledgeMessageResponse{Success: false})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*chronoqueue.AcknowledgeMessageResponse), err
}
func (cs *chronoqueueService) RenewMessageLease(ctx context.Context, request *chronoqueue.RenewMessageLeaseRequest) (*chronoqueue.RenewMessageLeaseResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		specificReq, ok := req.(*chronoqueue.RenewMessageLeaseRequest)
		if !ok {
			return nil, errors.New("invalid request type for renewing message lease")
		}
		return cs.storage.RenewMessageLease(ctx, specificReq)
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &chronoqueue.RenewMessageLeaseResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*chronoqueue.RenewMessageLeaseResponse), err
}
func (cs *chronoqueueService) PeekQueueMessages(ctx context.Context, request *chronoqueue.PeekQueueMessagesRequest) (*chronoqueue.PeekQueueMessagesResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		specificReq, ok := req.(*chronoqueue.PeekQueueMessagesRequest)
		if !ok {
			return nil, errors.New("invalid request type for peeking queue's messages")
		}
		return cs.storage.PeekQueueMessages(ctx, specificReq)
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &chronoqueue.PeekQueueMessagesResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*chronoqueue.PeekQueueMessagesResponse), err
}
func (cs *chronoqueueService) GetQueueState(ctx context.Context, request *chronoqueue.GetQueueStateRequest) (*chronoqueue.GetQueueStateResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		specificReq, ok := req.(*chronoqueue.GetQueueStateRequest)
		if !ok {
			return nil, errors.New("invalid request type for peeking queue's messages")
		}
		return cs.storage.GetQueueState(ctx, specificReq)
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &chronoqueue.GetQueueStateResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*chronoqueue.GetQueueStateResponse), err
}

func (cs *chronoqueueService) SendMessageHeartBeat(ctx context.Context, request *chronoqueue.SendMessageHeartBeatRequest) (*chronoqueue.SendMessageHeartBeatResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		specificReq, ok := req.(*chronoqueue.SendMessageHeartBeatRequest)
		if !ok {
			return nil, errors.New("invalid request type for sending message's heartbeat")
		}
		return cs.storage.SendMessageHeartBeat(ctx, specificReq)
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &chronoqueue.SendMessageHeartBeatResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*chronoqueue.SendMessageHeartBeatResponse), err
}

func (cs *chronoqueueService) ListQueues(ctx context.Context, request *chronoqueue.ListQueuesRequest) (*chronoqueue.ListQueuesResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		return cs.storage.ListQueues(ctx, req.(*chronoqueue.ListQueuesRequest))
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &chronoqueue.ListQueuesResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*chronoqueue.ListQueuesResponse), err
}

func (cs *chronoqueueService) CreateSchedule(ctx context.Context, request *chronoqueue.CreateScheduleRequest) (*chronoqueue.CreateScheduleResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		return cs.storage.CreateSchedule(ctx, req.(*chronoqueue.CreateScheduleRequest))
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &chronoqueue.CreateScheduleResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*chronoqueue.CreateScheduleResponse), err
}

func (cs *chronoqueueService) DeleteSchedule(ctx context.Context, request *chronoqueue.DeleteScheduleRequest) (*chronoqueue.DeleteScheduleResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		return cs.storage.DeleteSchedule(ctx, req.(*chronoqueue.DeleteScheduleRequest))
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &chronoqueue.DeleteScheduleResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*chronoqueue.DeleteScheduleResponse), err
}

func (cs *chronoqueueService) GetSchedule(ctx context.Context, request *chronoqueue.GetScheduleRequest) (*chronoqueue.GetScheduleResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		return cs.storage.GetSchedule(ctx, req.(*chronoqueue.GetScheduleRequest))
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &chronoqueue.GetScheduleResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*chronoqueue.GetScheduleResponse), err
}

func (cs *chronoqueueService) ListSchedules(ctx context.Context, request *chronoqueue.ListSchedulesRequest) (*chronoqueue.ListSchedulesResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		return cs.storage.ListSchedules(ctx, req.(*chronoqueue.ListSchedulesRequest))
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &chronoqueue.ListSchedulesResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*chronoqueue.ListSchedulesResponse), err
}

func (cs *chronoqueueService) GetScheduleHistory(ctx context.Context, request *chronoqueue.GetScheduleHistoryRequest) (*chronoqueue.GetScheduleHistoryResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		return cs.storage.GetScheduleHistory(ctx, req.(*chronoqueue.GetScheduleHistoryRequest))
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &chronoqueue.GetScheduleHistoryResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*chronoqueue.GetScheduleHistoryResponse), err
}

func (cs *chronoqueueService) PauseSchedule(ctx context.Context, request *chronoqueue.PauseScheduleRequest) (*chronoqueue.PauseScheduleResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		return cs.storage.PauseSchedule(ctx, req.(*chronoqueue.PauseScheduleRequest))
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &chronoqueue.PauseScheduleResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*chronoqueue.PauseScheduleResponse), err
}

func (cs *chronoqueueService) ResumeSchedule(ctx context.Context, request *chronoqueue.ResumeScheduleRequest) (*chronoqueue.ResumeScheduleResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		return cs.storage.ResumeSchedule(ctx, req.(*chronoqueue.ResumeScheduleRequest))
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &chronoqueue.ResumeScheduleResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*chronoqueue.ResumeScheduleResponse), err
}
