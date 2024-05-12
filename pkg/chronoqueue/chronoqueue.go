package chronoqueue

import (
	"context"
	"errors"

	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue/repository"
)

type chronoqueueService struct {
	storage repository.Storage
}

func NewChronoqueueService(storage repository.Storage) Service {
	return &chronoqueueService{storage: storage}
}

func (cs *chronoqueueService) CreateQueue(ctx context.Context, request *queueservice_pb.CreateQueueRequest) (*queueservice_pb.CreateQueueResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		specificReq, ok := req.(*queueservice_pb.CreateQueueRequest)
		if !ok {
			return &queueservice_pb.CreateQueueResponse{Success: false}, errors.New("invalid request type for creating a queue")
		}
		return cs.storage.CreateQueue(ctx, specificReq)
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &queueservice_pb.CreateQueueResponse{Success: false})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*queueservice_pb.CreateQueueResponse), err
}

func (cs *chronoqueueService) DeleteQueue(ctx context.Context, request *queueservice_pb.DeleteQueueRequest) (*queueservice_pb.DeleteQueueResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		specificReq, ok := req.(*queueservice_pb.DeleteQueueRequest)
		if !ok {
			return &queueservice_pb.DeleteQueueResponse{Success: false}, errors.New("invalid request type for removing a queue")
		}
		return cs.storage.DeleteQueue(ctx, specificReq)
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &queueservice_pb.DeleteQueueResponse{Success: false})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*queueservice_pb.DeleteQueueResponse), err
}

func (cs *chronoqueueService) PostMessage(ctx context.Context, request *queueservice_pb.PostMessageRequest) (*queueservice_pb.PostMessageResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		specificReq, ok := req.(*queueservice_pb.PostMessageRequest)
		if !ok {
			return &queueservice_pb.PostMessageResponse{Success: false}, errors.New("invalid request type for peeking queue's messages")
		}
		return cs.storage.CreateQueueMessage(ctx, specificReq)
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &queueservice_pb.PostMessageResponse{Success: false})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*queueservice_pb.PostMessageResponse), err
}

func (cs *chronoqueueService) GetNextMessage(ctx context.Context, request *queueservice_pb.GetNextMessageRequest) (*queueservice_pb.GetNextMessageResponse, error) {
	// Create an adapter around GetQueueMessage
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		// Type assert the request to its specific type
		specificReq, ok := req.(*queueservice_pb.GetNextMessageRequest)
		if !ok {
			return nil, errors.New("invalid request type for getting next message")
		}

		// Call the specific function
		return cs.storage.GetQueueMessage(ctx, specificReq)
	}
	// Wrap the handler with ErrorHandler
	wrappedHandler := util.ErrorHandler(adapterFunc, &queueservice_pb.GetNextMessageResponse{})
	// return cs.storage.GetQueueMessage(ctx, request)
	// Now use the wrapped function
	resp, err := wrappedHandler(ctx, request)
	return resp.(*queueservice_pb.GetNextMessageResponse), err
}

func (cs *chronoqueueService) AcknowledgeMessage(ctx context.Context, request *queueservice_pb.AcknowledgeMessageRequest) (*queueservice_pb.AcknowledgeMessageResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		specificReq, ok := req.(*queueservice_pb.AcknowledgeMessageRequest)
		if !ok {
			return &queueservice_pb.AcknowledgeMessageResponse{Success: false}, errors.New("invalid request type for acknowledging message request")
		}
		return cs.storage.AcknowledgeMessage(ctx, specificReq)
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &queueservice_pb.AcknowledgeMessageResponse{Success: false})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*queueservice_pb.AcknowledgeMessageResponse), err
}
func (cs *chronoqueueService) RenewMessageLease(ctx context.Context, request *queueservice_pb.RenewMessageLeaseRequest) (*queueservice_pb.RenewMessageLeaseResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		specificReq, ok := req.(*queueservice_pb.RenewMessageLeaseRequest)
		if !ok {
			return nil, errors.New("invalid request type for renewing message lease")
		}
		return cs.storage.RenewMessageLease(ctx, specificReq)
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &queueservice_pb.RenewMessageLeaseResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*queueservice_pb.RenewMessageLeaseResponse), err
}
func (cs *chronoqueueService) PeekQueueMessages(ctx context.Context, request *queueservice_pb.PeekQueueMessagesRequest) (*queueservice_pb.PeekQueueMessagesResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		specificReq, ok := req.(*queueservice_pb.PeekQueueMessagesRequest)
		if !ok {
			return nil, errors.New("invalid request type for peeking queue's messages")
		}
		return cs.storage.PeekQueueMessages(ctx, specificReq)
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &queueservice_pb.PeekQueueMessagesResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*queueservice_pb.PeekQueueMessagesResponse), err
}
func (cs *chronoqueueService) GetQueueState(ctx context.Context, request *queueservice_pb.GetQueueStateRequest) (*queueservice_pb.GetQueueStateResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		specificReq, ok := req.(*queueservice_pb.GetQueueStateRequest)
		if !ok {
			return nil, errors.New("invalid request type for peeking queue's messages")
		}
		return cs.storage.GetQueueState(ctx, specificReq)
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &queueservice_pb.GetQueueStateResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*queueservice_pb.GetQueueStateResponse), err
}

func (cs *chronoqueueService) SendMessageHeartBeat(ctx context.Context, request *queueservice_pb.SendMessageHeartBeatRequest) (*queueservice_pb.SendMessageHeartBeatResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		specificReq, ok := req.(*queueservice_pb.SendMessageHeartBeatRequest)
		if !ok {
			return nil, errors.New("invalid request type for sending message's heartbeat")
		}
		return cs.storage.SendMessageHeartBeat(ctx, specificReq)
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &queueservice_pb.SendMessageHeartBeatResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*queueservice_pb.SendMessageHeartBeatResponse), err
}

func (cs *chronoqueueService) ListQueues(ctx context.Context, request *queueservice_pb.ListQueuesRequest) (*queueservice_pb.ListQueuesResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		return cs.storage.ListQueues(ctx, req.(*queueservice_pb.ListQueuesRequest))
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &queueservice_pb.ListQueuesResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*queueservice_pb.ListQueuesResponse), err
}

func (cs *chronoqueueService) CreateSchedule(ctx context.Context, request *queueservice_pb.CreateScheduleRequest) (*queueservice_pb.CreateScheduleResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		return cs.storage.CreateSchedule(ctx, req.(*queueservice_pb.CreateScheduleRequest))
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &queueservice_pb.CreateScheduleResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*queueservice_pb.CreateScheduleResponse), err
}

func (cs *chronoqueueService) DeleteSchedule(ctx context.Context, request *queueservice_pb.DeleteScheduleRequest) (*queueservice_pb.DeleteScheduleResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		return cs.storage.DeleteSchedule(ctx, req.(*queueservice_pb.DeleteScheduleRequest))
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &queueservice_pb.DeleteScheduleResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*queueservice_pb.DeleteScheduleResponse), err
}

func (cs *chronoqueueService) GetSchedule(ctx context.Context, request *queueservice_pb.GetScheduleRequest) (*queueservice_pb.GetScheduleResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		return cs.storage.GetSchedule(ctx, req.(*queueservice_pb.GetScheduleRequest))
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &queueservice_pb.GetScheduleResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*queueservice_pb.GetScheduleResponse), err
}

func (cs *chronoqueueService) ListSchedules(ctx context.Context, request *queueservice_pb.ListSchedulesRequest) (*queueservice_pb.ListSchedulesResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		return cs.storage.ListSchedules(ctx, req.(*queueservice_pb.ListSchedulesRequest))
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &queueservice_pb.ListSchedulesResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*queueservice_pb.ListSchedulesResponse), err
}

func (cs *chronoqueueService) GetScheduleHistory(ctx context.Context, request *queueservice_pb.GetScheduleHistoryRequest) (*queueservice_pb.GetScheduleHistoryResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		return cs.storage.GetScheduleHistory(ctx, req.(*queueservice_pb.GetScheduleHistoryRequest))
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &queueservice_pb.GetScheduleHistoryResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*queueservice_pb.GetScheduleHistoryResponse), err
}

func (cs *chronoqueueService) PauseSchedule(ctx context.Context, request *queueservice_pb.PauseScheduleRequest) (*queueservice_pb.PauseScheduleResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		return cs.storage.PauseSchedule(ctx, req.(*queueservice_pb.PauseScheduleRequest))
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &queueservice_pb.PauseScheduleResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*queueservice_pb.PauseScheduleResponse), err
}

func (cs *chronoqueueService) ResumeSchedule(ctx context.Context, request *queueservice_pb.ResumeScheduleRequest) (*queueservice_pb.ResumeScheduleResponse, error) {
	adapterFunc := func(ctx context.Context, req interface{}) (interface{}, error) {
		return cs.storage.ResumeSchedule(ctx, req.(*queueservice_pb.ResumeScheduleRequest))
	}
	wrappedHandler := util.ErrorHandler(adapterFunc, &queueservice_pb.ResumeScheduleResponse{})

	resp, err := wrappedHandler(ctx, request)
	return resp.(*queueservice_pb.ResumeScheduleResponse), err
}
