package endpoints

import (
	"context"

	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue"
	"github.com/go-kit/kit/endpoint"
)

// Add the new endpoints to the Set struct
type Set struct {
	CreateQueueEndpoint          endpoint.Endpoint
	DeleteQueueEndpoint          endpoint.Endpoint
	PostMessageEndpoint          endpoint.Endpoint
	GetNextMessageEndpoint       endpoint.Endpoint
	AcknowledgeMessageEndpoint   endpoint.Endpoint
	RenewMessageLeaseEndpoint    endpoint.Endpoint
	PeekQueueMessagesEndpoint    endpoint.Endpoint
	GetQueueStateEndpoint        endpoint.Endpoint
	SendMessageHeartBeatEndpoint endpoint.Endpoint
	ListQueuesEndpoint           endpoint.Endpoint
	CreateScheduleEndpoint       endpoint.Endpoint
	DeleteScheduleEndpoint       endpoint.Endpoint
	GetScheduleEndpoint          endpoint.Endpoint
	ListSchedulesEndpoint        endpoint.Endpoint
	GetScheduleHistoryEndpoint   endpoint.Endpoint
	PauseScheduleEndpoint        endpoint.Endpoint
	ResumeScheduleEndpoint       endpoint.Endpoint
}

func NewEndpointSet(svc chronoqueue.Service) Set {
	return Set{
		CreateQueueEndpoint:          MakeCreateQueueEndpoint(svc),
		DeleteQueueEndpoint:          MakeDeleteQueueEndpoint(svc),
		PostMessageEndpoint:          MakePostMessageEndpoint(svc),
		GetNextMessageEndpoint:       MakeGetNextMessageEndpoint(svc),
		AcknowledgeMessageEndpoint:   MakeAcknowledgeMessageEndpoint(svc),
		RenewMessageLeaseEndpoint:    MakeRenewMessageLeaseEndpoint(svc),
		PeekQueueMessagesEndpoint:    MakePeekQueueMessagesEndpoint(svc),
		GetQueueStateEndpoint:        MakeGetQueueStateEndpoint(svc),
		SendMessageHeartBeatEndpoint: MakeSendMessageHeartBeatEndpoint(svc),
		ListQueuesEndpoint:           MakeListQueuesEndpoint(svc),
		CreateScheduleEndpoint:       MakeCreateScheduleEndpoint(svc),
		DeleteScheduleEndpoint:       MakeDeleteScheduleEndpoint(svc),
		GetScheduleEndpoint:          MakeGetScheduleEndpoint(svc),
		ListSchedulesEndpoint:        MakeListSchedulesEndpoint(svc),
		GetScheduleHistoryEndpoint:   MakeGetScheduleHistoryEndpoint(svc),
		PauseScheduleEndpoint:        MakePauseScheduleEndpoint(svc),
		ResumeScheduleEndpoint:       MakeResumeScheduleEndpoint(svc),
	}
}

func MakeCreateQueueEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.CreateQueueRequest)
		return svc.CreateQueue(ctx, req)
	}
}

func MakeDeleteQueueEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.DeleteQueueRequest)
		return svc.DeleteQueue(ctx, req)
	}
}

func MakePostMessageEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.PostMessageRequest)
		return svc.PostMessage(ctx, req)
	}
}

func MakeGetNextMessageEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.GetNextMessageRequest)
		return svc.GetNextMessage(ctx, req)
	}
}

func MakeAcknowledgeMessageEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.AcknowledgeMessageRequest)
		return svc.AcknowledgeMessage(ctx, req)
	}
}

func MakeRenewMessageLeaseEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.RenewMessageLeaseRequest)
		return svc.RenewMessageLease(ctx, req)
	}
}

func MakePeekQueueMessagesEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.PeekQueueMessagesRequest)
		return svc.PeekQueueMessages(ctx, req)
	}
}

func MakeGetQueueStateEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.GetQueueStateRequest)
		return svc.GetQueueState(ctx, req)
	}
}

func MakeSendMessageHeartBeatEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.SendMessageHeartBeatRequest)
		return svc.SendMessageHeartBeat(ctx, req)
	}
}

func MakeListQueuesEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.ListQueuesRequest)
		return svc.ListQueues(ctx, req)
	}
}

func MakeCreateScheduleEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.CreateScheduleRequest)
		return svc.CreateSchedule(ctx, req)
	}
}

func MakeDeleteScheduleEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.DeleteScheduleRequest)
		return svc.DeleteSchedule(ctx, req)
	}
}

func MakeGetScheduleEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.GetScheduleRequest)
		return svc.GetSchedule(ctx, req)
	}
}

func MakeListSchedulesEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.ListSchedulesRequest)
		return svc.ListSchedules(ctx, req)
	}
}

func MakeGetScheduleHistoryEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.GetScheduleHistoryRequest)
		return svc.GetScheduleHistory(ctx, req)
	}
}

func MakePauseScheduleEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.PauseScheduleRequest)
		return svc.PauseSchedule(ctx, req)
	}
}

func MakeResumeScheduleEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.ResumeScheduleRequest)
		return svc.ResumeSchedule(ctx, req)
	}
}

func (s *Set) CreateQueue(ctx context.Context, request *queueservice_pb.CreateQueueRequest) (*queueservice_pb.CreateQueueResponse, error) {
	resp, err := s.CreateQueueEndpoint(ctx, request)
	if err != nil {
		return resp.(*queueservice_pb.CreateQueueResponse), err
	}
	return resp.(*queueservice_pb.CreateQueueResponse), err
}

func (s *Set) DeleteQueue(ctx context.Context, request *queueservice_pb.DeleteQueueRequest) (*queueservice_pb.DeleteQueueResponse, error) {
	resp, err := s.DeleteQueueEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.DeleteQueueResponse{Success: false}, err
	}
	return resp.(*queueservice_pb.DeleteQueueResponse), err
}

func (s *Set) PostMessage(ctx context.Context, request *queueservice_pb.PostMessageRequest) (*queueservice_pb.PostMessageResponse, error) {
	resp, err := s.PostMessageEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.PostMessageResponse{Success: false}, err
	}
	return resp.(*queueservice_pb.PostMessageResponse), err
}

func (s *Set) GetNextMessage(ctx context.Context, request *queueservice_pb.GetNextMessageRequest) (*queueservice_pb.GetNextMessageResponse, error) {
	resp, err := s.GetNextMessageEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.GetNextMessageResponse{
			Message: nil,
		}, err
	}
	messageResp := resp.(*queueservice_pb.GetNextMessageResponse)
	return messageResp, nil

}

func (s *Set) AcknowledgeMessage(ctx context.Context, request *queueservice_pb.AcknowledgeMessageRequest) (*queueservice_pb.AcknowledgeMessageResponse, error) {
	resp, err := s.AcknowledgeMessageEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.AcknowledgeMessageResponse{Success: false}, err
	}
	ackResp := resp.(*queueservice_pb.AcknowledgeMessageResponse)
	return ackResp, nil
}

func (s *Set) RenewMessageLease(ctx context.Context, request *queueservice_pb.RenewMessageLeaseRequest) (*queueservice_pb.RenewMessageLeaseResponse, error) {
	resp, err := s.RenewMessageLeaseEndpoint(ctx, request)
	if err != nil || resp == nil {
		return nil, err
	}
	renewLeaseResp := resp.(*queueservice_pb.RenewMessageLeaseResponse)
	return renewLeaseResp, nil
}

func (s *Set) PeekQueueMessages(ctx context.Context, queueName string) (*queueservice_pb.PeekQueueMessagesResponse, error) {
	resp, err := s.PeekQueueMessagesEndpoint(ctx, queueName)
	if err != nil {
		return &queueservice_pb.PeekQueueMessagesResponse{}, err
	}
	messagesResp := resp.(*queueservice_pb.PeekQueueMessagesResponse)
	return messagesResp, nil

}

func (s *Set) GetQueueState(ctx context.Context, queueName string) (*queueservice_pb.GetQueueStateResponse, error) {
	resp, err := s.GetQueueStateEndpoint(ctx, queueName)
	if err != nil {
		return &queueservice_pb.GetQueueStateResponse{}, err
	}
	stateResp := resp.(*queueservice_pb.GetQueueStateResponse)

	return stateResp, nil

}

func (s *Set) SendMessageHeartBeat(ctx context.Context, queueName string) (*queueservice_pb.SendMessageHeartBeatResponse, error) {
	resp, err := s.GetQueueStateEndpoint(ctx, queueName)
	if err != nil {
		return &queueservice_pb.SendMessageHeartBeatResponse{}, err
	}
	stateResp := resp.(*queueservice_pb.SendMessageHeartBeatResponse)

	return stateResp, nil

}

func (s *Set) ListQueues(ctx context.Context, request *queueservice_pb.ListQueuesRequest) (*queueservice_pb.ListQueuesResponse, error) {
	resp, err := s.ListQueuesEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.ListQueuesResponse{}, err
	}
	listResp := resp.(*queueservice_pb.ListQueuesResponse)
	return listResp, nil
}

func (s *Set) CreateSchedule(ctx context.Context, request *queueservice_pb.CreateScheduleRequest) (*queueservice_pb.CreateScheduleResponse, error) {
	resp, err := s.CreateScheduleEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.CreateScheduleResponse{Success: false}, err
	}
	createResp := resp.(*queueservice_pb.CreateScheduleResponse)
	return createResp, nil
}

func (s *Set) DeleteSchedule(ctx context.Context, request *queueservice_pb.DeleteScheduleRequest) (*queueservice_pb.DeleteScheduleResponse, error) {
	resp, err := s.DeleteScheduleEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.DeleteScheduleResponse{Success: false}, err
	}
	deleteResp := resp.(*queueservice_pb.DeleteScheduleResponse)
	return deleteResp, nil
}

func (s *Set) GetSchedule(ctx context.Context, request *queueservice_pb.GetScheduleRequest) (*queueservice_pb.GetScheduleResponse, error) {
	resp, err := s.GetScheduleEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.GetScheduleResponse{}, err
	}
	getResp := resp.(*queueservice_pb.GetScheduleResponse)
	return getResp, nil
}

func (s *Set) ListSchedules(ctx context.Context, request *queueservice_pb.ListSchedulesRequest) (*queueservice_pb.ListSchedulesResponse, error) {
	resp, err := s.ListSchedulesEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.ListSchedulesResponse{}, err
	}
	listResp := resp.(*queueservice_pb.ListSchedulesResponse)
	return listResp, nil
}

func (s *Set) GetScheduleHistory(ctx context.Context, request *queueservice_pb.GetScheduleHistoryRequest) (*queueservice_pb.GetScheduleHistoryResponse, error) {
	resp, err := s.GetScheduleHistoryEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.GetScheduleHistoryResponse{}, err
	}
	historyResp := resp.(*queueservice_pb.GetScheduleHistoryResponse)
	return historyResp, nil
}

func (s *Set) PauseSchedule(ctx context.Context, request *queueservice_pb.PauseScheduleRequest) (*queueservice_pb.PauseScheduleResponse, error) {
	resp, err := s.PauseScheduleEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.PauseScheduleResponse{Success: false}, err
	}
	pauseResp := resp.(*queueservice_pb.PauseScheduleResponse)
	return pauseResp, nil
}

func (s *Set) ResumeSchedule(ctx context.Context, request *queueservice_pb.ResumeScheduleRequest) (*queueservice_pb.ResumeScheduleResponse, error) {
	resp, err := s.ResumeScheduleEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.ResumeScheduleResponse{Success: false}, err
	}
	resumeResp := resp.(*queueservice_pb.ResumeScheduleResponse)
	return resumeResp, nil
}
