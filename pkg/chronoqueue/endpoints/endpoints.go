package endpoints

import (
	"context"

	pb "github.com/adrien19/chronoqueue/api/chronoqueue/v1"
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
		req := request.(*pb.CreateQueueRequest)
		return svc.CreateQueue(ctx, req)
	}
}

func MakeDeleteQueueEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*pb.DeleteQueueRequest)
		return svc.DeleteQueue(ctx, req)
	}
}

func MakePostMessageEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*pb.PostMessageRequest)
		return svc.PostMessage(ctx, req)
	}
}

func MakeGetNextMessageEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*pb.GetNextMessageRequest)
		return svc.GetNextMessage(ctx, req)
	}
}

func MakeAcknowledgeMessageEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*pb.AcknowledgeMessageRequest)
		return svc.AcknowledgeMessage(ctx, req)
	}
}

func MakeRenewMessageLeaseEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*pb.RenewMessageLeaseRequest)
		return svc.RenewMessageLease(ctx, req)
	}
}

func MakePeekQueueMessagesEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*pb.PeekQueueMessagesRequest)
		return svc.PeekQueueMessages(ctx, req)
	}
}

func MakeGetQueueStateEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*pb.GetQueueStateRequest)
		return svc.GetQueueState(ctx, req)
	}
}

func MakeSendMessageHeartBeatEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*pb.SendMessageHeartBeatRequest)
		return svc.SendMessageHeartBeat(ctx, req)
	}
}

func MakeListQueuesEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*pb.ListQueuesRequest)
		return svc.ListQueues(ctx, req)
	}
}

func MakeCreateScheduleEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*pb.CreateScheduleRequest)
		return svc.CreateSchedule(ctx, req)
	}
}

func MakeDeleteScheduleEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*pb.DeleteScheduleRequest)
		return svc.DeleteSchedule(ctx, req)
	}
}

func MakeGetScheduleEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*pb.GetScheduleRequest)
		return svc.GetSchedule(ctx, req)
	}
}

func MakeListSchedulesEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*pb.ListSchedulesRequest)
		return svc.ListSchedules(ctx, req)
	}
}

func MakeGetScheduleHistoryEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*pb.GetScheduleHistoryRequest)
		return svc.GetScheduleHistory(ctx, req)
	}
}

func MakePauseScheduleEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*pb.PauseScheduleRequest)
		return svc.PauseSchedule(ctx, req)
	}
}

func MakeResumeScheduleEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*pb.ResumeScheduleRequest)
		return svc.ResumeSchedule(ctx, req)
	}
}

func (s *Set) CreateQueue(ctx context.Context, queueInfo *pb.Queue) (*pb.CreateQueueResponse, error) {
	resp, err := s.CreateQueueEndpoint(ctx, queueInfo)
	if err != nil {
		return resp.(*pb.CreateQueueResponse), err
	}
	return resp.(*pb.CreateQueueResponse), err
}

func (s *Set) DeleteQueue(ctx context.Context, request *pb.DeleteQueueRequest) (*pb.DeleteQueueResponse, error) {
	resp, err := s.DeleteQueueEndpoint(ctx, request)
	if err != nil {
		return &pb.DeleteQueueResponse{Success: false}, err
	}
	return resp.(*pb.DeleteQueueResponse), err
}

func (s *Set) PostMessage(ctx context.Context, request *pb.PostMessageRequest) (*pb.PostMessageResponse, error) {
	resp, err := s.PostMessageEndpoint(ctx, request)
	if err != nil {
		return &pb.PostMessageResponse{Success: false}, err
	}
	return resp.(*pb.PostMessageResponse), err
}

func (s *Set) GetNextMessage(ctx context.Context, request *pb.GetNextMessageRequest) (*pb.GetNextMessageResponse, error) {
	resp, err := s.GetNextMessageEndpoint(ctx, request)
	if err != nil {
		return &pb.GetNextMessageResponse{
			Message: nil,
		}, err
	}
	messageResp := resp.(*pb.GetNextMessageResponse)
	return messageResp, nil

}

func (s *Set) AcknowledgeMessage(ctx context.Context, request *pb.AcknowledgeMessageRequest) (*pb.AcknowledgeMessageResponse, error) {
	resp, err := s.AcknowledgeMessageEndpoint(ctx, request)
	if err != nil {
		return &pb.AcknowledgeMessageResponse{Success: false}, err
	}
	ackResp := resp.(*pb.AcknowledgeMessageResponse)
	return ackResp, nil
}

func (s *Set) RenewMessageLease(ctx context.Context, request *pb.RenewMessageLeaseRequest) (*pb.RenewMessageLeaseResponse, error) {
	resp, err := s.RenewMessageLeaseEndpoint(ctx, request)
	if err != nil || resp == nil {
		return nil, err
	}
	renewLeaseResp := resp.(*pb.RenewMessageLeaseResponse)
	return renewLeaseResp, nil
}

func (s *Set) PeekQueueMessages(ctx context.Context, queueName string) (*pb.PeekQueueMessagesResponse, error) {
	resp, err := s.PeekQueueMessagesEndpoint(ctx, queueName)
	if err != nil {
		return &pb.PeekQueueMessagesResponse{}, err
	}
	messagesResp := resp.(*pb.PeekQueueMessagesResponse)
	return messagesResp, nil

}

func (s *Set) GetQueueState(ctx context.Context, queueName string) (*pb.GetQueueStateResponse, error) {
	resp, err := s.GetQueueStateEndpoint(ctx, queueName)
	if err != nil {
		return &pb.GetQueueStateResponse{}, err
	}
	stateResp := resp.(*pb.GetQueueStateResponse)

	return stateResp, nil

}

func (s *Set) SendMessageHeartBeat(ctx context.Context, queueName string) (*pb.SendMessageHeartBeatResponse, error) {
	resp, err := s.GetQueueStateEndpoint(ctx, queueName)
	if err != nil {
		return &pb.SendMessageHeartBeatResponse{}, err
	}
	stateResp := resp.(*pb.SendMessageHeartBeatResponse)

	return stateResp, nil

}

func (s *Set) ListQueues(ctx context.Context, request *pb.ListQueuesRequest) (*pb.ListQueuesResponse, error) {
	resp, err := s.ListQueuesEndpoint(ctx, request)
	if err != nil {
		return &pb.ListQueuesResponse{}, err
	}
	listResp := resp.(*pb.ListQueuesResponse)
	return listResp, nil
}

func (s *Set) CreateSchedule(ctx context.Context, request *pb.CreateScheduleRequest) (*pb.CreateScheduleResponse, error) {
	resp, err := s.CreateScheduleEndpoint(ctx, request)
	if err != nil {
		return &pb.CreateScheduleResponse{Success: false}, err
	}
	createResp := resp.(*pb.CreateScheduleResponse)
	return createResp, nil
}

func (s *Set) DeleteSchedule(ctx context.Context, request *pb.DeleteScheduleRequest) (*pb.DeleteScheduleResponse, error) {
	resp, err := s.DeleteScheduleEndpoint(ctx, request)
	if err != nil {
		return &pb.DeleteScheduleResponse{Success: false}, err
	}
	deleteResp := resp.(*pb.DeleteScheduleResponse)
	return deleteResp, nil
}

func (s *Set) GetSchedule(ctx context.Context, request *pb.GetScheduleRequest) (*pb.GetScheduleResponse, error) {
	resp, err := s.GetScheduleEndpoint(ctx, request)
	if err != nil {
		return &pb.GetScheduleResponse{}, err
	}
	getResp := resp.(*pb.GetScheduleResponse)
	return getResp, nil
}

func (s *Set) ListSchedules(ctx context.Context, request *pb.ListSchedulesRequest) (*pb.ListSchedulesResponse, error) {
	resp, err := s.ListSchedulesEndpoint(ctx, request)
	if err != nil {
		return &pb.ListSchedulesResponse{}, err
	}
	listResp := resp.(*pb.ListSchedulesResponse)
	return listResp, nil
}

func (s *Set) GetScheduleHistory(ctx context.Context, request *pb.GetScheduleHistoryRequest) (*pb.GetScheduleHistoryResponse, error) {
	resp, err := s.GetScheduleHistoryEndpoint(ctx, request)
	if err != nil {
		return &pb.GetScheduleHistoryResponse{}, err
	}
	historyResp := resp.(*pb.GetScheduleHistoryResponse)
	return historyResp, nil
}

func (s *Set) PauseSchedule(ctx context.Context, request *pb.PauseScheduleRequest) (*pb.PauseScheduleResponse, error) {
	resp, err := s.PauseScheduleEndpoint(ctx, request)
	if err != nil {
		return &pb.PauseScheduleResponse{Success: false}, err
	}
	pauseResp := resp.(*pb.PauseScheduleResponse)
	return pauseResp, nil
}

func (s *Set) ResumeSchedule(ctx context.Context, request *pb.ResumeScheduleRequest) (*pb.ResumeScheduleResponse, error) {
	resp, err := s.ResumeScheduleEndpoint(ctx, request)
	if err != nil {
		return &pb.ResumeScheduleResponse{Success: false}, err
	}
	resumeResp := resp.(*pb.ResumeScheduleResponse)
	return resumeResp, nil
}
