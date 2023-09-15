package endpoints

import (
	"context"

	pb "github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue"
	"github.com/go-kit/kit/endpoint"
)

type Set struct {
	CreateQueueEndpoint        endpoint.Endpoint
	DeleteQueueEndpoint        endpoint.Endpoint
	PostMessageEndpoint        endpoint.Endpoint
	GetNextMessageEndpoint     endpoint.Endpoint
	AcknowledgeMessageEndpoint endpoint.Endpoint
	RenewMessageLeaseEndpoint  endpoint.Endpoint
	PeekQueueMessagesEndpoint  endpoint.Endpoint
	GetQueueStateEndpoint      endpoint.Endpoint
}

func NewEndpointSet(svc chronoqueue.Service) Set {
	return Set{
		CreateQueueEndpoint:        MakeCreateQueueEndpoint(svc),
		DeleteQueueEndpoint:        MakeDeleteQueueEndpoint(svc),
		PostMessageEndpoint:        MakePostMessageEndpoint(svc),
		GetNextMessageEndpoint:     MakeGetNextMessageEndpoint(svc),
		AcknowledgeMessageEndpoint: MakeAcknowledgeMessageEndpoint(svc),
		RenewMessageLeaseEndpoint:  MakeRenewMessageLeaseEndpoint(svc),
		PeekQueueMessagesEndpoint:  MakePeekQueueMessagesEndpoint(svc),
		GetQueueStateEndpoint:      MakeGetQueueStateEndpoint(svc),
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

func (s *Set) CreateQueue(ctx context.Context, queueInfo *pb.Queue) (*pb.CreateQueueResponse, error) {
	resp, err := s.CreateQueueEndpoint(ctx, queueInfo)
	if err != nil {
		return &pb.CreateQueueResponse{}, err
	}
	return resp.(*pb.CreateQueueResponse), err
}

func (s *Set) DeleteQueue(ctx context.Context, queueName string) (*pb.DeleteQueueResponse, error) {
	resp, err := s.DeleteQueueEndpoint(ctx, queueName)
	if err != nil {
		return &pb.DeleteQueueResponse{}, err
	}
	return resp.(*pb.DeleteQueueResponse), err
}

func (s *Set) PostMessage(ctx context.Context, queueName string, message *pb.Message) (*pb.PostMessageResponse, error) {
	resp, err := s.PostMessageEndpoint(ctx, &pb.PostMessageRequest{QueueName: queueName, Message: message})
	if err != nil {
		return &pb.PostMessageResponse{}, err
	}
	return resp.(*pb.PostMessageResponse), err
}

func (s *Set) GetNextMessage(ctx context.Context, queueName string, leaseDuration int64) (*pb.GetNextMessageResponse, error) {
	resp, err := s.GetNextMessageEndpoint(ctx, &pb.GetNextMessageRequest{QueueName: queueName, LeaseDuration: leaseDuration})
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
		return &pb.AcknowledgeMessageResponse{}, err
	}
	ackResp := resp.(*pb.AcknowledgeMessageResponse)
	return ackResp, nil
}

func (s *Set) RenewMessageLease(ctx context.Context, request *pb.RenewMessageLeaseRequest) (*pb.RenewMessageLeaseResponse, error) {
	resp, err := s.RenewMessageLeaseEndpoint(ctx, request)
	if err != nil {
		return &pb.RenewMessageLeaseResponse{}, err
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
