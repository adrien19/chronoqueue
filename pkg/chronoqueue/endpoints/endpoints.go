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
		req := request.(*pb.Queue)
		err = svc.CreateQueue(ctx, req)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}
}

func MakeDeleteQueueEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(DeleteQueueRequest)
		err = svc.DeleteQueue(ctx, req.QueueName)
		if err != nil {
			return nil, err
		}
		return nil, err
	}
}

func MakePostMessageEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*pb.PostMessageRequest)
		err = svc.PostMessage(ctx, req.QueueName, req.Message)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}
}

func MakeGetNextMessageEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*pb.GetNextMessageRequest)
		message, err := svc.GetNextMessage(ctx, req.QueueName, req.LeaseDuration)
		if err != nil {
			return &pb.GetNextMessageResponse{
				Message: nil,
			}, err
		}

		return &pb.GetNextMessageResponse{
			Message: message,
		}, nil
	}
}

func MakeAcknowledgeMessageEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(AcknowledgeMessageRequest)
		err = svc.AcknowledgeMessage(ctx, req.QueueName, req.MessageID, req.State)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}
}

func MakeRenewMessageLeaseEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(RenewMessageLeaseRequest)
		err = svc.RenewMessageLease(ctx, req.QueueName, req.LeaseDuration, req.MessageID)
		if err != nil {
			return nil, err
		}

		return nil, nil
	}
}

func MakePeekQueueMessagesEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(PeekQueueMessagesRequest)
		messages, err := svc.PeekQueueMessages(ctx, req.QueueName, req.Limit, req.PriorityRange)
		if err != nil {
			return PeekQueueMessagesResponse{}, err
		}

		return PeekQueueMessagesResponse{Messages: messages}, nil
	}
}

func MakeGetQueueStateEndpoint(svc chronoqueue.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(GetQueueStateRequest)
		state, err := svc.GetQueueState(ctx, req.QueueName)
		if err != nil {
			return GetQueueStateResponse{}, err
		}

		return GetQueueStateResponse{
			InvisibleMessagesCount: state.InvisibleMessagesCount,
			PendingMessagesCount:   state.PendingMessagesCount,
			RunningMessagesCount:   state.RunningMessagesCount,
			CompletedMessagesCount: state.CompletedMessagesCount,
			CanceledMessagesCount:  state.CanceledMessagesCount,
			ErroredMessagesCount:   state.ErroredMessagesCount,
			EarliestDeadline:       state.EarliestDeadline,
		}, nil
	}
}

func (s *Set) CreateQueue(ctx context.Context, queueInfo *pb.Queue) error {
	_, err := s.CreateQueueEndpoint(ctx, queueInfo)
	if err != nil {
		return err
	}
	return nil
}

func (s *Set) DeleteQueue(ctx context.Context, queueName string) error {
	_, err := s.DeleteQueueEndpoint(ctx, queueName)
	if err != nil {
		return err
	}
	return nil
}

func (s *Set) PostMessage(ctx context.Context, queueName string, message *pb.Message) error {
	_, err := s.PostMessageEndpoint(ctx, &pb.PostMessageRequest{QueueName: queueName, Message: message})
	if err != nil {
		return err
	}
	return nil
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

func (s *Set) AcknowledgeMessage(ctx context.Context, queueName string, messageID string) error {
	_, err := s.AcknowledgeMessageEndpoint(ctx, AcknowledgeMessageRequest{QueueName: queueName, MessageID: messageID})
	if err != nil {
		return err
	}
	return nil
}

func (s *Set) RenewMessageLease(ctx context.Context, queueName string, leaseDuration int64, messageID string) error {
	_, err := s.RenewMessageLeaseEndpoint(ctx, RenewMessageLeaseRequest{QueueName: queueName, LeaseDuration: leaseDuration})
	if err != nil {
		return err
	}
	return nil
}

func (s *Set) PeekQueueMessages(ctx context.Context, queueName string) (PeekQueueMessagesResponse, error) {
	resp, err := s.PeekQueueMessagesEndpoint(ctx, queueName)
	if err != nil {
		return PeekQueueMessagesResponse{}, err
	}
	messagesResp := resp.(PeekQueueMessagesResponse)
	if messagesResp.Messages == nil {
		return PeekQueueMessagesResponse{}, nil
	}
	return PeekQueueMessagesResponse{Messages: messagesResp.Messages}, nil

}

func (s *Set) GetQueueState(ctx context.Context, queueName string) (GetQueueStateResponse, error) {
	resp, err := s.GetQueueStateEndpoint(ctx, queueName)
	if err != nil {
		return GetQueueStateResponse{}, err
	}
	stateResp := resp.(GetQueueStateResponse)

	return GetQueueStateResponse{
		InvisibleMessagesCount: stateResp.InvisibleMessagesCount,
		PendingMessagesCount:   stateResp.PendingMessagesCount,
		RunningMessagesCount:   stateResp.RunningMessagesCount,
		CompletedMessagesCount: stateResp.CompletedMessagesCount,
		CanceledMessagesCount:  stateResp.CanceledMessagesCount,
		ErroredMessagesCount:   stateResp.ErroredMessagesCount,
		EarliestDeadline:       stateResp.EarliestDeadline,
	}, nil

}
