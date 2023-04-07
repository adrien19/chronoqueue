package transport

import (
	"context"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue/endpoints"
	grpctransport "github.com/go-kit/kit/transport/grpc"
)

type grpcServer struct {
	createQueue        grpctransport.Handler
	deleteQueue        grpctransport.Handler
	postMessage        grpctransport.Handler
	getNextMessage     grpctransport.Handler
	acknowledgeMessage grpctransport.Handler

	renewMessageLease grpctransport.Handler
	peekQueueMessages grpctransport.Handler
	getQueueState     grpctransport.Handler
	chronoqueue.UnimplementedChronoQueueServer
}

func NewGRPCServer(ep endpoints.Set) chronoqueue.ChronoQueueServer {
	return &grpcServer{
		createQueue: grpctransport.NewServer(
			ep.CreateQueueEndpoint,
			decodeGRPCCreateQueueRequest,
			decodeGRPCCreateQueueResponse,
		),
		deleteQueue: grpctransport.NewServer(
			ep.DeleteQueueEndpoint,
			decodeGRPCDeleteQueueRequest,
			decodeGRPCDeleteQueueResponse,
		),
		postMessage: grpctransport.NewServer(
			ep.PostMessageEndpoint,
			decodeGRPCPostMessageRequest,
			decodeGRPCPostMessageResponse,
		),
		getNextMessage: grpctransport.NewServer(
			ep.GetNextMessageEndpoint,
			decodeGRPCGetNextMessageRequest,
			decodeGRPCGetNextMessageResponse,
		),
		acknowledgeMessage: grpctransport.NewServer(
			ep.AcknowledgeMessageEndpoint,
			decodeGRPCAcknowledgeMessageRequest,
			decodeGRPCAcknowledgeMessageResponse,
		),
		renewMessageLease: grpctransport.NewServer(
			ep.RenewMessageLeaseEndpoint,
			decodeGRPCRenewMessageLeaseRequest,
			decodeGRPCRenewMessageLeaseResponse,
		),
		peekQueueMessages: grpctransport.NewServer(
			ep.PeekQueueMessagesEndpoint,
			decodeGRPCPeekQueueMessagesRequest,
			decodeGRPCPeekQueueMessagesResponse,
		),
		getQueueState: grpctransport.NewServer(
			ep.GetQueueStateEndpoint,
			decodeGRPCGetQueueStateRequest,
			decodeGRPCGetQueueStateResponse,
		),
	}
}

func (g *grpcServer) CreateQueue(ctx context.Context, r *chronoqueue.CreateQueueRequest) (*chronoqueue.CreateQueueResponse, error) {
	_, _, err := g.createQueue.ServeGRPC(ctx, r)
	if err != nil {
		return &chronoqueue.CreateQueueResponse{}, err
	}
	return &chronoqueue.CreateQueueResponse{}, nil
}

func (g *grpcServer) DeleteQueue(ctx context.Context, r *chronoqueue.DeleteQueueRequest) (*chronoqueue.DeleteQueueResponse, error) {
	_, _, err := g.deleteQueue.ServeGRPC(ctx, r)
	if err != nil {
		return &chronoqueue.DeleteQueueResponse{}, err
	}
	return &chronoqueue.DeleteQueueResponse{}, nil
}

func (g *grpcServer) PostMessage(ctx context.Context, r *chronoqueue.PostMessageRequest) (*chronoqueue.PostMessageResponse, error) {
	_, _, err := g.postMessage.ServeGRPC(ctx, r)
	if err != nil {
		return &chronoqueue.PostMessageResponse{}, err
	}
	return &chronoqueue.PostMessageResponse{}, nil
}

func (g *grpcServer) GetNextMessage(ctx context.Context, r *chronoqueue.GetNextMessageRequest) (*chronoqueue.GetNextMessageResponse, error) {
	_, rep, err := g.getNextMessage.ServeGRPC(ctx, r)
	if err != nil {
		return nil, err
	}
	return rep.(*chronoqueue.GetNextMessageResponse), nil
}

func (g *grpcServer) AcknowledgeMessage(ctx context.Context, r *chronoqueue.AcknowledgeMessageRequest) (*chronoqueue.AcknowledgeMessageResponse, error) {
	_, _, err := g.acknowledgeMessage.ServeGRPC(ctx, r)
	if err != nil {
		return &chronoqueue.AcknowledgeMessageResponse{}, err
	}
	return &chronoqueue.AcknowledgeMessageResponse{}, nil
}

func (g *grpcServer) RenewMessageLease(ctx context.Context, r *chronoqueue.RenewMessageLeaseRequest) (*chronoqueue.RenewMessageLeaseResponse, error) {
	_, _, err := g.renewMessageLease.ServeGRPC(ctx, r)
	if err != nil {
		return &chronoqueue.RenewMessageLeaseResponse{}, err
	}

	return &chronoqueue.RenewMessageLeaseResponse{}, nil
}

func (g *grpcServer) PeekQueueMessages(ctx context.Context, r *chronoqueue.PeekQueueMessagesRequest) (*chronoqueue.PeekQueueMessagesResponse, error) {
	_, resp, err := g.peekQueueMessages.ServeGRPC(ctx, r)
	if err != nil {
		return nil, err
	}
	return resp.(*chronoqueue.PeekQueueMessagesResponse), nil
}

func (g *grpcServer) GetQueueState(ctx context.Context, r *chronoqueue.GetQueueStateRequest) (*chronoqueue.GetQueueStateResponse, error) {
	_, rep, err := g.getQueueState.ServeGRPC(ctx, r)
	if err != nil {
		return nil, err
	}
	return rep.(*chronoqueue.GetQueueStateResponse), nil
}

func decodeGRPCCreateQueueRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.CreateQueueRequest)
	return req.Queue, nil
}

func decodeGRPCCreateQueueResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	if grpcReply == nil {
		return &chronoqueue.CreateQueueResponse{}, nil
	}
	_ = grpcReply.(*chronoqueue.CreateQueueResponse)
	return &chronoqueue.CreateQueueResponse{}, nil
}

func decodeGRPCDeleteQueueRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.DeleteQueueRequest)
	return endpoints.DeleteQueueRequest{QueueName: req.Name}, nil
}

func decodeGRPCDeleteQueueResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	if grpcReply == nil {
		return &chronoqueue.DeleteQueueResponse{}, nil
	}
	_ = grpcReply.(endpoints.ErrorResponse)
	return &chronoqueue.DeleteQueueResponse{}, nil
}

func decodeGRPCPostMessageRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.PostMessageRequest)
	return req, nil
}

func decodeGRPCPostMessageResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	if grpcReply == nil {
		return &chronoqueue.PostMessageResponse{}, nil
	}
	_ = grpcReply.(*chronoqueue.PostMessageResponse)
	return &chronoqueue.PostMessageResponse{}, nil
}

func decodeGRPCGetNextMessageRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.GetNextMessageRequest)
	return req, nil
}

func decodeGRPCGetNextMessageResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*chronoqueue.GetNextMessageResponse)
	return reply, nil
}

func decodeGRPCAcknowledgeMessageRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.AcknowledgeMessageRequest)
	return endpoints.AcknowledgeMessageRequest{QueueName: req.QueueName, MessageID: req.MessageId, State: internal.State(req.State)}, nil
}

func decodeGRPCAcknowledgeMessageResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	if grpcReply == nil {
		return &chronoqueue.AcknowledgeMessageResponse{}, util.ErrUnknown
	}
	_ = grpcReply.(endpoints.ErrorResponse)
	return &chronoqueue.AcknowledgeMessageResponse{}, nil
}

func decodeGRPCRenewMessageLeaseRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.RenewMessageLeaseRequest)
	return req, nil
}

func decodeGRPCRenewMessageLeaseResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	if grpcReply == nil {
		return &chronoqueue.RenewMessageLeaseResponse{}, nil
	}
	reply := grpcReply.(*chronoqueue.RenewMessageLeaseResponse)
	return reply, nil
}

func decodeGRPCPeekQueueMessagesRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.PeekQueueMessagesRequest)
	return req, nil
}

func decodeGRPCPeekQueueMessagesResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*chronoqueue.PeekQueueMessagesResponse)
	return reply, nil
}

func decodeGRPCGetQueueStateRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.GetQueueStateRequest)
	return req, nil
}

func decodeGRPCGetQueueStateResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*chronoqueue.GetQueueStateResponse)
	return reply, nil
}
