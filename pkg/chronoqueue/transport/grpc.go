package transport

import (
	"context"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
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

	renewMessageLease    grpctransport.Handler
	peekQueueMessages    grpctransport.Handler
	getQueueState        grpctransport.Handler
	sendMessageHeartBeat grpctransport.Handler
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
		sendMessageHeartBeat: grpctransport.NewServer(
			ep.SendMessageHeartBeatEndpoint,
			decodeGRPCSendMessageHeartBeatRequest,
			decodeGRPCSendMessageHeartBeatResponse,
		),
	}
}

func (g *grpcServer) CreateQueue(ctx context.Context, r *chronoqueue.CreateQueueRequest) (*chronoqueue.CreateQueueResponse, error) {
	_, resp, err := g.createQueue.ServeGRPC(ctx, r)
	if err != nil {
		return &chronoqueue.CreateQueueResponse{Success: false}, err
	}
	return resp.(*chronoqueue.CreateQueueResponse), nil
}

func (g *grpcServer) DeleteQueue(ctx context.Context, r *chronoqueue.DeleteQueueRequest) (*chronoqueue.DeleteQueueResponse, error) {
	_, resp, err := g.deleteQueue.ServeGRPC(ctx, r)
	if err != nil {
		return &chronoqueue.DeleteQueueResponse{Success: false}, err
	}
	return resp.(*chronoqueue.DeleteQueueResponse), nil
}

func (g *grpcServer) PostMessage(ctx context.Context, r *chronoqueue.PostMessageRequest) (*chronoqueue.PostMessageResponse, error) {
	// Validate the size of a message based on simple estimations.
	err := util.ValidateMessageSize(r.Message)
	if err != nil {
		return &chronoqueue.PostMessageResponse{}, err
	}
	_, _, err = g.postMessage.ServeGRPC(ctx, r)
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

func (g *grpcServer) SendMessageHeartBeat(ctx context.Context, r *chronoqueue.SendMessageHeartBeatRequest) (*chronoqueue.SendMessageHeartBeatResponse, error) {
	_, rep, err := g.sendMessageHeartBeat.ServeGRPC(ctx, r)
	if err != nil {
		return nil, err
	}
	return rep.(*chronoqueue.SendMessageHeartBeatResponse), nil
}

func decodeGRPCCreateQueueRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.CreateQueueRequest)
	return req, nil
}

func decodeGRPCCreateQueueResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*chronoqueue.CreateQueueResponse)
	return reply, nil
}

func decodeGRPCDeleteQueueRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.DeleteQueueRequest)
	return req, nil
}

func decodeGRPCDeleteQueueResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*chronoqueue.DeleteQueueResponse)
	return reply, nil
}

func decodeGRPCPostMessageRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.PostMessageRequest)
	return req, nil
}

func decodeGRPCPostMessageResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*chronoqueue.PostMessageResponse)
	return reply, nil
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
	return req, nil
}

func decodeGRPCAcknowledgeMessageResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*chronoqueue.AcknowledgeMessageResponse)
	return reply, nil
}

func decodeGRPCRenewMessageLeaseRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.RenewMessageLeaseRequest)
	return req, nil
}

func decodeGRPCRenewMessageLeaseResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
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

func decodeGRPCSendMessageHeartBeatRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.SendMessageHeartBeatRequest)
	return req, nil
}

func decodeGRPCSendMessageHeartBeatResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*chronoqueue.SendMessageHeartBeatResponse)
	return reply, nil
}
