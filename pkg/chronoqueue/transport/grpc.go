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
	listQueues           grpctransport.Handler

	createSchedule     grpctransport.Handler
	deleteSchedule     grpctransport.Handler
	getSchedule        grpctransport.Handler
	listSchedules      grpctransport.Handler
	getScheduleHistory grpctransport.Handler
	pauseSchedule      grpctransport.Handler
	resumeSchedule     grpctransport.Handler

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
		listQueues: grpctransport.NewServer(
			ep.ListQueuesEndpoint,
			decodeGRPCListQueuesRequest,
			decodeGRPCListQueuesResponse,
		),
		createSchedule: grpctransport.NewServer(
			ep.CreateScheduleEndpoint,
			decodeGRPCCreateScheduleRequest,
			decodeGRPCCreateScheduleResponse,
		),
		deleteSchedule: grpctransport.NewServer(
			ep.DeleteScheduleEndpoint,
			decodeGRPCDeleteScheduleRequest,
			decodeGRPCDeleteScheduleResponse,
		),
		getSchedule: grpctransport.NewServer(
			ep.GetScheduleEndpoint,
			decodeGRPCGetScheduleRequest,
			decodeGRPCGetScheduleResponse,
		),
		listSchedules: grpctransport.NewServer(
			ep.ListSchedulesEndpoint,
			decodeGRPCListSchedulesRequest,
			decodeGRPCListSchedulesResponse,
		),
		getScheduleHistory: grpctransport.NewServer(
			ep.GetScheduleHistoryEndpoint,
			decodeGRPCGetScheduleHistoryRequest,
			decodeGRPCGetScheduleHistoryResponse,
		),
		pauseSchedule: grpctransport.NewServer(
			ep.PauseScheduleEndpoint,
			decodeGRPCPauseScheduleRequest,
			decodeGRPCPauseScheduleResponse,
		),
		resumeSchedule: grpctransport.NewServer(
			ep.ResumeScheduleEndpoint,
			decodeGRPCResumeScheduleRequest,
			decodeGRPCResumeScheduleResponse,
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
		return &chronoqueue.PostMessageResponse{Success: false}, err
	}
	_, resp, err := g.postMessage.ServeGRPC(ctx, r)
	if err != nil {
		return &chronoqueue.PostMessageResponse{Success: false}, err
	}
	return resp.(*chronoqueue.PostMessageResponse), nil
}

func (g *grpcServer) GetNextMessage(ctx context.Context, r *chronoqueue.GetNextMessageRequest) (*chronoqueue.GetNextMessageResponse, error) {
	_, rep, err := g.getNextMessage.ServeGRPC(ctx, r)
	if err != nil {
		return nil, err
	}
	return rep.(*chronoqueue.GetNextMessageResponse), nil
}

func (g *grpcServer) AcknowledgeMessage(ctx context.Context, r *chronoqueue.AcknowledgeMessageRequest) (*chronoqueue.AcknowledgeMessageResponse, error) {
	_, resp, err := g.acknowledgeMessage.ServeGRPC(ctx, r)
	if err != nil {
		return &chronoqueue.AcknowledgeMessageResponse{Success: false}, err
	}
	return resp.(*chronoqueue.AcknowledgeMessageResponse), nil
}

func (g *grpcServer) RenewMessageLease(ctx context.Context, r *chronoqueue.RenewMessageLeaseRequest) (*chronoqueue.RenewMessageLeaseResponse, error) {
	_, resp, err := g.renewMessageLease.ServeGRPC(ctx, r)
	if err != nil || resp == nil {
		return nil, err
	}

	return resp.(*chronoqueue.RenewMessageLeaseResponse), nil
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

func (g *grpcServer) ListQueues(ctx context.Context, r *chronoqueue.ListQueuesRequest) (*chronoqueue.ListQueuesResponse, error) {
	_, rep, err := g.listQueues.ServeGRPC(ctx, r)
	if err != nil {
		return nil, err
	}
	return rep.(*chronoqueue.ListQueuesResponse), nil
}

func (g *grpcServer) CreateSchedule(ctx context.Context, r *chronoqueue.CreateScheduleRequest) (*chronoqueue.CreateScheduleResponse, error) {
	_, resp, err := g.createSchedule.ServeGRPC(ctx, r)
	if err != nil {
		return &chronoqueue.CreateScheduleResponse{Success: false}, err
	}
	return resp.(*chronoqueue.CreateScheduleResponse), nil
}

func (g *grpcServer) DeleteSchedule(ctx context.Context, r *chronoqueue.DeleteScheduleRequest) (*chronoqueue.DeleteScheduleResponse, error) {
	_, resp, err := g.deleteSchedule.ServeGRPC(ctx, r)
	if err != nil {
		return &chronoqueue.DeleteScheduleResponse{Success: false}, err
	}
	return resp.(*chronoqueue.DeleteScheduleResponse), nil
}

func (g *grpcServer) GetSchedule(ctx context.Context, r *chronoqueue.GetScheduleRequest) (*chronoqueue.GetScheduleResponse, error) {
	_, resp, err := g.getSchedule.ServeGRPC(ctx, r)
	if err != nil {
		return nil, err
	}
	return resp.(*chronoqueue.GetScheduleResponse), nil
}

func (g *grpcServer) ListSchedules(ctx context.Context, r *chronoqueue.ListSchedulesRequest) (*chronoqueue.ListSchedulesResponse, error) {
	_, resp, err := g.listSchedules.ServeGRPC(ctx, r)
	if err != nil {
		return nil, err
	}
	return resp.(*chronoqueue.ListSchedulesResponse), nil
}

func (g *grpcServer) GetScheduleHistory(ctx context.Context, r *chronoqueue.GetScheduleHistoryRequest) (*chronoqueue.GetScheduleHistoryResponse, error) {
	_, resp, err := g.getScheduleHistory.ServeGRPC(ctx, r)
	if err != nil {
		return nil, err
	}
	return resp.(*chronoqueue.GetScheduleHistoryResponse), nil
}

func (g *grpcServer) PauseSchedule(ctx context.Context, r *chronoqueue.PauseScheduleRequest) (*chronoqueue.PauseScheduleResponse, error) {
	_, resp, err := g.pauseSchedule.ServeGRPC(ctx, r)
	if err != nil {
		return &chronoqueue.PauseScheduleResponse{Success: false}, err
	}
	return resp.(*chronoqueue.PauseScheduleResponse), nil
}

func (g *grpcServer) ResumeSchedule(ctx context.Context, r *chronoqueue.ResumeScheduleRequest) (*chronoqueue.ResumeScheduleResponse, error) {
	_, resp, err := g.resumeSchedule.ServeGRPC(ctx, r)
	if err != nil {
		return &chronoqueue.ResumeScheduleResponse{Success: false}, err
	}
	return resp.(*chronoqueue.ResumeScheduleResponse), nil
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

func decodeGRPCListQueuesRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.ListQueuesRequest)
	return req, nil
}

func decodeGRPCListQueuesResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*chronoqueue.ListQueuesResponse)
	return reply, nil
}

func decodeGRPCCreateScheduleRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.CreateScheduleRequest)
	return req, nil
}

func decodeGRPCCreateScheduleResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*chronoqueue.CreateScheduleResponse)
	return reply, nil
}

func decodeGRPCDeleteScheduleRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.DeleteScheduleRequest)
	return req, nil
}

func decodeGRPCDeleteScheduleResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*chronoqueue.DeleteScheduleResponse)
	return reply, nil
}

func decodeGRPCGetScheduleRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.GetScheduleRequest)
	return req, nil
}

func decodeGRPCGetScheduleResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*chronoqueue.GetScheduleResponse)
	return reply, nil
}

func decodeGRPCListSchedulesRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.ListSchedulesRequest)
	return req, nil
}

func decodeGRPCListSchedulesResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*chronoqueue.ListSchedulesResponse)
	return reply, nil
}

func decodeGRPCGetScheduleHistoryRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.GetScheduleHistoryRequest)
	return req, nil
}

func decodeGRPCGetScheduleHistoryResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*chronoqueue.GetScheduleHistoryResponse)
	return reply, nil
}

func decodeGRPCPauseScheduleRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.PauseScheduleRequest)
	return req, nil
}

func decodeGRPCPauseScheduleResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*chronoqueue.PauseScheduleResponse)
	return reply, nil
}

func decodeGRPCResumeScheduleRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*chronoqueue.ResumeScheduleRequest)
	return req, nil
}

func decodeGRPCResumeScheduleResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*chronoqueue.ResumeScheduleResponse)
	return reply, nil
}
