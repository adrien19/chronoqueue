package transport

import (
	"context"

	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/adrien19/chronoqueue/pkg/endpoints"
	log "github.com/adrien19/chronoqueue/pkg/log"
	grpctransport "github.com/go-kit/kit/transport/grpc"
	// "github.com/go-kit/log"
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

	queueservice_pb.UnimplementedQueueServiceServer
}

func NewGRPCServer(ep endpoints.Set, logger *log.Logger) queueservice_pb.QueueServiceServer {
	// opts := []grpctransport.ServerOption{
	// 	grpctransport.ServerErrorHandler(transport.NewLogErrorHandler(logger)),
	// }
	return &grpcServer{
		createQueue: grpctransport.NewServer(
			ep.CreateQueueEndpoint,
			decodeGRPCCreateQueueRequest,
			decodeGRPCCreateQueueResponse,
			// opts...,
		),
		deleteQueue: grpctransport.NewServer(
			ep.DeleteQueueEndpoint,
			decodeGRPCDeleteQueueRequest,
			decodeGRPCDeleteQueueResponse,
			// opts...,
		),
		postMessage: grpctransport.NewServer(
			ep.PostMessageEndpoint,
			decodeGRPCPostMessageRequest,
			decodeGRPCPostMessageResponse,
			// opts...,
		),
		getNextMessage: grpctransport.NewServer(
			ep.GetNextMessageEndpoint,
			decodeGRPCGetNextMessageRequest,
			decodeGRPCGetNextMessageResponse,
			// opts...,
		),
		acknowledgeMessage: grpctransport.NewServer(
			ep.AcknowledgeMessageEndpoint,
			decodeGRPCAcknowledgeMessageRequest,
			decodeGRPCAcknowledgeMessageResponse,
			// opts...,
		),
		renewMessageLease: grpctransport.NewServer(
			ep.RenewMessageLeaseEndpoint,
			decodeGRPCRenewMessageLeaseRequest,
			decodeGRPCRenewMessageLeaseResponse,
			// opts...,
		),
		peekQueueMessages: grpctransport.NewServer(
			ep.PeekQueueMessagesEndpoint,
			decodeGRPCPeekQueueMessagesRequest,
			decodeGRPCPeekQueueMessagesResponse,
			// opts...,
		),
		getQueueState: grpctransport.NewServer(
			ep.GetQueueStateEndpoint,
			decodeGRPCGetQueueStateRequest,
			decodeGRPCGetQueueStateResponse,
			// opts...,
		),
		sendMessageHeartBeat: grpctransport.NewServer(
			ep.SendMessageHeartBeatEndpoint,
			decodeGRPCSendMessageHeartBeatRequest,
			decodeGRPCSendMessageHeartBeatResponse,
			// opts...,
		),
		listQueues: grpctransport.NewServer(
			ep.ListQueuesEndpoint,
			decodeGRPCListQueuesRequest,
			decodeGRPCListQueuesResponse,
			// opts...,
		),
		createSchedule: grpctransport.NewServer(
			ep.CreateScheduleEndpoint,
			decodeGRPCCreateScheduleRequest,
			decodeGRPCCreateScheduleResponse,
			// opts...,
		),
		deleteSchedule: grpctransport.NewServer(
			ep.DeleteScheduleEndpoint,
			decodeGRPCDeleteScheduleRequest,
			decodeGRPCDeleteScheduleResponse,
			// opts...,
		),
		getSchedule: grpctransport.NewServer(
			ep.GetScheduleEndpoint,
			decodeGRPCGetScheduleRequest,
			decodeGRPCGetScheduleResponse,
			// opts...,
		),
		listSchedules: grpctransport.NewServer(
			ep.ListSchedulesEndpoint,
			decodeGRPCListSchedulesRequest,
			decodeGRPCListSchedulesResponse,
			// opts...,
		),
		getScheduleHistory: grpctransport.NewServer(
			ep.GetScheduleHistoryEndpoint,
			decodeGRPCGetScheduleHistoryRequest,
			decodeGRPCGetScheduleHistoryResponse,
			// opts...,
		),
		pauseSchedule: grpctransport.NewServer(
			ep.PauseScheduleEndpoint,
			decodeGRPCPauseScheduleRequest,
			decodeGRPCPauseScheduleResponse,
			// opts...,
		),
		resumeSchedule: grpctransport.NewServer(
			ep.ResumeScheduleEndpoint,
			decodeGRPCResumeScheduleRequest,
			decodeGRPCResumeScheduleResponse,
			// opts...,
		),
	}
}

func (g *grpcServer) CreateQueue(ctx context.Context, r *queueservice_pb.CreateQueueRequest) (*queueservice_pb.CreateQueueResponse, error) {
	_, resp, err := g.createQueue.ServeGRPC(ctx, r)
	if err != nil {
		return &queueservice_pb.CreateQueueResponse{Success: false}, err
	}
	return resp.(*queueservice_pb.CreateQueueResponse), nil
}

func (g *grpcServer) DeleteQueue(ctx context.Context, r *queueservice_pb.DeleteQueueRequest) (*queueservice_pb.DeleteQueueResponse, error) {
	_, resp, err := g.deleteQueue.ServeGRPC(ctx, r)
	if err != nil {
		return &queueservice_pb.DeleteQueueResponse{Success: false}, err
	}
	return resp.(*queueservice_pb.DeleteQueueResponse), nil
}

func (g *grpcServer) PostMessage(ctx context.Context, r *queueservice_pb.PostMessageRequest) (*queueservice_pb.PostMessageResponse, error) {
	// Validate the size of a message based on simple estimations.
	err := util.ValidateMessageSize(r.Message)
	if err != nil {
		return &queueservice_pb.PostMessageResponse{Success: false}, err
	}
	_, resp, err := g.postMessage.ServeGRPC(ctx, r)
	if err != nil {
		return &queueservice_pb.PostMessageResponse{Success: false}, err
	}
	return resp.(*queueservice_pb.PostMessageResponse), nil
}

func (g *grpcServer) GetNextMessage(ctx context.Context, r *queueservice_pb.GetNextMessageRequest) (*queueservice_pb.GetNextMessageResponse, error) {
	_, rep, err := g.getNextMessage.ServeGRPC(ctx, r)
	if err != nil {
		return nil, err
	}
	return rep.(*queueservice_pb.GetNextMessageResponse), nil
}

func (g *grpcServer) AcknowledgeMessage(ctx context.Context, r *queueservice_pb.AcknowledgeMessageRequest) (*queueservice_pb.AcknowledgeMessageResponse, error) {
	_, resp, err := g.acknowledgeMessage.ServeGRPC(ctx, r)
	if err != nil {
		return &queueservice_pb.AcknowledgeMessageResponse{Success: false}, err
	}
	return resp.(*queueservice_pb.AcknowledgeMessageResponse), nil
}

func (g *grpcServer) RenewMessageLease(ctx context.Context, r *queueservice_pb.RenewMessageLeaseRequest) (*queueservice_pb.RenewMessageLeaseResponse, error) {
	_, resp, err := g.renewMessageLease.ServeGRPC(ctx, r)
	if err != nil || resp == nil {
		return nil, err
	}

	return resp.(*queueservice_pb.RenewMessageLeaseResponse), nil
}

func (g *grpcServer) PeekQueueMessages(ctx context.Context, r *queueservice_pb.PeekQueueMessagesRequest) (*queueservice_pb.PeekQueueMessagesResponse, error) {
	_, resp, err := g.peekQueueMessages.ServeGRPC(ctx, r)
	if err != nil {
		return nil, err
	}
	return resp.(*queueservice_pb.PeekQueueMessagesResponse), nil
}

func (g *grpcServer) GetQueueState(ctx context.Context, r *queueservice_pb.GetQueueStateRequest) (*queueservice_pb.GetQueueStateResponse, error) {
	_, rep, err := g.getQueueState.ServeGRPC(ctx, r)
	if err != nil {
		return nil, err
	}
	return rep.(*queueservice_pb.GetQueueStateResponse), nil
}

func (g *grpcServer) SendMessageHeartBeat(ctx context.Context, r *queueservice_pb.SendMessageHeartBeatRequest) (*queueservice_pb.SendMessageHeartBeatResponse, error) {
	_, rep, err := g.sendMessageHeartBeat.ServeGRPC(ctx, r)
	if err != nil {
		return nil, err
	}
	return rep.(*queueservice_pb.SendMessageHeartBeatResponse), nil
}

func (g *grpcServer) ListQueues(ctx context.Context, r *queueservice_pb.ListQueuesRequest) (*queueservice_pb.ListQueuesResponse, error) {
	_, rep, err := g.listQueues.ServeGRPC(ctx, r)
	if err != nil {
		return nil, err
	}
	return rep.(*queueservice_pb.ListQueuesResponse), nil
}

func (g *grpcServer) CreateSchedule(ctx context.Context, r *queueservice_pb.CreateScheduleRequest) (*queueservice_pb.CreateScheduleResponse, error) {
	_, resp, err := g.createSchedule.ServeGRPC(ctx, r)
	if err != nil {
		return &queueservice_pb.CreateScheduleResponse{Success: false}, err
	}
	return resp.(*queueservice_pb.CreateScheduleResponse), nil
}

func (g *grpcServer) DeleteSchedule(ctx context.Context, r *queueservice_pb.DeleteScheduleRequest) (*queueservice_pb.DeleteScheduleResponse, error) {
	_, resp, err := g.deleteSchedule.ServeGRPC(ctx, r)
	if err != nil {
		return &queueservice_pb.DeleteScheduleResponse{Success: false}, err
	}
	return resp.(*queueservice_pb.DeleteScheduleResponse), nil
}

func (g *grpcServer) GetSchedule(ctx context.Context, r *queueservice_pb.GetScheduleRequest) (*queueservice_pb.GetScheduleResponse, error) {
	_, resp, err := g.getSchedule.ServeGRPC(ctx, r)
	if err != nil {
		return nil, err
	}
	return resp.(*queueservice_pb.GetScheduleResponse), nil
}

func (g *grpcServer) ListSchedules(ctx context.Context, r *queueservice_pb.ListSchedulesRequest) (*queueservice_pb.ListSchedulesResponse, error) {
	_, resp, err := g.listSchedules.ServeGRPC(ctx, r)
	if err != nil {
		return nil, err
	}
	return resp.(*queueservice_pb.ListSchedulesResponse), nil
}

func (g *grpcServer) GetScheduleHistory(ctx context.Context, r *queueservice_pb.GetScheduleHistoryRequest) (*queueservice_pb.GetScheduleHistoryResponse, error) {
	_, resp, err := g.getScheduleHistory.ServeGRPC(ctx, r)
	if err != nil {
		return nil, err
	}
	return resp.(*queueservice_pb.GetScheduleHistoryResponse), nil
}

func (g *grpcServer) PauseSchedule(ctx context.Context, r *queueservice_pb.PauseScheduleRequest) (*queueservice_pb.PauseScheduleResponse, error) {
	_, resp, err := g.pauseSchedule.ServeGRPC(ctx, r)
	if err != nil {
		return &queueservice_pb.PauseScheduleResponse{Success: false}, err
	}
	return resp.(*queueservice_pb.PauseScheduleResponse), nil
}

func (g *grpcServer) ResumeSchedule(ctx context.Context, r *queueservice_pb.ResumeScheduleRequest) (*queueservice_pb.ResumeScheduleResponse, error) {
	_, resp, err := g.resumeSchedule.ServeGRPC(ctx, r)
	if err != nil {
		return &queueservice_pb.ResumeScheduleResponse{Success: false}, err
	}
	return resp.(*queueservice_pb.ResumeScheduleResponse), nil
}

func decodeGRPCCreateQueueRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*queueservice_pb.CreateQueueRequest)
	return req, nil
}

func decodeGRPCCreateQueueResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*queueservice_pb.CreateQueueResponse)
	return reply, nil
}

func decodeGRPCDeleteQueueRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*queueservice_pb.DeleteQueueRequest)
	return req, nil
}

func decodeGRPCDeleteQueueResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*queueservice_pb.DeleteQueueResponse)
	return reply, nil
}

func decodeGRPCPostMessageRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*queueservice_pb.PostMessageRequest)
	return req, nil
}

func decodeGRPCPostMessageResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*queueservice_pb.PostMessageResponse)
	return reply, nil
}

func decodeGRPCGetNextMessageRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*queueservice_pb.GetNextMessageRequest)
	return req, nil
}

func decodeGRPCGetNextMessageResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*queueservice_pb.GetNextMessageResponse)
	return reply, nil
}

func decodeGRPCAcknowledgeMessageRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*queueservice_pb.AcknowledgeMessageRequest)
	return req, nil
}

func decodeGRPCAcknowledgeMessageResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*queueservice_pb.AcknowledgeMessageResponse)
	return reply, nil
}

func decodeGRPCRenewMessageLeaseRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*queueservice_pb.RenewMessageLeaseRequest)
	return req, nil
}

func decodeGRPCRenewMessageLeaseResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*queueservice_pb.RenewMessageLeaseResponse)
	return reply, nil
}

func decodeGRPCPeekQueueMessagesRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*queueservice_pb.PeekQueueMessagesRequest)
	return req, nil
}

func decodeGRPCPeekQueueMessagesResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*queueservice_pb.PeekQueueMessagesResponse)
	return reply, nil
}

func decodeGRPCGetQueueStateRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*queueservice_pb.GetQueueStateRequest)
	return req, nil
}

func decodeGRPCGetQueueStateResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*queueservice_pb.GetQueueStateResponse)
	return reply, nil
}

func decodeGRPCSendMessageHeartBeatRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*queueservice_pb.SendMessageHeartBeatRequest)
	return req, nil
}

func decodeGRPCSendMessageHeartBeatResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*queueservice_pb.SendMessageHeartBeatResponse)
	return reply, nil
}

func decodeGRPCListQueuesRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*queueservice_pb.ListQueuesRequest)
	return req, nil
}

func decodeGRPCListQueuesResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*queueservice_pb.ListQueuesResponse)
	return reply, nil
}

func decodeGRPCCreateScheduleRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*queueservice_pb.CreateScheduleRequest)
	return req, nil
}

func decodeGRPCCreateScheduleResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*queueservice_pb.CreateScheduleResponse)
	return reply, nil
}

func decodeGRPCDeleteScheduleRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*queueservice_pb.DeleteScheduleRequest)
	return req, nil
}

func decodeGRPCDeleteScheduleResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*queueservice_pb.DeleteScheduleResponse)
	return reply, nil
}

func decodeGRPCGetScheduleRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*queueservice_pb.GetScheduleRequest)
	return req, nil
}

func decodeGRPCGetScheduleResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*queueservice_pb.GetScheduleResponse)
	return reply, nil
}

func decodeGRPCListSchedulesRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*queueservice_pb.ListSchedulesRequest)
	return req, nil
}

func decodeGRPCListSchedulesResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*queueservice_pb.ListSchedulesResponse)
	return reply, nil
}

func decodeGRPCGetScheduleHistoryRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*queueservice_pb.GetScheduleHistoryRequest)
	return req, nil
}

func decodeGRPCGetScheduleHistoryResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*queueservice_pb.GetScheduleHistoryResponse)
	return reply, nil
}

func decodeGRPCPauseScheduleRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*queueservice_pb.PauseScheduleRequest)
	return req, nil
}

func decodeGRPCPauseScheduleResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*queueservice_pb.PauseScheduleResponse)
	return reply, nil
}

func decodeGRPCResumeScheduleRequest(_ context.Context, grpcReq interface{}) (interface{}, error) {
	req := grpcReq.(*queueservice_pb.ResumeScheduleRequest)
	return req, nil
}

func decodeGRPCResumeScheduleResponse(_ context.Context, grpcReply interface{}) (interface{}, error) {
	reply := grpcReply.(*queueservice_pb.ResumeScheduleResponse)
	return reply, nil
}
