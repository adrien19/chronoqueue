package chronoqueue

import (
	"context"

	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/pkg/repository"
	schedule "github.com/adrien19/chronoqueue/pkg/schedule"
)

type chronoqueueService struct {
	storage repository.Storage
}

func NewChronoqueueService(storage repository.Storage) Service {
	return &chronoqueueService{storage: storage}
}

func (cs *chronoqueueService) CreateQueue(ctx context.Context, request *queueservice_pb.CreateQueueRequest) (*queueservice_pb.CreateQueueResponse, error) {
	return cs.storage.CreateQueue(ctx, request)
}

func (cs *chronoqueueService) DeleteQueue(ctx context.Context, request *queueservice_pb.DeleteQueueRequest) (*queueservice_pb.DeleteQueueResponse, error) {
	return cs.storage.DeleteQueue(ctx, request)
}

func (cs *chronoqueueService) PostMessage(ctx context.Context, request *queueservice_pb.PostMessageRequest) (*queueservice_pb.PostMessageResponse, error) {
	return cs.storage.CreateQueueMessage(ctx, request)
}

func (cs *chronoqueueService) GetNextMessage(ctx context.Context, request *queueservice_pb.GetNextMessageRequest) (*queueservice_pb.GetNextMessageResponse, error) {
	return cs.storage.GetQueueMessage(ctx, request)
}

func (cs *chronoqueueService) AcknowledgeMessage(ctx context.Context, request *queueservice_pb.AcknowledgeMessageRequest) (*queueservice_pb.AcknowledgeMessageResponse, error) {
	return cs.storage.AcknowledgeMessage(ctx, request)
}

func (cs *chronoqueueService) RenewMessageLease(ctx context.Context, request *queueservice_pb.RenewMessageLeaseRequest) (*queueservice_pb.RenewMessageLeaseResponse, error) {
	return cs.storage.RenewMessageLease(ctx, request)
}

func (cs *chronoqueueService) PeekQueueMessages(ctx context.Context, request *queueservice_pb.PeekQueueMessagesRequest) (*queueservice_pb.PeekQueueMessagesResponse, error) {
	return cs.storage.PeekQueueMessages(ctx, request)
}

func (cs *chronoqueueService) GetQueueState(ctx context.Context, request *queueservice_pb.GetQueueStateRequest) (*queueservice_pb.GetQueueStateResponse, error) {
	return cs.storage.GetQueueState(ctx, request)
}

func (cs *chronoqueueService) SendMessageHeartBeat(ctx context.Context, request *queueservice_pb.SendMessageHeartBeatRequest) (*queueservice_pb.SendMessageHeartBeatResponse, error) {
	return cs.storage.SendMessageHeartBeat(ctx, request)
}

func (cs *chronoqueueService) ListQueues(ctx context.Context, request *queueservice_pb.ListQueuesRequest) (*queueservice_pb.ListQueuesResponse, error) {
	return cs.storage.ListQueues(ctx, request)
}

func (cs *chronoqueueService) CreateSchedule(ctx context.Context, request *queueservice_pb.CreateScheduleRequest) (*queueservice_pb.CreateScheduleResponse, error) {
	schedule, err := schedule.NewScheduleFromProto(request.Schedule)
	if err != nil {
		return nil, err
	}
	return cs.storage.CreateSchedule(ctx, schedule)
}

func (cs *chronoqueueService) DeleteSchedule(ctx context.Context, request *queueservice_pb.DeleteScheduleRequest) (*queueservice_pb.DeleteScheduleResponse, error) {
	return cs.storage.DeleteSchedule(ctx, request.GetScheduleId())
}

func (cs *chronoqueueService) GetSchedule(ctx context.Context, request *queueservice_pb.GetScheduleRequest) (*queueservice_pb.GetScheduleResponse, error) {
	return cs.storage.GetSchedule(ctx, request.GetScheduleId())
}

func (cs *chronoqueueService) ListSchedules(ctx context.Context, request *queueservice_pb.ListSchedulesRequest) (*queueservice_pb.ListSchedulesResponse, error) {
	return cs.storage.ListSchedules(ctx, request.GetPrefix())
}

func (cs *chronoqueueService) GetScheduleHistory(ctx context.Context, request *queueservice_pb.GetScheduleHistoryRequest) (*queueservice_pb.GetScheduleHistoryResponse, error) {
	return cs.storage.GetScheduleHistory(ctx, request.GetScheduleId(), request.GetLimit())
}

func (cs *chronoqueueService) PauseSchedule(ctx context.Context, request *queueservice_pb.PauseScheduleRequest) (*queueservice_pb.PauseScheduleResponse, error) {
	return cs.storage.PauseSchedule(ctx, request.GetScheduleId())
}

func (cs *chronoqueueService) ResumeSchedule(ctx context.Context, request *queueservice_pb.ResumeScheduleRequest) (*queueservice_pb.ResumeScheduleResponse, error) {
	return cs.storage.ResumeSchedule(ctx, request.GetScheduleId())
}
