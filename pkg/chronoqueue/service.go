package chronoqueue

import (
	"context"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
)

type Service interface {
	CreateQueue(ctx context.Context, request *chronoqueue.CreateQueueRequest) (*chronoqueue.CreateQueueResponse, error)
	DeleteQueue(ctx context.Context, request *chronoqueue.DeleteQueueRequest) (*chronoqueue.DeleteQueueResponse, error)
	PostMessage(ctx context.Context, request *chronoqueue.PostMessageRequest) (*chronoqueue.PostMessageResponse, error)
	GetNextMessage(ctx context.Context, request *chronoqueue.GetNextMessageRequest) (*chronoqueue.GetNextMessageResponse, error)
	AcknowledgeMessage(ctx context.Context, request *chronoqueue.AcknowledgeMessageRequest) (*chronoqueue.AcknowledgeMessageResponse, error)
	RenewMessageLease(ctx context.Context, request *chronoqueue.RenewMessageLeaseRequest) (*chronoqueue.RenewMessageLeaseResponse, error)
	PeekQueueMessages(ctx context.Context, request *chronoqueue.PeekQueueMessagesRequest) (*chronoqueue.PeekQueueMessagesResponse, error)
	GetQueueState(ctx context.Context, request *chronoqueue.GetQueueStateRequest) (*chronoqueue.GetQueueStateResponse, error)
	SendMessageHeartBeat(ctx context.Context, request *chronoqueue.SendMessageHeartBeatRequest) (*chronoqueue.SendMessageHeartBeatResponse, error)
	ListQueues(ctx context.Context, request *chronoqueue.ListQueuesRequest) (*chronoqueue.ListQueuesResponse, error)
	CreateSchedule(ctx context.Context, request *chronoqueue.CreateScheduleRequest) (*chronoqueue.CreateScheduleResponse, error)
	DeleteSchedule(ctx context.Context, request *chronoqueue.DeleteScheduleRequest) (*chronoqueue.DeleteScheduleResponse, error)
	GetSchedule(ctx context.Context, request *chronoqueue.GetScheduleRequest) (*chronoqueue.GetScheduleResponse, error)
	ListSchedules(ctx context.Context, request *chronoqueue.ListSchedulesRequest) (*chronoqueue.ListSchedulesResponse, error)
	GetScheduleHistory(ctx context.Context, request *chronoqueue.GetScheduleHistoryRequest) (*chronoqueue.GetScheduleHistoryResponse, error)
	PauseSchedule(ctx context.Context, request *chronoqueue.PauseScheduleRequest) (*chronoqueue.PauseScheduleResponse, error)
	ResumeSchedule(ctx context.Context, request *chronoqueue.ResumeScheduleRequest) (*chronoqueue.ResumeScheduleResponse, error)
}
