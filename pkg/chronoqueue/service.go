package chronoqueue

import (
	"context"

	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
)

type Service interface {
	CreateQueue(ctx context.Context, request *queueservice_pb.CreateQueueRequest) (*queueservice_pb.CreateQueueResponse, error)
	DeleteQueue(ctx context.Context, request *queueservice_pb.DeleteQueueRequest) (*queueservice_pb.DeleteQueueResponse, error)
	PostMessage(ctx context.Context, request *queueservice_pb.PostMessageRequest) (*queueservice_pb.PostMessageResponse, error)
	GetNextMessage(ctx context.Context, request *queueservice_pb.GetNextMessageRequest) (*queueservice_pb.GetNextMessageResponse, error)
	AcknowledgeMessage(ctx context.Context, request *queueservice_pb.AcknowledgeMessageRequest) (*queueservice_pb.AcknowledgeMessageResponse, error)
	RenewMessageLease(ctx context.Context, request *queueservice_pb.RenewMessageLeaseRequest) (*queueservice_pb.RenewMessageLeaseResponse, error)
	PeekQueueMessages(ctx context.Context, request *queueservice_pb.PeekQueueMessagesRequest) (*queueservice_pb.PeekQueueMessagesResponse, error)
	GetQueueState(ctx context.Context, request *queueservice_pb.GetQueueStateRequest) (*queueservice_pb.GetQueueStateResponse, error)
	SendMessageHeartBeat(ctx context.Context, request *queueservice_pb.SendMessageHeartBeatRequest) (*queueservice_pb.SendMessageHeartBeatResponse, error)
	ListQueues(ctx context.Context, request *queueservice_pb.ListQueuesRequest) (*queueservice_pb.ListQueuesResponse, error)
	CreateSchedule(ctx context.Context, request *queueservice_pb.CreateScheduleRequest) (*queueservice_pb.CreateScheduleResponse, error)
	DeleteSchedule(ctx context.Context, request *queueservice_pb.DeleteScheduleRequest) (*queueservice_pb.DeleteScheduleResponse, error)
	GetSchedule(ctx context.Context, request *queueservice_pb.GetScheduleRequest) (*queueservice_pb.GetScheduleResponse, error)
	ListSchedules(ctx context.Context, request *queueservice_pb.ListSchedulesRequest) (*queueservice_pb.ListSchedulesResponse, error)
	GetScheduleHistory(ctx context.Context, request *queueservice_pb.GetScheduleHistoryRequest) (*queueservice_pb.GetScheduleHistoryResponse, error)
	PauseSchedule(ctx context.Context, request *queueservice_pb.PauseScheduleRequest) (*queueservice_pb.PauseScheduleResponse, error)
	ResumeSchedule(ctx context.Context, request *queueservice_pb.ResumeScheduleRequest) (*queueservice_pb.ResumeScheduleResponse, error)

	// calendar-based scheduling operations
	ValidateCalendarSchedule(ctx context.Context, request *queueservice_pb.ValidateCalendarScheduleRequest) (*queueservice_pb.ValidateCalendarScheduleResponse, error)
	PreviewCalendarSchedule(ctx context.Context, request *queueservice_pb.PreviewCalendarScheduleRequest) (*queueservice_pb.PreviewCalendarScheduleResponse, error)

	// Dead Letter Queue Management Operations
	GetDLQMessages(ctx context.Context, request *queueservice_pb.GetDLQMessagesRequest) (*queueservice_pb.GetDLQMessagesResponse, error)
	RequeueFromDLQ(ctx context.Context, request *queueservice_pb.RequeueFromDLQRequest) (*queueservice_pb.RequeueFromDLQResponse, error)
	DeleteFromDLQ(ctx context.Context, request *queueservice_pb.DeleteFromDLQRequest) (*queueservice_pb.DeleteFromDLQResponse, error)
	PurgeDLQ(ctx context.Context, request *queueservice_pb.PurgeDLQRequest) (*queueservice_pb.PurgeDLQResponse, error)
	GetDLQStats(ctx context.Context, request *queueservice_pb.GetDLQStatsRequest) (*queueservice_pb.GetDLQStatsResponse, error)
}
