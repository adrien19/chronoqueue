package endpoints

import (
	"context"
	"time"

	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue/log"
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

func NewEndpointSet(svc chronoqueue.Service, logger *log.Logger) Set {
	return Set{
		CreateQueueEndpoint:          MakeCreateQueueEndpoint(svc, logger),
		DeleteQueueEndpoint:          MakeDeleteQueueEndpoint(svc, logger),
		PostMessageEndpoint:          MakePostMessageEndpoint(svc, logger),
		GetNextMessageEndpoint:       MakeGetNextMessageEndpoint(svc, logger),
		AcknowledgeMessageEndpoint:   MakeAcknowledgeMessageEndpoint(svc, logger),
		RenewMessageLeaseEndpoint:    MakeRenewMessageLeaseEndpoint(svc, logger),
		PeekQueueMessagesEndpoint:    MakePeekQueueMessagesEndpoint(svc, logger),
		GetQueueStateEndpoint:        MakeGetQueueStateEndpoint(svc, logger),
		SendMessageHeartBeatEndpoint: MakeSendMessageHeartBeatEndpoint(svc, logger),
		ListQueuesEndpoint:           MakeListQueuesEndpoint(svc, logger),
		CreateScheduleEndpoint:       MakeCreateScheduleEndpoint(svc, logger),
		DeleteScheduleEndpoint:       MakeDeleteScheduleEndpoint(svc, logger),
		GetScheduleEndpoint:          MakeGetScheduleEndpoint(svc, logger),
		ListSchedulesEndpoint:        MakeListSchedulesEndpoint(svc, logger),
		GetScheduleHistoryEndpoint:   MakeGetScheduleHistoryEndpoint(svc, logger),
		PauseScheduleEndpoint:        MakePauseScheduleEndpoint(svc, logger),
		ResumeScheduleEndpoint:       MakeResumeScheduleEndpoint(svc, logger),
	}
}

func MakeCreateQueueEndpoint(svc chronoqueue.Service, logger *log.Logger) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.CreateQueueRequest)
		defer func(begin time.Time) {
			if err != nil {
				logger.ErrorWithFields(
					"Failed to create queue",
					"method", "createQueue",
					"queue", req.GetName(),
					"type", req.Metadata.GetType(),
					"took", time.Since(begin),
					"err", err,
				)
			} else {
				logger.InfoWithFields(
					"Queue created successfully",
					"method", "createQueue",
					"queue", req.GetName(),
					"type", req.Metadata.GetType(),
					"took", time.Since(begin),
					"err", err,
				)
			}
		}(time.Now())
		return svc.CreateQueue(ctx, req)
	}
}

func MakeDeleteQueueEndpoint(svc chronoqueue.Service, logger *log.Logger) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.DeleteQueueRequest)
		defer func(begin time.Time) {
			if err != nil {
				logger.ErrorWithFields(
					"Failed to delete queue",
					"method", "deleteQueue",
					"queue", req.GetName(),
					"took", time.Since(begin),
					"err", err,
				)
			} else {
				logger.InfoWithFields(
					"Queue deleted successfully",
					"method", "deleteQueue",
					"queue", req.GetName(),
					"took", time.Since(begin),
					"err", err,
				)
			}
		}(time.Now())
		return svc.DeleteQueue(ctx, req)
	}
}

func MakePostMessageEndpoint(svc chronoqueue.Service, logger *log.Logger) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.PostMessageRequest)
		defer func(begin time.Time) {
			if err != nil {
				logger.ErrorWithFields(
					"Failed to post message",
					"queue", req.GetQueueName(),
					"messageId", req.Message.GetMessageId(),
					"metadata", req.Message.GetMetadata(),
					"took", time.Since(begin),
					"err", err,
				)
			} else {
				logger.InfoWithFields(
					"Message posted successfully",
					"queue", req.GetQueueName(),
					"messageId", req.Message.GetMessageId(),
					"metadata", req.Message.GetMetadata(),
					"took", time.Since(begin),
					"err", err,
				)
			}
		}(time.Now())
		return svc.PostMessage(ctx, req)
	}
}

func MakeGetNextMessageEndpoint(svc chronoqueue.Service, logger *log.Logger) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.GetNextMessageRequest)
		defer func(begin time.Time) {
			if err != nil {
				logger.ErrorWithFields(
					"Failed to get next message",
					"method", "getNextMessage",
					"queue", req.GetQueueName(),
					"exclusivityKey", req.GetExclusivityKey(),
					"leaseDuration", req.GetLeaseDuration(),
					"took", time.Since(begin),
					"err", err,
				)
			} else {
				logger.InfoWithFields(
					"Next message returned successfully",
					"method", "getNextMessage",
					"queue", req.GetQueueName(),
					"exclusivityKey", req.GetExclusivityKey(),
					"leaseDuration", req.GetLeaseDuration(),
					"took", time.Since(begin),
					"err", err,
				)
			}
		}(time.Now())
		return svc.GetNextMessage(ctx, req)
	}
}

func MakeAcknowledgeMessageEndpoint(svc chronoqueue.Service, logger *log.Logger) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.AcknowledgeMessageRequest)
		defer func(begin time.Time) {
			if err != nil {
				logger.ErrorWithFields(
					"Failed to acknowledge message",
					"method", "acknowledgeMessage",
					"queue", req.GetQueueName(),
					"messageId", req.GetMessageId(),
					"state", req.GetState(),
					"took", time.Since(begin),
					"err", err,
				)
			} else {
				logger.InfoWithFields(
					"Message acknowledged successfully",
					"method", "acknowledgeMessage",
					"queue", req.GetQueueName(),
					"messageId", req.GetMessageId(),
					"state", req.GetState(),
					"took", time.Since(begin),
					"err", err,
				)
			}
		}(time.Now())
		return svc.AcknowledgeMessage(ctx, req)
	}
}

func MakeRenewMessageLeaseEndpoint(svc chronoqueue.Service, logger *log.Logger) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.RenewMessageLeaseRequest)
		defer func(begin time.Time) {
			if err != nil {
				logger.ErrorWithFields(
					"Failed to renew message lease",
					"method", "renewMessageLease",
					"queue", req.GetQueueName(),
					"messageId", req.GetMessageId(),
					"leaseDuration", req.GetLeaseDuration(),
					"took", time.Since(begin),
					"err", err,
				)
			} else {
				logger.InfoWithFields(
					"lease renewed successfully",
					"method", "renewMessageLease",
					"queue", req.GetQueueName(),
					"messageId", req.GetMessageId(),
					"leaseDuration", req.GetLeaseDuration(),
					"took", time.Since(begin),
					"err", err,
				)
			}
		}(time.Now())
		return svc.RenewMessageLease(ctx, req)
	}
}

func MakePeekQueueMessagesEndpoint(svc chronoqueue.Service, logger *log.Logger) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.PeekQueueMessagesRequest)
		defer func(begin time.Time) {
			if err != nil {
				logger.ErrorWithFields(
					"Failed to peek queue messages",
					"method", "peekQueueMessages",
					"queue", req.GetQueueName(),
					"limit", req.GetLimit(),
					"priorityRange", req.GetPriorityRange(),
					"took", time.Since(begin),
					"err", err,
				)
			} else {
				logger.InfoWithFields(
					"Queue messages peeked successfully",
					"method", "peekQueueMessages",
					"queue", req.GetQueueName(),
					"limit", req.GetLimit(),
					"priorityRange", req.GetPriorityRange(),
					"took", time.Since(begin),
					"err", err,
				)
			}
		}(time.Now())
		return svc.PeekQueueMessages(ctx, req)
	}
}

func MakeGetQueueStateEndpoint(svc chronoqueue.Service, logger *log.Logger) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.GetQueueStateRequest)
		defer func(begin time.Time) {
			if err != nil {
				logger.ErrorWithFields(
					"Failed to get queue state",
					"method", "getQueueState",
					"queue", req.GetQueueName(),
					"took", time.Since(begin),
					"err", err,
				)
			} else {
				logger.InfoWithFields(
					"Queue state returned successfully",
					"method", "getQueueState",
					"queue", req.GetQueueName(),
					"took", time.Since(begin),
					"err", err,
				)
			}
		}(time.Now())
		return svc.GetQueueState(ctx, req)
	}
}

func MakeSendMessageHeartBeatEndpoint(svc chronoqueue.Service, logger *log.Logger) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.SendMessageHeartBeatRequest)
		defer func(begin time.Time) {
			if err != nil {
				logger.ErrorWithFields(
					"Failed to send message heartbeat",
					"method", "sendMessageHeartBeat",
					"queue", req.GetQueueName(),
					"messageId", req.GetMessageId(),
					"took", time.Since(begin),
					"err", err,
				)
			} else {
				logger.InfoWithFields(
					"Heartbeat sent successfully",
					"method", "sendMessageHeartBeat",
					"queue", req.GetQueueName(),
					"messageId", req.GetMessageId(),
					"took", time.Since(begin),
					"err", err,
				)
			}
		}(time.Now())
		return svc.SendMessageHeartBeat(ctx, req)
	}
}

func MakeListQueuesEndpoint(svc chronoqueue.Service, logger *log.Logger) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.ListQueuesRequest)
		defer func(begin time.Time) {
			if err != nil {
				logger.ErrorWithFields(
					"Failed to list queues",
					"method", "listQueues",
					"prefix", req.GetPrefix(),
					"took", time.Since(begin),
					"err", err,
				)
			} else {
				logger.InfoWithFields(
					"Queues listed successfully",
					"method", "listQueues",
					"prefix", req.GetPrefix(),
					"took", time.Since(begin),
					"err", err,
				)
			}
		}(time.Now())
		return svc.ListQueues(ctx, req)
	}
}

func MakeCreateScheduleEndpoint(svc chronoqueue.Service, logger *log.Logger) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.CreateScheduleRequest)
		defer func(begin time.Time) {
			if err != nil {
				logger.ErrorWithFields(
					"Failed to create schedule",
					"method", "createSchedule",
					"scheduleId", req.Schedule.GetScheduleId(),
					"cronSchedule", req.Schedule.Metadata.GetCronSchedule(),
					"took", time.Since(begin),
					"err", err,
				)
			} else {
				logger.InfoWithFields(
					"Schedule created successfully",
					"method", "createSchedule",
					"scheduleId", req.Schedule.GetScheduleId(),
					"cronSchedule", req.Schedule.Metadata.GetCronSchedule(),
					"took", time.Since(begin),
					"err", err,
				)
			}
		}(time.Now())
		return svc.CreateSchedule(ctx, req)
	}
}

func MakeDeleteScheduleEndpoint(svc chronoqueue.Service, logger *log.Logger) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.DeleteScheduleRequest)
		defer func(begin time.Time) {
			if err != nil {
				logger.ErrorWithFields(
					"Failed to delete schedule",
					"method", "deleteSchedule",
					"scheduleId", req.GetScheduleId(),
					"took", time.Since(begin),
					"err", err,
				)
			} else {
				logger.InfoWithFields(
					"Schedule deleted successfully",
					"method", "deleteSchedule",
					"scheduleId", req.GetScheduleId(),
					"took", time.Since(begin),
					"err", err,
				)
			}
		}(time.Now())
		return svc.DeleteSchedule(ctx, req)
	}
}

func MakeGetScheduleEndpoint(svc chronoqueue.Service, logger *log.Logger) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.GetScheduleRequest)
		defer func(begin time.Time) {
			if err != nil {
				logger.ErrorWithFields(
					"Failed to retrieve schedule",
					"method", "getSchedule",
					"scheduleId", req.GetScheduleId(),
					"took", time.Since(begin),
					"err", err,
				)
			} else {
				logger.InfoWithFields(
					"Schedule retrieved successfully",
					"method", "getSchedule",
					"scheduleId", req.GetScheduleId(),
					"took", time.Since(begin),
					"err", err,
				)
			}
		}(time.Now())
		return svc.GetSchedule(ctx, req)
	}
}

func MakeListSchedulesEndpoint(svc chronoqueue.Service, logger *log.Logger) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.ListSchedulesRequest)
		defer func(begin time.Time) {
			if err != nil {
				logger.ErrorWithFields(
					"Failed to list schedules",
					"method", "listSchedules",
					"prefix", req.GetPrefix(),
					"took", time.Since(begin),
					"err", err,
				)
			} else {
				logger.InfoWithFields(
					"Schedules listed successfully",
					"method", "listSchedules",
					"prefix", req.GetPrefix(),
					"took", time.Since(begin),
					"err", err,
				)
			}
		}(time.Now())
		return svc.ListSchedules(ctx, req)
	}
}

func MakeGetScheduleHistoryEndpoint(svc chronoqueue.Service, logger *log.Logger) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.GetScheduleHistoryRequest)
		defer func(begin time.Time) {
			if err != nil {
				logger.ErrorWithFields(
					"Failed to retrieve schedule history",
					"method", "getScheduleHistory",
					"scheduleId", req.GetScheduleId(),
					"limit", req.GetLimit(),
					"took", time.Since(begin),
					"err", err,
				)
			} else {
				logger.InfoWithFields(
					"Schedule history retrieved successfully",
					"method", "getScheduleHistory",
					"scheduleId", req.GetScheduleId(),
					"limit", req.GetLimit(),
					"took", time.Since(begin),
					"err", err,
				)
			}
		}(time.Now())
		return svc.GetScheduleHistory(ctx, req)
	}
}

func MakePauseScheduleEndpoint(svc chronoqueue.Service, logger *log.Logger) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.PauseScheduleRequest)
		defer func(begin time.Time) {
			if err != nil {
				logger.ErrorWithFields(
					"Failed to pause schedule",
					"method", "pauseSchedule",
					"scheduleId", req.GetScheduleId(),
					"took", time.Since(begin),
					"err", err,
				)
			} else {
				logger.InfoWithFields(
					"Schedule paused successfully",
					"method", "pauseSchedule",
					"scheduleId", req.GetScheduleId(),
					"took", time.Since(begin),
					"err", err,
				)
			}
		}(time.Now())
		return svc.PauseSchedule(ctx, req)
	}
}

func MakeResumeScheduleEndpoint(svc chronoqueue.Service, logger *log.Logger) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*queueservice_pb.ResumeScheduleRequest)
		defer func(begin time.Time) {
			if err != nil {
				logger.ErrorWithFields(
					"Failed to resume schedule",
					"method", "resumeSchedule",
					"scheduleId", req.GetScheduleId(),
					"took", time.Since(begin),
					"err", err,
				)
			} else {
				logger.InfoWithFields(
					"Schedule resumed successfully",
					"method", "resumeSchedule",
					"scheduleId", req.GetScheduleId(),
					"took", time.Since(begin),
					"err", err,
				)
			}
		}(time.Now())
		return svc.ResumeSchedule(ctx, req)
	}
}

func (s *Set) CreateQueue(ctx context.Context, request *queueservice_pb.CreateQueueRequest) (*queueservice_pb.CreateQueueResponse, error) {
	resp, err := s.CreateQueueEndpoint(ctx, request)
	if err != nil {
		return resp.(*queueservice_pb.CreateQueueResponse), err
	}
	return resp.(*queueservice_pb.CreateQueueResponse), err
}

func (s *Set) DeleteQueue(ctx context.Context, request *queueservice_pb.DeleteQueueRequest) (*queueservice_pb.DeleteQueueResponse, error) {
	resp, err := s.DeleteQueueEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.DeleteQueueResponse{Success: false}, err
	}
	return resp.(*queueservice_pb.DeleteQueueResponse), err
}

func (s *Set) PostMessage(ctx context.Context, request *queueservice_pb.PostMessageRequest) (*queueservice_pb.PostMessageResponse, error) {
	resp, err := s.PostMessageEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.PostMessageResponse{Success: false}, err
	}
	return resp.(*queueservice_pb.PostMessageResponse), err
}

func (s *Set) GetNextMessage(ctx context.Context, request *queueservice_pb.GetNextMessageRequest) (*queueservice_pb.GetNextMessageResponse, error) {
	resp, err := s.GetNextMessageEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.GetNextMessageResponse{
			Message: nil,
		}, err
	}
	messageResp := resp.(*queueservice_pb.GetNextMessageResponse)
	return messageResp, nil

}

func (s *Set) AcknowledgeMessage(ctx context.Context, request *queueservice_pb.AcknowledgeMessageRequest) (*queueservice_pb.AcknowledgeMessageResponse, error) {
	resp, err := s.AcknowledgeMessageEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.AcknowledgeMessageResponse{Success: false}, err
	}
	ackResp := resp.(*queueservice_pb.AcknowledgeMessageResponse)
	return ackResp, nil
}

func (s *Set) RenewMessageLease(ctx context.Context, request *queueservice_pb.RenewMessageLeaseRequest) (*queueservice_pb.RenewMessageLeaseResponse, error) {
	resp, err := s.RenewMessageLeaseEndpoint(ctx, request)
	if err != nil || resp == nil {
		return nil, err
	}
	renewLeaseResp := resp.(*queueservice_pb.RenewMessageLeaseResponse)
	return renewLeaseResp, nil
}

func (s *Set) PeekQueueMessages(ctx context.Context, queueName string) (*queueservice_pb.PeekQueueMessagesResponse, error) {
	resp, err := s.PeekQueueMessagesEndpoint(ctx, queueName)
	if err != nil {
		return &queueservice_pb.PeekQueueMessagesResponse{}, err
	}
	messagesResp := resp.(*queueservice_pb.PeekQueueMessagesResponse)
	return messagesResp, nil

}

func (s *Set) GetQueueState(ctx context.Context, queueName string) (*queueservice_pb.GetQueueStateResponse, error) {
	resp, err := s.GetQueueStateEndpoint(ctx, queueName)
	if err != nil {
		return &queueservice_pb.GetQueueStateResponse{}, err
	}
	stateResp := resp.(*queueservice_pb.GetQueueStateResponse)

	return stateResp, nil

}

func (s *Set) SendMessageHeartBeat(ctx context.Context, queueName string) (*queueservice_pb.SendMessageHeartBeatResponse, error) {
	resp, err := s.GetQueueStateEndpoint(ctx, queueName)
	if err != nil {
		return &queueservice_pb.SendMessageHeartBeatResponse{}, err
	}
	stateResp := resp.(*queueservice_pb.SendMessageHeartBeatResponse)

	return stateResp, nil

}

func (s *Set) ListQueues(ctx context.Context, request *queueservice_pb.ListQueuesRequest) (*queueservice_pb.ListQueuesResponse, error) {
	resp, err := s.ListQueuesEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.ListQueuesResponse{}, err
	}
	listResp := resp.(*queueservice_pb.ListQueuesResponse)
	return listResp, nil
}

func (s *Set) CreateSchedule(ctx context.Context, request *queueservice_pb.CreateScheduleRequest) (*queueservice_pb.CreateScheduleResponse, error) {
	resp, err := s.CreateScheduleEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.CreateScheduleResponse{Success: false}, err
	}
	createResp := resp.(*queueservice_pb.CreateScheduleResponse)
	return createResp, nil
}

func (s *Set) DeleteSchedule(ctx context.Context, request *queueservice_pb.DeleteScheduleRequest) (*queueservice_pb.DeleteScheduleResponse, error) {
	resp, err := s.DeleteScheduleEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.DeleteScheduleResponse{Success: false}, err
	}
	deleteResp := resp.(*queueservice_pb.DeleteScheduleResponse)
	return deleteResp, nil
}

func (s *Set) GetSchedule(ctx context.Context, request *queueservice_pb.GetScheduleRequest) (*queueservice_pb.GetScheduleResponse, error) {
	resp, err := s.GetScheduleEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.GetScheduleResponse{}, err
	}
	getResp := resp.(*queueservice_pb.GetScheduleResponse)
	return getResp, nil
}

func (s *Set) ListSchedules(ctx context.Context, request *queueservice_pb.ListSchedulesRequest) (*queueservice_pb.ListSchedulesResponse, error) {
	resp, err := s.ListSchedulesEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.ListSchedulesResponse{}, err
	}
	listResp := resp.(*queueservice_pb.ListSchedulesResponse)
	return listResp, nil
}

func (s *Set) GetScheduleHistory(ctx context.Context, request *queueservice_pb.GetScheduleHistoryRequest) (*queueservice_pb.GetScheduleHistoryResponse, error) {
	resp, err := s.GetScheduleHistoryEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.GetScheduleHistoryResponse{}, err
	}
	historyResp := resp.(*queueservice_pb.GetScheduleHistoryResponse)
	return historyResp, nil
}

func (s *Set) PauseSchedule(ctx context.Context, request *queueservice_pb.PauseScheduleRequest) (*queueservice_pb.PauseScheduleResponse, error) {
	resp, err := s.PauseScheduleEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.PauseScheduleResponse{Success: false}, err
	}
	pauseResp := resp.(*queueservice_pb.PauseScheduleResponse)
	return pauseResp, nil
}

func (s *Set) ResumeSchedule(ctx context.Context, request *queueservice_pb.ResumeScheduleRequest) (*queueservice_pb.ResumeScheduleResponse, error) {
	resp, err := s.ResumeScheduleEndpoint(ctx, request)
	if err != nil {
		return &queueservice_pb.ResumeScheduleResponse{Success: false}, err
	}
	resumeResp := resp.(*queueservice_pb.ResumeScheduleResponse)
	return resumeResp, nil
}
