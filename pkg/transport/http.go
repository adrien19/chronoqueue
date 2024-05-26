package transport

import (
	"context"
	"encoding/json"
	"net/http"

	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/adrien19/chronoqueue/pkg/endpoints"

	httptransport "github.com/go-kit/kit/transport/http"
	// "github.com/go-kit/log"
	log "github.com/adrien19/chronoqueue/pkg/log"
)

func NewHTTPHandler(ep endpoints.Set, logger *log.Logger) http.Handler {
	// opts := []httptransport.ServerOption{
	// 	httptransport.ServerErrorHandler(transport.NewLogErrorHandler(logger)),
	// 	httptransport.ServerErrorEncoder(encodeError),
	// }
	m := http.NewServeMux()

	m.Handle("/queue/createQueue", httptransport.NewServer(
		ep.CreateQueueEndpoint,
		decodeHTTPCreateQueueRequest,
		encodeResponse,
		// opts...,
	))
	m.Handle("/queue/deleteQueue", httptransport.NewServer(
		ep.DeleteQueueEndpoint,
		decodeHTTPDeleteQueueRequest,
		encodeResponse,
		// opts...,
	))
	m.Handle("/queue/postMessage", httptransport.NewServer(
		ep.PostMessageEndpoint,
		decodeHTTPPostMessageRequest,
		encodeResponse,
		// opts...,
	))
	m.Handle("/queue/getNextMessage", httptransport.NewServer(
		ep.GetNextMessageEndpoint,
		decodeHTTPGetNextMessageRequest,
		encodeResponse,
		// opts...,
	))
	m.Handle("/queue/acknowledgeMessage", httptransport.NewServer(
		ep.AcknowledgeMessageEndpoint,
		decodeHTTPAcknowledgeMessageRequest,
		encodeResponse,
		// opts...,
	))
	m.Handle("/queue/renewMessageLease", httptransport.NewServer(
		ep.RenewMessageLeaseEndpoint,
		decodeHTTPRenewMessageLeaseRequest,
		encodeResponse,
		// opts...,
	))
	m.Handle("/queue/peekQueueMessages", httptransport.NewServer(
		ep.PeekQueueMessagesEndpoint,
		decodeHTTPGetPeekQueueMessagesRequest,
		encodeResponse,
		// opts...,
	))
	m.Handle("/queue/getQueueState", httptransport.NewServer(
		ep.GetQueueStateEndpoint,
		decodeHTTPGetQueueStateRequest,
		encodeResponse,
		// opts...,
	))
	m.Handle("/queue/sendMessageHeartBeat", httptransport.NewServer(
		ep.SendMessageHeartBeatEndpoint,
		decodeHTTPSendMessageHeartBeatRequest,
		encodeResponse,
		// opts...,
	))
	m.Handle("/queue/listQueues", httptransport.NewServer(
		ep.ListQueuesEndpoint,
		decodeHTTPListQueuesRequest,
		encodeResponse,
		// opts...,
	))
	m.Handle("/schedule/createSchedule", httptransport.NewServer(
		ep.CreateScheduleEndpoint,
		decodeHTTPCreateScheduleRequest,
		encodeResponse,
		// opts...,
	))
	m.Handle("/schedule/deleteSchedule", httptransport.NewServer(
		ep.DeleteScheduleEndpoint,
		decodeHTTPDeleteScheduleRequest,
		encodeResponse,
		// opts...,
	))
	m.Handle("/schedule/getSchedule", httptransport.NewServer(
		ep.GetScheduleEndpoint,
		decodeHTTPGetScheduleRequest,
		encodeResponse,
		// opts...,
	))
	m.Handle("/schedule/listSchedules", httptransport.NewServer(
		ep.ListSchedulesEndpoint,
		decodeHTTPListSchedulesRequest,
		encodeResponse,
		// opts...,
	))
	m.Handle("/schedule/getScheduleHistory", httptransport.NewServer(
		ep.GetScheduleHistoryEndpoint,
		decodeHTTPGetScheduleHistoryRequest,
		encodeResponse,
		// opts...,
	))
	m.Handle("/schedule/pauseSchedule", httptransport.NewServer(
		ep.PauseScheduleEndpoint,
		decodeHTTPPauseScheduleRequest,
		encodeResponse,
		// opts...,
	))
	m.Handle("/schedule/resumeSchedule", httptransport.NewServer(
		ep.ResumeScheduleEndpoint,
		decodeHTTPResumeScheduleRequest,
		encodeResponse,
		// opts...,
	))

	return m
}

func decodeHTTPCreateQueueRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req queueservice_pb.CreateQueueRequest
	if r.ContentLength == 0 {
		return &req, nil
	}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeHTTPDeleteQueueRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	var req queueservice_pb.DeleteQueueRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return &queueservice_pb.DeleteQueueResponse{Success: false}, err
	}
	return &req, nil
}

func decodeHTTPPostMessageRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req queueservice_pb.PostMessageRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return &queueservice_pb.PostMessageResponse{Success: false}, err
	}
	// Validate the size of a message based on simple estimations.
	err = util.ValidateMessageSize(req.Message)
	if err != nil {
		return &queueservice_pb.PostMessageResponse{Success: false}, err
	}
	return &req, nil
}

func decodeHTTPGetNextMessageRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req queueservice_pb.GetNextMessageRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeHTTPAcknowledgeMessageRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req queueservice_pb.AcknowledgeMessageRequest
	if r.ContentLength == 0 {
		return &req, nil
	}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return &queueservice_pb.AcknowledgeMessageResponse{Success: false}, err
	}
	return &req, nil
}

func decodeHTTPRenewMessageLeaseRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	var req queueservice_pb.RenewMessageLeaseRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeHTTPGetPeekQueueMessagesRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req queueservice_pb.PeekQueueMessagesRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeHTTPGetQueueStateRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req queueservice_pb.GetQueueStateRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeHTTPSendMessageHeartBeatRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req queueservice_pb.SendMessageHeartBeatRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeHTTPListQueuesRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req queueservice_pb.ListQueuesRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeHTTPCreateScheduleRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req queueservice_pb.CreateScheduleRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeHTTPDeleteScheduleRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req queueservice_pb.DeleteScheduleRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return &queueservice_pb.DeleteScheduleResponse{Success: false}, err
	}
	return &req, nil
}

func decodeHTTPGetScheduleRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req queueservice_pb.GetScheduleRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeHTTPListSchedulesRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req queueservice_pb.ListSchedulesRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeHTTPGetScheduleHistoryRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req queueservice_pb.GetScheduleHistoryRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeHTTPPauseScheduleRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req queueservice_pb.PauseScheduleRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return &queueservice_pb.PauseScheduleResponse{Success: false}, err
	}
	return &req, nil
}

func decodeHTTPResumeScheduleRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req queueservice_pb.ResumeScheduleRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return &queueservice_pb.ResumeScheduleResponse{Success: false}, err
	}
	return &req, nil
}

func encodeResponse(ctx context.Context, w http.ResponseWriter, response interface{}) error {
	if e, ok := response.(error); ok && e != nil {
		encodeError(ctx, e, w)
		return nil
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	return json.NewEncoder(w).Encode(response)
}

func encodeError(_ context.Context, err error, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	switch err {
	case util.ErrUnknown:
		w.WriteHeader(http.StatusNotFound)
	case util.ErrInvalidArgument:
		w.WriteHeader(http.StatusBadRequest)
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error": err.Error(),
	})
}
