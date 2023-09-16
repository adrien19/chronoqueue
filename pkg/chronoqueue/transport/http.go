package transport

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue/endpoints"

	httptransport "github.com/go-kit/kit/transport/http"
)

func NewHTTPHandler(ep endpoints.Set) http.Handler {
	m := http.NewServeMux()

	m.Handle("/queue/createQueue", httptransport.NewServer(
		ep.CreateQueueEndpoint,
		decodeHTTPCreateQueueRequest,
		encodeResponse,
	))
	m.Handle("/queue/deleteQueue", httptransport.NewServer(
		ep.DeleteQueueEndpoint,
		decodeHTTPDeleteQueueRequest,
		encodeResponse,
	))
	m.Handle("/queue/postMessage", httptransport.NewServer(
		ep.PostMessageEndpoint,
		decodeHTTPPostMessageRequest,
		encodeResponse,
	))
	m.Handle("/queue/getNextMessage", httptransport.NewServer(
		ep.GetNextMessageEndpoint,
		decodeHTTPGetNextMessageRequest,
		encodeResponse,
	))
	m.Handle("/queue/acknowledgeMessage", httptransport.NewServer(
		ep.AcknowledgeMessageEndpoint,
		decodeHTTPAcknowledgeMessageRequest,
		encodeResponse,
	))
	m.Handle("/queue/renewMessageLease", httptransport.NewServer(
		ep.RenewMessageLeaseEndpoint,
		decodeHTTPRenewMessageLeaseRequest,
		encodeResponse,
	))
	m.Handle("/queue/peekQueueMessages", httptransport.NewServer(
		ep.PeekQueueMessagesEndpoint,
		decodeHTTPGetPeekQueueMessagesRequest,
		encodeResponse,
	))
	m.Handle("/queue/getQueueState", httptransport.NewServer(
		ep.GetQueueStateEndpoint,
		decodeHTTPGetQueueStateRequest,
		encodeResponse,
	))

	return m
}

func decodeHTTPCreateQueueRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req chronoqueue.CreateQueueRequest
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
	var req chronoqueue.DeleteQueueRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeHTTPPostMessageRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req chronoqueue.PostMessageRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeHTTPGetNextMessageRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req chronoqueue.GetNextMessageRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeHTTPAcknowledgeMessageRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req chronoqueue.AcknowledgeMessageRequest
	if r.ContentLength == 0 {
		return &req, nil
	}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeHTTPRenewMessageLeaseRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	var req chronoqueue.RenewMessageLeaseRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeHTTPGetPeekQueueMessagesRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req chronoqueue.PeekQueueMessagesRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeHTTPGetQueueStateRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req chronoqueue.GetQueueStateRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func encodeResponse(ctx context.Context, w http.ResponseWriter, response interface{}) error {
	if e, ok := response.(error); ok && e != nil {
		return encodeError(ctx, e, w)
	}
	return json.NewEncoder(w).Encode(response)
}

func encodeError(_ context.Context, err error, w http.ResponseWriter) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	switch err {
	case util.ErrUnknown:
		w.WriteHeader(http.StatusNotFound)
	case util.ErrInvalidArgument:
		w.WriteHeader(http.StatusBadRequest)
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}
	return json.NewEncoder(w).Encode(map[string]interface{}{
		"error": err.Error(),
	})
}
