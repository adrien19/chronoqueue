package transport

import (
	"context"
	"encoding/json"
	"net/http"
	"os"

	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue/endpoints"

	httptransport "github.com/go-kit/kit/transport/http"
	"github.com/go-kit/log"
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
	var req endpoints.CreateQueueRequest
	if r.ContentLength == 0 {
		logger.Log("Get request with no body")
		return req, nil
	}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func decodeHTTPDeleteQueueRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	var req string
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func decodeHTTPPostMessageRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req endpoints.PostMessageRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func decodeHTTPGetNextMessageRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req endpoints.GetNextMessageRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func decodeHTTPAcknowledgeMessageRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req endpoints.AcknowledgeMessageRequest
	if r.ContentLength == 0 {
		logger.Log("Get request with no body")
		return req, nil
	}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func decodeHTTPRenewMessageLeaseRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	var req endpoints.RenewMessageLeaseRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func decodeHTTPGetPeekQueueMessagesRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req endpoints.PeekQueueMessagesRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func decodeHTTPGetQueueStateRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req endpoints.GetQueueStateRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func encodeResponse(ctx context.Context, w http.ResponseWriter, response interface{}) error {
	if e, ok := response.(error); ok && e != nil {
		encodeError(ctx, e, w)
		return nil
	}
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

var logger log.Logger

func init() {
	logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
}
