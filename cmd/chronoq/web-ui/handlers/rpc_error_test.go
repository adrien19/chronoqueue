package handlers

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	queueservicepb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/client"
	clusterstore "github.com/adrien19/chronoqueue/cmd/chronoq/web-ui/cluster"
	"github.com/adrien19/chronoqueue/pkg/log"
)

func TestMapRPCError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "invalid argument", err: status.Error(codes.InvalidArgument, "internal detail"), want: http.StatusBadRequest},
		{name: "out of range", err: status.Error(codes.OutOfRange, "internal detail"), want: http.StatusBadRequest},
		{name: "not found", err: status.Error(codes.NotFound, "internal detail"), want: http.StatusNotFound},
		{name: "already exists", err: status.Error(codes.AlreadyExists, "internal detail"), want: http.StatusConflict},
		{name: "aborted", err: status.Error(codes.Aborted, "internal detail"), want: http.StatusConflict},
		{name: "failed precondition", err: status.Error(codes.FailedPrecondition, "internal detail"), want: http.StatusConflict},
		{name: "unauthenticated", err: status.Error(codes.Unauthenticated, "internal detail"), want: http.StatusUnauthorized},
		{name: "permission denied", err: status.Error(codes.PermissionDenied, "internal detail"), want: http.StatusForbidden},
		{name: "resource exhausted", err: status.Error(codes.ResourceExhausted, "internal detail"), want: http.StatusTooManyRequests},
		{name: "deadline", err: context.DeadlineExceeded, want: http.StatusGatewayTimeout},
		{name: "gRPC deadline", err: status.Error(codes.DeadlineExceeded, "internal detail"), want: http.StatusGatewayTimeout},
		{name: "canceled", err: context.Canceled, want: http.StatusRequestTimeout},
		{name: "gRPC canceled", err: status.Error(codes.Canceled, "internal detail"), want: http.StatusRequestTimeout},
		{name: "unavailable", err: status.Error(codes.Unavailable, "internal detail"), want: http.StatusServiceUnavailable},
		{name: "wrapped unavailable", err: fmt.Errorf("operation failed: %w", status.Error(codes.Unavailable, "internal detail")), want: http.StatusServiceUnavailable},
		{name: "internal", err: status.Error(codes.Internal, "secret backend detail"), want: http.StatusInternalServerError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := mapRPCError(test.err)
			if got.statusCode != test.want {
				t.Fatalf("status = %d, want %d", got.statusCode, test.want)
			}
			if strings.Contains(got.message, "internal detail") || strings.Contains(got.message, "secret backend detail") {
				t.Fatalf("public message leaked internal error: %q", got.message)
			}
		})
	}
}

func TestWriteRPCErrorResponses(t *testing.T) {
	h := testFormErrorBaseHandler(t)
	err := status.Error(codes.NotFound, "database key leaked")

	t.Run("full page", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		h.writeRPCError(recorder, httptest.NewRequest(http.MethodGet, "/queues/missing", nil), "load queue", err)
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
		}
		if strings.Contains(recorder.Body.String(), "database key leaked") || !strings.Contains(recorder.Body.String(), "not found") {
			t.Fatalf("unexpected response body: %q", recorder.Body.String())
		}
	})

	t.Run("HTMX fragment", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/api/queues", nil)
		request.Header.Set("HX-Request", "true")
		h.writeRPCError(recorder, request, "load queue", err)
		if recorder.Code != http.StatusOK || !strings.Contains(recorder.Header().Get("Content-Type"), "text/html") {
			t.Fatalf("fragment response = (%d, %q)", recorder.Code, recorder.Header().Get("Content-Type"))
		}
		if recorder.Header().Get("HX-Reswap") != "innerHTML" {
			t.Fatalf("HX-Reswap = %q, want innerHTML", recorder.Header().Get("HX-Reswap"))
		}
		if strings.Contains(recorder.Body.String(), "database key leaked") {
			t.Fatalf("fragment leaked internal error: %q", recorder.Body.String())
		}
	})
}

func TestRenderHandlesTemplateAndWriteErrors(t *testing.T) {
	store := clusterstore.NewStore("")
	store.Seed("Local", "localhost:9000", true)

	t.Run("template error", func(t *testing.T) {
		h := BaseHandler{templates: template.Must(template.New("test").Parse(`{{ define "base" }}{{ call .Missing }}{{ end }}`)), store: store, logger: log.NewLogger()}
		recorder := httptest.NewRecorder()
		h.render(recorder, "test", map[string]any{})
		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
		}
	})

	t.Run("write error", func(t *testing.T) {
		h := BaseHandler{templates: template.Must(template.New("test").Parse(`{{ define "base" }}ok{{ end }}`)), store: store, logger: log.NewLogger()}
		writer := &errorResponseWriter{header: make(http.Header)}
		h.render(writer, "test", map[string]any{})
		if !errors.Is(writer.err, errTestWrite) {
			t.Fatalf("write error = %v", writer.err)
		}
	})

	t.Run("fragment template error", func(t *testing.T) {
		h := BaseHandler{templates: template.Must(template.New("test").Parse(`{{ define "fragment" }}{{ call .Missing }}{{ end }}`)), store: store, logger: log.NewLogger()}
		recorder := httptest.NewRecorder()
		h.renderFragment(recorder, "fragment", map[string]any{})
		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
		}
	})

	t.Run("fragment success", func(t *testing.T) {
		h := BaseHandler{templates: template.Must(template.New("test").Parse(`{{ define "fragment" }}hello {{ .Name }}{{ end }}`)), store: store, logger: log.NewLogger()}
		recorder := httptest.NewRecorder()
		h.renderFragment(recorder, "fragment", map[string]any{"Name": "world"})
		if recorder.Code != http.StatusOK || recorder.Body.String() != "hello world" {
			t.Fatalf("fragment response = (%d, %q)", recorder.Code, recorder.Body.String())
		}
		if got := recorder.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
			t.Fatalf("Content-Type = %q", got)
		}
	})

	t.Run("fragment write error", func(t *testing.T) {
		h := BaseHandler{templates: template.Must(template.New("test").Parse(`{{ define "fragment" }}ok{{ end }}`)), store: store, logger: log.NewLogger()}
		writer := &errorResponseWriter{header: make(http.Header)}
		h.renderFragment(writer, "fragment", nil)
		if !errors.Is(writer.err, errTestWrite) {
			t.Fatalf("write error = %v", writer.err)
		}
	})
}

func TestLiveOverviewSSE(t *testing.T) {
	tests := []struct {
		name       string
		listResult *queueservicepb.ListQueuesResponse
		listErr    error
		wantBody   string
		dontWant   string
	}{
		{name: "success", listResult: &queueservicepb.ListQueuesResponse{}, wantBody: "No active queues"},
		{name: "list queues failure", listErr: status.Error(codes.Unavailable, "private backend detail"), wantBody: "ChronoQueue is unavailable", dontWant: "private backend detail"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queueService := &listQueuesServiceClient{response: test.listResult, err: test.listErr}
			chronoClient, err := client.NewChronoQueueClient("unused", client.ClientOptions{
				MaxHeartBeatWorkers: 1,
				Connector: func(string, client.ClientOptions) (queueservicepb.QueueServiceClient, *grpc.ClientConn, error) {
					return queueService, nil, nil
				},
			})
			if err != nil {
				t.Fatalf("create client: %v", err)
			}

			templates := template.Must(template.New("test").Parse(`{{ define "live_overview" }}{{ if not .QueueSummary }}No active queues{{ end }}{{ end }}`))
			handler := &DashboardHandler{BaseHandler: BaseHandler{
				templates: templates,
				store:     clusterstore.NewStore(""),
				logger:    log.NewLogger(),
				clientProvider: func() (*client.ChronoQueueClient, error) {
					return chronoClient, nil
				},
			}}
			recorder := httptest.NewRecorder()
			handler.LiveOverview(recorder, httptest.NewRequest(http.MethodGet, "/fragments/live-overview", nil))

			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
			}
			if got := recorder.Header().Get("Content-Type"); got != "text/event-stream" {
				t.Fatalf("Content-Type = %q", got)
			}
			body := recorder.Body.String()
			if !strings.Contains(body, "event: overview\n") || !strings.Contains(body, test.wantBody) {
				t.Fatalf("SSE body = %q", body)
			}
			if test.dontWant != "" && strings.Contains(body, test.dontWant) {
				t.Fatalf("SSE body leaked internal error: %q", body)
			}
		})
	}
}

type listQueuesServiceClient struct {
	queueservicepb.QueueServiceClient
	response *queueservicepb.ListQueuesResponse
	err      error
}

func (c *listQueuesServiceClient) ListQueues(context.Context, *queueservicepb.ListQueuesRequest, ...grpc.CallOption) (*queueservicepb.ListQueuesResponse, error) {
	return c.response, c.err
}

var errTestWrite = errors.New("test write failure")

type errorResponseWriter struct {
	header http.Header
	err    error
}

func (w *errorResponseWriter) Header() http.Header { return w.header }
func (w *errorResponseWriter) WriteHeader(int)     {}
func (w *errorResponseWriter) Write([]byte) (int, error) {
	w.err = errTestWrite
	return 0, w.err
}
