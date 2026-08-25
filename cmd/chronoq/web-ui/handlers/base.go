package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"html/template"
	"net/http"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/adrien19/chronoqueue/client"
	clusterstore "github.com/adrien19/chronoqueue/cmd/chronoq/web-ui/cluster"
	"github.com/adrien19/chronoqueue/pkg/log"
	"github.com/adrien19/chronoqueue/pkg/version"
)

// NavItem represents a navigation link in the sidebar.
type NavItem struct {
	Label   string
	Href    string
	Key     string
	Section string
	Badge   string
}

// QueueRow is the view model for a queue in the queue table.
type QueueRow struct {
	Name       string
	Ready      string
	InFlight   string
	Delayed    string
	Errored    string
	ErroredInt int
	DLQ        string
	DLQInt     int
	Href       string
	IsDLQ      bool
}

// LeaseRow is the view model for an inflight message in the lease monitor.
type LeaseRow struct {
	MessageID     string
	Queue         string
	Status        string
	Worker        string
	Renewals      string
	Duration      string
	ExpiresIn     string
	LastHeartbeat string
}

// DayOption is used to build the days-of-week checkboxes in schedule_new.
type DayOption struct {
	Label string
	Value string
}

// BaseHandler provides common template rendering and navigation injection for all handlers.
type BaseHandler struct {
	templates      *template.Template
	store          *clusterstore.Store
	logger         *log.Logger
	clientProvider func() (*client.ChronoQueueClient, error)
}

// activeClient returns the gRPC client for the currently-active cluster.
func (h *BaseHandler) activeClient() (*client.ChronoQueueClient, error) {
	if h.clientProvider != nil {
		return h.clientProvider()
	}
	return h.store.ActiveClient()
}

func (h *BaseHandler) requireActiveClient(w http.ResponseWriter) (*client.ChronoQueueClient, bool) {
	client, err := h.activeClient()
	if err == nil {
		return client, true
	}
	if h.logger != nil {
		h.logger.ErrorWithFields("Failed to acquire active cluster client", "error", err)
	}
	h.renderError(w, http.StatusServiceUnavailable, "ChronoQueue backend is unavailable")
	return nil, false
}

// render executes the base layout with the given content template and data map.
func (h *BaseHandler) render(w http.ResponseWriter, contentTemplate string, data map[string]any) {
	h.injectBaseData(data)
	data["ContentTemplate"] = contentTemplate
	var rendered bytes.Buffer
	if err := h.templates.ExecuteTemplate(&rendered, "base", data); err != nil {
		h.logger.ErrorWithFields("template execution failed", "error", err, "content", contentTemplate)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := w.Write(rendered.Bytes()); err != nil {
		h.logger.ErrorWithFields("failed to write response", "error", err, "content", contentTemplate)
	}
}

func (h *BaseHandler) renderFragment(w http.ResponseWriter, templateName string, data any) {
	var rendered bytes.Buffer
	if err := h.templates.ExecuteTemplate(&rendered, templateName, data); err != nil {
		h.logger.ErrorWithFields("fragment template execution failed", "error", err, "content", templateName)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := w.Write(rendered.Bytes()); err != nil {
		h.logger.ErrorWithFields("failed to write fragment response", "error", err, "content", templateName)
	}
}

// renderError writes an error response in the full UI shell when templates are available.
func (h *BaseHandler) renderError(w http.ResponseWriter, statusCode int, message string) {
	if h.templates == nil || h.store == nil {
		http.Error(w, message, statusCode)
		return
	}

	data := map[string]any{
		"PageTitle":    "Error",
		"ErrorCode":    statusCode,
		"ErrorTitle":   http.StatusText(statusCode),
		"ErrorMessage": message,
	}
	h.injectBaseData(data)
	data["ContentTemplate"] = "error_content"

	var rendered bytes.Buffer
	if err := h.templates.ExecuteTemplate(&rendered, "base", data); err != nil {
		if h.logger != nil {
			h.logger.ErrorWithFields("template execution failed", "error", err, "content", "error_content")
		}
		http.Error(w, message, statusCode)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(statusCode)
	if _, err := w.Write(rendered.Bytes()); err != nil {
		if h.logger != nil {
			h.logger.ErrorWithFields("failed to write error response", "error", err)
		}
	}
}

// injectBaseData adds fields that every page template requires.
func (h *BaseHandler) injectBaseData(data map[string]any) {
	active := h.store.ActiveCluster()
	if active != nil {
		data["EnvName"] = active.Name
		data["EnvSlug"] = active.Slug
		data["BrokerAddress"] = active.BrokerAddress
	} else {
		data["EnvName"] = "ChronoQueue"
		data["EnvSlug"] = "local"
		data["BrokerAddress"] = ""
	}
	data["Version"] = version.Short()
	data["Clusters"] = h.store.List()
	data["ActiveCluster"] = active
	if _, ok := data["Layout"]; !ok {
		data["Layout"] = "console"
	}
	if _, ok := data["Nav"]; !ok {
		data["Nav"] = consoleNav()
	}
	data["SettingsNav"] = settingsNav()
}

// consoleNav returns the left-sidebar navigation items for the console layout.
func consoleNav() []NavItem {
	return []NavItem{
		{Label: "Home", Href: "/", Key: "home"},
		{Label: "Queues", Href: "/queues", Key: "queues"},
		{Label: "Schemas", Href: "/schemas", Key: "schemas"},
		{Label: "Workers", Href: "/workers", Key: "workers"},
		{Label: "Lease monitor", Href: "/lease-monitor", Key: "lease-monitor"},
		{Label: "Schedules", Href: "/schedules", Key: "schedules"},
	}
}

// settingsNav returns the left-sidebar navigation items for the settings layout.
func settingsNav() []NavItem {
	return []NavItem{
		{Section: "Advanced", Label: "Clusters", Href: "/settings/clusters", Key: "clusters"},
	}
}

// StatusClass returns the cq-badge CSS classes for a given status string.
func StatusClass(s string) string {
	switch s {
	case "good", "COMPLETED":
		return "cq-badge cq-badge-good"
	case "warn", "PAUSED", "DELAYED":
		return "cq-badge cq-badge-warn"
	case "danger", "ERRORED", "FAILED":
		return "cq-badge border-red-500/25 bg-red-500/10 text-red-300"
	case "RUNNING", "PENDING", "SCHEDULED":
		return "cq-badge border-sky-500/25 bg-sky-500/10 text-sky-300"
	default:
		return "cq-badge cq-badge-muted"
	}
}

// StatusTone returns a text-color class for a status.
func StatusTone(status string) string {
	switch status {
	case "COMPLETED":
		return "text-emerald-300"
	case "ERRORED", "FAILED":
		return "text-red-300"
	case "RUNNING":
		return "text-sky-300"
	case "DELAYED", "PAUSED":
		return "text-amber-300"
	default:
		return "text-zinc-200"
	}
}

// ToJSON marshals v to a template.JS value for safe inline JS injection.
func ToJSON(v any) template.JS {
	b, err := json.Marshal(v)
	if err != nil {
		return template.JS("null")
	}
	return template.JS(b) //nolint:gosec // data is already marshalled JSON, not user HTML
}

func isHTMXRequest(r *http.Request) bool {
	return strings.EqualFold(strings.TrimSpace(r.Header.Get("HX-Request")), "true")
}

type rpcErrorResponse struct {
	statusCode int
	message    string
}

func mapRPCError(err error) rpcErrorResponse {
	if err == nil {
		return rpcErrorResponse{}
	}
	if errors.Is(err, context.Canceled) {
		return rpcErrorResponse{statusCode: http.StatusRequestTimeout, message: "The request was canceled"}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return rpcErrorResponse{statusCode: http.StatusGatewayTimeout, message: "ChronoQueue did not respond before the deadline"}
	}

	switch status.Code(err) {
	case codes.InvalidArgument, codes.OutOfRange:
		return rpcErrorResponse{statusCode: http.StatusBadRequest, message: "ChronoQueue rejected the request"}
	case codes.NotFound:
		return rpcErrorResponse{statusCode: http.StatusNotFound, message: "The requested ChronoQueue resource was not found"}
	case codes.AlreadyExists, codes.Aborted, codes.FailedPrecondition:
		return rpcErrorResponse{statusCode: http.StatusConflict, message: "The request conflicts with the current ChronoQueue state"}
	case codes.Unauthenticated:
		return rpcErrorResponse{statusCode: http.StatusUnauthorized, message: "ChronoQueue authentication failed"}
	case codes.PermissionDenied:
		return rpcErrorResponse{statusCode: http.StatusForbidden, message: "ChronoQueue denied this operation"}
	case codes.ResourceExhausted:
		return rpcErrorResponse{statusCode: http.StatusTooManyRequests, message: "ChronoQueue is temporarily rate limited or out of capacity"}
	case codes.Canceled:
		return rpcErrorResponse{statusCode: http.StatusRequestTimeout, message: "The request was canceled"}
	case codes.DeadlineExceeded:
		return rpcErrorResponse{statusCode: http.StatusGatewayTimeout, message: "ChronoQueue did not respond before the deadline"}
	case codes.Unavailable:
		return rpcErrorResponse{statusCode: http.StatusServiceUnavailable, message: "ChronoQueue is unavailable"}
	default:
		return rpcErrorResponse{statusCode: http.StatusInternalServerError, message: "ChronoQueue could not complete the request"}
	}
}

func (h *BaseHandler) writeRPCError(w http.ResponseWriter, r *http.Request, operation string, err error) {
	mapped := mapRPCError(err)
	h.logger.ErrorWithFields("ChronoQueue RPC failed", "error", err, "operation", operation, "status", mapped.statusCode)
	if !isHTMXRequest(r) {
		h.renderError(w, mapped.statusCode, mapped.message)
		return
	}
	h.writeInlineErrorFragment(w, mapped.message, true)
}

func (h *BaseHandler) writeInlineFormError(w http.ResponseWriter, r *http.Request, message string) {
	if !isHTMXRequest(r) {
		h.renderError(w, http.StatusBadRequest, message)
		return
	}
	h.writeInlineErrorFragment(w, message, false)
}

func (h *BaseHandler) writeInlineErrorFragment(w http.ResponseWriter, message string, reswap bool) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if reswap {
		w.Header().Set("HX-Reswap", "innerHTML")
	}
	// HTMX does not swap non-2xx responses by default.
	w.WriteHeader(http.StatusOK)
	escaped := html.EscapeString(message)
	escaped = strings.ReplaceAll(escaped, "\n", "<br>")
	if _, err := fmt.Fprintf(w, `<div class="rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-300">%s</div>`, escaped); err != nil {
		h.logger.ErrorWithFields("Failed to write inline error fragment", "error", err)
	}
}
