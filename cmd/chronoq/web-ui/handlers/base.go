package handlers

import (
	"encoding/json"
	"html/template"
	"net/http"

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
	Retries    string
	RetriesInt int
	DLQ        string
	DLQInt     int
	Href       string
	IsDLQ      bool
}

// LeaseRow is the view model for an inflight message in the lease monitor.
type LeaseRow struct {
	MessageID string
	Queue     string
	Status    string
	Renewals  string
	Duration  string
	ExpiresIn string
}

// DayOption is used to build the days-of-week checkboxes in schedule_new.
type DayOption struct {
	Label string
	Value string
}

// BaseHandler provides common template rendering and navigation injection for all handlers.
type BaseHandler struct {
	templates *template.Template
	store     *clusterstore.Store
	logger    *log.Logger
}

// activeClient returns the gRPC client for the currently-active cluster.
func (h *BaseHandler) activeClient() *client.ChronoQueueClient {
	return h.store.ActiveClient()
}

// render executes the base layout with the given content template and data map.
func (h *BaseHandler) render(w http.ResponseWriter, contentTemplate string, data map[string]any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	h.injectBaseData(data)
	data["ContentTemplate"] = contentTemplate
	if err := h.templates.ExecuteTemplate(w, "base", data); err != nil {
		h.logger.ErrorWithFields("template execution failed", "error", err, "content", contentTemplate)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

// renderError writes a plain-text error response.
func (h *BaseHandler) renderError(w http.ResponseWriter, statusCode int, message string) {
	http.Error(w, message, statusCode)
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
		{Label: "Workers", Href: "/workers", Key: "workers"},
		{Label: "Lease monitor", Href: "/lease-monitor", Key: "lease-monitor"},
		{Label: "Schedules", Href: "/schedules", Key: "schedules"},
	}
}

// settingsNav returns the left-sidebar navigation items for the settings layout.
func settingsNav() []NavItem {
	return []NavItem{
		{Section: "Advanced", Label: "Clusters", Href: "/settings/clusters", Key: "clusters"},
		{Section: "Advanced", Label: "Partner Zones", Href: "/settings/partner-zones", Key: "partner-zones", Badge: "Preview"},
		{Section: "Advanced", Label: "Certificates", Href: "/settings/certificates", Key: "certificates"},
		{Section: "Advanced", Label: "Data Policies", Href: "/settings/policies", Key: "policies"},
		{Section: "Your Organization", Label: "Users", Href: "/settings/members", Key: "members"},
		{Section: "Your Organization", Label: "Groups", Href: "/settings/groups", Key: "groups"},
		{Section: "Your Organization", Label: "SSO", Href: "/settings/sso", Key: "sso"},
		{Section: "Your Organization", Label: "Audit Log", Href: "/settings/audit-log", Key: "audit-log"},
		{Section: "Your Organization", Label: "Integrations", Href: "/settings/integrations", Key: "integrations"},
		{Section: "Your Organization", Label: "Alerts", Href: "/settings/alerts", Key: "alerts"},
		{Section: "Your Organization", Label: "API Keys", Href: "/settings/public-api-keys", Key: "api-keys"},
		{Section: "Your Organization", Label: "Plan", Href: "/settings/plan", Key: "plan"},
		{Section: "Account", Label: "Profile", Href: "/settings/profile", Key: "profile"},
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
