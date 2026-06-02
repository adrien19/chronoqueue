package handlers

import (
	"html/template"
	"net/http"
	"strings"

	cluster "github.com/adrien19/chronoqueue/cmd/chronoq/web-ui/cluster"
	"github.com/adrien19/chronoqueue/pkg/log"
)

// SettingsHandler handles all settings sub-pages.
type SettingsHandler struct {
	BaseHandler
}

// NewSettingsHandler creates a SettingsHandler.
func NewSettingsHandler(
	templates *template.Template,
	store *cluster.Store,
	logger *log.Logger,
) *SettingsHandler {
	return &SettingsHandler{
		BaseHandler: BaseHandler{
			templates: templates,
			store:     store,
			logger:    logger,
		},
	}
}

func (h *SettingsHandler) settingsPage(w http.ResponseWriter, r *http.Request, active, pageTitle, contentTemplate string, extra map[string]any) {
	data := map[string]any{
		"PageTitle":      pageTitle,
		"SettingsActive": active,
	}
	for k, v := range extra {
		data[k] = v
	}
	h.render(w, contentTemplate, data)
}

// Clusters renders the cluster list.
func (h *SettingsHandler) Clusters(w http.ResponseWriter, r *http.Request) {
	h.settingsPage(w, r, "clusters", "Clusters", "settings_clusters_content", map[string]any{
		"Clusters": h.store.List(),
	})
}

// ClusterNew renders the add-cluster form.
func (h *SettingsHandler) ClusterNew(w http.ResponseWriter, r *http.Request) {
	h.settingsPage(w, r, "clusters", "Add Cluster", "settings_cluster_new_content", nil)
}

// ClusterDetail renders the edit form for a specific cluster.
func (h *SettingsHandler) ClusterDetail(w http.ResponseWriter, r *http.Request) {
	slug := r.PathValue("slug")
	c, ok := h.store.Get(slug)
	if !ok {
		http.NotFound(w, r)
		return
	}
	h.settingsPage(w, r, "clusters", "Edit: "+c.Name, "settings_cluster_detail_content", map[string]any{
		"Cluster": c,
	})
}

// ClusterCreate handles POST /api/clusters — adds a new cluster.
func (h *SettingsHandler) ClusterCreate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	c := cluster.Cluster{
		Name:          strings.TrimSpace(r.FormValue("name")),
		Description:   strings.TrimSpace(r.FormValue("description")),
		BrokerAddress: strings.TrimSpace(r.FormValue("brokerAddress")),
		SkipSSLCheck:  r.FormValue("skipSSLCheck") == "on",
	}
	if c.Name == "" || c.BrokerAddress == "" {
		h.settingsPage(w, r, "clusters", "Add Cluster", "settings_cluster_new_content", map[string]any{
			"Error": "Name and broker address are required",
			"Form":  c,
		})
		return
	}
	if err := h.store.Add(c); err != nil {
		h.settingsPage(w, r, "clusters", "Add Cluster", "settings_cluster_new_content", map[string]any{
			"Error": err.Error(),
			"Form":  c,
		})
		return
	}
	http.Redirect(w, r, "/settings/clusters", http.StatusSeeOther)
}

// ClusterUpdate handles POST /api/clusters/{slug} — saves edits to an existing cluster.
func (h *SettingsHandler) ClusterUpdate(w http.ResponseWriter, r *http.Request) {
	slug := r.PathValue("slug")
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	updated := cluster.Cluster{
		Name:          strings.TrimSpace(r.FormValue("name")),
		Description:   strings.TrimSpace(r.FormValue("description")),
		BrokerAddress: strings.TrimSpace(r.FormValue("brokerAddress")),
		SkipSSLCheck:  r.FormValue("skipSSLCheck") == "on",
	}
	if err := h.store.Update(slug, updated); err != nil {
		c, _ := h.store.Get(slug)
		h.settingsPage(w, r, "clusters", "Edit Cluster", "settings_cluster_detail_content", map[string]any{
			"Cluster": c,
			"Error":   err.Error(),
		})
		return
	}
	http.Redirect(w, r, "/settings/clusters", http.StatusSeeOther)
}

// ClusterDelete handles POST /api/clusters/{slug}/delete — removes a cluster.
func (h *SettingsHandler) ClusterDelete(w http.ResponseWriter, r *http.Request) {
	slug := r.PathValue("slug")
	if err := h.store.Delete(slug); err != nil {
		h.logger.ErrorWithFields("Failed to delete cluster", "error", err, "slug", slug)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	http.Redirect(w, r, "/settings/clusters", http.StatusSeeOther)
}

// ClusterSwitch handles POST /api/clusters/{slug}/switch — changes the active cluster.
func (h *SettingsHandler) ClusterSwitch(w http.ResponseWriter, r *http.Request) {
	slug := r.PathValue("slug")
	if err := h.store.SetActive(slug); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// Members renders the members settings page (stub).
func (h *SettingsHandler) Members(w http.ResponseWriter, r *http.Request) {
	h.settingsPage(w, r, "members", "Members", "settings_members_content", nil)
}

// MemberDetail renders the detail of a member (stub).
func (h *SettingsHandler) MemberDetail(w http.ResponseWriter, r *http.Request) {
	slug := r.PathValue("slug")
	h.settingsPage(w, r, "members", "Member: "+slug, "settings_member_detail_content", map[string]any{"Slug": slug})
}

// Groups renders the groups settings page (stub).
func (h *SettingsHandler) Groups(w http.ResponseWriter, r *http.Request) {
	h.settingsPage(w, r, "groups", "Groups", "settings_groups_content", nil)
}

// GroupDetail renders the detail of a group (stub).
func (h *SettingsHandler) GroupDetail(w http.ResponseWriter, r *http.Request) {
	slug := r.PathValue("slug")
	h.settingsPage(w, r, "groups", "Group: "+slug, "settings_group_detail_content", map[string]any{"Slug": slug})
}

// SSO renders the SSO settings page (stub).
func (h *SettingsHandler) SSO(w http.ResponseWriter, r *http.Request) {
	h.settingsPage(w, r, "sso", "SSO", "settings_sso_content", nil)
}

// AuditLog renders the audit log page (stub).
func (h *SettingsHandler) AuditLog(w http.ResponseWriter, r *http.Request) {
	h.settingsPage(w, r, "audit-log", "Audit Log", "settings_audit_log_content", nil)
}

// Integrations renders the integrations settings page (stub).
func (h *SettingsHandler) Integrations(w http.ResponseWriter, r *http.Request) {
	h.settingsPage(w, r, "integrations", "Integrations", "settings_integrations_content", nil)
}

// APIKeys renders the API keys settings page (stub).
func (h *SettingsHandler) APIKeys(w http.ResponseWriter, r *http.Request) {
	h.settingsPage(w, r, "api-keys", "API Keys", "settings_api_keys_content", nil)
}

// Profile renders the profile settings page (stub).
func (h *SettingsHandler) Profile(w http.ResponseWriter, r *http.Request) {
	h.settingsPage(w, r, "profile", "Profile", "settings_profile_content", nil)
}

// Placeholder renders a generic placeholder for unimplemented settings pages.
func (h *SettingsHandler) Placeholder(w http.ResponseWriter, r *http.Request) {
	h.settingsPage(w, r, "", "Settings", "settings_placeholder_content", nil)
}
