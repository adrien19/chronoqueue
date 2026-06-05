package webui

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	clusterstore "github.com/adrien19/chronoqueue/cmd/chronoq/web-ui/cluster"
	"github.com/adrien19/chronoqueue/cmd/chronoq/web-ui/handlers"
	"github.com/adrien19/chronoqueue/pkg/log"
)

//go:embed templates/* static/*
var content embed.FS

// UIServer serves the ChronoQueue web-UI.
type UIServer struct {
	templates *template.Template
	store     *clusterstore.Store
	logger    *log.Logger
	server    *http.Server
}

// NewUIServer creates a new UIServer, parses templates, and seeds the cluster store.
func NewUIServer(grpcAddr string, skipSSL bool, logger *log.Logger) (*UIServer, error) {
	tmpl := template.New("").Funcs(templateFuncs())

	tmpl, err := tmpl.ParseFS(
		content,
		"templates/layouts/*.gohtml",
		"templates/partials/*.gohtml",
		"templates/pages/*.gohtml",
	)
	if err != nil {
		return nil, fmt.Errorf("failed to parse templates: %w", err)
	}

	configDir, err := os.UserConfigDir()
	if err != nil {
		configDir = "."
	}
	store := clusterstore.NewStore(filepath.Join(configDir, "chronoqueue", "web-ui-clusters.json"))
	if err := store.Load(); err != nil {
		logger.WarnWithFields("Failed to load cluster store, starting fresh", "error", err)
	}
	store.Seed("Local", grpcAddr, skipSSL)

	return &UIServer{
		templates: tmpl,
		store:     store,
		logger:    logger,
	}, nil
}

// Start registers routes and starts the HTTP server.
func (s *UIServer) Start(addr string) error {
	mux := http.NewServeMux()

	// Static assets from the embedded filesystem
	staticFS, err := fs.Sub(content, "static")
	if err != nil {
		return fmt.Errorf("failed to create static sub-fs: %w", err)
	}
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))

	// Handlers
	dashboard := handlers.NewDashboardHandler(s.templates, s.store, s.logger)
	queues := handlers.NewQueuesHandler(s.templates, s.store, s.logger)
	workers := handlers.NewWorkersHandler(s.templates, s.store, s.logger)
	leaseMonitor := handlers.NewLeaseMonitorHandler(s.templates, s.store, s.logger)
	schedules := handlers.NewSchedulesHandler(s.templates, s.store, s.logger)
	settings := handlers.NewSettingsHandler(s.templates, s.store, s.logger)

	// Console pages
	mux.HandleFunc("GET /", dashboard.Index)
	mux.HandleFunc("GET /queues", queues.List)
	mux.HandleFunc("GET /queues/new", queues.New)
	mux.HandleFunc("GET /queues/{name}", queues.Detail)
	mux.HandleFunc("GET /queues/{name}/messages", queues.MessageDetail)
	mux.HandleFunc("GET /queues/{name}/messages/new", queues.NewMessage)
	mux.HandleFunc("POST /queues/{name}/requeue-all", queues.RequeueAll)
	mux.HandleFunc("POST /queues/{name}/purge", queues.Purge)
	mux.HandleFunc("POST /api/queues/{name}/messages/{messageId}/requeue", queues.RequeueMessage)
	mux.HandleFunc("POST /api/queues/{name}/messages/{messageId}/dlq-delete", queues.DeleteDLQMessage)
	mux.HandleFunc("GET /workers", workers.List)
	mux.HandleFunc("GET /lease-monitor", leaseMonitor.List)
	mux.HandleFunc("GET /schedules", schedules.List)
	mux.HandleFunc("GET /schedules/new", schedules.New)

	// HTMX / API endpoints
	mux.HandleFunc("POST /api/queues/create", queues.Create)
	mux.HandleFunc("POST /api/queues/{name}/messages", queues.PostMessage)
	mux.HandleFunc("POST /api/schedules/create", schedules.Create)
	mux.HandleFunc("POST /api/schedules/toggle", schedules.Toggle)
	mux.HandleFunc("DELETE /api/schedules/{id}", schedules.Delete)
	mux.HandleFunc("GET /fragments/live-overview", dashboard.LiveOverview)
	mux.HandleFunc("GET /fragments/dashboard-stats", dashboard.DashboardStats)
	mux.HandleFunc("GET /fragments/lease-table", leaseMonitor.Table)

	// Settings
	mux.HandleFunc("GET /settings/clusters", settings.Clusters)
	mux.HandleFunc("GET /settings/clusters/new", settings.ClusterNew)
	mux.HandleFunc("GET /settings/clusters/{slug}", settings.ClusterDetail)
	mux.HandleFunc("POST /api/clusters", settings.ClusterCreate)
	mux.HandleFunc("POST /api/clusters/{slug}", settings.ClusterUpdate)
	mux.HandleFunc("POST /api/clusters/{slug}/switch", settings.ClusterSwitch)
	mux.HandleFunc("POST /api/clusters/{slug}/delete", settings.ClusterDelete)
	mux.HandleFunc("GET /settings/members", settings.Members)
	mux.HandleFunc("GET /settings/members/{slug}", settings.MemberDetail)
	mux.HandleFunc("GET /settings/groups", settings.Groups)
	mux.HandleFunc("GET /settings/groups/{slug}", settings.GroupDetail)
	mux.HandleFunc("GET /settings/sso", settings.SSO)
	mux.HandleFunc("GET /settings/audit-log", settings.AuditLog)
	mux.HandleFunc("GET /settings/integrations", settings.Integrations)
	mux.HandleFunc("GET /settings/public-api-keys", settings.APIKeys)
	mux.HandleFunc("GET /settings/profile", settings.Profile)
	mux.HandleFunc("GET /settings/", settings.Placeholder)

	// Health check
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("OK")); err != nil {
			s.logger.Error("Failed to write health check response", "error", err)
		}
	})

	s.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return s.server.ListenAndServe()
}

// Stop gracefully shuts down the server and closes all cluster clients.
func (s *UIServer) Stop(ctx context.Context) error {
	if s.server != nil {
		if err := s.server.Shutdown(ctx); err != nil {
			return err
		}
	}
	s.store.CloseAll()
	return nil
}

// templateFuncs returns template functions available to all gohtml templates.
func templateFuncs() template.FuncMap {
	return template.FuncMap{
		"statusClass": handlers.StatusClass,
		"statusTone":  handlers.StatusTone,
		"json":        handlers.ToJSON,
		"join":        strings.Join,
		"lower":       strings.ToLower,
		"formatTime": func(t time.Time) string {
			return t.Format("2006-01-02 15:04:05")
		},
		"derefTime": func(t *time.Time) time.Time {
			if t == nil {
				return time.Time{}
			}
			return *t
		},
		"formatDuration": func(d time.Duration) string {
			if d < time.Minute {
				return fmt.Sprintf("%ds", int(d.Seconds()))
			}
			if d < time.Hour {
				return fmt.Sprintf("%dm %ds", int(d.Minutes()), int(d.Seconds())%60)
			}
			return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
		},
		"add": func(a, b int) int { return a + b },
		"sub": func(a, b int) int { return a - b },
		// domID sanitizes a string for safe use in HTML id attributes and CSS selectors
		// by replacing any character that is not alphanumeric or a hyphen with a hyphen.
		"domID": func(s string) string {
			var b strings.Builder
			for _, r := range s {
				if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' {
					b.WriteRune(r)
				} else {
					b.WriteRune('-')
				}
			}
			return b.String()
		},
	}
}
