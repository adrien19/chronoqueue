package webui

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
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
	templates         *template.Template
	store             *clusterstore.Store
	logger            *log.Logger
	server            *http.Server
	publicOrigin      normalizedOrigin
	hasPublicOrigin   bool
	trustProxyHeaders bool
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

	var (
		publicOrigin    normalizedOrigin
		hasPublicOrigin bool
	)
	if rawPublicOrigin := strings.TrimSpace(os.Getenv("CHRONOQUEUE_UI_PUBLIC_ORIGIN")); rawPublicOrigin != "" {
		parsedOrigin, err := parseOrigin(rawPublicOrigin)
		if err != nil {
			return nil, fmt.Errorf("invalid CHRONOQUEUE_UI_PUBLIC_ORIGIN: %w", err)
		}
		publicOrigin = parsedOrigin
		hasPublicOrigin = true
	}
	if !hasPublicOrigin {
		logger.Warn("CHRONOQUEUE_UI_PUBLIC_ORIGIN is not set; all UI mutation requests will be rejected with 403")
	}

	trustProxyHeaders := false
	if rawTrustProxy := strings.TrimSpace(os.Getenv("CHRONOQUEUE_UI_TRUST_PROXY_HEADERS")); rawTrustProxy != "" {
		parsedTrustProxy, err := strconv.ParseBool(rawTrustProxy)
		if err != nil {
			return nil, fmt.Errorf("invalid CHRONOQUEUE_UI_TRUST_PROXY_HEADERS: %w", err)
		}
		trustProxyHeaders = parsedTrustProxy
	}

	return &UIServer{
		templates:         tmpl,
		store:             store,
		logger:            logger,
		publicOrigin:      publicOrigin,
		hasPublicOrigin:   hasPublicOrigin,
		trustProxyHeaders: trustProxyHeaders,
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
	schemas := handlers.NewSchemasHandler(s.templates, s.store, s.logger)
	settings := handlers.NewSettingsHandler(s.templates, s.store, s.logger)
	protectMutation := s.protectMutationRoute

	// Console pages
	mux.HandleFunc("GET /", dashboard.Index)
	mux.HandleFunc("GET /queues", queues.List)
	mux.HandleFunc("GET /queues/new", queues.New)
	mux.HandleFunc("GET /queues/{name}", queues.Detail)
	mux.HandleFunc("GET /queues/{name}/messages", queues.MessageDetail)
	mux.HandleFunc("GET /queues/{name}/messages/new", queues.NewMessage)
	mux.HandleFunc("POST /queues/{name}/requeue-all", protectMutation(queues.RequeueAll))
	mux.HandleFunc("POST /queues/{name}/purge", protectMutation(queues.Purge))
	mux.HandleFunc("POST /api/queues/{name}/messages/{messageId}/requeue", protectMutation(queues.RequeueMessage))
	mux.HandleFunc("POST /api/queues/{name}/messages/{messageId}/dlq-delete", protectMutation(queues.DeleteDLQMessage))
	mux.HandleFunc("GET /workers", workers.List)
	mux.HandleFunc("GET /lease-monitor", leaseMonitor.List)
	mux.HandleFunc("GET /schedules", schedules.List)
	mux.HandleFunc("GET /schedules/new", schedules.New)
	mux.HandleFunc("GET /schemas", schemas.List)
	mux.HandleFunc("GET /schemas/new", schemas.New)
	mux.HandleFunc("GET /schemas/{schemaId}", schemas.Detail)

	// HTMX / API endpoints
	mux.HandleFunc("POST /api/queues/create", protectMutation(queues.Create))
	mux.HandleFunc("POST /api/queues/{name}/messages", protectMutation(queues.PostMessage))
	mux.HandleFunc("POST /api/queues/{name}/messages/validate", protectMutation(queues.ValidateMessage))
	mux.HandleFunc("POST /api/schedules/create", protectMutation(schedules.Create))
	mux.HandleFunc("POST /api/schedules/toggle", protectMutation(schedules.Toggle))
	mux.HandleFunc("DELETE /api/schedules/{id}", protectMutation(schedules.Delete))
	mux.HandleFunc("POST /api/schemas/register", protectMutation(schemas.Create))
	mux.HandleFunc("POST /api/schemas/{schemaId}/validate", protectMutation(schemas.Validate))
	mux.HandleFunc("POST /api/schemas/{schemaId}/versions/{version}/delete", protectMutation(schemas.Delete))
	mux.HandleFunc("GET /fragments/live-overview", dashboard.LiveOverview)
	mux.HandleFunc("GET /fragments/dashboard-stats", dashboard.DashboardStats)
	mux.HandleFunc("GET /fragments/lease-table", leaseMonitor.Table)

	// Settings
	mux.HandleFunc("GET /settings/clusters", settings.Clusters)
	mux.HandleFunc("GET /settings/clusters/new", settings.ClusterNew)
	mux.HandleFunc("GET /settings/clusters/{slug}", settings.ClusterDetail)
	mux.HandleFunc("POST /api/clusters", protectMutation(settings.ClusterCreate))
	mux.HandleFunc("POST /api/clusters/{slug}", protectMutation(settings.ClusterUpdate))
	mux.HandleFunc("POST /api/clusters/{slug}/switch", protectMutation(settings.ClusterSwitch))
	mux.HandleFunc("POST /api/clusters/{slug}/delete", protectMutation(settings.ClusterDelete))
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
		Addr:              addr,
		Handler:           uiSecurityHeaders(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	return s.server.ListenAndServe()
}

const uiContentSecurityPolicy = "default-src 'self'; base-uri 'none'; object-src 'none'; frame-ancestors 'none'; form-action 'self'; img-src 'self' data:; font-src 'self'; script-src 'self' 'sha256-IUOv9nmQrfTDlLJauGABujh0XbcvcUmLJX6WZmaEdzc='; style-src 'self'; connect-src 'self'"

func uiSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", uiContentSecurityPolicy)
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		next.ServeHTTP(w, r)
	})
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

func (s *UIServer) protectMutationRoute(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := validateMutationOrigin(r, s.publicOrigin, s.hasPublicOrigin, s.trustProxyHeaders); err != nil {
			s.logger.WarnWithFields("Rejected potential cross-site mutation request", "error", err, "method", r.Method, "path", r.URL.Path)
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

func validateMutationOrigin(
	r *http.Request,
	configuredOrigin normalizedOrigin,
	hasConfiguredOrigin bool,
	trustProxyHeaders bool,
) error {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		return nil
	}

	switch strings.ToLower(strings.TrimSpace(r.Header.Get("Sec-Fetch-Site"))) {
	case "cross-site", "none":
		return fmt.Errorf("sec-fetch-site=%s", r.Header.Get("Sec-Fetch-Site"))
	}

	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return fmt.Errorf("missing origin header")
	}

	requestOrigin, err := parseOrigin(origin)
	if err != nil {
		return fmt.Errorf("invalid origin header: %w", err)
	}

	if !hasConfiguredOrigin {
		return fmt.Errorf("CHRONOQUEUE_UI_PUBLIC_ORIGIN is not configured")
	}
	if requestOrigin != configuredOrigin {
		return fmt.Errorf("origin mismatch: %s != %s", requestOrigin, configuredOrigin)
	}

	effectiveTargetOrigin, err := requestTargetOrigin(r, trustProxyHeaders)
	if err != nil {
		return fmt.Errorf("invalid request target origin: %w", err)
	}
	if effectiveTargetOrigin != configuredOrigin {
		return fmt.Errorf("target origin mismatch: %s != %s", effectiveTargetOrigin, configuredOrigin)
	}

	return nil
}

type normalizedOrigin struct {
	Scheme string
	Host   string
	Port   string
}

func (o normalizedOrigin) String() string {
	return o.Scheme + "://" + net.JoinHostPort(o.Host, o.Port)
}

func parseOrigin(raw string) (normalizedOrigin, error) {
	originURL, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return normalizedOrigin{}, err
	}
	if originURL.Scheme == "" {
		return normalizedOrigin{}, fmt.Errorf("scheme is required")
	}
	host := originURL.Hostname()
	if host == "" {
		return normalizedOrigin{}, fmt.Errorf("host is required")
	}
	port := normalizePort(originURL.Scheme, originURL.Port())
	if port == "" {
		return normalizedOrigin{}, fmt.Errorf("unsupported scheme %q", originURL.Scheme)
	}

	return normalizedOrigin{
		Scheme: strings.ToLower(originURL.Scheme),
		Host:   strings.ToLower(host),
		Port:   port,
	}, nil
}

func requestTargetOrigin(r *http.Request, trustProxyHeaders bool) (normalizedOrigin, error) {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	hostPort := r.Host

	if trustProxyHeaders {
		if forwardedProto := firstHeaderValue(r.Header.Get("X-Forwarded-Proto")); forwardedProto != "" {
			scheme = strings.ToLower(forwardedProto)
		}
		if forwardedHost := firstHeaderValue(r.Header.Get("X-Forwarded-Host")); forwardedHost != "" {
			hostPort = forwardedHost
		}
	}

	hostPort = strings.TrimSpace(hostPort)
	if hostPort == "" {
		return normalizedOrigin{}, fmt.Errorf("host is required")
	}

	host := hostPort
	port := ""
	if parsedHost, parsedPort, err := net.SplitHostPort(hostPort); err == nil {
		host = parsedHost
		port = parsedPort
	} else if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return normalizedOrigin{}, fmt.Errorf("host is required")
	}

	normalizedPort := normalizePort(scheme, port)
	if normalizedPort == "" {
		return normalizedOrigin{}, fmt.Errorf("unsupported scheme %q", scheme)
	}

	return normalizedOrigin{
		Scheme: strings.ToLower(scheme),
		Host:   host,
		Port:   normalizedPort,
	}, nil
}

func normalizePort(scheme string, port string) string {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "http":
		if strings.TrimSpace(port) == "" {
			return "80"
		}
		return strings.TrimSpace(port)
	case "https":
		if strings.TrimSpace(port) == "" {
			return "443"
		}
		return strings.TrimSpace(port)
	default:
		return ""
	}
}

func firstHeaderValue(raw string) string {
	if raw == "" {
		return ""
	}
	parts := strings.Split(raw, ",")
	if len(parts) == 0 {
		return ""
	}
	return strings.TrimSpace(parts[0])
}
