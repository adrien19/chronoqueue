package ui

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"time"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/cmd/chronoq/ui/handlers"
	"github.com/adrien19/chronoqueue/pkg/log"
)

//go:embed templates/* static/*
var content embed.FS

// UIServer serves the ChronoQueue web interface
type UIServer struct {
	templates *template.Template
	client    *client.ChronoQueueClient
	logger    *log.Logger
	server    *http.Server
}

// NewUIServer creates a new UI server instance
func NewUIServer(grpcAddr string, logger *log.Logger) (*UIServer, error) {
	// Parse templates with custom functions
	tmpl := template.New("").Funcs(templateFuncs())

	// Parse all templates (layout + pages + components)
	tmpl, err := tmpl.ParseFS(content, "templates/*.html", "templates/pages/*.html", "templates/components/*.html")
	if err != nil {
		return nil, fmt.Errorf("failed to parse templates: %w", err)
	}

	// Connect to ChronoQueue gRPC server
	chronoClient, err := client.NewChronoQueueClient(grpcAddr, client.ClientOptions{
		MaxRetries: 3,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ChronoQueue server: %w", err)
	}

	return &UIServer{
		templates: tmpl,
		client:    chronoClient,
		logger:    logger,
	}, nil
}

// Start starts the UI HTTP server
func (s *UIServer) Start(addr string) error {
	mux := http.NewServeMux()

	// Static assets - serve from embedded filesystem
	staticFS, err := fs.Sub(content, "static")
	if err != nil {
		return fmt.Errorf("failed to create static filesystem: %w", err)
	}
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))

	// Create handlers
	dashboardHandler := handlers.NewDashboardHandler(s.templates, s.client, s.logger)
	queuesHandler := handlers.NewQueuesHandler(s.templates, s.client, s.logger)
	schedulesHandler := handlers.NewSchedulesHandler(s.templates, s.client, s.logger)
	dlqHandler := handlers.NewDLQHandler(s.templates, s.client, s.logger)

	// Page routes
	mux.HandleFunc("/", dashboardHandler.Index)
	mux.HandleFunc("/queues", queuesHandler.List)
	mux.HandleFunc("/queues/{name}", queuesHandler.Detail)
	mux.HandleFunc("/schedules", schedulesHandler.List)
	mux.HandleFunc("/schedules/new", schedulesHandler.New)
	mux.HandleFunc("/schedules/{id}", schedulesHandler.Detail)
	mux.HandleFunc("/dlq", dlqHandler.List)
	mux.HandleFunc("/dlq/{queue}", dlqHandler.Detail)

	// HTMX API endpoints for real-time updates
	mux.HandleFunc("/api/metrics/dashboard", dashboardHandler.Metrics)
	mux.HandleFunc("/api/queues/{name}/stats", queuesHandler.Stats)
	mux.HandleFunc("/api/queues/{name}/message-detail", queuesHandler.MessageDetail)
	mux.HandleFunc("/api/schedules/create", schedulesHandler.Create)
	mux.HandleFunc("/api/schedules/update", schedulesHandler.Update)
	mux.HandleFunc("/api/schedules/toggle", schedulesHandler.Toggle)
	mux.HandleFunc("/api/schedules/delete", schedulesHandler.Delete)
	mux.HandleFunc("/api/dlq/messages", dlqHandler.Messages)
	mux.HandleFunc("/api/dlq/requeue", dlqHandler.Requeue)
	mux.HandleFunc("/api/dlq/purge", dlqHandler.Purge)

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("OK")); err != nil {
			s.logger.Error("Failed to write health check response", "error", err)
		}
	})

	s.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	s.logger.InfoWithFields("Starting ChronoQueue UI server", "address", addr)
	return s.server.ListenAndServe()
}

// Stop gracefully shuts down the UI server
func (s *UIServer) Stop(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// templateFuncs returns custom template functions
func templateFuncs() template.FuncMap {
	return template.FuncMap{
		"formatTime": func(t time.Time) string {
			return t.Format("2006-01-02 15:04:05")
		},
		"formatDuration": func(d time.Duration) string {
			if d < time.Minute {
				return fmt.Sprintf("%ds", int(d.Seconds()))
			}
			if d < time.Hour {
				return fmt.Sprintf("%dm", int(d.Minutes()))
			}
			return fmt.Sprintf("%dh", int(d.Hours()))
		},
		"add": func(a, b int) int {
			return a + b
		},
		"sub": func(a, b int) int {
			return a - b
		},
	}
}
