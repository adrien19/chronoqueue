package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/examples/interview-platform/backend/internal/api"
	"github.com/adrien19/chronoqueue/examples/interview-platform/backend/internal/db"
	"github.com/adrien19/chronoqueue/examples/interview-platform/backend/internal/sse"
)

var (
	port            = flag.String("port", "8080", "HTTP server port")
	chronoqueueAddr = flag.String("chronoqueue", "localhost:50051", "ChronoQueue gRPC address")
	dbPath          = flag.String("db", "/workspaces/chronoqueue/logs/api_interview_platform.db", "SQLite database path")
)

func main() {
	flag.Parse()

	// Initialize database
	database, err := db.NewDatabase(*dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer database.Close()

	log.Println("Database initialized successfully")

	// Initialize ChronoQueue client
	log.Println("Attempting to connect to ChronoQueue...")
	queueClient, err := client.NewChronoQueueClient(*chronoqueueAddr, client.ClientOptions{
		MaxRetries:     10,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     10 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to connect to ChronoQueue: %v", err)
	}
	if queueClient == nil {
		log.Fatalf("ChronoQueue client is nil but no error returned!")
	}
	defer queueClient.Close()

	log.Printf("Connected to ChronoQueue at %s", *chronoqueueAddr)
	log.Printf("Client object: %+v", queueClient)

	// Initialize queues
	ctx := context.Background()
	if err := initializeQueues(ctx, queueClient); err != nil {
		log.Fatalf("Failed to initialize queues: %v", err)
	}

	// Initialize SSE broadcaster
	broadcaster := sse.NewBroadcaster()
	log.Println("SSE broadcaster initialized")

	// Initialize API handlers
	handlers := api.NewHandlers(database, queueClient, broadcaster)

	// Setup router
	r := chi.NewRouter()

	// Middleware
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))

	// CORS configuration
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"http://localhost:3000", "http://localhost:3001"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	// Health check
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"status": "healthy",
			"time":   time.Now().Format(time.RFC3339),
		})
	})

	// API routes
	r.Route("/api", func(r chi.Router) {
		// Server-Sent Events for real-time updates
		r.Get("/events", handlers.SSEHandler)

		// Dashboard
		r.Get("/dashboard/stats", handlers.GetDashboardStats)
		r.Get("/dashboard/activity", handlers.GetDashboardActivity)

		// Interviews
		r.Route("/interviews", func(r chi.Router) {
			r.Get("/", handlers.ListInterviews)
			r.Post("/", handlers.CreateInterview)
			r.Get("/{id}", handlers.GetInterview)
			r.Put("/{id}", handlers.UpdateInterview)
			r.Post("/{id}/cancel", handlers.CancelInterview)
			r.Post("/{id}/start", handlers.StartInterview)
			r.Post("/{id}/complete", handlers.CompleteInterview)
			r.Get("/{id}/evaluations", handlers.GetInterviewEvaluations)
			r.Get("/{id}/report", handlers.GetInterviewReport)
		})

		// Evaluations
		r.Route("/evaluations", func(r chi.Router) {
			r.Get("/", handlers.ListEvaluations)
			r.Post("/", handlers.CreateEvaluation)
			r.Get("/pending", handlers.GetPendingEvaluations)
			r.Get("/{id}", handlers.GetEvaluation)
			r.Put("/{id}", handlers.UpdateEvaluation)
		})

		// Reports
		r.Route("/reports", func(r chi.Router) {
			r.Get("/", handlers.ListReports)
			r.Post("/generate", handlers.GenerateReport)
			r.Get("/{id}", handlers.GetReport)
			r.Post("/{id}/send", handlers.SendReport)
			r.Get("/{id}/pdf", handlers.DownloadReportPDF)
		})

		// Queue monitoring
		r.Route("/queues", func(r chi.Router) {
			r.Get("/", handlers.ListQueues)
			r.Get("/{name}/stats", handlers.GetQueueStats)
			r.Get("/messages/recent", handlers.GetRecentMessages)
		})
	})

	// Start server
	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", *port),
		Handler: r,
	}

	// Graceful shutdown
	go func() {
		log.Printf("Starting API server on port %s", *port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited")
}

// initializeQueues creates all necessary queues for the interview platform
func initializeQueues(ctx context.Context, qClient *client.ChronoQueueClient) error {
	queueNames := []string{
		"interview-scheduler",
		"evaluation-processor",
		"report-generator",
		"notification-sender",
	}

	queueOpts := client.QueueOptions{
		Type:            0, // SIMPLE queue
		DequeueAttempts: 3,
		LeaseDuration:   "30s",
		AutoCreateDLQ:   true,
	}

	for _, queueName := range queueNames {
		_, err := qClient.CreateQueue(ctx, queueName, queueOpts)
		if err != nil {
			// Log warning but don't fail if queue already exists
			log.Printf("Warning: Failed to create queue %s: %v (may already exist)", queueName, err)
			continue
		}
		log.Printf("Created queue: %s", queueName)
	}

	log.Println("All queues initialized successfully")
	return nil
}
