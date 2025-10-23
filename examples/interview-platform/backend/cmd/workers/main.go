package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/examples/interview-platform/backend/internal/db"
	"github.com/adrien19/chronoqueue/examples/interview-platform/backend/pkg/workers"
)

func main() {
	log.Println("Starting ChronoQueue Workers...")

	// Initialize database (shared with API)
	database, err := db.NewDatabase("/workspaces/chronoqueue/logs/api_interview_platform.db")
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer database.Close()
	log.Println("Database initialized successfully")

	// Connect to ChronoQueue
	queueClient, err := client.NewChronoQueueClient("localhost:50051", client.ClientOptions{
		MaxRetries:     10,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     10 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to connect to ChronoQueue: %v", err)
	}
	defer queueClient.Close()
	log.Println("Connected to ChronoQueue at localhost:50051")

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start all workers
	var wg sync.WaitGroup

	// 1. Interview Scheduler Worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		worker := workers.NewInterviewSchedulerWorker(queueClient, database)
		if err := worker.Start(ctx); err != nil {
			log.Printf("Interview Scheduler Worker error: %v", err)
		}
	}()

	// 2. Evaluation Processor Worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		worker := workers.NewEvaluationProcessorWorker(queueClient, database)
		if err := worker.Start(ctx); err != nil {
			log.Printf("Evaluation Processor Worker error: %v", err)
		}
	}()

	// 3. Report Generator Worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		worker := workers.NewReportGeneratorWorker(queueClient, database)
		if err := worker.Start(ctx); err != nil {
			log.Printf("Report Generator Worker error: %v", err)
		}
	}()

	// 4. Notification Sender Worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		worker := workers.NewNotificationSenderWorker(queueClient, database)
		if err := worker.Start(ctx); err != nil {
			log.Printf("Notification Sender Worker error: %v", err)
		}
	}()

	log.Println("All workers started successfully")

	// Wait for shutdown signal
	<-sigChan
	log.Println("Shutdown signal received, stopping workers...")

	// Cancel context to stop all workers
	cancel()

	// Wait for all workers to finish with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All workers stopped gracefully")
	case <-time.After(30 * time.Second):
		log.Println("Shutdown timeout, forcing exit")
	}
}
