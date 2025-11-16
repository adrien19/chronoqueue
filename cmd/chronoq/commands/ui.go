package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/adrien19/chronoqueue/cmd/chronoq/ui"
	"github.com/adrien19/chronoqueue/pkg/log"
)

// NewUICommand creates the ui command
func NewUICommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ui",
		Short: "ChronoQueue web interface",
		Long:  `Web interface for monitoring and managing ChronoQueue`,
	}

	cmd.AddCommand(NewUIStartCommand())

	return cmd
}

// NewUIStartCommand creates the ui start command
func NewUIStartCommand() *cobra.Command {
	var (
		port     string
		grpcAddr string
	)

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the ChronoQueue web UI",
		Long: `Start the web interface for monitoring queues, managing schedules,
and viewing dead letter queues.

The UI provides:
- Real-time queue metrics and message monitoring
- Schedule creation and management (cron & calendar)
- Dead letter queue inspection and management
- Message browser with search and filtering

Example:
  chronoq ui start --port 8083 --grpc-address localhost:9000`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runUIStart(port, grpcAddr)
		},
	}

	cmd.Flags().StringVar(&port, "port", "8083", "Port for the web UI")
	cmd.Flags().StringVar(&grpcAddr, "grpc-address", "localhost:9000", "Address of the ChronoQueue gRPC server")

	return cmd
}

func runUIStart(port, grpcAddr string) error {
	logger := log.NewLogger()

	// Create UI server
	uiServer, err := ui.NewUIServer(grpcAddr, logger)
	if err != nil {
		return fmt.Errorf("failed to create UI server: %w", err)
	}

	// Handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start server in goroutine
	errChan := make(chan error, 1)
	go func() {
		addr := fmt.Sprintf(":%s", port)
		logger.InfoWithFields("Starting ChronoQueue UI", "address", addr, "grpc", grpcAddr)
		fmt.Printf("\n")
		fmt.Printf("🚀 ChronoQueue UI is starting...\n")
		fmt.Printf("📊 Dashboard: http://localhost:%s\n", port)
		fmt.Printf("🔗 Connected to: %s\n", grpcAddr)
		fmt.Printf("\n")
		fmt.Printf("Press Ctrl+C to stop\n\n")

		if err := uiServer.Start(addr); err != nil {
			errChan <- err
		}
	}()

	// Wait for shutdown signal or error
	select {
	case <-sigChan:
		logger.Info("Received shutdown signal, stopping UI server...")
		shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 10*time.Second)
		defer shutdownCancel()
		if err := uiServer.Stop(shutdownCtx); err != nil {
			return fmt.Errorf("error stopping UI server: %w", err)
		}
		logger.Info("UI server stopped gracefully")
		return nil
	case err := <-errChan:
		return fmt.Errorf("UI server error: %w", err)
	}
}
