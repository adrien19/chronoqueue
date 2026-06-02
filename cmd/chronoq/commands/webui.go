package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	webui "github.com/adrien19/chronoqueue/cmd/chronoq/web-ui"
	"github.com/adrien19/chronoqueue/pkg/log"
)

// NewWebUICommand creates the web-ui command.
func NewWebUICommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "web-ui",
		Short: "ChronoQueue next-generation web interface",
		Long:  `Next-generation dark-themed web interface for monitoring and managing ChronoQueue`,
	}

	cmd.AddCommand(newWebUIStartCommand())

	return cmd
}

func newWebUIStartCommand() *cobra.Command {
	var (
		port     string
		grpcAddr string
		skipSSL  bool
	)

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the ChronoQueue next-generation web UI",
		Long: `Start the next-generation web interface for monitoring queues, managing
schedules, and viewing dead letter queues. Uses the dark cq-* design system.

Example:
  chronoq web-ui start --port 8081 --grpc-address localhost:9000`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runWebUIStart(port, grpcAddr, skipSSL)
		},
	}

	cmd.Flags().StringVar(&port, "port", "8081", "Port for the web UI")
	cmd.Flags().StringVar(&grpcAddr, "grpc-address", "localhost:9000", "Address of the ChronoQueue gRPC server")
	cmd.Flags().BoolVar(&skipSSL, "skip-ssl", false, "Disable TLS verification for the gRPC connection (for local/dev use)")

	return cmd
}

func runWebUIStart(port, grpcAddr string, skipSSL bool) error {
	logger := log.NewLogger()

	server, err := webui.NewUIServer(grpcAddr, skipSSL, logger)
	if err != nil {
		return fmt.Errorf("failed to create web-UI server: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	errChan := make(chan error, 1)
	go func() {
		addr := fmt.Sprintf(":%s", port)
		logger.InfoWithFields("Starting ChronoQueue web-UI", "address", addr, "grpc", grpcAddr)
		fmt.Printf("\n")
		fmt.Printf("ChronoQueue web-UI is starting...\n")
		fmt.Printf("Dashboard: http://localhost:%s\n", port)
		fmt.Printf("Connected to: %s\n", grpcAddr)
		fmt.Printf("\n")
		fmt.Printf("Press Ctrl+C to stop\n\n")

		if err := server.Start(addr); err != nil {
			errChan <- err
		}
	}()

	select {
	case <-sigChan:
		logger.Info("Received shutdown signal, stopping web-UI server...")
		shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 10*time.Second)
		defer shutdownCancel()
		if err := server.Stop(shutdownCtx); err != nil {
			return fmt.Errorf("error stopping web-UI server: %w", err)
		}
		logger.Info("web-UI server stopped gracefully")
		return nil
	case err := <-errChan:
		return fmt.Errorf("web-UI server error: %w", err)
	}
}
