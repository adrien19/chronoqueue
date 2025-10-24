package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/adrien19/chronoqueue/cmd/chronoq/commands"
	"github.com/adrien19/chronoqueue/internal/server"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	ctx := context.Background()

	rootCmd := &cobra.Command{
		Use:   "chronoqueue",
		Short: "ChronoQueue - A powerful message queue management tool",
		Long: `ChronoQueue is a high-performance message queue system with scheduling capabilities.

This unified CLI provides both server operations and client management commands.

Examples:
  # Start development server
  chronoqueue server --dev

  # Start production server
  chronoqueue server --grpc-addr :9000 --http-addr :8080

  # Client operations
  chronoqueue queue create my-queue --type simple
  chronoqueue message post my-queue "Hello World"
  chronoqueue message get my-queue
  chronoqueue schedule create --cron "0 */5 * * *" --queue my-queue --message "Scheduled task"`,
		Version: fmt.Sprintf("%s (commit: %s, built: %s)", version, commit, date),
	}

	// Global flags for client operations
	rootCmd.PersistentFlags().String("server", "localhost:8080", "ChronoQueue server address")
	rootCmd.PersistentFlags().Bool("insecure", false, "Use insecure connection (no TLS)")
	rootCmd.PersistentFlags().String("cert-file", "", "Path to client certificate file")
	rootCmd.PersistentFlags().String("key-file", "", "Path to client private key file")
	rootCmd.PersistentFlags().String("ca-file", "", "Path to CA certificate file")
	rootCmd.PersistentFlags().String("output", "table", "Output format (table, json, yaml)")
	rootCmd.PersistentFlags().Bool("verbose", false, "Enable verbose output")
	rootCmd.PersistentFlags().Duration("timeout", 0, "Request timeout (0 for no timeout)")

	// Add command groups
	rootCmd.AddCommand(newServerCommand())
	rootCmd.AddCommand(commands.NewQueueCommand())
	rootCmd.AddCommand(commands.NewMessageCommand())
	rootCmd.AddCommand(commands.NewScheduleCommand())
	rootCmd.AddCommand(commands.NewDLQCommand())
	rootCmd.AddCommand(commands.NewSchemaCommand()) // Schema registry commands
	rootCmd.AddCommand(commands.NewStartCommand())  // Legacy compatibility

	// Execute root command
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// newServerCommand creates the unified server command
func newServerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "server",
		Short: "Start ChronoQueue server",
		Long: `Start a ChronoQueue server instance.

This command starts both gRPC and HTTP gateway servers. Use --dev for development
mode with additional features like API documentation and CORS enabled.

Examples:
  # Development server with defaults
  chronoqueue server --dev

  # Production server with custom configuration
  chronoqueue server --grpc-addr :9000 --http-addr :8080 --redis-addr localhost:6379

  # Server with TLS enabled
  chronoqueue server --enable-tls --cert-file server.crt --key-file server.key`,
		RunE: runServer,
	}

	// Server configuration flags
	config := server.DefaultConfig()
	server.AddServerFlags(cmd, config)

	// Add mode flags
	cmd.Flags().Bool("dev", false, "Start in development mode (enables CORS, API docs, reflection)")
	cmd.Flags().Bool("production", false, "Start in production mode (optimized for production use)")

	return cmd
}

// runServer handles the server command execution
func runServer(cmd *cobra.Command, args []string) error {
	// Determine configuration based on mode
	var config *server.Config

	devMode, _ := cmd.Flags().GetBool("dev")
	prodMode, _ := cmd.Flags().GetBool("production")

	if devMode && prodMode {
		return fmt.Errorf("cannot specify both --dev and --production modes")
	}

	if devMode {
		config = server.DefaultConfig() // Development mode
		config.IsDevelopment = true
	} else if prodMode {
		config = server.ProductionConfig() // Production mode
		config.IsDevelopment = false
	} else {
		// Default mode - development for ease of use
		config = server.DefaultConfig()
		config.IsDevelopment = true
	}

	// Parse configuration from flags
	parsedConfig, err := server.ParseConfigFromFlags(cmd)
	if err != nil {
		return fmt.Errorf("failed to parse configuration: %w", err)
	}

	// Merge the parsed config (preserving mode setting)
	parsedConfig.IsDevelopment = config.IsDevelopment
	config = parsedConfig

	// Create and start server
	srv, err := server.New(config)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	ctx := cmd.Context()
	return srv.Start(ctx)
}
