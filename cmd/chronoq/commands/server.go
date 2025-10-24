package commands

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/adrien19/chronoqueue/cmd/chronoq/outputs"
)

// NewServerCommand creates the server command group
func NewServerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "server",
		Short: "Server operations",
		Long:  `Server-related operations like health checks and version information.`,
	}

	cmd.AddCommand(newServerHealthCommand())
	cmd.AddCommand(newServerVersionCommand())

	return cmd
}

// newServerHealthCommand creates the server health subcommand
func newServerHealthCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "health",
		Short: "Check server health",
		Long:  `Check if the ChronoQueue server is healthy and responding.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			server, _ := cmd.Flags().GetString("server")

			// For now, just attempt a connection
			opts, err := GetClientOptions(cmd)
			if err != nil {
				return fmt.Errorf("failed to get client options: %w", err)
			}

			client, err := CreateClient(opts)
			if err != nil {
				outputs.PrintError(fmt.Sprintf("Server at %s is not healthy: %v", server, err))
				return nil
			}
			defer client.Close()

			outputs.PrintSuccess(fmt.Sprintf("Server at %s is healthy", server))
			return nil
		},
	}

	return cmd
}

// newServerVersionCommand creates the server version subcommand
func newServerVersionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Show server version",
		Long:  `Display version information for the ChronoQueue server.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			outputs.PrintInfo("Server version checking not yet implemented")
			return nil
		},
	}

	return cmd
}
