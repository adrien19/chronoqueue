package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/adrien19/chronoqueue/cmd/chronoq/outputs"
)

var versionHTTPClient = &http.Client{Timeout: 10 * time.Second}

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
			server, err := cmd.Flags().GetString("http-server")
			if err != nil {
				return fmt.Errorf("get HTTP server address: %w", err)
			}
			requestContext := cmd.Context()
			if requestContext == nil {
				requestContext = context.Background()
			}
			request, err := http.NewRequestWithContext(requestContext, http.MethodGet, strings.TrimRight(server, "/")+"/ready", nil)
			if err != nil {
				return fmt.Errorf("create version request: %w", err)
			}
			response, err := versionHTTPClient.Do(request)
			if err != nil {
				return fmt.Errorf("query server version: %w", err)
			}
			if response.StatusCode != http.StatusOK {
				if err := response.Body.Close(); err != nil {
					return fmt.Errorf("close version response: %w", err)
				}
				return fmt.Errorf("query server version: server returned %s", response.Status)
			}
			var metadata struct {
				Version   string `json:"version"`
				GitCommit string `json:"git_commit"`
				BuildDate string `json:"build_date"`
			}
			decodeErr := json.NewDecoder(response.Body).Decode(&metadata)
			closeErr := response.Body.Close()
			if decodeErr != nil {
				return fmt.Errorf("decode server version: %w", decodeErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close version response: %w", closeErr)
			}
			if metadata.Version == "" {
				return fmt.Errorf("decode server version: response omitted version")
			}
			outputs.PrintInfo(fmt.Sprintf("ChronoQueue v%s\n  Git Commit: %s\n  Built:      %s", metadata.Version, metadata.GitCommit, metadata.BuildDate))
			return nil
		},
	}
	cmd.Flags().String("http-server", "http://localhost:8080", "ChronoQueue HTTP server URL")

	return cmd
}
