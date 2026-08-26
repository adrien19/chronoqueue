package commands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/adrien19/chronoqueue/cmd/chronoq/outputs"
)

var serverHTTPClient = &http.Client{Timeout: 10 * time.Second}

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
			server, err := cmd.Flags().GetString("http-server")
			if err != nil {
				return fmt.Errorf("get HTTP server address: %w", err)
			}
			if !cmd.Flags().Changed("http-server") && cmd.Flags().Changed("server") {
				server, err = cmd.Flags().GetString("server")
				if err != nil {
					return fmt.Errorf("get deprecated server address: %w", err)
				}
			}
			if !strings.Contains(server, "://") {
				server = "http://" + server
			}
			requestContext := cmd.Context()
			if requestContext == nil {
				requestContext = context.Background()
			}
			request, err := http.NewRequestWithContext(requestContext, http.MethodGet, strings.TrimRight(server, "/")+"/ready", nil)
			if err != nil {
				return fmt.Errorf("create readiness request: %w", err)
			}
			response, err := serverHTTPClient.Do(request)
			if err != nil {
				return fmt.Errorf("server at %s is not ready: %w", server, err)
			}
			closeErr := response.Body.Close()
			if response.StatusCode != http.StatusOK {
				return errors.Join(fmt.Errorf("server at %s is not ready: returned %s", server, response.Status), closeErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close readiness response: %w", closeErr)
			}
			outputs.PrintSuccess(fmt.Sprintf("Server at %s is ready", server))
			return nil
		},
	}
	cmd.Flags().String("http-server", "http://localhost:8080", "ChronoQueue HTTP server URL")
	cmd.Flags().String("server", "", "Deprecated alias for --http-server")
	if err := cmd.Flags().MarkDeprecated("server", "use --http-server instead"); err != nil {
		panic(fmt.Sprintf("mark --server deprecated: %v", err))
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
			response, err := serverHTTPClient.Do(request)
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
