package commands

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func TestNewServerCommand(t *testing.T) {
	cmd := NewServerCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "server", cmd.Use)
	assert.Equal(t, "Server operations", cmd.Short)
	assert.Contains(t, cmd.Long, "Server-related operations")

	// Check that subcommands are properly added
	subcommands := cmd.Commands()
	assert.Len(t, subcommands, 2)

	// Check subcommand names
	subcommandNames := make([]string, len(subcommands))
	for i, subcmd := range subcommands {
		subcommandNames[i] = subcmd.Use
	}

	expectedCommands := []string{
		"health",
		"version",
	}

	for _, expected := range expectedCommands {
		assert.Contains(t, subcommandNames, expected)
	}
}

func TestNewServerHealthCommand(t *testing.T) {
	cmd := newServerHealthCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "health", cmd.Use)
	assert.Equal(t, "Check server health", cmd.Short)
	assert.Contains(t, cmd.Long, "Check if the ChronoQueue server is healthy")
	assert.NotNil(t, cmd.RunE)
}

func TestNewServerVersionCommand(t *testing.T) {
	cmd := newServerVersionCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "version", cmd.Use)
	assert.Equal(t, "Show server version", cmd.Short)
	assert.Contains(t, cmd.Long, "Display version information")
	assert.NotNil(t, cmd.RunE)
}

func TestServerHealthCommand_Execution(t *testing.T) {
	cmd := newServerHealthCommand()

	// Set up common client flags that are expected
	cmd.Flags().String("server", "localhost:9000", "Server address")
	cmd.Flags().Bool("insecure", false, "Use insecure connection")
	cmd.Flags().String("cert-file", "", "Client certificate file")
	cmd.Flags().String("key-file", "", "Client key file")
	cmd.Flags().String("ca-file", "", "CA certificate file")
	cmd.Flags().Duration("timeout", 0, "Request timeout")
	cmd.Flags().Bool("verbose", false, "Verbose output")

	// Since the health command tries to connect to a server,
	// we expect it to fail in the test environment, but it shouldn't panic
	err := cmd.RunE(cmd, []string{})

	// We expect this to fail since there's no server running in tests
	// The important thing is that it doesn't panic and handles the error gracefully
	t.Logf("Health command execution result: err=%v", err)
}

func TestServerVersionCommand_Execution(t *testing.T) {
	cmd := newServerVersionCommand()

	// Set up common client flags that are expected
	cmd.Flags().String("server", "localhost:9000", "Server address")
	cmd.Flags().Bool("insecure", false, "Use insecure connection")
	cmd.Flags().String("cert-file", "", "Client certificate file")
	cmd.Flags().String("key-file", "", "Client key file")
	cmd.Flags().String("ca-file", "", "CA certificate file")
	cmd.Flags().Duration("timeout", 0, "Request timeout")
	cmd.Flags().Bool("verbose", false, "Verbose output")

	// Since the version command tries to connect to a server,
	// we expect it to fail in the test environment, but it shouldn't panic
	err := cmd.RunE(cmd, []string{})

	// We expect this to fail since there's no server running in tests
	// The important thing is that it doesn't panic and handles the error gracefully
	t.Logf("Version command execution result: err=%v", err)
}

func TestServerCommand_SubcommandStructure(t *testing.T) {
	cmd := NewServerCommand()

	subcommands := cmd.Commands()
	assert.Len(t, subcommands, 2)

	// Find health command
	var healthCmd *cobra.Command
	var versionCmd *cobra.Command

	for _, subcmd := range subcommands {
		switch subcmd.Use {
		case "health":
			healthCmd = subcmd
		case "version":
			versionCmd = subcmd
		}
	}

	assert.NotNil(t, healthCmd, "Health command should exist")
	assert.NotNil(t, versionCmd, "Version command should exist")

	assert.Equal(t, "Check server health", healthCmd.Short)
	assert.Equal(t, "Show server version", versionCmd.Short)
}

func TestServerCommand_NoDirectExecution(t *testing.T) {
	cmd := NewServerCommand()

	// The main server command should not have a RunE function
	// It should only act as a parent for subcommands
	assert.Nil(t, cmd.RunE)
}

func TestServerHealthCommand_Description(t *testing.T) {
	cmd := newServerHealthCommand()

	assert.Contains(t, cmd.Long, "Check if the ChronoQueue server is healthy and responding")
}

func TestServerVersionCommand_Description(t *testing.T) {
	cmd := newServerVersionCommand()

	assert.Contains(t, cmd.Long, "Display version information")
}
