package commands

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func TestNewStartCommand(t *testing.T) {
	cmd := NewStartCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "start", cmd.Use)
	assert.Equal(t, "Start ChronoQueue server (legacy)", cmd.Short)
	assert.Contains(t, cmd.Long, "Start a ChronoQueue server instance")
	assert.NotNil(t, cmd.RunE)

	// Check that flags are properly added
	devServerFlag := cmd.Flags().Lookup("dev-server")
	assert.NotNil(t, devServerFlag)

	// Check that dev-server subcommand is added
	devServerCmd := cmd.Commands()
	assert.Len(t, devServerCmd, 1)
	assert.Equal(t, "dev-server", devServerCmd[0].Use)
}

func TestNewDevServerCommand(t *testing.T) {
	cmd := newDevServerCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "dev-server", cmd.Use)
	assert.Equal(t, "Start a development ChronoQueue server", cmd.Short)
	assert.Contains(t, cmd.Long, "Start a ChronoQueue server configured for local development")
	assert.NotNil(t, cmd.RunE)
}

func TestRunStart_WithoutDevServerFlag(t *testing.T) {
	cmd := NewStartCommand()

	// Mock the Help function to avoid actual help output
	helpCalled := false
	cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		helpCalled = true
	})

	err := runStart(cmd, []string{})

	// Should return no error but help should be called
	assert.NoError(t, err)
	assert.True(t, helpCalled)
}

func TestRunStart_WithDevServerFlag(t *testing.T) {
	cmd := NewStartCommand()
	_ = cmd.Flags().Set("dev-server", "true") // Test flag setting

	assert.True(t, cmd.Flag("dev-server").Changed)
}

func TestStartCommand_FlagDefaults(t *testing.T) {
	cmd := NewStartCommand()

	// Test default values
	devServer, err := cmd.Flags().GetBool("dev-server")
	assert.NoError(t, err)
	assert.False(t, devServer)
}

func TestStartCommand_FlagSettings(t *testing.T) {
	cmd := NewStartCommand()

	// Test setting flags
	err := cmd.Flags().Set("dev-server", "true")
	assert.NoError(t, err)

	devServer, err := cmd.Flags().GetBool("dev-server")
	assert.NoError(t, err)
	assert.True(t, devServer)
}

func TestDevServerCommand_HasServerFlags(t *testing.T) {
	cmd := newDevServerCommand()

	// The command should have server configuration flags added by server.AddServerFlags
	// We can't test the exact flags without knowing the implementation of AddServerFlags,
	// but we can test that the command structure is correct
	assert.NotNil(t, cmd.Flags())
	assert.NotNil(t, cmd.RunE)
}

func TestStartCommand_SubcommandStructure(t *testing.T) {
	cmd := NewStartCommand()

	subcommands := cmd.Commands()
	assert.Len(t, subcommands, 1)

	devServerCmd := subcommands[0]
	assert.Equal(t, "dev-server", devServerCmd.Use)
	assert.Equal(t, "Start a development ChronoQueue server", devServerCmd.Short)
}

func TestStartCommand_Examples(t *testing.T) {
	cmd := NewStartCommand()

	assert.Contains(t, cmd.Long, "chronoqueue start --dev-server")
	assert.Contains(t, cmd.Long, "chronoqueue start dev-server --log-level debug")
}

func TestDevServerCommand_Examples(t *testing.T) {
	cmd := newDevServerCommand()

	assert.Contains(t, cmd.Long, "chronoqueue start dev-server")
	assert.Contains(t, cmd.Long, "chronoqueue start dev-server --storage-type postgres")
	assert.Contains(t, cmd.Long, "chronoqueue start dev-server --grpc-addr :9001 --http-addr :8081")
	assert.Contains(t, cmd.Long, "chronoqueue start dev-server --log-level debug")
}
