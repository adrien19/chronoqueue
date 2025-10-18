package commands

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func TestNewQueueCommand(t *testing.T) {
	cmd := NewQueueCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "queue", cmd.Use)
	assert.Equal(t, "Queue management operations", cmd.Short)
	assert.Contains(t, cmd.Long, "Manage ChronoQueue queues")

	// Check that subcommands are properly added
	subcommands := cmd.Commands()
	assert.Len(t, subcommands, 4)

	// Check subcommand names
	subcommandNames := make([]string, len(subcommands))
	for i, subcmd := range subcommands {
		subcommandNames[i] = subcmd.Use
	}

	assert.Contains(t, subcommandNames, "create <queue-name>")
	assert.Contains(t, subcommandNames, "delete <queue-name>")
	assert.Contains(t, subcommandNames, "list")
	assert.Contains(t, subcommandNames, "state <queue-name>")
}

func TestNewQueueCreateCommand(t *testing.T) {
	cmd := newQueueCreateCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "create <queue-name>", cmd.Use)
	assert.Equal(t, "Create a new queue", cmd.Short)
	assert.Contains(t, cmd.Long, "Create a new ChronoQueue queue")
	assert.NotNil(t, cmd.RunE)

	// Check that command requires exactly one argument
	assert.NotNil(t, cmd.Args)

	// Check that flags are properly added
	flags := []string{"type", "dequeue-attempts", "lease-duration", "exclusivity-key", "invisibility-duration"}
	for _, flagName := range flags {
		flag := cmd.Flags().Lookup(flagName)
		assert.NotNil(t, flag, "Flag %s should be present", flagName)
	}
}

func TestNewQueueDeleteCommand(t *testing.T) {
	cmd := newQueueDeleteCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "delete <queue-name>", cmd.Use)
	assert.Equal(t, "Delete a queue", cmd.Short)
	assert.Contains(t, cmd.Long, "Delete an existing ChronoQueue queue")
	assert.NotNil(t, cmd.RunE)

	// Check that command requires exactly one argument
	assert.NotNil(t, cmd.Args)

	// Check that force flag is present
	forceFlag := cmd.Flags().Lookup("force")
	assert.NotNil(t, forceFlag)
}

func TestNewQueueListCommand(t *testing.T) {
	cmd := newQueueListCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "list", cmd.Use)
	assert.Equal(t, "List all queues", cmd.Short)
	assert.Contains(t, cmd.Long, "List all ChronoQueue queues")
	assert.NotNil(t, cmd.RunE)
}

func TestNewQueueStateCommand(t *testing.T) {
	cmd := newQueueStateCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "state <queue-name>", cmd.Use)
	assert.Equal(t, "Get queue state information", cmd.Short)
	assert.Contains(t, cmd.Long, "Get detailed state information for a specific queue")
	assert.NotNil(t, cmd.RunE)

	// Check that command requires exactly one argument
	assert.NotNil(t, cmd.Args)
}

func TestQueueCreateCommand_DefaultFlags(t *testing.T) {
	cmd := newQueueCreateCommand()

	// Test default flag values
	queueType, err := cmd.Flags().GetString("type")
	assert.NoError(t, err)
	assert.Equal(t, "simple", queueType)

	dequeueAttempts, err := cmd.Flags().GetInt32("dequeue-attempts")
	assert.NoError(t, err)
	assert.Equal(t, int32(3), dequeueAttempts)

	leaseDuration, err := cmd.Flags().GetString("lease-duration")
	assert.NoError(t, err)
	assert.Equal(t, "30s", leaseDuration)

	exclusivityKey, err := cmd.Flags().GetString("exclusivity-key")
	assert.NoError(t, err)
	assert.Equal(t, "", exclusivityKey)

	invisibilityDuration, err := cmd.Flags().GetString("invisibility-duration")
	assert.NoError(t, err)
	assert.Equal(t, "0s", invisibilityDuration)
}

func TestQueueCreateCommand_FlagSettings(t *testing.T) {
	cmd := newQueueCreateCommand()

	// Test setting flags
	tests := []struct {
		flagName     string
		flagValue    string
		expectedType interface{}
	}{
		{"type", "exclusive", "string"},
		{"dequeue-attempts", "5", "int32"},
		{"lease-duration", "60s", "string"},
		{"exclusivity-key", "test-key", "string"},
		{"invisibility-duration", "10s", "string"},
	}

	for _, tt := range tests {
		err := cmd.Flags().Set(tt.flagName, tt.flagValue)
		assert.NoError(t, err, "Setting flag %s should not error", tt.flagName)

		// Verify the flag was set correctly
		flag := cmd.Flags().Lookup(tt.flagName)
		assert.NotNil(t, flag)
		assert.Equal(t, tt.flagValue, flag.Value.String())
	}
}

func TestQueueDeleteCommand_DefaultFlags(t *testing.T) {
	cmd := newQueueDeleteCommand()

	// Test default flag values
	force, err := cmd.Flags().GetBool("force")
	assert.NoError(t, err)
	assert.False(t, force)
}

func TestQueueCommands_ArgumentValidation(t *testing.T) {
	tests := []struct {
		name        string
		cmd         *cobra.Command
		args        []string
		expectError bool
	}{
		{
			name:        "create command with valid args",
			cmd:         newQueueCreateCommand(),
			args:        []string{"test-queue"},
			expectError: false,
		},
		{
			name:        "create command with no args",
			cmd:         newQueueCreateCommand(),
			args:        []string{},
			expectError: true,
		},
		{
			name:        "create command with too many args",
			cmd:         newQueueCreateCommand(),
			args:        []string{"queue1", "queue2"},
			expectError: true,
		},
		{
			name:        "delete command with valid args",
			cmd:         newQueueDeleteCommand(),
			args:        []string{"test-queue"},
			expectError: false,
		},
		{
			name:        "delete command with no args",
			cmd:         newQueueDeleteCommand(),
			args:        []string{},
			expectError: true,
		},
		{
			name:        "list command with no args",
			cmd:         newQueueListCommand(),
			args:        []string{},
			expectError: false,
		},
		{
			name:        "state command with valid args",
			cmd:         newQueueStateCommand(),
			args:        []string{"test-queue"},
			expectError: false,
		},
		{
			name:        "state command with no args",
			cmd:         newQueueStateCommand(),
			args:        []string{},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.cmd.Args != nil {
				err := tt.cmd.Args(tt.cmd, tt.args)
				if tt.expectError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
			}
		})
	}
}

func TestQueueCommand_Flags(t *testing.T) {
	cmd := newQueueCreateCommand()

	// Test that long flags are properly defined
	longFlags := []string{"type", "dequeue-attempts", "lease-duration", "exclusivity-key", "invisibility-duration"}

	for _, longFlag := range longFlags {
		flag := cmd.Flags().Lookup(longFlag)
		assert.NotNil(t, flag, "Long flag %s should exist", longFlag)
	}
}
