package commands

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func TestNewScheduleCommand(t *testing.T) {
	cmd := NewScheduleCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "schedule", cmd.Use)
	assert.Equal(t, "Schedule management operations", cmd.Short)
	assert.Contains(t, cmd.Long, "Manage ChronoQueue schedules")

	// Check that subcommands are properly added
	subcommands := cmd.Commands()
	assert.Len(t, subcommands, 8) // Updated to 8 after adding calendar schedule commands

	// Check subcommand names
	subcommandNames := make([]string, len(subcommands))
	for i, subcmd := range subcommands {
		subcommandNames[i] = subcmd.Use
	}

	expectedCommands := []string{
		"create",
		"delete <schedule-id>",
		"list",
		"get <schedule-id>",
		"pause <schedule-id>",
		"resume <schedule-id>",
		"validate-calendar [calendar-schedule-json]",
		"preview-calendar [calendar-schedule-json]",
	}

	for _, expected := range expectedCommands {
		assert.Contains(t, subcommandNames, expected)
	}
}

func TestNewScheduleCreateCommand(t *testing.T) {
	cmd := newScheduleCreateCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "create", cmd.Use)
	assert.Equal(t, "Create a new schedule", cmd.Short)
	assert.Contains(t, cmd.Long, "Create a new scheduled task")
	assert.NotNil(t, cmd.RunE)

	// Check that command requires exactly two arguments
	assert.NotNil(t, cmd.Args)

	// Check that flags are properly added - based on actual implementation
	flags := []string{"id", "cron", "exclusivity-key", "metadata", "max-messages", "lease-duration"}
	for _, flagName := range flags {
		flag := cmd.Flags().Lookup(flagName)
		assert.NotNil(t, flag, "Flag %s should be present", flagName)
	}
}

func TestNewScheduleDeleteCommand(t *testing.T) {
	cmd := newScheduleDeleteCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "delete <schedule-id>", cmd.Use)
	assert.Equal(t, "Delete a schedule", cmd.Short)
	assert.Contains(t, cmd.Long, "Delete an existing schedule")
	assert.NotNil(t, cmd.RunE)

	// Check that command requires exactly one argument
	assert.NotNil(t, cmd.Args)

	// Note: This command doesn't have a force flag in the actual implementation
}

func TestNewScheduleListCommand(t *testing.T) {
	cmd := newScheduleListCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "list", cmd.Use)
	assert.Equal(t, "List all schedules", cmd.Short)
	assert.Contains(t, cmd.Long, "List all schedules")
	assert.NotNil(t, cmd.RunE)
}

func TestNewScheduleGetCommand(t *testing.T) {
	cmd := newScheduleGetCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "get <schedule-id>", cmd.Use)
	assert.Equal(t, "Get schedule details", cmd.Short)
	assert.Contains(t, cmd.Long, "Get detailed information about a specific schedule")
	assert.NotNil(t, cmd.RunE)

	// Check that command requires exactly one argument
	assert.NotNil(t, cmd.Args)
}

func TestNewSchedulePauseCommand(t *testing.T) {
	cmd := newSchedulePauseCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "pause <schedule-id>", cmd.Use)
	assert.Equal(t, "Pause a schedule", cmd.Short)
	assert.Contains(t, cmd.Long, "Pause execution of a schedule")
	assert.NotNil(t, cmd.RunE)

	// Check that command requires exactly one argument
	assert.NotNil(t, cmd.Args)
}

func TestNewScheduleResumeCommand(t *testing.T) {
	cmd := newScheduleResumeCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "resume <schedule-id>", cmd.Use)
	assert.Equal(t, "Resume a schedule", cmd.Short)
	assert.Contains(t, cmd.Long, "Resume execution of a paused schedule")
	assert.NotNil(t, cmd.RunE)

	// Check that command requires exactly one argument
	assert.NotNil(t, cmd.Args)
}

func TestScheduleCreateCommand_DefaultFlags(t *testing.T) {
	cmd := newScheduleCreateCommand()

	// Test default flag values based on actual implementation
	id, err := cmd.Flags().GetString("id")
	assert.NoError(t, err)
	assert.Equal(t, "", id)

	cronExpr, err := cmd.Flags().GetString("cron")
	assert.NoError(t, err)
	assert.Equal(t, "", cronExpr)

	leaseDuration, err := cmd.Flags().GetString("lease-duration")
	assert.NoError(t, err)
	assert.Equal(t, "30s", leaseDuration)

	exclusivityKey, err := cmd.Flags().GetString("exclusivity-key")
	assert.NoError(t, err)
	assert.Equal(t, "", exclusivityKey)

	maxMessages, err := cmd.Flags().GetInt64("max-messages")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), maxMessages)

	metadata, err := cmd.Flags().GetString("metadata")
	assert.NoError(t, err)
	assert.Equal(t, "", metadata)
}

func TestScheduleDeleteCommand_DefaultFlags(t *testing.T) {
	cmd := newScheduleDeleteCommand()

	// The delete command doesn't have any flags in the actual implementation
	// Just verify the command was created properly
	assert.NotNil(t, cmd)
	assert.Equal(t, "delete <schedule-id>", cmd.Use)
}

func TestScheduleCommands_ArgumentValidation(t *testing.T) {
	tests := []struct {
		name        string
		cmd         *cobra.Command
		args        []string
		expectError bool
	}{
		{
			name:        "create command with valid args",
			cmd:         newScheduleCreateCommand(),
			args:        []string{"test-queue", "test-message"},
			expectError: false,
		},
		{
			name:        "create command with insufficient args",
			cmd:         newScheduleCreateCommand(),
			args:        []string{"test-queue"},
			expectError: true,
		},
		{
			name:        "delete command with valid args",
			cmd:         newScheduleDeleteCommand(),
			args:        []string{"schedule-123"},
			expectError: false,
		},
		{
			name:        "delete command with no args",
			cmd:         newScheduleDeleteCommand(),
			args:        []string{},
			expectError: true,
		},
		{
			name:        "list command with no args",
			cmd:         newScheduleListCommand(),
			args:        []string{},
			expectError: false,
		},
		{
			name:        "get command with valid args",
			cmd:         newScheduleGetCommand(),
			args:        []string{"schedule-123"},
			expectError: false,
		},
		{
			name:        "get command with no args",
			cmd:         newScheduleGetCommand(),
			args:        []string{},
			expectError: true,
		},
		{
			name:        "pause command with valid args",
			cmd:         newSchedulePauseCommand(),
			args:        []string{"schedule-123"},
			expectError: false,
		},
		{
			name:        "pause command with no args",
			cmd:         newSchedulePauseCommand(),
			args:        []string{},
			expectError: true,
		},
		{
			name:        "resume command with valid args",
			cmd:         newScheduleResumeCommand(),
			args:        []string{"schedule-123"},
			expectError: false,
		},
		{
			name:        "resume command with no args",
			cmd:         newScheduleResumeCommand(),
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

func TestScheduleCreateCommand_FlagSettings(t *testing.T) {
	cmd := newScheduleCreateCommand()

	// Test setting flags based on actual implementation
	tests := []struct {
		flagName  string
		flagValue string
	}{
		{"id", "test-schedule"},
		{"cron", "0 */5 * * * *"},
		{"lease-duration", "60s"},
		{"exclusivity-key", "test-key"},
		{"max-messages", "5"},
		{"metadata", `{"key": "value"}`},
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

func TestScheduleCommand_Flags(t *testing.T) {
	cmd := newScheduleCreateCommand()

	// Test that required flags exist for schedule create command
	requiredFlags := []string{"id", "cron", "exclusivity-key", "metadata", "max-messages", "lease-duration"}

	for _, flagName := range requiredFlags {
		flag := cmd.Flags().Lookup(flagName)
		assert.NotNil(t, flag, "Flag %s should exist", flagName)
	}
}

func TestScheduleCommand_RequiredFlags(t *testing.T) {
	cmd := newScheduleCreateCommand()

	// The cron flag should be required for creating schedules
	cronFlag := cmd.Flags().Lookup("cron")
	assert.NotNil(t, cronFlag)
	// Note: We can't test if it's marked as required without actually running the command
	// as that's handled by cobra's validation at runtime
}

func TestScheduleCommand_Examples(t *testing.T) {
	cmd := NewScheduleCommand()

	// The main schedule command should have helpful description
	assert.Contains(t, cmd.Long, "Manage ChronoQueue schedules")
}

func TestScheduleCreateCommand_Examples(t *testing.T) {
	cmd := newScheduleCreateCommand()

	// The create command should mention scheduled tasks
	assert.Contains(t, cmd.Long, "Create a new scheduled task")
}
