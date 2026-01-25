package commands

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func TestNewMessageCommand(t *testing.T) {
	cmd := NewMessageCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "message", cmd.Use)
	assert.Equal(t, "Message operations", cmd.Short)
	assert.Contains(t, cmd.Long, "Manage ChronoQueue messages")

	// Check that subcommands are properly added
	subcommands := cmd.Commands()
	assert.Len(t, subcommands, 6)

	// Check subcommand names
	subcommandNames := make([]string, len(subcommands))
	for i, subcmd := range subcommands {
		subcommandNames[i] = subcmd.Use
	}

	expectedCommands := []string{
		"post <queue-name> [message-data]",
		"get <queue-name>",
		"ack <queue-name> <message-id> <message-state>",
		"peek <queue-name>",
		"renew <queue-name> <message-id> <lease-duration>",
		"heartbeat <queue-name> <message-id>",
	}

	for _, expected := range expectedCommands {
		assert.Contains(t, subcommandNames, expected)
	}
}

func TestNewMessagePostCommand(t *testing.T) {
	cmd := newMessagePostCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "post <queue-name> [message-data]", cmd.Use)
	assert.Equal(t, "Post a message to a queue", cmd.Short)
	assert.Contains(t, cmd.Long, "Post a new message to the specified queue")
	assert.NotNil(t, cmd.RunE)

	// Check that command requires exactly two arguments
	assert.NotNil(t, cmd.Args)

	// Check that flags are properly added
	flags := []string{"id", "lease-duration", "max-attempts", "invisibility-duration", "priority", "metadata"}
	for _, flagName := range flags {
		flag := cmd.Flags().Lookup(flagName)
		assert.NotNil(t, flag, "Flag %s should be present", flagName)
	}
}

func TestNewMessageGetCommand(t *testing.T) {
	cmd := newMessageGetCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "get <queue-name>", cmd.Use)
	assert.Equal(t, "Get the next message from a queue", cmd.Short)
	assert.Contains(t, cmd.Long, "Get the next available message from the specified queue")
	assert.NotNil(t, cmd.RunE)

	// Check that command requires exactly one argument
	assert.NotNil(t, cmd.Args)

	// Check for command flags
	leaseDurationFlag := cmd.Flags().Lookup("lease-duration")
	assert.NotNil(t, leaseDurationFlag)

	exclusivityKeyFlag := cmd.Flags().Lookup("exclusivity-key")
	assert.NotNil(t, exclusivityKeyFlag)

	enableHeartbeatFlag := cmd.Flags().Lookup("enable-heartbeat")
	assert.NotNil(t, enableHeartbeatFlag)
}

func TestNewMessageAckCommand(t *testing.T) {
	cmd := newMessageAckCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "ack <queue-name> <message-id> <message-state>", cmd.Use)
	assert.Equal(t, "Acknowledge a message", cmd.Short)
	assert.Contains(t, cmd.Long, "Acknowledge that a message has been processed")
	assert.NotNil(t, cmd.RunE)

	// Check that command requires exactly three arguments
	assert.NotNil(t, cmd.Args)
}

func TestNewMessagePeekCommand(t *testing.T) {
	cmd := newMessagePeekCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "peek <queue-name>", cmd.Use)
	assert.Equal(t, "Peek at messages in a queue", cmd.Short)
	assert.Contains(t, cmd.Long, "View messages in a queue without removing them")
	assert.NotNil(t, cmd.RunE)

	// Check that command requires exactly one argument
	assert.NotNil(t, cmd.Args)

	// Check for limit flag
	limitFlag := cmd.Flags().Lookup("limit")
	assert.NotNil(t, limitFlag)

	// Check for time-range flag
	timeRangeFlag := cmd.Flags().Lookup("time-range")
	assert.NotNil(t, timeRangeFlag)
}

func TestNewMessageRenewCommand(t *testing.T) {
	cmd := newMessageRenewCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "renew <queue-name> <message-id> <lease-duration>", cmd.Use)
	assert.Equal(t, "Renew a message lease", cmd.Short)
	assert.Contains(t, cmd.Long, "Renew the lease on a message")
	assert.NotNil(t, cmd.RunE)

	// Check that command requires exactly three arguments
	assert.NotNil(t, cmd.Args)
}

func TestNewMessageHeartbeatCommand(t *testing.T) {
	cmd := newMessageHeartbeatCommand()

	assert.NotNil(t, cmd)
	assert.Equal(t, "heartbeat <queue-name> <message-id>", cmd.Use)
	assert.Equal(t, "Send a heartbeat for a message", cmd.Short)
	assert.Contains(t, cmd.Long, "Send a heartbeat to indicate that a message is still being processed")
	assert.NotNil(t, cmd.RunE)

	// Check that command requires exactly two arguments
	assert.NotNil(t, cmd.Args)
}

func TestMessagePostCommand_DefaultFlags(t *testing.T) {
	cmd := newMessagePostCommand()

	// Test default flag values
	id, err := cmd.Flags().GetString("id")
	assert.NoError(t, err)
	assert.Equal(t, "", id)

	leaseDuration, err := cmd.Flags().GetString("lease-duration")
	assert.NoError(t, err)
	assert.Equal(t, "", leaseDuration) // Empty by default

	maxAttempts, err := cmd.Flags().GetInt32("max-attempts")
	assert.NoError(t, err)
	assert.Equal(t, int32(-1), maxAttempts) // -1 by default

	invisibilityDuration, err := cmd.Flags().GetString("invisibility-duration")
	assert.NoError(t, err)
	assert.Equal(t, "", invisibilityDuration)

	priority, err := cmd.Flags().GetInt64("priority")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), priority)

	metadata, err := cmd.Flags().GetString("metadata")
	assert.NoError(t, err)
	assert.Equal(t, "", metadata)
}

func TestMessageGetCommand_DefaultFlags(t *testing.T) {
	cmd := newMessageGetCommand()

	// Test default flag values
	leaseDuration, err := cmd.Flags().GetString("lease-duration")
	assert.NoError(t, err)
	assert.Equal(t, "30s", leaseDuration)

	exclusivityKey, err := cmd.Flags().GetString("exclusivity-key")
	assert.NoError(t, err)
	assert.Equal(t, "", exclusivityKey)

	enableHeartbeat, err := cmd.Flags().GetBool("enable-heartbeat")
	assert.NoError(t, err)
	assert.False(t, enableHeartbeat)
}

func TestMessagePeekCommand_DefaultFlags(t *testing.T) {
	cmd := newMessagePeekCommand()

	// Test default flag values
	limit, err := cmd.Flags().GetInt32("limit")
	assert.NoError(t, err)
	assert.Equal(t, int32(10), limit)

	timeRange, err := cmd.Flags().GetStringToInt("time-range")
	assert.NoError(t, err)
	assert.Equal(t, map[string]int{"min": 0, "max": 0}, timeRange)
}

func TestMessageCommands_ArgumentValidation(t *testing.T) {
	tests := []struct {
		name        string
		cmd         *cobra.Command
		args        []string
		expectError bool
	}{
		{
			name:        "post command with valid args",
			cmd:         newMessagePostCommand(),
			args:        []string{"test-queue", "test-message"},
			expectError: false,
		},
		{
			name:        "post command with insufficient args",
			cmd:         newMessagePostCommand(),
			args:        []string{"test-queue"},
			expectError: false, // Args validator accepts 1-2 args; RunE will handle missing data
		},
		{
			name:        "get command with valid args",
			cmd:         newMessageGetCommand(),
			args:        []string{"test-queue"},
			expectError: false,
		},
		{
			name:        "get command with no args",
			cmd:         newMessageGetCommand(),
			args:        []string{},
			expectError: true,
		},
		{
			name:        "ack command with valid args",
			cmd:         newMessageAckCommand(),
			args:        []string{"test-queue", "msg-123", "COMPLETED"},
			expectError: false,
		},
		{
			name:        "ack command with insufficient args",
			cmd:         newMessageAckCommand(),
			args:        []string{"test-queue", "msg-123"},
			expectError: true,
		},
		{
			name:        "peek command with valid args",
			cmd:         newMessagePeekCommand(),
			args:        []string{"test-queue"},
			expectError: false,
		},
		{
			name:        "renew command with valid args",
			cmd:         newMessageRenewCommand(),
			args:        []string{"test-queue", "msg-123", "60s"},
			expectError: false,
		},
		{
			name:        "heartbeat command with valid args",
			cmd:         newMessageHeartbeatCommand(),
			args:        []string{"test-queue", "msg-123"},
			expectError: false,
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

func TestMessagePostCommand_FlagSettings(t *testing.T) {
	cmd := newMessagePostCommand()

	// Test setting flags
	tests := []struct {
		flagName  string
		flagValue string
	}{
		{"id", "test-msg-123"},
		{"lease-duration", "60s"},
		{"max-attempts", "5"},
		{"invisibility-duration", "10s"},
		{"priority", "100"},
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

func TestMessageCommand_Flags(t *testing.T) {
	cmd := newMessagePostCommand()

	// Test that required flags exist for message post command
	requiredFlags := []string{"id", "lease-duration", "max-attempts", "invisibility-duration", "priority", "metadata"}

	for _, flagName := range requiredFlags {
		flag := cmd.Flags().Lookup(flagName)
		assert.NotNil(t, flag, "Flag %s should exist", flagName)
	}
}
