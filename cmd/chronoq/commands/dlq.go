package commands

import (
	"fmt"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/cmd/chronoq/outputs"
	"github.com/spf13/cobra"
)

// NewDLQCommand creates the Dead Letter Queue command group
func NewDLQCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dlq",
		Short: "Dead Letter Queue management operations",
		Long:  `Manage Dead Letter Queues - list messages, requeue, delete, purge, and get statistics.`,
	}

	cmd.AddCommand(newDLQListCommand())
	cmd.AddCommand(newDLQRequeueCommand())
	cmd.AddCommand(newDLQDeleteCommand())
	cmd.AddCommand(newDLQPurgeCommand())
	cmd.AddCommand(newDLQStatsCommand())

	return cmd
}

// newDLQListCommand creates the dlq list subcommand
func newDLQListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list <dlq-name>",
		Short: "List messages in a Dead Letter Queue",
		Long:  `List all messages currently in the specified Dead Letter Queue.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dlqName := args[0]
			limit, _ := cmd.Flags().GetInt32("limit")

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				resp, err := client.GetDLQMessages(cmd.Context(), dlqName, limit)
				if err != nil {
					return err
				}

				outputFormat := GetOutputFormat(cmd)
				formatter := outputs.NewOutputFormatter(outputFormat)

				if len(resp.Messages) == 0 {
					outputs.PrintInfo("No messages found in DLQ")
					return nil
				}

				if outputFormat == outputs.OutputJSON || outputFormat == outputs.OutputYAML {
					return formatter.Print(resp)
				}

				// Simple text output for table format
				outputs.PrintInfo(fmt.Sprintf("Messages in DLQ '%s': %d", dlqName, len(resp.Messages)))
				for i, msg := range resp.Messages {
					outputs.PrintInfo(fmt.Sprintf("Message %d:", i+1))
					outputs.PrintInfo(fmt.Sprintf("  ID: %s", msg.MessageId))

					if msg.Metadata != nil {
						outputs.PrintInfo(fmt.Sprintf("  State: %s", msg.Metadata.State.String()))
						outputs.PrintInfo(fmt.Sprintf("  Attempts Left: %d", msg.Metadata.AttemptsLeft))
						outputs.PrintInfo(fmt.Sprintf("  Max Attempts: %d", msg.Metadata.MaxAttempts))
						outputs.PrintInfo(fmt.Sprintf("  Priority: %d", msg.Metadata.Priority))
					}
					outputs.PrintInfo("")
				}
				return nil
			})
		},
	}

	cmd.Flags().Int32P("limit", "l", 100, "Maximum number of messages to retrieve")

	return cmd
}

// newDLQRequeueCommand creates the dlq requeue subcommand
func newDLQRequeueCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "requeue <dlq-name> <message-id> <target-queue>",
		Short: "Requeue a message from DLQ back to a queue",
		Long:  `Move a message from the Dead Letter Queue back to its original queue or specified target queue.`,
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			dlqName := args[0]
			messageID := args[1]
			targetQueue := args[2]

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				resp, err := client.RequeueFromDLQ(cmd.Context(), dlqName, messageID, targetQueue)
				if err != nil {
					return err
				}

				if resp.Success {
					outputs.PrintSuccess(fmt.Sprintf("Message '%s' successfully requeued from DLQ '%s' to queue '%s'", messageID, dlqName, targetQueue))
				} else {
					outputs.PrintError("Failed to requeue message")
				}
				return nil
			})
		},
	}

	return cmd
}

// newDLQDeleteCommand creates the dlq delete subcommand
func newDLQDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <dlq-name> <message-id>",
		Short: "Delete a message from DLQ",
		Long:  `Permanently delete a message from the Dead Letter Queue.`,
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dlqName := args[0]
			messageID := args[1]
			force, _ := cmd.Flags().GetBool("force")

			if !force {
				fmt.Printf("Are you sure you want to permanently delete message '%s' from DLQ '%s'? This action cannot be undone. [y/N]: ", messageID, dlqName)
				var confirm string
				fmt.Scanln(&confirm)
				if confirm != "y" && confirm != "Y" {
					outputs.PrintInfo("Operation cancelled")
					return nil
				}
			}

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				resp, err := client.DeleteFromDLQ(cmd.Context(), dlqName, messageID)
				if err != nil {
					return err
				}

				if resp.Success {
					outputs.PrintSuccess(fmt.Sprintf("Message '%s' successfully deleted from DLQ '%s'", messageID, dlqName))
				} else {
					outputs.PrintError("Failed to delete message")
				}
				return nil
			})
		},
	}

	cmd.Flags().BoolP("force", "f", false, "Skip confirmation prompt")

	return cmd
}

// newDLQPurgeCommand creates the dlq purge subcommand
func newDLQPurgeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "purge <dlq-name>",
		Short: "Purge all messages from DLQ",
		Long:  `Remove all messages from the specified Dead Letter Queue. This operation cannot be undone.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dlqName := args[0]
			force, _ := cmd.Flags().GetBool("force")

			if !force {
				fmt.Printf("Are you sure you want to purge ALL messages from DLQ '%s'? This action cannot be undone. [y/N]: ", dlqName)
				var confirm string
				fmt.Scanln(&confirm)
				if confirm != "y" && confirm != "Y" {
					outputs.PrintInfo("Operation cancelled")
					return nil
				}
			}

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				resp, err := client.PurgeDLQ(cmd.Context(), dlqName)
				if err != nil {
					return err
				}

				if resp.Success {
					outputs.PrintSuccess(fmt.Sprintf("All messages successfully purged from DLQ '%s'", dlqName))
				} else {
					outputs.PrintError("Failed to purge DLQ")
				}
				return nil
			})
		},
	}

	cmd.Flags().BoolP("force", "f", false, "Skip confirmation prompt")

	return cmd
}

// newDLQStatsCommand creates the dlq stats subcommand
func newDLQStatsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stats <dlq-name>",
		Short: "Get DLQ statistics",
		Long:  `Get statistics about the specified Dead Letter Queue, including message count and timestamps.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dlqName := args[0]

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				resp, err := client.GetDLQStats(cmd.Context(), dlqName)
				if err != nil {
					return err
				}

				outputFormat := GetOutputFormat(cmd)
				formatter := outputs.NewOutputFormatter(outputFormat)

				if outputFormat == outputs.OutputJSON || outputFormat == outputs.OutputYAML {
					return formatter.Print(resp)
				}

				// Simple text output for table format
				outputs.PrintInfo(fmt.Sprintf("DLQ Statistics: %s", resp.Name))
				outputs.PrintInfo(fmt.Sprintf("Message Count: %d", resp.MessageCount))

				if resp.CreatedAt > 0 {
					outputs.PrintInfo(fmt.Sprintf("Created At: %s", formatUnixTimestamp(resp.CreatedAt)))
				}

				if resp.UpdatedAt > 0 {
					outputs.PrintInfo(fmt.Sprintf("Updated At: %s", formatUnixTimestamp(resp.UpdatedAt)))
				}
				return nil
			})
		},
	}

	return cmd
}

// formatUnixTimestamp formats a Unix timestamp to a human-readable string
func formatUnixTimestamp(timestamp int64) string {
	if timestamp == 0 {
		return "N/A"
	}
	// For now, just return the timestamp as string
	// In a real implementation, you'd format it properly
	return fmt.Sprintf("%d", timestamp)
}
