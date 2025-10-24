package commands

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/cmd/chronoq/outputs"
)

// NewQueueCommand creates the queue command group
func NewQueueCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queue",
		Short: "Queue management operations",
		Long:  `Manage ChronoQueue queues - create, delete, list, and get queue state.`,
	}

	cmd.AddCommand(newQueueCreateCommand())
	cmd.AddCommand(newQueueDeleteCommand())
	cmd.AddCommand(newQueueListCommand())
	cmd.AddCommand(newQueueStateCommand())

	return cmd
}

// newQueueCreateCommand creates the queue create subcommand
func newQueueCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <queue-name>",
		Short: "Create a new queue",
		Long:  `Create a new ChronoQueue queue with the specified configuration.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			queueName := args[0]

			// Parse flags
			queueType, _ := cmd.Flags().GetString("type")
			queueTypeInt := client.ParseQueueType(queueType)

			dequeueAttempts, _ := cmd.Flags().GetInt32("default-max-attempts")
			leaseDurationStr, _ := cmd.Flags().GetString("lease-duration")
			exclusivityKey, _ := cmd.Flags().GetString("exclusivity-key")
			invisibilityDurationStr, _ := cmd.Flags().GetString("invisibility-duration")
			deadLetterQueueName, _ := cmd.Flags().GetString("dlq-name")
			autoCreateDLQ, _ := cmd.Flags().GetBool("auto-create-dlq")
			queueOpts := client.QueueOptions{
				Type:                 queueTypeInt,
				DequeueAttempts:      dequeueAttempts,
				LeaseDuration:        leaseDurationStr,
				ExclusivityKey:       exclusivityKey,
				InvisibilityDuration: invisibilityDurationStr,
				DeadLetterQueueName:  deadLetterQueueName,
				AutoCreateDLQ:        autoCreateDLQ,
			}

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				// Stub implementation for now
				resp, err := client.CreateQueue(cmd.Context(), queueName, queueOpts)
				if err != nil {
					return err
				}
				outputs.PrintInfo(fmt.Sprintf("Status: %v", resp.GetSuccess()))
				return nil
			})
		},
	}

	cmd.Flags().StringP("type", "t", "simple", "Queue type (simple, exclusive)")
	cmd.Flags().Int32P("default-max-attempts", "a", 3, "Default maximum attempts for messages in this queue")
	cmd.Flags().StringP("lease-duration", "l", "30s", "Default message lease duration")
	cmd.Flags().StringP("exclusivity-key", "k", "", "Exclusivity key (required for exclusive queues)")
	cmd.Flags().StringP("invisibility-duration", "i", "0s", "Message invisibility duration")
	cmd.Flags().StringP("dlq-name", "d", "", "Dead letter queue name for failed messages")
	cmd.Flags().BoolP("auto-create-dlq", "", false, "Automatically create the dead letter queue if it doesn't exist")

	return cmd
}

// newQueueDeleteCommand creates the queue delete subcommand
func newQueueDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <queue-name>",
		Short: "Delete a queue",
		Long:  `Delete an existing ChronoQueue queue and all its messages.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			queueName := args[0]
			force, _ := cmd.Flags().GetBool("force")

			if !force {
				fmt.Printf("Are you sure you want to delete queue '%s'? This action cannot be undone. [y/N]: ", queueName)
				var confirm string
				_, _ = fmt.Scanln(&confirm) // Ignore scan error, default to 'N'
				if confirm != "y" && confirm != "Y" {
					outputs.PrintInfo("Operation cancelled")
					return nil
				}
			}

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				resp, err := client.DeleteQueue(cmd.Context(), queueName)
				if err != nil {
					return err
				}
				outputs.PrintInfo(fmt.Sprintf("Status: %v", resp.GetSuccess()))
				return nil
			})
		},
	}

	cmd.Flags().BoolP("force", "f", false, "Force deletion without confirmation")

	return cmd
}

// newQueueListCommand creates the queue list subcommand
func newQueueListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all queues",
		Long:  `List all ChronoQueue queues with their metadata.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				queues, err := client.ListQueues(cmd.Context(), "")
				if err != nil {
					return err
				}
				outputs.PrintInfo(fmt.Sprintf("Queues: %d", len(queues.GetQueues())))
				for _, q := range queues.GetQueues() {
					outputs.PrintInfo(fmt.Sprintf("Queue: %s, Type: %s, Metadata: %v", q.Name, q.Metadata.Type, q.Metadata))
				}
				return nil
			})
		},
	}

	return cmd
}

// newQueueStateCommand creates the queue state subcommand
func newQueueStateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "state <queue-name>",
		Short: "Get queue state information",
		Long:  `Get detailed state information for a specific queue.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			queueName := args[0]

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				state, err := client.GetQueueState(cmd.Context(), queueName)
				if err != nil {
					return err
				}
				outputs.PrintInfo(fmt.Sprintf("Queue: %s", queueName))
				outputs.PrintInfo(fmt.Sprintf("Invisible Messages: %d", state.GetStateCounts()["INVISIBLE"]))
				outputs.PrintInfo(fmt.Sprintf("Pending Messages: %d", state.GetStateCounts()["PENDING"]))
				outputs.PrintInfo(fmt.Sprintf("Running Messages: %d", state.GetStateCounts()["RUNNING"]))
				outputs.PrintInfo(fmt.Sprintf("Errored Messages: %d", state.GetStateCounts()["ERRORED"]))
				outputs.PrintInfo(fmt.Sprintf("Completed Messages: %d", state.GetStateCounts()["COMPLETED"]))
				if state.GetEarliestDeadline() != nil {
					outputs.PrintInfo(fmt.Sprintf("Earliest Deadline: %s", state.GetEarliestDeadline().AsTime().String()))
				}
				return nil
			})
		},
	}

	return cmd
}

// Helper functions (removed unused parseDurationToPb and formatTimestamp)
