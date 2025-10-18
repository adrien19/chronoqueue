package commands

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/cmd/chronoq/outputs"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/structpb"
)

// NewScheduleCommand creates the schedule command group
func NewScheduleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Schedule management operations",
		Long:  `Manage ChronoQueue schedules - create, delete, list, and manage scheduled tasks.`,
	}

	cmd.AddCommand(newScheduleCreateCommand())
	cmd.AddCommand(newScheduleDeleteCommand())
	cmd.AddCommand(newScheduleListCommand())
	cmd.AddCommand(newScheduleGetCommand())
	cmd.AddCommand(newSchedulePauseCommand())
	cmd.AddCommand(newScheduleResumeCommand())

	return cmd
}

// newScheduleCreateCommand creates the schedule create subcommand
func newScheduleCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a new schedule",
		Long:  `Create a new scheduled task.`,
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			queueName := args[0]
			messageData := args[1]

			// Parse flags
			cronExpr, err := cmd.Flags().GetString("cron")
			if err != nil {
				return fmt.Errorf("failed to get cron flag: %w", err)
			}

			leaseDuration, err := cmd.Flags().GetString("lease-duration")
			if err != nil {
				return err
			}

			exclusivityKey, err := cmd.Flags().GetString("exclusivity-key")
			if err != nil {
				return err
			}

			maxMessages, err := cmd.Flags().GetInt64("max-messages")
			if err != nil {
				return err
			}

			metadataStr, err := cmd.Flags().GetString("metadata")
			if err != nil {
				return err
			}

			// Convert string data to structpb.Struct
			var dataStruct *structpb.Struct
			if messageData != "" {
				// Try to parse as JSON first, if that fails, treat as simple string value
				var jsonData interface{}
				var err error
				if jsonErr := json.Unmarshal([]byte(messageData), &jsonData); jsonErr == nil {
					dataStruct, err = structpb.NewStruct(map[string]interface{}{
						"value": jsonData,
					})
				} else {
					// If not valid JSON, store as a simple string value
					dataStruct, err = structpb.NewStruct(map[string]interface{}{
						"value": messageData,
					})
				}
				if err != nil {
					return fmt.Errorf("failed to create data struct: %w", err)
				}
			}

			// Parse metadata JSON if provided
			var metadataMap map[string]*structpb.Value
			if metadataStr != "" {
				var metadataJson map[string]interface{}
				if err := json.Unmarshal([]byte(metadataStr), &metadataJson); err != nil {
					return fmt.Errorf("failed to parse metadata JSON: %w", err)
				}

				metadataMap = make(map[string]*structpb.Value)
				for k, v := range metadataJson {
					val, err := structpb.NewValue(v)
					if err != nil {
						return fmt.Errorf("failed to convert metadata value for key %s: %w", k, err)
					}
					metadataMap[k] = val
				}
			}

			// Use the client to create the schedule
			cronOpts := client.ScheduleOptions{
				CronSchedule:   cronExpr,
				QueueName:      queueName,
				ExclusivityKey: exclusivityKey,
				MaxMessages:    maxMessages,
				LeaseDuration:  leaseDuration,
				Payload: client.Payload{
					Data:     dataStruct,
					Metadata: metadataMap,
				},
			}

			// Parse additional flags
			scheduleID, _ := cmd.Flags().GetString("id")
			if scheduleID == "" {
				scheduleID = fmt.Sprintf("schedule-%d", time.Now().Unix())
			}

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				resp, err := client.CreateSchedule(cmd.Context(), scheduleID, cronOpts)
				if err != nil {
					return fmt.Errorf("failed to create schedule: %w", err)
				}

				outputs.PrintSuccess(fmt.Sprintf("Success %t", resp.GetSuccess()))
				return nil
			})
		},
	}

	cmd.Flags().StringP("id", "i", "", "Schedule ID (auto-generated if not provided)")
	cmd.Flags().StringP("cron", "c", "", "Cron expression for the schedule")
	cmd.Flags().StringP("exclusivity-key", "k", "", "Exclusivity key for exclusive queues")
	cmd.Flags().StringP("metadata", "d", "", "Message metadata as JSON")
	cmd.Flags().Int64P("max-messages", "m", 1, "Maximum number of messages to send per schedule run")
	cmd.Flags().StringP("lease-duration", "l", "30s", "Lease duration for the scheduled messages in seconds")
	cmd.MarkFlagRequired("cron")

	return cmd
}

// newScheduleDeleteCommand creates the schedule delete subcommand
func newScheduleDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <schedule-id>",
		Short: "Delete a schedule",
		Long:  `Delete an existing schedule.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			scheduleID := args[0]

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				ctx, cancel := CreateContext(cmd)
				defer cancel()

				response, err := client.DeleteSchedule(ctx, scheduleID)
				if err != nil {
					return fmt.Errorf("failed to delete schedule: %w", err)
				}

				if response.Success {
					outputs.PrintSuccess(fmt.Sprintf("Schedule '%s' deleted", scheduleID))
				} else {
					return fmt.Errorf("failed to delete schedule")
				}

				return nil
			})
		},
	}

	return cmd
}

// newScheduleListCommand creates the schedule list subcommand
func newScheduleListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all schedules",
		Long:  `List all schedules.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				ctx, cancel := CreateContext(cmd)
				defer cancel()

				response, err := client.ListSchedules(ctx, "")
				if err != nil {
					return fmt.Errorf("failed to list schedules: %w", err)
				}

				formatter := outputs.NewOutputFormatter(GetOutputFormat(cmd))

				if len(response.Schedules) == 0 {
					outputs.PrintInfo("No schedules found")
					return nil
				}

				return formatter.Print(response.Schedules)
			})
		},
	}

	return cmd
}

// newScheduleGetCommand creates the schedule get subcommand
func newScheduleGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <schedule-id>",
		Short: "Get schedule details",
		Long:  `Get detailed information about a specific schedule.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			scheduleID := args[0]

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				ctx, cancel := CreateContext(cmd)
				defer cancel()

				response, err := client.GetSchedule(ctx, scheduleID)
				if err != nil {
					return fmt.Errorf("failed to get schedule: %w", err)
				}

				formatter := outputs.NewOutputFormatter(GetOutputFormat(cmd))
				return formatter.Print(response.Schedule)
			})
		},
	}

	return cmd
}

// newSchedulePauseCommand creates the schedule pause subcommand
func newSchedulePauseCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pause <schedule-id>",
		Short: "Pause a schedule",
		Long:  `Pause execution of a schedule.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			scheduleID := args[0]

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				ctx, cancel := CreateContext(cmd)
				defer cancel()

				response, err := client.PauseSchedule(ctx, scheduleID)
				if err != nil {
					return fmt.Errorf("failed to pause schedule: %w", err)
				}

				if response.Success {
					outputs.PrintSuccess(fmt.Sprintf("Schedule '%s' paused", scheduleID))
				} else {
					return fmt.Errorf("failed to pause schedule")
				}

				return nil
			})
		},
	}

	return cmd
}

// newScheduleResumeCommand creates the schedule resume subcommand
func newScheduleResumeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resume <schedule-id>",
		Short: "Resume a schedule",
		Long:  `Resume execution of a paused schedule.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			scheduleID := args[0]

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				ctx, cancel := CreateContext(cmd)
				defer cancel()

				response, err := client.ResumeSchedule(ctx, scheduleID)
				if err != nil {
					return fmt.Errorf("failed to resume schedule: %w", err)
				}

				if response.Success {
					outputs.PrintSuccess(fmt.Sprintf("Schedule '%s' resumed", scheduleID))
				} else {
					return fmt.Errorf("failed to resume schedule")
				}

				return nil
			})
		},
	}

	return cmd
}
