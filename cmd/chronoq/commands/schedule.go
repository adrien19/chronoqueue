package commands

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	schedule_pb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/cmd/chronoq/outputs"
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
	cmd.AddCommand(newCalendarScheduleValidateCommand())
	cmd.AddCommand(newCalendarSchedulePreviewCommand())

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

			calendarScheduleJSON, err := cmd.Flags().GetString("calendar")
			if err != nil {
				return fmt.Errorf("failed to get calendar flag: %w", err)
			}

			// Validate that either cron or calendar is provided, but not both
			if cronExpr == "" && calendarScheduleJSON == "" {
				return fmt.Errorf("either --cron or --calendar must be provided")
			}
			if cronExpr != "" && calendarScheduleJSON != "" {
				return fmt.Errorf("cannot specify both --cron and --calendar")
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
			scheduleOpts := client.ScheduleOptions{
				QueueName:      queueName,
				ExclusivityKey: exclusivityKey,
				MaxMessages:    maxMessages,
				LeaseDuration:  leaseDuration,
				Payload: client.Payload{
					Data:     dataStruct,
					Metadata: metadataMap,
				},
			}

			// Set either cron or calendar schedule
			if calendarScheduleJSON != "" {
				// Parse calendar schedule JSON
				var calendarSchedule schedule_pb.CalendarSchedule
				if err := protojson.Unmarshal([]byte(calendarScheduleJSON), &calendarSchedule); err != nil {
					return fmt.Errorf("failed to parse calendar schedule JSON: %w", err)
				}
				scheduleOpts.CalendarSchedule = &calendarSchedule
			} else {
				scheduleOpts.CronSchedule = cronExpr
			}

			// Parse additional flags
			scheduleID, _ := cmd.Flags().GetString("id")
			if scheduleID == "" {
				scheduleID = fmt.Sprintf("schedule-%d", time.Now().Unix())
			}

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				resp, err := client.CreateSchedule(cmd.Context(), scheduleID, scheduleOpts)
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
	cmd.Flags().StringP("calendar", "", "", "Calendar schedule configuration as JSON")
	cmd.Flags().StringP("exclusivity-key", "k", "", "Exclusivity key for exclusive queues")
	cmd.Flags().StringP("metadata", "d", "", "Message metadata as JSON")
	cmd.Flags().Int64P("max-messages", "m", 1, "Maximum number of messages to send per schedule run")
	cmd.Flags().StringP("lease-duration", "l", "30s", "Lease duration for the scheduled messages in seconds")

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

// newCalendarScheduleValidateCommand creates the calendar schedule validate subcommand
func newCalendarScheduleValidateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate-calendar [calendar-schedule-json]",
		Short: "Validate a calendar schedule configuration",
		Long:  `Validate a calendar schedule configuration before creating it. Accepts calendar schedule JSON.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			calendarScheduleJSON := args[0]

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				ctx, cancel := CreateContext(cmd)
				defer cancel()

				response, err := client.ValidateCalendarSchedule(ctx, calendarScheduleJSON)
				if err != nil {
					return fmt.Errorf("failed to validate calendar schedule: %w", err)
				}

				if response.Valid {
					outputs.PrintSuccess("Calendar schedule is valid")
				} else {
					outputs.PrintError(fmt.Sprintf("Calendar schedule validation failed: %s", response.ErrorMessage))
					if len(response.ValidationIssues) > 0 {
						fmt.Println("\nValidation Issues:")
						for _, issue := range response.ValidationIssues {
							fmt.Printf("  [%s] %s: %s\n", issue.Severity, issue.Field, issue.Message)
							if issue.Suggestion != "" {
								fmt.Printf("    Suggestion: %s\n", issue.Suggestion)
							}
						}
					}
					return fmt.Errorf("validation failed")
				}

				return nil
			})
		},
	}

	return cmd
}

// newCalendarSchedulePreviewCommand creates the calendar schedule preview subcommand
func newCalendarSchedulePreviewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "preview-calendar [calendar-schedule-json]",
		Short: "Preview execution times for a calendar schedule",
		Long:  `Preview upcoming execution times for a calendar schedule configuration. Accepts calendar schedule JSON.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			calendarScheduleJSON := args[0]

			count, err := cmd.Flags().GetInt32("count")
			if err != nil {
				return err
			}

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				ctx, cancel := CreateContext(cmd)
				defer cancel()

				response, err := client.PreviewCalendarSchedule(ctx, calendarScheduleJSON, count)
				if err != nil {
					return fmt.Errorf("failed to preview calendar schedule: %w", err)
				}

				outputs.PrintSuccess(fmt.Sprintf("Calendar Schedule Preview (%d executions)", response.TotalCount))
				fmt.Printf("\nTimezone: %s\n", response.Timezone)
				fmt.Printf("Preview Start: %s\n\n", response.PreviewStart.AsTime().Format(time.RFC3339))

				if len(response.ExecutionTimes) > 0 {
					fmt.Println("Upcoming Execution Times:")
					for i, ts := range response.ExecutionTimes {
						fmt.Printf("  %2d. %s\n", i+1, ts.AsTime().Format(time.RFC3339))
					}
				} else {
					fmt.Println("No execution times found in the preview period.")
				}

				return nil
			})
		},
	}

	cmd.Flags().Int32("count", 10, "Number of execution times to preview (max 100)")

	return cmd
}
