package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	serverAddr string
	insecure   bool
	verbose    bool
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "event-processor",
		Short: "Event Processing System Demo for ChronoQueue",
		Long:  `A comprehensive demonstration of high-throughput event processing with Redis Streams architecture.`,
	}

	rootCmd.PersistentFlags().StringVar(&serverAddr, "server", "host.docker.internal:9000", "ChronoQueue server address")
	rootCmd.PersistentFlags().BoolVar(&insecure, "insecure", true, "Use insecure connection (no TLS)")
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "Enable verbose output")

	rootCmd.AddCommand(
		newInitCommand(),
		newPublishCommand(),
		newWorkerCommand(),
		newStatsCommand(),
		newMonitorCommand(),
		newPeekCommand(),
		newDLQCommand(),
		newGenerateCommand(),
	)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func newInitCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Initialize event processing system (create queues)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			return initializeSystem(ctx)
		},
	}
}

func newPublishCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "publish <events-file.json>",
		Short: "Publish events from JSON file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			return publishEvents(ctx, args[0])
		},
	}
}

func newWorkerCommand() *cobra.Command {
	var (
		workerType string
		numWorkers int
		name       string
	)

	cmd := &cobra.Command{
		Use:   "worker",
		Short: "Start event processing workers",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			return startWorker(ctx, workerType, numWorkers, name)
		},
	}

	cmd.Flags().StringVar(&workerType, "type", "webhook", "Worker type: email, webhook, sms")
	cmd.Flags().IntVar(&numWorkers, "workers", 1, "Number of concurrent workers")
	cmd.Flags().StringVar(&name, "name", "", "Worker group name (default: auto-generated)")

	return cmd
}

func newStatsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "stats",
		Short: "Show queue statistics",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			return showStats(ctx)
		},
	}
}

func newMonitorCommand() *cobra.Command {
	var interval string

	cmd := &cobra.Command{
		Use:   "monitor",
		Short: "Monitor event processing in real-time",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			return monitorSystem(ctx, interval)
		},
	}

	cmd.Flags().StringVar(&interval, "interval", "2s", "Update interval")

	return cmd
}

func newPeekCommand() *cobra.Command {
	var limit int32

	cmd := &cobra.Command{
		Use:   "peek <queue-name>",
		Short: "Peek at pending events in queue",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			return peekQueue(ctx, args[0], limit)
		},
	}

	cmd.Flags().Int32Var(&limit, "limit", 10, "Maximum number of events to show")

	return cmd
}

func newDLQCommand() *cobra.Command {
	dlqCmd := &cobra.Command{
		Use:   "dlq",
		Short: "Manage Dead Letter Queue",
	}

	dlqCmd.AddCommand(
		&cobra.Command{
			Use:   "list",
			Short: "List failed events in DLQ",
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx := context.Background()
				return listDLQ(ctx)
			},
		},
		&cobra.Command{
			Use:   "inspect <event-id>",
			Short: "Inspect failed event details",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx := context.Background()
				return inspectDLQEvent(ctx, args[0])
			},
		},
		&cobra.Command{
			Use:   "requeue <event-id>",
			Short: "Requeue failed event for retry",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx := context.Background()
				return requeueFromDLQ(ctx, args[0])
			},
		},
		&cobra.Command{
			Use:   "delete <event-id>",
			Short: "Delete event from DLQ",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx := context.Background()
				return deleteFromDLQ(ctx, args[0])
			},
		},
		&cobra.Command{
			Use:   "purge",
			Short: "Purge all events from DLQ",
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx := context.Background()
				confirm, _ := cmd.Flags().GetBool("confirm")
				return purgeDLQ(ctx, confirm)
			},
		},
	)

	dlqCmd.PersistentFlags().Bool("confirm", false, "Confirm destructive operations")

	return dlqCmd
}

func newGenerateCommand() *cobra.Command {
	var (
		count  int
		output string
	)

	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate sample events for load testing",
		RunE: func(cmd *cobra.Command, args []string) error {
			return generateEvents(count, output)
		},
	}

	cmd.Flags().IntVar(&count, "count", 100, "Number of events to generate")
	cmd.Flags().StringVar(&output, "output", "events/generated.json", "Output file")

	return cmd
}
