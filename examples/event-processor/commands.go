package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	messagev1 "github.com/adrien19/chronoqueue/api/message/v1"
	queuev1 "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/client"
)

const (
	EmailQueue   = "email-notifications"
	WebhookQueue = "webhook-events"
	SMSQueue     = "sms-alerts"
)

type Event struct {
	Type              string                 `json:"type"`
	Priority          string                 `json:"priority"`
	ScheduleInMinutes int                    `json:"schedule_in_minutes,omitempty"`
	Data              map[string]interface{} `json:"data"`
}

type EventBatch struct {
	Events []Event `json:"events"`
}

func initializeSystem(ctx context.Context) error {
	c, err := connectToServer()
	if err != nil {
		return err
	}
	defer c.Close()

	// Queue configurations with different retention policies based on use case
	queues := []struct {
		name            string
		description     string
		retentionPolicy *client.RetentionPolicyOption
	}{
		{
			name:        EmailQueue,
			description: "Email notification events",
			// Retain email events for 7 days for delivery tracking and debugging
			retentionPolicy: &client.RetentionPolicyOption{
				Mode:             client.RETENTION_RETAIN_DURATION,
				RetentionSeconds: 7 * 24 * 60 * 60, // 7 days
			},
		},
		{
			name:        WebhookQueue,
			description: "Webhook delivery events",
			// Retain webhook events for 30 days for audit trail and replay capability
			retentionPolicy: &client.RetentionPolicyOption{
				Mode:             client.RETENTION_RETAIN_DURATION,
				RetentionSeconds: 30 * 24 * 60 * 60, // 30 days
			},
		},
		{
			name:        SMSQueue,
			description: "SMS alert events",
			// SMS alerts are transient - delete immediately after processing
			retentionPolicy: nil, // DELETE_IMMEDIATELY (default)
		},
	}

	fmt.Println("Initializing Event Processing System...")
	for _, q := range queues {
		queueOpts := client.QueueOptions{
			Type: int32(queuev1.QueueType_SIMPLE),
			LeasePolicy: client.LeasePolicyOptions{
				BaseLease:        "30s",
				HeartbeatTimeout: "10s",
				MaxExtension:     "60s",
				ExtendStep:       "10s",
			},
			DequeueAttempts: 3,
			RetentionPolicy: q.retentionPolicy,
		}

		_, err := c.CreateQueue(ctx, q.name, queueOpts)
		if err != nil {
			if strings.Contains(err.Error(), "already exists") {
				fmt.Printf("  ✓ Queue %s already exists\n", q.name)
			} else {
				return fmt.Errorf("failed to create queue %s: %w", q.name, err)
			}
		} else {
			fmt.Printf("  ✓ Created queue: %s\n", q.name)
		}
	}

	fmt.Println("\n✨ System initialized successfully!")
	fmt.Printf("\nQueues created:\n")
	fmt.Printf("  • %s - High-priority email notifications (7-day retention)\n", EmailQueue)
	fmt.Printf("  • %s - Webhook delivery with retries (30-day retention)\n", WebhookQueue)
	fmt.Printf("  • %s - SMS alerts for critical events (immediate delete)\n", SMSQueue)

	return nil
}

func publishEvents(ctx context.Context, filename string) error {
	c, err := connectToServer()
	if err != nil {
		return err
	}
	defer c.Close()

	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	var batch EventBatch
	if err := json.Unmarshal(data, &batch); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	fmt.Printf("Publishing %d events from %s...\n\n", len(batch.Events), filename)

	published := make(map[string]int)
	start := time.Now()

	for i, event := range batch.Events {
		queueName := getQueueForType(event.Type)
		priority := parsePriority(event.Priority)

		payloadData, err := structpb.NewStruct(event.Data)
		if err != nil {
			fmt.Printf("  ⚠️  Event %d: Failed to convert payload: %v\n", i+1, err)
			continue
		}

		msgID := fmt.Sprintf("evt-%d-%d", time.Now().Unix(), i)
		msgOpts := client.MessageOptions{
			Payload: client.Payload{
				Data:        payloadData,
				ContentType: "application/json",
			},
			LeasePolicy: client.LeasePolicyOptions{
				BaseLease:        "30s",
				HeartbeatTimeout: "10s",
			},
			MaxAttempts: 3,
			Priority:    int64(priority),
		}

		// Handle scheduled messages
		var scheduleInfo string
		if event.ScheduleInMinutes > 0 {
			scheduledTime := time.Now().Add(time.Duration(event.ScheduleInMinutes) * time.Minute)
			msgOpts.ScheduledTime = &scheduledTime
			scheduleInfo = fmt.Sprintf(", Scheduled: %s (%dm)", scheduledTime.Format("15:04:05"), event.ScheduleInMinutes)
		}

		_, err = c.PostMessage(ctx, queueName, msgID, msgOpts)
		if err != nil {
			fmt.Printf("  ❌ Event %d: Failed to publish: %v\n", i+1, err)
			continue
		}

		published[queueName]++
		fmt.Printf("  ✓ Event %d: Published %s event (ID: %s, Priority: %s%s)\n",
			i+1, event.Type, msgID, event.Priority, scheduleInfo)
	}

	duration := time.Since(start)

	fmt.Printf("\n📊 Summary:\n")
	fmt.Printf("  • Total Events: %d\n", len(batch.Events))
	fmt.Printf("  • Published: %d\n", sumValues(published))
	fmt.Printf("  • Duration: %v\n", duration)
	fmt.Printf("\n  By Queue:\n")
	for q, count := range published {
		fmt.Printf("    • %s: %d events\n", q, count)
	}

	return nil
}

func publishEventsBulk(ctx context.Context, filename string, mode string) error {
	c, err := connectToServer()
	if err != nil {
		return err
	}
	defer c.Close()

	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	var batch EventBatch
	if err := json.Unmarshal(data, &batch); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Parse transaction mode
	var transactionMode queueservice_pb.PostMessagesBulkRequest_TransactionMode
	switch strings.ToLower(mode) {
	case "all-or-nothing":
		transactionMode = queueservice_pb.PostMessagesBulkRequest_ALL_OR_NOTHING
	case "best-effort":
		transactionMode = queueservice_pb.PostMessagesBulkRequest_BEST_EFFORT
	default:
		return fmt.Errorf("invalid mode: %s (must be 'all-or-nothing' or 'best-effort')", mode)
	}

	fmt.Printf("📦 Publishing %d events in BULK mode from %s\n", len(batch.Events), filename)
	fmt.Printf("   Transaction Mode: %s\n\n", strings.ToUpper(mode))

	// Group events by queue
	eventsByQueue := make(map[string][]struct {
		event Event
		index int
	})

	for i, event := range batch.Events {
		queueName := getQueueForType(event.Type)
		eventsByQueue[queueName] = append(eventsByQueue[queueName], struct {
			event Event
			index int
		}{event, i})
	}

	start := time.Now()
	totalPublished := 0
	totalFailed := 0

	// Bulk post to each queue
	for queueName, events := range eventsByQueue {
		fmt.Printf("📤 Posting %d events to %s...\n", len(events), queueName)

		// Build messages for this queue
		type messageEntry struct {
			msg        client.MessageWithID
			eventIndex int
			event      Event
		}
		var messageEntries []messageEntry
		for _, item := range events {
			event := item.event
			priority := parsePriority(event.Priority)

			payloadData, err := structpb.NewStruct(event.Data)
			if err != nil {
				fmt.Printf("  ⚠️  Event %d: Failed to convert payload: %v\n", item.index+1, err)
				continue
			}

			msgID := fmt.Sprintf("evt-%d-%d", time.Now().Unix(), item.index)
			msgOpts := client.MessageOptions{
				Payload: client.Payload{
					Data:        payloadData,
					ContentType: "application/json",
				},
				LeasePolicy: client.LeasePolicyOptions{
					BaseLease:        "30s",
					HeartbeatTimeout: "10s",
				},
				MaxAttempts: 3,
				Priority:    int64(priority),
			}

			// Handle scheduled messages
			if event.ScheduleInMinutes > 0 {
				scheduledTime := time.Now().Add(time.Duration(event.ScheduleInMinutes) * time.Minute)
				msgOpts.ScheduledTime = &scheduledTime
			}

			messageEntries = append(messageEntries, messageEntry{
				msg: client.MessageWithID{
					MessageID: msgID,
					Options:   msgOpts,
				},
				eventIndex: item.index,
				event:      event,
			})
		}

		// Extract messages for API call
		messages := make([]client.MessageWithID, len(messageEntries))
		for i, entry := range messageEntries {
			messages[i] = entry.msg
		}

		// Post messages in bulk
		resp, err := c.PostMessagesBulk(ctx, queueName, messages, transactionMode)
		if err != nil {
			fmt.Printf("  ❌ Bulk post failed: %v\n", err)
			totalFailed += len(messages)
			continue
		}

		// Display results
		fmt.Printf("  ✓ Success: %d/%d messages posted\n", resp.GetSuccessfulCount(), len(messages))

		if resp.GetFailedCount() > 0 {
			fmt.Printf("  ⚠️  Failed: %d messages\n", resp.GetFailedCount())
			for i, result := range resp.GetResults() {
				if !result.GetSuccess() {
					entry := messageEntries[i]
					fmt.Printf("    ❌ Event %d (%s): %s - %s\n",
						entry.eventIndex+1,
						entry.event.Type,
						result.GetErrorCode().String(),
						result.GetError())
				}
			}
		}

		totalPublished += int(resp.GetSuccessfulCount())
		totalFailed += int(resp.GetFailedCount())
		fmt.Println()
	}

	duration := time.Since(start)

	fmt.Printf("📊 Bulk Posting Summary:\n")
	fmt.Printf("  • Total Events: %d\n", len(batch.Events))
	fmt.Printf("  • Published: %d\n", totalPublished)
	fmt.Printf("  • Failed: %d\n", totalFailed)
	fmt.Printf("  • Duration: %v\n", duration)
	fmt.Printf("  • Transaction Mode: %s\n", strings.ToUpper(mode))
	fmt.Printf("\n  By Queue:\n")
	for queueName, events := range eventsByQueue {
		fmt.Printf("    • %s: %d events\n", queueName, len(events))
	}

	if totalPublished > 0 {
		avgTime := duration.Milliseconds() / int64(totalPublished)
		fmt.Printf("\n  ⚡ Average: %dms per message\n", avgTime)
	}

	return nil
}

func startWorker(ctx context.Context, workerType string, numWorkers int, name string) error {
	c, err := connectToServer()
	if err != nil {
		return err
	}
	defer c.Close()

	queueName := getQueueForType(workerType)
	if name == "" {
		name = fmt.Sprintf("%s-worker-%d", workerType, rand.Intn(10000))
	}

	fmt.Printf("🚀 Starting %s worker (queue: %s)\n", workerType, queueName)
	fmt.Printf("   Workers: %d\n", numWorkers)
	fmt.Printf("   Name: %s\n", name)
	fmt.Printf("   Press Ctrl+C to stop\n\n")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\n\n🛑 Shutting down worker...")
		cancel()
	}()

	processedCount := 0
	failedCount := 0

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("\n📊 Worker statistics:\n")
			fmt.Printf("  • Processed: %d events\n", processedCount)
			fmt.Printf("  • Failed: %d events\n", failedCount)
			return nil
		default:
		}

		resp, err := c.GetNextMessage(ctx, queueName, "30s", true)
		if err != nil {
			if ctx.Err() != nil {
				continue
			}
			if strings.Contains(err.Error(), "no messages available") {
				time.Sleep(1 * time.Second)
				continue
			}
			fmt.Printf("  ⚠️  Failed to fetch message: %v\n", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if resp.Message == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		msg := resp.Message
		msgID := msg.MessageId

		var event Event
		if msg.Metadata != nil && msg.Metadata.Payload != nil && msg.Metadata.Payload.Data != nil {
			dataBytes, _ := json.Marshal(msg.Metadata.Payload.Data.AsMap())
			json.Unmarshal(dataBytes, &event.Data)
			event.Type = workerType
		}

		fmt.Printf("  ⚙️  [%s] Processing %s event...\n", msgID, workerType)

		if err := processEvent(ctx, workerType, event); err != nil {
			fmt.Printf("  ❌ [%s] Failed: %v\n", msgID, err)
			c.AcknowledgeMessage(ctx, queueName, msgID, client.MESSAGE_ERRORED)
			failedCount++
		} else {
			fmt.Printf("  ✓ [%s] Successfully processed\n", msgID)
			c.AcknowledgeMessage(ctx, queueName, msgID, client.MESSAGE_COMPLETED)
			processedCount++
		}
	}
}

func processEvent(ctx context.Context, workerType string, event Event) error {
	processingTime := time.Duration(2+rand.Intn(3)) * time.Second

	destination := ""
	if event.Data != nil {
		if dest, ok := event.Data["destination"]; ok {
			destination = fmt.Sprint(dest)
		}
		if dest, ok := event.Data["webhook_url"]; ok {
			destination = fmt.Sprint(dest)
		}
	}

	time.Sleep(processingTime)

	failureRate := 0.1
	if strings.Contains(destination, "fail") {
		failureRate = 0.8
	}

	if rand.Float64() < failureRate {
		return fmt.Errorf("simulated %s failure", workerType)
	}

	return nil
}

func showStats(ctx context.Context) error {
	c, err := connectToServer()
	if err != nil {
		return err
	}
	defer c.Close()

	queues := []string{EmailQueue, WebhookQueue, SMSQueue}

	fmt.Println("📊 Event Processing Statistics")
	fmt.Println("═══════════════════════════════════════════════════════════════")

	totalStats := make(map[string]int32)

	for _, qName := range queues {
		state, err := c.GetQueueState(ctx, qName)
		if err != nil {
			fmt.Printf("⚠️  Failed to get state for %s: %v\n\n", qName, err)
			continue
		}

		counts := state.StateCounts

		if verbose {
			fmt.Printf("Debug - StateCounts for %s: %v\n", qName, counts)
		}

		pending := counts[messagev1.Message_Metadata_PENDING.String()]
		processing := counts[messagev1.Message_Metadata_RUNNING.String()]
		completed := counts[messagev1.Message_Metadata_COMPLETED.String()]
		errored := counts[messagev1.Message_Metadata_ERRORED.String()]

		fmt.Printf("\n%s:\n", qName)
		fmt.Printf("  Pending:    %6d\n", pending)
		fmt.Printf("  Processing: %6d\n", processing)
		fmt.Printf("  Completed:  %6d\n", completed)
		fmt.Printf("  Failed:     %6d\n", errored)

		totalStats["pending"] += pending
		totalStats["processing"] += processing
		totalStats["completed"] += completed
		totalStats["failed"] += errored
	}

	fmt.Println("\n═══════════════════════════════════════════════════════════════")
	fmt.Printf("\nTotals Across All Queues:\n")
	fmt.Printf("  Pending:    %6d\n", totalStats["pending"])
	fmt.Printf("  Processing: %6d\n", totalStats["processing"])
	fmt.Printf("  Completed:  %6d\n", totalStats["completed"])
	fmt.Printf("  Failed:     %6d\n", totalStats["failed"])
	fmt.Printf("  Total:      %6d\n",
		totalStats["pending"]+totalStats["processing"]+totalStats["completed"]+totalStats["failed"])

	return nil
}

func monitorSystem(ctx context.Context, interval string) error {
	c, err := connectToServer()
	if err != nil {
		return err
	}
	defer c.Close()

	duration, err := time.ParseDuration(interval)
	if err != nil {
		return fmt.Errorf("invalid interval: %w", err)
	}

	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("🔍 Real-time Event Processing Monitor")
	fmt.Printf("    Update interval: %s\n", interval)
	fmt.Println("    Press Ctrl+C to stop")

	queues := []string{EmailQueue, WebhookQueue, SMSQueue}

	for {
		select {
		case <-ticker.C:
			fmt.Print("\033[H\033[2J")
			fmt.Printf("🔍 Event Processing Monitor - %s\n\n", time.Now().Format("15:04:05"))

			for _, qName := range queues {
				state, err := c.GetQueueState(ctx, qName)
				if err != nil {
					fmt.Printf("⚠️  %s: Error - %v\n\n", qName, err)
					continue
				}

				counts := state.StateCounts
				pending := counts[messagev1.Message_Metadata_PENDING.String()]
				processing := counts[messagev1.Message_Metadata_RUNNING.String()]
				completed := counts[messagev1.Message_Metadata_COMPLETED.String()]
				errored := counts[messagev1.Message_Metadata_ERRORED.String()]

				fmt.Printf("%s:\n", qName)
				fmt.Printf("  Pending: %d | Processing: %d | Completed: %d | Failed: %d\n",
					pending, processing, completed, errored)

				if processing > 0 {
					fmt.Printf("  🔄 Active processing\n")
				} else if pending > 0 {
					fmt.Printf("  ⏸  Waiting for workers\n")
				} else {
					fmt.Printf("  ✓ Idle\n")
				}
				fmt.Println()
			}

		case <-sigChan:
			fmt.Println("\n\n🛑 Stopping monitor...")
			return nil
		}
	}
}

func peekQueue(ctx context.Context, queueName string, limit int32) error {
	c, err := connectToServer()
	if err != nil {
		return err
	}
	defer c.Close()

	resp, err := c.PeekQueueMessages(ctx, queueName, limit, client.TimeRangeOption{})
	if err != nil {
		return fmt.Errorf("failed to peek queue: %w", err)
	}

	messages := resp.Messages

	fmt.Printf("👀 Peeking at queue: %s (limit: %d)\n\n", queueName, limit)

	if len(messages) == 0 {
		fmt.Println("No pending messages in queue.")
		return nil
	}

	fmt.Printf("Found %d pending messages:\n\n", len(messages))

	for i, msg := range messages {
		fmt.Printf("%d. Message ID: %s\n", i+1, msg.MessageId)

		if msg.Metadata != nil {
			fmt.Printf("   Priority: %d\n", msg.Metadata.Priority)
			if msg.Metadata.Payload != nil && msg.Metadata.Payload.Data != nil {
				fmt.Printf("   Data:\n")
				for k, v := range msg.Metadata.Payload.Data.AsMap() {
					fmt.Printf("     • %s: %v\n", k, v)
				}
			}
		}
		fmt.Println()
	}

	return nil
}

func listDLQ(ctx context.Context) error {
	c, err := connectToServer()
	if err != nil {
		return err
	}
	defer c.Close()

	queues := []string{EmailQueue, WebhookQueue, SMSQueue}

	fmt.Println("☠️  Dead Letter Queue Contents")

	totalFailed := 0

	for _, qName := range queues {
		dlqName := qName + "-dlq"
		resp, err := c.GetDLQMessages(ctx, dlqName, 100)
		if err != nil {
			continue
		}

		messages := resp.Messages
		if len(messages) == 0 {
			continue
		}

		fmt.Printf("\n%s (%d failed):\n", qName, len(messages))
		totalFailed += len(messages)

		for i, msg := range messages {
			if i >= 5 {
				fmt.Printf("  ... and %d more\n", len(messages)-5)
				break
			}

			fmt.Printf("  • %s", msg.MessageId)
			if msg.Metadata != nil {
				fmt.Printf(" (%d retries)", msg.Metadata.MaxAttempts-msg.Metadata.AttemptsLeft)
			}
			fmt.Println()
		}
	}

	if totalFailed == 0 {
		fmt.Println("✨ No failed messages in DLQ")
	} else {
		fmt.Printf("\nTotal failed messages: %d\n", totalFailed)
		fmt.Println("\nUse 'event-processor dlq inspect <event-id>' to see details")
	}

	return nil
}

func inspectDLQEvent(ctx context.Context, eventID string) error {
	c, err := connectToServer()
	if err != nil {
		return err
	}
	defer c.Close()

	queues := []string{EmailQueue, WebhookQueue, SMSQueue}

	for _, qName := range queues {
		dlqName := qName + "-dlq"
		resp, err := c.GetDLQMessages(ctx, dlqName, 1000)
		if err != nil {
			continue
		}

		for _, msg := range resp.Messages {
			if msg.MessageId == eventID {
				fmt.Printf("🔍 DLQ Event Details\n\n")
				fmt.Printf("Message ID:     %s\n", msg.MessageId)
				fmt.Printf("Queue:          %s\n", qName)

				if msg.Metadata != nil {
					fmt.Printf("Priority:       %d\n", msg.Metadata.Priority)
					fmt.Printf("Retry Count:    %d\n", msg.Metadata.MaxAttempts-msg.Metadata.AttemptsLeft)
					fmt.Printf("Max Attempts:   %d\n", msg.Metadata.MaxAttempts)

					if msg.Metadata.Payload != nil && msg.Metadata.Payload.Data != nil {
						fmt.Printf("\nEvent Data:\n")
						data, _ := json.MarshalIndent(msg.Metadata.Payload.Data.AsMap(), "  ", "  ")
						fmt.Printf("  %s\n", string(data))
					}
				}

				return nil
			}
		}
	}

	return fmt.Errorf("event %s not found in DLQ", eventID)
}

func requeueFromDLQ(ctx context.Context, eventID string) error {
	c, err := connectToServer()
	if err != nil {
		return err
	}
	defer c.Close()

	queues := []string{EmailQueue, WebhookQueue, SMSQueue}

	for _, qName := range queues {
		dlqName := qName + "-dlq"
		_, err := c.RequeueFromDLQ(ctx, dlqName, eventID, qName)
		if err == nil {
			fmt.Printf("✓ Requeued event %s from DLQ to %s\n", eventID, qName)
			return nil
		}
	}

	return fmt.Errorf("failed to requeue event %s", eventID)
}

func deleteFromDLQ(ctx context.Context, eventID string) error {
	c, err := connectToServer()
	if err != nil {
		return err
	}
	defer c.Close()

	queues := []string{EmailQueue, WebhookQueue, SMSQueue}

	for _, qName := range queues {
		dlqName := qName + "-dlq"
		_, err := c.DeleteFromDLQ(ctx, dlqName, eventID)
		if err == nil {
			fmt.Printf("✓ Deleted event %s from DLQ\n", eventID)
			return nil
		}
	}

	return fmt.Errorf("failed to delete event %s", eventID)
}

func purgeDLQ(ctx context.Context, confirm bool) error {
	if !confirm {
		fmt.Println("⚠️  This will delete ALL failed messages from the DLQ")
		fmt.Println("   Use --confirm flag to proceed")
		return nil
	}

	c, err := connectToServer()
	if err != nil {
		return err
	}
	defer c.Close()

	queues := []string{EmailQueue, WebhookQueue, SMSQueue}

	totalPurged := 0

	for _, qName := range queues {
		dlqName := qName + "-dlq"
		_, err := c.PurgeDLQ(ctx, dlqName)
		if err != nil {
			fmt.Printf("⚠️  Failed to purge DLQ for %s: %v\n", qName, err)
			continue
		}

		fmt.Printf("✓ Purged DLQ for %s\n", qName)
		totalPurged++
	}

	fmt.Printf("\n✨ Purged %d queues\n", totalPurged)

	return nil
}

func generateEvents(count int, output string) error {
	events := EventBatch{
		Events: make([]Event, 0, count),
	}

	types := []string{"email", "webhook", "sms"}
	priorities := []string{"low", "medium", "high", "critical"}

	for i := 0; i < count; i++ {
		eventType := types[rand.Intn(len(types))]
		priority := priorities[rand.Intn(len(priorities))]

		event := Event{
			Type:     eventType,
			Priority: priority,
			Data: map[string]interface{}{
				"id":          fmt.Sprintf("evt-%d", i+1),
				"timestamp":   time.Now().Unix(),
				"destination": fmt.Sprintf("user-%d@example.com", rand.Intn(1000)),
				"content":     fmt.Sprintf("Test %s event #%d", eventType, i+1),
			},
		}

		events.Events = append(events.Events, event)
	}

	data, err := json.MarshalIndent(events, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal events: %w", err)
	}

	if err := os.MkdirAll("events", 0o755); err != nil {
		return fmt.Errorf("failed to create events directory: %w", err)
	}

	if err := os.WriteFile(output, data, 0o644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	fmt.Printf("✓ Generated %d events → %s\n", count, output)

	return nil
}

func connectToServer() (*client.ChronoQueueClient, error) {
	opts := client.ClientOptions{
		MaxRetries:          client.DefaultMaxRetries,
		InitialBackoff:      client.DefaultInitialBackoff,
		MaxBackoff:          client.DefaultMaxBackoff,
		MaxHeartBeatWorkers: client.DefaultMaxHeartBeatWorkers,
	}

	if verbose {
		fmt.Printf("Connecting to %s (insecure: %v)\n", serverAddr, insecure)
	}

	return client.NewChronoQueueClient(serverAddr, opts)
}

func getQueueForType(eventType string) string {
	switch strings.ToLower(eventType) {
	case "email":
		return EmailQueue
	case "webhook":
		return WebhookQueue
	case "sms":
		return SMSQueue
	default:
		return WebhookQueue
	}
}

func parsePriority(priority string) int64 {
	switch strings.ToLower(priority) {
	case "critical":
		return 100
	case "high":
		return 75
	case "medium":
		return 50
	case "low":
		return 25
	default:
		return 50
	}
}

func sumValues(m map[string]int) int {
	sum := 0
	for _, v := range m {
		sum += v
	}
	return sum
}
