package commands

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/structpb"

	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/cmd/chronoq/outputs"
)

// NewMessageCommand creates the message command group
func NewMessageCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "message",
		Short: "Message operations",
		Long:  `Manage ChronoQueue messages - post, get, acknowledge, and peek messages.`,
	}

	cmd.AddCommand(newMessagePostCommand())
	cmd.AddCommand(newMessagePostBulkCommand())
	cmd.AddCommand(newMessageGetCommand())
	cmd.AddCommand(newMessageAckCommand())
	cmd.AddCommand(newMessagePeekCommand())
	cmd.AddCommand(newMessageRenewCommand())
	cmd.AddCommand(newMessageHeartbeatCommand())
	cmd.AddCommand(newMessageCancelCommand())

	return cmd
}

// newMessagePostCommand creates the message post subcommand
func newMessagePostCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "post <queue-name> [message-data]",
		Short: "Post a message to a queue",
		Long: `Post a new message to the specified queue.

Message data can be provided in three ways:
  1. Inline JSON: chronoq message post orders '{"key":"value"}'
  2. From file:   chronoq message post orders --file /path/to/data.json
  3. From stdin:  cat data.json | chronoq message post orders -
  
The --file flag takes precedence if both inline and file are provided.`,
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			queueName := args[0]

			// Get message data from --file flag, inline argument, or stdin
			filePath, _ := cmd.Flags().GetString("file")
			var messageData string
			var err error

			if filePath != "" {
				// Read from file
				data, err := os.ReadFile(filePath)
				if err != nil {
					return fmt.Errorf("failed to read file %s: %w", filePath, err)
				}
				messageData = string(data)
			} else if len(args) == 2 {
				// Use inline argument
				messageData = args[1]

				// Support stdin with "-" convention
				if messageData == "-" {
					data, err := io.ReadAll(os.Stdin)
					if err != nil {
						return fmt.Errorf("failed to read from stdin: %w", err)
					}
					messageData = string(data)
				}
			} else {
				return fmt.Errorf("message data required: provide inline JSON, use --file flag, or pipe to stdin with '-'")
			}

			messageId, err := cmd.Flags().GetString("id")
			if err != nil {
				return err
			}
			leaseDuration, err := cmd.Flags().GetString("lease-duration")
			if err != nil {
				return err
			}
			maxAttempts, err := cmd.Flags().GetInt32("max-attempts")
			if err != nil {
				return err
			}
			priority, err := cmd.Flags().GetInt64("priority")
			if err != nil {
				return err
			}
			metadataStr, err := cmd.Flags().GetString("metadata")
			if err != nil {
				return err
			}
			contentType, err := cmd.Flags().GetString("content-type")
			if err != nil {
				return err
			}
			schemaID, err := cmd.Flags().GetString("schema-id")
			if err != nil {
				return err
			}
			schemaVersion, err := cmd.Flags().GetInt32("schema-version")
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

			messageOpts := client.MessageOptions{
				Payload: client.Payload{
					Data:          dataStruct,
					Metadata:      metadataMap,
					ContentType:   contentType,
					SchemaID:      schemaID,
					SchemaVersion: schemaVersion,
				},
				LeaseDuration: leaseDuration,
				Priority:      priority,
				MaxAttempts:   maxAttempts, // Set max attempts for the message
			}

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				resp, err := client.PostMessage(cmd.Context(), queueName, messageId, messageOpts)
				if err != nil {
					return err
				}
				outputs.PrintInfo(fmt.Sprintf("Success: %t", resp.GetSuccess()))
				return nil
			})
		},
	}

	cmd.Flags().StringP("id", "i", "", "Message ID (auto-generated if not provided)")
	cmd.Flags().StringP("file", "f", "", "Path to file containing message data (JSON)")
	cmd.Flags().StringP("lease-duration", "l", "", "Message lease duration")
	cmd.Flags().StringP("invisibility-duration", "v", "", "Message invisibility duration")
	cmd.Flags().Int32P("max-attempts", "a", -1, "Maximum message processing attempts")
	cmd.Flags().Int64P("priority", "p", 0, "Message priority")
	cmd.Flags().StringP("metadata", "m", "", "Message metadata as JSON")
	cmd.Flags().StringP("content-type", "t", "application/json", "Content type (MIME type)")
	cmd.Flags().StringP("schema-id", "s", "", "Schema ID for validation")
	cmd.Flags().Int32("schema-version", 0, "Schema version")

	return cmd
}

// newMessagePostBulkCommand creates the message post-bulk subcommand
func newMessagePostBulkCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "post-bulk <queue-name> <messages-file>",
		Short: "Post multiple messages to a queue in bulk",
		Long: `Post multiple messages to a queue in a single operation.

The messages file should contain a JSON array of message objects. Each message object should have:
  - id: Message ID (required)
  - data: Message payload (required)
  - priority: Message priority (optional, default: 5)
  - maxAttempts: Maximum processing attempts (optional)

Example messages file (messages.json):
[
  {
    "id": "msg-001",
    "data": {"order": "item-1", "quantity": 2},
    "priority": 10
  },
  {
    "id": "msg-002",
    "data": {"order": "item-2", "quantity": 5},
    "priority": 5
  }
]

Transaction Modes:
  - all-or-nothing: All messages succeed or all fail (default)
  - best-effort: Process as many as possible, continue on failures

Limits:
  - Maximum 1000 messages per request
  - Maximum 1MB total payload size`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			queueName := args[0]
			messagesFile := args[1]

			// Read messages from file
			data, err := os.ReadFile(messagesFile)
			if err != nil {
				return fmt.Errorf("failed to read messages file %s: %w", messagesFile, err)
			}

			// Parse messages JSON
			var messagesData []map[string]interface{}
			if err := json.Unmarshal(data, &messagesData); err != nil {
				return fmt.Errorf("failed to parse messages JSON: %w", err)
			}

			if len(messagesData) == 0 {
				return fmt.Errorf("no messages found in file")
			}

			if len(messagesData) > 1000 {
				return fmt.Errorf("too many messages: %d (max 1000)", len(messagesData))
			}

			// Get flags
			transactionModeStr, err := cmd.Flags().GetString("mode")
			if err != nil {
				return err
			}
			leaseDuration, err := cmd.Flags().GetString("lease-duration")
			if err != nil {
				return err
			}
			contentType, err := cmd.Flags().GetString("content-type")
			if err != nil {
				return err
			}

			// Parse transaction mode
			var transactionMode queueservice_pb.PostMessagesBulkRequest_TransactionMode
			switch transactionModeStr {
			case "all-or-nothing":
				transactionMode = queueservice_pb.PostMessagesBulkRequest_ALL_OR_NOTHING
			case "best-effort":
				transactionMode = queueservice_pb.PostMessagesBulkRequest_BEST_EFFORT
			default:
				return fmt.Errorf("invalid transaction mode: %s (must be 'all-or-nothing' or 'best-effort')", transactionModeStr)
			}

			// Convert to client messages
			messages := make([]client.MessageWithID, len(messagesData))
			for i, msgData := range messagesData {
				// Extract message ID
				msgID, ok := msgData["id"].(string)
				if !ok || msgID == "" {
					return fmt.Errorf("message[%d] missing 'id' field", i)
				}

				// Extract message data
				data, ok := msgData["data"]
				if !ok {
					return fmt.Errorf("message[%d] missing 'data' field", i)
				}

				// Convert data to structpb.Struct
				dataStruct, err := structpb.NewStruct(map[string]interface{}{
					"value": data,
				})
				if err != nil {
					return fmt.Errorf("failed to create data struct for message[%d]: %w", i, err)
				}

				// Extract optional fields
				priority := int64(5) // default
				if p, ok := msgData["priority"].(float64); ok {
					priority = int64(p)
				}

				maxAttempts := int32(-1) // use queue default
				if ma, ok := msgData["maxAttempts"].(float64); ok {
					maxAttempts = int32(ma)
				}

				messages[i] = client.MessageWithID{
					MessageID: msgID,
					Options: client.MessageOptions{
						Payload: client.Payload{
							Data:        dataStruct,
							ContentType: contentType,
						},
						LeaseDuration: leaseDuration,
						Priority:      priority,
						MaxAttempts:   maxAttempts,
					},
				}
			}

			return WithClient(cmd, func(c *client.ChronoQueueClient) error {
				resp, err := c.PostMessagesBulk(cmd.Context(), queueName, messages, transactionMode)
				if err != nil {
					return err
				}

				// Print results
				formatter := outputs.NewOutputFormatter(GetOutputFormat(cmd))

				outputs.PrintInfo(fmt.Sprintf("Transaction Mode: %s", transactionModeStr))
				outputs.PrintInfo(fmt.Sprintf("Total Messages: %d", len(messages)))
				outputs.PrintInfo(fmt.Sprintf("Successful: %d", resp.GetSuccessfulCount()))
				outputs.PrintInfo(fmt.Sprintf("Failed: %d", resp.GetFailedCount()))

				// Show detailed results if any failures
				if resp.GetFailedCount() > 0 {
					outputs.PrintWarning("\nFailed Messages:")
					failedResults := make([]map[string]interface{}, 0)
					for _, result := range resp.GetResults() {
						if !result.GetSuccess() {
							failedResults = append(failedResults, map[string]interface{}{
								"message_id": result.GetMessageId(),
								"error_code": result.GetErrorCode().String(),
								"error":      result.GetError(),
							})
						}
					}
					return formatter.Print(failedResults)
				}

				outputs.PrintSuccess("All messages posted successfully")
				return nil
			})
		},
	}

	cmd.Flags().StringP("mode", "m", "all-or-nothing", "Transaction mode: 'all-or-nothing' or 'best-effort'")
	cmd.Flags().StringP("lease-duration", "l", "", "Default lease duration for all messages")
	cmd.Flags().StringP("content-type", "t", "application/json", "Default content type for all messages")

	return cmd
}

// newMessageGetCommand creates the message get subcommand
func newMessageGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <queue-name>",
		Short: "Get the next message from a queue",
		Long:  `Get the next available message from the specified queue.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			queueName := args[0]
			leaseDuration, err := cmd.Flags().GetString("lease-duration")
			if err != nil {
				return err
			}
			enableHeartbeat, err := cmd.Flags().GetBool("enable-heartbeat")
			if err != nil {
				return err
			}

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				resp, err := client.GetNextMessage(cmd.Context(), queueName, leaseDuration, enableHeartbeat)
				if err != nil {
					return err
				}
				if resp.GetMessage() == nil {
					outputs.PrintInfo("No messages available")
					return nil
				}
				outputs.PrintInfo(fmt.Sprintf("Message ID: %s", resp.GetMessage().GetMessageId()))
				if workerID := resp.GetWorkerId(); workerID != "" {
					outputs.PrintInfo(fmt.Sprintf("Worker ID: %s", workerID))
				}
				if attemptID := resp.GetAttemptId(); attemptID != "" {
					outputs.PrintInfo(fmt.Sprintf("Attempt ID: %s", attemptID))
				}
				outputs.PrintInfo(fmt.Sprintf("Metadata: %v", resp.GetMessage().GetMetadata()))
				outputs.PrintInfo(fmt.Sprintf("Payload: %v", resp.GetMessage().GetMetadata().GetPayload()))
				outputs.PrintInfo(fmt.Sprintf("Data: %v", resp.GetMessage().GetMetadata().GetPayload().GetData()))
				return nil
			})
		},
	}

	cmd.Flags().StringP("lease-duration", "l", "30s", "Message lease duration")
	cmd.Flags().StringP("exclusivity-key", "k", "", "Exclusivity key for exclusive queues")
	cmd.Flags().BoolP("enable-heartbeat", "b", false, "Enable automatic heartbeat to renew lease while processing")

	return cmd
}

// newMessageAckCommand creates the message acknowledge subcommand
func newMessageAckCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ack <queue-name> <message-id> <message-state>",
		Short: "Acknowledge a message",
		Long:  `Acknowledge that a message has been processed successfully.`,
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			queueName := args[0]
			messageID := args[1]
			stateStr := args[2]

			attemptID, err := cmd.Flags().GetString("attempt-id")
			if err != nil {
				return err
			}
			workerID, err := cmd.Flags().GetString("worker-id")
			if err != nil {
				return err
			}

			state, err := client.ParseMessageState(stateStr)
			if err != nil {
				return err
			}

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				if attemptID != "" || workerID != "" {
					client.SetAttemptInfo(messageID, attemptID, workerID)
				}
				resp, err := client.AcknowledgeMessage(cmd.Context(), queueName, messageID, state)
				if err != nil {
					return err
				}
				outputs.PrintInfo(fmt.Sprintf("Success: %t", resp.GetSuccess()))
				return nil
			})
		},
	}

	cmd.Flags().String("attempt-id", "", "Attempt ID returned by message get (required for Postgres)")
	cmd.Flags().String("worker-id", "", "Worker ID returned by message get (required for Postgres)")

	return cmd
}

// newMessagePeekCommand creates the message peek subcommand
func newMessagePeekCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "peek <queue-name>",
		Short: "Peek at messages in a queue",
		Long:  `View messages in a queue without removing them.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			queueName := args[0]
			limit, err := cmd.Flags().GetInt32("limit")
			if err != nil {
				return err
			}
			timeRangeInt, err := cmd.Flags().GetStringToInt("time-range")
			if err != nil {
				return err
			}
			timeRange := client.TimeRangeOption{
				Min: int64(timeRangeInt["min"]),
				Max: int64(timeRangeInt["max"]),
			}

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				resp, err := client.PeekQueueMessages(cmd.Context(), queueName, limit, timeRange)
				if err != nil {
					return err
				}
				if len(resp.GetMessages()) == 0 {
					outputs.PrintInfo("No messages available")
					return nil
				}
				for _, msg := range resp.GetMessages() {
					outputs.PrintInfo(fmt.Sprintf("Message ID: %s", msg.GetMessageId()))
					outputs.PrintInfo(fmt.Sprintf("Metadata: %v", msg.GetMetadata()))
					outputs.PrintInfo(fmt.Sprintf("Payload: %v", msg.GetMetadata().GetPayload()))
					outputs.PrintInfo(fmt.Sprintf("Data: %v", msg.GetMetadata().GetPayload().GetData()))
				}
				return nil
			})
		},
	}

	cmd.Flags().Int32P("limit", "l", 10, "Number of messages to peek")
	cmd.Flags().StringToInt("time-range", map[string]int{"min": 0, "max": 0}, "Time range for messages to peek")

	return cmd
}

// newMessageRenewCommand creates the message lease renewal subcommand
func newMessageRenewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "renew <queue-name> <message-id> <lease-duration>",
		Short: "Renew a message lease",
		Long:  `Renew the lease on a message to extend processing time (e.g., 30s).`,
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			queueName := args[0]
			messageID := args[1]
			leaseDuration := args[2]

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				resp, err := client.RenewMessageLease(cmd.Context(), queueName, messageID, leaseDuration)
				if err != nil {
					return err
				}
				outputs.PrintInfo(fmt.Sprintf("Remaining Time: %s", resp.GetRemainingTime().AsDuration()))
				outputs.PrintInfo(fmt.Sprintf("State: %s", resp.GetState()))
				return nil
			})
		},
	}

	return cmd
}

// newMessageHeartbeatCommand creates the message heartbeat subcommand
func newMessageHeartbeatCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "heartbeat <queue-name> <message-id>",
		Short: "Send a heartbeat for a message",
		Long:  `Send a heartbeat to indicate that a message is still being processed.`,
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			queueName := args[0]
			messageID := args[1]

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				resp, err := client.SendMessageHeartbeat(cmd.Context(), queueName, messageID)
				if err != nil {
					return err
				}
				outputs.PrintInfo(fmt.Sprintf("Remaining Time: %s", resp.RemainingTime.AsDuration()))
				outputs.PrintInfo(fmt.Sprintf("State: %s", resp.GetState()))
				return nil
			})
		},
	}

	return cmd
}

// newMessageCancelCommand creates the message cancel subcommand
func newMessageCancelCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cancel <queue-name> <message-id>",
		Short: "Cancel a message",
		Long: `Cancel a message that is in INVISIBLE or PENDING state.

Only messages that have not started processing can be cancelled.
Messages in RUNNING, COMPLETED, ERRORED, or CANCELED states cannot be cancelled.

Example:
  chronoq message cancel orders msg-123
  chronoq message cancel orders msg-123 --reason "Order expired"`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			queueName := args[0]
			messageID := args[1]

			reason, err := cmd.Flags().GetString("reason")
			if err != nil {
				return err
			}

			return WithClient(cmd, func(client *client.ChronoQueueClient) error {
				resp, err := client.CancelMessage(cmd.Context(), queueName, messageID, reason)
				if err != nil {
					return err
				}
				outputs.PrintInfo(fmt.Sprintf("Success: %t", resp.GetSuccess()))
				if reason != "" {
					outputs.PrintInfo(fmt.Sprintf("Cancellation reason: %s", reason))
				}
				return nil
			})
		},
	}

	cmd.Flags().StringP("reason", "r", "", "Reason for cancelling the message (optional, for audit trail)")

	return cmd
}
