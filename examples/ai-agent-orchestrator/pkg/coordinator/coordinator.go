package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	messagev1 "github.com/adrien19/chronoqueue/api/message/v1"
	queueservicev1 "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/examples/ai-agent-orchestrator/pkg/llm"
	"github.com/adrien19/chronoqueue/examples/ai-agent-orchestrator/pkg/models"
)

// Coordinator handles task decomposition and routing
type Coordinator struct {
	client    *client.ChronoQueueClient
	llm       llm.LLMClient
	queueName string
	workers   int
	verbose   bool
	stopChan  chan struct{}
	wg        sync.WaitGroup
}

// NewCoordinator creates a new coordinator agent
func NewCoordinator(c *client.ChronoQueueClient, llmClient llm.LLMClient, workers int, verbose bool) *Coordinator {
	return &Coordinator{
		client:    c,
		llm:       llmClient,
		queueName: "agent-coordinator",
		workers:   workers,
		verbose:   verbose,
		stopChan:  make(chan struct{}),
	}
}

// Start begins processing tasks from the coordinator queue
func (coord *Coordinator) Start(ctx context.Context) error {
	if coord.verbose {
		fmt.Printf("🎯 Starting coordinator with %d workers\n", coord.workers)
		fmt.Printf("   Queue: %s\n", coord.queueName)
		fmt.Println()
	}

	// Start worker goroutines
	for i := 0; i < coord.workers; i++ {
		coord.wg.Add(1)
		go coord.worker(ctx, i+1)
	}

	// Wait for all workers to finish
	coord.wg.Wait()
	return nil
}

// Stop stops all coordinator workers
func (coord *Coordinator) Stop() {
	close(coord.stopChan)
}

// worker processes tasks from the queue
func (coord *Coordinator) worker(ctx context.Context, workerID int) {
	defer coord.wg.Done()

	if coord.verbose {
		fmt.Printf("[Worker %d] Started\n", workerID)
	}

	for {
		select {
		case <-coord.stopChan:
			if coord.verbose {
				fmt.Printf("[Worker %d] Stopping\n", workerID)
			}
			return
		case <-ctx.Done():
			if coord.verbose {
				fmt.Printf("[Worker %d] Context cancelled\n", workerID)
			}
			return
		default:
			// Get next message
			if coord.verbose {
				fmt.Printf("[Worker %d] Polling for messages...\n", workerID)
			}
			resp, err := coord.client.GetNextMessage(ctx, coord.queueName, "", true)
			if err != nil {
				fmt.Printf("[Worker %d] Error getting message: %v\n", workerID, err)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			if resp.Message == nil {
				// No messages available
				if coord.verbose {
					fmt.Printf("[Worker %d] No messages available\n", workerID)
				}
				time.Sleep(500 * time.Millisecond)
				continue
			}

			if coord.verbose {
				fmt.Printf("[Worker %d] Got message: %s\n", workerID, resp.Message.MessageId)
			}

			// Process the message
			if err := coord.processMessage(ctx, workerID, resp); err != nil {
				fmt.Printf("[Worker %d] Error processing message %s: %v\n", workerID, resp.Message.MessageId, err)
			}
		}
	}
}

// processMessage handles a single task message
func (coord *Coordinator) processMessage(ctx context.Context, workerID int, resp *queueservicev1.GetNextMessageResponse) error {
	msg := resp.Message
	streamEntryID := resp.StreamEntryId
	msgID := msg.MessageId

	if coord.verbose {
		fmt.Printf("[Worker %d] Processing task: %s\n", workerID, msgID)
	}

	// Parse the task from the message payload
	task, err := coord.parseTask(msg)
	if err != nil {
		// Acknowledge to remove from queue (invalid format)
		if _, ackErr := coord.client.AcknowledgeMessage(ctx, coord.queueName, msgID, client.MESSAGE_ERRORED, streamEntryID); ackErr != nil {
			fmt.Printf("[Worker %d] Warning: failed to acknowledge message: %v\n", workerID, ackErr)
		}
		return fmt.Errorf("failed to parse task: %w", err)
	}

	// Decompose the task using LLM
	decomposition, err := coord.llm.DecomposeTask(task)
	if err != nil {
		// Acknowledge to remove from queue (decomposition failed)
		if _, ackErr := coord.client.AcknowledgeMessage(ctx, coord.queueName, msgID, client.MESSAGE_ERRORED, streamEntryID); ackErr != nil {
			fmt.Printf("[Worker %d] Warning: failed to acknowledge message: %v\n", workerID, ackErr)
		}
		return fmt.Errorf("failed to decompose task: %w", err)
	}

	if coord.verbose {
		fmt.Printf("[Worker %d] Task %s decomposed into %d subtasks\n", workerID, task.TaskID, len(decomposition.Subtasks))
	}

	// Route subtasks to appropriate agent queues
	if err := coord.routeSubtasks(ctx, decomposition); err != nil {
		if _, ackErr := coord.client.AcknowledgeMessage(ctx, coord.queueName, msgID, client.MESSAGE_ERRORED, streamEntryID); ackErr != nil {
			fmt.Printf("[Worker %d] Warning: failed to acknowledge message: %v\n", workerID, ackErr)
		}
		return fmt.Errorf("failed to route subtasks: %w", err)
	}

	// Acknowledge the original task message
	_, err = coord.client.AcknowledgeMessage(ctx, coord.queueName, msgID, client.MESSAGE_COMPLETED, streamEntryID)
	if err != nil {
		return fmt.Errorf("failed to acknowledge message: %w", err)
	}

	if coord.verbose {
		fmt.Printf("[Worker %d] ✓ Task %s completed successfully\n", workerID, task.TaskID)
		fmt.Println()
	}

	return nil
}

// parseTask converts a ChronoQueue message to a Task
func (coord *Coordinator) parseTask(msg *messagev1.Message) (*models.Task, error) {
	if msg.Metadata == nil || msg.Metadata.Payload == nil || msg.Metadata.Payload.Data == nil {
		return nil, fmt.Errorf("message payload is empty")
	}

	// Convert structpb.Struct to map
	dataMap := msg.Metadata.Payload.Data.AsMap()

	// Create task with manual field mapping to handle timestamp conversion
	task := &models.Task{
		TaskID:      getString(dataMap, "task_id"),
		TaskType:    getString(dataMap, "task_type"),
		Description: getString(dataMap, "description"),
		Priority:    getInt32(dataMap, "priority"),
		TenantID:    getString(dataMap, "tenant_id"),
		Status:      models.TaskStatusPending,
	}

	// Handle input map
	if inputVal, ok := dataMap["input"]; ok {
		if inputMap, ok := inputVal.(map[string]interface{}); ok {
			task.Input = inputMap
		}
	}

	// Handle created_at timestamp
	if createdAt, ok := dataMap["created_at"]; ok {
		if timestamp, ok := createdAt.(float64); ok {
			task.CreatedAt = time.Unix(int64(timestamp), 0)
		} else {
			task.CreatedAt = time.Now()
		}
	} else {
		task.CreatedAt = time.Now()
	}

	return task, nil
}

func getString(m map[string]interface{}, key string) string {
	if val, ok := m[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func getInt32(m map[string]interface{}, key string) int32 {
	if val, ok := m[key]; ok {
		if num, ok := val.(float64); ok {
			return int32(num)
		}
		if num, ok := val.(int64); ok {
			return int32(num)
		}
		if num, ok := val.(int32); ok {
			return num
		}
	}
	return 0
}

// routeSubtasks posts subtasks to their respective agent queues
func (coord *Coordinator) routeSubtasks(ctx context.Context, decomposition *models.TaskDecomposition) error {
	for _, subtask := range decomposition.Subtasks {
		// Determine the target queue based on agent type
		queueName := fmt.Sprintf("agent-%s", subtask.AgentType)

		// Convert subtask to structpb-compatible format
		subtaskData := map[string]interface{}{
			"subtask_id":  subtask.SubtaskID,
			"parent_id":   subtask.ParentID,
			"agent_type":  subtask.AgentType,
			"description": subtask.Description,
			"priority":    subtask.Priority,
			"status":      string(subtask.Status),
			"created_at":  subtask.CreatedAt.Unix(),
		}

		// Handle input (convert nested structures)
		if subtask.Input != nil {
			subtaskData["input"] = convertToStructPBCompatible(subtask.Input)
		}

		// Handle depends_on slice
		if len(subtask.DependsOn) > 0 {
			dependsList := make([]interface{}, len(subtask.DependsOn))
			for i, dep := range subtask.DependsOn {
				dependsList[i] = dep
			}
			subtaskData["depends_on"] = dependsList
		}

		payloadStruct, err := structpb.NewStruct(subtaskData)
		if err != nil {
			return fmt.Errorf("failed to create subtask payload: %w", err)
		}

		// Post to the agent's queue
		_, err = coord.client.PostMessage(ctx, queueName, subtask.SubtaskID, client.MessageOptions{
			Priority: int64(subtask.Priority),
			Payload: client.Payload{
				Data:        payloadStruct,
				ContentType: "application/json",
			},
		})

		if err != nil {
			return fmt.Errorf("failed to post subtask %s to %s: %w", subtask.SubtaskID, queueName, err)
		}

		if coord.verbose {
			fmt.Printf("   → Routed subtask %s to %s\n", subtask.SubtaskID, queueName)
		}
	}

	return nil
}

// convertToStructPBCompatible converts Go types to structpb-compatible types
func convertToStructPBCompatible(input interface{}) interface{} {
	switch v := input.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for key, val := range v {
			result[key] = convertToStructPBCompatible(val)
		}
		return result
	case []string:
		result := make([]interface{}, len(v))
		for i, str := range v {
			result[i] = str
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, val := range v {
			result[i] = convertToStructPBCompatible(val)
		}
		return result
	default:
		return v
	}
}
