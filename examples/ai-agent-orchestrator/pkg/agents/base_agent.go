package agents

import (
	"context"
	"fmt"
	"sync"
	"time"

	messagev1 "github.com/adrien19/chronoqueue/api/message/v1"
	queueservicev1 "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/client"
	"google.golang.org/protobuf/types/known/structpb"
)

// Subtask represents a subtask to be processed by an agent
type Subtask struct {
	SubtaskID   string
	ParentID    string
	AgentType   string
	Description string
	Input       map[string]interface{}
	Priority    int32
	Status      string
	CreatedAt   time.Time
	DependsOn   []string
}

// AgentResult represents the result of processing a subtask
type AgentResult struct {
	SubtaskID   string                 `json:"subtask_id"`
	ParentID    string                 `json:"parent_id"`
	AgentType   string                 `json:"agent_type"`
	Status      string                 `json:"status"` // "completed", "failed"
	Result      map[string]interface{} `json:"result"`
	Error       string                 `json:"error,omitempty"`
	CompletedAt time.Time              `json:"completed_at"`
}

// Agent defines the interface for all specialized agents
type Agent interface {
	Start(ctx context.Context) error
	Stop()
	ProcessSubtask(ctx context.Context, subtask *Subtask) (*AgentResult, error)
}

// BaseAgent provides common functionality for all agents
type BaseAgent struct {
	client      *client.ChronoQueueClient
	queueName   string
	agentType   string
	workers     int
	verbose     bool
	stopChan    chan struct{}
	wg          sync.WaitGroup
	resultStore *ResultStore
}

// NewBaseAgent creates a new base agent
func NewBaseAgent(c *client.ChronoQueueClient, queueName, agentType string, workers int, verbose bool) *BaseAgent {
	return &BaseAgent{
		client:      c,
		queueName:   queueName,
		agentType:   agentType,
		workers:     workers,
		verbose:     verbose,
		stopChan:    make(chan struct{}),
		resultStore: NewResultStore(c, verbose),
	}
}

// SetResultStore allows setting a shared result store across agents
func (agent *BaseAgent) SetResultStore(rs *ResultStore) {
	agent.resultStore = rs
}

// Start begins processing messages from the queue
func (agent *BaseAgent) Start(ctx context.Context, processor func(context.Context, *Subtask) (*AgentResult, error)) error {
	if agent.verbose {
		fmt.Printf("🚀 Starting %s agent with %d workers\n", agent.agentType, agent.workers)
		fmt.Printf("   Queue: %s\n\n", agent.queueName)
	}

	// Start worker goroutines
	for i := 1; i <= agent.workers; i++ {
		agent.wg.Add(1)
		go agent.worker(ctx, i, processor)
	}

	// Wait for all workers to finish
	agent.wg.Wait()
	return nil
}

// Stop gracefully stops the agent
func (agent *BaseAgent) Stop() {
	close(agent.stopChan)
}

// worker processes messages from the queue
func (agent *BaseAgent) worker(ctx context.Context, workerID int, processor func(context.Context, *Subtask) (*AgentResult, error)) {
	defer agent.wg.Done()

	if agent.verbose {
		fmt.Printf("[%s Worker %d] Started\n", agent.agentType, workerID)
	}

	for {
		select {
		case <-agent.stopChan:
			if agent.verbose {
				fmt.Printf("[%s Worker %d] Stopping\n", agent.agentType, workerID)
			}
			return
		case <-ctx.Done():
			if agent.verbose {
				fmt.Printf("[%s Worker %d] Context cancelled\n", agent.agentType, workerID)
			}
			return
		default:
			// Get next message
			resp, err := agent.client.GetNextMessage(ctx, agent.queueName, "2m", true)
			if err != nil {
				fmt.Printf("[%s Worker %d] Error getting message: %v\n", agent.agentType, workerID, err)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			if resp.Message == nil {
				// No messages available
				time.Sleep(500 * time.Millisecond)
				continue
			}

			// Process the message
			if err := agent.processMessage(ctx, workerID, resp, processor); err != nil {
				fmt.Printf("[%s Worker %d] Error processing message %s: %v\n", agent.agentType, workerID, resp.Message.MessageId, err)
			}
		}
	}
}

// processMessage handles a single message
func (agent *BaseAgent) processMessage(ctx context.Context, workerID int, resp *queueservicev1.GetNextMessageResponse, processor func(context.Context, *Subtask) (*AgentResult, error)) error {
	msg := resp.Message

	if agent.verbose {
		fmt.Printf("[%s Worker %d] Processing subtask: %s\n", agent.agentType, workerID, msg.MessageId)
	}

	// Parse subtask from message payload
	subtask, err := agent.parseSubtask(msg)
	if err != nil {
		// Acknowledge with error
		err := agent.acknowledgeMessage(ctx, resp.StreamEntryId, msg.MessageId, client.MESSAGE_ERRORED)
		if err != nil {
			fmt.Printf("[%s Worker %d] Error acknowledging message %s: %v\n", agent.agentType, workerID, msg.MessageId, err)
		}

		return fmt.Errorf("failed to parse subtask: %w", err)
	}

	// Process the subtask using the provided processor function
	result, err := processor(ctx, subtask)
	if err != nil {
		// Acknowledge with error
		err := agent.acknowledgeMessage(ctx, resp.StreamEntryId, msg.MessageId, client.MESSAGE_ERRORED)
		if err != nil {
			fmt.Printf("[%s Worker %d] Error acknowledging message %s: %v\n", agent.agentType, workerID, msg.MessageId, err)
		}

		return fmt.Errorf("failed to process subtask: %w", err)
	}

	// Post result to results queue (for aggregator to pick up)
	// Skip if posting fails (results queue may not exist in demo)
	if err := agent.postResult(ctx, result); err != nil {
		if agent.verbose {
			fmt.Printf("[%s Worker %d] Failed to post Result: %v\n", agent.agentType, workerID, result)
			fmt.Printf("[%s Worker %d] Note: Could not post result (queue may not exist): %v\n", agent.agentType, workerID, err)
		}
		// Continue anyway - result was processed successfully
	}

	// Acknowledge successful processing
	if err := agent.acknowledgeMessage(ctx, resp.StreamEntryId, msg.MessageId, client.MESSAGE_COMPLETED); err != nil {
		return fmt.Errorf("failed to acknowledge message: %w", err)
	}

	if agent.verbose {
		fmt.Printf("[%s Worker %d] ✓ Subtask %s completed\n", agent.agentType, workerID, msg.MessageId)
	}

	return nil
}

// parseSubtask extracts subtask data from message payload
func (agent *BaseAgent) parseSubtask(msg *messagev1.Message) (*Subtask, error) {
	if msg.Metadata == nil || msg.Metadata.Payload == nil || msg.Metadata.Payload.Data == nil {
		return nil, fmt.Errorf("message has no payload")
	}

	fields := msg.Metadata.Payload.Data.Fields

	subtask := &Subtask{
		SubtaskID:   getString(fields, "subtask_id"),
		ParentID:    getString(fields, "parent_id"),
		AgentType:   getString(fields, "agent_type"),
		Description: getString(fields, "description"),
		Priority:    getInt32(fields, "priority"),
		Status:      getString(fields, "status"),
	}

	// Parse created_at timestamp
	if timestamp := getFloat64(fields, "created_at"); timestamp > 0 {
		subtask.CreatedAt = time.Unix(int64(timestamp), 0)
	}

	// Parse input map
	if inputValue, ok := fields["input"]; ok {
		if inputStruct := inputValue.GetStructValue(); inputStruct != nil {
			subtask.Input = structToMap(inputStruct)
		}
	}

	// Parse depends_on array
	if depsValue, ok := fields["depends_on"]; ok {
		if depsList := depsValue.GetListValue(); depsList != nil {
			for _, v := range depsList.Values {
				if s := v.GetStringValue(); s != "" {
					subtask.DependsOn = append(subtask.DependsOn, s)
				}
			}
		}
	}

	return subtask, nil
}

// postResult posts the agent result to a results queue
func (agent *BaseAgent) postResult(ctx context.Context, result *AgentResult) error {
	// Use the result store to save results for historical reference
	if agent.resultStore != nil {
		return agent.resultStore.StoreResult(ctx, result)
	}

	// Fallback: old behavior (shouldn't happen)
	return fmt.Errorf("result store not initialized")
}

// acknowledgeMessage acknowledges message processing
func (agent *BaseAgent) acknowledgeMessage(ctx context.Context, streamEntryID, messageID string, state client.State) error {
	_, err := agent.client.AcknowledgeMessage(ctx, agent.queueName, messageID, state, streamEntryID)
	return err
}

// Helper functions

func getString(fields map[string]*structpb.Value, key string) string {
	if v, ok := fields[key]; ok {
		return v.GetStringValue()
	}
	return ""
}

func getInt32(fields map[string]*structpb.Value, key string) int32 {
	if v, ok := fields[key]; ok {
		return int32(v.GetNumberValue())
	}
	return 0
}

func getFloat64(fields map[string]*structpb.Value, key string) float64 {
	if v, ok := fields[key]; ok {
		return v.GetNumberValue()
	}
	return 0
}

func structToMap(s *structpb.Struct) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range s.Fields {
		result[k] = valueToInterface(v)
	}
	return result
}

func valueToInterface(v *structpb.Value) interface{} {
	switch v.Kind.(type) {
	case *structpb.Value_StringValue:
		return v.GetStringValue()
	case *structpb.Value_NumberValue:
		return v.GetNumberValue()
	case *structpb.Value_BoolValue:
		return v.GetBoolValue()
	case *structpb.Value_StructValue:
		return structToMap(v.GetStructValue())
	case *structpb.Value_ListValue:
		list := v.GetListValue()
		result := make([]interface{}, len(list.Values))
		for i, val := range list.Values {
			result[i] = valueToInterface(val)
		}
		return result
	default:
		return nil
	}
}

func convertToStructPBCompatible(input interface{}) interface{} {
	switch v := input.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for key, val := range v {
			result[key] = convertToStructPBCompatible(val)
		}
		return result
	case []map[string]interface{}:
		result := make([]interface{}, len(v))
		for i, m := range v {
			result[i] = convertToStructPBCompatible(m)
		}
		return result
	case []string:
		result := make([]interface{}, len(v))
		for i, str := range v {
			result[i] = str
		}
		return result
	case []float64:
		result := make([]interface{}, len(v))
		for i, f := range v {
			result[i] = f
		}
		return result
	case []int:
		result := make([]interface{}, len(v))
		for i, num := range v {
			result[i] = float64(num)
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
