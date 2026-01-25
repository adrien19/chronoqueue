package agents

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/examples/ai-agent-orchestrator/pkg/llm"
	"github.com/adrien19/chronoqueue/examples/ai-agent-orchestrator/pkg/models"
)

// AggregatorAgent collects and synthesizes results from other agents
type AggregatorAgent struct {
	*BaseAgent
	llmClient llm.LLMClient
}

// NewAggregatorAgent creates a new aggregator agent
func NewAggregatorAgent(c *client.ChronoQueueClient, llmClient llm.LLMClient, workers int, verbose bool) *AggregatorAgent {
	baseAgent := NewBaseAgent(c, "agent-aggregator", "aggregator", workers, verbose)
	baseAgent.skipResultPosting = true // Aggregator results don't need to be in agent-results
	return &AggregatorAgent{
		BaseAgent: baseAgent,
		llmClient: llmClient,
	}
}

// Start begins processing aggregation subtasks
func (agent *AggregatorAgent) Start(ctx context.Context) error {
	return agent.BaseAgent.Start(ctx, agent.processAggregation)
}

// processAggregation collects results and synthesizes final output
func (agent *AggregatorAgent) processAggregation(ctx context.Context, subtask *Subtask) (*AgentResult, error) {
	if agent.verbose {
		fmt.Printf("   🎯 Aggregating: %s\n", subtask.Description)
	}

	// Get dependencies (subtasks that must complete first)
	dependencies := subtask.DependsOn
	if agent.verbose && len(dependencies) > 0 {
		fmt.Printf("   → Waiting for %d dependencies: %v\n", len(dependencies), dependencies)
	}

	// Simulate waiting for and collecting results
	time.Sleep(300 * time.Millisecond)

	// Collect results from dependency subtasks
	collectedResults := agent.collectResults(ctx, subtask.ParentID, dependencies)

	if agent.verbose {
		fmt.Printf("   → Collected %d results\n", len(collectedResults))
	}

	// Convert collected results to AgentResult format
	agentResults := convertToAgentResults(collectedResults)

	// Synthesize final result using LLM
	report, err := agent.llmClient.SynthesizeResults(ctx, subtask.ParentID, agentResults)
	if err != nil {
		return nil, fmt.Errorf("failed to synthesize results: %w", err)
	}

	// Create final aggregated result
	// Convert sections to simple format
	sections := make([]map[string]interface{}, len(report.Sections))
	for i, sec := range report.Sections {
		sections[i] = map[string]interface{}{
			"title":   sec.Title,
			"content": sec.Content,
		}
	}

	aggregatedResult := map[string]interface{}{
		"task_id":           subtask.ParentID,
		"subtasks_analyzed": len(collectedResults),
		"summary":           report.Summary,
		"sections":          sections,
		"generated_at":      report.GeneratedAt.Unix(),
		"raw_results":       collectedResults,
	}

	if agent.verbose {
		fmt.Printf("   ✅ Synthesis complete (%d sections)\n", len(report.Sections))
	}

	// Save aggregated results to file for user inspection
	outputPath, err := agent.saveResultsToFile(subtask.ParentID, report)
	if err != nil {
		if agent.verbose {
			fmt.Printf("   ⚠️  Could not save results to file: %v\n", err)
		}
	} else {
		// Always show the output file path (not just verbose)
		fmt.Printf("   💾 Results saved: %s\n", outputPath)
	}

	// Send aggregated report to notification agent
	if err := agent.sendToNotification(ctx, subtask.ParentID, aggregatedResult); err != nil {
		if agent.verbose {
			fmt.Printf("   ⚠️  Could not send to notification queue: %v\n", err)
		}
		// Don't fail - we still return the aggregated result
	}

	// Log aggregation summary
	fmt.Printf("   📊 Aggregation complete: %d subtasks → %d sections\n",
		len(collectedResults), len(report.Sections))

	return &AgentResult{
		SubtaskID:   subtask.SubtaskID,
		ParentID:    subtask.ParentID,
		AgentType:   "aggregator",
		Status:      "completed",
		Result:      aggregatedResult,
		CompletedAt: time.Now(),
	}, nil
}

// saveResultsToFile writes the aggregated report to a JSON file
func (agent *AggregatorAgent) saveResultsToFile(parentID string, report *models.Report) (string, error) {
	// Create output directory if it doesn't exist
	outputDir := "output"
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}

	// Generate filename with timestamp
	timestamp := time.Now().Format("20060102-150405")
	filename := fmt.Sprintf("%s/%s-%s.json", outputDir, parentID, timestamp)

	// Create output structure
	output := map[string]interface{}{
		"task_id":      parentID,
		"generated_at": report.GeneratedAt.Format(time.RFC3339),
		"summary":      report.Summary,
		"sections":     report.Sections,
		"metadata": map[string]interface{}{
			"total_sections": len(report.Sections),
			"aggregator":     "ai-agent-orchestrator",
			"version":        "1.0.0",
		},
	}

	// Marshal to pretty JSON
	jsonData, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal results: %w", err)
	}

	// Write to file
	if err := os.WriteFile(filename, jsonData, 0o644); err != nil {
		return "", fmt.Errorf("failed to write file: %w", err)
	}

	return filename, nil
}

// sendToNotification sends the aggregated report to the notification queue
func (agent *AggregatorAgent) sendToNotification(ctx context.Context, parentID string, reportData map[string]interface{}) error {
	subtaskID := fmt.Sprintf("%s-notify", parentID)

	// Convert reportData to structpb-compatible format
	compatibleInput := convertToStructPBCompatible(reportData)

	// Prepare subtask data
	subtaskData := map[string]interface{}{
		"subtask_id":  subtaskID,
		"parent_id":   parentID,
		"agent_type":  "notification",
		"description": "Deliver final report",
		"input":       compatibleInput,
		"status":      "pending",
		"priority":    int32(8),
		"created_at":  time.Now().Unix(),
	}

	payloadStruct, err := structpb.NewStruct(subtaskData)
	if err != nil {
		return fmt.Errorf("failed to create notification payload: %w", err)
	}

	_, err = agent.client.PostMessage(ctx, "agent-notification", subtaskID, client.MessageOptions{
		Priority: 8,
		Payload: client.Payload{
			Data:        payloadStruct,
			ContentType: "application/json",
		},
	})
	if err != nil {
		return fmt.Errorf("failed to post notification task: %w", err)
	}

	if agent.verbose {
		fmt.Printf("   → Sent to notification queue: %s\n", subtaskID)
	}

	return nil
}

// collectResults fetches results from agent-results queue and ACKs them
func (agent *AggregatorAgent) collectResults(ctx context.Context, parentID string, dependencies []string) []map[string]interface{} {
	results := make([]map[string]interface{}, 0, len(dependencies))
	collectedIDs := make(map[string]bool)

	// Create dependency map for quick lookup
	depMap := make(map[string]bool)
	for _, dep := range dependencies {
		depMap[dep] = true
	}

	if agent.verbose {
		fmt.Printf("   → Collecting from agent-results queue (need %d results)\n", len(dependencies))
	}

	// Poll agent-results queue until we have all dependencies
	maxAttempts := 30 // 30 attempts * 500ms = 15 seconds max wait
	attempts := 0

	for len(collectedIDs) < len(dependencies) && attempts < maxAttempts {
		// Get next message from results queue
		resp, err := agent.client.GetNextMessage(ctx, "agent-results", "30s", true)
		if err != nil {
			if agent.verbose {
				fmt.Printf("   ⚠️  Error fetching from agent-results: %v\n", err)
			}
			time.Sleep(500 * time.Millisecond)
			attempts++
			continue
		}

		if resp.Message == nil {
			// No messages available, wait and retry
			time.Sleep(500 * time.Millisecond)
			attempts++
			continue
		}

		msg := resp.Message

		// Parse the result from message payload
		if msg.Metadata == nil || msg.Metadata.Payload == nil || msg.Metadata.Payload.Data == nil {
			// Invalid message, ACK to remove it
			if _, err := agent.client.AcknowledgeMessage(ctx, "agent-results", msg.MessageId, client.MESSAGE_COMPLETED); err != nil && agent.verbose {
				fmt.Printf("   ⚠️  Failed to ACK invalid message: %v\n", err)
			}
			continue
		}

		resultData := msg.Metadata.Payload.Data.AsMap()
		subtaskID := getStringFromMap(resultData, "subtask_id")
		resultParentID := getStringFromMap(resultData, "parent_id")

		// Check if this result belongs to our parent task and is in our dependencies
		if resultParentID == parentID && depMap[subtaskID] {
			// Only collect if we haven't already
			if !collectedIDs[subtaskID] {
				result := map[string]interface{}{
					"subtask_id": subtaskID,
					"parent_id":  resultParentID,
					"agent_type": getStringFromMap(resultData, "agent_type"),
					"status":     getStringFromMap(resultData, "status"),
					"result":     resultData["result"],
				}

				results = append(results, result)
				collectedIDs[subtaskID] = true

				if agent.verbose {
					fmt.Printf("   ✓ Collected result %d/%d: %s\n", len(collectedIDs), len(dependencies), subtaskID)
				}

				// ACK the message to remove it from queue
				if _, err := agent.client.AcknowledgeMessage(ctx, "agent-results", msg.MessageId, client.MESSAGE_COMPLETED); err != nil {
					if agent.verbose {
						fmt.Printf("   ⚠️  Failed to ACK message %s: %v\n", msg.MessageId, err)
					}
				}
			} else {
				// Already collected, just ACK to remove duplicate
				if _, err := agent.client.AcknowledgeMessage(ctx, "agent-results", msg.MessageId, client.MESSAGE_COMPLETED); err != nil {
					if agent.verbose {
						fmt.Printf("   ⚠️  Failed to ACK duplicate message %s: %v\n", msg.MessageId, err)
					}
				}
			}
		} else {
			// Not our result, also ACK it (another aggregator task will collect its own results)
			if _, err := agent.client.AcknowledgeMessage(ctx, "agent-results", msg.MessageId, client.MESSAGE_COMPLETED); err != nil {
				if agent.verbose {
					fmt.Printf("   ⚠️  Failed to ACK unrelated message %s: %v\n", msg.MessageId, err)
				}
			}
		}

		attempts++
	}

	if len(collectedIDs) < len(dependencies) {
		if agent.verbose {
			fmt.Printf("   ⚠️  Timeout: collected %d/%d results\n", len(collectedIDs), len(dependencies))
		}
	}

	return results
}

// convertToAgentResults converts collected results to AgentResult format
func convertToAgentResults(collected []map[string]interface{}) []*models.AgentResult {
	results := make([]*models.AgentResult, 0, len(collected))

	for _, item := range collected {
		result := &models.AgentResult{
			SubtaskID:   getStringFromMap(item, "subtask_id"),
			AgentType:   getStringFromMap(item, "agent_type"),
			Success:     true,
			ProcessedAt: time.Now(),
			Duration:    500 * time.Millisecond,
		}

		// Extract result data
		if resultData, ok := item["result"].(map[string]interface{}); ok {
			result.Data = resultData
		}

		results = append(results, result)
	}

	return results
}

// Helper to get string from map
func getStringFromMap(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}
