package agents

import (
	"context"
	"fmt"
	"time"

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
	return &AggregatorAgent{
		BaseAgent: NewBaseAgent(c, "agent-aggregator", "aggregator", workers, verbose),
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
	report, err := agent.llmClient.SynthesizeResults(subtask.ParentID, agentResults)
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

	return &AgentResult{
		SubtaskID:   subtask.SubtaskID,
		ParentID:    subtask.ParentID,
		AgentType:   "aggregator",
		Status:      "completed",
		Result:      aggregatedResult,
		CompletedAt: time.Now(),
	}, nil
}

// collectResults fetches results from completed dependency subtasks
func (agent *AggregatorAgent) collectResults(ctx context.Context, parentID string, dependencies []string) []map[string]interface{} {
	// In a real implementation, this would:
	// 1. Query the results queue for the parent task
	// 2. Filter by dependency subtask IDs
	// 3. Return collected results

	// For now, return mock collected results
	results := make([]map[string]interface{}, 0, len(dependencies))

	for i, depID := range dependencies {
		// Mock result based on dependency type
		var mockResult map[string]interface{}

		if contains(depID, "web") {
			mockResult = map[string]interface{}{
				"subtask_id": depID,
				"agent_type": "web-search",
				"result": map[string]interface{}{
					"query":      "sample query",
					"count":      5,
					"top_result": "Relevant information found",
				},
			}
		} else if contains(depID, "code") {
			mockResult = map[string]interface{}{
				"subtask_id": depID,
				"agent_type": "code-analyzer",
				"result": map[string]interface{}{
					"quality_score":  85.5,
					"findings_count": 4,
					"status":         "analyzed",
				},
			}
		} else if contains(depID, "data") {
			mockResult = map[string]interface{}{
				"subtask_id": depID,
				"agent_type": "data-processor",
				"result": map[string]interface{}{
					"records_processed": 15780,
					"insights_count":    4,
					"trend":             "positive",
				},
			}
		} else {
			mockResult = map[string]interface{}{
				"subtask_id": depID,
				"agent_type": "unknown",
				"result": map[string]interface{}{
					"status": "completed",
				},
			}
		}

		results = append(results, mockResult)

		// Simulate incremental collection
		if i < len(dependencies)-1 {
			time.Sleep(100 * time.Millisecond)
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

// contains checks if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
