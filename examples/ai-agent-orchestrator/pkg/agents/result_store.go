package agents

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/adrien19/chronoqueue/client"
	"google.golang.org/protobuf/types/known/structpb"
)

// ResultStore manages agent results for historical reference and aggregation
type ResultStore struct {
	client    *client.ChronoQueueClient
	queueName string
	mu        sync.RWMutex
	cache     map[string][]*AgentResult // parentID -> results
	verbose   bool
	// Note: Results are stored in-memory cache within the running agent process.
	// For persistent cross-process access, results would need to be stored in
	// a database or retrieved from the ChronoQueue results queue.
}

// NewResultStore creates a new result store
func NewResultStore(c *client.ChronoQueueClient, verbose bool) *ResultStore {
	return &ResultStore{
		client:    c,
		queueName: "agent-results",
		cache:     make(map[string][]*AgentResult),
		verbose:   verbose,
	}
}

// Initialize creates the results queue (no-op since queue is created via init command)
func (rs *ResultStore) Initialize(ctx context.Context) error {
	// Results queue is created via the init command
	// No initialization needed here
	return nil
}

// StoreResult saves an agent result to the queue and cache
func (rs *ResultStore) StoreResult(ctx context.Context, result *AgentResult) error {
	// Add to cache
	rs.mu.Lock()
	rs.cache[result.ParentID] = append(rs.cache[result.ParentID], result)
	rs.mu.Unlock()

	// Convert result to structpb-compatible format
	resultData := map[string]interface{}{
		"subtask_id":   result.SubtaskID,
		"parent_id":    result.ParentID,
		"agent_type":   result.AgentType,
		"status":       result.Status,
		"completed_at": result.CompletedAt.Unix(),
	}

	if result.Result != nil {
		resultData["result"] = convertToStructPBCompatible(result.Result)
	}

	if result.Error != "" {
		resultData["error"] = result.Error
	}

	payloadStruct, err := structpb.NewStruct(resultData)
	if err != nil {
		return fmt.Errorf("failed to create result payload: %w", err)
	}

	// Store in ChronoQueue for persistence and historical reference
	messageID := fmt.Sprintf("%s-%s", result.ParentID, result.SubtaskID)
	_, err = rs.client.PostMessage(ctx, rs.queueName, messageID, client.MessageOptions{
		Priority: 5,
		Payload: client.Payload{
			Data:        payloadStruct,
			ContentType: "application/json",
		},
	})

	if err != nil {
		// Don't fail if we can't persist to queue - we have it in cache
		if rs.verbose {
			fmt.Printf("⚠️  Could not persist result to queue (cached only): %v\n", err)
		}
		return nil // Return success since we have it in cache
	}

	if rs.verbose {
		fmt.Printf("✓ Stored result: %s (parent: %s, agent: %s)\n",
			result.SubtaskID, result.ParentID, result.AgentType)
	}

	return nil
}

// GetResultsByParentID retrieves all results for a parent task from cache
func (rs *ResultStore) GetResultsByParentID(parentID string) []*AgentResult {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	results, ok := rs.cache[parentID]
	if !ok {
		return nil
	}

	// Return a copy to avoid external modification
	resultsCopy := make([]*AgentResult, len(results))
	copy(resultsCopy, results)
	return resultsCopy
}

// GetResultsByAgentType retrieves all results for a specific agent type
func (rs *ResultStore) GetResultsByAgentType(parentID, agentType string) []*AgentResult {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	results, ok := rs.cache[parentID]
	if !ok {
		return nil
	}

	filtered := make([]*AgentResult, 0)
	for _, result := range results {
		if result.AgentType == agentType {
			filtered = append(filtered, result)
		}
	}

	return filtered
}

// ListAllResults returns all cached results
func (rs *ResultStore) ListAllResults() map[string][]*AgentResult {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	// Return a deep copy
	copy := make(map[string][]*AgentResult)
	for k, v := range rs.cache {
		resultsCopy := make([]*AgentResult, len(v))
		copy[k] = resultsCopy
		for i, r := range v {
			resultsCopy[i] = r
		}
	}

	return copy
}

// GetCompletionStatus returns completion stats for a parent task
func (rs *ResultStore) GetCompletionStatus(parentID string) map[string]int {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	results := rs.cache[parentID]
	stats := map[string]int{
		"total":     len(results),
		"completed": 0,
		"failed":    0,
	}

	for _, result := range results {
		switch result.Status {
		case "completed":
			stats["completed"]++
		case "failed":
			stats["failed"]++
		}
	}

	return stats
}

// ExportResults exports all results for a parent task as JSON
func (rs *ResultStore) ExportResults(parentID string) (string, error) {
	results := rs.GetResultsByParentID(parentID)
	if results == nil {
		return "", fmt.Errorf("no results found for parent ID: %s", parentID)
	}

	jsonData, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal results: %w", err)
	}

	return string(jsonData), nil
}

// ClearCache clears the in-memory cache (results still persist in queue)
func (rs *ResultStore) ClearCache() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.cache = make(map[string][]*AgentResult)
}
