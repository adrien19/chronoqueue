package agents

import (
	"context"
	"fmt"
	"time"

	"github.com/adrien19/chronoqueue/client"
)

// WebSearchAgent processes web search subtasks
type WebSearchAgent struct {
	*BaseAgent
}

// NewWebSearchAgent creates a new web search agent
func NewWebSearchAgent(c *client.ChronoQueueClient, workers int, verbose bool) *WebSearchAgent {
	return &WebSearchAgent{
		BaseAgent: NewBaseAgent(c, "agent-web-search", "web-search", workers, verbose),
	}
}

// Start begins processing web search subtasks
func (agent *WebSearchAgent) Start(ctx context.Context) error {
	return agent.BaseAgent.Start(ctx, agent.processWebSearch)
}

// processWebSearch performs the actual web search (mocked for demo)
func (agent *WebSearchAgent) processWebSearch(ctx context.Context, subtask *Subtask) (*AgentResult, error) {
	if agent.verbose {
		fmt.Printf("   🔍 Searching: %s\n", subtask.Description)
	}

	// Simulate web search delay
	time.Sleep(500 * time.Millisecond)

	// Extract query from input
	query, _ := subtask.Input["query"].(string)
	if query == "" {
		query = subtask.Description
	}

	// Mock search results
	results := map[string]interface{}{
		"query":   query,
		"results": generateMockSearchResults(query),
		"count":   5,
	}

	return &AgentResult{
		SubtaskID:   subtask.SubtaskID,
		ParentID:    subtask.ParentID,
		AgentType:   "web-search",
		Status:      "completed",
		Result:      results,
		CompletedAt: time.Now(),
	}, nil
}

// generateMockSearchResults creates mock search results based on query
func generateMockSearchResults(query string) []map[string]interface{} {
	return []map[string]interface{}{
		{
			"title":   fmt.Sprintf("Top result for: %s", query),
			"url":     "https://example.com/article1",
			"snippet": "Comprehensive analysis and insights...",
			"score":   0.95,
		},
		{
			"title":   fmt.Sprintf("Guide to %s", query),
			"url":     "https://example.com/guide",
			"snippet": "Step-by-step instructions and best practices...",
			"score":   0.89,
		},
		{
			"title":   fmt.Sprintf("%s - Latest Updates", query),
			"url":     "https://example.com/news",
			"snippet": "Recent developments and trends...",
			"score":   0.82,
		},
		{
			"title":   fmt.Sprintf("Expert opinion on %s", query),
			"url":     "https://example.com/expert",
			"snippet": "Professional insights and recommendations...",
			"score":   0.78,
		},
		{
			"title":   fmt.Sprintf("%s Resources", query),
			"url":     "https://example.com/resources",
			"snippet": "Curated collection of tools and references...",
			"score":   0.75,
		},
	}
}
