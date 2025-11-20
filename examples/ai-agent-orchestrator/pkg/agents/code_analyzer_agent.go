package agents

import (
	"context"
	"fmt"
	"time"

	"github.com/adrien19/chronoqueue/client"
)

// CodeAnalyzerAgent processes code analysis subtasks
type CodeAnalyzerAgent struct {
	*BaseAgent
}

// NewCodeAnalyzerAgent creates a new code analyzer agent
func NewCodeAnalyzerAgent(c *client.ChronoQueueClient, workers int, verbose bool) *CodeAnalyzerAgent {
	return &CodeAnalyzerAgent{
		BaseAgent: NewBaseAgent(c, "agent-code-analyzer", "code-analyzer", workers, verbose),
	}
}

// Start begins processing code analysis subtasks
func (agent *CodeAnalyzerAgent) Start(ctx context.Context) error {
	return agent.BaseAgent.Start(ctx, agent.processCodeAnalysis)
}

// processCodeAnalysis performs the actual code analysis (mocked for demo)
func (agent *CodeAnalyzerAgent) processCodeAnalysis(ctx context.Context, subtask *Subtask) (*AgentResult, error) {
	if agent.verbose {
		fmt.Printf("   📊 Analyzing: %s\n", subtask.Description)
	}

	// Simulate analysis delay
	time.Sleep(700 * time.Millisecond)

	// Extract parameters from input
	repository, ok := subtask.Input["repository"].(string)
	if !ok {
		repository = ""
	}
	analysisType, ok := subtask.Input["analysis_type"].(string)
	if !ok {
		analysisType = ""
	}

	if repository == "" {
		repository = "unknown"
	}
	if analysisType == "" {
		analysisType = "general"
	}

	// Mock analysis results
	results := map[string]interface{}{
		"repository":     repository,
		"analysis_type":  analysisType,
		"findings":       generateMockCodeFindings(analysisType),
		"quality_score":  85.5,
		"lines_analyzed": 12543,
	}

	return &AgentResult{
		SubtaskID:   subtask.SubtaskID,
		ParentID:    subtask.ParentID,
		AgentType:   "code-analyzer",
		Status:      "completed",
		Result:      results,
		CompletedAt: time.Now(),
	}, nil
}

// generateMockCodeFindings creates mock code analysis findings
func generateMockCodeFindings(analysisType string) []map[string]interface{} {
	return []map[string]interface{}{
		{
			"type":        "code_quality",
			"severity":    "medium",
			"description": "Found 3 functions exceeding complexity threshold",
			"locations":   []string{"src/main.go:45", "src/utils.go:128"},
		},
		{
			"type":        "security",
			"severity":    "low",
			"description": "Potential SQL injection vulnerability",
			"locations":   []string{"src/database.go:67"},
		},
		{
			"type":        "performance",
			"severity":    "low",
			"description": "Inefficient loop iteration detected",
			"locations":   []string{"src/processor.go:234"},
		},
		{
			"type":        "best_practices",
			"severity":    "info",
			"description": "Consider using context.Context for cancellation",
			"locations":   []string{"src/handler.go:89", "src/service.go:156"},
		},
	}
}
