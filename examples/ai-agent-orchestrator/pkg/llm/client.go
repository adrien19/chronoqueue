package llm

import (
	"context"

	"github.com/adrien19/chronoqueue/examples/ai-agent-orchestrator/pkg/models"
)

// LLMClient defines the interface for language model operations
type LLMClient interface {
	// DecomposeTask breaks down a complex task into smaller subtasks
	DecomposeTask(task *models.Task) (*models.TaskDecomposition, error)

	// SynthesizeResults combines multiple agent results into a comprehensive report
	SynthesizeResults(taskID string, results []*models.AgentResult) (*models.Report, error)

	// Generate produces a response based on system and user prompts
	Generate(ctx context.Context, systemPrompt, userPrompt string) (string, error)
}
