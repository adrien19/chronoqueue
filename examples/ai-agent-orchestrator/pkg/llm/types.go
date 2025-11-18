package llm

import (
	"github.com/adrien19/chronoqueue/examples/ai-agent-orchestrator/pkg/models"
)

// LLMClient defines the interface for interacting with language models
type LLMClient interface {
	// DecomposeTask takes a task and returns a decomposition into subtasks
	DecomposeTask(task *models.Task) (*models.TaskDecomposition, error)

	// SynthesizeResults takes multiple agent results and creates a final report
	SynthesizeResults(taskID string, results []*models.AgentResult) (*models.Report, error)
}

// DecomposeTaskRequest represents a request to decompose a task
type DecomposeTaskRequest struct {
	TaskID      string                 `json:"task_id"`
	TaskType    models.TaskType        `json:"task_type"`
	Description string                 `json:"description"`
	Input       map[string]interface{} `json:"input"`
	Priority    int32                  `json:"priority"`
}

// DecomposeTaskResponse represents the LLM response for task decomposition
type DecomposeTaskResponse struct {
	Subtasks  []SubtaskSpec `json:"subtasks"`
	Reasoning string        `json:"reasoning,omitempty"`
}

// SubtaskSpec defines a subtask from the LLM
type SubtaskSpec struct {
	SubtaskID    string                 `json:"subtask_id"`
	AgentType    models.AgentType       `json:"agent_type"`
	Description  string                 `json:"description"`
	Input        map[string]interface{} `json:"input"`
	Dependencies []string               `json:"dependencies,omitempty"`
	Priority     int32                  `json:"priority"`
}

// SynthesizeResultsRequest represents a request to synthesize results
type SynthesizeResultsRequest struct {
	TaskID      string                `json:"task_id"`
	TaskType    models.TaskType       `json:"task_type"`
	Description string                `json:"description"`
	Results     []*models.AgentResult `json:"results"`
}

// SynthesizeResultsResponse represents the LLM response for result synthesis
type SynthesizeResultsResponse struct {
	Title    string                 `json:"title"`
	Summary  string                 `json:"summary"`
	Sections []models.ReportSection `json:"sections"`
}
