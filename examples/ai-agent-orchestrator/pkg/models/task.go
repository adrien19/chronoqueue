package models

import "time"

// Task represents a user-submitted task
type Task struct {
	TaskID      string                 `json:"task_id"`
	TaskType    string                 `json:"task_type"`
	Description string                 `json:"description"`
	Input       map[string]interface{} `json:"input"`
	Priority    int32                  `json:"priority"`
	TenantID    string                 `json:"tenant_id"`
	CreatedAt   time.Time              `json:"created_at"`
	Status      TaskStatus             `json:"status"`
	Subtasks    []Subtask              `json:"subtasks,omitempty"`
}

// TaskStatus represents the current state of a task
type TaskStatus string

const (
	TaskStatusPending    TaskStatus = "pending"
	TaskStatusProcessing TaskStatus = "processing"
	TaskStatusCompleted  TaskStatus = "completed"
	TaskStatusFailed     TaskStatus = "failed"
)

// Subtask represents a decomposed subtask assigned to a specialized agent
type Subtask struct {
	SubtaskID   string                 `json:"subtask_id"`
	ParentID    string                 `json:"parent_id"`
	AgentType   string                 `json:"agent_type"`
	Description string                 `json:"description"`
	Input       map[string]interface{} `json:"input"`
	Priority    int32                  `json:"priority"`
	DependsOn   []string               `json:"depends_on,omitempty"`
	Status      TaskStatus             `json:"status"`
	Result      *AgentResult           `json:"result,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
	CompletedAt *time.Time             `json:"completed_at,omitempty"`
}

// AgentResult represents the result from an agent's processing
type AgentResult struct {
	SubtaskID   string                 `json:"subtask_id"`
	AgentType   string                 `json:"agent_type"`
	Success     bool                   `json:"success"`
	Data        map[string]interface{} `json:"data"`
	Error       string                 `json:"error,omitempty"`
	ProcessedAt time.Time              `json:"processed_at"`
	Duration    time.Duration          `json:"duration"`
	Metadata    map[string]string      `json:"metadata,omitempty"`
}

// Report represents the final aggregated report
type Report struct {
	TaskID      string                 `json:"task_id"`
	TaskType    string                 `json:"task_type"`
	Summary     string                 `json:"summary"`
	Sections    []ReportSection        `json:"sections"`
	Metadata    map[string]interface{} `json:"metadata"`
	GeneratedAt time.Time              `json:"generated_at"`
}

// ReportSection represents a section in the final report
type ReportSection struct {
	Title   string                 `json:"title"`
	Content string                 `json:"content"`
	Data    map[string]interface{} `json:"data,omitempty"`
	Source  string                 `json:"source"` // Which agent produced this
}

// TaskDecomposition represents the LLM's task decomposition output
type TaskDecomposition struct {
	TaskID   string                 `json:"task_id"`
	Subtasks []Subtask              `json:"subtasks"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// AgentType represents the type of specialized agent
type AgentType string

const (
	AgentTypeCoordinator   AgentType = "coordinator"
	AgentTypeWebSearch     AgentType = "web-search"
	AgentTypeCodeAnalyzer  AgentType = "code-analyzer"
	AgentTypeDataProcessor AgentType = "data-processor"
	AgentTypeAggregator    AgentType = "aggregator"
	AgentTypeNotification  AgentType = "notification"
)

// QueueName returns the queue name for an agent type
func (a AgentType) QueueName() string {
	return "agent-" + string(a)
}

// TaskType represents predefined task types
type TaskType string

const (
	TaskTypeCompetitorAnalysis TaskType = "competitor_analysis"
	TaskTypeMarketResearch     TaskType = "market_research"
	TaskTypeCodeReview         TaskType = "code_review"
	TaskTypeDataAnalysis       TaskType = "data_analysis"
	TaskTypeWebSearch          TaskType = "web_search"
        TaskTypeLLMCreative    TaskType = "llm_creative"
        TaskTypeLLMResearch    TaskType = "llm_research"
        TaskTypeLLMCoding      TaskType = "llm_coding"
)
