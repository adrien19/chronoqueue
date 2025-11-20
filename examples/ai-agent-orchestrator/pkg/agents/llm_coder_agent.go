package agents

import (
	"context"
	"fmt"
	"time"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/examples/ai-agent-orchestrator/pkg/llm"
)

// LLMCoderAgent generates code using LLM
type LLMCoderAgent struct {
	*BaseAgent
	llmClient llm.LLMClient
}

// NewLLMCoderAgent creates a new LLM-powered coder agent
func NewLLMCoderAgent(c *client.ChronoQueueClient, llmClient llm.LLMClient, workers int, verbose bool) *LLMCoderAgent {
	return &LLMCoderAgent{
		BaseAgent: NewBaseAgent(c, "agent-llm-coder", "llm-coder", workers, verbose),
		llmClient: llmClient,
	}
}

// Start begins processing coding tasks
func (agent *LLMCoderAgent) Start(ctx context.Context) error {
	return agent.BaseAgent.Start(ctx, agent.processCoding)
}

// processCoding uses LLM to generate or analyze code
func (agent *LLMCoderAgent) processCoding(ctx context.Context, subtask *Subtask) (*AgentResult, error) {
	if agent.verbose {
		fmt.Printf("   💻 Coding with LLM: %s\n", subtask.Description)
	}

	// Extract coding parameters
	task, _ := subtask.Input["task"].(string)
	if task == "" {
		task = subtask.Description
	}

	language, _ := subtask.Input["language"].(string)
	if language == "" {
		language = "Go"
	}

	// Construct the LLM prompt
	systemPrompt := fmt.Sprintf("You are an expert %s programmer. Write clean, efficient, well-documented code following best practices.", language)
	userPrompt := fmt.Sprintf("Programming task: %s\n\nProvide complete, production-ready code with comments explaining the implementation.", task)

	// Call LLM
	startTime := time.Now()
	response, err := agent.llmClient.Generate(ctx, systemPrompt, userPrompt)
	if err != nil {
		return nil, fmt.Errorf("LLM call failed: %w", err)
	}
	duration := time.Since(startTime)

	result := map[string]interface{}{
		"task":         task,
		"language":     language,
		"code":         response,
		"lines":        len(response) / 40, // Rough estimate
		"generated_at": time.Now().Unix(),
		"duration_ms":  duration.Milliseconds(),
		"llm_powered":  true,
	}

	if agent.verbose {
		fmt.Printf("   ✓ Generated ~%d lines in %v\n", result["lines"], duration)
	}

	return &AgentResult{
		SubtaskID:   subtask.SubtaskID,
		ParentID:    subtask.ParentID,
		AgentType:   "llm-coder",
		Status:      "completed",
		Result:      result,
		CompletedAt: time.Now(),
	}, nil
}
