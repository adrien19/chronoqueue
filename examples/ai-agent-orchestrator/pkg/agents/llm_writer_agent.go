package agents

import (
	"context"
	"fmt"
	"time"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/examples/ai-agent-orchestrator/pkg/llm"
)

// LLMWriterAgent processes creative writing tasks using LLM
type LLMWriterAgent struct {
	*BaseAgent
	llmClient llm.LLMClient
}

// NewLLMWriterAgent creates a new LLM-powered writer agent
func NewLLMWriterAgent(c *client.ChronoQueueClient, llmClient llm.LLMClient, workers int, verbose bool) *LLMWriterAgent {
	return &LLMWriterAgent{
		BaseAgent: NewBaseAgent(c, "agent-llm-writer", "llm-writer", workers, verbose),
		llmClient: llmClient,
	}
}

// Start begins processing writing tasks
func (agent *LLMWriterAgent) Start(ctx context.Context) error {
	return agent.BaseAgent.Start(ctx, agent.processWriting)
}

// processWriting uses LLM to generate creative content
func (agent *LLMWriterAgent) processWriting(ctx context.Context, subtask *Subtask) (*AgentResult, error) {
	if agent.verbose {
		fmt.Printf("   ✍️  Writing with LLM: %s\n", subtask.Description)
	}

	// Extract writing parameters from input
	prompt, _ := subtask.Input["prompt"].(string)
	if prompt == "" {
		prompt = subtask.Description
	}

	tone, _ := subtask.Input["tone"].(string)
	if tone == "" {
		tone = "creative and engaging"
	}

	length, _ := subtask.Input["length"].(string)
	if length == "" {
		length = "medium"
	}

	// Construct the LLM prompt
	systemPrompt := fmt.Sprintf("You are a professional creative writer. Your tone should be %s. Generate content that is %s in length.", tone, length)
	userPrompt := fmt.Sprintf("Write about: %s\n\nProvide the content in a clear, well-structured format.", prompt)

	// Call LLM
	startTime := time.Now()
	response, err := agent.callLLM(ctx, systemPrompt, userPrompt)
	if err != nil {
		return nil, fmt.Errorf("LLM call failed: %w", err)
	}
	duration := time.Since(startTime)

	result := map[string]interface{}{
		"prompt":       prompt,
		"content":      response,
		"tone":         tone,
		"length":       length,
		"word_count":   len(response) / 5, // Rough estimate
		"generated_at": time.Now().Unix(),
		"duration_ms":  duration.Milliseconds(),
		"llm_powered":  true,
	}

	if agent.verbose {
		fmt.Printf("   ✓ Generated %d words in %v\n", result["word_count"], duration)
	}

	return &AgentResult{
		SubtaskID:   subtask.SubtaskID,
		ParentID:    subtask.ParentID,
		AgentType:   "llm-writer",
		Status:      "completed",
		Result:      result,
		CompletedAt: time.Now(),
	}, nil
}

// callLLM makes a simple LLM call for content generation
func (agent *LLMWriterAgent) callLLM(ctx context.Context, systemPrompt, userPrompt string) (string, error) {
	return agent.llmClient.Generate(ctx, systemPrompt, userPrompt)
}
