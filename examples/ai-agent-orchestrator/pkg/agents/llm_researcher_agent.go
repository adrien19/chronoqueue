package agents

import (
	"context"
	"fmt"
	"time"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/examples/ai-agent-orchestrator/pkg/llm"
)

// LLMResearcherAgent performs research tasks using LLM
type LLMResearcherAgent struct {
	*BaseAgent
	llmClient llm.LLMClient
}

// NewLLMResearcherAgent creates a new LLM-powered researcher agent
func NewLLMResearcherAgent(c *client.ChronoQueueClient, llmClient llm.LLMClient, workers int, verbose bool) *LLMResearcherAgent {
	return &LLMResearcherAgent{
		BaseAgent: NewBaseAgent(c, "agent-llm-researcher", "llm-researcher", workers, verbose),
		llmClient: llmClient,
	}
}

// Start begins processing research tasks
func (agent *LLMResearcherAgent) Start(ctx context.Context) error {
	return agent.BaseAgent.Start(ctx, agent.processResearch)
}

// processResearch uses LLM to perform research and analysis
func (agent *LLMResearcherAgent) processResearch(ctx context.Context, subtask *Subtask) (*AgentResult, error) {
	if agent.verbose {
		fmt.Printf("   🔬 Researching with LLM: %s\n", subtask.Description)
	}

	// Extract research parameters
	topic, _ := subtask.Input["topic"].(string)
	if topic == "" {
		topic = subtask.Description
	}

	depth, _ := subtask.Input["depth"].(string)
	if depth == "" {
		depth = "comprehensive"
	}

	// Construct the LLM prompt
	systemPrompt := fmt.Sprintf("You are an expert researcher. Provide %s research on the given topic. Include key facts, trends, and insights.", depth)
	userPrompt := fmt.Sprintf("Research topic: %s\n\nProvide a detailed analysis with sources, statistics, and actionable insights.", topic)

	// Call LLM
	startTime := time.Now()
	response, err := agent.llmClient.Generate(ctx, systemPrompt, userPrompt)
	if err != nil {
		return nil, fmt.Errorf("LLM call failed: %w", err)
	}
	duration := time.Since(startTime)

	result := map[string]interface{}{
		"topic":        topic,
		"depth":        depth,
		"findings":     response,
		"word_count":   len(response) / 5,
		"generated_at": time.Now().Unix(),
		"duration_ms":  duration.Milliseconds(),
		"llm_powered":  true,
	}

	if agent.verbose {
		fmt.Printf("   ✓ Researched %d words in %v\n", result["word_count"], duration)
	}

	return &AgentResult{
		SubtaskID:   subtask.SubtaskID,
		ParentID:    subtask.ParentID,
		AgentType:   "llm-researcher",
		Status:      "completed",
		Result:      result,
		CompletedAt: time.Now(),
	}, nil
}
