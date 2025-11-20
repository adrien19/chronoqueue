package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/adrien19/chronoqueue/examples/ai-agent-orchestrator/pkg/llm/prompts"
	"github.com/adrien19/chronoqueue/examples/ai-agent-orchestrator/pkg/models"
	openai "github.com/sashabaranov/go-openai"
)

// OllamaClient implements LLMClient using Ollama via OpenAI-compatible API
type OllamaClient struct {
	client      *openai.Client
	config      *OllamaConfig
	verbose     bool
	retryConfig RetryConfig
}

// RetryConfig holds retry configuration
type RetryConfig struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// NewOllamaClient creates a new Ollama client
func NewOllamaClient(config *OllamaConfig, verbose bool) (*OllamaClient, error) {
	if config == nil {
		return nil, fmt.Errorf("ollama config is required")
	}

	// Configure OpenAI client to use Ollama endpoint
	clientConfig := openai.DefaultConfig("")
	clientConfig.BaseURL = config.BaseURL + "/v1"

	client := openai.NewClientWithConfig(clientConfig)

	return &OllamaClient{
		client:  client,
		config:  config,
		verbose: verbose,
		retryConfig: RetryConfig{
			MaxRetries:     config.MaxRetries,
			InitialBackoff: 500 * time.Millisecond,
			MaxBackoff:     10 * time.Second,
		},
	}, nil
}

// DecomposeTask breaks down a complex task into smaller subtasks using Ollama
func (c *OllamaClient) DecomposeTask(task *models.Task) (*models.TaskDecomposition, error) {
	if c.verbose {
		fmt.Printf("[OllamaClient] Decomposing task %s (type: %s)\n", task.TaskID, task.TaskType)
	}

	// Handle simple LLM tasks directly without complex decomposition
	taskType := models.TaskType(task.TaskType)
	switch taskType {
	case models.TaskTypeLLMCreative, models.TaskTypeLLMResearch, models.TaskTypeLLMCoding:
		return c.decomposeSimpleLLMTask(task)
	}

	// Get model for coordinator
	model := c.getModelForAgent("coordinator")
	if c.verbose {
		fmt.Printf("[OllamaClient] Using model: %s\n", model)
	}

	// Format input as JSON string
	inputJSON, err := json.Marshal(task.Input)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal task input: %w", err)
	}

	// Create user prompt from template
	userPrompt := fmt.Sprintf(
		prompts.DecomposeUserPromptTemplate,
		task.TaskID,
		task.TaskType,
		task.Description,
		string(inputJSON),
		task.Priority,
	)

	// Call LLM with retry logic
	var response string
	err = c.retryWithBackoff(func() error {
		var callErr error
		response, callErr = c.callLLM(model, prompts.DecomposeSystemPrompt, userPrompt)
		return callErr
	})

	if err != nil {
		return nil, fmt.Errorf("failed to call LLM: %w", err)
	}

	// Parse LLM response
	decomposition, err := c.parseDecompositionResponse(task.TaskID, response)
	if err != nil {
		return nil, fmt.Errorf("failed to parse decomposition response: %w", err)
	}

	if c.verbose {
		fmt.Printf("[OllamaClient] Generated %d subtasks\n", len(decomposition.Subtasks))
	}

	return decomposition, nil
}

// SynthesizeResults combines multiple agent results into a comprehensive report
func (c *OllamaClient) SynthesizeResults(taskID string, results []*models.AgentResult) (*models.Report, error) {
	if c.verbose {
		fmt.Printf("[OllamaClient] Synthesizing %d results for task %s\n", len(results), taskID)
	}

	// Get model for aggregator
	model := c.getModelForAgent("aggregator")
	if c.verbose {
		fmt.Printf("[OllamaClient] Using model: %s\n", model)
	}

	// Format results for prompt
	resultsText := c.formatResultsForPrompt(results)

	// Get original task description (from first result's context if available)
	taskDescription := taskID
	if len(results) > 0 && taskID != "" {
		taskDescription = taskID
	}

	// Create user prompt from template
	userPrompt := fmt.Sprintf(
		prompts.SynthesizeUserPromptTemplate,
		taskID,
		taskDescription,
		resultsText,
	)

	// Call LLM with retry logic
	var response string
	err := c.retryWithBackoff(func() error {
		var callErr error
		response, callErr = c.callLLM(model, prompts.SynthesizeSystemPrompt, userPrompt)
		return callErr
	})

	if err != nil {
		return nil, fmt.Errorf("failed to call LLM: %w", err)
	}

	// Parse LLM response
	report, err := c.parseSynthesisResponse(taskID, response)
	if err != nil {
		return nil, fmt.Errorf("failed to parse synthesis response: %w", err)
	}

	if c.verbose {
		fmt.Printf("[OllamaClient] Generated report with %d sections\n", len(report.Sections))
	}

	return report, nil
}

// callLLM makes a chat completion request to Ollama
func (c *OllamaClient) callLLM(model, systemPrompt, userPrompt string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.config.Timeout)
	defer cancel()

	req := openai.ChatCompletionRequest{
		Model: model,
		Messages: []openai.ChatCompletionMessage{
			{
				Role:    openai.ChatMessageRoleSystem,
				Content: systemPrompt,
			},
			{
				Role:    openai.ChatMessageRoleUser,
				Content: userPrompt,
			},
		},
		Temperature: c.config.Temperature,
	}

	if c.verbose {
		fmt.Printf("[OllamaClient] Sending request to model %s...\n", model)
	}

	resp, err := c.client.CreateChatCompletion(ctx, req)
	if err != nil {
		return "", fmt.Errorf("chat completion failed: %w", err)
	}

	if len(resp.Choices) == 0 {
		return "", fmt.Errorf("no response choices returned")
	}

	content := resp.Choices[0].Message.Content

	if c.verbose {
		fmt.Printf("[OllamaClient] Received response (%d tokens)\n", resp.Usage.TotalTokens)
	}

	return content, nil
}

// parseDecompositionResponse parses the LLM response into a TaskDecomposition
func (c *OllamaClient) parseDecompositionResponse(taskID, response string) (*models.TaskDecomposition, error) {
	// Clean response - remove markdown code blocks if present
	response = c.cleanJSONResponse(response)

	// Parse the response
	var llmResponse struct {
		Subtasks []struct {
			SubtaskID   string                 `json:"subtask_id"`
			AgentType   string                 `json:"agent_type"`
			Description string                 `json:"description"`
			Input       map[string]interface{} `json:"input"`
			DependsOn   []string               `json:"depends_on,omitempty"`
			Priority    int32                  `json:"priority"`
		} `json:"subtasks"`
	}

	if err := json.Unmarshal([]byte(response), &llmResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w\nResponse: %s", err, response)
	}

	// Convert to models.TaskDecomposition
	decomposition := &models.TaskDecomposition{
		TaskID:   taskID,
		Subtasks: make([]models.Subtask, 0, len(llmResponse.Subtasks)),
	}

	now := time.Now()
	for _, st := range llmResponse.Subtasks {
		subtask := models.Subtask{
			SubtaskID:   st.SubtaskID,
			ParentID:    taskID,
			AgentType:   st.AgentType,
			Description: st.Description,
			Input:       st.Input,
			DependsOn:   st.DependsOn,
			Priority:    st.Priority,
			Status:      models.TaskStatusPending,
			CreatedAt:   now,
		}
		decomposition.Subtasks = append(decomposition.Subtasks, subtask)
	}

	return decomposition, nil
}

// parseSynthesisResponse parses the LLM response into a Report
func (c *OllamaClient) parseSynthesisResponse(taskID, response string) (*models.Report, error) {
	// Clean response - remove markdown code blocks if present
	response = c.cleanJSONResponse(response)

	// Parse the response
	var llmResponse struct {
		Summary         string   `json:"summary"`
		KeyFindings     []string `json:"key_findings,omitempty"`
		Recommendations []string `json:"recommendations,omitempty"`
		ConfidenceScore float64  `json:"confidence_score,omitempty"`
		Sections        []struct {
			Title   string                 `json:"title"`
			Content string                 `json:"content"`
			Source  string                 `json:"source,omitempty"`
			Data    map[string]interface{} `json:"data,omitempty"`
		} `json:"sections"`
	}

	if err := json.Unmarshal([]byte(response), &llmResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w\nResponse: %s", err, response)
	}

	// Convert to models.Report
	report := &models.Report{
		TaskID:      taskID,
		Summary:     llmResponse.Summary,
		Sections:    make([]models.ReportSection, 0, len(llmResponse.Sections)),
		GeneratedAt: time.Now(),
	}

	for _, section := range llmResponse.Sections {
		reportSection := models.ReportSection{
			Title:   section.Title,
			Content: section.Content,
			Source:  section.Source,
			Data:    section.Data,
		}
		report.Sections = append(report.Sections, reportSection)
	}

	return report, nil
}

// cleanJSONResponse removes markdown code blocks and extra whitespace
func (c *OllamaClient) cleanJSONResponse(response string) string {
	// Remove markdown code blocks
	response = strings.TrimPrefix(response, "```json")
	response = strings.TrimPrefix(response, "```")
	response = strings.TrimSuffix(response, "```")

	// Trim whitespace
	response = strings.TrimSpace(response)

	return response
}

// formatResultsForPrompt formats agent results into a text format for the prompt
func (c *OllamaClient) formatResultsForPrompt(results []*models.AgentResult) string {
	var builder strings.Builder

	for i, result := range results {
		builder.WriteString(fmt.Sprintf("\n--- Result %d ---\n", i+1))
		builder.WriteString(fmt.Sprintf("Agent: %s\n", result.AgentType))
		builder.WriteString(fmt.Sprintf("Subtask ID: %s\n", result.SubtaskID))
		builder.WriteString(fmt.Sprintf("Status: %v\n", result.Success))
		builder.WriteString(fmt.Sprintf("Duration: %v\n", result.Duration))

		if result.Success {
			dataJSON, _ := json.MarshalIndent(result.Data, "", "  ")
			builder.WriteString(fmt.Sprintf("Data:\n%s\n", string(dataJSON)))
		} else {
			builder.WriteString(fmt.Sprintf("Error: %s\n", result.Error))
		}
	}

	return builder.String()
}

// getModelForAgent returns the appropriate model for a given agent type
func (c *OllamaClient) getModelForAgent(agentType string) string {
	if model, ok := c.config.Models[agentType]; ok {
		return model
	}

	// Default to coordinator model if specific agent model not found
	if model, ok := c.config.Models["coordinator"]; ok {
		return model
	}

	// Fallback to a reasonable default
	return "llama3.2:3b"
}

// retryWithBackoff executes a function with exponential backoff retry logic
func (c *OllamaClient) retryWithBackoff(fn func() error) error {
	var lastErr error
	backoff := c.retryConfig.InitialBackoff

	for attempt := 0; attempt <= c.retryConfig.MaxRetries; attempt++ {
		if attempt > 0 {
			if c.verbose {
				fmt.Printf("[OllamaClient] Retry attempt %d/%d after %v\n",
					attempt, c.retryConfig.MaxRetries, backoff)
			}
			time.Sleep(backoff)

			// Exponential backoff with jitter
			backoff = backoff * 2
			if backoff > c.retryConfig.MaxBackoff {
				backoff = c.retryConfig.MaxBackoff
			}
		}

		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if error is retryable
		if !c.isRetryableError(err) {
			return lastErr
		}
	}

	return fmt.Errorf("max retries exceeded: %w", lastErr)
}

// isRetryableError determines if an error should trigger a retry
func (c *OllamaClient) isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()

	// Retry on network errors, timeouts, and rate limits
	retryablePatterns := []string{
		"connection refused",
		"timeout",
		"rate limit",
		"temporary failure",
		"service unavailable",
		"too many requests",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(strings.ToLower(errMsg), pattern) {
			return true
		}
	}

	return false
}

// Generate produces a response based on system and user prompts
func (c *OllamaClient) Generate(ctx context.Context, systemPrompt, userPrompt string) (string, error) {
	if c.verbose {
		fmt.Printf("[OllamaClient] Generating response...\n")
	}

	// Use default model for general generation
	model := c.config.Models["default"]
	if model == "" {
		model = c.config.Models["coordinator"] // Fallback to coordinator model
	}

	if c.verbose {
		fmt.Printf("[OllamaClient] Using model: %s\n", model)
	}

	// Call LLM with retry logic
	var response string
	err := c.retryWithBackoff(func() error {
		var callErr error
		response, callErr = c.callLLM(model, systemPrompt, userPrompt)
		return callErr
	})

	if err != nil {
		return "", fmt.Errorf("failed to call LLM: %w", err)
	}

	if c.verbose {
		fmt.Printf("[OllamaClient] Generated %d characters\n", len(response))
	}

	return response, nil
}

// decomposeSimpleLLMTask handles direct routing for LLM-powered tasks
func (c *OllamaClient) decomposeSimpleLLMTask(task *models.Task) (*models.TaskDecomposition, error) {
	now := time.Now()

	// Map task type to agent type
	var agentType string
	switch models.TaskType(task.TaskType) {
	case models.TaskTypeLLMCreative:
		agentType = "llm-writer"
	case models.TaskTypeLLMResearch:
		agentType = "llm-researcher"
	case models.TaskTypeLLMCoding:
		agentType = "llm-coder"
	default:
		return nil, fmt.Errorf("unsupported LLM task type: %s", task.TaskType)
	}

	if c.verbose {
		fmt.Printf("[OllamaClient] Routing LLM task directly to %s\n", agentType)
	}

	subtasks := []models.Subtask{
		{
			SubtaskID:   fmt.Sprintf("%s-001", task.TaskID),
			ParentID:    task.TaskID,
			AgentType:   agentType,
			Description: task.Description,
			Input:       task.Input,
			Priority:    task.Priority,
			Status:      models.TaskStatusPending,
			CreatedAt:   now,
		},
	}

	return &models.TaskDecomposition{
		TaskID:   task.TaskID,
		Subtasks: subtasks,
	}, nil
}
