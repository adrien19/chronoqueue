package llm

import (
	"context"
	"fmt"
	"time"

	"github.com/adrien19/chronoqueue/examples/ai-agent-orchestrator/pkg/models"
)

// MockLLMClient implements LLMClient with predefined responses for demo purposes
type MockLLMClient struct {
	verbose bool
}

// NewMockLLMClient creates a new mock LLM client
func NewMockLLMClient(verbose bool) *MockLLMClient {
	return &MockLLMClient{
		verbose: verbose,
	}
}

// DecomposeTask decomposes a task into subtasks based on predefined templates
func (m *MockLLMClient) DecomposeTask(task *models.Task) (*models.TaskDecomposition, error) {
	if m.verbose {
		fmt.Printf("[MockLLM] Decomposing task %s (type: %s)\n", task.TaskID, task.TaskType)
	}

	// Simulate LLM processing time
	time.Sleep(100 * time.Millisecond)

	taskType := models.TaskType(task.TaskType)
	switch taskType {
	case models.TaskTypeCompetitorAnalysis:
		return m.decomposeCompetitorAnalysis(task), nil
	case models.TaskTypeMarketResearch:
		return m.decomposeMarketResearch(task), nil
	case models.TaskTypeCodeReview:
		return m.decomposeCodeReview(task), nil
	case models.TaskTypeWebSearch:
		return m.decomposeWebSearch(task), nil
	case models.TaskTypeDataAnalysis:
		return m.decomposeDataAnalysis(task), nil
	case models.TaskTypeLLMCreative:
		return m.decomposeLLMCreative(task), nil
	case models.TaskTypeLLMResearch:
		return m.decomposeLLMResearch(task), nil
	case models.TaskTypeLLMCoding:
		return m.decomposeLLMCoding(task), nil
	default:
		return nil, fmt.Errorf("unknown task type: %s", task.TaskType)
	}
}

// SynthesizeResults synthesizes agent results into a final report
func (m *MockLLMClient) SynthesizeResults(taskID string, results []*models.AgentResult) (*models.Report, error) {
	if m.verbose {
		fmt.Printf("[MockLLM] Synthesizing %d results for task %s\n", len(results), taskID)
	}

	// Simulate LLM processing time
	time.Sleep(200 * time.Millisecond)

	report := &models.Report{
		TaskID:      taskID,
		Summary:     fmt.Sprintf("Comprehensive analysis based on %d agent results", len(results)),
		Sections:    make([]models.ReportSection, 0),
		GeneratedAt: time.Now(),
	}

	// Create sections from results
	for _, result := range results {
		section := models.ReportSection{
			Title:   fmt.Sprintf("%s Results", result.AgentType),
			Content: fmt.Sprintf("Agent %s completed in %v", result.AgentType, result.Duration),
			Source:  result.AgentType,
		}

		if result.Success {
			section.Content += "\n\nKey Findings:\n"
			for key, value := range result.Data {
				section.Content += fmt.Sprintf("- %s: %v\n", key, value)
			}
			section.Data = result.Data
		} else {
			section.Content += fmt.Sprintf("\n\nError: %s", result.Error)
		}

		report.Sections = append(report.Sections, section)
	}

	return report, nil
}

// decomposeCompetitorAnalysis creates subtasks for competitor analysis
func (m *MockLLMClient) decomposeCompetitorAnalysis(task *models.Task) *models.TaskDecomposition {
	companyURL, _ := task.Input["company_url"].(string)
	includeCode, _ := task.Input["include_code_analysis"].(bool)
	includeTraffic, _ := task.Input["include_traffic_analysis"].(bool)
	now := time.Now()

	subtasks := []models.Subtask{
		{
			SubtaskID:   fmt.Sprintf("%s-web-1", task.TaskID),
			ParentID:    task.TaskID,
			AgentType:   string(models.AgentTypeWebSearch),
			Description: fmt.Sprintf("Gather competitor information for %s", companyURL),
			Input: map[string]interface{}{
				"url":     companyURL,
				"queries": []string{"company overview", "products", "pricing"},
			},
			Priority:  task.Priority,
			Status:    models.TaskStatusPending,
			CreatedAt: now,
		},
	}

	if includeCode {
		subtasks = append(subtasks, models.Subtask{
			SubtaskID:   fmt.Sprintf("%s-code-1", task.TaskID),
			ParentID:    task.TaskID,
			AgentType:   string(models.AgentTypeCodeAnalyzer),
			Description: fmt.Sprintf("Analyze code repositories for %s", companyURL),
			Input: map[string]interface{}{
				"repository_url": companyURL,
				"focus_areas":    []string{"architecture", "technologies"},
			},
			Priority:  task.Priority,
			Status:    models.TaskStatusPending,
			CreatedAt: now,
		})
	}

	if includeTraffic {
		subtasks = append(subtasks, models.Subtask{
			SubtaskID:   fmt.Sprintf("%s-data-1", task.TaskID),
			ParentID:    task.TaskID,
			AgentType:   string(models.AgentTypeDataProcessor),
			Description: fmt.Sprintf("Analyze traffic and SEO data for %s", companyURL),
			Input: map[string]interface{}{
				"url":     companyURL,
				"metrics": []string{"traffic", "keywords", "backlinks"},
			},
			Priority:  task.Priority,
			Status:    models.TaskStatusPending,
			CreatedAt: now,
		})
	}

	// Aggregator depends on all previous subtasks
	dependencies := make([]string, len(subtasks))
	for i, st := range subtasks {
		dependencies[i] = st.SubtaskID
	}

	subtasks = append(subtasks, models.Subtask{
		SubtaskID:   fmt.Sprintf("%s-agg-1", task.TaskID),
		ParentID:    task.TaskID,
		AgentType:   string(models.AgentTypeAggregator),
		Description: "Synthesize competitor analysis findings",
		Input:       map[string]interface{}{"report_type": "competitor_analysis"},
		DependsOn:   dependencies,
		Priority:    task.Priority,
		Status:      models.TaskStatusPending,
		CreatedAt:   now,
	})

	return &models.TaskDecomposition{
		TaskID:   task.TaskID,
		Subtasks: subtasks,
	}
}

// decomposeMarketResearch creates subtasks for market research
func (m *MockLLMClient) decomposeMarketResearch(task *models.Task) *models.TaskDecomposition {
	segment, _ := task.Input["market_segment"].(string)
	geo, _ := task.Input["geographic_focus"].(string)
	now := time.Now()

	subtasks := []models.Subtask{
		{
			SubtaskID:   fmt.Sprintf("%s-web-1", task.TaskID),
			ParentID:    task.TaskID,
			AgentType:   string(models.AgentTypeWebSearch),
			Description: fmt.Sprintf("Research market size and trends for %s in %s", segment, geo),
			Input: map[string]interface{}{
				"segment":   segment,
				"geography": geo,
				"queries":   []string{"market size", "growth trends", "forecasts"},
			},
			Priority:  task.Priority,
			Status:    models.TaskStatusPending,
			CreatedAt: now,
		},
		{
			SubtaskID:   fmt.Sprintf("%s-web-2", task.TaskID),
			ParentID:    task.TaskID,
			AgentType:   string(models.AgentTypeWebSearch),
			Description: "Identify key players and competitors",
			Input: map[string]interface{}{
				"segment": segment,
				"queries": []string{"top companies", "market leaders", "emerging players"},
			},
			Priority:  task.Priority,
			Status:    models.TaskStatusPending,
			CreatedAt: now,
		},
		{
			SubtaskID:   fmt.Sprintf("%s-data-1", task.TaskID),
			ParentID:    task.TaskID,
			AgentType:   string(models.AgentTypeDataProcessor),
			Description: "Analyze market data and statistics",
			Input: map[string]interface{}{
				"segment": segment,
				"metrics": []string{"market share", "revenue", "growth rate"},
			},
			DependsOn: []string{fmt.Sprintf("%s-web-1", task.TaskID)},
			Priority:  task.Priority,
			Status:    models.TaskStatusPending,
			CreatedAt: now,
		},
		{
			SubtaskID:   fmt.Sprintf("%s-agg-1", task.TaskID),
			ParentID:    task.TaskID,
			AgentType:   string(models.AgentTypeAggregator),
			Description: "Create comprehensive market research report",
			Input:       map[string]interface{}{"report_type": "market_research"},
			DependsOn: []string{
				fmt.Sprintf("%s-web-1", task.TaskID),
				fmt.Sprintf("%s-web-2", task.TaskID),
				fmt.Sprintf("%s-data-1", task.TaskID),
			},
			Priority:  task.Priority,
			Status:    models.TaskStatusPending,
			CreatedAt: now,
		},
	}

	return &models.TaskDecomposition{
		TaskID:   task.TaskID,
		Subtasks: subtasks,
	}
}

// decomposeCodeReview creates subtasks for code review
func (m *MockLLMClient) decomposeCodeReview(task *models.Task) *models.TaskDecomposition {
	repoURL, _ := task.Input["repository_url"].(string)
	focusAreas, _ := task.Input["focus_areas"].([]interface{})
	now := time.Now()

	subtasks := []models.Subtask{
		{
			SubtaskID:   fmt.Sprintf("%s-code-1", task.TaskID),
			ParentID:    task.TaskID,
			AgentType:   string(models.AgentTypeCodeAnalyzer),
			Description: fmt.Sprintf("Analyze repository structure for %s", repoURL),
			Input: map[string]interface{}{
				"repository_url": repoURL,
				"analysis_type":  "structure",
			},
			Priority:  task.Priority,
			Status:    models.TaskStatusPending,
			CreatedAt: now,
		},
		{
			SubtaskID:   fmt.Sprintf("%s-code-2", task.TaskID),
			ParentID:    task.TaskID,
			AgentType:   string(models.AgentTypeCodeAnalyzer),
			Description: "Perform code quality review",
			Input: map[string]interface{}{
				"repository_url": repoURL,
				"focus_areas":    focusAreas,
				"analysis_type":  "quality",
			},
			Priority:  task.Priority,
			Status:    models.TaskStatusPending,
			CreatedAt: now,
		},
		{
			SubtaskID:   fmt.Sprintf("%s-agg-1", task.TaskID),
			ParentID:    task.TaskID,
			AgentType:   string(models.AgentTypeAggregator),
			Description: "Create code review report",
			Input:       map[string]interface{}{"report_type": "code_review"},
			DependsOn: []string{
				fmt.Sprintf("%s-code-1", task.TaskID),
				fmt.Sprintf("%s-code-2", task.TaskID),
			},
			Priority:  task.Priority,
			Status:    models.TaskStatusPending,
			CreatedAt: now,
		},
	}

	return &models.TaskDecomposition{
		TaskID:   task.TaskID,
		Subtasks: subtasks,
	}
}

// decomposeWebSearch creates subtasks for simple web search
func (m *MockLLMClient) decomposeWebSearch(task *models.Task) *models.TaskDecomposition {
	query, _ := task.Input["query"].(string)
	now := time.Now()

	subtasks := []models.Subtask{
		{
			SubtaskID:   fmt.Sprintf("%s-web-1", task.TaskID),
			ParentID:    task.TaskID,
			AgentType:   string(models.AgentTypeWebSearch),
			Description: fmt.Sprintf("Search for: %s", query),
			Input: map[string]interface{}{
				"query": query,
			},
			Priority:  task.Priority,
			Status:    models.TaskStatusPending,
			CreatedAt: now,
		},
	}

	return &models.TaskDecomposition{
		TaskID:   task.TaskID,
		Subtasks: subtasks,
	}
}

// decomposeDataAnalysis creates subtasks for data analysis
func (m *MockLLMClient) decomposeDataAnalysis(task *models.Task) *models.TaskDecomposition {
	dataSource, _ := task.Input["data_source"].(string)
	now := time.Now()

	subtasks := []models.Subtask{
		{
			SubtaskID:   fmt.Sprintf("%s-data-1", task.TaskID),
			ParentID:    task.TaskID,
			AgentType:   string(models.AgentTypeDataProcessor),
			Description: fmt.Sprintf("Analyze data from: %s", dataSource),
			Input: map[string]interface{}{
				"data_source": dataSource,
			},
			Priority:  task.Priority,
			Status:    models.TaskStatusPending,
			CreatedAt: now,
		},
	}

	return &models.TaskDecomposition{
		TaskID:   task.TaskID,
		Subtasks: subtasks,
	}
}

// Generate produces a mock response for general LLM queries
func (m *MockLLMClient) Generate(ctx context.Context, systemPrompt, userPrompt string) (string, error) {
	if m.verbose {
		fmt.Printf("[MockLLM] Generating response (system: %.50s..., user: %.50s...)\n", systemPrompt, userPrompt)
	}

	// Simulate LLM processing time
	time.Sleep(150 * time.Millisecond)

	// Return a generic mock response
	return fmt.Sprintf("Mock LLM Response:\n\nSystem Context: %s\n\nUser Request: %s\n\nThis is a simulated response. To see real LLM outputs, use --llm-provider ollama",
		systemPrompt, userPrompt), nil
}

// Generate produces a mock response for general LLM queries

// decomposeLLMCreative creates subtasks for LLM creative writing
func (m *MockLLMClient) decomposeLLMCreative(task *models.Task) *models.TaskDecomposition {
	now := time.Now()

	subtasks := []models.Subtask{
		{
			SubtaskID:   fmt.Sprintf("%s-001", task.TaskID),
			ParentID:    task.TaskID,
			AgentType:   "llm-writer",
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
	}
}

// decomposeLLMResearch creates subtasks for LLM research
func (m *MockLLMClient) decomposeLLMResearch(task *models.Task) *models.TaskDecomposition {
	now := time.Now()

	subtasks := []models.Subtask{
		{
			SubtaskID:   fmt.Sprintf("%s-001", task.TaskID),
			ParentID:    task.TaskID,
			AgentType:   "llm-researcher",
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
	}
}

// decomposeLLMCoding creates subtasks for LLM coding
func (m *MockLLMClient) decomposeLLMCoding(task *models.Task) *models.TaskDecomposition {
	now := time.Now()

	subtasks := []models.Subtask{
		{
			SubtaskID:   fmt.Sprintf("%s-001", task.TaskID),
			ParentID:    task.TaskID,
			AgentType:   "llm-coder",
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
	}
}
