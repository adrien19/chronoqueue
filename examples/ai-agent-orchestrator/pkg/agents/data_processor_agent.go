package agents

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/adrien19/chronoqueue/client"
)

// DataProcessorAgent processes data analysis subtasks
type DataProcessorAgent struct {
	*BaseAgent
}

// NewDataProcessorAgent creates a new data processor agent
func NewDataProcessorAgent(c *client.ChronoQueueClient, workers int, verbose bool) *DataProcessorAgent {
	return &DataProcessorAgent{
		BaseAgent: NewBaseAgent(c, "agent-data-processor", "data-processor", workers, verbose),
	}
}

// Start begins processing data analysis subtasks
func (agent *DataProcessorAgent) Start(ctx context.Context) error {
	return agent.BaseAgent.Start(ctx, agent.processData)
}

// processData performs the actual data processing (mocked for demo)
func (agent *DataProcessorAgent) processData(ctx context.Context, subtask *Subtask) (*AgentResult, error) {
	if agent.verbose {
		fmt.Printf("   📈 Processing: %s\n", subtask.Description)
	}

	// Simulate processing delay
	time.Sleep(600 * time.Millisecond)

	// Extract parameters from input
	dataSource, _ := subtask.Input["data_source"].(string)
	analysisType, _ := subtask.Input["analysis_type"].(string)

	if dataSource == "" {
		dataSource = "unknown"
	}
	if analysisType == "" {
		analysisType = "general"
	}

	// Mock processing results
	results := map[string]interface{}{
		"data_source":       dataSource,
		"analysis_type":     analysisType,
		"statistics":        generateMockStatistics(),
		"insights":          generateMockInsights(analysisType),
		"records_processed": 15780,
	}

	return &AgentResult{
		SubtaskID:   subtask.SubtaskID,
		ParentID:    subtask.ParentID,
		AgentType:   "data-processor",
		Status:      "completed",
		Result:      results,
		CompletedAt: time.Now(),
	}, nil
}

// generateMockStatistics creates mock statistical data
func generateMockStatistics() map[string]interface{} {
	return map[string]interface{}{
		"mean":        45.6,
		"median":      42.3,
		"std_dev":     12.8,
		"min":         18.2,
		"max":         89.4,
		"quartiles":   []float64{32.5, 42.3, 58.7},
		"correlation": 0.78,
	}
}

// generateMockInsights creates mock data insights
func generateMockInsights(analysisType string) []map[string]interface{} {
	// Note: As of Go 1.20, rand is automatically seeded

	return []map[string]interface{}{
		{
			"type":        "trend",
			"description": fmt.Sprintf("Detected %d%% increase in activity over last quarter", 15+rand.Intn(20)),
			"confidence":  0.87,
		},
		{
			"type":        "anomaly",
			"description": "Unusual spike detected on weekends",
			"confidence":  0.72,
		},
		{
			"type":        "pattern",
			"description": "Strong correlation between user engagement and time of day",
			"confidence":  0.91,
		},
		{
			"type":        "prediction",
			"description": "Expected growth of 25-30% in next period based on current trajectory",
			"confidence":  0.68,
		},
	}
}
