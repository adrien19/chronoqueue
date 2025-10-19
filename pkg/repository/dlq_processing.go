package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ProcessErroredMessagesForDLQ processes ERRORED messages and moves them to their respective DLQs
func (as *storage) ProcessErroredMessagesForDLQ(ctx context.Context) error {
	start := time.Now()
	as.logger.InfoWithFields("Starting DLQ processing for ERRORED messages", "timestamp", start.UnixMilli())

	// Execute the processErroredMessages Lua script
	result, err := processErroredMessages.Run(ctx, as.redisClient, []string{}, time.Now().UnixMilli()).Result()
	if err != nil {
		as.logger.ErrorWithFields("Failed to execute DLQ processing script", "error", err)
		return fmt.Errorf("failed to execute DLQ processing script: %w", err)
	}

	// Parse the result
	var scriptResult struct {
		Success   bool        `json:"success"`
		Processed int         `json:"processed"`
		Errors    int         `json:"errors"`
		DLQMoves  int         `json:"dlq_moves"`
		DebugInfo interface{} `json:"debug_info"` // Can be array or object
		TotalKeys int         `json:"total_keys"`
		Timestamp int64       `json:"timestamp"`
		Error     string      `json:"error,omitempty"`
		Type      string      `json:"type,omitempty"`
	}

	if err := json.Unmarshal([]byte(result.(string)), &scriptResult); err != nil {
		as.logger.ErrorWithFields("Failed to parse DLQ processing result", "error", err, "result", result)
		return fmt.Errorf("failed to parse DLQ processing result: %w", err)
	}

	duration := time.Since(start)

	if !scriptResult.Success {
		as.logger.ErrorWithFields(
			"DLQ processing script failed",
			"error", scriptResult.Error,
			"type", scriptResult.Type,
			"duration", duration.String(),
		)
		return fmt.Errorf("DLQ processing script failed: %s", scriptResult.Error)
	}

	// Log detailed results
	if scriptResult.DLQMoves > 0 || scriptResult.Processed > 0 {
		as.logger.InfoWithFields(
			"DLQ processing completed successfully",
			"processed", scriptResult.Processed,
			"dlq_moves", scriptResult.DLQMoves,
			"errors", scriptResult.Errors,
			"total_keys", scriptResult.TotalKeys,
			"duration", duration.String(),
		)
	}

	// Log debug info if there were any actions taken
	if debugItems, ok := scriptResult.DebugInfo.([]interface{}); ok && len(debugItems) > 0 {
		for _, debugItem := range debugItems {
			if debugMap, ok := debugItem.(map[string]interface{}); ok {
				if action, exists := debugMap["action"]; exists {
					as.logger.DebugWithFields(
						"DLQ processing action",
						"action", action,
						"details", debugMap,
					)
				}
			}
		}
	}

	return nil
}
