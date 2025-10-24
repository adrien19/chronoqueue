package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ProcessExpiredInvisibleMessages processes INVISIBLE messages that have expired and moves them to PENDING
func (as *storage) ProcessExpiredInvisibleMessages(ctx context.Context) error {
	start := time.Now()
	as.logger.InfoWithFields("Starting processing of expired INVISIBLE messages", "timestamp", start.UnixMilli())

	// Execute the invisibleToPending Lua script
	result, err := invisibleToPending.Run(ctx, as.redisClient, []string{}, time.Now().UnixMilli()).Result()
	if err != nil {
		as.logger.ErrorWithFields("Failed to execute invisible-to-pending script", "error", err)
		return fmt.Errorf("failed to execute invisible-to-pending script: %w", err)
	}

	// Parse the result for invisible messages
	var scriptResult struct {
		Success   bool        `json:"success"`
		Processed int         `json:"processed"`
		Errors    int         `json:"errors"`
		DebugInfo interface{} `json:"debug_info"` // Can be array or object
		TotalKeys int         `json:"total_keys"`
		Error     string      `json:"error,omitempty"`
		Type      string      `json:"type,omitempty"`
	}

	if err := json.Unmarshal([]byte(result.(string)), &scriptResult); err != nil {
		as.logger.ErrorWithFields("Failed to parse invisible-to-pending result", "error", err, "result", result)
		return fmt.Errorf("failed to parse invisible-to-pending result: %w", err)
	}

	duration := time.Since(start)

	if !scriptResult.Success {
		as.logger.ErrorWithFields(
			"Invisible-to-pending script failed",
			"error", scriptResult.Error,
			"type", scriptResult.Type,
			"duration", duration.String(),
		)
		return fmt.Errorf("invisible-to-pending script failed: %s", scriptResult.Error)
	}

	// Log results if there was any processing
	if scriptResult.Processed > 0 {
		as.logger.InfoWithFields(
			"Processed expired INVISIBLE messages",
			"processed", scriptResult.Processed,
			"errors", scriptResult.Errors,
			"total_keys", scriptResult.TotalKeys,
			"duration", duration.String(),
		)
	}

	return nil
}

// ProcessExpiredRunningMessages processes RUNNING messages that have expired leases
func (as *storage) ProcessExpiredRunningMessages(ctx context.Context) error {
	start := time.Now()
	as.logger.InfoWithFields("Starting processing of expired RUNNING messages", "timestamp", start.UnixMilli())

	// Execute the runningToPending Lua script
	result, err := runningToPending.Run(ctx, as.redisClient, []string{}, time.Now().UnixMilli()).Result()
	if err != nil {
		as.logger.ErrorWithFields("Failed to execute running-to-pending script", "error", err)
		return fmt.Errorf("failed to execute running-to-pending script: %w", err)
	}

	// Parse the result
	var scriptResult struct {
		Success     bool        `json:"success"`
		Processed   int         `json:"processed"`
		Errors      int         `json:"errors"`
		Transitions int         `json:"transitions"`
		DebugInfo   interface{} `json:"debug_info"` // Can be array or object
		TotalKeys   int         `json:"total_keys"`
		Error       string      `json:"error,omitempty"`
		Type        string      `json:"type,omitempty"`
	}

	if err := json.Unmarshal([]byte(result.(string)), &scriptResult); err != nil {
		as.logger.ErrorWithFields("Failed to parse running-to-pending result", "error", err, "result", result)
		return fmt.Errorf("failed to parse running-to-pending result: %w", err)
	}

	duration := time.Since(start)

	if !scriptResult.Success {
		as.logger.ErrorWithFields(
			"Running-to-pending script failed",
			"error", scriptResult.Error,
			"type", scriptResult.Type,
			"duration", duration.String(),
		)
		return fmt.Errorf("running-to-pending script failed: %s", scriptResult.Error)
	}

	// Log results if there was any processing
	if scriptResult.Processed > 0 || scriptResult.Transitions > 0 {
		as.logger.InfoWithFields(
			"Processed expired RUNNING messages",
			"processed", scriptResult.Processed,
			"transitions", scriptResult.Transitions,
			"errors", scriptResult.Errors,
			"total_keys", scriptResult.TotalKeys,
			"duration", duration.String(),
		)
	}

	// Log debug info for transitions if there were any
	if debugItems, ok := scriptResult.DebugInfo.([]interface{}); ok && len(debugItems) > 0 {
		for _, debugItem := range debugItems {
			if debugMap, ok := debugItem.(map[string]interface{}); ok {
				if transition, exists := debugMap["transition"]; exists {
					as.logger.DebugWithFields(
						"Message state transition",
						"transition", transition,
						"key", debugMap["key"],
						"attemptsLeft", debugMap["attemptsLeft"],
					)
				}
			}
		}
	}

	return nil
}
