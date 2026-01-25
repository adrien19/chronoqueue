package schema

import (
	"encoding/json"
	"fmt"
)

// validateSchemaContent validates that content is valid JSON Schema
func validateSchemaContent(content string) error {
	// Validate that content is valid JSON
	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(content), &schema); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	// Check for required JSON Schema fields
	if _, ok := schema["type"]; !ok {
		return fmt.Errorf("schema missing required 'type' field")
	}

	return nil
}

// getRequiredFields extracts the required fields from a JSON Schema
func getRequiredFields(schema map[string]interface{}) map[string]bool {
	required := make(map[string]bool)

	if requiredArray, ok := schema["required"].([]interface{}); ok {
		for _, field := range requiredArray {
			if fieldStr, ok := field.(string); ok {
				required[fieldStr] = true
			}
		}
	}

	return required
}
