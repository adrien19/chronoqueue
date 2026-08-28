package schema

import (
	"encoding/json"
	"fmt"
)

// validateSchemaContent validates that content is valid JSON Schema
func validateSchemaContent(contentType, content string) error {
	if contentType != "" && contentType != "json-schema" {
		return fmt.Errorf("unsupported content type %q: only json-schema is supported", contentType)
	}
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

func encodeMetadata(metadata map[string]string) (string, error) {
	if metadata == nil {
		metadata = map[string]string{}
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return "", fmt.Errorf("encode schema metadata: %w", err)
	}
	return string(encoded), nil
}

func decodeMetadata(encoded string) (map[string]string, error) {
	metadata := make(map[string]string)
	if encoded == "" {
		return metadata, nil
	}
	if err := json.Unmarshal([]byte(encoded), &metadata); err != nil {
		return nil, fmt.Errorf("decode schema metadata: %w", err)
	}
	return metadata, nil
}
