package schema

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xeipuuv/gojsonschema"
)

const maxSchemaContentBytes = 1 << 20

// validateSchemaContent validates that content is valid JSON Schema
func validateSchemaContent(contentType, content string) error {
	if contentType != "" && contentType != "json-schema" {
		return fmt.Errorf("unsupported content type %q: only json-schema is supported", contentType)
	}
	if len(content) > maxSchemaContentBytes {
		return fmt.Errorf("schema exceeds maximum size of %d bytes", maxSchemaContentBytes)
	}
	var schema any
	if err := json.Unmarshal([]byte(content), &schema); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if err := rejectExternalReferences(schema); err != nil {
		return err
	}
	if _, err := gojsonschema.NewSchema(gojsonschema.NewStringLoader(content)); err != nil {
		return fmt.Errorf("invalid Draft 7 schema: %w", err)
	}

	return nil
}

func rejectExternalReferences(value any) error {
	switch value := value.(type) {
	case map[string]any:
		if ref, ok := value["$ref"].(string); ok && ref != "" && !strings.HasPrefix(ref, "#") {
			return fmt.Errorf("external schema reference %q is not allowed", ref)
		}
		for _, child := range value {
			if err := rejectExternalReferences(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range value {
			if err := rejectExternalReferences(child); err != nil {
				return err
			}
		}
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
