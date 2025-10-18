package outputs

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestNewOutputFormatter(t *testing.T) {
	tests := []struct {
		name   string
		format OutputFormat
	}{
		{"json formatter", OutputJSON},
		{"yaml formatter", OutputYAML},
		{"table formatter", OutputTable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			formatter := NewOutputFormatter(tt.format)
			assert.NotNil(t, formatter)
			assert.Equal(t, tt.format, formatter.format)
		})
	}
}

func TestOutputFormatter_PrintJSON(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	formatter := NewOutputFormatter(OutputJSON)
	testData := map[string]interface{}{
		"name":  "test",
		"value": 123,
		"items": []string{"a", "b", "c"},
	}

	err := formatter.Print(testData)

	// Restore stdout
	w.Close()
	os.Stdout = oldStdout

	// Read captured output
	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	assert.NoError(t, err)

	// Verify it's valid JSON
	var parsed map[string]interface{}
	err = json.Unmarshal([]byte(output), &parsed)
	assert.NoError(t, err)
	assert.Equal(t, "test", parsed["name"])
	assert.Equal(t, float64(123), parsed["value"]) // JSON numbers are float64
}

func TestOutputFormatter_PrintYAML(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	formatter := NewOutputFormatter(OutputYAML)
	testData := map[string]interface{}{
		"name":  "test",
		"value": 123,
		"items": []string{"a", "b", "c"},
	}

	err := formatter.Print(testData)

	// Restore stdout
	w.Close()
	os.Stdout = oldStdout

	// Read captured output
	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	assert.NoError(t, err)

	// Verify it's valid YAML
	var parsed map[string]interface{}
	err = yaml.Unmarshal([]byte(output), &parsed)
	assert.NoError(t, err)
	assert.Equal(t, "test", parsed["name"])
	assert.Equal(t, 123, parsed["value"])
}

func TestOutputFormatter_PrintTable(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	formatter := NewOutputFormatter(OutputTable)
	testData := map[string]interface{}{
		"name":  "test",
		"value": 123,
	}

	err := formatter.Print(testData)

	// Restore stdout
	w.Close()
	os.Stdout = oldStdout

	// Read captured output
	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	assert.NoError(t, err)
	// For table format, we just check that something was printed
	assert.NotEmpty(t, output)
}

func TestOutputFormatter_DefaultsToTable(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Use an invalid format - should default to table
	formatter := &OutputFormatter{format: "invalid"}
	testData := map[string]interface{}{
		"name": "test",
	}

	err := formatter.Print(testData)

	// Restore stdout
	w.Close()
	os.Stdout = oldStdout

	// Read captured output
	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	assert.NoError(t, err)
	assert.NotEmpty(t, output)
}

func TestOutputFormat_Constants(t *testing.T) {
	assert.Equal(t, OutputFormat("table"), OutputTable)
	assert.Equal(t, OutputFormat("json"), OutputJSON)
	assert.Equal(t, OutputFormat("yaml"), OutputYAML)
}

func TestPrintFunctions(t *testing.T) {
	// These functions print to stdout/stderr, so we test that they don't panic
	// In a more comprehensive test, you would capture the output

	tests := []struct {
		name string
		fn   func()
	}{
		{
			name: "PrintSuccess",
			fn:   func() { PrintSuccess("Test success message") },
		},
		{
			name: "PrintError",
			fn:   func() { PrintError("Test error message") },
		},
		{
			name: "PrintInfo",
			fn:   func() { PrintInfo("Test info message") },
		},
		{
			name: "PrintWarning",
			fn:   func() { PrintWarning("Test warning message") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Just ensure the function doesn't panic
			assert.NotPanics(t, tt.fn)
		})
	}
}

// Test with complex nested data structures
func TestOutputFormatter_ComplexData(t *testing.T) {
	formatter := NewOutputFormatter(OutputJSON)

	complexData := map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":       "test-queue",
			"type":       "simple",
			"created_at": "2023-01-01T00:00:00Z",
		},
		"statistics": map[string]interface{}{
			"pending":   10,
			"running":   5,
			"completed": 100,
		},
		"tags": []string{"production", "high-priority"},
	}

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := formatter.Print(complexData)

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	assert.NoError(t, err)

	// Verify the JSON is valid and contains expected data
	var parsed map[string]interface{}
	err = json.Unmarshal([]byte(output), &parsed)
	require.NoError(t, err)

	metadata, ok := parsed["metadata"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "test-queue", metadata["name"])

	statistics, ok := parsed["statistics"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, float64(10), statistics["pending"])
}

// Test error handling
func TestOutputFormatter_ErrorHandling(t *testing.T) {
	formatter := NewOutputFormatter(OutputJSON)

	// Test with data that might cause JSON marshaling issues
	// (though most Go types are JSON serializable)
	testData := map[string]interface{}{
		"valid_field": "test",
		"nil_field":   nil,
	}

	err := formatter.Print(testData)
	assert.NoError(t, err) // This should not error with normal data
}

func TestOutputFormatter_EmptyData(t *testing.T) {
	tests := []struct {
		name   string
		format OutputFormat
		data   interface{}
	}{
		{"json with nil", OutputJSON, nil},
		{"json with empty map", OutputJSON, map[string]interface{}{}},
		{"yaml with nil", OutputYAML, nil},
		{"yaml with empty map", OutputYAML, map[string]interface{}{}},
		{"table with nil", OutputTable, nil},
		{"table with empty map", OutputTable, map[string]interface{}{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			formatter := NewOutputFormatter(tt.format)

			// Capture stdout to avoid test output pollution
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			err := formatter.Print(tt.data)

			w.Close()
			os.Stdout = oldStdout

			// Discard output
			io.Copy(io.Discard, r)

			assert.NoError(t, err)
		})
	}
}
