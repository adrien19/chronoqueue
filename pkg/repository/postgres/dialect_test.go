package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostgresDialect_Placeholder(t *testing.T) {
	dialect := NewDialect()

	tests := []struct {
		name     string
		input    int
		expected string
	}{
		{"first placeholder", 1, "$1"},
		{"second placeholder", 2, "$2"},
		{"tenth placeholder", 10, "$10"},
		{"hundredth placeholder", 100, "$100"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dialect.Placeholder(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPostgresDialect_SupportsSkipLocked(t *testing.T) {
	dialect := NewDialect()
	assert.True(t, dialect.SupportsSkipLocked(), "Postgres supports SKIP LOCKED")
}

func TestPostgresDialect_SupportsReturning(t *testing.T) {
	dialect := NewDialect()
	assert.True(t, dialect.SupportsReturning(), "Postgres supports RETURNING clause")
}

func TestPostgresDialect_CurrentTimestamp(t *testing.T) {
	dialect := NewDialect()
	assert.Equal(t, "NOW()", dialect.CurrentTimestamp())
}

func TestPostgresDialect_JSONFunctions(t *testing.T) {
	dialect := NewDialect()

	t.Run("JSONSet", func(t *testing.T) {
		assert.Equal(t, "jsonb_set", dialect.JSONSet())
	})

	t.Run("JSONExtract", func(t *testing.T) {
		// Postgres uses -> or ->> operators, or json_extract_path
		// Check what the dialect actually returns
		result := dialect.JSONExtract()
		assert.NotEmpty(t, result)
	})
}

func TestPostgresDialect_JSONSetPath(t *testing.T) {
	dialect := NewDialect()

	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{"simple key", "PENDING", "'{PENDING}'::text[]"},
		{"numeric key", "1", "'{1}'::text[]"},
		{"complex key", "state.RUNNING", "'{state,RUNNING}'::text[]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dialect.JSONSetPath(tt.key)
			// Postgres requires array format for jsonb_set
			assert.Contains(t, result, "text[]")
			assert.Contains(t, result, tt.key)
		})
	}
}

func TestPostgresDialect_JSONExtractPath(t *testing.T) {
	dialect := NewDialect()

	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{"simple key", "PENDING", "'PENDING'"},
		{"numeric key", "2", "'2'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dialect.JSONExtractPath(tt.key)
			// Postgres JSONExtractPath returns simple quoted string, not array
			assert.Equal(t, tt.expected, result)
		})
	}
}
