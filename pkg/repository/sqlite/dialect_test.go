package sqlite

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSQLiteDialect_Placeholder(t *testing.T) {
	dialect := NewDialect()

	tests := []struct {
		name     string
		input    int
		expected string
	}{
		{"first placeholder", 1, "?"},
		{"second placeholder", 2, "?"},
		{"tenth placeholder", 10, "?"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dialect.Placeholder(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSQLiteDialect_SupportsSkipLocked(t *testing.T) {
	dialect := NewDialect()
	assert.False(t, dialect.SupportsSkipLocked(), "SQLite does not support SKIP LOCKED")
}

func TestSQLiteDialect_SupportsReturning(t *testing.T) {
	dialect := NewDialect()
	assert.True(t, dialect.SupportsReturning(), "SQLite supports RETURNING clause")
}

func TestSQLiteDialect_CurrentTimestamp(t *testing.T) {
	dialect := NewDialect()
	assert.Equal(t, "CURRENT_TIMESTAMP", dialect.CurrentTimestamp())
}

func TestSQLiteDialect_JSONFunctions(t *testing.T) {
	dialect := NewDialect()

	t.Run("JSONSet", func(t *testing.T) {
		assert.Equal(t, "json_set", dialect.JSONSet())
	})

	t.Run("JSONExtract", func(t *testing.T) {
		assert.Equal(t, "json_extract", dialect.JSONExtract())
	})
}

func TestSQLiteDialect_JSONSetPath(t *testing.T) {
	dialect := NewDialect()

	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{"simple key", "PENDING", "'$.PENDING'"},
		{"numeric key", "1", "'$.1'"},
		{"complex key", "state.RUNNING", "'$.state.RUNNING'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dialect.JSONSetPath(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSQLiteDialect_JSONExtractPath(t *testing.T) {
	dialect := NewDialect()

	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{"simple key", "PENDING", "'$.PENDING'"},
		{"numeric key", "2", "'$.2'"},
		{"complex key", "counters.active", "'$.counters.active'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dialect.JSONExtractPath(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}
