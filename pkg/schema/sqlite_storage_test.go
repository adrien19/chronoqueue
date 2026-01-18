//go:build sqlite
// +build sqlite

package schema

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schema_pb "github.com/adrien19/chronoqueue/api/schema/v1"
	"github.com/adrien19/chronoqueue/pkg/log"
)

func setupTestSQLiteRegistry(t *testing.T) (*SQLiteRegistry, func()) {
	// Create in-memory database
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)

	logger := log.NewLogger()

	registry, err := NewSQLiteRegistry(db, logger)
	require.NoError(t, err)

	cleanup := func() {
		db.Close()
	}

	return registry, cleanup
}

func TestSQLiteRegistry_Register(t *testing.T) {
	registry, cleanup := setupTestSQLiteRegistry(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("RegisterNewSchema", func(t *testing.T) {
		schema := &schema_pb.Schema{
			SchemaId:    "user.profile.v1",
			Name:        "User Profile",
			Description: "Schema for user profile data",
			Content: `{
				"type": "object",
				"required": ["name", "email"],
				"properties": {
					"name": {"type": "string"},
					"email": {"type": "string", "format": "email"}
				}
			}`,
			ContentType: "json-schema",
		}

		metadata, err := registry.Register(ctx, schema)
		require.NoError(t, err)
		assert.Equal(t, "user.profile.v1", metadata.SchemaID)
		assert.Equal(t, int32(1), metadata.LatestVersion)
		assert.Equal(t, int32(1), metadata.TotalVersions)
	})

	t.Run("RegisterNewVersion", func(t *testing.T) {
		// Register first version
		schema1 := &schema_pb.Schema{
			SchemaId: "product.v1",
			Name:     "Product Schema",
			Content: `{
				"type": "object",
				"required": ["id", "name"],
				"properties": {
					"id": {"type": "string"},
					"name": {"type": "string"}
				}
			}`,
		}

		_, err := registry.Register(ctx, schema1)
		require.NoError(t, err)

		// Register second version
		schema2 := &schema_pb.Schema{
			SchemaId: "product.v1",
			Name:     "Product Schema v2",
			Content: `{
				"type": "object",
				"required": ["id", "name", "price"],
				"properties": {
					"id": {"type": "string"},
					"name": {"type": "string"},
					"price": {"type": "number"}
				}
			}`,
		}

		metadata, err := registry.Register(ctx, schema2)
		require.NoError(t, err)
		assert.Equal(t, int32(2), metadata.LatestVersion)
		assert.Equal(t, int32(2), metadata.TotalVersions)
	})

	t.Run("RegisterInvalidSchema", func(t *testing.T) {
		schema := &schema_pb.Schema{
			SchemaId: "invalid.schema",
			Name:     "Invalid Schema",
			Content:  `{"invalid": "json"`, // Invalid JSON
		}

		_, err := registry.Register(ctx, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid schema content")
	})
}

func TestSQLiteRegistry_Get(t *testing.T) {
	registry, cleanup := setupTestSQLiteRegistry(t)
	defer cleanup()

	ctx := context.Background()

	// Register test schema
	schema := &schema_pb.Schema{
		SchemaId: "test.schema",
		Name:     "Test Schema",
		Content: `{
			"type": "object",
			"required": ["field1"],
			"properties": {
				"field1": {"type": "string"}
			}
		}`,
	}

	_, err := registry.Register(ctx, schema)
	require.NoError(t, err)

	t.Run("GetExistingSchema", func(t *testing.T) {
		retrieved, err := registry.Get(ctx, "test.schema", 1)
		require.NoError(t, err)
		assert.Equal(t, "test.schema", retrieved.SchemaId)
		assert.Equal(t, int32(1), retrieved.Version)
		assert.Equal(t, "Test Schema", retrieved.Name)
		assert.True(t, retrieved.IsActive)
	})

	t.Run("GetNonExistentSchema", func(t *testing.T) {
		_, err := registry.Get(ctx, "nonexistent", 1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "schema not found")
	})

	t.Run("GetWithVersionZero", func(t *testing.T) {
		// Version 0 should return latest
		retrieved, err := registry.Get(ctx, "test.schema", 0)
		require.NoError(t, err)
		assert.Equal(t, int32(1), retrieved.Version)
	})
}

func TestSQLiteRegistry_GetLatest(t *testing.T) {
	registry, cleanup := setupTestSQLiteRegistry(t)
	defer cleanup()

	ctx := context.Background()

	// Register multiple versions
	for i := 1; i <= 3; i++ {
		schema := &schema_pb.Schema{
			SchemaId: "versioned.schema",
			Name:     "Versioned Schema",
			Content: `{
				"type": "object",
				"required": ["field1"],
				"properties": {
					"field1": {"type": "string"}
				}
			}`,
		}

		_, err := registry.Register(ctx, schema)
		require.NoError(t, err)
	}

	t.Run("GetLatestVersion", func(t *testing.T) {
		latest, err := registry.GetLatest(ctx, "versioned.schema")
		require.NoError(t, err)
		assert.Equal(t, int32(3), latest.Version)
	})

	t.Run("GetLatestNonExistent", func(t *testing.T) {
		_, err := registry.GetLatest(ctx, "nonexistent")
		assert.Error(t, err)
	})
}

func TestSQLiteRegistry_List(t *testing.T) {
	registry, cleanup := setupTestSQLiteRegistry(t)
	defer cleanup()

	ctx := context.Background()

	// Register multiple schemas
	schemas := []string{"schema1", "schema2", "schema3"}
	for _, schemaID := range schemas {
		schema := &schema_pb.Schema{
			SchemaId: schemaID,
			Name:     schemaID + " name",
			Content: `{
				"type": "object",
				"properties": {
					"field": {"type": "string"}
				}
			}`,
		}

		_, err := registry.Register(ctx, schema)
		require.NoError(t, err)
	}

	t.Run("ListAllSchemas", func(t *testing.T) {
		list, err := registry.List(ctx)
		require.NoError(t, err)
		assert.Len(t, list, 3)

		// Check schema IDs
		schemaIDs := make(map[string]bool)
		for _, s := range list {
			schemaIDs[s.SchemaId] = true
		}
		assert.True(t, schemaIDs["schema1"])
		assert.True(t, schemaIDs["schema2"])
		assert.True(t, schemaIDs["schema3"])
	})

	t.Run("ListOnlyActiveSchemas", func(t *testing.T) {
		// Deactivate one schema
		err := registry.Deactivate(ctx, "schema2", 1)
		require.NoError(t, err)

		list, err := registry.List(ctx)
		require.NoError(t, err)
		assert.Len(t, list, 2) // Only active schemas

		schemaIDs := make(map[string]bool)
		for _, s := range list {
			schemaIDs[s.SchemaId] = true
		}
		assert.True(t, schemaIDs["schema1"])
		assert.False(t, schemaIDs["schema2"]) // Deactivated
		assert.True(t, schemaIDs["schema3"])
	})
}

func TestSQLiteRegistry_Deactivate(t *testing.T) {
	registry, cleanup := setupTestSQLiteRegistry(t)
	defer cleanup()

	ctx := context.Background()

	// Register test schema
	schema := &schema_pb.Schema{
		SchemaId: "deactivate.test",
		Name:     "Deactivate Test",
		Content: `{
			"type": "object",
			"properties": {
				"field": {"type": "string"}
			}
		}`,
	}

	_, err := registry.Register(ctx, schema)
	require.NoError(t, err)

	t.Run("DeactivateExisting", func(t *testing.T) {
		err := registry.Deactivate(ctx, "deactivate.test", 1)
		require.NoError(t, err)

		// Verify it's deactivated
		retrieved, err := registry.Get(ctx, "deactivate.test", 1)
		require.NoError(t, err)
		assert.False(t, retrieved.IsActive)
	})

	t.Run("DeactivateNonExistent", func(t *testing.T) {
		err := registry.Deactivate(ctx, "nonexistent", 1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "schema not found")
	})
}

func TestSQLiteRegistry_Validate(t *testing.T) {
	registry, cleanup := setupTestSQLiteRegistry(t)
	defer cleanup()

	ctx := context.Background()

	// Register test schema
	schema := &schema_pb.Schema{
		SchemaId: "validation.test",
		Name:     "Validation Test",
		Content: `{
			"type": "object",
			"required": ["name", "email"],
			"properties": {
				"name": {"type": "string"},
				"email": {"type": "string", "format": "email"},
				"age": {"type": "number", "minimum": 0}
			}
		}`,
	}

	_, err := registry.Register(ctx, schema)
	require.NoError(t, err)

	t.Run("ValidPayload", func(t *testing.T) {
		payload := []byte(`{
			"name": "John Doe",
			"email": "john@example.com",
			"age": 30
		}`)

		result, err := registry.Validate(ctx, "validation.test", 1, payload)
		require.NoError(t, err)
		assert.True(t, result.Valid)
		assert.Empty(t, result.Errors)
		assert.Equal(t, "validation.test", result.SchemaId)
		assert.Equal(t, int32(1), result.SchemaVersion)
	})

	t.Run("MissingRequiredField", func(t *testing.T) {
		payload := []byte(`{
			"name": "John Doe"
		}`)

		result, err := registry.Validate(ctx, "validation.test", 1, payload)
		require.NoError(t, err)
		assert.False(t, result.Valid)
		assert.NotEmpty(t, result.Errors)
	})

	t.Run("InvalidFieldType", func(t *testing.T) {
		payload := []byte(`{
			"name": "John Doe",
			"email": "john@example.com",
			"age": "thirty"
		}`)

		result, err := registry.Validate(ctx, "validation.test", 1, payload)
		require.NoError(t, err)
		assert.False(t, result.Valid)
		assert.NotEmpty(t, result.Errors)
	})

	t.Run("ValidateWithVersionZero", func(t *testing.T) {
		payload := []byte(`{
			"name": "Jane Doe",
			"email": "jane@example.com"
		}`)

		// Version 0 should use latest
		result, err := registry.Validate(ctx, "validation.test", 0, payload)
		require.NoError(t, err)
		assert.True(t, result.Valid)
		assert.Equal(t, int32(1), result.SchemaVersion)
	})

	t.Run("ValidateNonExistentSchema", func(t *testing.T) {
		payload := []byte(`{"field": "value"}`)

		result, err := registry.Validate(ctx, "nonexistent", 1, payload)
		require.NoError(t, err)
		assert.False(t, result.Valid)
		assert.NotEmpty(t, result.Errors)
		assert.Contains(t, result.Errors[0].Message, "Schema not found")
	})
}

func TestSQLiteRegistry_IsCompatible(t *testing.T) {
	registry, cleanup := setupTestSQLiteRegistry(t)
	defer cleanup()

	ctx := context.Background()

	// Register base schema
	baseSchema := &schema_pb.Schema{
		SchemaId: "compat.test",
		Name:     "Compatibility Test",
		Content: `{
			"type": "object",
			"required": ["field1", "field2"],
			"properties": {
				"field1": {"type": "string"},
				"field2": {"type": "number"}
			}
		}`,
	}

	_, err := registry.Register(ctx, baseSchema)
	require.NoError(t, err)

	t.Run("CompatibleChange", func(t *testing.T) {
		// Adding optional field is compatible
		newContent := `{
			"type": "object",
			"required": ["field1", "field2"],
			"properties": {
				"field1": {"type": "string"},
				"field2": {"type": "number"},
				"field3": {"type": "string"}
			}
		}`

		compatible, err := registry.IsCompatible(ctx, "compat.test", newContent)
		require.NoError(t, err)
		assert.True(t, compatible)
	})

	t.Run("IncompatibleChange", func(t *testing.T) {
		// Removing required field is incompatible
		newContent := `{
			"type": "object",
			"required": ["field1"],
			"properties": {
				"field1": {"type": "string"}
			}
		}`

		compatible, err := registry.IsCompatible(ctx, "compat.test", newContent)
		require.NoError(t, err)
		assert.False(t, compatible)
	})

	t.Run("NewSchemaIsCompatible", func(t *testing.T) {
		// Non-existent schema is always compatible
		newContent := `{
			"type": "object",
			"properties": {
				"field": {"type": "string"}
			}
		}`

		compatible, err := registry.IsCompatible(ctx, "newschema", newContent)
		require.NoError(t, err)
		assert.True(t, compatible)
	})
}

func TestSQLiteRegistry_Timestamps(t *testing.T) {
	registry, cleanup := setupTestSQLiteRegistry(t)
	defer cleanup()

	ctx := context.Background()

	schema := &schema_pb.Schema{
		SchemaId: "timestamp.test",
		Name:     "Timestamp Test",
		Content: `{
			"type": "object",
			"properties": {
				"field": {"type": "string"}
			}
		}`,
	}

	beforeRegister := time.Now().UnixMilli()
	time.Sleep(10 * time.Millisecond)

	metadata, err := registry.Register(ctx, schema)
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)
	afterRegister := time.Now().UnixMilli()

	// Verify timestamps are reasonable
	assert.True(t, metadata.CreatedAt.UnixMilli() >= beforeRegister)
	assert.True(t, metadata.CreatedAt.UnixMilli() <= afterRegister)
	assert.True(t, metadata.UpdatedAt.UnixMilli() >= beforeRegister)
	assert.True(t, metadata.UpdatedAt.UnixMilli() <= afterRegister)
}
