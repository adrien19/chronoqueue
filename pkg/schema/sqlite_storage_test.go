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

func TestSQLiteRegistryRejectsUnsupportedContentType(t *testing.T) {
	registry, cleanup := setupTestSQLiteRegistry(t)
	defer cleanup()

	_, err := registry.Register(context.Background(), &schema_pb.Schema{
		SchemaId: "avro-schema", Name: "Avro", ContentType: "avro", Content: `{"type":"record"}`,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported schema content type")
}

func TestSQLiteRegistryRoundTripsMetadata(t *testing.T) {
	registry, cleanup := setupTestSQLiteRegistry(t)
	defer cleanup()
	ctx := context.Background()
	want := map[string]string{"owner": "checkout", "contact": "checkout@example.com"}
	_, err := registry.Register(ctx, &schema_pb.Schema{
		SchemaId: "metadata-schema", Name: "Metadata", Content: `{"type":"object"}`, Metadata: want,
	})
	require.NoError(t, err)

	got, err := registry.Get(ctx, "metadata-schema", 1)
	require.NoError(t, err)
	require.Equal(t, want, got.GetMetadata())
	listed, err := registry.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, want, listed[0].GetMetadata())
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
		_, err := registry.Deactivate(ctx, "schema2", 1)
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

	t.Run("ListWithOptions", func(t *testing.T) {
		for _, limit := range []int32{0, -1} {
			unlimited, err := registry.ListWithOptions(ctx, ListOptions{Prefix: "schema", Limit: limit})
			require.NoError(t, err)
			assert.Len(t, unlimited.Schemas, 3)
			assert.Equal(t, int32(3), unlimited.TotalCount)
		}

		limited, err := registry.ListWithOptions(ctx, ListOptions{Prefix: "schema", Limit: 1})
		require.NoError(t, err)
		assert.Len(t, limited.Schemas, 1)
		assert.Equal(t, int32(3), limited.TotalCount)
		assert.Equal(t, int32(1), limited.Metadata[limited.Schemas[0].GetSchemaId()].TotalVersions)

		secondPage, err := registry.ListWithOptions(ctx, ListOptions{Prefix: "schema", Limit: 1, Cursor: "schema1"})
		require.NoError(t, err)
		require.Len(t, secondPage.Schemas, 1)
		assert.Equal(t, "schema2", secondPage.Schemas[0].GetSchemaId())
		assert.Equal(t, int32(3), secondPage.TotalCount)

		emptyPage, err := registry.ListWithOptions(ctx, ListOptions{Prefix: "schema", Limit: 1, Cursor: "schema3"})
		require.NoError(t, err)
		assert.Empty(t, emptyPage.Schemas)
		assert.Equal(t, int32(3), emptyPage.TotalCount)

		inactive, err := registry.ListWithOptions(ctx, ListOptions{Prefix: "schema2", Limit: 100})
		require.NoError(t, err)
		require.Len(t, inactive.Schemas, 1)
		assert.False(t, inactive.Schemas[0].GetIsActive())
		assert.Equal(t, int32(1), inactive.TotalCount)

		activeOnly, err := registry.ListWithOptions(ctx, ListOptions{Prefix: "schema2", Limit: 100, ActiveOnly: true})
		require.NoError(t, err)
		assert.Empty(t, activeOnly.Schemas)
		assert.Zero(t, activeOnly.TotalCount)

		literalPrefix, err := registry.ListWithOptions(ctx, ListOptions{Prefix: "%", Limit: 100})
		require.NoError(t, err)
		assert.Empty(t, literalPrefix.Schemas)
		assert.Zero(t, literalPrefix.TotalCount)
	})
}

func TestSQLiteRegistry_ActiveSelectionFallsBackFromInactiveLatestVersion(t *testing.T) {
	registry, cleanup := setupTestSQLiteRegistry(t)
	defer cleanup()

	ctx := context.Background()
	schemaID := "latest-inactive"
	for _, name := range []string{"version 1", "version 2"} {
		_, err := registry.Register(ctx, &schema_pb.Schema{
			SchemaId: schemaID,
			Name:     name,
			Content:  `{"type":"object","properties":{"field":{"type":"string"}}}`,
		})
		require.NoError(t, err)
	}
	_, err := registry.Deactivate(ctx, schemaID, 2)
	require.NoError(t, err)

	activeOnly, err := registry.ListWithOptions(ctx, ListOptions{Prefix: schemaID, Limit: 100, ActiveOnly: true})
	require.NoError(t, err)
	require.Len(t, activeOnly.Schemas, 1)
	assert.Equal(t, int32(1), activeOnly.Schemas[0].GetVersion())
	assert.True(t, activeOnly.Schemas[0].GetIsActive())
	assert.Equal(t, int32(1), activeOnly.TotalCount)

	latest, err := registry.GetLatest(ctx, schemaID)
	require.NoError(t, err)
	assert.Equal(t, int32(1), latest.GetVersion())

	validation, err := registry.Validate(ctx, schemaID, 0, []byte(`{"field":"value"}`))
	require.NoError(t, err)
	require.True(t, validation.GetValid())
	assert.Equal(t, int32(1), validation.GetSchemaVersion())

	all, err := registry.ListWithOptions(ctx, ListOptions{Prefix: schemaID, Limit: 100})
	require.NoError(t, err)
	require.Len(t, all.Schemas, 1)
	assert.Equal(t, int32(2), all.Schemas[0].GetVersion())
	assert.False(t, all.Schemas[0].GetIsActive())
	assert.Equal(t, int32(1), all.TotalCount)
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
		count, err := registry.Deactivate(ctx, "deactivate.test", 1)
		require.NoError(t, err)
		assert.Equal(t, int32(1), count)

		// Verify it's deactivated
		retrieved, err := registry.Get(ctx, "deactivate.test", 1)
		require.NoError(t, err)
		assert.False(t, retrieved.IsActive)
	})

	t.Run("DeactivateNonExistent", func(t *testing.T) {
		_, err := registry.Deactivate(ctx, "nonexistent", 1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "schema not found")
	})
}

func TestSQLiteRegistry_DeactivateAllVersions(t *testing.T) {
	registry, cleanup := setupTestSQLiteRegistry(t)
	defer cleanup()

	ctx := context.Background()
	for range 2 {
		_, err := registry.Register(ctx, &schema_pb.Schema{
			SchemaId: "deactivate-all.test",
			Name:     "Deactivate All Test",
			Content:  `{"type":"object"}`,
		})
		require.NoError(t, err)
	}

	count, err := registry.Deactivate(ctx, "deactivate-all.test", 0)
	require.NoError(t, err)
	assert.Equal(t, int32(2), count)

	result, err := registry.ListWithOptions(ctx, ListOptions{Prefix: "deactivate-all.test", Limit: 10, ActiveOnly: true})
	require.NoError(t, err)
	assert.Empty(t, result.Schemas)

	_, err = registry.GetLatest(ctx, "deactivate-all.test")
	require.ErrorContains(t, err, "schema")

	validation, err := registry.Validate(ctx, "deactivate-all.test", 0, []byte(`{}`))
	require.NoError(t, err)
	require.False(t, validation.GetValid())
	require.NotEmpty(t, validation.GetErrors())
	assert.Equal(t, schema_pb.ErrorCode_SCHEMA_NOT_FOUND.String(), validation.GetErrors()[0].GetErrorCode())
}

func TestSQLiteRegistry_RegisterRejectsExistingVersion(t *testing.T) {
	registry, cleanup := setupTestSQLiteRegistry(t)
	defer cleanup()

	ctx := context.Background()
	schema := &schema_pb.Schema{SchemaId: "duplicate.test", Version: 1, Name: "Original", Content: `{"type":"object"}`}
	_, err := registry.Register(ctx, schema)
	require.NoError(t, err)

	_, err = registry.Register(ctx, &schema_pb.Schema{SchemaId: schema.SchemaId, Version: 1, Name: "Replacement", Content: `{"type":"string"}`})
	require.ErrorContains(t, err, "already exists")

	stored, err := registry.Get(ctx, schema.SchemaId, 1)
	require.NoError(t, err)
	assert.Equal(t, "Original", stored.Name)
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
	firstCreatedAt := metadata.CreatedAt

	time.Sleep(10 * time.Millisecond)
	metadata, err = registry.Register(ctx, &schema_pb.Schema{
		SchemaId: schema.SchemaId,
		Name:     "Timestamp Test v2",
		Content:  schema.Content,
	})
	require.NoError(t, err)
	assert.Equal(t, firstCreatedAt, metadata.CreatedAt)

	listed, err := registry.ListWithOptions(ctx, ListOptions{Prefix: schema.SchemaId, Limit: 1})
	require.NoError(t, err)
	listedMetadata := listed.Metadata[schema.SchemaId]
	assert.Equal(t, metadata.CreatedAt, listedMetadata.CreatedAt)
	assert.Equal(t, metadata.UpdatedAt, listedMetadata.UpdatedAt)
	assert.Equal(t, metadata.TotalVersions, listedMetadata.TotalVersions)

	time.Sleep(10 * time.Millisecond)
	afterRegister := time.Now().UnixMilli()

	// Verify timestamps are reasonable
	assert.True(t, metadata.CreatedAt.UnixMilli() >= beforeRegister)
	assert.True(t, metadata.CreatedAt.UnixMilli() <= afterRegister)
	assert.True(t, metadata.UpdatedAt.UnixMilli() >= beforeRegister)
	assert.True(t, metadata.UpdatedAt.UnixMilli() <= afterRegister)
}
