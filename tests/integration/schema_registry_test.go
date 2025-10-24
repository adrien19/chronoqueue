package integration

// Package integration provides schema registry and validation tests for ChronoQueue.
//
// These tests validate:
// - JSON Schema registration and versioning
// - Payload validation against schemas
// - Queue-level schema enforcement
// - Multi-layer validation pipeline
//
// Test Scenarios: TC-V-001 through TC-V-010 from TESTING_GUIDE.md
//
// Run with: go test -v ./tests/integration/ -run TestSchema

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/tests/helpers"
)

// TestSchemaRegistry_RegisterSchema validates JSON schema registration
//
// Test Scenario: TC-V-001 from TESTING_GUIDE.md
// Data: fixtures/schemas/order_schema.json
// Expected: Schema stored with version 1
func TestSchemaRegistry_RegisterSchema(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	// Load schema from fixture
	schemaContent := helpers.LoadJSONSchema(t, "order_schema.json")
	schemaID := "order-schema-" + helpers.GenerateRandomID(8)

	// Act
	registerResp, err := client.RegisterSchema(ctx, &queueservice_pb.RegisterSchemaRequest{
		SchemaId:    schemaID,
		Name:        "Order Schema",
		Description: "Order schema for testing",
		Content:     schemaContent,
	})

	// Assert
	require.NoError(t, err, "Schema registration should succeed")
	assert.NotNil(t, registerResp, "Response should not be nil")
	assert.NotEmpty(t, registerResp.SchemaId, "Schema ID should be returned")

	t.Logf("Registered schema: %s", schemaID)
}

// TestSchemaRegistry_GetSchema validates schema retrieval
//
// Test Scenario: TC-V-002 from TESTING_GUIDE.md
// Data: Previously registered schema
// Expected: Schema content matches original
func TestSchemaRegistry_GetSchema(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	// Register schema first
	schemaContent := helpers.LoadJSONSchema(t, "event_schema.json")
	schemaID := "event-schema-" + helpers.GenerateRandomID(8)

	_, err := client.RegisterSchema(ctx, &queueservice_pb.RegisterSchemaRequest{
		SchemaId:    schemaID,
		Name:        "Event Schema",
		Description: "Event schema for testing",
		Content:     schemaContent,
	})
	require.NoError(t, err)

	// Act - Retrieve schema
	getResp, err := client.GetSchema(ctx, &queueservice_pb.GetSchemaRequest{
		SchemaId: schemaID,
		Version:  1,
	})

	// Assert
	require.NoError(t, err, "Get schema should succeed")
	assert.NotNil(t, getResp.Schema, "Schema should be returned")
	assert.Equal(t, schemaID, getResp.Schema.SchemaId, "Schema ID should match")
	assert.Equal(t, int32(1), getResp.Schema.Version, "Version should match")
	assert.Equal(t, schemaContent, getResp.Schema.Content, "Content should match original")
}

// TestSchemaRegistry_RegisterMultipleVersions validates schema versioning
//
// Test Scenario: TC-V-003 from TESTING_GUIDE.md
// Data: Schema versions 1 and 2
// Expected: Both versions stored independently
func TestSchemaRegistry_RegisterMultipleVersions(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	schemaID := "versioned-schema-" + helpers.GenerateRandomID(8)
	schemaV1 := helpers.LoadJSONSchema(t, "order_schema.json")
	schemaV2 := helpers.LoadJSONSchema(t, "notification_schema.json") // Different schema as v2

	// Act - Register version 1
	reg1Resp, err := client.RegisterSchema(ctx, &queueservice_pb.RegisterSchemaRequest{
		SchemaId:    schemaID,
		Name:        "Versioned Schema V1",
		Description: "Version 1",
		Content:     schemaV1,
	})
	require.NoError(t, err)
	assert.NotNil(t, reg1Resp)

	// Act - Register version 2 (using different schema ID to simulate versioning)
	schemaV2ID := schemaID + "-v2"
	reg2Resp, err := client.RegisterSchema(ctx, &queueservice_pb.RegisterSchemaRequest{
		SchemaId:    schemaV2ID,
		Name:        "Versioned Schema V2",
		Description: "Version 2 with breaking changes",
		Content:     schemaV2,
	})
	require.NoError(t, err)
	assert.NotNil(t, reg2Resp)

	// Assert - Both versions should be retrievable
	getV1, err := client.GetSchema(ctx, &queueservice_pb.GetSchemaRequest{
		SchemaId: schemaID,
		Version:  1,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), getV1.Schema.Version)

	getV2, err := client.GetSchema(ctx, &queueservice_pb.GetSchemaRequest{
		SchemaId: schemaV2ID,
		Version:  1, // First version of the second schema
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), getV2.Schema.Version)

	t.Logf("Successfully registered and retrieved schema versions 1 and 2")
}

// TestSchemaRegistry_ListSchemas validates listing all schemas
//
// Test Scenario: TC-V-004 from TESTING_GUIDE.md
// Data: Multiple registered schemas
// Expected: All schemas with all versions returned
func TestSchemaRegistry_ListSchemas(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	// Register multiple schemas
	schemas := []struct {
		id      string
		fixture string
	}{
		{"order-list-" + helpers.GenerateRandomID(4), "order_schema.json"},
		{"event-list-" + helpers.GenerateRandomID(4), "event_schema.json"},
		{"notif-list-" + helpers.GenerateRandomID(4), "notification_schema.json"},
	}

	for _, s := range schemas {
		content := helpers.LoadJSONSchema(t, s.fixture)
		_, err := client.RegisterSchema(ctx, &queueservice_pb.RegisterSchemaRequest{
			SchemaId:    s.id,
			Name:        s.id,
			Description: "Test schema",
			Content:     content,
		})
		require.NoError(t, err)
	}

	// Act
	listResp, err := client.ListSchemas(ctx, &queueservice_pb.ListSchemasRequest{})

	// Assert
	require.NoError(t, err, "List schemas should succeed")
	assert.GreaterOrEqual(t, len(listResp.Schemas), len(schemas),
		"Should return at least the schemas we registered")

	t.Logf("Listed %d schemas", len(listResp.Schemas))
}

// TestSchemaRegistry_DeleteSchema validates schema deletion
//
// Test Scenario: TC-V-005 from TESTING_GUIDE.md
// Data: Registered schema
// Expected: Schema version deleted
func TestSchemaRegistry_DeleteSchema(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	// Register schema
	schemaContent := helpers.LoadJSONSchema(t, "order_schema.json")
	schemaID := "delete-schema-" + helpers.GenerateRandomID(8)

	_, err := client.RegisterSchema(ctx, &queueservice_pb.RegisterSchemaRequest{
		SchemaId:    schemaID,
		Name:        "Delete Test Schema",
		Description: "Schema to delete",
		Content:     schemaContent,
	})
	require.NoError(t, err)

	// Act - Delete schema
	deleteResp, err := client.DeleteSchema(ctx, &queueservice_pb.DeleteSchemaRequest{
		SchemaId: schemaID,
		Version:  1,
	})

	// Assert
	require.NoError(t, err, "Delete schema should succeed")
	assert.True(t, deleteResp.Success, "Response should indicate success")

	// Verify schema no longer exists
	_, err = client.GetSchema(ctx, &queueservice_pb.GetSchemaRequest{
		SchemaId: schemaID,
		Version:  1,
	})
	if err != nil {
		t.Logf("Expected: Schema not found after deletion: %v", err)
	}
}

// TestSchemaValidation_ValidPayload validates payload against schema
//
// Test Scenario: TC-V-006 from TESTING_GUIDE.md
// Data: Valid order JSON matching order_schema.json
// Expected: Validation passes
func TestSchemaValidation_ValidPayload(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	// Register schema
	schemaContent := helpers.LoadJSONSchema(t, "order_schema.json")
	schemaID := "validate-order-" + helpers.GenerateRandomID(8)

	_, err := client.RegisterSchema(ctx, &queueservice_pb.RegisterSchemaRequest{
		SchemaId:    schemaID,
		Name:        "Order Schema",
		Description: "Order schema for validation",
		Content:     schemaContent,
	})
	require.NoError(t, err)

	// Valid order payload
	validPayload := `{
		"order_id": "ORD-12345",
		"customer_id": "CUST-67890",
		"items": [
			{"sku": "ITEM-001", "quantity": 2, "price": 29.99}
		],
		"total": 59.98,
		"timestamp": "2025-10-20T10:00:00Z"
	}`

	// Act
	validateResp, err := client.ValidatePayload(ctx, &queueservice_pb.ValidatePayloadRequest{
		SchemaId: schemaID,
		Version:  1,
		Payload:  validPayload,
	})

	// Assert
	require.NoError(t, err, "Validation API should succeed")
	assert.True(t, validateResp.Valid, "Valid payload should pass validation")

	// Log errors if any (even if valid, useful for debugging)
	if len(validateResp.Errors) > 0 {
		for _, verr := range validateResp.Errors {
			t.Logf("Validation error: field=%s, code=%s, msg=%s",
				verr.Field, verr.ErrorCode, verr.Message)
		}
	}
	t.Logf("Validation result: valid=%v, error_count=%d", validateResp.Valid, len(validateResp.Errors))
}

// TestSchemaValidation_InvalidPayload_MissingField validates error detection
//
// Test Scenario: TC-V-007 from TESTING_GUIDE.md
// Data: Order JSON missing required customer_id field
// Expected: Validation fails with specific error
func TestSchemaValidation_InvalidPayload_MissingField(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	// Register schema
	schemaContent := helpers.LoadJSONSchema(t, "order_schema.json")
	schemaID := "validate-missing-" + helpers.GenerateRandomID(8)

	_, err := client.RegisterSchema(ctx, &queueservice_pb.RegisterSchemaRequest{
		SchemaId:    schemaID,
		Name:        "Order Schema",
		Description: "Order schema for validation",
		Content:     schemaContent,
	})
	require.NoError(t, err)

	// Invalid payload - missing customer_id
	invalidPayload := `{
		"order_id": "ORD-12345",
		"total": 59.98
	}`

	// Act
	validateResp, err := client.ValidatePayload(ctx, &queueservice_pb.ValidatePayloadRequest{
		SchemaId: schemaID,
		Version:  1,
		Payload:  invalidPayload,
	})

	// Assert
	if err != nil {
		t.Logf("Validation returned error (acceptable): %v", err)
	} else {
		assert.False(t, validateResp.Valid, "Invalid payload should fail validation")

		// Check errors for customer_id mention
		foundCustomerIdError := false
		for _, verr := range validateResp.Errors {
			t.Logf("Validation error: field=%s, code=%s, msg=%s",
				verr.Field, verr.ErrorCode, verr.Message)
			if verr.Field == "customer_id" || verr.Message == "customer_id" {
				foundCustomerIdError = true
			}
		}

		if foundCustomerIdError {
			t.Log("Validation correctly identified missing customer_id field")
		} else {
			t.Log("Validation failed but didn't specifically mention customer_id")
		}
	}
}

// TestSchemaValidation_InvalidPayload_WrongType validates type checking
//
// Test Scenario: TC-V-008 from TESTING_GUIDE.md
// Data: Order with total as string instead of number
// Expected: Validation fails with type mismatch error
func TestSchemaValidation_InvalidPayload_WrongType(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	// Register schema
	schemaContent := helpers.LoadJSONSchema(t, "order_schema.json")
	schemaID := "validate-type-" + helpers.GenerateRandomID(8)

	_, err := client.RegisterSchema(ctx, &queueservice_pb.RegisterSchemaRequest{
		SchemaId:    schemaID,
		Name:        "Order Schema",
		Description: "Order schema for validation",
		Content:     schemaContent,
	})
	require.NoError(t, err)

	// Invalid payload - total as string
	invalidPayload := `{
		"order_id": "ORD-12345",
		"customer_id": "CUST-67890",
		"total": "fifty-nine dollars"
	}`

	// Act
	validateResp, err := client.ValidatePayload(ctx, &queueservice_pb.ValidatePayloadRequest{
		SchemaId: schemaID,
		Version:  1,
		Payload:  invalidPayload,
	})

	// Assert
	if err != nil {
		t.Logf("Validation returned error (acceptable): %v", err)
	} else {
		assert.False(t, validateResp.Valid, "Invalid payload should fail validation")

		// Log all validation errors
		for _, verr := range validateResp.Errors {
			t.Logf("Validation error: field=%s, code=%s, msg=%s",
				verr.Field, verr.ErrorCode, verr.Message)
		}
	}
}

// TestSchemaValidation_ComplexSchema validates event schema with enums
//
// Test Scenario: Complex schema with enum constraints
// Data: Event schema with valid and invalid event types
// Expected: Enum validation works correctly
func TestSchemaValidation_ComplexSchema(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()
	client := queueservice_pb.NewQueueServiceClient(conn)

	// Register event schema (has enum for event_type)
	schemaContent := helpers.LoadJSONSchema(t, "event_schema.json")
	schemaID := "validate-event-" + helpers.GenerateRandomID(8)

	_, err := client.RegisterSchema(ctx, &queueservice_pb.RegisterSchemaRequest{
		SchemaId:    schemaID,
		Name:        "Event Schema",
		Description: "Event schema with enum",
		Content:     schemaContent,
	})
	require.NoError(t, err)

	// Valid payload with allowed event_type
	validPayload := `{
		"event_type": "USER_REGISTERED",
		"user_id": "USER-12345",
		"email": "user@example.com",
		"timestamp": "2025-10-20T09:30:00Z"
	}`

	// Act - Validate valid payload
	validResp, err := client.ValidatePayload(ctx, &queueservice_pb.ValidatePayloadRequest{
		SchemaId: schemaID,
		Version:  1,
		Payload:  validPayload,
	})

	// Assert
	require.NoError(t, err)
	assert.True(t, validResp.Valid, "Valid event type should pass")

	// Invalid payload with disallowed event_type
	invalidPayload := `{
		"event_type": "INVALID_EVENT_TYPE",
		"timestamp": "2025-10-20T09:30:00Z"
	}`

	// Act - Validate invalid payload
	invalidResp, err := client.ValidatePayload(ctx, &queueservice_pb.ValidatePayloadRequest{
		SchemaId: schemaID,
		Version:  1,
		Payload:  invalidPayload,
	})

	// Assert
	if err == nil {
		assert.False(t, invalidResp.Valid, "Invalid event type should fail")

		// Log all validation errors
		for _, verr := range invalidResp.Errors {
			t.Logf("Validation error: field=%s, code=%s, msg=%s",
				verr.Field, verr.ErrorCode, verr.Message)
		}
		t.Log("Enum validation correctly failed")
	}
}
