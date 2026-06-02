//go:build integration && sqlite
// +build integration,sqlite

package integration

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/adrien19/chronoqueue/internal/server"

	queueservicepb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	schema_pb "github.com/adrien19/chronoqueue/api/schema/v1"
)

func TestSQLiteSchemaIntegration(t *testing.T) {
	// Create temporary directory for test database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test-schema.db")

	// Configure server with SQLite
	config := server.DefaultConfig()
	config.StorageType = "sqlite"
	config.SQLiteDBPath = dbPath
	config.GRPCAddr = ":19001" // Use different port
	config.HTTPAddr = ":18081"
	config.IsDevelopment = true

	// Create and start server
	srv, err := server.New(config)
	require.NoError(t, err)

	// Start server in background
	serverDone := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		serverDone <- srv.Start(ctx)
	}()

	// Give server time to start
	time.Sleep(500 * time.Millisecond)

	// Connect to gRPC server
	conn, err := grpc.Dial(
		"localhost:19001",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := queueservicepb.NewQueueServiceClient(conn)

	// Test 1: Register a schema
	t.Run("RegisterSchema", func(t *testing.T) {
		registerReq := &queueservicepb.RegisterSchemaRequest{
			Schema: &schema_pb.Schema{
				SchemaId:    "user.profile.v1",
				Name:        "User Profile Schema",
				Description: "Schema for user profile data",
				Content: `{
					"type": "object",
					"required": ["name", "email"],
					"properties": {
						"name": {"type": "string"},
						"email": {"type": "string", "format": "email"},
						"age": {"type": "number", "minimum": 0}
					}
				}`,
				ContentType: "json-schema",
			},
		}

		resp, err := client.RegisterSchema(ctx, registerReq)
		require.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, "user.profile.v1", resp.SchemaId)
		assert.Equal(t, int32(1), resp.Version)
	})

	// Test 2: Get the registered schema
	t.Run("GetSchema", func(t *testing.T) {
		getReq := &queueservicepb.GetSchemaRequest{
			SchemaId: "user.profile.v1",
			Version:  1,
		}

		resp, err := client.GetSchema(ctx, getReq)
		require.NoError(t, err)
		assert.NotNil(t, resp.Schema)
		assert.Equal(t, "user.profile.v1", resp.Schema.SchemaId)
		assert.Equal(t, int32(1), resp.Schema.Version)
		assert.Equal(t, "User Profile Schema", resp.Schema.Name)
		assert.True(t, resp.Schema.IsActive)
	})

	// Test 3: List schemas
	t.Run("ListSchemas", func(t *testing.T) {
		listReq := &queueservicepb.ListSchemasRequest{}

		resp, err := client.ListSchemas(ctx, listReq)
		require.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Len(t, resp.Schemas, 1)
		assert.Equal(t, "user.profile.v1", resp.Schemas[0].SchemaId)
	})

	// Test 4: Validate payload against schema
	t.Run("ValidatePayload", func(t *testing.T) {
		// Valid payload
		validateReq := &queueservicepb.ValidatePayloadRequest{
			SchemaId: "user.profile.v1",
			Version:  1,
			Payload: []byte(`{
				"name": "John Doe",
				"email": "john@example.com",
				"age": 30
			}`),
		}

		resp, err := client.ValidatePayload(ctx, validateReq)
		require.NoError(t, err)
		assert.NotNil(t, resp.Result)
		assert.True(t, resp.Result.Valid)
		assert.Empty(t, resp.Result.Errors)
	})

	// Test 5: Validate invalid payload
	t.Run("ValidateInvalidPayload", func(t *testing.T) {
		// Missing required field
		validateReq := &queueservicepb.ValidatePayloadRequest{
			SchemaId: "user.profile.v1",
			Version:  1,
			Payload: []byte(`{
				"name": "John Doe"
			}`),
		}

		resp, err := client.ValidatePayload(ctx, validateReq)
		require.NoError(t, err)
		assert.NotNil(t, resp.Result)
		assert.False(t, resp.Result.Valid)
		assert.NotEmpty(t, resp.Result.Errors)
	})

	// Test 6: Create queue with schema validation
	t.Run("CreateQueueWithSchema", func(t *testing.T) {
		createReq := &queueservicepb.CreateQueueRequest{
			Name:     "validated-queue",
			Type:     "simple",
			SchemaId: "user.profile.v1",
		}

		resp, err := client.CreateQueue(ctx, createReq)
		require.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, "validated-queue", resp.Queue.Name)
	})

	// Test 7: Post message with valid payload (should succeed)
	t.Run("PostValidMessage", func(t *testing.T) {
		postReq := &queueservicepb.PostMessageRequest{
			QueueName: "validated-queue",
			MessageId: "msg-valid",
			Payload: []byte(`{
				"name": "Jane Doe",
				"email": "jane@example.com",
				"age": 25
			}`),
		}

		resp, err := client.PostMessage(ctx, postReq)
		require.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, "msg-valid", resp.MessageId)
	})

	// Test 8: Register new version of schema
	t.Run("RegisterSchemaVersion2", func(t *testing.T) {
		registerReq := &queueservicepb.RegisterSchemaRequest{
			Schema: &schema_pb.Schema{
				SchemaId:    "user.profile.v1",
				Name:        "User Profile Schema v2",
				Description: "Updated schema with phone field",
				Content: `{
					"type": "object",
					"required": ["name", "email"],
					"properties": {
						"name": {"type": "string"},
						"email": {"type": "string", "format": "email"},
						"age": {"type": "number", "minimum": 0},
						"phone": {"type": "string"}
					}
				}`,
				ContentType: "json-schema",
			},
		}

		resp, err := client.RegisterSchema(ctx, registerReq)
		require.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, int32(2), resp.Version)
	})

	// Test 9: Deactivate schema version
	t.Run("DeactivateSchema", func(t *testing.T) {
		deactivateReq := &queueservicepb.DeactivateSchemaRequest{
			SchemaId: "user.profile.v1",
			Version:  1,
		}

		_, err := client.DeactivateSchema(ctx, deactivateReq)
		require.NoError(t, err)

		// Verify it's deactivated
		getReq := &queueservicepb.GetSchemaRequest{
			SchemaId: "user.profile.v1",
			Version:  1,
		}

		resp, err := client.GetSchema(ctx, getReq)
		require.NoError(t, err)
		assert.False(t, resp.Schema.IsActive)
	})

	// Shutdown server
	cancel()
	select {
	case err := <-serverDone:
		if err != nil {
			t.Logf("Server shutdown with: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Server did not shutdown in time")
	}
}
