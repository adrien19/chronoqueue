//go:build integration && sqlite && cgo
// +build integration,sqlite,cgo

package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservicepb "github.com/adrien19/chronoqueue/api/queueservice/v1"
)

func TestSQLiteSchemaIntegration(t *testing.T) {
	h := startSQLiteServer(t, "test-schema.db")
	defer h.shutdown(t)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(h.grpcTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			t.Errorf("failed to close grpc connection: %v", closeErr)
		}
	}()

	client := queueservicepb.NewQueueServiceClient(conn)
	queueName := "validated-queue"

	t.Run("RegisterSchema", func(t *testing.T) {
		resp, err := client.RegisterSchema(ctx, &queueservicepb.RegisterSchemaRequest{
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
		})
		require.NoError(t, err)
		assert.Equal(t, "user.profile.v1", resp.GetSchemaId())
		assert.Equal(t, int32(1), resp.GetVersion())
	})

	t.Run("GetSchema", func(t *testing.T) {
		resp, err := client.GetSchema(ctx, &queueservicepb.GetSchemaRequest{
			SchemaId: "user.profile.v1",
			Version:  1,
		})
		require.NoError(t, err)
		require.NotNil(t, resp.GetSchema())
		assert.Equal(t, "user.profile.v1", resp.GetSchema().GetSchemaId())
		assert.Equal(t, int32(1), resp.GetSchema().GetVersion())
		assert.Equal(t, "User Profile Schema", resp.GetSchema().GetName())
		assert.True(t, resp.GetSchema().GetIsActive())
	})

	t.Run("ListSchemas", func(t *testing.T) {
		resp, err := client.ListSchemas(ctx, &queueservicepb.ListSchemasRequest{})
		require.NoError(t, err)
		require.NotEmpty(t, resp.GetSchemas())
		assert.Equal(t, "user.profile.v1", resp.GetSchemas()[0].GetSchemaId())
	})

	t.Run("ValidatePayload", func(t *testing.T) {
		resp, err := client.ValidatePayload(ctx, &queueservicepb.ValidatePayloadRequest{
			SchemaId: "user.profile.v1",
			Version:  1,
			Payload: `{
				"name": "John Doe",
				"email": "john@example.com",
				"age": 30
			}`,
		})
		require.NoError(t, err)
		assert.True(t, resp.GetValid())
		assert.Empty(t, resp.GetErrors())
	})

	t.Run("ValidateInvalidPayload", func(t *testing.T) {
		resp, err := client.ValidatePayload(ctx, &queueservicepb.ValidatePayloadRequest{
			SchemaId: "user.profile.v1",
			Version:  1,
			Payload: `{
				"name": "John Doe"
			}`,
		})
		require.NoError(t, err)
		assert.False(t, resp.GetValid())
		assert.NotEmpty(t, resp.GetErrors())
	})

	t.Run("CreateQueueWithSchema", func(t *testing.T) {
		resp, err := client.CreateQueue(ctx, &queueservicepb.CreateQueueRequest{
			Name: queueName,
			Metadata: &queuepb.QueueMetadata{
				Type:           queuepb.QueueType_SIMPLE,
				SchemaId:       "user.profile.v1",
				SchemaRequired: true,
			},
		})
		require.NoError(t, err)
		assert.True(t, resp.GetSuccess())
	})

	t.Run("PostValidMessage", func(t *testing.T) {
		payloadData := mustJSONStruct(t, map[string]interface{}{
			"name":  "Jane Doe",
			"email": "jane@example.com",
			"age":   25,
		})

		resp, err := client.PostMessage(ctx, &queueservicepb.PostMessageRequest{
			QueueName: queueName,
			Message: &messagepb.Message{
				MessageId: "msg-valid",
				Metadata: &messagepb.Message_Metadata{
					Payload: &commonpb.Payload{
						Data:        payloadData,
						ContentType: "application/json",
					},
					Priority:    1,
					MaxAttempts: 1,
				},
			},
		})
		require.NoError(t, err)
		assert.True(t, resp.GetSuccess())
	})

	t.Run("PostInvalidMessageFailsValidation", func(t *testing.T) {
		payloadData := mustJSONStruct(t, map[string]interface{}{
			"name": "Only Name",
		})

		_, err := client.PostMessage(ctx, &queueservicepb.PostMessageRequest{
			QueueName: queueName,
			Message: &messagepb.Message{
				MessageId: "msg-invalid",
				Metadata: &messagepb.Message_Metadata{
					Payload: &commonpb.Payload{
						Data:        payloadData,
						ContentType: "application/json",
					},
					Priority:    1,
					MaxAttempts: 1,
				},
			},
		})
		require.Error(t, err)
	})

	t.Run("RegisterSchemaVersion2", func(t *testing.T) {
		resp, err := client.RegisterSchema(ctx, &queueservicepb.RegisterSchemaRequest{
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
		})
		require.NoError(t, err)
		assert.Equal(t, int32(2), resp.GetVersion())
	})

	t.Run("DeleteSchemaVersion", func(t *testing.T) {
		resp, err := client.DeleteSchema(ctx, &queueservicepb.DeleteSchemaRequest{
			SchemaId: "user.profile.v1",
			Version:  1,
		})
		require.NoError(t, err)
		assert.True(t, resp.GetSuccess())
		assert.Equal(t, int32(1), resp.GetVersionsDeleted())

		schemaResp, err := client.GetSchema(ctx, &queueservicepb.GetSchemaRequest{
			SchemaId: "user.profile.v1",
			Version:  1,
		})
		require.NoError(t, err)
		require.NotNil(t, schemaResp.GetSchema())
		assert.False(t, schemaResp.GetSchema().GetIsActive())

		activeOnlyResp, err := client.ListSchemas(ctx, &queueservicepb.ListSchemasRequest{
			ActiveOnly: true,
		})
		require.NoError(t, err)
		foundActiveV2 := false
		for _, s := range activeOnlyResp.GetSchemas() {
			if s.GetSchemaId() == "user.profile.v1" && s.GetLatestVersion() == 2 {
				foundActiveV2 = true
			}
		}
		assert.True(t, foundActiveV2, "schema user.profile.v1 should remain active via version 2")
	})
}

func mustJSONStruct(t *testing.T, v map[string]interface{}) *structpb.Struct {
	t.Helper()

	b, err := json.Marshal(v)
	require.NoError(t, err)

	var asMap map[string]interface{}
	err = json.Unmarshal(b, &asMap)
	require.NoError(t, err)

	s, err := structpb.NewStruct(asMap)
	require.NoError(t, err)
	return s
}
