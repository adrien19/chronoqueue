package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservicepb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/tests/helpers"
)

func TestQueueConfigurationValidation(t *testing.T) {
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { require.NoError(t, conn.Close()) }()
	client := queueservicepb.NewQueueServiceClient(conn)

	tests := []struct {
		name      string
		queueName string
		metadata  *queuepb.QueueMetadata
	}{
		{name: "invalid name", queueName: "invalid/name"},
		{name: "invalid type", metadata: &queuepb.QueueMetadata{Type: 99}},
		{name: "invalid attempts", metadata: &queuepb.QueueMetadata{DefaultMaxAttempts: -2}},
		{name: "invalid legacy lease", metadata: &queuepb.QueueMetadata{LeaseDuration: durationpb.New(0)}},
		{name: "invalid lease policy", metadata: &queuepb.QueueMetadata{LeasePolicy: &commonpb.LeasePolicy{BaseLease: durationpb.New(-time.Second)}}},
		{name: "conflicting leases", metadata: &queuepb.QueueMetadata{LeaseDuration: durationpb.New(time.Minute), LeasePolicy: &commonpb.LeasePolicy{BaseLease: durationpb.New(30 * time.Second)}}},
		{name: "exclusive key required", metadata: &queuepb.QueueMetadata{Type: queuepb.QueueType_EXCLUSIVE}},
		{name: "invalid payload size", metadata: &queuepb.QueueMetadata{MaxPayloadSize: -1}},
		{name: "required schema missing", metadata: &queuepb.QueueMetadata{SchemaRequired: true}},
		{name: "invalid retention", metadata: &queuepb.QueueMetadata{MessageRetentionPolicy: &queuepb.MessageRetentionPolicy{Mode: queuepb.MessageRetentionPolicy_RETAIN_DURATION}}},
		{name: "invalid priority weight", metadata: &queuepb.QueueMetadata{PriorityConfig: &queuepb.PriorityConfig{Policy: queuepb.FairnessPolicy_WEIGHTED, PriorityWeights: map[int32]int32{4: 0}}}},
		{name: "invalid content type", metadata: &queuepb.QueueMetadata{AllowedContentTypes: []string{"invalid"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queueName := tt.queueName
			if queueName == "" {
				queueName = helpers.GenerateUniqueQueueName(t, "invalid-config")
			}
			_, err := client.CreateQueue(context.Background(), &queueservicepb.CreateQueueRequest{Name: queueName, Metadata: tt.metadata})
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestMessageConfigurationValidation(t *testing.T) {
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	conn := env.NewGRPCClientShared(t)
	defer func() { require.NoError(t, conn.Close()) }()
	client := queueservicepb.NewQueueServiceClient(conn)
	queueName := helpers.GenerateUniqueQueueName(t, "message-validation")
	_, err := client.CreateQueue(ctx, &queueservicepb.CreateQueueRequest{Name: queueName})
	require.NoError(t, err)

	tests := []struct {
		name     string
		metadata *messagepb.Message_Metadata
	}{
		{name: "priority", metadata: &messagepb.Message_Metadata{Priority: 5}},
		{name: "max attempts", metadata: &messagepb.Message_Metadata{MaxAttempts: -2}},
		{name: "lease policy", metadata: &messagepb.Message_Metadata{LeasePolicy: &commonpb.LeasePolicy{BaseLease: durationpb.New(0)}}},
		{name: "scheduled time", metadata: &messagepb.Message_Metadata{ScheduledTime: &timestamppb.Timestamp{Seconds: 253402300800}}},
		{name: "server managed state", metadata: &messagepb.Message_Metadata{State: messagepb.Message_Metadata_RUNNING}},
		{name: "server managed lease expiry", metadata: &messagepb.Message_Metadata{LeaseExpiry: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.metadata.Payload = &commonpb.Payload{ContentType: "application/json"}
			_, err := client.PostMessage(ctx, &queueservicepb.PostMessageRequest{
				QueueName: queueName,
				Message:   &messagepb.Message{MessageId: helpers.GenerateUniqueMessageID(t), Metadata: tt.metadata},
			})
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestHTTPValidationErrorContract(t *testing.T) {
	env := helpers.SharedTestEnvironment(t)
	body := bytes.NewBufferString(`{"name":"invalid/name"}`)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, env.HTTPAddr+"/v1/queues", body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("api-key", helpers.TestAPIKey)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var responseBody struct {
		Code    int32  `json:"code"`
		Message string `json:"message"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&responseBody))
	assert.Equal(t, int32(codes.InvalidArgument), responseBody.Code)
	assert.Contains(t, responseBody.Message, "queue name")
	assert.NotContains(t, responseBody.Message, "insert queue")
}
