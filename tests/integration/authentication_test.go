package integration

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	queueservicepb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	chronoqueueclient "github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/tests/helpers"
)

func TestAuthenticationGRPC(t *testing.T) {
	env := helpers.SharedTestEnvironment(t)

	unauthenticatedConn, err := grpc.NewClient(env.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = unauthenticatedConn.Close() }()

	service := queueservicepb.NewQueueServiceClient(unauthenticatedConn)
	_, err = service.ListQueues(context.Background(), &queueservicepb.ListQueuesRequest{})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))

	client, err := chronoqueueclient.NewChronoQueueClient(env.GRPCAddr, chronoqueueclient.ClientOptions{APIKey: helpers.TestAPIKey})
	require.NoError(t, err)
	defer client.Close()
	_, err = client.ListQueues(context.Background(), "")
	assert.NoError(t, err)
}

func TestAuthenticationHTTPGateway(t *testing.T) {
	env := helpers.SharedTestEnvironment(t)
	tests := []struct {
		name       string
		headerName string
		header     string
		wantStatus int
	}{
		{name: "missing", wantStatus: http.StatusUnauthorized},
		{name: "invalid", headerName: "api-key", header: "invalid", wantStatus: http.StatusUnauthorized},
		{name: "API key", headerName: "api-key", header: helpers.TestAPIKey, wantStatus: http.StatusOK},
		{name: "bearer", headerName: "Authorization", header: "Bearer " + helpers.TestAPIKey, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, env.HTTPAddr+"/v1/queues", nil)
			require.NoError(t, err)
			if tt.headerName != "" {
				req.Header.Set(tt.headerName, tt.header)
			}

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}
}
