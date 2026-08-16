//go:build integration
// +build integration

package integration

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/tests/helpers"
)

// TestGRPCWithMTLS verifies that gRPC server accepts connections with valid client certificates
// and rejects connections without client certificates when mTLS is enabled.
func TestGRPCWithMTLS(t *testing.T) {
	t.Parallel()

	// Generate test certificates
	certs := helpers.GenerateTestCertificates(t)

	// Setup test environment with TLS
	env := helpers.SetupTestEnvironmentWithTLS(t, certs)

	t.Run("ValidClientCertificate", func(t *testing.T) {
		// Create TLS credentials with client certificate
		tlsConfig := certs.LoadClientTLSConfig(t)
		creds := credentials.NewTLS(tlsConfig)

		// Create gRPC client with TLS
		conn, err := grpc.NewClient(
			env.GRPCAddr,
			grpc.WithTransportCredentials(creds),
		)
		require.NoError(t, err, "Failed to create gRPC client with TLS")
		defer conn.Close()

		// Create queue service client
		client := queueservice_pb.NewQueueServiceClient(conn)

		// Test creating a queue - should succeed with valid certificate
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		queueName := fmt.Sprintf("test-mtls-queue-%d", time.Now().UnixNano())
		createResp, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
			Name: queueName,
			Metadata: &queue_pb.QueueMetadata{
				Type: queue_pb.QueueType_SIMPLE,
			},
		})

		require.NoError(t, err, "CreateQueue should succeed with valid client certificate")
		assert.NotNil(t, createResp)
		assert.True(t, createResp.Success)

		t.Logf("Successfully created queue with mTLS: %s", queueName)
	})

	t.Run("NoClientCertificate", func(t *testing.T) {
		// Create TLS credentials WITHOUT client certificate
		// This should fail because server requires mTLS
		caPool := x509.NewCertPool()
		caPEM, err := os.ReadFile(certs.CACert)
		require.NoError(t, err)
		caPool.AppendCertsFromPEM(caPEM)

		tlsConfig := &tls.Config{
			RootCAs:    caPool,
			ServerName: "localhost",
			// Note: No Certificates field - no client cert
		}
		creds := credentials.NewTLS(tlsConfig)

		// Create gRPC client with TLS but no client cert
		conn, err := grpc.NewClient(
			env.GRPCAddr,
			grpc.WithTransportCredentials(creds),
		)
		require.NoError(t, err, "Failed to create gRPC client")
		defer conn.Close()

		// Create queue service client
		client := queueservice_pb.NewQueueServiceClient(conn)

		// Test creating a queue - should fail without client certificate
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		queueName := fmt.Sprintf("test-mtls-queue-fail-%d", time.Now().UnixNano())
		_, err = client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
			Name: queueName,
			Metadata: &queue_pb.QueueMetadata{
				Type: queue_pb.QueueType_SIMPLE,
			},
		})

		// We expect this to fail due to missing client certificate
		// Note: The exact error might vary, but it should not succeed
		assert.Error(t, err, "CreateQueue should fail without client certificate")
		t.Logf("Expected error without client cert: %v", err)
	})

	t.Run("InsecureConnection", func(t *testing.T) {
		// Attempt to connect without TLS - should fail
		conn, err := grpc.NewClient(
			env.GRPCAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err, "Failed to create insecure gRPC client")
		defer conn.Close()

		client := queueservice_pb.NewQueueServiceClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		queueName := fmt.Sprintf("test-insecure-fail-%d", time.Now().UnixNano())
		_, err = client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
			Name: queueName,
			Metadata: &queue_pb.QueueMetadata{
				Type: queue_pb.QueueType_SIMPLE,
			},
		})

		// Should fail because server requires TLS
		assert.Error(t, err, "CreateQueue should fail with insecure connection to TLS server")
		t.Logf("Expected error with insecure connection: %v", err)
	})
}

// TestHTTPGatewayWithTLS verifies TLS on the public HTTP gateway and its connection
// to the gRPC backend.
func TestHTTPGatewayWithTLS(t *testing.T) {
	t.Parallel()

	// Generate test certificates
	certs := helpers.GenerateTestCertificates(t)

	// Setup test environment with TLS
	env := helpers.SetupTestEnvironmentWithTLS(t, certs)

	t.Run("HTTPGatewayAccess", func(t *testing.T) {
		httpClient := &http.Client{
			Transport: &http.Transport{TLSClientConfig: certs.LoadClientTLSConfig(t)},
			Timeout:   10 * time.Second,
		}

		resp, err := httpClient.Get(fmt.Sprintf("%s/v1/queues", env.HTTPAddr))
		require.NoError(t, err, "Failed to access proxied endpoint over HTTPS")
		defer func() { assert.NoError(t, resp.Body.Close()) }()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Proxied endpoint should return 200 OK")
		require.NotNil(t, resp.TLS)
		assert.GreaterOrEqual(t, resp.TLS.Version, uint16(tls.VersionTLS12))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "Failed to read response body")
		t.Logf("Proxied endpoint response: %s", string(body))

		legacyTLSConfig := certs.LoadClientTLSConfig(t)
		legacyTLSConfig.MinVersion = tls.VersionTLS10
		legacyTLSConfig.MaxVersion = tls.VersionTLS11
		legacyClient := &http.Client{
			Transport: &http.Transport{TLSClientConfig: legacyTLSConfig},
			Timeout:   10 * time.Second,
		}
		legacyResp, err := legacyClient.Get(fmt.Sprintf("%s/v1/queues", env.HTTPAddr))
		if legacyResp != nil {
			defer func() { assert.NoError(t, legacyResp.Body.Close()) }()
		}
		assert.Error(t, err, "TLS 1.1 and older should be rejected")
	})
}

// TestGatewayProxiedRequestWithMTLS verifies that the HTTP gateway presents its
// client certificate when proxying to the mTLS-protected gRPC server.
func TestGatewayProxiedRequestWithMTLS(t *testing.T) {
	t.Parallel()

	// Generate test certificates
	certs := helpers.GenerateTestCertificates(t)

	// Setup test environment with TLS
	env := helpers.SetupTestEnvironmentWithTLS(t, certs)

	t.Run("GatewayUsesInternalTLS", func(t *testing.T) {
		httpClient := &http.Client{
			Transport: &http.Transport{TLSClientConfig: certs.LoadClientTLSConfig(t)},
			Timeout:   10 * time.Second,
		}

		queueName := fmt.Sprintf("gateway-mtls-%d", time.Now().UnixNano())
		requestBody := fmt.Sprintf(`{"name":%q,"metadata":{"type":"SIMPLE"}}`, queueName)
		resp, err := httpClient.Post(
			fmt.Sprintf("%s/v1/queues", env.HTTPAddr),
			"application/json",
			bytes.NewBufferString(requestBody),
		)
		require.NoError(t, err, "Failed to create queue through HTTP gateway")
		defer func() { assert.NoError(t, resp.Body.Close()) }()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Queue creation should return 200 OK")
		var result struct {
			Success bool `json:"success"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		assert.True(t, result.Success)
	})
}

func TestGatewayRejectsUntrustedClientCertificate(t *testing.T) {
	t.Parallel()

	serverCerts := helpers.GenerateTestCertificates(t)
	untrustedGatewayCerts := helpers.GenerateTestCertificates(t)
	env := helpers.SetupTestEnvironmentWithTLSGatewayCredentials(
		t,
		serverCerts,
		untrustedGatewayCerts.ClientCert,
		untrustedGatewayCerts.ClientKey,
	)

	httpClient := &http.Client{
		Transport: &http.Transport{TLSClientConfig: serverCerts.LoadClientTLSConfig(t)},
		Timeout:   10 * time.Second,
	}
	resp, err := httpClient.Get(fmt.Sprintf("%s/v1/queues", env.HTTPAddr))
	require.NoError(t, err, "Public HTTPS connection should succeed")
	defer func() { assert.NoError(t, resp.Body.Close()) }()

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestGatewayRejectsUntrustedServerCertificate(t *testing.T) {
	t.Parallel()

	serverCerts := helpers.GenerateTestCertificates(t)
	gatewayCerts := helpers.GenerateTestCertificates(t)
	env := helpers.SetupTestEnvironmentWithTLSGatewayCertificates(
		t,
		serverCerts,
		gatewayCerts.CACert,
		serverCerts.ClientCert,
		serverCerts.ClientKey,
	)

	httpClient := &http.Client{
		Transport: &http.Transport{TLSClientConfig: serverCerts.LoadClientTLSConfig(t)},
		Timeout:   10 * time.Second,
	}
	resp, err := httpClient.Get(fmt.Sprintf("%s/v1/queues", env.HTTPAddr))
	require.NoError(t, err, "Public HTTPS connection should succeed")
	defer func() { assert.NoError(t, resp.Body.Close()) }()

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// TestTLSCertificateValidation verifies proper certificate validation behavior.
func TestTLSCertificateValidation(t *testing.T) {
	t.Parallel()

	// Generate test certificates
	certs := helpers.GenerateTestCertificates(t)

	// Setup test environment with TLS
	env := helpers.SetupTestEnvironmentWithTLS(t, certs)

	t.Run("ValidServerCertificate", func(t *testing.T) {
		// Create TLS config with proper CA trust
		tlsConfig := certs.LoadClientTLSConfig(t)
		creds := credentials.NewTLS(tlsConfig)

		// Connect with proper certificate validation
		conn, err := grpc.NewClient(
			env.GRPCAddr,
			grpc.WithTransportCredentials(creds),
		)
		require.NoError(t, err, "Should connect with valid server certificate")
		defer conn.Close()

		// Verify we can make a successful call
		client := queueservice_pb.NewQueueServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err = client.ListQueues(ctx, &queueservice_pb.ListQueuesRequest{})
		require.NoError(t, err, "ListQueues should succeed with valid certificate")
	})

	t.Run("WrongServerName", func(t *testing.T) {
		// Create TLS config with wrong server name
		tlsConfig := certs.LoadClientTLSConfig(t)
		tlsConfig.ServerName = "wrong.example.com" // Wrong server name
		creds := credentials.NewTLS(tlsConfig)

		// Attempt to connect with wrong server name
		conn, err := grpc.NewClient(
			env.GRPCAddr,
			grpc.WithTransportCredentials(creds),
		)
		require.NoError(t, err, "Client creation succeeds, connection happens on first RPC")
		defer conn.Close()

		// Make a call - this should fail due to server name mismatch
		client := queueservice_pb.NewQueueServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err = client.ListQueues(ctx, &queueservice_pb.ListQueuesRequest{})
		assert.Error(t, err, "Should fail with wrong server name")
		t.Logf("Expected error with wrong server name: %v", err)
	})
}
