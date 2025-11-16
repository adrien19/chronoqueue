package integration

// Package integration provides integration tests for ChronoQueue UI.
//
// These tests validate the UI's HTTP handlers and interactions with the
// ChronoQueue server using real containers via Testcontainers.
//
// The UI server is started in-process and connects to the containerized
// ChronoQueue server (gRPC) and Redis.
//
// Prerequisites:
//   - Build the test Docker image first: make build-test-image
//   - Or run via: make test-integration
//
// Manual run:
//   go test -v -tags="test_dep integration" ./tests/integration/ -run=TestUIIntegration

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	ui "github.com/adrien19/chronoqueue/cmd/chronoq/ui"
	"github.com/adrien19/chronoqueue/pkg/log"
	"github.com/adrien19/chronoqueue/tests/helpers"
)

// startUIServer starts the UI server on a random available port and returns the URL.
// Uses a retry mechanism to handle potential port binding race conditions.
func startUIServer(t *testing.T, grpcAddr string) (string, func()) {
	logger := log.NewLogger()

	// Create UI server
	server, err := ui.NewUIServer(grpcAddr, logger)
	require.NoError(t, err, "Failed to create UI server")

	var uiURL string
	var startErr error

	// Retry server start with different ports to handle race conditions
	maxAttempts := 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Find an available port
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			startErr = fmt.Errorf("failed to find available port: %w", err)
			continue
		}

		port := listener.Addr().(*net.TCPAddr).Port
		_ = listener.Close()

		uiAddr := fmt.Sprintf("127.0.0.1:%d", port)
		uiURL = fmt.Sprintf("http://%s", uiAddr)

		// Start UI server in background goroutine
		go func() {
			_ = server.Start(uiAddr) // Ignore error as it will be http.ErrServerClosed on shutdown
		}()

		// Wait for server to be ready (also serves as port binding validation)
		ready := false
		for i := 0; i < 30; i++ {
			resp, err := http.Get(uiURL + "/health")
			if err == nil && resp.StatusCode == http.StatusOK {
				_ = resp.Body.Close()
				ready = true
				break
			}
			if err == nil {
				_ = resp.Body.Close()
			}
			time.Sleep(200 * time.Millisecond)
		}

		if ready {
			t.Logf("UI server started at %s", uiURL)
			cleanup := func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = server.Stop(ctx)
			}
			return uiURL, cleanup
		}

		// Server failed to start, likely port race - retry
		startErr = fmt.Errorf("server failed to start on port %d (attempt %d/%d)", port, attempt+1, maxAttempts)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		_ = server.Stop(ctx)
		cancel()
	}

	// All attempts failed
	require.NoError(t, startErr, "Failed to start UI server after retries")
	return "", nil // Unreachable, but satisfies compiler
}

// TestUIIntegration_Dashboard_Success validates the dashboard page renders correctly.
//
// Test Scenario: UI-001
// Expected: Dashboard loads, displays metrics, no errors
func TestUIIntegration_Dashboard_Success(t *testing.T) {
	// Note: Cannot be parallel as it starts its own UI server

	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	defer env.FlushRedis(t) // Clean up after test

	// Start UI server
	uiURL, cleanup := startUIServer(t, env.GRPCAddr)
	defer cleanup()

	// Create some test queues for the dashboard to display
	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()

	client := queueservice_pb.NewQueueServiceClient(conn)

	// Create test queues
	for i := 1; i <= 3; i++ {
		queueName := fmt.Sprintf("ui-dashboard-%d-%d", time.Now().UnixNano(), i)
		_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
			Name: queueName,
			Metadata: &queue_pb.QueueMetadata{
				Type:               queue_pb.QueueType_SIMPLE,
				DefaultMaxAttempts: 3,
				AutoCreateDlq:      true,
				LeaseDuration:      durationpb.New(30 * time.Second),
			},
		})
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond) // Small delay to ensure unique names
	}

	// Act - Make HTTP request to UI dashboard
	dashboardURL := uiURL + "/"
	resp, err := http.Get(dashboardURL)

	// Assert
	require.NoError(t, err, "Dashboard request should succeed")
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "Dashboard should return 200")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// Verify expected content in dashboard
	bodyStr := string(body)
	assert.Contains(t, bodyStr, "ChronoQueue", "Dashboard should contain ChronoQueue title")
	assert.Contains(t, bodyStr, "Dashboard", "Dashboard should contain Dashboard text")
	assert.Contains(t, bodyStr, "Total Queues", "Dashboard should display queue metrics")
}

// TestUIIntegration_QueuesList_Success validates the queues list page.
//
// Test Scenario: UI-002
// Expected: Queues page loads, displays created queues
func TestUIIntegration_QueuesList_Success(t *testing.T) {
	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	defer env.FlushRedis(t)

	uiURL, cleanup := startUIServer(t, env.GRPCAddr)
	defer cleanup()

	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()

	client := queueservice_pb.NewQueueServiceClient(conn)

	// Create a test queue
	queueName := fmt.Sprintf("ui-list-%d", time.Now().UnixNano())
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:               queue_pb.QueueType_SIMPLE,
			DefaultMaxAttempts: 3,
			AutoCreateDlq:      true,
			LeaseDuration:      durationpb.New(30 * time.Second),
		},
	})
	require.NoError(t, err)

	// Act - Make HTTP request to queues list
	queuesURL := uiURL + "/queues"
	resp, err := http.Get(queuesURL)

	// Assert
	require.NoError(t, err, "Queues list request should succeed")
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "Queues page should return 200")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	bodyStr := string(body)
	assert.Contains(t, bodyStr, "Queues", "Page should contain Queues title")
	assert.Contains(t, bodyStr, queueName, "Page should display the created queue")
}

// TestUIIntegration_QueueDetail_Success validates the queue detail page.
//
// Test Scenario: UI-003
// Expected: Queue detail page loads with correct queue information
func TestUIIntegration_QueueDetail_Success(t *testing.T) {
	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	defer env.FlushRedis(t)

	uiURL, cleanup := startUIServer(t, env.GRPCAddr)
	defer cleanup()

	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()

	client := queueservice_pb.NewQueueServiceClient(conn)

	// Create a test queue
	queueName := fmt.Sprintf("ui-detail-%d", time.Now().UnixNano())
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:               queue_pb.QueueType_SIMPLE,
			DefaultMaxAttempts: 3,
			AutoCreateDlq:      true,
			LeaseDuration:      durationpb.New(30 * time.Second),
		},
	})
	require.NoError(t, err)

	// Act - Make HTTP request to queue detail page
	detailURL := uiURL + "/queues/" + queueName
	resp, err := http.Get(detailURL)

	// Assert
	require.NoError(t, err, "Queue detail request should succeed")
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "Queue detail page should return 200")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	bodyStr := string(body)
	assert.Contains(t, bodyStr, queueName, "Page should display queue name")
	assert.Contains(t, bodyStr, "Pending", "Page should show queue state")
}

// TestUIIntegration_DashboardMetricsAPI_Success validates the metrics API endpoint.
//
// Test Scenario: UI-004
// Expected: API returns HTML component with metrics (HTMX endpoint)
func TestUIIntegration_DashboardMetricsAPI_Success(t *testing.T) {
	// Arrange
	ctx := context.Background()
	env := helpers.SharedTestEnvironment(t)
	defer env.FlushRedis(t)

	uiURL, cleanup := startUIServer(t, env.GRPCAddr)
	defer cleanup()

	conn := env.NewGRPCClientShared(t)
	defer func() { _ = conn.Close() }()

	client := queueservice_pb.NewQueueServiceClient(conn)

	// Create test queue
	queueName := fmt.Sprintf("ui-metrics-api-%d", time.Now().UnixNano())
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type:               queue_pb.QueueType_SIMPLE,
			DefaultMaxAttempts: 3,
			AutoCreateDlq:      false,
			LeaseDuration:      durationpb.New(30 * time.Second),
		},
	})
	require.NoError(t, err)

	// Act - Make API request for dashboard metrics (HTMX endpoint)
	metricsURL := uiURL + "/api/metrics/dashboard"
	resp, err := http.Get(metricsURL)

	// Assert
	require.NoError(t, err, "Metrics API request should succeed")
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "Metrics API should return 200")
	assert.Equal(t, "text/html; charset=utf-8", resp.Header.Get("Content-Type"), "Should return HTML for HTMX")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	bodyStr := string(body)
	assert.Contains(t, bodyStr, "Total Queues", "Metrics should include queue count")
	assert.Contains(t, bodyStr, "Pending", "Metrics should include pending count")
}

// TestUIIntegration_StaticAssets_Success validates static assets are served correctly.
//
// Test Scenario: UI-005
// Expected: CSS files load successfully
func TestUIIntegration_StaticAssets_Success(t *testing.T) {
	// Arrange
	env := helpers.SharedTestEnvironment(t)
	defer env.FlushRedis(t)

	uiURL, cleanup := startUIServer(t, env.GRPCAddr)
	defer cleanup()

	// Act - Request CSS file
	cssURL := uiURL + "/static/css/styles.css"
	resp, err := http.Get(cssURL)

	// Assert
	require.NoError(t, err, "Static asset request should succeed")
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "Static CSS should be served")
	// Note: Content-Type may vary, just verify it loads
}
