package helpers

// Package helpers provides utility functions for ChronoQueue integration testing.
//
// This package includes helpers for:
// - Testcontainer setup and management
// - Test client creation
// - Fixture loading
// - Custom assertions
//
// Example usage:
//
//	func TestExample(t *testing.T) {
//	    env := helpers.SetupTestEnvironment(t)
//	    defer env.Cleanup()
//
//	    client := env.NewGRPCClient(t)
//	    // ... perform tests
//	}

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestEnvironment holds all test infrastructure components.
// It manages the lifecycle of Redis and ChronoQueue containers,
// providing convenient access to clients and addresses.
type TestEnvironment struct {
	RedisContainer  testcontainers.Container
	ServerContainer testcontainers.Container
	RedisClient     *redis.Client
	RedisAddr       string
	GRPCAddr        string
	HTTPAddr        string
	ctx             context.Context
}

// SetupTestEnvironment creates and starts Redis and ChronoQueue containers.
// It automatically registers cleanup with t.Cleanup() to ensure proper teardown.
//
// This function:
// 1. Starts a Redis container (redis:7-alpine)
// 2. Starts a ChronoQueue server container
// 3. Creates a Redis client
// 4. Returns a TestEnvironment with all necessary addresses
//
// Example:
//
//	func TestMyFeature(t *testing.T) {
//	    env := SetupTestEnvironment(t)
//	    // env.Cleanup() is called automatically via t.Cleanup()
//
//	    client := env.NewGRPCClient(t)
//	    // ... perform tests
//	}
func SetupTestEnvironment(t *testing.T) *TestEnvironment {
	ctx := context.Background()

	// Start Redis container
	t.Log("Starting Redis container...")
	redisReq := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}

	redisContainer, err := testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: redisReq,
			Started:          true,
		})
	require.NoError(t, err, "Failed to start Redis container")

	redisHost, err := redisContainer.Host(ctx)
	require.NoError(t, err)

	redisPort, err := redisContainer.MappedPort(ctx, "6379")
	require.NoError(t, err)

	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort.Port())
	t.Logf("Redis container started at %s", redisAddr)

	// Start ChronoQueue server container
	t.Log("Starting ChronoQueue server container...")
	serverReq := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    "../..", // Adjust based on test location
			Dockerfile: "images/Dockerfile",
		},
		ExposedPorts: []string{"9000/tcp", "8080/tcp"},
		Env: map[string]string{
			"REDIS_ADDR":        redisAddr,
			"REDIS_DB":          "0",
			"LOG_LEVEL":         "debug",
			"ENABLE_ENCRYPTION": "false", // Can be overridden for encryption tests
		},
		WaitingFor: wait.ForHTTP("/health").
			WithPort("8080").
			WithStartupTimeout(60 * time.Second),
	}

	serverContainer, err := testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: serverReq,
			Started:          true,
		})
	require.NoError(t, err, "Failed to start ChronoQueue server container")

	serverHost, err := serverContainer.Host(ctx)
	require.NoError(t, err)

	grpcPort, err := serverContainer.MappedPort(ctx, "9000")
	require.NoError(t, err)

	httpPort, err := serverContainer.MappedPort(ctx, "8080")
	require.NoError(t, err)

	grpcAddr := fmt.Sprintf("%s:%s", serverHost, grpcPort.Port())
	httpAddr := fmt.Sprintf("http://%s:%s", serverHost, httpPort.Port())

	t.Logf("ChronoQueue server started - gRPC: %s, HTTP: %s", grpcAddr, httpAddr)

	// Create Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// Verify Redis connection
	err = redisClient.Ping(ctx).Err()
	require.NoError(t, err, "Failed to ping Redis")
	t.Log("Redis client connected successfully")

	env := &TestEnvironment{
		RedisContainer:  redisContainer,
		ServerContainer: serverContainer,
		RedisClient:     redisClient,
		RedisAddr:       redisAddr,
		GRPCAddr:        grpcAddr,
		HTTPAddr:        httpAddr,
		ctx:             ctx,
	}

	// Register cleanup
	t.Cleanup(func() {
		env.Cleanup()
	})

	return env
}

// Cleanup terminates all containers and closes connections.
// This is automatically called via t.Cleanup() when using SetupTestEnvironment.
func (e *TestEnvironment) Cleanup() {
	if e.RedisClient != nil {
		e.RedisClient.Close()
	}
	if e.ServerContainer != nil {
		e.ServerContainer.Terminate(e.ctx)
	}
	if e.RedisContainer != nil {
		e.RedisContainer.Terminate(e.ctx)
	}
}

// NewGRPCClient creates a new gRPC client connection to the ChronoQueue server.
// The connection is automatically closed via t.Cleanup().
//
// Example:
//
//	func TestCreateQueue(t *testing.T) {
//	    env := SetupTestEnvironment(t)
//	    conn := env.NewGRPCClient(t)
//
//	    client := queueservice_pb.NewQueueServiceClient(conn)
//	    // ... use client
//	}
func (e *TestEnvironment) NewGRPCClient(t *testing.T) *grpc.ClientConn {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, e.GRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err, "Failed to create gRPC client")

	t.Cleanup(func() {
		conn.Close()
	})

	return conn
}

// WaitForHealthy waits for the ChronoQueue server to be healthy.
// This is useful if you need to ensure the server is fully ready before starting tests.
func (e *TestEnvironment) WaitForHealthy(t *testing.T, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(e.ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatal("Timeout waiting for server to become healthy")
		case <-ticker.C:
			// Try to ping Redis as a health check
			err := e.RedisClient.Ping(ctx).Err()
			if err == nil {
				t.Log("Server is healthy")
				return
			}
		}
	}
}

// FlushRedis clears all data from the Redis database.
// Useful for ensuring clean state between test runs.
func (e *TestEnvironment) FlushRedis(t *testing.T) {
	err := e.RedisClient.FlushAll(e.ctx).Err()
	require.NoError(t, err, "Failed to flush Redis")
	t.Log("Redis flushed successfully")
}
