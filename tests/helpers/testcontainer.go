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
	redismodule "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestEnvironment holds all test infrastructure components.
// It manages the lifecycle of Redis and ChronoQueue containers,
// providing convenient access to clients and addresses.
type TestEnvironment struct {
	RedisContainer  *redismodule.RedisContainer
	ServerContainer testcontainers.Container
	Network         *testcontainers.DockerNetwork
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

	// Create a Docker network for container communication
	net, err := network.New(ctx,
		network.WithDriver("bridge"),
	)
	require.NoError(t, err, "Failed to create Docker network")

	// Start Redis container using the Redis module
	t.Log("Starting Redis container...")
	redisContainer, err := redismodule.Run(ctx,
		"redis:7-alpine",
		network.WithNetwork([]string{"redis"}, net),
	)
	require.NoError(t, err, "Failed to start Redis container")

	// Get connection string using Redis module's helper method
	connectionString, err := redisContainer.ConnectionString(ctx)
	require.NoError(t, err)
	t.Logf("Redis container started at %s", connectionString)

	// Use redis:6379 for container-to-container communication
	redisInternalAddr := "redis:6379"

	// Start ChronoQueue server container
	t.Log("Starting ChronoQueue server container...")
	serverReq := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    "../..", // Adjust based on test location
			Dockerfile: "images/Dockerfile",
		},
		ExposedPorts: []string{"9000/tcp", "8080/tcp"},
		Networks:     []string{net.Name},
		Env: map[string]string{
			"SERVER_MODE":       "development",     // Use development mode for tests
			"REDIS_ADDR":        redisInternalAddr, // Use internal network address
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

	// Create Redis client using the connection string
	redisOpts, err := redis.ParseURL(connectionString)
	require.NoError(t, err, "Failed to parse Redis connection string")

	redisClient := redis.NewClient(redisOpts)

	// Verify Redis connection
	err = redisClient.Ping(ctx).Err()
	require.NoError(t, err, "Failed to ping Redis")
	t.Log("Redis client connected successfully")

	env := &TestEnvironment{
		RedisContainer:  redisContainer,
		ServerContainer: serverContainer,
		Network:         net,
		RedisClient:     redisClient,
		RedisAddr:       connectionString,
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
		_ = e.RedisClient.Close()
	}
	if e.ServerContainer != nil {
		_ = e.ServerContainer.Terminate(e.ctx)
	}
	if e.RedisContainer != nil {
		_ = e.RedisContainer.Terminate(e.ctx)
	}
	if e.Network != nil {
		_ = e.Network.Remove(e.ctx)
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
	conn, err := grpc.NewClient(e.GRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "Failed to create gRPC client")

	t.Cleanup(func() {
		_ = conn.Close()
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
