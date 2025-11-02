package helpers

import (
	"context"
	"fmt"
	"sync"
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

var (
	sharedEnv     *TestEnvironment
	sharedEnvOnce sync.Once
	sharedEnvErr  error
)

// SharedTestEnvironment returns a shared test environment for all tests in a package.
// Containers are created once and reused across all tests. Each test should clean
// up its own data (e.g., by calling env.FlushRedis(t)).
//
// This approach significantly speeds up test execution by avoiding container
// creation overhead for each test.
//
// Example usage in test file:
//
//	func TestMain(m *testing.M) {
//	    os.Exit(helpers.RunWithSharedEnvironment(m))
//	}
//
//	func TestSomething(t *testing.T) {
//	    env := helpers.SharedTestEnvironment(t)
//	    defer env.FlushRedis(t) // Clean up after test
//
//	    // ... perform tests
//	}
func SharedTestEnvironment(t *testing.T) *TestEnvironment {
	sharedEnvOnce.Do(func() {
		sharedEnv, sharedEnvErr = createSharedEnvironment()
	})

	if sharedEnvErr != nil {
		t.Fatalf("Failed to create shared test environment: %v", sharedEnvErr)
	}

	return sharedEnv
}

// RunWithSharedEnvironment wraps m.Run() with shared environment setup and teardown.
// Use this in TestMain to manage the lifecycle of shared containers.
//
// Example:
//
//	func TestMain(m *testing.M) {
//	    os.Exit(helpers.RunWithSharedEnvironment(m))
//	}
func RunWithSharedEnvironment(m *testing.M) int {
	// Run tests
	exitCode := m.Run()

	// Cleanup shared environment
	if sharedEnv != nil {
		sharedEnv.Cleanup()
	}

	return exitCode
}

// createSharedEnvironment creates the test infrastructure once for reuse across tests.
func createSharedEnvironment() (*TestEnvironment, error) {
	ctx := context.Background()

	// Create a Docker network for container communication
	net, err := network.New(ctx,
		network.WithDriver("bridge"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker network: %w", err)
	}

	// Start Redis container using the Redis module
	redisContainer, err := redismodule.Run(ctx,
		"redis:7-alpine",
		network.WithNetwork([]string{"redis"}, net),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start Redis container: %w", err)
	}

	// Get connection string using Redis module's helper method
	connectionString, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get Redis connection string: %w", err)
	}

	// For container→container connections, use network alias
	// ChronoQueue container will connect to Redis via internal Docker network
	// (connectionString is used by test code to connect from host → Redis)
	redisInternalAddr := "redis:6379"

	// Use pre-built test image (built via `make build-test-image`)
	// This avoids rebuilding the image every time tests run
	serverReq := testcontainers.ContainerRequest{
		Image:        "chronoqueue:test-latest",
		ExposedPorts: []string{"9000/tcp", "8080/tcp"},
		Networks:     []string{net.Name},
		NetworkAliases: map[string][]string{
			net.Name: {"chronoqueue"},
		},
		Env: map[string]string{
			"SERVER_MODE":           "development",     // Use development mode for tests
			"REDIS_ADDR":            redisInternalAddr, // Use internal network address for container→Redis
			"REDIS_DB":              "0",
			"LOG_LEVEL":             "debug",
			"ENABLE_ENCRYPTION":     "false",
			"SCHEDULER_INTERVAL_MS": "300",  // Faster scheduler for tests (300ms vs 1000ms)
			"RECLAIM_INTERVAL_MS":   "2000", // Faster reclaim for tests (2s vs 5s)
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
	if err != nil {
		return nil, fmt.Errorf("failed to start ChronoQueue server container (did you run 'make build-test-image'?): %w", err)
	}

	serverHost, err := serverContainer.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get server host: %w", err)
	}

	grpcPort, err := serverContainer.MappedPort(ctx, "9000")
	if err != nil {
		return nil, fmt.Errorf("failed to get gRPC port: %w", err)
	}

	httpPort, err := serverContainer.MappedPort(ctx, "8080")
	if err != nil {
		return nil, fmt.Errorf("failed to get HTTP port: %w", err)
	}

	grpcAddr := fmt.Sprintf("%s:%s", serverHost, grpcPort.Port())
	httpAddr := fmt.Sprintf("http://%s:%s", serverHost, httpPort.Port())

	// Create Redis client using the connection string
	redisOpts, err := redis.ParseURL(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Redis connection string: %w", err)
	}

	redisClient := redis.NewClient(redisOpts)

	// Verify Redis connection
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping Redis: %w", err)
	}

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

	// Allow time for scheduler and reclaim services to initialize
	// Scheduler runs every 300ms, reclaim every 2s - wait 2.5s to ensure both have started
	time.Sleep(2500 * time.Millisecond)

	return env, nil
}

// NewGRPCClientShared creates a new gRPC client for the shared environment.
// Unlike NewGRPCClient, this does NOT auto-close the connection via t.Cleanup.
// The caller is responsible for closing the connection.
//
// Example:
//
//	func TestWithSharedEnv(t *testing.T) {
//	    env := helpers.SharedTestEnvironment(t)
//	    defer env.FlushRedis(t)
//
//	    conn := env.NewGRPCClientShared(t)
//	    defer conn.Close()
//
//	    client := queueservice_pb.NewQueueServiceClient(conn)
//	    // ... use client
//	}
func (e *TestEnvironment) NewGRPCClientShared(t *testing.T) *grpc.ClientConn {
	conn, err := grpc.NewClient(e.GRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "Failed to create gRPC client")

	return conn
}
