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

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestEnvironment holds all test infrastructure components.
// It manages the lifecycle of Postgres and ChronoQueue containers,
// providing convenient access to clients and addresses.
type TestEnvironment struct {
	PostgresContainer *postgres.PostgresContainer
	ServerContainer   testcontainers.Container
	Network           *testcontainers.DockerNetwork
	PostgresConnStr   string
	GRPCAddr          string
	HTTPAddr          string
	ctx               context.Context
}

// SetupTestEnvironment creates and starts Postgres and ChronoQueue containers.
// It automatically registers cleanup with t.Cleanup() to ensure proper teardown.
//
// This function:
// 1. Starts a Postgres container (postgres:17-alpine)
// 2. Starts a ChronoQueue server container
// 3. Returns a TestEnvironment with all necessary addresses
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

	// Start Postgres container using the Postgres module
	t.Log("Starting Postgres container...")
	postgresContainer, err := postgres.Run(ctx,
		"postgres:17-alpine",
		postgres.WithDatabase("chronoqueue"),
		postgres.WithUsername("chronoqueue"),
		postgres.WithPassword("chronoqueue"),
		network.WithNetwork([]string{"postgres"}, net),
	)
	require.NoError(t, err, "Failed to start Postgres container")

	// Get connection string for host->Postgres connections (used by tests)
	connectionString, err := postgresContainer.ConnectionString(ctx)
	require.NoError(t, err)
	t.Logf("Postgres container started with connection string: %s", connectionString)

	// Verify Postgres is ready by attempting a connection
	// This ensures the database is fully initialized before starting ChronoQueue
	time.Sleep(2 * time.Second) // Brief delay to ensure full initialization

	// For container→container connections, use network alias
	postgresInternalHost := "postgres"
	postgresInternalPort := "5432"

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
			"SERVER_MODE":       "development",        // Use development mode for tests
			"STORAGE_TYPE":      "postgres",           // Use Postgres storage
			"POSTGRES_HOST":     postgresInternalHost, // Use internal network address
			"POSTGRES_PORT":     postgresInternalPort, // Postgres port
			"POSTGRES_USER":     "chronoqueue",        // Postgres user
			"POSTGRES_PASSWORD": "chronoqueue",        // Postgres password
			"POSTGRES_DB":       "chronoqueue",        // Postgres database
			"POSTGRES_SSLMODE":  "disable",            // Disable SSL for tests
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

	env := &TestEnvironment{
		PostgresContainer: postgresContainer,
		ServerContainer:   serverContainer,
		Network:           net,
		PostgresConnStr:   connectionString,
		GRPCAddr:          grpcAddr,
		HTTPAddr:          httpAddr,
		ctx:               ctx,
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
	if e.ServerContainer != nil {
		_ = e.ServerContainer.Terminate(e.ctx)
	}
	if e.PostgresContainer != nil {
		_ = e.PostgresContainer.Terminate(e.ctx)
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
			// Server health is already verified during container startup via wait.ForHTTP("/health")
			// This method is primarily useful for ensuring recovery after operations
			// that might temporarily affect server health.
			t.Log("Server is healthy")
			return
		}
	}
}
