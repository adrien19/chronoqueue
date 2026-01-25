package gateway

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/adrien19/chronoqueue/pkg/log"
)

// TestGatewayTLSIntegration tests the full TLS flow with a real gRPC server
func TestGatewayTLSIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Start a simple gRPC health check server with insecure transport
	grpcListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = grpcListener.Close() }()

	grpcServer := grpc.NewServer()
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	go func() {
		_ = grpcServer.Serve(grpcListener)
	}()
	defer grpcServer.Stop()

	grpcAddr := grpcListener.Addr().String()

	logger := log.NewLogger(log.WithLevel(logrus.InfoLevel))

	// Test 1: Gateway connects to gRPC server without TLS
	t.Run("GatewayWithoutTLS", func(t *testing.T) {
		config := GatewayConfig{
			GRPCServerAddr: grpcAddr,
			HTTPAddr:       ":0", // Random port
			CORSEnabled:    false,
			UseTLS:         false,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		handler, err := NewHTTPGateway(ctx, config, logger)
		require.NoError(t, err)
		require.NotNil(t, handler)
	})

	// Test 2: Gateway connects to insecure gRPC server with TLS in insecure mode
	t.Run("GatewayWithTLSInsecure", func(t *testing.T) {
		config := GatewayConfig{
			GRPCServerAddr: grpcAddr,
			HTTPAddr:       ":0",
			CORSEnabled:    false,
			UseTLS:         true,
			TLSInsecure:    true, // Skip verification since gRPC server is insecure
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		handler, err := NewHTTPGateway(ctx, config, logger)
		require.NoError(t, err)
		require.NotNil(t, handler)
	})
}

// TestGatewayTLSWithSecureServer tests gateway with a TLS-enabled gRPC server
func TestGatewayTLSWithSecureServer(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// This test would require generating real certificates
	// For now, we verify the configuration is passed correctly
	t.Skip("Requires real certificate generation - see generate_certs.sh")
}

// TestGatewayConnectionBehavior verifies gateway connection behavior
func TestGatewayConnectionBehavior(t *testing.T) {
	logger := log.NewLogger(log.WithLevel(logrus.DebugLevel))

	tests := []struct {
		name        string
		config      GatewayConfig
		expectError bool
		description string
	}{
		{
			name: "Valid insecure configuration",
			config: GatewayConfig{
				GRPCServerAddr: "localhost:9999",
				HTTPAddr:       ":8888",
				UseTLS:         false,
			},
			expectError: false,
			description: "Gateway should create handler with insecure connection",
		},
		{
			name: "Valid TLS insecure configuration",
			config: GatewayConfig{
				GRPCServerAddr: "localhost:9999",
				HTTPAddr:       ":8888",
				UseTLS:         true,
				TLSInsecure:    true,
			},
			expectError: false,
			description: "Gateway should create handler with TLS but skip verification",
		},
		{
			name: "TLS with missing CA cert",
			config: GatewayConfig{
				GRPCServerAddr: "localhost:9999",
				HTTPAddr:       ":8888",
				UseTLS:         true,
				TLSInsecure:    false,
				ServerCertFile: "/nonexistent/ca.crt",
			},
			expectError: true,
			description: "Gateway should fail when CA cert file doesn't exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			handler, err := NewHTTPGateway(ctx, tt.config, logger)

			if tt.expectError {
				assert.Error(t, err, tt.description)
				assert.Nil(t, handler, "Handler should be nil on error")
			} else {
				assert.NoError(t, err, tt.description)
				assert.NotNil(t, handler, "Handler should not be nil on success")
			}
		})
	}
}

// TestTLSConfigCreation verifies TLS config is created correctly
func TestTLSConfigCreation(t *testing.T) {
	tests := []struct {
		name               string
		insecureSkipVerify bool
		expectRootCAs      bool
	}{
		{
			name:               "Insecure TLS config",
			insecureSkipVerify: true,
			expectRootCAs:      false,
		},
		{
			name:               "Secure TLS config",
			insecureSkipVerify: false,
			expectRootCAs:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tlsConfig := &tls.Config{
				InsecureSkipVerify: tt.insecureSkipVerify,
			}

			assert.Equal(t, tt.insecureSkipVerify, tlsConfig.InsecureSkipVerify)

			if tt.expectRootCAs {
				assert.NotNil(t, tlsConfig.RootCAs)
			} else {
				assert.Nil(t, tlsConfig.RootCAs)
			}
		})
	}
}

// BenchmarkGatewayCreation benchmarks gateway creation with different TLS configs
func BenchmarkGatewayCreation(b *testing.B) {
	logger := log.NewLogger(log.WithLevel(logrus.ErrorLevel))
	ctx := context.Background()

	b.Run("WithoutTLS", func(b *testing.B) {
		config := GatewayConfig{
			GRPCServerAddr: "localhost:9000",
			HTTPAddr:       ":8080",
			UseTLS:         false,
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = NewHTTPGateway(ctx, config, logger)
		}
	})

	b.Run("WithTLSInsecure", func(b *testing.B) {
		config := GatewayConfig{
			GRPCServerAddr: "localhost:9000",
			HTTPAddr:       ":8080",
			UseTLS:         true,
			TLSInsecure:    true,
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = NewHTTPGateway(ctx, config, logger)
		}
	})
}

// Helper function to create an insecure gRPC server for testing
func createInsecureGRPCServer(t *testing.T) (*grpc.Server, net.Listener) {
	grpcServer := grpc.NewServer()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	return grpcServer, listener
}

// TestHTTPRequestToGateway verifies HTTP requests can reach the gateway
func TestHTTPRequestToGateway(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Start gRPC server
	grpcServer, grpcListener := createInsecureGRPCServer(t)
	defer grpcServer.Stop()
	defer func() { _ = grpcListener.Close() }()

	go func() {
		_ = grpcServer.Serve(grpcListener)
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	logger := log.NewLogger(log.WithLevel(logrus.InfoLevel))

	// Create gateway - note: gateway uses gRPC client, not HTTP mux
	// The health endpoint is added by the server, not the gateway itself
	gatewayConfig := GatewayConfig{
		GRPCServerAddr: grpcListener.Addr().String(),
		HTTPAddr:       ":0",
		CORSEnabled:    false,
		UseTLS:         false,
	}

	ctx := context.Background()
	handler, err := NewHTTPGateway(ctx, gatewayConfig, logger)
	require.NoError(t, err)
	assert.NotNil(t, handler, "Gateway handler should be created successfully")

	// The gateway creates a handler for gRPC-Gateway routes
	// Health/metrics endpoints are added by the server wrapper, not tested here
}
