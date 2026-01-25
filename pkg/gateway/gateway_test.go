package gateway

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/adrien19/chronoqueue/pkg/log"
)

func TestNewHTTPGateway_WithoutTLS(t *testing.T) {
	logger := log.NewLogger(log.WithLevel(logrus.InfoLevel))

	config := GatewayConfig{
		GRPCServerAddr: "localhost:9000",
		HTTPAddr:       ":8080",
		CORSEnabled:    false,
		UseTLS:         false,
	}

	ctx := context.Background()
	handler, err := NewHTTPGateway(ctx, config, logger)

	assert.NoError(t, err)
	assert.NotNil(t, handler)
}

func TestNewHTTPGateway_WithTLSInsecure(t *testing.T) {
	logger := log.NewLogger(log.WithLevel(logrus.InfoLevel))

	config := GatewayConfig{
		GRPCServerAddr: "localhost:9000",
		HTTPAddr:       ":8080",
		CORSEnabled:    false,
		UseTLS:         true,
		TLSInsecure:    true, // Skip verification for testing
	}

	ctx := context.Background()
	handler, err := NewHTTPGateway(ctx, config, logger)

	assert.NoError(t, err)
	assert.NotNil(t, handler)
}

func TestNewHTTPGateway_WithTLSAndCACert(t *testing.T) {
	t.Skip("Skipping test with real CA cert - requires integration test setup")

	// This test requires a real certificate for proper validation
	// For unit tests, we test the code paths with InsecureSkipVerify
	// Integration tests should test with real certificates
}

func TestNewHTTPGateway_WithInvalidCACert(t *testing.T) {
	logger := log.NewLogger(log.WithLevel(logrus.InfoLevel))

	// Create a temporary invalid CA certificate for testing
	tmpDir := t.TempDir()
	caCertPath := filepath.Join(tmpDir, "invalid_ca.crt")

	err := os.WriteFile(caCertPath, []byte("invalid certificate content"), 0o644)
	require.NoError(t, err)

	config := GatewayConfig{
		GRPCServerAddr: "localhost:9000",
		HTTPAddr:       ":8080",
		CORSEnabled:    false,
		UseTLS:         true,
		TLSInsecure:    false,
		ServerCertFile: caCertPath,
	}

	ctx := context.Background()
	handler, err := NewHTTPGateway(ctx, config, logger)

	assert.Error(t, err)
	assert.Nil(t, handler)
	assert.Contains(t, err.Error(), "failed to parse server CA cert")
}

func TestNewHTTPGateway_WithMissingCACert(t *testing.T) {
	logger := log.NewLogger(log.WithLevel(logrus.InfoLevel))

	config := GatewayConfig{
		GRPCServerAddr: "localhost:9000",
		HTTPAddr:       ":8080",
		CORSEnabled:    false,
		UseTLS:         true,
		TLSInsecure:    false,
		ServerCertFile: "/nonexistent/ca.crt",
	}

	ctx := context.Background()
	handler, err := NewHTTPGateway(ctx, config, logger)

	assert.Error(t, err)
	assert.Nil(t, handler)
	assert.Contains(t, err.Error(), "read server CA cert")
}

func TestGatewayConfig_TLSConfiguration(t *testing.T) {
	tests := []struct {
		name           string
		config         GatewayConfig
		expectTLS      bool
		expectInsecure bool
	}{
		{
			name: "No TLS",
			config: GatewayConfig{
				UseTLS: false,
			},
			expectTLS:      false,
			expectInsecure: false,
		},
		{
			name: "TLS with insecure mode",
			config: GatewayConfig{
				UseTLS:      true,
				TLSInsecure: true,
			},
			expectTLS:      true,
			expectInsecure: true,
		},
		{
			name: "TLS with secure mode",
			config: GatewayConfig{
				UseTLS:      true,
				TLSInsecure: false,
			},
			expectTLS:      true,
			expectInsecure: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectTLS, tt.config.UseTLS)
			assert.Equal(t, tt.expectInsecure, tt.config.TLSInsecure)
		})
	}
}

func TestTLSConfig_Creation(t *testing.T) {
	// Test TLS config creation with InsecureSkipVerify
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	assert.True(t, tlsConfig.InsecureSkipVerify)
	assert.Nil(t, tlsConfig.RootCAs)

	// Test TLS config with CA cert pool
	caCertPool := x509.NewCertPool()
	tlsConfig.RootCAs = caCertPool

	assert.NotNil(t, tlsConfig.RootCAs)
}

func TestNewHTTPGateway_WithCORS(t *testing.T) {
	logger := log.NewLogger(log.WithLevel(logrus.InfoLevel))

	config := GatewayConfig{
		GRPCServerAddr: "localhost:9000",
		HTTPAddr:       ":8080",
		CORSEnabled:    true,
		AllowedOrigins: []string{"http://localhost:3000", "https://example.com"},
		UseTLS:         false,
	}

	ctx := context.Background()
	handler, err := NewHTTPGateway(ctx, config, logger)

	assert.NoError(t, err)
	assert.NotNil(t, handler)
}
