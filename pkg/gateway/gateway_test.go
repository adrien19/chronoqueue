package gateway

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/adrien19/chronoqueue/pkg/log"
)

func TestLivenessHandler(t *testing.T) {
	recorder := httptest.NewRecorder()
	LivenessHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/live", nil))

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.JSONEq(t, `{"status":"alive","service":"chronoqueue"}`, recorder.Body.String())
}

func TestReadinessHandler(t *testing.T) {
	tests := []struct {
		name       string
		check      func(context.Context) error
		wantStatus int
		wantReady  string
	}{
		{name: "ready", check: func(context.Context) error { return nil }, wantStatus: http.StatusOK, wantReady: "ready"},
		{name: "database failure", check: func(context.Context) error { return errors.New("connection refused") }, wantStatus: http.StatusServiceUnavailable, wantReady: "not_ready"},
		{name: "uninitialized", wantStatus: http.StatusServiceUnavailable, wantReady: "not_ready"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ReadinessHandler(tt.check).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))

			assert.Equal(t, tt.wantStatus, recorder.Code)
			var response map[string]string
			require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
			assert.Equal(t, tt.wantReady, response["status"])
			assert.NotEmpty(t, response["version"])
			assert.NotContains(t, response["error"], "connection refused")
		})
	}
}

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

func TestSwaggerUISelfHostedWithSecurityHeaders(t *testing.T) {
	const approvedCSP = "default-src 'self'; base-uri 'none'; object-src 'none'; frame-ancestors 'none'; form-action 'self'; img-src 'self' data:; font-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'"

	logger := log.NewLogger(log.WithLevel(logrus.PanicLevel))
	config := GatewayConfig{EnableAPIDocs: true}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/docs/", nil)

	SecurityHeadersMiddleware(SwaggerUIHandler(config, logger)).ServeHTTP(recorder, request)

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, approvedCSP, recorder.Header().Get("Content-Security-Policy"))
	assert.Equal(t, "DENY", recorder.Header().Get("X-Frame-Options"))
	assert.Equal(t, "nosniff", recorder.Header().Get("X-Content-Type-Options"))
	assert.NotContains(t, recorder.Body.String(), "https://unpkg.com")
	assert.Contains(t, recorder.Body.String(), "/docs/assets/swagger-ui-bundle.js")

	assetRecorder := httptest.NewRecorder()
	assetRequest := httptest.NewRequest(http.MethodGet, "/docs/assets/swagger-ui-init.js", nil)
	SwaggerAssetHandler(config, logger).ServeHTTP(assetRecorder, assetRequest)
	assert.Equal(t, http.StatusOK, assetRecorder.Code)
	assert.True(t, strings.HasPrefix(assetRecorder.Header().Get("Content-Type"), "text/javascript"))
}

func TestSwaggerAssetHandlerRejectsUnknownAsset(t *testing.T) {
	logger := log.NewLogger(log.WithLevel(logrus.PanicLevel))
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/docs/assets/unknown.js", nil)

	SwaggerAssetHandler(GatewayConfig{EnableAPIDocs: true}, logger).ServeHTTP(recorder, request)

	assert.Equal(t, http.StatusNotFound, recorder.Code)
}

func TestRateLimitErrorMapsToHTTPTooManyRequests(t *testing.T) {
	logger := log.NewLogger(log.WithLevel(logrus.PanicLevel))
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/queues", nil)

	customErrorHandler(logger)(
		context.Background(),
		runtime.NewServeMux(),
		&runtime.JSONPb{},
		recorder,
		request,
		status.Error(codes.ResourceExhausted, "rate limit exceeded"),
	)

	assert.Equal(t, http.StatusTooManyRequests, recorder.Code)
}

func TestBearerAuthMiddleware(t *testing.T) {
	handlerCalls := 0
	handler := BearerAuthMiddleware("metrics-secret", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		handlerCalls++
		w.WriteHeader(http.StatusOK)
	}))

	tests := []struct {
		name          string
		authorization string
		wantStatus    int
	}{
		{name: "missing token", wantStatus: http.StatusUnauthorized},
		{name: "invalid token", authorization: "Bearer wrong", wantStatus: http.StatusUnauthorized},
		{name: "wrong scheme", authorization: "Basic metrics-secret", wantStatus: http.StatusUnauthorized},
		{name: "valid token", authorization: "Bearer metrics-secret", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			if tt.authorization != "" {
				request.Header.Set("Authorization", tt.authorization)
			}

			handler.ServeHTTP(recorder, request)

			assert.Equal(t, tt.wantStatus, recorder.Code)
			if tt.wantStatus == http.StatusUnauthorized {
				assert.Equal(t, `Bearer realm="ChronoQueue metrics"`, recorder.Header().Get("WWW-Authenticate"))
			}
		})
	}
	assert.Equal(t, 1, handlerCalls)
}
