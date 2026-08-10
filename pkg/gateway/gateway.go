package gateway

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"embed"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/pkg/log"
	"github.com/adrien19/chronoqueue/pkg/metrics"
)

//go:embed chronoqueue.swagger.json
var swaggerSpec []byte

//go:embed swagger-ui/*
var swaggerUIAssets embed.FS

// GatewayConfig holds configuration for the HTTP gateway
type GatewayConfig struct {
	GRPCServerAddr string
	HTTPAddr       string
	CORSEnabled    bool
	AllowedOrigins []string

	// TLS Configuration for gateway→gRPC connection
	UseTLS         bool   // Enable TLS for internal gateway→gRPC connection
	TLSInsecure    bool   // Skip TLS verification (for localhost)
	ServerCertFile string // Optional: CA cert to verify server certificate
	ClientCertFile string // Optional: client certificate for mTLS
	ClientKeyFile  string // Optional: client key for mTLS

	// API Documentation Configuration
	EnableAPIDocs       bool     // Enable API documentation endpoints (disabled by default in production)
	APIDocsAllowOrigins []string // Allowed CORS origins for API docs (empty = use AllowedOrigins)
}

// NewHTTPGateway creates a new HTTP-to-gRPC gateway
func NewHTTPGateway(ctx context.Context, config GatewayConfig, logger *log.Logger) (http.Handler, error) {
	// Create a new gRPC-Gateway mux
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{}),
		runtime.WithErrorHandler(customErrorHandler(logger)),
		runtime.WithForwardResponseOption(responseModifier),
		runtime.WithIncomingHeaderMatcher(incomingHeaderMatcher),
	)

	// Set up gRPC client options
	// Note: We don't use WithBlock() here because the connection is established lazily
	// The first request will trigger the connection
	var opts []grpc.DialOption

	if (config.ClientCertFile == "") != (config.ClientKeyFile == "") {
		return nil, fmt.Errorf("gateway client cert and key files must be specified together")
	}

	if config.UseTLS {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: config.TLSInsecure,
			MinVersion:         tls.VersionTLS12,
		}

		// Optionally load server CA cert for verification
		if config.ServerCertFile != "" && !config.TLSInsecure {
			caCert, err := os.ReadFile(config.ServerCertFile)
			if err != nil {
				logger.ErrorWithFields("Failed to read server CA cert for gateway", "error", err)
				return nil, fmt.Errorf("read server CA cert: %w", err)
			}

			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("failed to parse server CA cert")
			}
			tlsConfig.RootCAs = caCertPool
		}

		if config.ClientCertFile != "" {
			clientCert, err := tls.LoadX509KeyPair(config.ClientCertFile, config.ClientKeyFile)
			if err != nil {
				return nil, fmt.Errorf("load gateway client certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{clientCert}
		}

		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
		logger.Info("Gateway using TLS for internal gRPC connection")
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		logger.Warn("Gateway using INSECURE connection to gRPC server")
	}

	logger.InfoWithFields("Registering HTTP gateway handler", "grpc_addr", config.GRPCServerAddr)

	// Register the QueueService handler
	err := queueservice_pb.RegisterQueueServiceHandlerFromEndpoint(
		ctx,
		mux,
		config.GRPCServerAddr,
		opts,
	)
	if err != nil {
		logger.ErrorWithFields("Failed to register QueueService handler", "error", err, "grpc_addr", config.GRPCServerAddr)
		return nil, fmt.Errorf("failed to register QueueService handler: %w", err)
	}

	logger.InfoWithFields("HTTP gateway handler registered successfully", "grpc_addr", config.GRPCServerAddr)

	// Wrap with CORS if enabled
	if config.CORSEnabled {
		return corsHandler(mux, config.AllowedOrigins), nil
	}

	return mux, nil
}

func incomingHeaderMatcher(key string) (string, bool) {
	switch {
	case strings.EqualFold(key, "api-key"):
		return "api-key", true
	case strings.EqualFold(key, "authorization"):
		return "authorization", true
	}
	return runtime.DefaultHeaderMatcher(key)
}

// customErrorHandler provides custom error handling for the gateway
func customErrorHandler(logger *log.Logger) runtime.ErrorHandlerFunc {
	return func(ctx context.Context, mux *runtime.ServeMux, marshaler runtime.Marshaler, w http.ResponseWriter, r *http.Request, err error) {
		// Check if this is a NotFound error - common for unimplemented endpoints
		// Log at DEBUG level instead of ERROR to reduce noise
		if err != nil && strings.Contains(err.Error(), "NotFound") {
			logger.DebugWithFields(
				"Gateway endpoint not found",
				"method", r.Method,
				"path", r.URL.Path,
			)
		} else {
			logger.ErrorWithFields(
				"Gateway error",
				"method", r.Method,
				"path", r.URL.Path,
				"error", err,
			)
		}

		// Use the default error handler but log the error
		runtime.DefaultHTTPErrorHandler(ctx, mux, marshaler, w, r, err)
	}
}

// responseModifier allows modification of the response before it's sent
func responseModifier(ctx context.Context, w http.ResponseWriter, p proto.Message) error {
	// Add custom headers
	w.Header().Set("X-ChronoQueue-Version", "2.0")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")

	// If the response carries a worker_id, expose it for HTTP clients
	if resp, ok := p.(*queueservice_pb.GetNextMessageResponse); ok {
		if resp.GetWorkerId() != "" {
			w.Header().Set("X-Worker-ID", resp.GetWorkerId())
		}
		if resp.GetAttemptId() != "" {
			w.Header().Set("X-Attempt-ID", resp.GetAttemptId())
		}
	}

	return nil
}

// corsHandler wraps the handler with CORS support
func corsHandler(handler http.Handler, allowedOrigins []string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")

		// Check if origin is allowed
		allowed := false
		for _, allowedOrigin := range allowedOrigins {
			if allowedOrigin == "*" || allowedOrigin == origin {
				allowed = true
				break
			}
		}

		if allowed {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}

		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, Authorization, api-key, X-CSRF-Token")
		w.Header().Set("Access-Control-Expose-Headers", "X-Worker-ID, X-ChronoQueue-Version, X-Attempt-ID")

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		handler.ServeHTTP(w, r)
	})
}

// HealthCheckHandler provides a simple health check endpoint
func HealthCheckHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		response := map[string]interface{}{
			"status":  "healthy",
			"service": "chronoqueue",
			"version": "2.0",
		}

		// In production, you might want to check database connectivity,
		// Redis connectivity, etc.

		_, _ = fmt.Fprintf(w, `{"status": "%s", "service": "%s", "version": "%s"}`,
			response["status"], response["service"], response["version"])
	})
}

// Global metrics registry instance
var metricsRegistry *metrics.MetricsRegistry

// InitMetrics initializes the global metrics registry
func InitMetrics() {
	metricsRegistry = metrics.NewMetricsRegistry()
}

// MetricsHandler provides a Prometheus metrics endpoint
func MetricsHandler() http.Handler {
	if metricsRegistry == nil {
		InitMetrics()
	}
	return metricsRegistry.Handler()
}

// BearerAuthMiddleware requires a bearer token before serving a protected endpoint.
func BearerAuthMiddleware(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme, credential, found := strings.Cut(r.Header.Get("Authorization"), " ")
		authorized := found && strings.EqualFold(scheme, "Bearer") &&
			len(credential) == len(token) && subtle.ConstantTimeCompare([]byte(credential), []byte(token)) == 1
		if !authorized {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("WWW-Authenticate", `Bearer realm="ChronoQueue metrics"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// SwaggerUIHandler serves the Swagger UI for API documentation
func SwaggerUIHandler(config GatewayConfig, logger *log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Security: Check if API docs are enabled
		if !config.EnableAPIDocs {
			logger.WarnWithFields(
				"API documentation access denied - disabled in configuration",
				"remote_addr", r.RemoteAddr,
				"path", r.URL.Path,
			)
			http.Error(w, "API documentation is disabled", http.StatusNotFound)
			return
		}

		// Security: Restrict to GET and OPTIONS methods only
		if r.Method != http.MethodGet && r.Method != http.MethodOptions {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Check if request is for the root docs path
		if r.URL.Path == "/docs/" || r.URL.Path == "/docs" {
			// Serve the Swagger UI HTML
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusOK)

			swaggerHTML := `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>ChronoQueue API Documentation</title>
    <link rel="stylesheet" type="text/css" href="/docs/assets/swagger-ui.css" />
    <link rel="stylesheet" type="text/css" href="/docs/assets/chronoqueue.css" />
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="/docs/assets/swagger-ui-bundle.js"></script>
    <script src="/docs/assets/swagger-ui-standalone-preset.js"></script>
    <script src="/docs/assets/swagger-ui-init.js"></script>
</body>
</html>`
			if _, err := fmt.Fprint(w, swaggerHTML); err != nil {
				logger.ErrorWithFields("Failed to write Swagger UI response", "error", err)
			}
		} else {
			// Handle other paths under /docs/
			http.NotFound(w, r)
		}
	})
}

// SwaggerAssetHandler serves the embedded Swagger UI assets.
func SwaggerAssetHandler(config GatewayConfig, logger *log.Logger) http.Handler {
	contentTypes := map[string]string{
		"chronoqueue.css":                 "text/css; charset=utf-8",
		"swagger-ui.css":                  "text/css; charset=utf-8",
		"swagger-ui-bundle.js":            "text/javascript; charset=utf-8",
		"swagger-ui-init.js":              "text/javascript; charset=utf-8",
		"swagger-ui-standalone-preset.js": "text/javascript; charset=utf-8",
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !config.EnableAPIDocs {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		assetName := strings.TrimPrefix(r.URL.Path, "/docs/assets/")
		contentType, ok := contentTypes[assetName]
		if !ok {
			http.NotFound(w, r)
			return
		}
		asset, err := swaggerUIAssets.ReadFile("swagger-ui/" + assetName)
		if err != nil {
			logger.ErrorWithFields("Failed to read embedded Swagger UI asset", "asset", assetName, "error", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "public, max-age=86400")
		if r.Method == http.MethodHead {
			return
		}
		if _, err := w.Write(asset); err != nil {
			logger.ErrorWithFields("Failed to write Swagger UI asset", "asset", assetName, "error", err)
		}
	})
}

// SwaggerSpecHandler serves the OpenAPI specification JSON
func SwaggerSpecHandler(config GatewayConfig, logger *log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Security: Check if API docs are enabled
		if !config.EnableAPIDocs {
			logger.WarnWithFields(
				"API documentation access denied - disabled in configuration",
				"remote_addr", r.RemoteAddr,
				"path", r.URL.Path,
			)
			http.Error(w, "API documentation is disabled", http.StatusNotFound)
			return
		}

		// Security: Restrict to GET and OPTIONS methods only
		if r.Method != http.MethodGet && r.Method != http.MethodOptions {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// Security: Use specific CORS origins instead of wildcard
		allowedOrigins := config.APIDocsAllowOrigins
		if len(allowedOrigins) == 0 {
			// Fall back to main gateway CORS config
			allowedOrigins = config.AllowedOrigins
		}

		origin := r.Header.Get("Origin")
		if origin != "" {
			// Check if origin is allowed
			allowed := false
			for _, allowedOrigin := range allowedOrigins {
				if allowedOrigin == "*" || allowedOrigin == origin {
					allowed = true
					w.Header().Set("Access-Control-Allow-Origin", origin)
					break
				}
			}

			if !allowed {
				logger.WarnWithFields(
					"API documentation CORS origin denied",
					"origin", origin,
					"remote_addr", r.RemoteAddr,
				)
			}
		}

		// Handle CORS preflight
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		w.WriteHeader(http.StatusOK)
		n, err := w.Write(swaggerSpec)
		if err != nil {
			logger.ErrorWithFields(
				"Failed to write swagger spec",
				"error", err,
				"bytes_written", n,
				"expected_bytes", len(swaggerSpec),
				"remote_addr", r.RemoteAddr,
				"path", r.URL.Path,
			)
			return
		}
		if n != len(swaggerSpec) {
			logger.WarnWithFields(
				"Short write when sending swagger spec",
				"bytes_written", n,
				"expected_bytes", len(swaggerSpec),
				"remote_addr", r.RemoteAddr,
				"path", r.URL.Path,
			)
		}
	})
}
