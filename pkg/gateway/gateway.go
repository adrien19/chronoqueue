package gateway

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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
}

// NewHTTPGateway creates a new HTTP-to-gRPC gateway
func NewHTTPGateway(ctx context.Context, config GatewayConfig, logger *log.Logger) (http.Handler, error) {
	// Create a new gRPC-Gateway mux
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{}),
		runtime.WithErrorHandler(customErrorHandler(logger)),
		runtime.WithForwardResponseOption(responseModifier),
	)

	// Set up gRPC client options
	// Note: We don't use WithBlock() here because the connection is established lazily
	// The first request will trigger the connection
	var opts []grpc.DialOption

	if config.UseTLS {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: config.TLSInsecure,
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

// customErrorHandler provides custom error handling for the gateway
func customErrorHandler(logger *log.Logger) runtime.ErrorHandlerFunc {
	return func(ctx context.Context, mux *runtime.ServeMux, marshaler runtime.Marshaler, w http.ResponseWriter, r *http.Request, err error) {
		// Check if this is a NotFound error - common for unimplemented endpoints
		// Log at DEBUG level instead of ERROR to reduce noise
		if err != nil && strings.Contains(err.Error(), "NotFound") {
			logger.DebugWithFields("Gateway endpoint not found",
				"method", r.Method,
				"path", r.URL.Path,
			)
		} else {
			logger.ErrorWithFields("Gateway error",
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
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, Authorization, X-CSRF-Token")
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

// SwaggerUIHandler serves the Swagger UI for API documentation
func SwaggerUIHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
    <link rel="stylesheet" type="text/css" href="https://unpkg.com/swagger-ui-dist@3.52.5/swagger-ui.css" />
    <style>
        html {
            box-sizing: border-box;
            overflow: -moz-scrollbars-vertical;
            overflow-y: scroll;
        }
        *, *:before, *:after {
            box-sizing: inherit;
        }
        body {
            margin:0;
            background: #fafafa;
        }
    </style>
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@3.52.5/swagger-ui-bundle.js"></script>
    <script src="https://unpkg.com/swagger-ui-dist@3.52.5/swagger-ui-standalone-preset.js"></script>
    <script>
        window.onload = function() {
            const ui = SwaggerUIBundle({
                url: '/docs/swagger.json',
                dom_id: '#swagger-ui',
                deepLinking: true,
                presets: [
                    SwaggerUIBundle.presets.apis,
                    SwaggerUIStandalonePreset
                ],
                plugins: [
                    SwaggerUIBundle.plugins.DownloadUrl
                ],
                layout: "StandaloneLayout",
                docExpansion: "list",
                defaultModelExpandDepth: 3,
                defaultModelsExpandDepth: 1,
                tryItOutEnabled: true
            });
        };
    </script>
</body>
</html>`
			_, _ = fmt.Fprint(w, swaggerHTML)
		} else {
			// Handle other paths under /docs/
			http.NotFound(w, r)
		}
	})
}

// SwaggerSpecHandler serves the OpenAPI specification JSON
func SwaggerSpecHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Read the swagger.json file from the docs directory
		specPath := "/workspaces/chronoqueue/docs/api/chronoqueue.swagger.json"

		specContent, err := os.ReadFile(specPath)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to read OpenAPI spec: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// Handle CORS preflight
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(specContent)
	})
}
