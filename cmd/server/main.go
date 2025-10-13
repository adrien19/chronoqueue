package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"

	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue"
	"github.com/adrien19/chronoqueue/pkg/gateway"
	"github.com/adrien19/chronoqueue/pkg/log"
	"github.com/adrien19/chronoqueue/pkg/metrics"
	"github.com/adrien19/chronoqueue/pkg/repository"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type ServerConfig struct {
	GRPCAddr     string
	HTTPAddr     string
	RedisAddr    string
	LogLevel     string
	LogFormat    string
	EnableTLS    bool
	CertFile     string
	KeyFile      string
	CACertFile   string
	EnableCORS   bool
	AllowOrigins []string
}

func main() {
	// Parse command-line flags
	config := parseFlags()

	// Initialize logger
	logger := initializeLogger(config.LogLevel, config.LogFormat)
	logger.Info("Starting ChronoQueue server...")

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize dependencies
	redisClient := initializeRedis(ctx, config.RedisAddr, logger)
	encryptionKeyManager := initializeEncryptionKeyManager(logger)

	// Initialize metrics
	gateway.InitMetrics()

	// Initialize storage layer (database)
	database := repository.NewQueueStorage(ctx, redisClient, encryptionKeyManager, logger)

	// Initialize gRPC server directly with storage (no intermediate service layer)
	grpcServer := chronoqueue.NewChronoQueueServer(database, logger)

	// Start servers
	startServers(ctx, config, grpcServer, logger)
}

func parseFlags() ServerConfig {
	var config ServerConfig

	pflag.StringVar(&config.GRPCAddr, "grpc-addr", ":9000", "gRPC server address")
	pflag.StringVar(&config.HTTPAddr, "http-addr", ":8080", "HTTP gateway address")
	pflag.StringVar(&config.RedisAddr, "redis-addr", "localhost:6379", "Redis server address")
	pflag.StringVar(&config.LogLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	pflag.StringVar(&config.LogFormat, "log-format", "text", "Log format (text, json)")
	pflag.BoolVar(&config.EnableTLS, "enable-tls", false, "Enable TLS")
	pflag.StringVar(&config.CertFile, "cert-file", "", "TLS certificate file")
	pflag.StringVar(&config.KeyFile, "key-file", "", "TLS key file")
	pflag.StringVar(&config.CACertFile, "ca-cert-file", "", "CA certificate file for mutual TLS (optional)")
	pflag.BoolVar(&config.EnableCORS, "enable-cors", false, "Enable CORS for HTTP gateway")
	pflag.StringSliceVar(&config.AllowOrigins, "cors-origins", []string{"*"}, "Allowed CORS origins")

	// Add help flag
	help := pflag.BoolP("help", "h", false, "Show help message")

	pflag.Parse()

	if *help {
		fmt.Println("ChronoQueue - High-performance message queue system")
		fmt.Println()
		fmt.Println("Usage:")
		pflag.PrintDefaults()
		os.Exit(0)
	}

	// Validate configuration
	if config.EnableTLS && (config.CertFile == "" || config.KeyFile == "") {
		fmt.Fprintf(os.Stderr, "Error: TLS enabled but cert-file or key-file not specified\n")
		os.Exit(1)
	}

	return config
}

func initializeLogger(logLevel, logFormat string) *log.Logger {
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		level = logrus.InfoLevel
	}

	var formatter logrus.Formatter
	fieldMap := logrus.FieldMap{
		logrus.FieldKeyTime:  "timestamp",
		logrus.FieldKeyLevel: "level",
		logrus.FieldKeyMsg:   "message",
		logrus.FieldKeyFunc:  "caller",
	}

	if logFormat == "json" {
		formatter = &logrus.JSONFormatter{
			PrettyPrint: true,
			FieldMap:    fieldMap,
		}
	} else {
		formatter = &logrus.TextFormatter{
			DisableColors:          false,
			FullTimestamp:          true,
			DisableLevelTruncation: true,
			TimestampFormat:        "2006-01-02 15:04:05",
			FieldMap:               fieldMap,
		}
	}

	return log.NewLogger(log.WithLevel(level), log.WithFormatter(formatter))
}

func initializeRedis(ctx context.Context, addr string, logger *log.Logger) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	// Test connection
	_, err := client.Ping(ctx).Result()
	if err != nil {
		logger.ErrorWithFields("Failed to connect to Redis", "addr", addr, "error", err)
		os.Exit(1)
	}

	logger.InfoWithFields("Connected to Redis", "addr", addr)
	return client
}

func initializeEncryptionKeyManager(logger *log.Logger) *keymanager.EncryptionKeyManager {
	// This is a simplified version. In production, you might want to
	// load encryption keys from environment variables or a secure vault
	km, err := keymanager.NewEncryptionKeyManager(logger)
	if err != nil {
		logger.ErrorWithFields("Failed to initialize encryption key manager", "error", err)
		os.Exit(1)
	}
	logger.Info("Encryption key manager initialized")
	return km
}

func startServers(ctx context.Context, config ServerConfig, grpcServer *chronoqueue.ChronoQueueServer, logger *log.Logger) {
	// Channel to listen for interrupt signal to terminate gracefully
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	// Start gRPC server
	grpcDone := make(chan error, 1)
	go func() {
		grpcDone <- startGRPCServer(config, grpcServer, logger)
	}()

	// Start HTTP gateway
	httpDone := make(chan error, 1)
	go func() {
		httpDone <- startHTTPGateway(ctx, config, logger)
	}()

	// Wait for interrupt signal or server errors
	select {
	case <-interrupt:
		logger.Info("Received interrupt signal, shutting down gracefully...")
	case err := <-grpcDone:
		if err != nil {
			logger.ErrorWithFields("gRPC server error", "error", err)
		}
	case err := <-httpDone:
		if err != nil {
			logger.ErrorWithFields("HTTP server error", "error", err)
		}
	}

	// Graceful shutdown
	logger.Info("Server shutdown complete")
}

func startGRPCServer(config ServerConfig, grpcServer *chronoqueue.ChronoQueueServer, logger *log.Logger) error {
	listener, err := net.Listen("tcp", config.GRPCAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", config.GRPCAddr, err)
	}

	var opts []grpc.ServerOption

	// Add interceptors
	interceptors := []grpc.UnaryServerInterceptor{
		gateway.RecoveryInterceptor(logger),
		gateway.LoggingInterceptor(logger),
		gateway.AuthInterceptor(logger),
		gateway.MetricsInterceptor(logger),
		gateway.ValidationInterceptor(logger),
	}

	// Get TLS configuration
	tlsConfig := getTLSConfigIfEnabled(config, logger)
	if tlsConfig != nil {
		// Add TLS certificate verification interceptor if mTLS is enabled
		if tlsConfig.ClientAuth == tls.RequireAndVerifyClientCert {
			interceptors = append(interceptors, gateway.VerifyPeerCertificateInterceptor)
			logger.InfoWithFields("Added peer certificate verification interceptor for mTLS")
		}

		creds := credentials.NewTLS(tlsConfig)
		opts = append(opts, grpc.Creds(creds))

		logger.InfoWithFields("TLS enabled for gRPC server",
			"cert_file", config.CertFile,
			"mutual_tls", tlsConfig.ClientAuth == tls.RequireAndVerifyClientCert)
	}

	opts = append(opts, grpc.ChainUnaryInterceptor(interceptors...))

	server := grpc.NewServer(opts...)

	// Register services
	queueservice_pb.RegisterQueueServiceServer(server, grpcServer)

	// Enable reflection for development
	reflection.Register(server)

	logger.InfoWithFields("Starting gRPC server", "addr", config.GRPCAddr)

	return server.Serve(listener)
}

func startHTTPGateway(ctx context.Context, config ServerConfig, logger *log.Logger) error {
	// Use the gateway helper function from gateway package
	gatewayConfig := gateway.GatewayConfig{
		GRPCServerAddr: config.GRPCAddr,
		HTTPAddr:       config.HTTPAddr,
		CORSEnabled:    config.EnableCORS,
		AllowedOrigins: config.AllowOrigins,
	}

	gatewayHandler, err := gateway.NewHTTPGateway(ctx, gatewayConfig, logger)
	if err != nil {
		return fmt.Errorf("failed to create HTTP gateway: %w", err)
	}

	// Create HTTP mux for additional endpoints
	httpMux := http.NewServeMux()

	// Mount the gRPC-Gateway at the root
	httpMux.Handle("/", gatewayHandler)

	// Add health check endpoint
	httpMux.Handle("/health", gateway.HealthCheckHandler())

	// Add metrics endpoint
	httpMux.Handle("/metrics", gateway.MetricsHandler())

	// Add Swagger UI endpoint
	httpMux.Handle("/docs/", gateway.SwaggerUIHandler())

	// Serve the OpenAPI spec
	httpMux.Handle("/docs/swagger.json", gateway.SwaggerSpecHandler())

	// Wrap with metrics middleware
	var handler http.Handler = httpMux
	handler = metrics.HTTPMetricsMiddleware(handler)

	// Get TLS configuration and apply client certificate middleware if mTLS is enabled
	tlsConfig := getTLSConfigIfEnabled(config, logger)
	if tlsConfig != nil && tlsConfig.ClientAuth == tls.RequireAndVerifyClientCert {
		handler = gateway.ClientCertMiddleware(handler)
		logger.InfoWithFields("Added client certificate middleware for HTTP mTLS")
	}

	server := &http.Server{
		Addr:         config.HTTPAddr,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		TLSConfig:    tlsConfig,
	}

	logger.InfoWithFields("Starting HTTP gateway",
		"addr", config.HTTPAddr,
		"grpc_endpoint", config.GRPCAddr,
		"tls_enabled", tlsConfig != nil,
		"mutual_tls", tlsConfig != nil && tlsConfig.ClientAuth == tls.RequireAndVerifyClientCert)

	// Start server with or without TLS
	if tlsConfig != nil {
		return server.ListenAndServeTLS("", "") // Certificates are in TLSConfig
	}
	return server.ListenAndServe()
}

// getTLSConfigIfEnabled checks if TLS is enabled and returns the TLS configuration
func getTLSConfigIfEnabled(config ServerConfig, logger *log.Logger) *tls.Config {
	if !config.EnableTLS {
		return nil
	}

	// Load server certificate and key
	cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
	if err != nil {
		logger.ErrorWithFields("Failed to load server certificate/key", "cert_file", config.CertFile, "key_file", config.KeyFile, "error", err)
		os.Exit(1)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	// If CA certificate is provided, enable mutual TLS
	if config.CACertFile != "" {
		caCert, err := os.ReadFile(config.CACertFile)
		if err != nil {
			logger.ErrorWithFields("Failed to read CA certificate", "ca_cert_file", config.CACertFile, "error", err)
			os.Exit(1)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			logger.ErrorWithFields("Failed to parse CA certificate", "ca_cert_file", config.CACertFile)
			os.Exit(1)
		}

		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		tlsConfig.ClientCAs = caCertPool

		logger.InfoWithFields("Mutual TLS enabled", "ca_cert_file", config.CACertFile)
	} else {
		// Server-side TLS only
		logger.InfoWithFields("Server-side TLS enabled (no client certificates required)")
	}

	return tlsConfig
}
