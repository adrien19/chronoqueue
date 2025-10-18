package server

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

// Server represents the ChronoQueue server instance
type Server struct {
	config               *Config
	logger               *log.Logger
	redisClient          *redis.Client
	encryptionKeyManager *keymanager.EncryptionKeyManager
	grpcServer           *chronoqueue.ChronoQueueServer
	database             repository.Storage
}

// New creates a new server instance with the given configuration
func New(config *Config) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &Server{
		config: config,
	}, nil
}

// Start initializes and starts the server
func (s *Server) Start(ctx context.Context) error {
	// Initialize logger
	logger, err := s.initializeLogger()
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	s.logger = logger

	s.logger.Info("Starting ChronoQueue server...")

	// Initialize Redis connection
	redisClient, err := s.initializeRedis(ctx)
	if err != nil {
		return fmt.Errorf("failed to initialize Redis: %w", err)
	}
	s.redisClient = redisClient

	// Initialize encryption key manager
	encryptionKeyManager, err := s.initializeEncryptionKeyManager()
	if err != nil {
		return fmt.Errorf("failed to initialize encryption key manager: %w", err)
	}
	s.encryptionKeyManager = encryptionKeyManager

	// Initialize metrics
	gateway.InitMetrics()

	// Initialize storage layer (database)
	s.database = repository.NewQueueStorage(ctx, s.redisClient, s.encryptionKeyManager, s.logger)

	// Initialize gRPC server directly with storage
	s.grpcServer = chronoqueue.NewChronoQueueServer(s.database, s.logger)

	// Print startup information
	s.printStartupInfo()

	// Start servers
	return s.startServers(ctx)
}

// printStartupInfo prints server startup information
func (s *Server) printStartupInfo() {
	mode := "production"
	if s.config.IsDevelopment {
		mode = "development"
	}

	s.logger.InfoWithFields("Server configuration",
		"mode", mode,
		"grpc_addr", s.config.GRPCAddr,
		"http_addr", s.config.HTTPAddr,
		"redis_addr", s.config.RedisAddr,
		"log_level", s.config.LogLevel,
		"tls_enabled", s.config.EnableTLS,
		"cors_enabled", s.config.EnableCORS,
	)

	fmt.Printf("✓ ChronoQueue server starting in %s mode\n", mode)
	fmt.Printf("ℹ gRPC server will listen on: %s\n", s.config.GRPCAddr)
	fmt.Printf("ℹ HTTP gateway will listen on: %s\n", s.config.HTTPAddr)
	fmt.Printf("ℹ Redis connection: %s\n", s.config.RedisAddr)
	fmt.Printf("ℹ Log level: %s\n", s.config.LogLevel)

	if s.config.IsDevelopment {
		fmt.Printf("ℹ Available endpoints:\n")
		fmt.Printf("  - Health: http://localhost%s/health\n", s.config.HTTPAddr)
		fmt.Printf("  - Metrics: http://localhost%s/metrics\n", s.config.HTTPAddr)
		fmt.Printf("  - API Docs: http://localhost%s/docs/\n", s.config.HTTPAddr)
	}

	fmt.Printf("ℹ Press Ctrl+C to stop the server\n")
}

// initializeLogger creates and configures the logger
func (s *Server) initializeLogger() (*log.Logger, error) {
	level, err := logrus.ParseLevel(s.config.LogLevel)
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

	if s.config.LogFormat == "json" {
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

	return log.NewLogger(log.WithLevel(level), log.WithFormatter(formatter)), nil
}

// initializeRedis creates and tests the Redis connection
func (s *Server) initializeRedis(ctx context.Context) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr: s.config.RedisAddr,
	})

	// Test connection
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis at %s: %w", s.config.RedisAddr, err)
	}

	s.logger.InfoWithFields("Connected to Redis", "addr", s.config.RedisAddr)
	return client, nil
}

// initializeEncryptionKeyManager creates the encryption key manager
func (s *Server) initializeEncryptionKeyManager() (*keymanager.EncryptionKeyManager, error) {
	km, err := keymanager.NewEncryptionKeyManager(s.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize encryption key manager: %w", err)
	}
	s.logger.Info("Encryption key manager initialized")
	return km, nil
}

// startServers starts both gRPC and HTTP servers
func (s *Server) startServers(ctx context.Context) error {
	// Channel to listen for interrupt signal to terminate gracefully
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	// Start gRPC server
	grpcDone := make(chan error, 1)
	go func() {
		grpcDone <- s.startGRPCServer()
	}()

	// Start HTTP gateway
	httpDone := make(chan error, 1)
	go func() {
		httpDone <- s.startHTTPGateway(ctx)
	}()

	// Wait for interrupt signal or server errors
	select {
	case <-interrupt:
		s.logger.Info("Received interrupt signal, shutting down gracefully...")
		fmt.Printf("ℹ Shutting down server...\n")
	case err := <-grpcDone:
		if err != nil {
			s.logger.ErrorWithFields("gRPC server error", "error", err)
			return fmt.Errorf("gRPC server error: %w", err)
		}
	case err := <-httpDone:
		if err != nil {
			s.logger.ErrorWithFields("HTTP server error", "error", err)
			return fmt.Errorf("HTTP server error: %w", err)
		}
	}

	// Graceful shutdown
	s.logger.Info("Server shutdown complete")
	fmt.Printf("✓ Server stopped successfully\n")
	return nil
}

// startGRPCServer starts the gRPC server
func (s *Server) startGRPCServer() error {
	listener, err := net.Listen("tcp", s.config.GRPCAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.config.GRPCAddr, err)
	}

	var opts []grpc.ServerOption

	// Add interceptors
	interceptors := []grpc.UnaryServerInterceptor{
		gateway.RecoveryInterceptor(s.logger),
		gateway.LoggingInterceptor(s.logger),
		gateway.AuthInterceptor(s.logger),
		gateway.MetricsInterceptor(s.logger),
		gateway.ValidationInterceptor(s.logger),
	}

	// Get TLS configuration
	tlsConfig := s.getTLSConfig()
	if tlsConfig != nil {
		// Add TLS certificate verification interceptor if mTLS is enabled
		if tlsConfig.ClientAuth == tls.RequireAndVerifyClientCert {
			interceptors = append(interceptors, gateway.VerifyPeerCertificateInterceptor)
			s.logger.InfoWithFields("Added peer certificate verification interceptor for mTLS")
		}

		creds := credentials.NewTLS(tlsConfig)
		opts = append(opts, grpc.Creds(creds))

		s.logger.InfoWithFields("TLS enabled for gRPC server",
			"cert_file", s.config.CertFile,
			"mutual_tls", tlsConfig.ClientAuth == tls.RequireAndVerifyClientCert)
	}

	opts = append(opts, grpc.ChainUnaryInterceptor(interceptors...))

	server := grpc.NewServer(opts...)

	// Register services
	queueservice_pb.RegisterQueueServiceServer(server, s.grpcServer)

	// Enable reflection for development
	if s.config.IsDevelopment {
		reflection.Register(server)
	}

	s.logger.InfoWithFields("Starting gRPC server", "addr", s.config.GRPCAddr)

	if err := server.Serve(listener); err != nil {
		return fmt.Errorf("failed to serve gRPC: %w", err)
	}

	return nil
}

// startHTTPGateway starts the HTTP gateway server
func (s *Server) startHTTPGateway(ctx context.Context) error {
	// Use the gateway helper function from gateway package
	gatewayConfig := gateway.GatewayConfig{
		GRPCServerAddr: s.config.GRPCAddr,
		HTTPAddr:       s.config.HTTPAddr,
		CORSEnabled:    s.config.EnableCORS,
		AllowedOrigins: s.config.AllowOrigins,
	}

	gatewayHandler, err := gateway.NewHTTPGateway(ctx, gatewayConfig, s.logger)
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

	// Add development endpoints
	if s.config.IsDevelopment {
		httpMux.Handle("/docs/", gateway.SwaggerUIHandler())
		httpMux.Handle("/docs/swagger.json", gateway.SwaggerSpecHandler())
	}

	// Wrap with metrics middleware
	var handler http.Handler = httpMux
	handler = metrics.HTTPMetricsMiddleware(handler)

	s.logger.InfoWithFields("Starting HTTP gateway", "addr", s.config.HTTPAddr)

	server := &http.Server{
		Addr:    s.config.HTTPAddr,
		Handler: handler,
	}

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to serve HTTP: %w", err)
	}

	return nil
}

// getTLSConfig returns TLS configuration if enabled
func (s *Server) getTLSConfig() *tls.Config {
	if !s.config.EnableTLS {
		return nil
	}

	// Load server certificate and key
	cert, err := tls.LoadX509KeyPair(s.config.CertFile, s.config.KeyFile)
	if err != nil {
		s.logger.ErrorWithFields("Failed to load server certificate", "error", err)
		os.Exit(1)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.NoClientCert,
	}

	// Load CA certificate for mutual TLS if provided
	if s.config.CACertFile != "" {
		caCert, err := os.ReadFile(s.config.CACertFile)
		if err != nil {
			s.logger.ErrorWithFields("Failed to read CA certificate", "error", err)
			os.Exit(1)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			s.logger.Error("Failed to parse CA certificate")
			os.Exit(1)
		}

		tlsConfig.ClientCAs = caCertPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		s.logger.Info("Mutual TLS (mTLS) enabled")
	}

	return tlsConfig
}
