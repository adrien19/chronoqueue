package server

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the complete server configuration
type Config struct {
	// Version Information
	Version   string
	GitCommit string
	BuildDate string

	// Network Configuration
	GRPCAddr string
	HTTPAddr string

	// Storage Configuration
	StorageType      string // "sqlite" or "postgres"
	SQLiteDBPath     string // Path to SQLite database file
	PostgresDSN      string // Optional DSN override
	PostgresHost     string
	PostgresPort     int
	PostgresUser     string
	PostgresPassword string
	PostgresDBName   string
	PostgresSSLMode  string
	// PostgreSQL Client Certificate Configuration (for mTLS with database)
	PostgresClientCertFile string // Path to PostgreSQL client certificate file
	PostgresClientKeyFile  string // Path to PostgreSQL client key file
	PostgresRootCertFile   string // Path to PostgreSQL root CA certificate file
	// Logging Configuration
	LogLevel  string
	LogFormat string

	// TLS Configuration
	EnableTLS  bool
	CertFile   string
	KeyFile    string
	CACertFile string

	// Gateway TLS Configuration
	GatewayUseTLS   bool // Use TLS for gateway→gRPC internal connection
	GatewayInsecure bool // Skip TLS verification for gateway→gRPC (for localhost)

	// HTTP Gateway Configuration
	EnableCORS   bool
	AllowOrigins []string

	// API Documentation Configuration
	EnableAPIDocs       bool     // Enable API documentation endpoints (default: false in production)
	APIDocsAllowOrigins []string // Allowed CORS origins for API docs (comma-separated)

	// Background Services Configuration
	SchedulerIntervalMs int // Scheduler interval in milliseconds (default: 1000ms)
	ReclaimIntervalMs   int // Reclaim service interval in milliseconds (default: 5000ms)

	// Development/Runtime Configuration
	IsDevelopment bool
}

// DefaultConfig returns a configuration suitable for development
func DefaultConfig() *Config {
	return &Config{
		GRPCAddr:            getEnv("GRPC_ADDR", ":9000"),
		HTTPAddr:            getEnv("HTTP_ADDR", ":8080"),
		StorageType:         getEnv("STORAGE_TYPE", "postgres"),
		SQLiteDBPath:        getEnv("SQLITE_DB_PATH", "chronoqueue.db"),
		PostgresDSN:         getEnv("POSTGRES_DSN", ""),
		PostgresHost:        getEnv("POSTGRES_HOST", "localhost"),
		PostgresPort:        getEnvInt("POSTGRES_PORT", 5432),
		PostgresUser:        getEnv("POSTGRES_USER", "chronoqueue"),
		PostgresPassword:    getEnv("POSTGRES_PASSWORD", "chronoqueue"),
		PostgresDBName:      getEnv("POSTGRES_DB", "chronoqueue"),
		PostgresSSLMode:     getEnv("POSTGRES_SSLMODE", "disable"),
		LogLevel:            getEnv("LOG_LEVEL", "info"),
		LogFormat:           getEnv("LOG_FORMAT", "text"),
		EnableTLS:           getEnvBool("CHRONOQUEUE_TLS_ENABLED", false),
		EnableCORS:          getEnvBool("ENABLE_CORS", true),
		AllowOrigins:        getEnvSlice("ALLOW_ORIGINS", []string{"*"}),
		SchedulerIntervalMs: getEnvInt("SCHEDULER_INTERVAL_MS", 1000),
		ReclaimIntervalMs:   getEnvInt("RECLAIM_INTERVAL_MS", 5000),
		IsDevelopment:       true,
	}
}

// ProductionConfig returns a configuration suitable for production
func ProductionConfig() *Config {
	return &Config{
		GRPCAddr:            getEnv("GRPC_ADDR", ":9000"),
		HTTPAddr:            getEnv("HTTP_ADDR", ":8080"),
		StorageType:         getEnv("STORAGE_TYPE", "postgres"),
		SQLiteDBPath:        getEnv("SQLITE_DB_PATH", "chronoqueue.db"),
		PostgresDSN:         getEnv("POSTGRES_DSN", ""),
		PostgresHost:        getEnv("POSTGRES_HOST", "localhost"),
		PostgresPort:        getEnvInt("POSTGRES_PORT", 5432),
		PostgresUser:        getEnv("POSTGRES_USER", "chronoqueue"),
		PostgresPassword:    getEnv("POSTGRES_PASSWORD", "chronoqueue"),
		PostgresDBName:      getEnv("POSTGRES_DB", "chronoqueue"),
		PostgresSSLMode:     getEnv("POSTGRES_SSLMODE", "disable"),
		LogLevel:            getEnv("LOG_LEVEL", "info"),
		LogFormat:           getEnv("LOG_FORMAT", "json"),
		EnableTLS:           getEnvBool("CHRONOQUEUE_TLS_ENABLED", false),
		EnableCORS:          getEnvBool("ENABLE_CORS", false),
		AllowOrigins:        getEnvSlice("ALLOW_ORIGINS", []string{}),
		EnableAPIDocs:       getEnvBool("ENABLE_API_DOCS", false), // Disabled by default in production
		APIDocsAllowOrigins: getEnvSlice("API_DOCS_CORS_ORIGINS", []string{}),
		SchedulerIntervalMs: getEnvInt("SCHEDULER_INTERVAL_MS", 1000),
		ReclaimIntervalMs:   getEnvInt("RECLAIM_INTERVAL_MS", 5000),
		IsDevelopment:       false,
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.EnableTLS && (c.CertFile == "" || c.KeyFile == "") {
		return fmt.Errorf("TLS enabled but cert-file or key-file not specified")
	}

	if c.GRPCAddr == "" {
		return fmt.Errorf("gRPC address cannot be empty")
	}

	if c.HTTPAddr == "" {
		return fmt.Errorf("HTTP address cannot be empty")
	}

	// Validate storage configuration
	if c.StorageType != "sqlite" && c.StorageType != "postgres" {
		return fmt.Errorf("storage-type must be 'sqlite' or 'postgres', got: %s", c.StorageType)
	}

	if c.StorageType == "sqlite" && c.SQLiteDBPath == "" {
		return fmt.Errorf("sqlite-db-path cannot be empty when using sqlite storage")
	}

	if c.StorageType == "postgres" {
		hasDSN := c.PostgresDSN != ""
		hasHost := c.PostgresHost != ""
		if !hasDSN && !hasHost {
			return fmt.Errorf("postgres configuration requires either postgres-dsn or host details")
		}
		if c.PostgresPort <= 0 {
			return fmt.Errorf("postgres-port must be greater than 0")
		}
	}

	return nil
}

// GetTimeout returns a reasonable timeout for the server configuration
func (c *Config) GetTimeout() time.Duration {
	if c.IsDevelopment {
		return 30 * time.Second
	}
	return 60 * time.Second
}

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvInt retrieves an integer environment variable or returns a default value
func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

// getEnvBool retrieves a boolean environment variable or returns a default value
func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		return strings.ToLower(value) == "true" || value == "1"
	}
	return defaultValue
}

// getEnvSlice retrieves a comma-separated environment variable as a slice or returns a default value
func getEnvSlice(key string, defaultValue []string) []string {
	if value := os.Getenv(key); value != "" {
		// Split by comma and trim spaces
		parts := strings.Split(value, ",")
		result := make([]string, 0, len(parts))
		for _, part := range parts {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				result = append(result, trimmed)
			}
		}
		return result
	}
	return defaultValue
}
