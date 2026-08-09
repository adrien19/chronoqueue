package server

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

var postgresDSNParameterPattern = regexp.MustCompile(`(?i)(?:^|\s)([a-z_]+)\s*=\s*(?:'([^']*)'|"([^"]*)"|([^\s]+))`)

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
	GatewayUseTLS         bool // Use TLS for gateway→gRPC internal connection
	GatewayInsecure       bool // Skip TLS verification for gateway→gRPC (for localhost)
	GatewayClientCertFile string
	GatewayClientKeyFile  string

	// HTTP Gateway Configuration
	EnableCORS            bool
	AllowOrigins          []string
	HTTPReadHeaderTimeout time.Duration
	HTTPReadTimeout       time.Duration
	HTTPWriteTimeout      time.Duration
	HTTPIdleTimeout       time.Duration

	// Authentication Configuration
	AuthEnabled bool
	APIKeys     []string

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
		GRPCAddr:               getEnv("GRPC_ADDR", ":9000"),
		HTTPAddr:               getEnv("HTTP_ADDR", ":8080"),
		StorageType:            getEnv("STORAGE_TYPE", "postgres"),
		SQLiteDBPath:           getEnv("SQLITE_DB_PATH", "chronoqueue.db"),
		PostgresDSN:            getEnv("POSTGRES_DSN", ""),
		PostgresHost:           getEnv("POSTGRES_HOST", "localhost"),
		PostgresPort:           getEnvInt("POSTGRES_PORT", 5432),
		PostgresUser:           getEnv("POSTGRES_USER", "chronoqueue"),
		PostgresPassword:       getEnv("POSTGRES_PASSWORD", "chronoqueue"),
		PostgresDBName:         getEnv("POSTGRES_DB", "chronoqueue"),
		PostgresSSLMode:        getEnv("POSTGRES_SSLMODE", "disable"),
		PostgresClientCertFile: getEnv("POSTGRES_CLIENT_CERT", ""),
		PostgresClientKeyFile:  getEnv("POSTGRES_CLIENT_KEY", ""),
		PostgresRootCertFile:   getEnv("POSTGRES_ROOT_CERT", ""),
		LogLevel:               getEnv("LOG_LEVEL", "info"),
		LogFormat:              getEnv("LOG_FORMAT", "text"),
		EnableTLS:              getEnvBool("CHRONOQUEUE_TLS_ENABLED", false),
		CertFile:               getEnv("CERT_FILE", ""),
		KeyFile:                getEnv("KEY_FILE", ""),
		CACertFile:             getEnv("CA_CERT_FILE", ""),
		GatewayClientCertFile:  getEnv("GATEWAY_CLIENT_CERT_FILE", ""),
		GatewayClientKeyFile:   getEnv("GATEWAY_CLIENT_KEY_FILE", ""),
		EnableCORS:             getEnvBool("ENABLE_CORS", true),
		AllowOrigins:           getEnvSlice("ALLOW_ORIGINS", []string{"*"}),
		HTTPReadHeaderTimeout:  getEnvDuration("HTTP_READ_HEADER_TIMEOUT", 5*time.Second),
		HTTPReadTimeout:        getEnvDuration("HTTP_READ_TIMEOUT", 15*time.Second),
		HTTPWriteTimeout:       getEnvDuration("HTTP_WRITE_TIMEOUT", 30*time.Second),
		HTTPIdleTimeout:        getEnvDuration("HTTP_IDLE_TIMEOUT", 60*time.Second),
		AuthEnabled:            getEnvBool("AUTH_ENABLED", false),
		APIKeys:                getEnvSlice("API_KEYS", nil),
		SchedulerIntervalMs:    getEnvInt("SCHEDULER_INTERVAL_MS", 1000),
		ReclaimIntervalMs:      getEnvInt("RECLAIM_INTERVAL_MS", 5000),
		IsDevelopment:          true,
	}
}

// ProductionConfig returns a configuration suitable for production
func ProductionConfig() *Config {
	return &Config{
		GRPCAddr:               getEnv("GRPC_ADDR", ":9000"),
		HTTPAddr:               getEnv("HTTP_ADDR", ":8080"),
		StorageType:            getEnv("STORAGE_TYPE", "postgres"),
		SQLiteDBPath:           getEnv("SQLITE_DB_PATH", "chronoqueue.db"),
		PostgresDSN:            getEnv("POSTGRES_DSN", ""),
		PostgresHost:           getEnv("POSTGRES_HOST", "localhost"),
		PostgresPort:           getEnvInt("POSTGRES_PORT", 5432),
		PostgresUser:           getEnv("POSTGRES_USER", "chronoqueue"),
		PostgresPassword:       getEnv("POSTGRES_PASSWORD", ""),
		PostgresDBName:         getEnv("POSTGRES_DB", "chronoqueue"),
		PostgresSSLMode:        getEnv("POSTGRES_SSLMODE", "verify-full"),
		PostgresClientCertFile: getEnv("POSTGRES_CLIENT_CERT", ""),
		PostgresClientKeyFile:  getEnv("POSTGRES_CLIENT_KEY", ""),
		PostgresRootCertFile:   getEnv("POSTGRES_ROOT_CERT", ""),
		LogLevel:               getEnv("LOG_LEVEL", "info"),
		LogFormat:              getEnv("LOG_FORMAT", "json"),
		EnableTLS:              getEnvBool("CHRONOQUEUE_TLS_ENABLED", true),
		CertFile:               getEnv("CERT_FILE", ""),
		KeyFile:                getEnv("KEY_FILE", ""),
		CACertFile:             getEnv("CA_CERT_FILE", ""),
		GatewayClientCertFile:  getEnv("GATEWAY_CLIENT_CERT_FILE", ""),
		GatewayClientKeyFile:   getEnv("GATEWAY_CLIENT_KEY_FILE", ""),
		EnableCORS:             getEnvBool("ENABLE_CORS", false),
		AllowOrigins:           getEnvSlice("ALLOW_ORIGINS", []string{}),
		HTTPReadHeaderTimeout:  getEnvDuration("HTTP_READ_HEADER_TIMEOUT", 5*time.Second),
		HTTPReadTimeout:        getEnvDuration("HTTP_READ_TIMEOUT", 15*time.Second),
		HTTPWriteTimeout:       getEnvDuration("HTTP_WRITE_TIMEOUT", 30*time.Second),
		HTTPIdleTimeout:        getEnvDuration("HTTP_IDLE_TIMEOUT", 60*time.Second),
		AuthEnabled:            getEnvBool("AUTH_ENABLED", true),
		APIKeys:                getEnvSlice("API_KEYS", nil),
		EnableAPIDocs:          getEnvBool("ENABLE_API_DOCS", false), // Disabled by default in production
		APIDocsAllowOrigins:    getEnvSlice("API_DOCS_CORS_ORIGINS", []string{}),
		SchedulerIntervalMs:    getEnvInt("SCHEDULER_INTERVAL_MS", 1000),
		ReclaimIntervalMs:      getEnvInt("RECLAIM_INTERVAL_MS", 5000),
		IsDevelopment:          false,
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if !c.IsDevelopment && !c.EnableTLS {
		return fmt.Errorf("TLS must be enabled in production")
	}

	if c.AuthEnabled && len(c.APIKeys) == 0 {
		return fmt.Errorf("authentication enabled but no API keys configured")
	}
	if slices.Contains(c.APIKeys, "") {
		return fmt.Errorf("API keys cannot be empty")
	}

	if c.EnableTLS && (c.CertFile == "" || c.KeyFile == "") {
		return fmt.Errorf("TLS enabled but cert-file or key-file not specified")
	}
	if !c.IsDevelopment && c.GatewayInsecure {
		return fmt.Errorf("gateway-insecure cannot be enabled in production")
	}

	if (c.GatewayClientCertFile == "") != (c.GatewayClientKeyFile == "") {
		return fmt.Errorf("gateway client cert and key files must be specified together")
	}
	if c.GatewayClientCertFile != "" && !c.GatewayUseTLS {
		return fmt.Errorf("gateway client certificates require gateway TLS")
	}
	if c.GatewayClientCertFile != "" && c.CACertFile == "" {
		return fmt.Errorf("gateway client certificates require ca-cert-file")
	}
	if c.CACertFile != "" && (c.GatewayClientCertFile == "" || c.GatewayClientKeyFile == "") {
		return fmt.Errorf("mTLS enabled but gateway client cert or key file not specified")
	}

	if c.GRPCAddr == "" {
		return fmt.Errorf("gRPC address cannot be empty")
	}

	if c.HTTPAddr == "" {
		return fmt.Errorf("HTTP address cannot be empty")
	}
	if c.HTTPReadHeaderTimeout <= 0 || c.HTTPReadTimeout <= 0 || c.HTTPWriteTimeout <= 0 || c.HTTPIdleTimeout <= 0 {
		return fmt.Errorf("HTTP gateway timeouts must be greater than 0")
	}

	// Validate storage configuration
	if c.StorageType != "sqlite" && c.StorageType != "postgres" {
		return fmt.Errorf("storage-type must be 'sqlite' or 'postgres', got: %s", c.StorageType)
	}

	if c.StorageType == "sqlite" && c.SQLiteDBPath == "" {
		return fmt.Errorf("sqlite-db-path cannot be empty when using sqlite storage")
	}

	if c.StorageType == "postgres" {
		if (c.PostgresClientCertFile == "") != (c.PostgresClientKeyFile == "") {
			return fmt.Errorf("postgres client cert and key files must be specified together")
		}

		hasDSN := c.PostgresDSN != ""
		hasHost := c.PostgresHost != ""
		if !hasDSN && !hasHost {
			return fmt.Errorf("postgres configuration requires either postgres-dsn or host details")
		}
		if c.PostgresPort <= 0 {
			return fmt.Errorf("postgres-port must be greater than 0")
		}
		if !c.IsDevelopment {
			if !hasDSN && c.PostgresPassword == "" {
				return fmt.Errorf("postgres password is required in production when postgres-dsn is not configured")
			}
			if !hasDSN {
				if err := validateProductionPostgresTLS(c.PostgresSSLMode, c.PostgresRootCertFile); err != nil {
					return err
				}
			} else if err := validateProductionPostgresDSN(c.PostgresDSN); err != nil {
				return err
			}
		}
	}

	return nil
}

func validateProductionPostgresDSN(dsn string) error {
	sslMode, rootCert, err := postgresDSNTLSSettings(dsn)
	if err != nil {
		return err
	}
	if sslMode == "" {
		sslMode = "require"
	}
	return validateProductionPostgresTLS(sslMode, rootCert)
}

func postgresDSNTLSSettings(dsn string) (string, string, error) {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		parsed, err := url.Parse(dsn)
		if err != nil {
			return "", "", fmt.Errorf("invalid postgres-dsn: %w", err)
		}
		return parsed.Query().Get("sslmode"), parsed.Query().Get("sslrootcert"), nil
	}

	parameters := make(map[string]string)
	for _, match := range postgresDSNParameterPattern.FindAllStringSubmatch(dsn, -1) {
		value := match[2]
		if value == "" {
			value = match[3]
		}
		if value == "" {
			value = match[4]
		}
		parameters[strings.ToLower(match[1])] = value
	}
	return parameters["sslmode"], parameters["sslrootcert"], nil
}

func validateProductionPostgresTLS(sslMode, rootCert string) error {
	switch strings.ToLower(sslMode) {
	case "require", "verify-ca":
		return nil
	case "verify-full":
		if rootCert == "" {
			return fmt.Errorf("postgres-root-cert is required with sslmode=verify-full in production")
		}
		return nil
	default:
		return fmt.Errorf("postgres sslmode must be require, verify-ca, or verify-full in production")
	}
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

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
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
