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
	RedisAddr     string
	RedisPassword string
	RedisUsername string // For Redis 6+ ACL
	RedisDB       int
	RedisTLS      bool

	// Logging Configuration
	LogLevel  string
	LogFormat string

	// TLS Configuration
	EnableTLS  bool
	CertFile   string
	KeyFile    string
	CACertFile string

	// HTTP Gateway Configuration
	EnableCORS   bool
	AllowOrigins []string

	// Background Services Configuration
	SchedulerIntervalMs int // Scheduler interval in milliseconds (default: 1000ms)
	ReclaimIntervalMs   int // Reclaim service interval in milliseconds (default: 5000ms)

	// Development/Runtime Configuration
	IsDevelopment bool
}

// DefaultConfig returns a configuration suitable for development
func DefaultConfig() *Config {
	return &Config{
		GRPCAddr:            ":9000",
		HTTPAddr:            ":8080",
		RedisAddr:           getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:       getEnv("REDIS_PASSWORD", ""),
		RedisUsername:       getEnv("REDIS_USERNAME", ""),
		RedisDB:             getEnvInt("REDIS_DB", 0),
		RedisTLS:            getEnvBool("REDIS_TLS_ENABLED", false),
		LogLevel:            "info",
		LogFormat:           "text",
		EnableTLS:           false,
		EnableCORS:          true,
		AllowOrigins:        []string{"*"},
		SchedulerIntervalMs: getEnvInt("SCHEDULER_INTERVAL_MS", 1000),
		ReclaimIntervalMs:   getEnvInt("RECLAIM_INTERVAL_MS", 5000),
		IsDevelopment:       true,
	}
}

// ProductionConfig returns a configuration suitable for production
func ProductionConfig() *Config {
	return &Config{
		GRPCAddr:            ":9000",
		HTTPAddr:            ":8080",
		RedisAddr:           getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:       getEnv("REDIS_PASSWORD", ""),
		RedisUsername:       getEnv("REDIS_USERNAME", ""),
		RedisDB:             getEnvInt("REDIS_DB", 0),
		RedisTLS:            getEnvBool("REDIS_TLS_ENABLED", false),
		LogLevel:            "info",
		LogFormat:           "json",
		EnableTLS:           false,
		EnableCORS:          false,
		AllowOrigins:        []string{},
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

	if c.RedisAddr == "" {
		return fmt.Errorf("redis address cannot be empty")
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
