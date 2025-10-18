package server

import (
	"fmt"
	"time"
)

// Config holds the complete server configuration
type Config struct {
	// Network Configuration
	GRPCAddr string
	HTTPAddr string

	// Storage Configuration
	RedisAddr string

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

	// Development/Runtime Configuration
	IsDevelopment bool
}

// DefaultConfig returns a configuration suitable for development
func DefaultConfig() *Config {
	return &Config{
		GRPCAddr:      ":9000",
		HTTPAddr:      ":8080",
		RedisAddr:     "localhost:6379",
		LogLevel:      "info",
		LogFormat:     "text",
		EnableTLS:     false,
		EnableCORS:    true,
		AllowOrigins:  []string{"*"},
		IsDevelopment: true,
	}
}

// ProductionConfig returns a configuration suitable for production
func ProductionConfig() *Config {
	return &Config{
		GRPCAddr:      ":9000",
		HTTPAddr:      ":8080",
		RedisAddr:     "localhost:6379",
		LogLevel:      "info",
		LogFormat:     "json",
		EnableTLS:     false,
		EnableCORS:    false,
		AllowOrigins:  []string{},
		IsDevelopment: false,
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
		return fmt.Errorf("Redis address cannot be empty")
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
