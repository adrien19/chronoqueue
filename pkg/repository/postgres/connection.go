package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq" // postgres driver
)

// ConnectionConfig holds PostgreSQL connection configuration.
type ConnectionConfig struct {
	DSN      string
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string
	// PostgreSQL Client Certificate Configuration (for mTLS with database)
	ClientCertFile  string // Path to PostgreSQL client certificate file
	ClientKeyFile   string // Path to PostgreSQL client key file
	RootCertFile    string // Path to PostgreSQL root CA certificate file
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// DefaultConnectionConfig returns a baseline configuration for local development.
func DefaultConnectionConfig() *ConnectionConfig {
	return &ConnectionConfig{
		Host:            "localhost",
		Port:            5432,
		User:            "postgres",
		Password:        "postgres",
		Database:        "chronoqueue",
		SSLMode:         "disable",
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	}
}

func (c *ConnectionConfig) dsn() string {
	if c.DSN != "" {
		return c.DSN
	}

	// Build base DSN
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.Database, c.SSLMode,
	)

	// Add client certificate parameters if provided
	if c.ClientCertFile != "" {
		dsn += fmt.Sprintf(" sslcert=%s", c.ClientCertFile)
	}
	if c.ClientKeyFile != "" {
		dsn += fmt.Sprintf(" sslkey=%s", c.ClientKeyFile)
	}
	if c.RootCertFile != "" {
		dsn += fmt.Sprintf(" sslrootcert=%s", c.RootCertFile)
	}

	return dsn
}

// OpenConnection opens and configures a PostgreSQL database connection.
func OpenConnection(ctx context.Context, config *ConnectionConfig) (*sql.DB, error) {
	if config == nil {
		return nil, fmt.Errorf("connection config is required")
	}

	db, err := sql.Open("postgres", config.dsn())
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	if config.MaxOpenConns > 0 {
		db.SetMaxOpenConns(config.MaxOpenConns)
	}
	if config.MaxIdleConns > 0 {
		db.SetMaxIdleConns(config.MaxIdleConns)
	}
	if config.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(config.ConnMaxLifetime)
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return db, nil
}
