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
	DSN             string
	Host            string
	Port            int
	User            string
	Password        string
	Database        string
	SSLMode         string
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

	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.Database, c.SSLMode,
	)
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
