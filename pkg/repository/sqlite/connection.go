package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// ConnectionConfig holds SQLite connection configuration
type ConnectionConfig struct {
	Path            string        // Database file path or ":memory:"
	MaxOpenConns    int           // Maximum number of open connections (default: 1 for SQLite)
	MaxIdleConns    int           // Maximum number of idle connections
	ConnMaxLifetime time.Duration // Maximum connection lifetime
	BusyTimeout     time.Duration // Busy timeout (default: 30s)
	Synchronous     string        // Synchronous mode: OFF, NORMAL, FULL (default: NORMAL)
}

// DefaultConnectionConfig returns default SQLite connection configuration
func DefaultConnectionConfig(path string) *ConnectionConfig {
	return &ConnectionConfig{
		Path:            path,
		MaxOpenConns:    1, // SQLite: single writer
		MaxIdleConns:    1,
		ConnMaxLifetime: time.Hour,
		BusyTimeout:     30 * time.Second,
		Synchronous:     "NORMAL",
	}
}

// OpenConnection opens and configures a SQLite database connection
func OpenConnection(ctx context.Context, config *ConnectionConfig) (*sql.DB, error) {
	if config == nil {
		return nil, fmt.Errorf("connection config is required")
	}

	// Open database with additional parameters
	// _journal_mode=WAL: Write-Ahead Logging for better concurrency
	// _busy_timeout: Wait time in milliseconds when database is locked
	// _foreign_keys=1: Enable foreign key constraints
	dsn := fmt.Sprintf(
		"%s?_journal_mode=WAL&_busy_timeout=%d&_foreign_keys=1",
		config.Path,
		config.BusyTimeout.Milliseconds(),
	)

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)

	// Verify connection
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	// Configure pragmas
	if err := configurePragmas(ctx, db, config); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure pragmas: %w", err)
	}

	return db, nil
}

// configurePragmas sets SQLite pragmas for optimal performance
func configurePragmas(ctx context.Context, db *sql.DB, config *ConnectionConfig) error {
	pragmas := []string{
		fmt.Sprintf("PRAGMA synchronous = %s", config.Synchronous),
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 268435456", // 256MB memory-mapped I/O
	}

	for _, pragma := range pragmas {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("execute pragma %s: %w", pragma, err)
		}
	}

	return nil
}
