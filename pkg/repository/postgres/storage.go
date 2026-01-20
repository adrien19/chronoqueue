package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
	"github.com/adrien19/chronoqueue/pkg/log"
	repositorysql "github.com/adrien19/chronoqueue/pkg/repository/sql"
)

// Storage implements the persistence.Storage interface for Postgres
type Storage struct {
	*repositorysql.BaseSQL
	schema *SchemaManager
}

// Config holds Postgres storage configuration
type Config struct {
	// Conn carries connection details (DSN overrides discrete fields when set).
	Conn ConnectionConfig

	Logger     *log.Logger
	KeyManager *keymanager.EncryptionKeyManager
}

func (c *Config) connectionConfig() *ConnectionConfig {
	connConfig := DefaultConnectionConfig()

	if c == nil {
		return connConfig
	}

	// Allow legacy DSN override while preferring discrete fields
	if c.Conn.DSN != "" {
		connConfig.DSN = c.Conn.DSN
	}
	if c.Conn.Host != "" {
		connConfig.Host = c.Conn.Host
	}
	if c.Conn.Port != 0 {
		connConfig.Port = c.Conn.Port
	}
	if c.Conn.User != "" {
		connConfig.User = c.Conn.User
	}
	if c.Conn.Password != "" {
		connConfig.Password = c.Conn.Password
	}
	if c.Conn.Database != "" {
		connConfig.Database = c.Conn.Database
	}
	if c.Conn.SSLMode != "" {
		connConfig.SSLMode = c.Conn.SSLMode
	}
	if c.Conn.MaxOpenConns > 0 {
		connConfig.MaxOpenConns = c.Conn.MaxOpenConns
	}
	if c.Conn.MaxIdleConns > 0 {
		connConfig.MaxIdleConns = c.Conn.MaxIdleConns
	}
	if c.Conn.ConnMaxLifetime > 0 {
		connConfig.ConnMaxLifetime = c.Conn.ConnMaxLifetime
	}

	return connConfig
}

// NewStorage creates a new Postgres storage instance
func NewStorage(ctx context.Context, config *Config) (*Storage, error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}

	// Open database connection
	connConfig := config.connectionConfig()

	db, err := OpenConnection(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}

	// Create schema manager
	schemaManager := NewSchemaManager()

	// Ensure version table exists
	if err := schemaManager.EnsureVersionTable(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ensure version table: %w", err)
	}

	// Initialize schema if needed
	if err := schemaManager.Initialize(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize schema: %w", err)
	}

	if err := schemaManager.Migrate(ctx, db, latestVersion); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate schema: %w", err)
	}

	// Create dialect
	dialect := NewDialect()

	// Create base SQL instance
	baseSQL := repositorysql.NewBaseSQL(db, config.Logger, config.KeyManager, dialect)

	storage := &Storage{
		BaseSQL: baseSQL,
		schema:  schemaManager,
	}

	return storage, nil
}

// Close closes the storage
func (s *Storage) Close() error {
	return s.BaseSQL.Close()
}

// ph replaces ? placeholders with Postgres $N placeholders
func (s *Storage) ph(query string) string {
	var b strings.Builder
	idx := 1
	for _, r := range query {
		if r == '?' {
			b.WriteString(s.Dialect.Placeholder(idx))
			idx++
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// nowMs returns the current time in milliseconds
func (s *Storage) nowMs() int64 {
	return s.Clock.NowMs()
}
