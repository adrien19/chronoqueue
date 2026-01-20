package sqlite

import (
	"context"
	"fmt"

	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
	"github.com/adrien19/chronoqueue/pkg/log"
	repositorysql "github.com/adrien19/chronoqueue/pkg/repository/sql"
)

// Storage implements the persistence.Storage interface for SQLite
type Storage struct {
	*repositorysql.BaseSQL
	schema *SchemaManager
}

// Config holds SQLite storage configuration
type Config struct {
	Path       string
	Logger     *log.Logger
	KeyManager *keymanager.EncryptionKeyManager
}

// NewStorage creates a new SQLite storage instance
func NewStorage(ctx context.Context, config *Config) (*Storage, error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}

	// Open database connection
	connConfig := DefaultConnectionConfig(config.Path)
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

// nowMs returns the current time in milliseconds from the clock.
func (s *Storage) nowMs() int64 {
	return s.Clock.NowMs()
}
