package sql

import (
	"context"
	"database/sql"

	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
	"github.com/adrien19/chronoqueue/pkg/log"
	"github.com/adrien19/chronoqueue/pkg/repository/common"
)

// BaseSQL provides common SQL storage functionality shared across SQL backends.
// This eliminates code duplication between SQLite, Postgres, MySQL, etc.
type BaseSQL struct {
	DB           *sql.DB
	Logger       *log.Logger
	Serializer   *common.ProtoSerializer
	KeyManager   *keymanager.EncryptionKeyManager
	Dialect      SQLDialect
	Clock        *Clock
	StateManager *StateManager
	LeaseRuntime *LeaseRuntimeCalculator
}

// NewBaseSQL creates a new BaseSQL instance
func NewBaseSQL(
	db *sql.DB,
	logger *log.Logger,
	keyManager *keymanager.EncryptionKeyManager,
	dialect SQLDialect,
) *BaseSQL {
	return &BaseSQL{
		DB:           db,
		Logger:       logger,
		Serializer:   common.NewProtoSerializer(),
		KeyManager:   keyManager,
		Dialect:      dialect,
		Clock:        NewClock(),
		StateManager: NewStateManager(dialect),
		LeaseRuntime: NewLeaseRuntimeCalculator(NewClock()),
	}
}

// SQLDialect abstracts database-specific SQL syntax and capabilities.
// Each SQL backend (SQLite, Postgres, MySQL) implements this interface.
type SQLDialect interface {
	// Query syntax
	Placeholder(n int) string          // Positional parameter syntax: $1 (Postgres) vs ? (SQLite/MySQL)
	CurrentTimestamp() string          // CURRENT_TIMESTAMP vs NOW()
	JSONSet() string                   // json_set (SQLite) vs jsonb_set (Postgres)
	JSONSetPath(key string) string     // Path argument for JSONSet
	JSONExtract() string               // json_extract (SQLite) vs jsonb_extract_path_text (Postgres)
	JSONExtractPath(key string) string // Path argument for JSONExtract
	ToJSON(value string) string        // Convert value to JSON: raw value (SQLite) vs to_jsonb() (Postgres)
	UnixMillis(column string) string   // Extract unix milliseconds from timestamp column

	// Type mappings
	BlobType() string      // BLOB vs BYTEA
	TimestampType() string // TIMESTAMP vs TIMESTAMPTZ
	BigIntType() string    // INTEGER vs BIGINT
	JSONType() string      // TEXT vs JSONB

	// Feature support
	SupportsReturning() bool            // RETURNING clause support
	SupportsSkipLocked() bool           // SELECT FOR UPDATE SKIP LOCKED
	SupportsAdvisoryLocks() bool        // Postgres advisory locks
	RequiresSerializableForClaim() bool // Needs SERIALIZABLE isolation for claiming

	// Connection configuration
	SetWALMode() string                 // SQLite: PRAGMA journal_mode=WAL
	SetBusyTimeout(ms int) string       // SQLite: PRAGMA busy_timeout
	SetSynchronous(level string) string // PRAGMA synchronous
}

// Config holds SQL storage configuration
type Config struct {
	// Connection pooling
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime int // seconds

	// SQLite-specific
	BusyTimeout int // milliseconds
	WALMode     bool
	Synchronous string // NORMAL, FULL, etc.
}

// ApplyConfig applies configuration to the database connection
func (b *BaseSQL) ApplyConfig(ctx context.Context, cfg *Config) error {
	// Set connection pool limits
	if cfg.MaxOpenConns > 0 {
		b.DB.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		b.DB.SetMaxIdleConns(cfg.MaxIdleConns)
	}

	// SQLite-specific configuration
	if cfg.WALMode {
		if walCmd := b.Dialect.SetWALMode(); walCmd != "" {
			if _, err := b.DB.ExecContext(ctx, walCmd); err != nil {
				return err
			}
		}
	}

	if cfg.BusyTimeout > 0 {
		if timeoutCmd := b.Dialect.SetBusyTimeout(cfg.BusyTimeout); timeoutCmd != "" {
			if _, err := b.DB.ExecContext(ctx, timeoutCmd); err != nil {
				return err
			}
		}
	}

	if cfg.Synchronous != "" {
		if syncCmd := b.Dialect.SetSynchronous(cfg.Synchronous); syncCmd != "" {
			if _, err := b.DB.ExecContext(ctx, syncCmd); err != nil {
				return err
			}
		}
	}

	return nil
}

// Close closes the database connection
func (b *BaseSQL) Close() error {
	return b.DB.Close()
}
