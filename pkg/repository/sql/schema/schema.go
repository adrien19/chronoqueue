package schema

import (
	"context"
	"database/sql"
)

// Manager handles schema initialization and migrations for SQL storage backends.
// Each backend (SQLite, Postgres) implements this interface.
type Manager interface {
	// Initialize creates the initial schema if it doesn't exist
	Initialize(ctx context.Context, db *sql.DB) error
	// Migrate runs migrations to bring schema to target version
	Migrate(ctx context.Context, db *sql.DB, targetVersion uint) error
	// Version returns the current schema version
	Version(ctx context.Context, db *sql.DB) (uint, bool, error)
	// Validate checks that the schema is in a valid state
	Validate(ctx context.Context, db *sql.DB) error
}

// BaseManager provides common schema management functionality
type BaseManager struct {
	versionTable string
}

// NewBaseManager creates a new base schema manager
func NewBaseManager() *BaseManager {
	return &BaseManager{
		versionTable: "cq_schema_version",
	}
}

// EnsureVersionTable creates the schema version table if it doesn't exist
func (m *BaseManager) EnsureVersionTable(ctx context.Context, db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS cq_schema_version (
			version INTEGER PRIMARY KEY,
			applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			description TEXT
		)
	`
	_, err := db.ExecContext(ctx, query)
	return err
}

// GetVersion retrieves the current schema version
func (m *BaseManager) GetVersion(ctx context.Context, db *sql.DB) (uint, bool, error) {
	if err := m.EnsureVersionTable(ctx, db); err != nil {
		return 0, false, err
	}

	var version uint
	err := db.QueryRowContext(ctx, `
		SELECT version FROM cq_schema_version ORDER BY version DESC LIMIT 1
	`).Scan(&version)
	if err == sql.ErrNoRows {
		return 0, false, nil // No version found, schema not initialized
	}
	if err != nil {
		return 0, false, err
	}
	return version, true, nil
}

// SetVersion records a new schema version
func (m *BaseManager) SetVersion(ctx context.Context, db *sql.DB, version uint, description string) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO cq_schema_version (version, description) VALUES (?, ?)
	`, version, description)
	return err
}
