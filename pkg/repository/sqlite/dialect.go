package sqlite

import (
	"fmt"

	"github.com/adrien19/chronoqueue/pkg/repository/sql"
)

// Dialect implements sql.SQLDialect for SQLite databases.
// It provides SQLite-specific SQL syntax and capabilities.
type Dialect struct{}

// NewDialect creates a new SQLite dialect
func NewDialect() *Dialect {
	return &Dialect{}
}

// Ensure Dialect implements sql.SQLDialect
var _ sql.SQLDialect = (*Dialect)(nil)

// Query syntax methods

// Placeholder returns the SQLite positional parameter syntax
func (d *Dialect) Placeholder(n int) string {
	return "?" // SQLite uses ? for all parameters
}

// CurrentTimestamp returns the SQLite current timestamp function
func (d *Dialect) CurrentTimestamp() string {
	return "CURRENT_TIMESTAMP"
}

// JSONSet returns the SQLite JSON modification function
func (d *Dialect) JSONSet() string {
	return "json_set"
}

// JSONSetPath returns a JSON path for json_set
func (d *Dialect) JSONSetPath(key string) string {
	return fmt.Sprintf("'$.%s'", key)
}

// JSONExtract returns the SQLite JSON extraction function
func (d *Dialect) JSONExtract() string {
	return "json_extract"
}

// JSONExtractPath returns a JSON path argument for json_extract
func (d *Dialect) JSONExtractPath(key string) string {
	return fmt.Sprintf("'$.%s'", key)
}

// ToJSON converts a value to JSON (SQLite json_set accepts raw values directly)
func (d *Dialect) ToJSON(value string) string {
	return value // SQLite doesn't need to_jsonb(), json_set accepts values directly
}

// UnixMillis extracts unix milliseconds from a timestamp column
func (d *Dialect) UnixMillis(column string) string {
	// SQLite: Convert timestamp to unix milliseconds
	return fmt.Sprintf("CAST((julianday(%s) - 2440587.5) * 86400000 AS INTEGER)", column)
}

// Type mapping methods

// BlobType returns the SQLite BLOB type
func (d *Dialect) BlobType() string {
	return "BLOB"
}

// TimestampType returns the SQLite timestamp type
func (d *Dialect) TimestampType() string {
	return "TIMESTAMP"
}

// BigIntType returns the SQLite integer type (SQLite uses INTEGER for all integers)
func (d *Dialect) BigIntType() string {
	return "INTEGER"
}

// JSONType returns the SQLite JSON type (stored as TEXT)
func (d *Dialect) JSONType() string {
	return "TEXT"
}

// Feature support methods

// SupportsReturning indicates if RETURNING clause is supported
func (d *Dialect) SupportsReturning() bool {
	return true // SQLite 3.35+ supports RETURNING
}

// SupportsSkipLocked indicates if SELECT FOR UPDATE SKIP LOCKED is supported
func (d *Dialect) SupportsSkipLocked() bool {
	return false // SQLite does not support SKIP LOCKED
}

// SupportsAdvisoryLocks indicates if advisory locks are supported
func (d *Dialect) SupportsAdvisoryLocks() bool {
	return false // SQLite does not have advisory locks
}

// RequiresSerializableForClaim indicates if SERIALIZABLE isolation is needed for claiming
func (d *Dialect) RequiresSerializableForClaim() bool {
	return true // SQLite requires SERIALIZABLE to prevent concurrent claims
}

// Connection configuration methods

// SetWALMode returns the PRAGMA to enable WAL mode
func (d *Dialect) SetWALMode() string {
	return "PRAGMA journal_mode=WAL"
}

// SetBusyTimeout returns the PRAGMA to set busy timeout
func (d *Dialect) SetBusyTimeout(ms int) string {
	return fmt.Sprintf("PRAGMA busy_timeout=%d", ms)
}

// SetSynchronous returns the PRAGMA to set synchronous mode
func (d *Dialect) SetSynchronous(level string) string {
	return fmt.Sprintf("PRAGMA synchronous=%s", level)
}
