package postgres

import (
	"fmt"

	repositorysql "github.com/adrien19/chronoqueue/pkg/repository/sql"
)

// Dialect implements sql.SQLDialect for PostgreSQL databases.
type Dialect struct{}

// NewDialect creates a new PostgreSQL dialect.
func NewDialect() *Dialect {
	return &Dialect{}
}

// Ensure Dialect implements repositorysql.SQLDialect.
var _ repositorysql.SQLDialect = (*Dialect)(nil)

// Placeholder returns the PostgreSQL positional parameter syntax.
func (d *Dialect) Placeholder(n int) string {
	return fmt.Sprintf("$%d", n)
}

// CurrentTimestamp returns the PostgreSQL current timestamp expression.
func (d *Dialect) CurrentTimestamp() string {
	return "NOW()"
}

// JSONSet returns the PostgreSQL JSON modification function.
func (d *Dialect) JSONSet() string {
	return "jsonb_set"
}

// JSONSetPath returns a JSON path for jsonb_set (text[] literal)
func (d *Dialect) JSONSetPath(key string) string {
	return fmt.Sprintf("'{%s}'::text[]", key)
}

// JSONExtract returns the PostgreSQL JSON extraction function.
func (d *Dialect) JSONExtract() string {
	return "jsonb_extract_path_text"
}

// JSONExtractPath returns a JSON path argument for jsonb_extract_path_text
func (d *Dialect) JSONExtractPath(key string) string {
	return fmt.Sprintf("'%s'", key)
}

// ToJSON converts a value to JSONB
func (d *Dialect) ToJSON(value string) string {
	return fmt.Sprintf("to_jsonb(%s)", value) // Postgres needs to_jsonb() to convert to JSONB
}

// UnixMillis extracts unix milliseconds from a timestamp expression.
func (d *Dialect) UnixMillis(column string) string {
	return fmt.Sprintf("(EXTRACT(EPOCH FROM %s) * 1000)::BIGINT", column)
}

// BlobType returns the PostgreSQL binary type.
func (d *Dialect) BlobType() string {
	return "BYTEA"
}

// TimestampType returns the PostgreSQL timestamp type.
func (d *Dialect) TimestampType() string {
	return "TIMESTAMPTZ"
}

// BigIntType returns the PostgreSQL big integer type.
func (d *Dialect) BigIntType() string {
	return "BIGINT"
}

// JSONType returns the PostgreSQL JSON type.
func (d *Dialect) JSONType() string {
	return "JSONB"
}

// SupportsReturning indicates RETURNING clause support.
func (d *Dialect) SupportsReturning() bool {
	return true
}

// SupportsSkipLocked indicates SKIP LOCKED support.
func (d *Dialect) SupportsSkipLocked() bool {
	return true
}

// SupportsAdvisoryLocks indicates advisory lock support.
func (d *Dialect) SupportsAdvisoryLocks() bool {
	return true
}

// RequiresSerializableForClaim indicates if SERIALIZABLE isolation is required.
func (d *Dialect) RequiresSerializableForClaim() bool {
	return false
}

// SetWALMode returns an empty string; not applicable to PostgreSQL.
func (d *Dialect) SetWALMode() string {
	return ""
}

// SetBusyTimeout returns an empty string; not applicable to PostgreSQL.
func (d *Dialect) SetBusyTimeout(ms int) string {
	return ""
}

// SetSynchronous returns an empty string; not applicable to PostgreSQL.
func (d *Dialect) SetSynchronous(level string) string {
	return ""
}
