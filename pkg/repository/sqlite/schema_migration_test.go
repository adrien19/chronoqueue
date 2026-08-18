//go:build sqlite && cgo

package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaMigration_FromV1ToLatest(t *testing.T) {
	ctx := context.Background()
	db, err := OpenConnection(ctx, DefaultConnectionConfig(filepath.Join(t.TempDir(), "migration.db")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	statements := []string{
		`CREATE TABLE cq_schema_version (version INTEGER PRIMARY KEY, applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, description TEXT)`,
		`INSERT INTO cq_schema_version (version, description) VALUES (1, 'release fixture')`,
		`CREATE TABLE cq_schedules (id TEXT PRIMARY KEY, state INTEGER NOT NULL)`,
		`CREATE TABLE cq_messages (id INTEGER PRIMARY KEY, queue_name TEXT NOT NULL, state INTEGER NOT NULL)`,
	}
	for _, statement := range statements {
		_, err := db.ExecContext(ctx, statement)
		require.NoError(t, err)
	}

	manager := NewSchemaManager()
	require.NoError(t, manager.Migrate(ctx, db, latestVersion))
	version, exists, err := manager.Version(ctx, db)
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, latestVersion, version)

	assertSQLiteColumns(t, ctx, db, "cq_schedules", "next_run", "last_run", "cron_schedule", "execution_count")
	assertSQLiteColumns(t, ctx, db, "cq_messages", "completed_at", "deleted_at", "cancellation_reason")
}

func TestSchemaMigration_RollsBackFailedVersion(t *testing.T) {
	ctx := context.Background()
	db, err := OpenConnection(ctx, DefaultConnectionConfig(filepath.Join(t.TempDir(), "migration.db")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	statements := []string{
		`CREATE TABLE cq_schema_version (version INTEGER PRIMARY KEY, applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, description TEXT)`,
		`INSERT INTO cq_schema_version (version, description) VALUES (1, 'release fixture')`,
		`CREATE TABLE cq_schedules (id TEXT PRIMARY KEY, state INTEGER NOT NULL, last_run INTEGER)`,
	}
	for _, statement := range statements {
		_, err := db.ExecContext(ctx, statement)
		require.NoError(t, err)
	}

	manager := NewSchemaManager()
	err = manager.Migrate(ctx, db, 2)
	require.ErrorContains(t, err, "migrate to version 2: execute migration statement")
	require.ErrorContains(t, err, "duplicate column name: last_run")

	version, exists, err := manager.Version(ctx, db)
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, uint(1), version)

	columns := sqliteColumns(t, ctx, db, "cq_schedules")
	assert.False(t, columns["next_run"], "the first migration statement must be rolled back")
	assert.True(t, columns["last_run"], "the pre-migration schema must be preserved")
}

func assertSQLiteColumns(t *testing.T, ctx context.Context, db queryer, table string, expected ...string) {
	t.Helper()
	columns := sqliteColumns(t, ctx, db, table)
	for _, name := range expected {
		assert.True(t, columns[name], "column %s.%s is missing", table, name)
	}
}

func sqliteColumns(t *testing.T, ctx context.Context, db queryer, table string) map[string]bool {
	t.Helper()
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	columns := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue interface{}
		require.NoError(t, rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey))
		columns[name] = true
	}
	require.NoError(t, rows.Err())
	return columns
}

type queryer interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}
