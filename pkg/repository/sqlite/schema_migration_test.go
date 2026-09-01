//go:build sqlite && cgo

package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
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
		`CREATE TABLE cq_queues (name TEXT PRIMARY KEY, metadata_pb BLOB NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
		`INSERT INTO cq_queues (name, metadata_pb, created_at, updated_at) VALUES ('queue-a', X'', 1, 1), ('queue-b', X'', 1, 1)`,
		`CREATE TABLE cq_messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT, queue_name TEXT NOT NULL, message_id TEXT NOT NULL UNIQUE,
			metadata_pb BLOB NOT NULL, state INTEGER NOT NULL, priority INTEGER NOT NULL DEFAULT 5,
			scheduled_at INTEGER, lease_expiry INTEGER, heartbeat_expiry INTEGER, attempts_left INTEGER,
			max_attempts INTEGER, current_attempt_id TEXT, current_worker_id TEXT, lease_started_at INTEGER,
			lease_extension_used INTEGER DEFAULT 0, lease_renewal_count INTEGER DEFAULT 0,
			last_heartbeat_at INTEGER, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		)`,
		`INSERT INTO cq_messages (queue_name, message_id, metadata_pb, state, priority, created_at, updated_at) VALUES ('queue-a', 'shared-id', X'00', 1, 1, 1, 1)`,
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
	assertSQLiteColumns(t, ctx, db, "cq_schedule_history", "message_pb")
	assertSQLiteColumns(t, ctx, db, "cq_queues", "dead_letter_queue_name")
	var archiveTableCount int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'cq_schedule_archive'`).Scan(&archiveTableCount))
	assert.Equal(t, 1, archiveTableCount)
	var schedulerIndexSQL string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'idx_messages_scheduler'`).Scan(&schedulerIndexSQL))
	assert.Contains(t, schedulerIndexSQL, "WHERE state = 0")
	assert.NotContains(t, schedulerIndexSQL, "WHERE state = 1")
	_, err = db.ExecContext(ctx, `INSERT INTO cq_messages (queue_name, message_id, metadata_pb, state, priority, created_at, updated_at) VALUES ('queue-b', 'shared-id', X'00', 1, 1, 1, 1)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO cq_messages (queue_name, message_id, metadata_pb, state, priority, created_at, updated_at) VALUES ('queue-a', 'shared-id', X'00', 1, 1, 1, 1)`)
	require.Error(t, err)
}

func TestSchemaMigration_V10PreservesFractionalTimestampsAndExcludesDeletedMessages(t *testing.T) {
	ctx := context.Background()
	db, err := OpenConnection(ctx, DefaultConnectionConfig(filepath.Join(t.TempDir(), "v10-migration.db")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	statements := []string{
		`CREATE TABLE cq_schema_version (version INTEGER PRIMARY KEY, applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, description TEXT)`,
		`INSERT INTO cq_schema_version (version, description) VALUES (9, 'v9 fixture')`,
		`CREATE TABLE cq_queues (name TEXT PRIMARY KEY, state_counts TEXT DEFAULT '{}')`,
		`INSERT INTO cq_queues (name) VALUES ('queue-a')`,
		`CREATE TABLE cq_messages (id INTEGER PRIMARY KEY, queue_name TEXT NOT NULL, state INTEGER NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, deleted_at INTEGER)`,
		`INSERT INTO cq_messages (id, queue_name, state, created_at, updated_at) VALUES (1, 'queue-a', 1, '2026-08-31 21:01:04.678', '2026-08-31 21:01:05.987')`,
		`INSERT INTO cq_messages (id, queue_name, state, created_at, updated_at, deleted_at) VALUES (2, 'queue-a', 1, 1, 1, 2)`,
	}
	for _, statement := range statements {
		_, err := db.ExecContext(ctx, statement)
		require.NoError(t, err)
	}

	require.NoError(t, NewSchemaManager().Migrate(ctx, db, latestVersion))
	var createdAt, updatedAt, pendingCount int64
	require.NoError(t, db.QueryRowContext(ctx, `SELECT created_at, updated_at FROM cq_messages WHERE id = 1`).Scan(&createdAt, &updatedAt))
	require.Equal(t, time.Date(2026, time.August, 31, 21, 1, 4, 678_000_000, time.UTC).UnixMilli(), createdAt)
	require.Equal(t, time.Date(2026, time.August, 31, 21, 1, 5, 987_000_000, time.UTC).UnixMilli(), updatedAt)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT json_extract(state_counts, '$.pending') FROM cq_queues WHERE name = 'queue-a'`).Scan(&pendingCount))
	require.EqualValues(t, 1, pendingCount)
}

func TestSchemaMigration_V9BackfillsDLQRelationship(t *testing.T) {
	ctx := context.Background()
	db, err := OpenConnection(ctx, DefaultConnectionConfig(filepath.Join(t.TempDir(), "dlq-migration.db")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.ExecContext(ctx, `CREATE TABLE cq_schema_version (version INTEGER PRIMARY KEY, applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, description TEXT)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES (8, 'pre-DLQ-index fixture')`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE cq_queues (name TEXT PRIMARY KEY, metadata_pb BLOB NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`)
	require.NoError(t, err)
	queueBytes, err := proto.Marshal(&queuepb.Queue{Name: "source", Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "source-dlq"}})
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO cq_queues (name, metadata_pb, created_at, updated_at) VALUES (?, ?, 1, 1)`, "source", queueBytes)
	require.NoError(t, err)

	require.NoError(t, NewSchemaManager().Migrate(ctx, db, latestVersion))
	var dlqName string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT dead_letter_queue_name FROM cq_queues WHERE name = ?`, "source").Scan(&dlqName))
	require.Equal(t, "source-dlq", dlqName)
	var indexCount int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_queues_dead_letter_queue_name'`).Scan(&indexCount))
	require.Equal(t, 1, indexCount)
}

func TestSchemaMigration_V9RejectsInvalidQueueMetadataAndRollsBack(t *testing.T) {
	ctx := context.Background()
	db, err := OpenConnection(ctx, DefaultConnectionConfig(filepath.Join(t.TempDir(), "invalid-dlq-migration.db")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.ExecContext(ctx, `CREATE TABLE cq_schema_version (version INTEGER PRIMARY KEY, applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, description TEXT)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES (8, 'pre-DLQ-index fixture')`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE cq_queues (name TEXT PRIMARY KEY, metadata_pb BLOB NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO cq_queues (name, metadata_pb, created_at, updated_at) VALUES ('broken', X'00', 1, 1)`)
	require.NoError(t, err)

	err = NewSchemaManager().Migrate(ctx, db, latestVersion)
	require.ErrorContains(t, err, `unmarshal queue "broken"`)
	version, exists, err := NewSchemaManager().Version(ctx, db)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint(8), version)
}

func TestSchemaMigration_V7RollsBackWithoutMessagesTable(t *testing.T) {
	ctx := context.Background()
	db, err := OpenConnection(ctx, DefaultConnectionConfig(filepath.Join(t.TempDir(), "migration.db")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	_, err = db.ExecContext(ctx, `CREATE TABLE cq_schema_version (version INTEGER PRIMARY KEY, applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, description TEXT)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES (6, 'fixture without messages table')`)
	require.NoError(t, err)

	manager := NewSchemaManager()
	err = manager.Migrate(ctx, db, 7)
	require.ErrorContains(t, err, "migrate to version 7: create scheduler index")

	version, exists, err := manager.Version(ctx, db)
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, uint(6), version)
}

func TestSchemaMigration_V8PreservesExistingScheduleHistory(t *testing.T) {
	ctx := context.Background()
	db, err := OpenConnection(ctx, DefaultConnectionConfig(filepath.Join(t.TempDir(), "migration.db")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	statements := []string{
		`CREATE TABLE cq_schema_version (version INTEGER PRIMARY KEY, applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, description TEXT)`,
		`INSERT INTO cq_schema_version (version, description) VALUES (7, 'v7 fixture')`,
		`CREATE TABLE cq_schedules (id TEXT PRIMARY KEY, queue_name TEXT NOT NULL)`,
		`CREATE TABLE cq_messages (id INTEGER PRIMARY KEY, queue_name TEXT NOT NULL, message_id TEXT NOT NULL, metadata_pb BLOB NOT NULL)`,
		`CREATE TABLE cq_schedule_history (id INTEGER PRIMARY KEY AUTOINCREMENT, schedule_id TEXT NOT NULL, message_id TEXT NOT NULL, executed_at INTEGER NOT NULL, success INTEGER NOT NULL, error_message TEXT, FOREIGN KEY (schedule_id) REFERENCES cq_schedules(id) ON DELETE CASCADE)`,
		`CREATE INDEX idx_schedule_history_schedule ON cq_schedule_history(schedule_id, executed_at DESC)`,
		`INSERT INTO cq_schedules (id, queue_name) VALUES ('schedule-a', 'queue-a')`,
		`INSERT INTO cq_messages (id, queue_name, message_id, metadata_pb) VALUES (1, 'queue-a', 'message-a', X'0102')`,
		`INSERT INTO cq_schedule_history (schedule_id, message_id, executed_at, success) VALUES ('schedule-a', 'message-a', 1234, 1)`,
	}
	for _, statement := range statements {
		_, err := db.ExecContext(ctx, statement)
		require.NoError(t, err)
	}

	manager := NewSchemaManager()
	require.NoError(t, manager.Migrate(ctx, db, 8))
	var snapshot []byte
	require.NoError(t, db.QueryRowContext(ctx, `SELECT message_pb FROM cq_schedule_history WHERE schedule_id = 'schedule-a'`).Scan(&snapshot))
	assert.Equal(t, []byte{1, 2}, snapshot)
	_, err = db.ExecContext(ctx, `DELETE FROM cq_schedules WHERE id = 'schedule-a'`)
	require.NoError(t, err)
	var historyCount int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM cq_schedule_history WHERE schedule_id = 'schedule-a'`).Scan(&historyCount))
	assert.Equal(t, 1, historyCount)
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

func TestSchemaMigration_RejectsFutureVersionAndRepeatedCurrentMigration(t *testing.T) {
	ctx := context.Background()
	db, err := OpenConnection(ctx, DefaultConnectionConfig(filepath.Join(t.TempDir(), "migration.db")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	manager := NewSchemaManager()
	require.NoError(t, manager.Initialize(ctx, db))
	require.NoError(t, manager.Migrate(ctx, db, latestVersion))
	require.NoError(t, manager.Migrate(ctx, db, latestVersion))
	require.ErrorContains(t, manager.Migrate(ctx, db, latestVersion+1), "newer than supported")

	_, err = db.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES (?, ?)`, latestVersion+1, "future release")
	require.NoError(t, err)
	require.ErrorContains(t, manager.Migrate(ctx, db, latestVersion), "newer than supported")
	require.ErrorContains(t, manager.Initialize(ctx, db), "newer than supported")
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
