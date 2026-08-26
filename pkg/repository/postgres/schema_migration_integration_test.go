//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestSchemaMigration_FromV1ToLatest(t *testing.T) {
	ctx := context.Background()
	container, err := postgrescontainer.Run(
		ctx,
		"postgres:17-alpine",
		postgrescontainer.WithDatabase("chronoqueue"),
		postgrescontainer.WithUsername("chronoqueue"),
		postgrescontainer.WithPassword("chronoqueue"),
		postgrescontainer.BasicWaitStrategies(),
		testcontainers.WithTmpfs(map[string]string{"/var/lib/postgresql/data": "rw"}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(ctx)) })
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := OpenConnection(ctx, &ConnectionConfig{DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	statements := []string{
		`CREATE TABLE cq_schema_version (version INTEGER PRIMARY KEY, applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, description TEXT)`,
		`INSERT INTO cq_schema_version (version, description) VALUES (1, 'release fixture')`,
		`CREATE TABLE cq_schedules (id TEXT PRIMARY KEY, state INTEGER NOT NULL)`,
		`CREATE TABLE cq_queues (name TEXT PRIMARY KEY, metadata_pb BYTEA NOT NULL, created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL)`,
		`INSERT INTO cq_queues (name, metadata_pb, created_at, updated_at) VALUES ('queue-a', '\x00', 1, 1), ('queue-b', '\x00', 1, 1)`,
		`CREATE TABLE cq_messages (
			id BIGSERIAL PRIMARY KEY, queue_name TEXT NOT NULL, message_id TEXT NOT NULL UNIQUE,
			metadata_pb BYTEA NOT NULL, state INTEGER NOT NULL, priority INTEGER NOT NULL DEFAULT 5,
			scheduled_at BIGINT, lease_expiry BIGINT, heartbeat_expiry BIGINT, attempts_left INTEGER,
			max_attempts INTEGER, current_attempt_id TEXT, current_worker_id TEXT, lease_started_at BIGINT,
			lease_extension_used BIGINT DEFAULT 0, lease_renewal_count BIGINT DEFAULT 0,
			last_heartbeat_at BIGINT, created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL
		)`,
		`INSERT INTO cq_messages (queue_name, message_id, metadata_pb, state, priority, created_at, updated_at) VALUES ('queue-a', 'shared-id', '\x00', 1, 1, 1, 1)`,
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

	for table, expected := range map[string][]string{
		"cq_schedules": {"next_run", "last_run", "cron_schedule", "execution_count"},
		"cq_messages":  {"completed_at", "deleted_at", "cancellation_reason"},
	} {
		for _, column := range expected {
			var exists bool
			err := db.QueryRowContext(ctx, `SELECT EXISTS (
				SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = $2
			)`, table, column).Scan(&exists)
			require.NoError(t, err)
			assert.True(t, exists, "column %s.%s is missing", table, column)
		}
	}
	var schedulerIndexDefinition string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT indexdef FROM pg_indexes WHERE tablename = 'cq_messages' AND indexname = 'idx_messages_scheduler'`).Scan(&schedulerIndexDefinition))
	assert.Contains(t, schedulerIndexDefinition, "WHERE (state = 0)")
	assert.NotContains(t, schedulerIndexDefinition, "WHERE (state = 1)")

	_, err = db.ExecContext(ctx, `INSERT INTO cq_messages (queue_name, message_id, metadata_pb, state, priority, created_at, updated_at) VALUES ('queue-b', 'shared-id', '\x00', 1, 1, 1, 1)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO cq_messages (queue_name, message_id, metadata_pb, state, priority, created_at, updated_at) VALUES ('queue-a', 'shared-id', '\x00', 1, 1, 1, 1)`)
	require.Error(t, err)

	require.NoError(t, manager.Migrate(ctx, db, latestVersion))
	require.ErrorContains(t, manager.Migrate(ctx, db, latestVersion+1), "newer than supported")
	_, err = db.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES ($1, $2)`, latestVersion+1, "future release")
	require.NoError(t, err)
	require.ErrorContains(t, manager.Migrate(ctx, db, latestVersion), "newer than supported")
	require.ErrorContains(t, manager.Initialize(ctx, db), "newer than supported")
}
