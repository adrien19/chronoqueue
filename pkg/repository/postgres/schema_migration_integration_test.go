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
		`CREATE TABLE cq_messages (id BIGSERIAL PRIMARY KEY, queue_name TEXT NOT NULL, state INTEGER NOT NULL)`,
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
}
