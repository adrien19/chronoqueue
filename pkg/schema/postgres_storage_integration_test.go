//go:build integration

package schema

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"

	schemapb "github.com/adrien19/chronoqueue/api/schema/v1"
	"github.com/adrien19/chronoqueue/pkg/log"
)

func TestPostgresRegistryRoundTripsMetadata(t *testing.T) {
	ctx := context.Background()
	container, err := postgrescontainer.Run(ctx, "postgres:17-alpine",
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
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	registry, err := NewPostgresRegistry(db, log.NewLogger())
	require.NoError(t, err)
	want := map[string]string{"owner": "checkout", "contact": "checkout@example.com"}
	_, err = registry.Register(ctx, &schemapb.Schema{
		SchemaId: "events", Name: "Events", Content: `{"type":"object"}`, Metadata: want,
	})
	require.NoError(t, err)
	got, err := registry.Get(ctx, "events", 1)
	require.NoError(t, err)
	require.Equal(t, want, got.GetMetadata())
	listed, err := registry.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, want, listed[0].GetMetadata())

	_, err = db.ExecContext(ctx, `UPDATE cq_schemas SET metadata_json = '{' WHERE schema_id = $1 AND version = $2`, "events", 1)
	require.NoError(t, err)
	_, err = registry.Get(ctx, "events", 1)
	require.ErrorContains(t, err, "decode schema metadata")
}

func TestPostgresRegistryActiveSelectionFallsBackFromInactiveLatestVersion(t *testing.T) {
	ctx := context.Background()
	container, err := postgrescontainer.Run(ctx, "postgres:17-alpine",
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
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	registry, err := NewPostgresRegistry(db, log.NewLogger())
	require.NoError(t, err)
	for range 2 {
		_, err := registry.Register(ctx, &schemapb.Schema{
			SchemaId: "active-fallback", Name: "Active fallback", Content: `{"type":"object"}`,
		})
		require.NoError(t, err)
	}
	_, err = registry.Deactivate(ctx, "active-fallback", 2)
	require.NoError(t, err)

	latest, err := registry.GetLatest(ctx, "active-fallback")
	require.NoError(t, err)
	require.Equal(t, int32(1), latest.GetVersion())
	listed, err := registry.ListWithOptions(ctx, ListOptions{Prefix: "active-fallback", Limit: 10, ActiveOnly: true})
	require.NoError(t, err)
	require.Len(t, listed.Schemas, 1)
	require.Equal(t, int32(1), listed.Schemas[0].GetVersion())
	require.Equal(t, int32(2), listed.Metadata["active-fallback"].TotalVersions)
	emptyPage, err := registry.ListWithOptions(ctx, ListOptions{Prefix: "active-fallback", Limit: 1, Offset: 1, ActiveOnly: true})
	require.NoError(t, err)
	require.Empty(t, emptyPage.Schemas)
	require.Equal(t, int32(1), emptyPage.TotalCount)

	_, err = registry.Deactivate(ctx, "active-fallback", 1)
	require.NoError(t, err)
	_, err = registry.GetLatest(ctx, "active-fallback")
	require.Error(t, err)
	validation, err := registry.Validate(ctx, "active-fallback", 0, []byte(`{}`))
	require.NoError(t, err)
	require.False(t, validation.GetValid())
	require.NotEmpty(t, validation.GetErrors())
	require.Equal(t, schemapb.ErrorCode_SCHEMA_NOT_FOUND.String(), validation.GetErrors()[0].GetErrorCode())
}
