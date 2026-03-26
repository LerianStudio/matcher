// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build integration

package bootstrap

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
)

type systemplaneKeyRename struct {
	oldKey string
	newKey string
}

var systemplaneKeyRenames = []systemplaneKeyRename{
	{oldKey: "postgres.max_open_connections", newKey: "postgres.max_open_conns"},
	{oldKey: "postgres.max_idle_connections", newKey: "postgres.max_idle_conns"},
	{oldKey: "redis.min_idle_conn", newKey: "redis.min_idle_conns"},
	{oldKey: "rabbitmq.uri", newKey: "rabbitmq.url"},
	{oldKey: "server.cors_allowed_origins", newKey: "cors.allowed_origins"},
	{oldKey: "server.cors_allowed_methods", newKey: "cors.allowed_methods"},
	{oldKey: "server.cors_allowed_headers", newKey: "cors.allowed_headers"},
}

func TestMigrations_020_SystemplaneKeyRenames_ApplyAndRollback(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dsn, cleanup := newSystemplaneRenameTestDB(t, ctx, "matcher_020_apply_test")
	defer cleanup()

	logger := &libLog.NopLogger{}
	require.NoError(t, RunMigrations(ctx, dsn, "matcher_020_apply_test", "migrations", logger, false))

	migrator, err := newMigrator(db, "matcher_020_apply_test", "migrations")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, closeMigrator(migrator))
	}()

	stepper, ok := migrator.(interface{ Steps(int) error })
	require.True(t, ok, "migrator must support stepping for rollback verification")
	require.NoError(t, stepper.Steps(-1))

	for _, rename := range systemplaneKeyRenames {
		insertRuntimeEntry(t, ctx, db, rename.oldKey, rename.oldKey+"-value")
		insertRuntimeHistory(t, ctx, db, rename.oldKey, rename.oldKey+"-old", rename.oldKey+"-new")
	}

	require.NoError(t, stepper.Steps(1))

	for _, rename := range systemplaneKeyRenames {
		assert.Equal(t, 0, runtimeEntryCount(t, ctx, db, rename.oldKey), "old runtime entry key must be renamed")
		assert.Equal(t, 1, runtimeEntryCount(t, ctx, db, rename.newKey), "new runtime entry key must exist after migration")
		assert.Equal(t, 0, runtimeHistoryCount(t, ctx, db, rename.oldKey), "old runtime history key must be renamed")
		assert.Equal(t, 1, runtimeHistoryCount(t, ctx, db, rename.newKey), "new runtime history key must exist after migration")
	}

	require.NoError(t, stepper.Steps(-1))

	for _, rename := range systemplaneKeyRenames {
		assert.Equal(t, 1, runtimeEntryCount(t, ctx, db, rename.oldKey), "old runtime entry key must be restored on rollback")
		assert.Equal(t, 0, runtimeEntryCount(t, ctx, db, rename.newKey), "new runtime entry key must be removed on rollback")
		assert.Equal(t, 1, runtimeHistoryCount(t, ctx, db, rename.oldKey), "old runtime history key must be restored on rollback")
		assert.Equal(t, 0, runtimeHistoryCount(t, ctx, db, rename.newKey), "new runtime history key must be removed on rollback")
	}
}

func TestMigrations_020_SystemplaneKeyRenames_BlockOnCollision(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dsn, cleanup := newSystemplaneRenameTestDB(t, ctx, "matcher_020_collision_test")
	defer cleanup()

	logger := &libLog.NopLogger{}
	require.NoError(t, RunMigrations(ctx, dsn, "matcher_020_collision_test", "migrations", logger, false))

	migrator, err := newMigrator(db, "matcher_020_collision_test", "migrations")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, closeMigrator(migrator))
	}()

	stepper, ok := migrator.(interface{ Steps(int) error })
	require.True(t, ok, "migrator must support stepping for rollback verification")
	require.NoError(t, stepper.Steps(-1))

	rename := systemplaneKeyRenames[0]
	insertRuntimeEntry(t, ctx, db, rename.oldKey, "old-value")
	insertRuntimeEntry(t, ctx, db, rename.newKey, "new-value")

	err = stepper.Steps(1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "migration_000020_blocked_resolve_systemplane_key_rename_collisions_before_apply")
	assert.Equal(t, 1, runtimeEntryCount(t, ctx, db, rename.oldKey), "old key must remain untouched when collision blocks migration")
	assert.Equal(t, 1, runtimeEntryCount(t, ctx, db, rename.newKey), "new key must remain untouched when collision blocks migration")
}

func TestMigrations_020_SystemplaneKeyRenames_RollbackBlocksOnCollision(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dsn, cleanup := newSystemplaneRenameTestDB(t, ctx, "matcher_020_rollback_collision_test")
	defer cleanup()

	logger := &libLog.NopLogger{}
	require.NoError(t, RunMigrations(ctx, dsn, "matcher_020_rollback_collision_test", "migrations", logger, false))

	rename := systemplaneKeyRenames[0]
	insertRuntimeEntry(t, ctx, db, rename.oldKey, "old-value")
	insertRuntimeEntry(t, ctx, db, rename.newKey, "new-value")

	migrator, err := newMigrator(db, "matcher_020_rollback_collision_test", "migrations")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, closeMigrator(migrator))
	}()

	stepper, ok := migrator.(interface{ Steps(int) error })
	require.True(t, ok, "migrator must support stepping for rollback verification")

	err = stepper.Steps(-1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "migration_000020_rollback_blocked_resolve_systemplane_key_rename_collisions_before_apply")
	assert.Equal(t, 1, runtimeEntryCount(t, ctx, db, rename.oldKey), "old key must remain untouched when rollback collision blocks migration")
	assert.Equal(t, 1, runtimeEntryCount(t, ctx, db, rename.newKey), "new key must remain untouched when rollback collision blocks migration")
}

func newSystemplaneRenameTestDB(t *testing.T, ctx context.Context, dbName string) (*sql.DB, string, func()) {
	t.Helper()

	pgContainer, err := postgres.Run(ctx,
		"postgres:17-alpine",
		postgres.WithDatabase(dbName),
		postgres.WithUsername("matcher"),
		postgres.WithPassword("matcher_test"),
		testcontainers.WithWaitStrategy(
			wait.ForAll(
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
				wait.ForListeningPort("5432/tcp"),
			).WithStartupTimeout(90*time.Second),
		),
	)
	require.NoError(t, err)

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))

	cleanup := func() {
		require.NoError(t, db.Close())
		require.NoError(t, pgContainer.Terminate(context.Background()))
	}

	return db, dsn, cleanup
}

func insertRuntimeEntry(t *testing.T, ctx context.Context, db *sql.DB, key, value string) {
	t.Helper()

	_, err := db.ExecContext(ctx, `
		INSERT INTO system.runtime_entries (kind, scope, subject, key, value, revision, updated_by, source)
		VALUES ('config', 'global', '', $1, to_jsonb($2::text), 1, 'migration-test', 'integration')`, key, value)
	require.NoError(t, err)
}

func insertRuntimeHistory(t *testing.T, ctx context.Context, db *sql.DB, key, oldValue, newValue string) {
	t.Helper()

	_, err := db.ExecContext(ctx, `
		INSERT INTO system.runtime_history (kind, scope, subject, key, old_value, new_value, revision, actor_id, source)
		VALUES ('config', 'global', '', $1, to_jsonb($2::text), to_jsonb($3::text), 1, 'migration-test', 'integration')`, key, oldValue, newValue)
	require.NoError(t, err)
}

func runtimeEntryCount(t *testing.T, ctx context.Context, db *sql.DB, key string) int {
	t.Helper()

	var count int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM system.runtime_entries
		WHERE kind = 'config' AND scope = 'global' AND subject = '' AND key = $1`, key).Scan(&count)
	require.NoError(t, err)

	return count
}

func runtimeHistoryCount(t *testing.T, ctx context.Context, db *sql.DB, key string) int {
	t.Helper()

	var count int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM system.runtime_history
		WHERE kind = 'config' AND scope = 'global' AND subject = '' AND key = $1`, key).Scan(&count)
	require.NoError(t, err)

	return count
}
