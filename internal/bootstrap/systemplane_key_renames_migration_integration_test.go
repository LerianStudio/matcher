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

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// systemplaneKeyRename pairs the pre-migration key with the post-migration
// key for 000020. Kept in lockstep with the up/down SQL.
type systemplaneKeyRename struct {
	oldKey string
	newKey string
}

// systemplaneKeyRenames is the canonical list of renames performed by
// 000020_systemplane_key_renames. A regression that adds/removes a rename
// here but not in the SQL (or vice versa) fails the apply/rollback test.
var systemplaneKeyRenames = []systemplaneKeyRename{
	{oldKey: "postgres.max_open_connections", newKey: "postgres.max_open_conns"},
	{oldKey: "postgres.max_idle_connections", newKey: "postgres.max_idle_conns"},
	{oldKey: "redis.min_idle_conn", newKey: "redis.min_idle_conns"},
	{oldKey: "rabbitmq.uri", newKey: "rabbitmq.url"},
	{oldKey: "server.cors_allowed_origins", newKey: "cors.allowed_origins"},
	{oldKey: "server.cors_allowed_methods", newKey: "cors.allowed_methods"},
	{oldKey: "server.cors_allowed_headers", newKey: "cors.allowed_headers"},
}

// TestMigrations_020_SystemplaneKeyRenames_ApplyAndRollback seeds the
// pre-migration key set in system.runtime_entries + system.runtime_history,
// steps the migrator forward to apply 000020, asserts every old key was
// renamed to its canonical form, then rolls back and asserts the state is
// restored. Covers both the runtime_entries UPDATE (lines 80-94 of the .up
// SQL) and the runtime_history UPDATE (lines 96-110) as a single
// transactional unit.
//
// Scope: migrates strictly to version 19 before seeding, then applies
// 000020 in isolation. We deliberately do NOT migrate to HEAD because
// migration 000030 uses a DO $$ BEGIN ... END$$ block that the
// golang-migrate multi-statement parser splits on embedded semicolons,
// producing "unterminated dollar-quoted string" errors in-process. That
// is a separate production bug; scoping to 000020 keeps this test
// focused on the rename target.
func TestMigrations_020_SystemplaneKeyRenames_ApplyAndRollback(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, _, cleanup := newSystemplaneRenameTestDB(t, ctx, "matcher_020_apply_test")
	defer cleanup()

	migrator, err := newMigrator(db, "matcher_020_apply_test", "migrations")
	require.NoError(t, err)

	defer func() {
		require.NoError(t, closeMigrator(migrator))
	}()

	stepper, ok := migrator.(interface{ Steps(int) error })
	require.True(t, ok, "migrator must support stepping for rollback verification")

	// The system.runtime_entries + system.runtime_history tables are
	// created inside 000020 itself (CREATE TABLE IF NOT EXISTS at the top
	// of .up.sql / .down.sql). To seed rows before the rename UPDATE runs
	// we have to: apply 000020 once to materialise the tables, roll back
	// so the WHERE-filtered UPDATE reverts, seed data, then re-apply.
	//
	// This also models the production drift pattern — an earlier 020 was
	// applied, rolled back, and the data was re-seeded by operators
	// before a second apply.
	require.NoError(t, stepper.Steps(20))
	require.NoError(t, stepper.Steps(-1))

	for _, rename := range systemplaneKeyRenames {
		insertRuntimeEntry(t, ctx, db, rename.oldKey, rename.oldKey+"-value")
		insertRuntimeHistory(t, ctx, db, rename.oldKey, rename.oldKey+"-old", rename.oldKey+"-new")
	}

	// Apply 000020 (re-apply).
	require.NoError(t, stepper.Steps(1))

	for _, rename := range systemplaneKeyRenames {
		assert.Equalf(t, 0, runtimeEntryCount(t, ctx, db, rename.oldKey),
			"runtime_entries row for old key %q must be renamed", rename.oldKey)
		assert.Equalf(t, 1, runtimeEntryCount(t, ctx, db, rename.newKey),
			"runtime_entries row for new key %q must exist after migration", rename.newKey)
		assert.Equalf(t, 0, runtimeHistoryCount(t, ctx, db, rename.oldKey),
			"runtime_history rows for old key %q must be renamed", rename.oldKey)
		assert.Equalf(t, 1, runtimeHistoryCount(t, ctx, db, rename.newKey),
			"runtime_history rows for new key %q must exist after migration", rename.newKey)
	}

	// Roll 000020 back.
	require.NoError(t, stepper.Steps(-1))

	for _, rename := range systemplaneKeyRenames {
		assert.Equalf(t, 1, runtimeEntryCount(t, ctx, db, rename.oldKey),
			"runtime_entries row for old key %q must be restored on rollback", rename.oldKey)
		assert.Equalf(t, 0, runtimeEntryCount(t, ctx, db, rename.newKey),
			"runtime_entries row for new key %q must be removed on rollback", rename.newKey)
		assert.Equalf(t, 1, runtimeHistoryCount(t, ctx, db, rename.oldKey),
			"runtime_history rows for old key %q must be restored on rollback", rename.oldKey)
		assert.Equalf(t, 0, runtimeHistoryCount(t, ctx, db, rename.newKey),
			"runtime_history rows for new key %q must be removed on rollback", rename.newKey)
	}
}

// TestMigrations_020_SystemplaneKeyRenames_Idempotent asserts that applying
// 000020 on data already at the canonical (post-020) key names is a
// structurally-clean no-op. After a rerun the table row count must not
// change — the UPDATE's WHERE key = old_key filters zero rows, and the
// transaction is a no-op by design.
//
// The "reapply" shape (step back, seed canonical, step forward) models a
// production rollback followed by operator-driven data cleanup to
// canonical keys before a second up-apply.
//
// Scope: 000020 only; see TestMigrations_020_SystemplaneKeyRenames_
// ApplyAndRollback doc for why we skip the walk to HEAD.
func TestMigrations_020_SystemplaneKeyRenames_Idempotent(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, _, cleanup := newSystemplaneRenameTestDB(t, ctx, "matcher_020_idempotent_test")
	defer cleanup()

	migrator, err := newMigrator(db, "matcher_020_idempotent_test", "migrations")
	require.NoError(t, err)

	defer func() {
		require.NoError(t, closeMigrator(migrator))
	}()

	stepper, ok := migrator.(interface{ Steps(int) error })
	require.True(t, ok, "migrator must support stepping")

	// Apply through 000020 then step back so tables exist (see
	// ApplyAndRollback doc) but the rename is reverted. Seed canonical
	// (post-020) rows, then re-apply 000020. The UPDATE's WHERE key =
	// old_key must match zero rows — idempotency is the test target.
	require.NoError(t, stepper.Steps(20))
	require.NoError(t, stepper.Steps(-1))

	for _, rename := range systemplaneKeyRenames {
		insertRuntimeEntry(t, ctx, db, rename.newKey, rename.newKey+"-value")
	}

	require.NoError(t, stepper.Steps(1))

	for _, rename := range systemplaneKeyRenames {
		assert.Equalf(t, 0, runtimeEntryCount(t, ctx, db, rename.oldKey),
			"old key %q must remain absent when only canonical data is seeded", rename.oldKey)
		assert.Equalf(t, 1, runtimeEntryCount(t, ctx, db, rename.newKey),
			"canonical key %q must remain exactly once (no duplication, no deletion)", rename.newKey)
	}

	// Second apply — step back to version 19 and step forward again.
	// Once 000020 has been reverted via its down migration the canonical
	// rows stay intact (down's WHERE matches only post-020 keys that were
	// produced FROM old keys; seeded-canonical rows are untouched by both
	// the up's WHERE and the down's WHERE). Re-applying 020 is then a
	// second no-op — the core idempotency guarantee.
	require.NoError(t, stepper.Steps(-1))
	require.NoError(t, stepper.Steps(1))

	for _, rename := range systemplaneKeyRenames {
		assert.Equalf(t, 0, runtimeEntryCount(t, ctx, db, rename.oldKey),
			"old key %q must remain absent after second apply on canonical data", rename.oldKey)
		assert.Equalf(t, 1, runtimeEntryCount(t, ctx, db, rename.newKey),
			"canonical key %q must remain exactly once after second apply", rename.newKey)
	}
}

// TestMigrations_020_SystemplaneKeyRenames_BlockOnCollision asserts the
// collision guard at lines 52-78 of the up SQL: when BOTH the old and new
// key rows exist simultaneously, the migration must refuse to apply
// (error on current_setting() lookup of the synthesized "blocked" GUC)
// and the pre-existing rows must remain untouched.
//
// Scope: 000020 only; see TestMigrations_020_SystemplaneKeyRenames_
// ApplyAndRollback doc for why we skip the walk to HEAD.
func TestMigrations_020_SystemplaneKeyRenames_BlockOnCollision(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, _, cleanup := newSystemplaneRenameTestDB(t, ctx, "matcher_020_collision_test")
	defer cleanup()

	migrator, err := newMigrator(db, "matcher_020_collision_test", "migrations")
	require.NoError(t, err)

	defer func() {
		require.NoError(t, closeMigrator(migrator))
	}()

	stepper, ok := migrator.(interface{ Steps(int) error })
	require.True(t, ok, "migrator must support stepping")

	// Apply through 000020 then step back so tables exist (see
	// ApplyAndRollback doc). Seed both old + new keys and re-apply; the
	// guard at lines 52-78 of .up.sql must trip on the overlap.
	require.NoError(t, stepper.Steps(20))
	require.NoError(t, stepper.Steps(-1))

	// Seed both rows for the first rename pair. The guard must detect any
	// overlap — picking the first entry is sufficient.
	rename := systemplaneKeyRenames[0]
	insertRuntimeEntry(t, ctx, db, rename.oldKey, "old-value")
	insertRuntimeEntry(t, ctx, db, rename.newKey, "new-value")

	err = stepper.Steps(1)
	require.Error(t, err, "migration must abort when both old and new keys exist")
	assert.Contains(t, err.Error(),
		"migration_000020_blocked_resolve_systemplane_key_rename_collisions_before_apply",
		"error must surface the canonical blocker name so ops can grep for it")

	assert.Equal(t, 1, runtimeEntryCount(t, ctx, db, rename.oldKey),
		"old key must remain untouched when collision blocks migration")
	assert.Equal(t, 1, runtimeEntryCount(t, ctx, db, rename.newKey),
		"new key must remain untouched when collision blocks migration")
}

// TestMigrations_020_SystemplaneKeyRenames_RollbackBlocksOnCollision is the
// mirror case for the down SQL at lines 43-69: a rollback must refuse when
// both the canonical (post-020) and pre-020 keys exist.
//
// Scope: 000020 only; see TestMigrations_020_SystemplaneKeyRenames_
// ApplyAndRollback doc for why we skip the walk to HEAD.
func TestMigrations_020_SystemplaneKeyRenames_RollbackBlocksOnCollision(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, _, cleanup := newSystemplaneRenameTestDB(t, ctx, "matcher_020_rollback_collision_test")
	defer cleanup()

	migrator, err := newMigrator(db, "matcher_020_rollback_collision_test", "migrations")
	require.NoError(t, err)

	defer func() {
		require.NoError(t, closeMigrator(migrator))
	}()

	stepper, ok := migrator.(interface{ Steps(int) error })
	require.True(t, ok, "migrator must support stepping")

	// Walk up through 000020 (version 20), then seed both keys. Rolling
	// back one step must hit the guard.
	require.NoError(t, stepper.Steps(20))

	rename := systemplaneKeyRenames[0]
	insertRuntimeEntry(t, ctx, db, rename.oldKey, "old-value")
	insertRuntimeEntry(t, ctx, db, rename.newKey, "new-value")

	err = stepper.Steps(-1)
	require.Error(t, err, "rollback must abort when both old and new keys exist")
	assert.Contains(t, err.Error(),
		"migration_000020_rollback_blocked_resolve_systemplane_key_rename_collisions_before_apply",
		"rollback error must surface the canonical blocker name so ops can grep for it")

	assert.Equal(t, 1, runtimeEntryCount(t, ctx, db, rename.oldKey),
		"old key must remain untouched when rollback collision blocks migration")
	assert.Equal(t, 1, runtimeEntryCount(t, ctx, db, rename.newKey),
		"new key must remain untouched when rollback collision blocks migration")
}

// --- Helpers (file-scoped, kept in lockstep with production schema) ---

// newSystemplaneRenameTestDB starts a throwaway Postgres 17 container and
// returns an open sql.DB, the DSN for golang-migrate, and a cleanup closure.
// The waitStrategy combines log + port so the mapping is ready before the
// test issues its first ping — without ForListeningPort the first Open can
// race and return a refused-connection error on macOS/Docker-Desktop.
//
// The DSN is returned alongside the DB so callers that want to go through
// the full RunMigrations path (rather than stepping the migrator directly)
// can. Current callers use direct stepping because migration 000030 cannot
// run in-process; see the ApplyAndRollback test for the full explanation.
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

// insertRuntimeEntry inserts a single row into system.runtime_entries at
// (config, global, ”, key) — the same tuple matcher uses for global
// systemplane configuration — with a JSON-encoded string value.
func insertRuntimeEntry(t *testing.T, ctx context.Context, db *sql.DB, key, value string) {
	t.Helper()

	_, err := db.ExecContext(ctx, `
		INSERT INTO system.runtime_entries (kind, scope, subject, key, value, revision, updated_by, source)
		VALUES ('config', 'global', '', $1, to_jsonb($2::text), 1, 'migration-test', 'integration')`, key, value)
	require.NoError(t, err)
}

// insertRuntimeHistory inserts a single audit row into
// system.runtime_history for the given key.
func insertRuntimeHistory(t *testing.T, ctx context.Context, db *sql.DB, key, oldValue, newValue string) {
	t.Helper()

	_, err := db.ExecContext(ctx, `
		INSERT INTO system.runtime_history (kind, scope, subject, key, old_value, new_value, revision, actor_id, source)
		VALUES ('config', 'global', '', $1, to_jsonb($2::text), to_jsonb($3::text), 1, 'migration-test', 'integration')`,
		key, oldValue, newValue)
	require.NoError(t, err)
}

// runtimeEntryCount returns the number of runtime_entries rows at the
// canonical (config, global, ”) scope with the given key.
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

// runtimeHistoryCount returns the number of runtime_history rows at the
// canonical (config, global, ”) scope with the given key.
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
