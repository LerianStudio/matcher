//go:build unit

// Copyright 2025 Lerian Studio.

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

func TestStore_ReadRevision(t *testing.T) {
	t.Parallel()

	t.Run("returns revision when row exists", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		mock.ExpectQuery(`SELECT revision FROM`).
			WithArgs("config", "global", "").
			WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(7))

		rev, err := store.readRevision(ctx, store.db, target)

		require.NoError(t, err)
		assert.Equal(t, domain.Revision(7), rev)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns zero when row does not exist", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		mock.ExpectQuery(`SELECT revision FROM`).
			WithArgs("config", "global", "").
			WillReturnRows(sqlmock.NewRows([]string{"revision"}))

		rev, err := store.readRevision(ctx, store.db, target)

		require.NoError(t, err)
		assert.Equal(t, domain.RevisionZero, rev)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error on query failure", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		mock.ExpectQuery(`SELECT revision FROM`).
			WithArgs("config", "global", "").
			WillReturnError(errors.New("database gone"))

		_, err := store.readRevision(ctx, store.db, target)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "select revision")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("reads from tenant-scoped target", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := tenantTarget()

		mock.ExpectQuery(`SELECT revision FROM`).
			WithArgs("setting", "tenant", "tenant-abc").
			WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(12))

		rev, err := store.readRevision(ctx, store.db, target)

		require.NoError(t, err)
		assert.Equal(t, domain.Revision(12), rev)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestStore_EnsureRevisionRow(t *testing.T) {
	t.Parallel()

	t.Run("inserts new revision row", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		actor := domain.Actor{ID: "admin"}
		now := time.Now().UTC()

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectExec(`INSERT INTO test_schema.runtime_revisions`).
			WithArgs("config", "global", "", uint64(0), now, "admin", "api").
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = store.ensureRevisionRow(ctx, tx, target, actor, "api", now)

		require.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("does nothing on conflict (idempotent)", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		actor := domain.Actor{ID: "admin"}
		now := time.Now().UTC()

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		// ON CONFLICT DO NOTHING - 0 rows affected is fine
		mock.ExpectExec(`INSERT INTO test_schema.runtime_revisions`).
			WithArgs("config", "global", "", uint64(0), now, "admin", "api").
			WillReturnResult(sqlmock.NewResult(0, 0))

		err = store.ensureRevisionRow(ctx, tx, target, actor, "api", now)

		require.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error on exec failure", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		actor := domain.Actor{ID: "admin"}
		now := time.Now().UTC()

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectExec(`INSERT INTO test_schema.runtime_revisions`).
			WithArgs("config", "global", "", uint64(0), now, "admin", "api").
			WillReturnError(errors.New("table does not exist"))

		err = store.ensureRevisionRow(ctx, tx, target, actor, "api", now)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "insert revision row")
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestStore_LockAndReadRevision(t *testing.T) {
	t.Parallel()

	t.Run("returns current revision with FOR UPDATE lock", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectQuery(`SELECT revision`).
			WithArgs("config", "global", "").
			WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(10))

		rev, err := store.lockAndReadRevision(ctx, tx, target)

		require.NoError(t, err)
		assert.Equal(t, domain.Revision(10), rev)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns zero when no row exists", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectQuery(`SELECT revision`).
			WithArgs("config", "global", "").
			WillReturnRows(sqlmock.NewRows([]string{"revision"}))

		rev, err := store.lockAndReadRevision(ctx, tx, target)

		require.NoError(t, err)
		assert.Equal(t, domain.RevisionZero, rev)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error on query failure", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectQuery(`SELECT revision`).
			WithArgs("config", "global", "").
			WillReturnError(errors.New("deadlock detected"))

		_, err = store.lockAndReadRevision(ctx, tx, target)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "select revision for update")
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestStore_UpdateRevisionRow(t *testing.T) {
	t.Parallel()

	t.Run("updates revision row successfully", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		actor := domain.Actor{ID: "admin"}
		now := time.Now().UTC()

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectExec(`UPDATE test_schema.runtime_revisions`).
			WithArgs("config", "global", "", uint64(5), string(domain.ApplyBundleRebuild), now, "admin", "api").
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = store.updateRevisionRow(ctx, tx, target, domain.Revision(5), domain.ApplyBundleRebuild, now, actor, "api")

		require.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error when no row affected", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		actor := domain.Actor{ID: "admin"}
		now := time.Now().UTC()

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectExec(`UPDATE test_schema.runtime_revisions`).
			WithArgs("config", "global", "", uint64(5), string(domain.ApplyBundleRebuild), now, "admin", "api").
			WillReturnResult(sqlmock.NewResult(0, 0))

		err = store.updateRevisionRow(ctx, tx, target, domain.Revision(5), domain.ApplyBundleRebuild, now, actor, "api")

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrRevisionRowUpdateMismatch)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error on exec failure", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		actor := domain.Actor{ID: "admin"}
		now := time.Now().UTC()

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectExec(`UPDATE test_schema.runtime_revisions`).
			WithArgs("config", "global", "", uint64(5), string(domain.ApplyBundleRebuild), now, "admin", "api").
			WillReturnError(errors.New("connection reset"))

		err = store.updateRevisionRow(ctx, tx, target, domain.Revision(5), domain.ApplyBundleRebuild, now, actor, "api")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "update revision row exec")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("stores apply behavior in query", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := tenantTarget()
		actor := domain.Actor{ID: "user1"}
		now := time.Now().UTC()

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectExec(`UPDATE test_schema.runtime_revisions`).
			WithArgs("setting", "tenant", "tenant-abc", uint64(3), string(domain.ApplyLiveRead), now, "user1", "ui").
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = store.updateRevisionRow(ctx, tx, target, domain.Revision(3), domain.ApplyLiveRead, now, actor, "ui")

		require.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}
