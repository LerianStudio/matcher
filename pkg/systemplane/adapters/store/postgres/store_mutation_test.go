//go:build unit

// Copyright 2025 Lerian Studio.

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

func TestStore_FetchOldValue(t *testing.T) {
	t.Parallel()

	t.Run("returns value when key exists", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		expected, err := json.Marshal("existing-value")
		require.NoError(t, err)

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectQuery(`SELECT value FROM`).
			WithArgs("config", "global", "", "log_level").
			WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(expected))

		raw, err := store.fetchOldValue(ctx, tx, target, "log_level")

		require.NoError(t, err)
		assert.Equal(t, expected, raw)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns nil when key does not exist", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectQuery(`SELECT value FROM`).
			WithArgs("config", "global", "", "nonexistent").
			WillReturnError(sql.ErrNoRows)

		raw, err := store.fetchOldValue(ctx, tx, target, "nonexistent")

		require.NoError(t, err)
		assert.Nil(t, raw)
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

		mock.ExpectQuery(`SELECT value FROM`).
			WithArgs("config", "global", "", "broken_key").
			WillReturnError(errors.New("connection reset"))

		_, err = store.fetchOldValue(ctx, tx, target, "broken_key")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "select old value")
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestStore_DeleteEntry(t *testing.T) {
	t.Parallel()

	t.Run("deletes existing entry successfully", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectExec(`DELETE FROM`).
			WithArgs("config", "global", "", "log_level").
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = store.deleteEntry(ctx, tx, target, "log_level")

		require.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("succeeds even when key does not exist", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectExec(`DELETE FROM`).
			WithArgs("config", "global", "", "nonexistent").
			WillReturnResult(sqlmock.NewResult(0, 0))

		err = store.deleteEntry(ctx, tx, target, "nonexistent")

		require.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error on exec failure", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectExec(`DELETE FROM`).
			WithArgs("config", "global", "", "log_level").
			WillReturnError(errors.New("permission denied"))

		err = store.deleteEntry(ctx, tx, target, "log_level")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "delete entry exec")
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestStore_UpsertEntry(t *testing.T) {
	t.Parallel()

	t.Run("inserts new entry successfully", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		op := ports.WriteOp{Key: "log_level", Value: "debug"}
		now := time.Now().UTC()
		actor := domain.Actor{ID: "admin"}

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectExec(`INSERT INTO`).
			WithArgs("config", "global", "", "log_level",
				sqlmock.AnyArg(), uint64(1), now, "admin", "api",
			).WillReturnResult(sqlmock.NewResult(0, 1))

		err = store.upsertEntry(ctx, tx, target, op, domain.Revision(1), now, actor, "api")

		require.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error on exec failure", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		op := ports.WriteOp{Key: "log_level", Value: "debug"}
		now := time.Now().UTC()
		actor := domain.Actor{ID: "admin"}

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectExec(`INSERT INTO`).
			WithArgs("config", "global", "", "log_level",
				sqlmock.AnyArg(), uint64(1), now, "admin", "api",
			).WillReturnError(errors.New("unique violation"))

		err = store.upsertEntry(ctx, tx, target, op, domain.Revision(1), now, actor, "api")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "upsert entry exec")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("encrypts secret value before storing", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		withSecretCodec(t, store, "db.password")
		ctx := context.Background()
		target := configTarget()
		op := ports.WriteOp{Key: "db.password", Value: "s3cr3t"}
		now := time.Now().UTC()
		actor := domain.Actor{ID: "admin"}

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectExec(`INSERT INTO`).
			WithArgs("config", "global", "", "db.password",
				sqlmock.AnyArg(), uint64(1), now, "admin", "api",
			).WillReturnResult(sqlmock.NewResult(0, 1))

		err = store.upsertEntry(ctx, tx, target, op, domain.Revision(1), now, actor, "api")

		require.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestStore_InsertHistory(t *testing.T) {
	t.Parallel()

	t.Run("inserts history with old and new values", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		op := ports.WriteOp{Key: "log_level", Value: "debug"}
		oldVal, err := json.Marshal("info")
		require.NoError(t, err)
		now := time.Now().UTC()
		actor := domain.Actor{ID: "admin"}

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectExec(`INSERT INTO test_schema.runtime_history`).
			WithArgs("config", "global", "", "log_level",
				oldVal,           // old_value
				sqlmock.AnyArg(), // new_value
				uint64(2),        // revision
				"admin",          // actor_id
				now,              // changed_at
				"api",            // source
			).WillReturnResult(sqlmock.NewResult(1, 1))

		err = store.insertHistory(ctx, tx, target, op, oldVal, domain.Revision(2), now, actor, "api")

		require.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("inserts history with nil old value for new key", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		op := ports.WriteOp{Key: "new_key", Value: "value"}
		now := time.Now().UTC()
		actor := domain.Actor{ID: "admin"}

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectExec(`INSERT INTO test_schema.runtime_history`).
			WithArgs("config", "global", "", "new_key",
				nil,              // old_value (nil because new key)
				sqlmock.AnyArg(), // new_value
				uint64(1),        // revision
				"admin",          // actor_id
				now,              // changed_at
				"api",            // source
			).WillReturnResult(sqlmock.NewResult(1, 1))

		err = store.insertHistory(ctx, tx, target, op, nil, domain.Revision(1), now, actor, "api")

		require.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("inserts history with nil new value for reset op", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		op := ports.WriteOp{Key: "removed_key", Reset: true}
		oldVal, err := json.Marshal("old-value")
		require.NoError(t, err)
		now := time.Now().UTC()
		actor := domain.Actor{ID: "admin"}

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectExec(`INSERT INTO test_schema.runtime_history`).
			WithArgs("config", "global", "", "removed_key",
				oldVal,    // old_value
				nil,       // new_value (nil because reset)
				uint64(3), // revision
				"admin",   // actor_id
				now,       // changed_at
				"api",     // source
			).WillReturnResult(sqlmock.NewResult(1, 1))

		err = store.insertHistory(ctx, tx, target, op, oldVal, domain.Revision(3), now, actor, "api")

		require.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error on exec failure", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		op := ports.WriteOp{Key: "log_level", Value: "debug"}
		now := time.Now().UTC()
		actor := domain.Actor{ID: "admin"}

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectExec(`INSERT INTO test_schema.runtime_history`).
			WithArgs("config", "global", "", "log_level",
				nil, sqlmock.AnyArg(), uint64(1), "admin", now, "api",
			).WillReturnError(errors.New("disk full"))

		err = store.insertHistory(ctx, tx, target, op, nil, domain.Revision(1), now, actor, "api")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "insert history exec")
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestStore_ApplyOp(t *testing.T) {
	t.Parallel()

	t.Run("upserts for normal write op", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		op := ports.WriteOp{Key: "log_level", Value: "debug"}
		now := time.Now().UTC()
		actor := domain.Actor{ID: "admin"}

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		// fetchOldValue
		mock.ExpectQuery(`SELECT value FROM`).
			WithArgs("config", "global", "", "log_level").
			WillReturnError(sql.ErrNoRows)

		// upsertEntry
		mock.ExpectExec(`INSERT INTO`).
			WithArgs("config", "global", "", "log_level",
				sqlmock.AnyArg(), uint64(1), now, "admin", "api",
			).WillReturnResult(sqlmock.NewResult(0, 1))

		// insertHistory
		mock.ExpectExec(`INSERT INTO test_schema.runtime_history`).
			WithArgs("config", "global", "", "log_level",
				nil, sqlmock.AnyArg(), uint64(1), "admin", now, "api",
			).WillReturnResult(sqlmock.NewResult(1, 1))

		err = store.applyOp(ctx, tx, target, op, domain.Revision(1), now, actor, "api")

		require.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("deletes for reset op", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		op := ports.WriteOp{Key: "log_level", Reset: true}
		now := time.Now().UTC()
		actor := domain.Actor{ID: "admin"}

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		// fetchOldValue
		oldVal, err := json.Marshal("info")
		require.NoError(t, err)

		mock.ExpectQuery(`SELECT value FROM`).
			WithArgs("config", "global", "", "log_level").
			WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(oldVal))

		// deleteEntry
		mock.ExpectExec(`DELETE FROM`).
			WithArgs("config", "global", "", "log_level").
			WillReturnResult(sqlmock.NewResult(0, 1))

		// insertHistory
		mock.ExpectExec(`INSERT INTO test_schema.runtime_history`).
			WithArgs("config", "global", "", "log_level",
				oldVal, nil, uint64(2), "admin", now, "api",
			).WillReturnResult(sqlmock.NewResult(1, 1))

		err = store.applyOp(ctx, tx, target, op, domain.Revision(2), now, actor, "api")

		require.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("deletes for nil value op", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		op := ports.WriteOp{Key: "log_level", Value: nil}
		now := time.Now().UTC()
		actor := domain.Actor{ID: "admin"}

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		// fetchOldValue - no rows
		mock.ExpectQuery(`SELECT value FROM`).
			WithArgs("config", "global", "", "log_level").
			WillReturnError(sql.ErrNoRows)

		// deleteEntry
		mock.ExpectExec(`DELETE FROM`).
			WithArgs("config", "global", "", "log_level").
			WillReturnResult(sqlmock.NewResult(0, 0))

		// insertHistory
		mock.ExpectExec(`INSERT INTO test_schema.runtime_history`).
			WithArgs("config", "global", "", "log_level",
				nil, nil, uint64(1), "admin", now, "api",
			).WillReturnResult(sqlmock.NewResult(1, 1))

		err = store.applyOp(ctx, tx, target, op, domain.Revision(1), now, actor, "api")

		require.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error when fetchOldValue fails", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		op := ports.WriteOp{Key: "log_level", Value: "debug"}
		now := time.Now().UTC()
		actor := domain.Actor{ID: "admin"}

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		mock.ExpectQuery(`SELECT value FROM`).
			WithArgs("config", "global", "", "log_level").
			WillReturnError(errors.New("connection lost"))

		err = store.applyOp(ctx, tx, target, op, domain.Revision(1), now, actor, "api")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "fetch old value")
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}
