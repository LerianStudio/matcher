// Copyright 2025 Lerian Studio.

//go:build unit

package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/adapters/store/secretcodec"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// newTestStore creates a Store with a sqlmock-backed *sql.DB for unit testing.
func newTestStore(t *testing.T) (*Store, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	t.Cleanup(func() { db.Close() })

	store := &Store{
		db:            db,
		schema:        "test_schema",
		entriesTable:  "runtime_entries",
		historyTable:  "runtime_history",
		revisionTable: "runtime_revisions",
		notifyChannel: "test_changes",
	}

	return store, mock
}

func withSecretCodec(t *testing.T, store *Store, keys ...string) {
	t.Helper()

	codec, err := secretcodec.New("0123456789abcdef0123456789abcdef", keys)
	require.NoError(t, err)
	store.secretCodec = codec
}

func configTarget() domain.Target {
	return domain.Target{
		Kind:      domain.KindConfig,
		Scope:     domain.ScopeGlobal,
		SubjectID: "",
	}
}

func tenantTarget() domain.Target {
	return domain.Target{
		Kind:      domain.KindSetting,
		Scope:     domain.ScopeTenant,
		SubjectID: "tenant-abc",
	}
}

type notifyPayloadMatcher struct {
	t        *testing.T
	expected notifyPayload
}

func (matcher notifyPayloadMatcher) Match(value driver.Value) bool {
	matcher.t.Helper()

	payloadText, ok := value.(string)
	if !ok {
		return false
	}

	var payload notifyPayload
	if err := json.Unmarshal([]byte(payloadText), &payload); err != nil {
		return false
	}

	assert.Equal(matcher.t, matcher.expected, payload)

	return true
}

func TestStore_Get(t *testing.T) {
	t.Parallel()

	t.Run("returns entries and max revision", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		now := time.Now().UTC()
		val1, err := json.Marshal("hello")
		require.NoError(t, err)
		val2, err := json.Marshal(42)
		require.NoError(t, err)

		mock.ExpectBegin()

		rows := sqlmock.NewRows([]string{"key", "value", "revision", "updated_at", "updated_by", "source"}).
			AddRow("log_level", val1, 3, now, "admin", "api").
			AddRow("max_conns", val2, 3, now, "admin", "api")

		mock.ExpectQuery(`SELECT key, value, revision, updated_at, updated_by, source`).
			WithArgs("config", "global", "").
			WillReturnRows(rows)

		mock.ExpectQuery(`SELECT revision FROM`).
			WithArgs("config", "global", "").
			WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(3))
		mock.ExpectCommit()

		result, err := store.Get(ctx, target)

		require.NoError(t, err)
		assert.Len(t, result.Entries, 2)
		assert.Equal(t, domain.Revision(3), result.Revision)
		assert.Equal(t, domain.KindConfig, result.Entries[0].Kind)
		assert.Equal(t, domain.ScopeGlobal, result.Entries[0].Scope)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns empty result for unknown target", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		rows := sqlmock.NewRows([]string{"key", "value", "revision", "updated_at", "updated_by", "source"})
		mock.ExpectBegin()

		mock.ExpectQuery(`SELECT key, value, revision, updated_at, updated_by, source`).
			WithArgs("config", "global", "").
			WillReturnRows(rows)

		mock.ExpectQuery(`SELECT revision FROM`).
			WithArgs("config", "global", "").
			WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(0))
		mock.ExpectCommit()

		result, err := store.Get(ctx, target)

		require.NoError(t, err)
		assert.Nil(t, result.Entries)
		assert.Equal(t, domain.RevisionZero, result.Revision)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error on query failure", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		mock.ExpectBegin()
		mock.ExpectQuery(`SELECT key, value, revision, updated_at, updated_by, source`).
			WithArgs("config", "global", "").
			WillReturnError(errors.New("connection lost"))
		mock.ExpectRollback()

		_, err := store.Get(ctx, target)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "postgres store get: query:")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error on scan failure", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		// Return a row with wrong column count to trigger scan error
		rows := sqlmock.NewRows([]string{"key", "value"}).
			AddRow("log_level", "not-json")
		mock.ExpectBegin()

		mock.ExpectQuery(`SELECT key, value, revision, updated_at, updated_by, source`).
			WithArgs("config", "global", "").
			WillReturnRows(rows)
		mock.ExpectRollback()

		_, err := store.Get(ctx, target)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "postgres store get: scan:")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error on invalid JSON value", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		now := time.Now().UTC()
		mock.ExpectBegin()
		rows := sqlmock.NewRows([]string{"key", "value", "revision", "updated_at", "updated_by", "source"}).
			AddRow("bad_json", []byte("{invalid}"), 1, now, "admin", "api")

		mock.ExpectQuery(`SELECT key, value, revision, updated_at, updated_by, source`).
			WithArgs("config", "global", "").
			WillReturnRows(rows)
		mock.ExpectRollback()

		_, err := store.Get(ctx, target)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal value")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("handles nil JSONB value correctly", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		now := time.Now().UTC()
		mock.ExpectBegin()
		rows := sqlmock.NewRows([]string{"key", "value", "revision", "updated_at", "updated_by", "source"}).
			AddRow("empty_key", nil, 1, now, "admin", "api")

		mock.ExpectQuery(`SELECT key, value, revision, updated_at, updated_by, source`).
			WithArgs("config", "global", "").
			WillReturnRows(rows)

		mock.ExpectQuery(`SELECT revision FROM`).
			WithArgs("config", "global", "").
			WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(1))
		mock.ExpectCommit()

		result, err := store.Get(ctx, target)

		require.NoError(t, err)
		assert.Len(t, result.Entries, 1)
		assert.Nil(t, result.Entries[0].Value)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("tracks max revision across rows", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		now := time.Now().UTC()
		val, err := json.Marshal("v")
		require.NoError(t, err)
		mock.ExpectBegin()

		rows := sqlmock.NewRows([]string{"key", "value", "revision", "updated_at", "updated_by", "source"}).
			AddRow("a", val, 2, now, "admin", "api").
			AddRow("b", val, 5, now, "admin", "api").
			AddRow("c", val, 3, now, "admin", "api")

		mock.ExpectQuery(`SELECT key, value, revision, updated_at, updated_by, source`).
			WithArgs("config", "global", "").
			WillReturnRows(rows)

		mock.ExpectQuery(`SELECT revision FROM`).
			WithArgs("config", "global", "").
			WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(9))
		mock.ExpectCommit()

		result, err := store.Get(ctx, target)

		require.NoError(t, err)
		assert.Equal(t, domain.Revision(9), result.Revision)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("populates target fields from parameter", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := tenantTarget()

		now := time.Now().UTC()
		val, err := json.Marshal("dark")
		require.NoError(t, err)
		mock.ExpectBegin()

		rows := sqlmock.NewRows([]string{"key", "value", "revision", "updated_at", "updated_by", "source"}).
			AddRow("theme", val, 1, now, "user1", "ui")

		mock.ExpectQuery(`SELECT key, value, revision, updated_at, updated_by, source`).
			WithArgs("setting", "tenant", "tenant-abc").
			WillReturnRows(rows)

		mock.ExpectQuery(`SELECT revision FROM`).
			WithArgs("setting", "tenant", "tenant-abc").
			WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(1))
		mock.ExpectCommit()

		result, err := store.Get(ctx, target)

		require.NoError(t, err)
		require.Len(t, result.Entries, 1)
		assert.Equal(t, domain.KindSetting, result.Entries[0].Kind)
		assert.Equal(t, domain.ScopeTenant, result.Entries[0].Scope)
		assert.Equal(t, "tenant-abc", result.Entries[0].Subject)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("decrypts secret values transparently", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		withSecretCodec(t, store, "postgres.primary_password")
		ctx := context.Background()
		target := configTarget()
		now := time.Now().UTC()

		encryptedValue, err := store.encryptValue(target, "postgres.primary_password", "secret-value")
		require.NoError(t, err)
		encryptedBytes, err := json.Marshal(encryptedValue)
		require.NoError(t, err)

		mock.ExpectBegin()
		rows := sqlmock.NewRows([]string{"key", "value", "revision", "updated_at", "updated_by", "source"}).
			AddRow("postgres.primary_password", encryptedBytes, 2, now, "admin", "api")
		mock.ExpectQuery(`SELECT key, value, revision, updated_at, updated_by, source`).
			WithArgs("config", "global", "").
			WillReturnRows(rows)
		mock.ExpectQuery(`SELECT revision FROM`).
			WithArgs("config", "global", "").
			WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(2))
		mock.ExpectCommit()

		result, err := store.Get(ctx, target)
		require.NoError(t, err)
		require.Len(t, result.Entries, 1)
		assert.Equal(t, "secret-value", result.Entries[0].Value)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestStore_Put(t *testing.T) {
	t.Parallel()

	t.Run("successful upsert with history and notify", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		actor := domain.Actor{ID: "admin"}
		ops := []ports.WriteOp{
			{Key: "log_level", Value: "debug"},
		}

		mock.ExpectBegin()

		// ensureRevisionRow
		mock.ExpectExec(`INSERT INTO test_schema.runtime_revisions`).
			WithArgs("config", "global", "", uint64(0), sqlmock.AnyArg(), "admin", "api").
			WillReturnResult(sqlmock.NewResult(0, 1))

		// lockAndReadRevision
		mock.ExpectQuery(`SELECT revision`).
			WithArgs("config", "global", "").
			WillReturnRows(sqlmock.NewRows([]string{"rev"}).AddRow(0))

		// fetchOldValue
		mock.ExpectQuery(`SELECT value FROM`).
			WithArgs("config", "global", "", "log_level").
			WillReturnError(sql.ErrNoRows)

		// upsertEntry
		mock.ExpectExec(`INSERT INTO`).
			WithArgs("config", "global", "", "log_level",
				sqlmock.AnyArg(), // value bytes
				uint64(1),        // revision
				sqlmock.AnyArg(), // updated_at
				"admin", "api",
			).WillReturnResult(sqlmock.NewResult(0, 1))

		// insertHistory
		mock.ExpectExec(`INSERT INTO test_schema.runtime_history`).
			WithArgs("config", "global", "", "log_level",
				nil,              // old_value
				sqlmock.AnyArg(), // new_value
				uint64(1),        // revision
				"admin",
				sqlmock.AnyArg(), // changed_at
				"api",
			).WillReturnResult(sqlmock.NewResult(1, 1))

		// updateRevisionRow
		mock.ExpectExec(`UPDATE test_schema.runtime_revisions`).
			WithArgs("config", "global", "", uint64(1), sqlmock.AnyArg(), sqlmock.AnyArg(), "admin", "api").
			WillReturnResult(sqlmock.NewResult(0, 1))

		// notify
		mock.ExpectExec(`SELECT pg_notify`).
			WithArgs("test_changes", sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 0))

		mock.ExpectCommit()

		rev, err := store.Put(ctx, target, ops, domain.RevisionZero, actor, "api")

		require.NoError(t, err)
		assert.Equal(t, domain.Revision(1), rev)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("revision mismatch returns current revision and sentinel error", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		actor := domain.Actor{ID: "admin"}
		ops := []ports.WriteOp{
			{Key: "log_level", Value: "debug"},
		}

		mock.ExpectBegin()

		// ensureRevisionRow
		mock.ExpectExec(`INSERT INTO test_schema.runtime_revisions`).
			WithArgs("config", "global", "", uint64(0), sqlmock.AnyArg(), "admin", "api").
			WillReturnResult(sqlmock.NewResult(0, 1))

		// lockAndReadRevision returns revision 5
		mock.ExpectQuery(`SELECT revision`).
			WithArgs("config", "global", "").
			WillReturnRows(sqlmock.NewRows([]string{"rev"}).AddRow(5))

		mock.ExpectRollback()

		rev, err := store.Put(ctx, target, ops, domain.Revision(3), actor, "api")

		require.ErrorIs(t, err, domain.ErrRevisionMismatch)
		assert.Equal(t, domain.Revision(5), rev)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("reset op deletes entry and records history", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		actor := domain.Actor{ID: "admin"}
		ops := []ports.WriteOp{
			{Key: "log_level", Reset: true},
		}

		oldVal, err := json.Marshal("info")
		require.NoError(t, err)

		mock.ExpectBegin()

		mock.ExpectExec(`INSERT INTO test_schema.runtime_revisions`).
			WithArgs("config", "global", "", uint64(0), sqlmock.AnyArg(), "admin", "api").
			WillReturnResult(sqlmock.NewResult(0, 1))

		mock.ExpectQuery(`SELECT revision`).
			WithArgs("config", "global", "").
			WillReturnRows(sqlmock.NewRows([]string{"rev"}).AddRow(1))

		// fetchOldValue returns existing value
		mock.ExpectQuery(`SELECT value FROM`).
			WithArgs("config", "global", "", "log_level").
			WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(oldVal))

		// deleteEntry
		mock.ExpectExec(`DELETE FROM`).
			WithArgs("config", "global", "", "log_level").
			WillReturnResult(sqlmock.NewResult(0, 1))

		// insertHistory with nil new_value
		mock.ExpectExec(`INSERT INTO test_schema.runtime_history`).
			WithArgs("config", "global", "", "log_level",
				sqlmock.AnyArg(), // old_value
				nil,              // new_value (reset)
				uint64(2),        // revision
				"admin",
				sqlmock.AnyArg(), // changed_at
				"api",
			).WillReturnResult(sqlmock.NewResult(1, 1))

		mock.ExpectExec(`UPDATE test_schema.runtime_revisions`).
			WithArgs("config", "global", "", uint64(2), sqlmock.AnyArg(), sqlmock.AnyArg(), "admin", "api").
			WillReturnResult(sqlmock.NewResult(0, 1))

		mock.ExpectExec(`SELECT pg_notify`).
			WithArgs("test_changes", sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 0))

		mock.ExpectCommit()

		rev, err := store.Put(ctx, target, ops, domain.Revision(1), actor, "api")

		require.NoError(t, err)
		assert.Equal(t, domain.Revision(2), rev)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("nil value op deletes entry", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		actor := domain.Actor{ID: "admin"}
		ops := []ports.WriteOp{
			{Key: "log_level", Value: nil},
		}

		mock.ExpectBegin()

		mock.ExpectExec(`INSERT INTO test_schema.runtime_revisions`).
			WithArgs("config", "global", "", uint64(0), sqlmock.AnyArg(), "admin", "api").
			WillReturnResult(sqlmock.NewResult(0, 1))

		mock.ExpectQuery(`SELECT revision`).
			WithArgs("config", "global", "").
			WillReturnRows(sqlmock.NewRows([]string{"rev"}).AddRow(1))

		mock.ExpectQuery(`SELECT value FROM`).
			WithArgs("config", "global", "", "log_level").
			WillReturnError(sql.ErrNoRows)

		mock.ExpectExec(`DELETE FROM`).
			WithArgs("config", "global", "", "log_level").
			WillReturnResult(sqlmock.NewResult(0, 0))

		mock.ExpectExec(`INSERT INTO test_schema.runtime_history`).
			WithArgs("config", "global", "", "log_level",
				nil,       // old_value
				nil,       // new_value
				uint64(2), // revision
				"admin",
				sqlmock.AnyArg(), // changed_at
				"api",
			).WillReturnResult(sqlmock.NewResult(1, 1))

		mock.ExpectExec(`UPDATE test_schema.runtime_revisions`).
			WithArgs("config", "global", "", uint64(2), sqlmock.AnyArg(), sqlmock.AnyArg(), "admin", "api").
			WillReturnResult(sqlmock.NewResult(0, 1))

		mock.ExpectExec(`SELECT pg_notify`).
			WithArgs("test_changes", sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 0))

		mock.ExpectCommit()

		rev, err := store.Put(ctx, target, ops, domain.Revision(1), actor, "api")

		require.NoError(t, err)
		assert.Equal(t, domain.Revision(2), rev)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("begin tx failure", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		mock.ExpectBegin().WillReturnError(errors.New("connection refused"))

		_, err := store.Put(ctx, target, []ports.WriteOp{{Key: "feature_flag", Value: true}}, domain.RevisionZero, domain.Actor{}, "api")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "begin tx")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("lock revision query failure", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		mock.ExpectBegin()
		mock.ExpectExec(`INSERT INTO test_schema.runtime_revisions`).
			WithArgs("config", "global", "", uint64(0), sqlmock.AnyArg(), "", "api").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery(`SELECT revision`).
			WithArgs("config", "global", "").
			WillReturnError(errors.New("deadlock"))
		mock.ExpectRollback()

		_, err := store.Put(ctx, target, []ports.WriteOp{{Key: "feature_flag", Value: true}}, domain.RevisionZero, domain.Actor{}, "api")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "lock revision")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("multiple ops in single transaction", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		actor := domain.Actor{ID: "admin"}
		ops := []ports.WriteOp{
			{Key: "a", Value: "val_a"},
			{Key: "b", Value: "val_b"},
		}

		mock.ExpectBegin()

		mock.ExpectExec(`INSERT INTO test_schema.runtime_revisions`).
			WithArgs("config", "global", "", uint64(0), sqlmock.AnyArg(), "admin", "api").
			WillReturnResult(sqlmock.NewResult(0, 1))

		mock.ExpectQuery(`SELECT revision`).
			WithArgs("config", "global", "").
			WillReturnRows(sqlmock.NewRows([]string{"rev"}).AddRow(0))

		// Op 1: key "a"
		mock.ExpectQuery(`SELECT value FROM`).
			WithArgs("config", "global", "", "a").
			WillReturnError(sql.ErrNoRows)
		mock.ExpectExec(`INSERT INTO`).
			WithArgs("config", "global", "", "a",
				sqlmock.AnyArg(), uint64(1), sqlmock.AnyArg(), "admin", "api",
			).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(`INSERT INTO test_schema.runtime_history`).
			WithArgs("config", "global", "", "a",
				nil, sqlmock.AnyArg(), uint64(1), "admin", sqlmock.AnyArg(), "api",
			).WillReturnResult(sqlmock.NewResult(1, 1))

		// Op 2: key "b"
		mock.ExpectQuery(`SELECT value FROM`).
			WithArgs("config", "global", "", "b").
			WillReturnError(sql.ErrNoRows)
		mock.ExpectExec(`INSERT INTO`).
			WithArgs("config", "global", "", "b",
				sqlmock.AnyArg(), uint64(1), sqlmock.AnyArg(), "admin", "api",
			).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(`INSERT INTO test_schema.runtime_history`).
			WithArgs("config", "global", "", "b",
				nil, sqlmock.AnyArg(), uint64(1), "admin", sqlmock.AnyArg(), "api",
			).WillReturnResult(sqlmock.NewResult(1, 1))

		mock.ExpectExec(`UPDATE test_schema.runtime_revisions`).
			WithArgs("config", "global", "", uint64(1), sqlmock.AnyArg(), sqlmock.AnyArg(), "admin", "api").
			WillReturnResult(sqlmock.NewResult(0, 1))

		mock.ExpectExec(`SELECT pg_notify`).
			WithArgs("test_changes", sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 0))

		mock.ExpectCommit()

		rev, err := store.Put(ctx, target, ops, domain.RevisionZero, actor, "api")

		require.NoError(t, err)
		assert.Equal(t, domain.Revision(1), rev)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("upsert failure rolls back", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()
		actor := domain.Actor{ID: "admin"}
		ops := []ports.WriteOp{
			{Key: "log_level", Value: "debug"},
		}

		mock.ExpectBegin()

		mock.ExpectExec(`INSERT INTO test_schema.runtime_revisions`).
			WithArgs("config", "global", "", uint64(0), sqlmock.AnyArg(), "admin", "api").
			WillReturnResult(sqlmock.NewResult(0, 1))

		mock.ExpectQuery(`SELECT revision`).
			WithArgs("config", "global", "").
			WillReturnRows(sqlmock.NewRows([]string{"rev"}).AddRow(0))

		mock.ExpectQuery(`SELECT value FROM`).
			WithArgs("config", "global", "", "log_level").
			WillReturnError(sql.ErrNoRows)

		mock.ExpectExec(`INSERT INTO`).
			WithArgs("config", "global", "", "log_level",
				sqlmock.AnyArg(), uint64(1), sqlmock.AnyArg(), "admin", "api",
			).WillReturnError(errors.New("disk full"))

		mock.ExpectRollback()

		_, err := store.Put(ctx, target, ops, domain.RevisionZero, actor, "api")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "apply op")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("notify payload contains target and revision", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := tenantTarget()
		actor := domain.Actor{ID: "user1"}
		ops := []ports.WriteOp{
			{Key: "theme", Value: "dark"},
		}
		expectedPayload := notifyPayload{Kind: "setting", Scope: "tenant", Subject: "tenant-abc", Revision: 1, ApplyBehavior: string(domain.ApplyBundleRebuild)}

		mock.ExpectBegin()

		mock.ExpectExec(`INSERT INTO test_schema.runtime_revisions`).
			WithArgs("setting", "tenant", "tenant-abc", uint64(0), sqlmock.AnyArg(), "user1", "ui").
			WillReturnResult(sqlmock.NewResult(0, 1))

		mock.ExpectQuery(`SELECT revision`).
			WithArgs("setting", "tenant", "tenant-abc").
			WillReturnRows(sqlmock.NewRows([]string{"rev"}).AddRow(0))

		mock.ExpectQuery(`SELECT value FROM`).
			WithArgs("setting", "tenant", "tenant-abc", "theme").
			WillReturnError(sql.ErrNoRows)

		mock.ExpectExec(`INSERT INTO`).
			WithArgs("setting", "tenant", "tenant-abc", "theme",
				sqlmock.AnyArg(), uint64(1), sqlmock.AnyArg(), "user1", "ui",
			).WillReturnResult(sqlmock.NewResult(0, 1))

		mock.ExpectExec(`INSERT INTO test_schema.runtime_history`).
			WithArgs("setting", "tenant", "tenant-abc", "theme",
				nil, sqlmock.AnyArg(), uint64(1), "user1", sqlmock.AnyArg(), "ui",
			).WillReturnResult(sqlmock.NewResult(1, 1))

		mock.ExpectExec(`UPDATE test_schema.runtime_revisions`).
			WithArgs("setting", "tenant", "tenant-abc", uint64(1), sqlmock.AnyArg(), sqlmock.AnyArg(), "user1", "ui").
			WillReturnResult(sqlmock.NewResult(0, 1))

		// Capture the notify payload to validate its structure
		mock.ExpectExec(`SELECT pg_notify`).
			WithArgs("test_changes", notifyPayloadMatcher{t: t, expected: expectedPayload}).
			WillReturnResult(sqlmock.NewResult(0, 0))

		mock.ExpectCommit()

		rev, err := store.Put(ctx, target, ops, domain.RevisionZero, actor, "ui")

		require.NoError(t, err)
		assert.Equal(t, domain.Revision(1), rev)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestStore_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	// This test verifies compile-time interface satisfaction.
	var _ ports.Store = (*Store)(nil)
}

func TestNullableJSONB(t *testing.T) {
	t.Parallel()

	t.Run("nil input returns nil", func(t *testing.T) {
		t.Parallel()

		result := nullableJSONB(nil)
		assert.Nil(t, result)
	})

	t.Run("empty input returns nil", func(t *testing.T) {
		t.Parallel()

		result := nullableJSONB([]byte{})
		assert.Nil(t, result)
	})

	t.Run("non-empty input returns bytes", func(t *testing.T) {
		t.Parallel()

		data := []byte(`{"key":"value"}`)
		result := nullableJSONB(data)
		assert.Equal(t, data, result)
	})
}
