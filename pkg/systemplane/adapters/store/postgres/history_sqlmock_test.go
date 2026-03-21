// Copyright 2025 Lerian Studio.

//go:build unit

package postgres

import (
	"context"
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

// newTestHistoryStore creates a HistoryStore with a sqlmock-backed *sql.DB.
func newTestHistoryStore(t *testing.T) (*HistoryStore, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	t.Cleanup(func() { db.Close() })

	store := &HistoryStore{
		db:           db,
		schema:       "test_schema",
		historyTable: "runtime_history",
	}

	return store, mock
}

func withHistorySecretCodec(t *testing.T, store *HistoryStore, keys ...string) {
	t.Helper()

	codec, err := secretcodec.New("0123456789abcdef0123456789abcdef", keys)
	require.NoError(t, err)
	store.secretCodec = codec
}

func TestHistoryStore_ListHistory(t *testing.T) {
	t.Parallel()

	t.Run("returns all entries without filters", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestHistoryStore(t)
		ctx := context.Background()

		now := time.Now().UTC()
		oldVal, err := json.Marshal("info")
		require.NoError(t, err)
		newVal, err := json.Marshal("debug")
		require.NoError(t, err)

		rows := sqlmock.NewRows([]string{"kind", "key", "scope", "subject", "old_value", "new_value", "revision", "actor_id", "changed_at"}).
			AddRow("config", "log_level", "global", "", oldVal, newVal, 2, "admin", now).
			AddRow("config", "log_level", "global", "", nil, oldVal, 1, "admin", now.Add(-time.Hour))

		mock.ExpectQuery(`SELECT kind, key, scope, subject, old_value, new_value, revision, actor_id, changed_at FROM test_schema.runtime_history ORDER BY changed_at DESC`).
			WillReturnRows(rows)

		entries, err := store.ListHistory(ctx, ports.HistoryFilter{})

		require.NoError(t, err)
		require.Len(t, entries, 2)
		assert.Equal(t, domain.Revision(2), entries[0].Revision)
		assert.Equal(t, "info", entries[0].OldValue)
		assert.Equal(t, "debug", entries[0].NewValue)
		assert.Nil(t, entries[1].OldValue)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("decrypts secret history values", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestHistoryStore(t)
		withHistorySecretCodec(t, store, "rabbitmq.password")
		ctx := context.Background()
		now := time.Now().UTC()
		target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}

		oldEncrypted, err := store.secretCodec.Encrypt(target, "rabbitmq.password", "old-secret")
		require.NoError(t, err)
		newEncrypted, err := store.secretCodec.Encrypt(target, "rabbitmq.password", "new-secret")
		require.NoError(t, err)
		oldRaw, err := json.Marshal(oldEncrypted)
		require.NoError(t, err)
		newRaw, err := json.Marshal(newEncrypted)
		require.NoError(t, err)

		rows := sqlmock.NewRows([]string{"kind", "key", "scope", "subject", "old_value", "new_value", "revision", "actor_id", "changed_at"}).
			AddRow("config", "rabbitmq.password", "global", "", oldRaw, newRaw, 3, "admin", now)
		mock.ExpectQuery(`SELECT kind, key, scope, subject, old_value, new_value, revision, actor_id, changed_at FROM test_schema.runtime_history ORDER BY changed_at DESC`).
			WillReturnRows(rows)

		entries, err := store.ListHistory(ctx, ports.HistoryFilter{})
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, "old-secret", entries[0].OldValue)
		assert.Equal(t, "new-secret", entries[0].NewValue)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("filters by kind", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestHistoryStore(t)
		ctx := context.Background()

		now := time.Now().UTC()
		newVal, err := json.Marshal("value")
		require.NoError(t, err)

		rows := sqlmock.NewRows([]string{"kind", "key", "scope", "subject", "old_value", "new_value", "revision", "actor_id", "changed_at"}).
			AddRow("config", "key1", "global", "", nil, newVal, 1, "admin", now)

		mock.ExpectQuery(`SELECT kind, key, scope, subject, old_value, new_value, revision, actor_id, changed_at FROM test_schema.runtime_history WHERE kind = \$1 ORDER BY changed_at DESC`).
			WithArgs("config").
			WillReturnRows(rows)

		entries, err := store.ListHistory(ctx, ports.HistoryFilter{
			Kind: domain.KindConfig,
		})

		require.NoError(t, err)
		assert.Len(t, entries, 1)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("filters by kind and scope", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestHistoryStore(t)
		ctx := context.Background()

		rows := sqlmock.NewRows([]string{"kind", "key", "scope", "subject", "old_value", "new_value", "revision", "actor_id", "changed_at"})

		mock.ExpectQuery(`SELECT kind, key, scope, subject, old_value, new_value, revision, actor_id, changed_at FROM test_schema.runtime_history WHERE kind = \$1 AND scope = \$2 ORDER BY changed_at DESC`).
			WithArgs("setting", "tenant").
			WillReturnRows(rows)

		entries, err := store.ListHistory(ctx, ports.HistoryFilter{
			Kind:  domain.KindSetting,
			Scope: domain.ScopeTenant,
		})

		require.NoError(t, err)
		assert.Empty(t, entries)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("filters by subject", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestHistoryStore(t)
		ctx := context.Background()

		rows := sqlmock.NewRows([]string{"kind", "key", "scope", "subject", "old_value", "new_value", "revision", "actor_id", "changed_at"})

		mock.ExpectQuery(`SELECT kind, key, scope, subject, old_value, new_value, revision, actor_id, changed_at FROM test_schema.runtime_history WHERE subject = \$1 ORDER BY changed_at DESC`).
			WithArgs("tenant-xyz").
			WillReturnRows(rows)

		entries, err := store.ListHistory(ctx, ports.HistoryFilter{
			SubjectID: "tenant-xyz",
		})

		require.NoError(t, err)
		assert.Empty(t, entries)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("filters by key", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestHistoryStore(t)
		ctx := context.Background()

		rows := sqlmock.NewRows([]string{"kind", "key", "scope", "subject", "old_value", "new_value", "revision", "actor_id", "changed_at"})

		mock.ExpectQuery(`SELECT kind, key, scope, subject, old_value, new_value, revision, actor_id, changed_at FROM test_schema.runtime_history WHERE key = \$1 ORDER BY changed_at DESC`).
			WithArgs("log_level").
			WillReturnRows(rows)

		entries, err := store.ListHistory(ctx, ports.HistoryFilter{
			Key: "log_level",
		})

		require.NoError(t, err)
		assert.Empty(t, entries)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("applies all filters together", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestHistoryStore(t)
		ctx := context.Background()

		rows := sqlmock.NewRows([]string{"kind", "key", "scope", "subject", "old_value", "new_value", "revision", "actor_id", "changed_at"})

		mock.ExpectQuery(`SELECT kind, key, scope, subject, old_value, new_value, revision, actor_id, changed_at FROM test_schema.runtime_history WHERE kind = \$1 AND scope = \$2 AND subject = \$3 AND key = \$4 ORDER BY changed_at DESC`).
			WithArgs("setting", "tenant", "tenant-abc", "theme").
			WillReturnRows(rows)

		entries, err := store.ListHistory(ctx, ports.HistoryFilter{
			Kind:      domain.KindSetting,
			Scope:     domain.ScopeTenant,
			SubjectID: "tenant-abc",
			Key:       "theme",
		})

		require.NoError(t, err)
		assert.Empty(t, entries)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("applies limit", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestHistoryStore(t)
		ctx := context.Background()

		rows := sqlmock.NewRows([]string{"kind", "key", "scope", "subject", "old_value", "new_value", "revision", "actor_id", "changed_at"})

		mock.ExpectQuery(`SELECT kind, key, scope, subject, old_value, new_value, revision, actor_id, changed_at FROM test_schema.runtime_history ORDER BY changed_at DESC, revision DESC, id DESC LIMIT \$1`).
			WithArgs(10).
			WillReturnRows(rows)

		entries, err := store.ListHistory(ctx, ports.HistoryFilter{
			Limit: 10,
		})

		require.NoError(t, err)
		assert.Empty(t, entries)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("applies offset", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestHistoryStore(t)
		ctx := context.Background()

		rows := sqlmock.NewRows([]string{"kind", "key", "scope", "subject", "old_value", "new_value", "revision", "actor_id", "changed_at"})

		mock.ExpectQuery(`SELECT kind, key, scope, subject, old_value, new_value, revision, actor_id, changed_at FROM test_schema.runtime_history ORDER BY changed_at DESC, revision DESC, id DESC OFFSET \$1`).
			WithArgs(5).
			WillReturnRows(rows)

		entries, err := store.ListHistory(ctx, ports.HistoryFilter{
			Offset: 5,
		})

		require.NoError(t, err)
		assert.Empty(t, entries)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("applies limit and offset together", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestHistoryStore(t)
		ctx := context.Background()

		rows := sqlmock.NewRows([]string{"kind", "key", "scope", "subject", "old_value", "new_value", "revision", "actor_id", "changed_at"})

		mock.ExpectQuery(`SELECT kind, key, scope, subject, old_value, new_value, revision, actor_id, changed_at FROM test_schema.runtime_history ORDER BY changed_at DESC, revision DESC, id DESC LIMIT \$1 OFFSET \$2`).
			WithArgs(10, 20).
			WillReturnRows(rows)

		entries, err := store.ListHistory(ctx, ports.HistoryFilter{
			Limit:  10,
			Offset: 20,
		})

		require.NoError(t, err)
		assert.Empty(t, entries)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("filter with limit and kind", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestHistoryStore(t)
		ctx := context.Background()

		rows := sqlmock.NewRows([]string{"kind", "key", "scope", "subject", "old_value", "new_value", "revision", "actor_id", "changed_at"})

		mock.ExpectQuery(`SELECT kind, key, scope, subject, old_value, new_value, revision, actor_id, changed_at FROM test_schema.runtime_history WHERE kind = \$1 ORDER BY changed_at DESC, revision DESC, id DESC LIMIT \$2`).
			WithArgs("config", 5).
			WillReturnRows(rows)

		entries, err := store.ListHistory(ctx, ports.HistoryFilter{
			Kind:  domain.KindConfig,
			Limit: 5,
		})

		require.NoError(t, err)
		assert.Empty(t, entries)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error on query failure", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestHistoryStore(t)
		ctx := context.Background()

		mock.ExpectQuery(`SELECT kind, key, scope, subject, old_value, new_value, revision, actor_id, changed_at`).
			WillReturnError(errors.New("connection lost"))

		_, err := store.ListHistory(ctx, ports.HistoryFilter{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "postgres history list: query:")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error on scan failure", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestHistoryStore(t)
		ctx := context.Background()

		// Wrong column count to trigger scan error
		rows := sqlmock.NewRows([]string{"key", "scope"}).
			AddRow("log_level", "global")

		mock.ExpectQuery(`SELECT kind, key, scope, subject, old_value, new_value, revision, actor_id, changed_at`).
			WillReturnRows(rows)

		_, err := store.ListHistory(ctx, ports.HistoryFilter{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "postgres history list: scan:")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error on invalid old_value JSON", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestHistoryStore(t)
		ctx := context.Background()

		now := time.Now().UTC()
		rows := sqlmock.NewRows([]string{"kind", "key", "scope", "subject", "old_value", "new_value", "revision", "actor_id", "changed_at"}).
			AddRow("config", "key1", "global", "", []byte("{bad}"), nil, 1, "admin", now)

		mock.ExpectQuery(`SELECT kind, key, scope, subject, old_value, new_value, revision, actor_id, changed_at`).
			WillReturnRows(rows)

		_, err := store.ListHistory(ctx, ports.HistoryFilter{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal old_value")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error on invalid new_value JSON", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestHistoryStore(t)
		ctx := context.Background()

		now := time.Now().UTC()
		rows := sqlmock.NewRows([]string{"kind", "key", "scope", "subject", "old_value", "new_value", "revision", "actor_id", "changed_at"}).
			AddRow("config", "key1", "global", "", nil, []byte("{bad}"), 1, "admin", now)

		mock.ExpectQuery(`SELECT kind, key, scope, subject, old_value, new_value, revision, actor_id, changed_at`).
			WillReturnRows(rows)

		_, err := store.ListHistory(ctx, ports.HistoryFilter{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal new_value")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("handles nil old and new values", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestHistoryStore(t)
		ctx := context.Background()

		now := time.Now().UTC()
		rows := sqlmock.NewRows([]string{"kind", "key", "scope", "subject", "old_value", "new_value", "revision", "actor_id", "changed_at"}).
			AddRow("config", "key1", "global", "", nil, nil, 1, "admin", now)

		mock.ExpectQuery(`SELECT kind, key, scope, subject, old_value, new_value, revision, actor_id, changed_at`).
			WillReturnRows(rows)

		entries, err := store.ListHistory(ctx, ports.HistoryFilter{})

		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Nil(t, entries[0].OldValue)
		assert.Nil(t, entries[0].NewValue)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("populates all entry fields correctly", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestHistoryStore(t)
		ctx := context.Background()

		now := time.Now().UTC()
		oldVal, err := json.Marshal(map[string]any{"old": true})
		require.NoError(t, err)
		newVal, err := json.Marshal(map[string]any{"new": true})
		require.NoError(t, err)

		rows := sqlmock.NewRows([]string{"kind", "key", "scope", "subject", "old_value", "new_value", "revision", "actor_id", "changed_at"}).
			AddRow("setting", "theme", "tenant", "tenant-123", oldVal, newVal, 5, "user1", now)

		mock.ExpectQuery(`SELECT kind, key, scope, subject, old_value, new_value, revision, actor_id, changed_at`).
			WillReturnRows(rows)

		entries, err := store.ListHistory(ctx, ports.HistoryFilter{})

		require.NoError(t, err)
		require.Len(t, entries, 1)

		e := entries[0]
		assert.Equal(t, "theme", e.Key)
		assert.Equal(t, domain.ScopeTenant, e.Scope)
		assert.Equal(t, "tenant-123", e.SubjectID)
		assert.Equal(t, domain.Revision(5), e.Revision)
		assert.Equal(t, "user1", e.ActorID)
		assert.Equal(t, now, e.ChangedAt)
		assert.NotNil(t, e.OldValue)
		assert.NotNil(t, e.NewValue)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestHistoryStore_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	var _ ports.HistoryStore = (*HistoryStore)(nil)
}
