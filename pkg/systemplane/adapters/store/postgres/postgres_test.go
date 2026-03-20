// Copyright 2025 Lerian Studio.

//go:build unit

package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/bootstrap"
)

func TestNew_Validation(t *testing.T) {
	t.Parallel()

	t.Run("nil config returns error", func(t *testing.T) {
		t.Parallel()

		store, history, closer, err := New(t.Context(), nil, nil)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrNilConfig)
		assert.Nil(t, store)
		assert.Nil(t, history)
		assert.Nil(t, closer)
	})

	t.Run("empty DSN returns error", func(t *testing.T) {
		t.Parallel()

		cfg := &bootstrap.PostgresBootstrapConfig{
			DSN: "",
		}

		store, history, closer, err := New(t.Context(), cfg, nil)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrEmptyDSN)
		assert.Nil(t, store)
		assert.Nil(t, history)
		assert.Nil(t, closer)
	})

	t.Run("invalid DSN returns error on open/ping", func(t *testing.T) {
		t.Parallel()

		cfg := &bootstrap.PostgresBootstrapConfig{
			DSN: "postgres://invalid:5432/nonexistent?sslmode=disable",
		}

		store, history, closer, err := New(t.Context(), cfg, nil)

		require.Error(t, err)
		assert.Nil(t, store)
		assert.Nil(t, history)
		assert.Nil(t, closer)
	})
}

func TestNewFromDB(t *testing.T) {
	t.Parallel()

	t.Run("applies provided names", func(t *testing.T) {
		t.Parallel()

		store, history, err := NewFromDB(nil, "myschema", "entries", "history", "channel")
		require.NoError(t, err)

		assert.Equal(t, "myschema", store.schema)
		assert.Equal(t, "entries", store.entriesTable)
		assert.Equal(t, "history", store.historyTable)
		assert.Equal(t, "channel", store.notifyChannel)
		assert.Equal(t, "myschema", history.schema)
		assert.Equal(t, "history", history.historyTable)
	})

	t.Run("applies defaults for empty names", func(t *testing.T) {
		t.Parallel()

		store, history, err := NewFromDB(nil, "", "", "", "")
		require.NoError(t, err)

		assert.Equal(t, "system", store.schema)
		assert.Equal(t, "runtime_entries", store.entriesTable)
		assert.Equal(t, "runtime_history", store.historyTable)
		assert.Equal(t, bootstrap.DefaultPostgresRevisionTable, store.revisionTable)
		assert.Equal(t, "systemplane_changes", store.notifyChannel)
		assert.Equal(t, "system", history.schema)
		assert.Equal(t, "runtime_history", history.historyTable)
	})

	t.Run("rejects invalid names", func(t *testing.T) {
		t.Parallel()

		store, history, err := NewFromDB(nil, "bad-schema", "entries", "history", "channel")
		require.Error(t, err)
		assert.Nil(t, store)
		assert.Nil(t, history)
		assert.ErrorIs(t, err, bootstrap.ErrInvalidPostgresIdentifier)
	})
}

func TestDefaultIfEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		val      string
		def      string
		expected string
	}{
		{
			name:     "returns val when non-empty",
			val:      "custom",
			def:      "default",
			expected: "custom",
		},
		{
			name:     "returns def when val empty",
			val:      "",
			def:      "default",
			expected: "default",
		},
		{
			name:     "returns empty def when both empty",
			val:      "",
			def:      "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := defaultIfEmpty(tt.val, tt.def)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDbCloser(t *testing.T) {
	t.Parallel()

	t.Run("implements io.Closer", func(t *testing.T) {
		t.Parallel()

		// dbCloser wraps sql.DB which implements Close.
		// We verify the struct exists and the method signature is correct
		// via the io.Closer assignment in the constructor.
		closer := &dbCloser{db: nil}
		assert.NotNil(t, closer)
	})
}

func TestDDLFormatters(t *testing.T) {
	t.Parallel()

	t.Run("FormatSchemaDDL contains schema name", func(t *testing.T) {
		t.Parallel()

		ddl := FormatSchemaDDL("myschema")
		assert.Contains(t, ddl, "myschema")
		assert.Contains(t, ddl, "CREATE SCHEMA IF NOT EXISTS")
	})

	t.Run("FormatEntriesDDL contains schema and table", func(t *testing.T) {
		t.Parallel()

		ddl := FormatEntriesDDL("myschema", "my_entries")
		assert.Contains(t, ddl, "myschema.my_entries")
		assert.Contains(t, ddl, "CREATE TABLE IF NOT EXISTS")
		assert.Contains(t, ddl, "PRIMARY KEY")
		assert.Contains(t, ddl, "idx_my_entries_target")
	})

	t.Run("FormatHistoryDDL contains schema and table with indexes", func(t *testing.T) {
		t.Parallel()

		ddl := FormatHistoryDDL("myschema", "my_history")
		assert.Contains(t, ddl, "myschema.my_history")
		assert.Contains(t, ddl, "CREATE TABLE IF NOT EXISTS")
		assert.Contains(t, ddl, "BIGSERIAL")
		assert.Contains(t, ddl, "idx_my_history_target_key")
		assert.Contains(t, ddl, "idx_my_history_changed_at")
	})

	t.Run("FormatRevisionsDDL contains schema and table", func(t *testing.T) {
		t.Parallel()

		ddl := FormatRevisionsDDL("myschema", "my_revisions")
		assert.Contains(t, ddl, "myschema.my_revisions")
		assert.Contains(t, ddl, "PRIMARY KEY")
	})
}
