//go:build unit

// Copyright 2025 Lerian Studio.

package postgres

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

func TestStore_QualifiedEntries(t *testing.T) {
	t.Parallel()

	store := &Store{
		schema:       "myschema",
		entriesTable: "my_entries",
	}

	assert.Equal(t, "myschema.my_entries", store.qualifiedEntries())
}

func TestStore_QualifiedHistory(t *testing.T) {
	t.Parallel()

	store := &Store{
		schema:       "myschema",
		historyTable: "my_history",
	}

	assert.Equal(t, "myschema.my_history", store.qualifiedHistory())
}

func TestStore_QualifiedRevisions(t *testing.T) {
	t.Parallel()

	store := &Store{
		schema:        "myschema",
		revisionTable: "my_revisions",
	}

	assert.Equal(t, "myschema.my_revisions", store.qualifiedRevisions())
}

func TestStore_Get_NilGuards(t *testing.T) {
	t.Parallel()

	t.Run("nil store returns ErrNilDB", func(t *testing.T) {
		t.Parallel()

		var nilStore *Store
		_, err := nilStore.Get(context.Background(), configTarget())

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNilDB)
	})

	t.Run("nil db returns ErrNilDB", func(t *testing.T) {
		t.Parallel()

		store := &Store{db: nil}
		_, err := store.Get(context.Background(), configTarget())

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNilDB)
	})
}

func TestStore_Put_NilGuards(t *testing.T) {
	t.Parallel()

	t.Run("nil store returns ErrNilDB", func(t *testing.T) {
		t.Parallel()

		var nilStore *Store
		_, err := nilStore.Put(context.Background(), configTarget(), nil, domain.RevisionZero, domain.Actor{}, "test")

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNilDB)
	})

	t.Run("nil db returns ErrNilDB", func(t *testing.T) {
		t.Parallel()

		store := &Store{db: nil}
		_, err := store.Put(context.Background(), configTarget(), nil, domain.RevisionZero, domain.Actor{}, "test")

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNilDB)
	})
}

func TestStore_Put_EmptyOps(t *testing.T) {
	t.Parallel()

	t.Run("returns current revision for empty ops slice", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		mock.ExpectQuery(`SELECT revision FROM`).
			WithArgs("config", "global", "").
			WillReturnRows(
				sqlmock.NewRows([]string{"revision"}).AddRow(5),
			)

		rev, err := store.Put(ctx, target, []ports.WriteOp{}, domain.RevisionZero, domain.Actor{}, "api")

		require.NoError(t, err)
		assert.Equal(t, domain.Revision(5), rev)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestStore_NotifyPayloadStruct(t *testing.T) {
	t.Parallel()

	t.Run("holds all required fields", func(t *testing.T) {
		t.Parallel()

		payload := notifyPayload{
			Kind:          "config",
			Scope:         "global",
			Subject:       "subject-1",
			Revision:      99,
			ApplyBehavior: "live-read",
		}

		assert.Equal(t, "config", payload.Kind)
		assert.Equal(t, "global", payload.Scope)
		assert.Equal(t, "subject-1", payload.Subject)
		assert.Equal(t, uint64(99), payload.Revision)
		assert.Equal(t, "live-read", payload.ApplyBehavior)
	})
}

func TestStore_SentinelErrors(t *testing.T) {
	t.Parallel()

	t.Run("ErrRevisionRowUpdateMismatch is distinct", func(t *testing.T) {
		t.Parallel()

		assert.NotNil(t, ErrRevisionRowUpdateMismatch)
		assert.Contains(t, ErrRevisionRowUpdateMismatch.Error(), "revision row update mismatch")
	})

	t.Run("ErrNilDB is distinct", func(t *testing.T) {
		t.Parallel()

		assert.NotNil(t, ErrNilDB)
		assert.Contains(t, ErrNilDB.Error(), "db is nil")
	})

	t.Run("ErrNilConfig is distinct", func(t *testing.T) {
		t.Parallel()

		assert.NotNil(t, ErrNilConfig)
		assert.Contains(t, ErrNilConfig.Error(), "config is nil")
	})

	t.Run("ErrEmptyDSN is distinct", func(t *testing.T) {
		t.Parallel()

		assert.NotNil(t, ErrEmptyDSN)
		assert.Contains(t, ErrEmptyDSN.Error(), "DSN is required")
	})

	t.Run("errors are distinguishable from each other", func(t *testing.T) {
		t.Parallel()

		assert.NotEqual(t, ErrNilConfig, ErrEmptyDSN)
		assert.NotEqual(t, ErrNilDB, ErrNilConfig)
		assert.NotEqual(t, ErrRevisionRowUpdateMismatch, ErrNilDB)
	})
}
