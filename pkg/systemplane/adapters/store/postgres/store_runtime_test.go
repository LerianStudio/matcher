//go:build unit

// Copyright 2025 Lerian Studio.

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/adapters/store/secretcodec"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

func TestStore_EncryptValue(t *testing.T) {
	t.Parallel()

	t.Run("nil store returns value unchanged", func(t *testing.T) {
		t.Parallel()

		var nilStore *Store
		target := configTarget()

		result, err := nilStore.encryptValue(target, "some_key", "plain-text")

		require.NoError(t, err)
		assert.Equal(t, "plain-text", result)
	})

	t.Run("nil codec returns value unchanged", func(t *testing.T) {
		t.Parallel()

		store := &Store{secretCodec: nil}
		target := configTarget()

		result, err := store.encryptValue(target, "some_key", "plain-text")

		require.NoError(t, err)
		assert.Equal(t, "plain-text", result)
	})

	t.Run("encrypts secret key value", func(t *testing.T) {
		t.Parallel()

		codec, err := secretcodec.New("0123456789abcdef0123456789abcdef", []string{"db.password"})
		require.NoError(t, err)

		store := &Store{secretCodec: codec}
		target := configTarget()

		encrypted, err := store.encryptValue(target, "db.password", "s3cr3t")

		require.NoError(t, err)
		assert.NotEqual(t, "s3cr3t", encrypted)
		assert.NotNil(t, encrypted)
	})

	t.Run("does not encrypt non-secret key", func(t *testing.T) {
		t.Parallel()

		codec, err := secretcodec.New("0123456789abcdef0123456789abcdef", []string{"db.password"})
		require.NoError(t, err)

		store := &Store{secretCodec: codec}
		target := configTarget()

		result, err := store.encryptValue(target, "log_level", "debug")

		require.NoError(t, err)
		assert.Equal(t, "debug", result)
	})
}

func TestStore_DecryptValue(t *testing.T) {
	t.Parallel()

	t.Run("nil store returns value unchanged", func(t *testing.T) {
		t.Parallel()

		var nilStore *Store
		target := configTarget()

		result, err := nilStore.decryptValue(target, "some_key", "plain-text")

		require.NoError(t, err)
		assert.Equal(t, "plain-text", result)
	})

	t.Run("nil codec returns value unchanged", func(t *testing.T) {
		t.Parallel()

		store := &Store{secretCodec: nil}
		target := configTarget()

		result, err := store.decryptValue(target, "some_key", "plain-text")

		require.NoError(t, err)
		assert.Equal(t, "plain-text", result)
	})

	t.Run("round-trips encrypt then decrypt", func(t *testing.T) {
		t.Parallel()

		codec, err := secretcodec.New("0123456789abcdef0123456789abcdef", []string{"redis.password"})
		require.NoError(t, err)

		store := &Store{secretCodec: codec}
		target := configTarget()

		encrypted, err := store.encryptValue(target, "redis.password", "my-secret")
		require.NoError(t, err)

		decrypted, err := store.decryptValue(target, "redis.password", encrypted)

		require.NoError(t, err)
		assert.Equal(t, "my-secret", decrypted)
	})
}

func TestStore_Notify(t *testing.T) {
	t.Parallel()

	t.Run("sends pg_notify with correct payload", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := configTarget()

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		expectedPayload := notifyPayload{
			Kind:          "config",
			Scope:         "global",
			Subject:       "",
			Revision:      5,
			ApplyBehavior: "bundle-rebuild",
		}

		mock.ExpectExec(`SELECT pg_notify`).
			WithArgs("test_changes", notifyPayloadMatcher{t: t, expected: expectedPayload}).
			WillReturnResult(sqlmock.NewResult(0, 0))

		err = store.notify(ctx, tx, target, domain.Revision(5), domain.ApplyBundleRebuild)

		require.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("sends pg_notify for tenant target", func(t *testing.T) {
		t.Parallel()

		store, mock := newTestStore(t)
		ctx := context.Background()
		target := tenantTarget()

		mock.ExpectBegin()
		tx, err := store.db.Begin()
		require.NoError(t, err)

		expectedPayload := notifyPayload{
			Kind:          "setting",
			Scope:         "tenant",
			Subject:       "tenant-abc",
			Revision:      3,
			ApplyBehavior: "live-read",
		}

		mock.ExpectExec(`SELECT pg_notify`).
			WithArgs("test_changes", notifyPayloadMatcher{t: t, expected: expectedPayload}).
			WillReturnResult(sqlmock.NewResult(0, 0))

		err = store.notify(ctx, tx, target, domain.Revision(3), domain.ApplyLiveRead)

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

		mock.ExpectExec(`SELECT pg_notify`).
			WithArgs("test_changes", sqlmock.AnyArg()).
			WillReturnError(errors.New("connection lost"))

		err = store.notify(ctx, tx, target, domain.Revision(1), domain.ApplyBundleRebuild)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "notify exec")
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestNotifyPayload_MarshalJSON(t *testing.T) {
	t.Parallel()

	t.Run("marshals complete payload", func(t *testing.T) {
		t.Parallel()

		payload := notifyPayload{
			Kind:          "config",
			Scope:         "global",
			Subject:       "",
			Revision:      42,
			ApplyBehavior: "bundle-rebuild",
		}

		data, err := json.Marshal(payload)

		require.NoError(t, err)
		assert.Contains(t, string(data), `"kind":"config"`)
		assert.Contains(t, string(data), `"scope":"global"`)
		assert.Contains(t, string(data), `"revision":42`)
		assert.Contains(t, string(data), `"apply_behavior":"bundle-rebuild"`)
	})

	t.Run("omits empty apply_behavior", func(t *testing.T) {
		t.Parallel()

		payload := notifyPayload{
			Kind:     "config",
			Scope:    "global",
			Revision: 1,
		}

		data, err := json.Marshal(payload)

		require.NoError(t, err)
		assert.NotContains(t, string(data), "apply_behavior")
	})
}

func TestStore_EscalateBehavior(t *testing.T) {
	t.Parallel()

	t.Run("nil store returns bundle rebuild", func(t *testing.T) {
		t.Parallel()

		var nilStore *Store

		result := nilStore.escalateBehavior([]ports.WriteOp{{Key: "any"}})
		assert.Equal(t, domain.ApplyBundleRebuild, result)
	})

	t.Run("unknown key returns bundle rebuild", func(t *testing.T) {
		t.Parallel()

		store := &Store{
			applyBehaviors: map[string]domain.ApplyBehavior{
				"known_key": domain.ApplyLiveRead,
			},
		}

		result := store.escalateBehavior([]ports.WriteOp{{Key: "unknown_key"}})
		assert.Equal(t, domain.ApplyBundleRebuild, result)
	})

	t.Run("single live-read key returns live-read", func(t *testing.T) {
		t.Parallel()

		store := &Store{
			applyBehaviors: map[string]domain.ApplyBehavior{
				"log_level": domain.ApplyLiveRead,
			},
		}

		result := store.escalateBehavior([]ports.WriteOp{{Key: "log_level"}})
		assert.Equal(t, domain.ApplyLiveRead, result)
	})

	t.Run("escalates to strongest behavior across ops", func(t *testing.T) {
		t.Parallel()

		store := &Store{
			applyBehaviors: map[string]domain.ApplyBehavior{
				"log_level": domain.ApplyLiveRead,
				"db_host":   domain.ApplyBundleRebuild,
				"max_conns": domain.ApplyWorkerReconcile,
			},
		}

		result := store.escalateBehavior([]ports.WriteOp{
			{Key: "log_level"},
			{Key: "max_conns"},
			{Key: "db_host"},
		})
		assert.Equal(t, domain.ApplyBundleRebuild, result)
	})

	t.Run("empty ops returns live-read (base level)", func(t *testing.T) {
		t.Parallel()

		store := &Store{
			applyBehaviors: map[string]domain.ApplyBehavior{},
		}

		result := store.escalateBehavior([]ports.WriteOp{})
		assert.Equal(t, domain.ApplyLiveRead, result)
	})

	t.Run("worker-reconcile is stronger than live-read", func(t *testing.T) {
		t.Parallel()

		store := &Store{
			applyBehaviors: map[string]domain.ApplyBehavior{
				"key_a": domain.ApplyLiveRead,
				"key_b": domain.ApplyWorkerReconcile,
			},
		}

		result := store.escalateBehavior([]ports.WriteOp{
			{Key: "key_a"},
			{Key: "key_b"},
		})
		assert.Equal(t, domain.ApplyWorkerReconcile, result)
	})
}
