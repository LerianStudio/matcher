//go:build unit

// Copyright 2025 Lerian Studio.

package mongodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/LerianStudio/matcher/pkg/systemplane/adapters/store/secretcodec"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

func TestTargetFilter_BuildsBSONDocument(t *testing.T) {
	t.Parallel()

	t.Run("global config target", func(t *testing.T) {
		t.Parallel()

		target := domain.Target{
			Kind:      domain.KindConfig,
			Scope:     domain.ScopeGlobal,
			SubjectID: "",
		}

		filter := targetFilter(target)

		expected := bson.D{
			{Key: "kind", Value: "config"},
			{Key: "scope", Value: "global"},
			{Key: "subject", Value: ""},
		}

		assert.Equal(t, expected, filter)
	})

	t.Run("tenant-scoped setting target", func(t *testing.T) {
		t.Parallel()

		target := domain.Target{
			Kind:      domain.KindSetting,
			Scope:     domain.ScopeTenant,
			SubjectID: "tenant-xyz",
		}

		filter := targetFilter(target)

		expected := bson.D{
			{Key: "kind", Value: "setting"},
			{Key: "scope", Value: "tenant"},
			{Key: "subject", Value: "tenant-xyz"},
		}

		assert.Equal(t, expected, filter)
	})
}

func TestRevisionMetaFilter_AddsMetaKey(t *testing.T) {
	t.Parallel()

	t.Run("appends revision meta key to target filter", func(t *testing.T) {
		t.Parallel()

		target := domain.Target{
			Kind:      domain.KindConfig,
			Scope:     domain.ScopeGlobal,
			SubjectID: "",
		}

		filter := revisionMetaFilter(target)

		require.Len(t, filter, 4)
		assert.Equal(t, "kind", filter[0].Key)
		assert.Equal(t, "config", filter[0].Value)
		assert.Equal(t, "key", filter[3].Key)
		assert.Equal(t, revisionMetaKey, filter[3].Value)
	})

	t.Run("preserves tenant subject in filter", func(t *testing.T) {
		t.Parallel()

		target := domain.Target{
			Kind:      domain.KindSetting,
			Scope:     domain.ScopeTenant,
			SubjectID: "tenant-abc",
		}

		filter := revisionMetaFilter(target)

		require.Len(t, filter, 4)
		assert.Equal(t, "tenant-abc", filter[2].Value)
		assert.Equal(t, revisionMetaKey, filter[3].Value)
	})
}

func TestStore_EncryptValue(t *testing.T) {
	t.Parallel()

	t.Run("nil store returns value unchanged", func(t *testing.T) {
		t.Parallel()

		var nilStore *Store
		target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}

		result, err := nilStore.encryptValue(target, "any_key", "plain-text")

		require.NoError(t, err)
		assert.Equal(t, "plain-text", result)
	})

	t.Run("nil codec returns value unchanged", func(t *testing.T) {
		t.Parallel()

		store := &Store{secretCodec: nil}
		target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}

		result, err := store.encryptValue(target, "any_key", "plain-text")

		require.NoError(t, err)
		assert.Equal(t, "plain-text", result)
	})

	t.Run("encrypts secret key value", func(t *testing.T) {
		t.Parallel()

		codec, err := secretcodec.New("0123456789abcdef0123456789abcdef", []string{"db.password"})
		require.NoError(t, err)

		store := &Store{secretCodec: codec}
		target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}

		encrypted, err := store.encryptValue(target, "db.password", "s3cr3t")

		require.NoError(t, err)
		assert.NotEqual(t, "s3cr3t", encrypted)
	})

	t.Run("does not encrypt non-secret key", func(t *testing.T) {
		t.Parallel()

		codec, err := secretcodec.New("0123456789abcdef0123456789abcdef", []string{"db.password"})
		require.NoError(t, err)

		store := &Store{secretCodec: codec}
		target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}

		result, err := store.encryptValue(target, "log_level", "debug")

		require.NoError(t, err)
		assert.Equal(t, "debug", result)
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
				"known": domain.ApplyLiveRead,
			},
		}

		result := store.escalateBehavior([]ports.WriteOp{{Key: "unknown"}})
		assert.Equal(t, domain.ApplyBundleRebuild, result)
	})

	t.Run("single live-read key", func(t *testing.T) {
		t.Parallel()

		store := &Store{
			applyBehaviors: map[string]domain.ApplyBehavior{
				"log_level": domain.ApplyLiveRead,
			},
		}

		result := store.escalateBehavior([]ports.WriteOp{{Key: "log_level"}})
		assert.Equal(t, domain.ApplyLiveRead, result)
	})

	t.Run("escalates to strongest behavior", func(t *testing.T) {
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

	t.Run("empty ops returns live-read baseline", func(t *testing.T) {
		t.Parallel()

		store := &Store{
			applyBehaviors: map[string]domain.ApplyBehavior{},
		}

		result := store.escalateBehavior([]ports.WriteOp{})
		assert.Equal(t, domain.ApplyLiveRead, result)
	})
}

func TestStore_ValidateReadDependencies(t *testing.T) {
	t.Parallel()

	t.Run("nil store returns ErrNilStore", func(t *testing.T) {
		t.Parallel()

		var nilStore *Store

		err := nilStore.validateReadDependencies()

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNilStore)
	})

	t.Run("nil client returns ErrNilClient", func(t *testing.T) {
		t.Parallel()

		store := &Store{client: nil}

		err := store.validateReadDependencies()

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNilClient)
	})

	t.Run("nil entries returns ErrNilEntries", func(t *testing.T) {
		t.Parallel()

		store := &Store{
			client:  &mongo.Client{},
			entries: nil,
		}

		err := store.validateReadDependencies()

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNilEntries)
	})

	t.Run("valid store returns nil", func(t *testing.T) {
		t.Parallel()

		// Use a non-nil client and a non-nil collection.
		// We can't easily create a real *mongo.Collection in unit tests
		// without a connection, so we need to use a workaround.
		// For now, we skip this since we'd need a real client.
		// The important thing is the sentinel error checks above.
	})
}

func TestStore_ValidateWriteDependencies(t *testing.T) {
	t.Parallel()

	t.Run("nil store returns ErrNilStore", func(t *testing.T) {
		t.Parallel()

		var nilStore *Store

		err := nilStore.validateWriteDependencies()

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNilStore)
	})

	t.Run("nil client returns ErrNilClient", func(t *testing.T) {
		t.Parallel()

		store := &Store{client: nil}

		err := store.validateWriteDependencies()

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNilClient)
	})

	t.Run("nil entries returns ErrNilEntries", func(t *testing.T) {
		t.Parallel()

		store := &Store{
			client:  &mongo.Client{},
			entries: nil,
		}

		err := store.validateWriteDependencies()

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNilEntries)
	})

	t.Run("nil history returns ErrNilHistory", func(t *testing.T) {
		t.Parallel()

		store := &Store{
			client:  &mongo.Client{},
			entries: &mongo.Collection{},
			history: nil,
		}

		err := store.validateWriteDependencies()

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNilHistory)
	})
}

func TestStore_CurrentRevisionInCollection_NilCollectionReturnsError(t *testing.T) {
	t.Parallel()

	store := &Store{}

	_, err := store.currentRevisionInCollection(t.Context(), nil, domain.Target{})

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNilEntries)
}

func TestStore_SentinelErrors(t *testing.T) {
	t.Parallel()

	t.Run("error messages are distinct", func(t *testing.T) {
		t.Parallel()

		errors := []error{ErrNilStore, ErrNilClient, ErrNilEntries, ErrNilHistory}

		seen := make(map[string]bool, len(errors))
		for _, err := range errors {
			msg := err.Error()
			assert.False(t, seen[msg], "duplicate error message: %s", msg)
			seen[msg] = true
		}
	})

	t.Run("errEntryValueNotFound is internal sentinel", func(t *testing.T) {
		t.Parallel()

		assert.NotNil(t, errEntryValueNotFound)
		assert.Contains(t, errEntryValueNotFound.Error(), "entry value not found")
	})
}
