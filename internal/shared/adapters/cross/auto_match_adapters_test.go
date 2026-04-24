// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package cross

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// errTestAutoMatch is a sentinel error used for testing auto-match adapter failure scenarios.
var errTestAutoMatch = errors.New("auto-match test error")

func TestNewAutoMatchContextProviderAdapter(t *testing.T) {
	t.Parallel()

	t.Run("returns error when repo is nil", func(t *testing.T) {
		t.Parallel()

		adapter, err := NewAutoMatchContextProviderAdapter(nil)

		require.Nil(t, adapter)
		require.ErrorIs(t, err, ErrNilContextRepository)
	})

	t.Run("returns adapter when repo is provided", func(t *testing.T) {
		t.Parallel()

		repo := &stubContextRepository{}
		adapter, err := NewAutoMatchContextProviderAdapter(repo)

		require.NoError(t, err)
		require.NotNil(t, adapter)
		assert.Equal(t, repo, adapter.repo)
	})
}

func TestAutoMatchContextProviderAdapter_IsAutoMatchEnabled(t *testing.T) {
	t.Parallel()

	t.Run("returns error when adapter is nil", func(t *testing.T) {
		t.Parallel()

		var adapter *AutoMatchContextProviderAdapter

		enabled, err := adapter.IsAutoMatchEnabled(context.Background(), testutil.DeterministicUUID("nil-adapter-ctx"))

		assert.False(t, enabled)
		require.ErrorIs(t, err, ErrContextRepositoryRequired)
	})

	t.Run("returns error when repo is nil", func(t *testing.T) {
		t.Parallel()

		adapter := &AutoMatchContextProviderAdapter{repo: nil}

		enabled, err := adapter.IsAutoMatchEnabled(context.Background(), testutil.DeterministicUUID("nil-repo-ctx"))

		assert.False(t, enabled)
		require.ErrorIs(t, err, ErrContextRepositoryRequired)
	})

	t.Run("returns false with no error when sql.ErrNoRows", func(t *testing.T) {
		t.Parallel()

		repo := &stubContextRepository{ctx: nil, err: sql.ErrNoRows}
		adapter, adapterErr := NewAutoMatchContextProviderAdapter(repo)
		require.NoError(t, adapterErr)

		enabled, err := adapter.IsAutoMatchEnabled(context.Background(), testutil.DeterministicUUID("not-found-ctx"))

		assert.False(t, enabled)
		require.NoError(t, err)
	})

	t.Run("returns wrapped error when repository fails", func(t *testing.T) {
		t.Parallel()

		repo := &stubContextRepository{ctx: nil, err: errTestAutoMatch}
		adapter, adapterErr := NewAutoMatchContextProviderAdapter(repo)
		require.NoError(t, adapterErr)

		enabled, err := adapter.IsAutoMatchEnabled(context.Background(), testutil.DeterministicUUID("repo-error-ctx"))

		assert.False(t, enabled)
		require.Error(t, err)
		require.ErrorIs(t, err, errTestAutoMatch)
		assert.Contains(t, err.Error(), "check auto-match enabled")
	})

	t.Run("returns false when entity is nil", func(t *testing.T) {
		t.Parallel()

		repo := &stubContextRepository{ctx: nil, err: nil}
		adapter, adapterErr := NewAutoMatchContextProviderAdapter(repo)
		require.NoError(t, adapterErr)

		enabled, err := adapter.IsAutoMatchEnabled(context.Background(), testutil.DeterministicUUID("nil-entity-ctx"))

		assert.False(t, enabled)
		require.NoError(t, err)
	})

	t.Run("returns false when auto-match is disabled", func(t *testing.T) {
		t.Parallel()

		entity := &configEntities.ReconciliationContext{
			ID:                testutil.DeterministicUUID("disabled-context"),
			AutoMatchOnUpload: false,
			Status:            configVO.ContextStatusActive,
		}
		repo := &stubContextRepository{ctx: entity, err: nil}
		adapter, adapterErr := NewAutoMatchContextProviderAdapter(repo)
		require.NoError(t, adapterErr)

		enabled, err := adapter.IsAutoMatchEnabled(context.Background(), entity.ID)

		assert.False(t, enabled)
		require.NoError(t, err)
	})

	t.Run("returns false when auto-match is enabled but context is paused", func(t *testing.T) {
		t.Parallel()

		entity := &configEntities.ReconciliationContext{
			ID:                testutil.DeterministicUUID("paused-context"),
			AutoMatchOnUpload: true,
			Status:            configVO.ContextStatusPaused,
		}
		repo := &stubContextRepository{ctx: entity, err: nil}
		adapter, adapterErr := NewAutoMatchContextProviderAdapter(repo)
		require.NoError(t, adapterErr)

		enabled, err := adapter.IsAutoMatchEnabled(context.Background(), entity.ID)

		assert.False(t, enabled)
		require.NoError(t, err)
	})

	t.Run("returns true when auto-match is enabled and context is active", func(t *testing.T) {
		t.Parallel()

		entity := &configEntities.ReconciliationContext{
			ID:                testutil.DeterministicUUID("active-context"),
			AutoMatchOnUpload: true,
			Status:            configVO.ContextStatusActive,
		}
		repo := &stubContextRepository{ctx: entity, err: nil}
		adapter, adapterErr := NewAutoMatchContextProviderAdapter(repo)
		require.NoError(t, adapterErr)

		enabled, err := adapter.IsAutoMatchEnabled(context.Background(), entity.ID)

		assert.True(t, enabled)
		require.NoError(t, err)
	})
}
