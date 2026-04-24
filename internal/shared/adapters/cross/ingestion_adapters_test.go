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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
)

var errTestDBError = errors.New("test error")

func TestNewContextAccessProviderAdapter(t *testing.T) {
	t.Parallel()

	t.Run("returns nil when repo is nil", func(t *testing.T) {
		t.Parallel()

		adapter := NewContextAccessProviderAdapter(nil)
		require.Nil(t, adapter)
	})

	t.Run("returns adapter when repo is provided", func(t *testing.T) {
		t.Parallel()

		repo := &stubContextRepository{}
		adapter := NewContextAccessProviderAdapter(repo)
		require.NotNil(t, adapter)
	})
}

func TestContextAccessProviderAdapter_FindByID(t *testing.T) {
	t.Parallel()

	t.Run("returns error when adapter is nil", func(t *testing.T) {
		t.Parallel()

		var adapter *ContextAccessProviderAdapter

		_, err := adapter.FindByID(context.Background(), uuid.New())
		require.ErrorIs(t, err, ErrContextRepositoryRequired)
	})

	t.Run("returns nil when context not found", func(t *testing.T) {
		t.Parallel()

		repo := &stubContextRepository{ctx: nil, err: nil}
		adapter := NewContextAccessProviderAdapter(repo)

		info, err := adapter.FindByID(context.Background(), uuid.New())
		require.NoError(t, err)
		require.Nil(t, info)
	})

	t.Run("returns error when repository fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errTestDBError
		repo := &stubContextRepository{ctx: nil, err: expectedErr}
		adapter := NewContextAccessProviderAdapter(repo)

		_, err := adapter.FindByID(context.Background(), uuid.New())
		require.Error(t, err)
		require.Contains(t, err.Error(), "find context by id")
	})

	t.Run("returns context info on success", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		ctx := &configEntities.ReconciliationContext{
			ID:     contextID,
			Status: configVO.ContextStatusActive,
		}
		repo := &stubContextRepository{ctx: ctx, err: nil}
		adapter := NewContextAccessProviderAdapter(repo)

		info, err := adapter.FindByID(context.Background(), contextID)
		require.NoError(t, err)
		require.NotNil(t, info)
		require.Equal(t, contextID, info.ID)
		assert.True(t, info.Active)
	})

	t.Run("returns nil when sql.ErrNoRows", func(t *testing.T) {
		t.Parallel()

		repo := &stubContextRepository{ctx: nil, err: sql.ErrNoRows}
		adapter := NewContextAccessProviderAdapter(repo)

		info, err := adapter.FindByID(context.Background(), uuid.New())
		require.NoError(t, err)
		require.Nil(t, info)
	})

	t.Run("returns inactive context info", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		ctx := &configEntities.ReconciliationContext{
			ID:     contextID,
			Status: configVO.ContextStatusPaused,
		}
		repo := &stubContextRepository{ctx: ctx, err: nil}
		adapter := NewContextAccessProviderAdapter(repo)

		info, err := adapter.FindByID(context.Background(), contextID)
		require.NoError(t, err)
		require.NotNil(t, info)
		require.Equal(t, contextID, info.ID)
		assert.False(t, info.Active)
	})
}
