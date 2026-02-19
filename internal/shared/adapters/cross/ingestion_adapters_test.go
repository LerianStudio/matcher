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

func TestNewIngestionContextProviderAdapter(t *testing.T) {
	t.Parallel()

	t.Run("returns nil when repo is nil", func(t *testing.T) {
		t.Parallel()

		adapter := NewIngestionContextProviderAdapter(nil)
		require.Nil(t, adapter)
	})

	t.Run("returns adapter when repo is provided", func(t *testing.T) {
		t.Parallel()

		repo := &stubContextRepository{}
		adapter := NewIngestionContextProviderAdapter(repo)
		require.NotNil(t, adapter)
	})
}

func TestIngestionContextProviderAdapter_FindByID(t *testing.T) {
	t.Parallel()

	t.Run("returns error when adapter is nil", func(t *testing.T) {
		t.Parallel()

		var adapter *IngestionContextProviderAdapter

		_, err := adapter.FindByID(context.Background(), uuid.New(), uuid.New())
		require.ErrorIs(t, err, ErrContextRepositoryRequired)
	})

	t.Run("returns nil when context not found", func(t *testing.T) {
		t.Parallel()

		repo := &stubContextRepository{ctx: nil, err: nil}
		adapter := NewIngestionContextProviderAdapter(repo)

		info, err := adapter.FindByID(context.Background(), uuid.New(), uuid.New())
		require.NoError(t, err)
		require.Nil(t, info)
	})

	t.Run("returns error when repository fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errTestDBError
		repo := &stubContextRepository{ctx: nil, err: expectedErr}
		adapter := NewIngestionContextProviderAdapter(repo)

		_, err := adapter.FindByID(context.Background(), uuid.New(), uuid.New())
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
		adapter := NewIngestionContextProviderAdapter(repo)

		info, err := adapter.FindByID(context.Background(), uuid.New(), contextID)
		require.NoError(t, err)
		require.NotNil(t, info)
		require.Equal(t, contextID, info.ID)
		assert.True(t, info.Active)
	})

	t.Run("returns nil when sql.ErrNoRows", func(t *testing.T) {
		t.Parallel()

		repo := &stubContextRepository{ctx: nil, err: sql.ErrNoRows}
		adapter := NewIngestionContextProviderAdapter(repo)

		info, err := adapter.FindByID(context.Background(), uuid.New(), uuid.New())
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
		adapter := NewIngestionContextProviderAdapter(repo)

		info, err := adapter.FindByID(context.Background(), uuid.New(), contextID)
		require.NoError(t, err)
		require.NotNil(t, info)
		require.Equal(t, contextID, info.ID)
		assert.False(t, info.Active)
	})
}

func TestNewReportingContextProviderAdapter(t *testing.T) {
	t.Parallel()

	t.Run("returns nil when repo is nil", func(t *testing.T) {
		t.Parallel()

		adapter := NewReportingContextProviderAdapter(nil)
		require.Nil(t, adapter)
	})

	t.Run("returns adapter when repo is provided", func(t *testing.T) {
		t.Parallel()

		repo := &stubContextRepository{}
		adapter := NewReportingContextProviderAdapter(repo)
		require.NotNil(t, adapter)
	})
}

func TestReportingContextProviderAdapter_FindByID(t *testing.T) {
	t.Parallel()

	t.Run("returns error when adapter is nil", func(t *testing.T) {
		t.Parallel()

		var adapter *ReportingContextProviderAdapter

		_, err := adapter.FindByID(context.Background(), uuid.New(), uuid.New())
		require.ErrorIs(t, err, ErrContextRepositoryRequired)
	})

	t.Run("returns nil when context not found", func(t *testing.T) {
		t.Parallel()

		repo := &stubContextRepository{ctx: nil, err: nil}
		adapter := NewReportingContextProviderAdapter(repo)

		info, err := adapter.FindByID(context.Background(), uuid.New(), uuid.New())
		require.NoError(t, err)
		require.Nil(t, info)
	})

	t.Run("returns error when repository fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errTestDBError
		repo := &stubContextRepository{ctx: nil, err: expectedErr}
		adapter := NewReportingContextProviderAdapter(repo)

		_, err := adapter.FindByID(context.Background(), uuid.New(), uuid.New())
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
		adapter := NewReportingContextProviderAdapter(repo)

		info, err := adapter.FindByID(context.Background(), uuid.New(), contextID)
		require.NoError(t, err)
		require.NotNil(t, info)
		require.Equal(t, contextID, info.ID)
		assert.True(t, info.Active)
	})

	t.Run("returns nil when sql.ErrNoRows", func(t *testing.T) {
		t.Parallel()

		repo := &stubContextRepository{ctx: nil, err: sql.ErrNoRows}
		adapter := NewReportingContextProviderAdapter(repo)

		info, err := adapter.FindByID(context.Background(), uuid.New(), uuid.New())
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
		adapter := NewReportingContextProviderAdapter(repo)

		info, err := adapter.FindByID(context.Background(), uuid.New(), contextID)
		require.NoError(t, err)
		require.NotNil(t, info)
		require.Equal(t, contextID, info.ID)
		assert.False(t, info.Active)
	})
}
