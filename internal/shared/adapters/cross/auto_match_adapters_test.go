//go:build unit

package cross

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"
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

func TestNewMatchTriggerAdapter(t *testing.T) {
	t.Parallel()

	t.Run("returns error when use case is nil", func(t *testing.T) {
		t.Parallel()

		adapter, err := NewMatchTriggerAdapter(nil)

		assert.Nil(t, adapter)
		require.ErrorIs(t, err, ErrNilMatchingUseCase)
	})

	t.Run("returns adapter when use case is provided", func(t *testing.T) {
		t.Parallel()

		uc := &matchingCommand.UseCase{}

		adapter, err := NewMatchTriggerAdapter(uc)

		require.NoError(t, err)
		require.NotNil(t, adapter)
		assert.Equal(t, uc, adapter.matchingUseCase)
	})
}

func TestMatchTriggerAdapter_TriggerMatchForContext(t *testing.T) {
	t.Parallel()

	t.Run("launches asynchronous match without blocking", func(t *testing.T) {
		t.Parallel()

		// Create adapter with a zero-value UseCase. RunMatch will return
		// an error immediately (nil dependency validation), which the
		// goroutine handles gracefully by logging a warning.
		uc := &matchingCommand.UseCase{}

		adapter, err := NewMatchTriggerAdapter(uc)
		require.NoError(t, err)

		// TriggerMatchForContext spawns a goroutine and returns immediately.
		// It must not panic or block the caller.
		adapter.TriggerMatchForContext(
			context.Background(),
			testutil.DeterministicUUID("trigger-tenant"),
			testutil.DeterministicUUID("trigger-context"),
		)

		// Allow the goroutine enough time to execute and complete.
		// time.Sleep is acceptable here: TriggerMatchForContext is fire-and-forget
		// by design, so there is no synchronization channel to wait on. We only
		// verify that the goroutine does not panic.
		time.Sleep(100 * time.Millisecond)
	})
}
