//go:build unit

package connection

import (
	"errors"
	"testing"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
)

func TestConnectionSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{
			name:    "ErrConnectionNotFound",
			err:     ErrConnectionNotFound,
			message: "fetcher connection not found",
		},
		{
			name:    "ErrProviderRequired",
			err:     ErrProviderRequired,
			message: "infrastructure provider is required",
		},
		{
			name:    "ErrRepoNotInitialized",
			err:     ErrRepoNotInitialized,
			message: "connection repository not initialized",
		},
		{
			name:    "ErrEntityRequired",
			err:     ErrEntityRequired,
			message: "fetcher connection entity is required",
		},
		{
			name:    "ErrModelRequired",
			err:     ErrModelRequired,
			message: "fetcher connection model is required",
		},
		{
			name:    "ErrTransactionRequired",
			err:     ErrTransactionRequired,
			message: "transaction is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.EqualError(t, tt.err, tt.message)
		})
	}
}

func TestConnectionErrors_CanonicalIdentity(t *testing.T) {
	t.Parallel()

	t.Run("ErrTransactionRequired re-exports pgcommon", func(t *testing.T) {
		t.Parallel()

		assert.True(t, errors.Is(ErrTransactionRequired, pgcommon.ErrTransactionRequired))
	})

	t.Run("ErrConnectionNotFound re-exports repositories", func(t *testing.T) {
		t.Parallel()

		assert.True(t, errors.Is(ErrConnectionNotFound, repositories.ErrConnectionNotFound))
	})

	t.Run("ErrProviderRequired re-exports repositories", func(t *testing.T) {
		t.Parallel()

		assert.True(t, errors.Is(ErrProviderRequired, repositories.ErrProviderRequired))
	})

	t.Run("ErrRepoNotInitialized re-exports repositories", func(t *testing.T) {
		t.Parallel()

		assert.True(t, errors.Is(ErrRepoNotInitialized, repositories.ErrRepoNotInitialized))
	})

	t.Run("ErrEntityRequired re-exports repositories", func(t *testing.T) {
		t.Parallel()

		assert.True(t, errors.Is(ErrEntityRequired, repositories.ErrEntityRequired))
	})

	t.Run("ErrModelRequired re-exports repositories", func(t *testing.T) {
		t.Parallel()

		assert.True(t, errors.Is(ErrModelRequired, repositories.ErrModelRequired))
	})
}
