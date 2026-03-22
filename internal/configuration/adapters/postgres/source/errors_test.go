//go:build unit

package source

import (
	"errors"
	"testing"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/stretchr/testify/assert"
)

func TestSourceSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{
			name:    "ErrSourceEntityRequired",
			err:     ErrSourceEntityRequired,
			message: "source entity is required",
		},
		{
			name:    "ErrSourceEntityIDRequired",
			err:     ErrSourceEntityIDRequired,
			message: "source entity ID is required",
		},
		{
			name:    "ErrSourceModelRequired",
			err:     ErrSourceModelRequired,
			message: "source model is required",
		},
		{
			name:    "ErrRepoNotInitialized",
			err:     ErrRepoNotInitialized,
			message: "source repository not initialized",
		},
		{
			name:    "ErrConnectionRequired",
			err:     ErrConnectionRequired,
			message: "postgres connection is required",
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

func TestSourceSentinelErrors_CanonicalIdentity(t *testing.T) {
	t.Parallel()

	assert.True(t, errors.Is(ErrConnectionRequired, pgcommon.ErrConnectionRequired))
	assert.True(t, errors.Is(ErrTransactionRequired, pgcommon.ErrTransactionRequired))
}
