//go:build unit

package dispute

import (
	"errors"
	"testing"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDisputeSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{"ErrRepoNotInitialized", ErrRepoNotInitialized, "dispute repository not initialized"},
		{"ErrDisputeNotFound", ErrDisputeNotFound, "dispute not found"},
		{"ErrDisputeNil", ErrDisputeNil, "dispute is nil"},
		{"ErrTransactionRequired", ErrTransactionRequired, "transaction is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestDisputeErrorsAreDifferent(t *testing.T) {
	t.Parallel()

	require.NotErrorIs(t, ErrRepoNotInitialized, ErrDisputeNil)
	require.NotErrorIs(t, ErrDisputeNil, ErrTransactionRequired)
}

func TestErrTransactionRequired_CanonicalIdentity(t *testing.T) {
	t.Parallel()

	require.True(t, errors.Is(ErrTransactionRequired, pgcommon.ErrTransactionRequired))
}
