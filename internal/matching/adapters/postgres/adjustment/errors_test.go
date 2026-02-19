//go:build unit

package adjustment

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{"ErrRepoNotInitialized", ErrRepoNotInitialized, "adjustment repository not initialized"},
		{"ErrAdjustmentEntityNeeded", ErrAdjustmentEntityNeeded, "adjustment entity is required"},
		{"ErrAdjustmentModelNeeded", ErrAdjustmentModelNeeded, "adjustment model is required"},
		{"ErrInvalidTx", ErrInvalidTx, "invalid transaction type"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestErrorsAreDifferent(t *testing.T) {
	t.Parallel()

	require.NotErrorIs(t, ErrRepoNotInitialized, ErrAdjustmentEntityNeeded)
	require.NotErrorIs(t, ErrAdjustmentEntityNeeded, ErrAdjustmentModelNeeded)
	require.NotErrorIs(t, ErrAdjustmentModelNeeded, ErrInvalidTx)
	require.NotErrorIs(t, ErrInvalidTx, ErrTransactionRequired)
}
