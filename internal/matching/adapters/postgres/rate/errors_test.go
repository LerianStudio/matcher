//go:build unit

package rate

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
		{"ErrRepoNotInitialized", ErrRepoNotInitialized, "rate repository not initialized"},
		{"ErrRateModelNeeded", ErrRateModelNeeded, "rate model is required"},
		{"ErrRateNotFound", ErrRateNotFound, "rate not found"},
		{"ErrUnknownStructureType", ErrUnknownStructureType, "unknown fee structure type"},
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

	require.NotErrorIs(t, ErrRepoNotInitialized, ErrRateModelNeeded)
	require.NotErrorIs(t, ErrRateModelNeeded, ErrRateNotFound)
	require.NotErrorIs(t, ErrRateNotFound, ErrUnknownStructureType)
}
