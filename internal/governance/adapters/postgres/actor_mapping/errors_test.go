//go:build unit

package actormapping

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActorMappingSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{"ErrRepositoryNotInitialized", ErrRepositoryNotInitialized, "actor mapping repository not initialized"},
		{"ErrActorMappingRequired", ErrActorMappingRequired, "actor mapping is required"},
		{"ErrActorIDRequired", ErrActorIDRequired, "actor id is required"},
		{"ErrNilScanner", ErrNilScanner, "nil scanner"},
		{"ErrActorMappingNotFound", ErrActorMappingNotFound, "actor mapping not found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestActorMappingErrorsAreDifferent(t *testing.T) {
	t.Parallel()

	require.NotErrorIs(t, ErrRepositoryNotInitialized, ErrActorMappingRequired)
	require.NotErrorIs(t, ErrActorMappingRequired, ErrActorIDRequired)
	require.NotErrorIs(t, ErrActorIDRequired, ErrNilScanner)
}
