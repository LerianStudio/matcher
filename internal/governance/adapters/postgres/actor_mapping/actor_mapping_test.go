//go:build unit

package actormapping

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSentinelErrors_Distinctness(t *testing.T) {
	t.Parallel()

	allErrors := []error{
		ErrRepositoryNotInitialized,
		ErrActorMappingRequired,
		ErrActorIDRequired,
		ErrActorMappingNotFound,
		ErrNilScanner,
	}

	for i, a := range allErrors {
		for j, b := range allErrors {
			if i == j {
				continue
			}

			assert.NotEqual(t, a.Error(), b.Error(),
				"sentinel errors at index %d and %d must have distinct messages", i, j)
			assert.False(t, errors.Is(a, b),
				"sentinel error %q must not match %q via errors.Is", a, b)
		}
	}
}

func TestSentinelErrors_ExpectedMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		wantMsg string
	}{
		{
			name:    "ErrRepositoryNotInitialized",
			err:     ErrRepositoryNotInitialized,
			wantMsg: "actor mapping repository not initialized",
		},
		{
			name:    "ErrActorMappingRequired",
			err:     ErrActorMappingRequired,
			wantMsg: "actor mapping is required",
		},
		{
			name:    "ErrActorIDRequired",
			err:     ErrActorIDRequired,
			wantMsg: "actor id is required",
		},
		{
			name:    "ErrActorMappingNotFound",
			err:     ErrActorMappingNotFound,
			wantMsg: "actor mapping not found",
		},
		{
			name:    "ErrNilScanner",
			err:     ErrNilScanner,
			wantMsg: "nil scanner",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.wantMsg, tt.err.Error())
		})
	}
}

func TestScanActorMapping_NilScanner_ReturnsNilEntity(t *testing.T) {
	t.Parallel()

	mapping, err := scanActorMapping(nil)

	require.Error(t, err)
	assert.Nil(t, mapping, "nil scanner must return nil entity")
	assert.ErrorIs(t, err, ErrNilScanner)
	assert.Contains(t, err.Error(), "scanning actor mapping")
}

func TestScanActorMapping_NilOptionalFields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	mapping, err := scanActorMapping(fakeScanner{scan: func(dest ...any) error {
		if ptr, ok := dest[0].(*string); ok {
			*ptr = "actor-nil-opts"
		}

		// dest[1] (*string for DisplayName) and dest[2] (*string for Email)
		// are left at their zero values (nil), simulating NULL columns.

		if ptr, ok := dest[3].(*time.Time); ok {
			*ptr = now
		}

		if ptr, ok := dest[4].(*time.Time); ok {
			*ptr = now
		}

		return nil
	}})

	require.NoError(t, err)
	require.NotNil(t, mapping)
	assert.Equal(t, "actor-nil-opts", mapping.ActorID)
	assert.Nil(t, mapping.DisplayName, "DisplayName should be nil when DB column is NULL")
	assert.Nil(t, mapping.Email, "Email should be nil when DB column is NULL")
	assert.Equal(t, now, mapping.CreatedAt)
	assert.Equal(t, now, mapping.UpdatedAt)
}

func TestScanActorMapping_ErrorWrapping_PreservesOriginal(t *testing.T) {
	t.Parallel()

	originalErr := errors.New("column mismatch")

	_, err := scanActorMapping(fakeScanner{scan: func(_ ...any) error {
		return originalErr
	}})

	require.Error(t, err)
	assert.ErrorIs(t, err, originalErr,
		"wrapped error must be unwrappable to the original scan error")
	assert.Contains(t, err.Error(), "scanning actor mapping",
		"error message must include the wrapping context")
}

func TestScanActorMapping_AllFieldsPopulated_ReturnsCompleteMapping(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	later := now.Add(time.Hour)
	displayName := "Alice"
	email := "alice@example.com"

	mapping, err := scanActorMapping(fakeScanner{scan: func(dest ...any) error {
		if ptr, ok := dest[0].(*string); ok {
			*ptr = "actor-complete"
		}

		if ptr, ok := dest[1].(**string); ok {
			*ptr = &displayName
		}

		if ptr, ok := dest[2].(**string); ok {
			*ptr = &email
		}

		if ptr, ok := dest[3].(*time.Time); ok {
			*ptr = now
		}

		if ptr, ok := dest[4].(*time.Time); ok {
			*ptr = later
		}

		return nil
	}})

	require.NoError(t, err)
	require.NotNil(t, mapping)

	assert.Equal(t, "actor-complete", mapping.ActorID)
	require.NotNil(t, mapping.DisplayName)
	assert.Equal(t, "Alice", *mapping.DisplayName)
	require.NotNil(t, mapping.Email)
	assert.Equal(t, "alice@example.com", *mapping.Email)
	assert.Equal(t, now, mapping.CreatedAt)
	assert.Equal(t, later, mapping.UpdatedAt,
		"UpdatedAt should differ from CreatedAt when set independently")
}
