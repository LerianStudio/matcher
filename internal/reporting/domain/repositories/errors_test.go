//go:build unit

package repositories

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrExportJobNotFound(t *testing.T) {
	t.Parallel()

	t.Run("error is not nil", func(t *testing.T) {
		t.Parallel()

		assert.Error(t, ErrExportJobNotFound)
	})

	t.Run("error message is correct", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "export job not found", ErrExportJobNotFound.Error())
	})

	t.Run("errors.Is works correctly", func(t *testing.T) {
		t.Parallel()

		assert.NotErrorIs(t, ErrExportJobNotFound, assert.AnError)
	})

	t.Run("errors.Is detects wrapped sentinel error", func(t *testing.T) {
		t.Parallel()

		wrapped := fmt.Errorf("failed to retrieve job with id 123: %w", ErrExportJobNotFound)

		assert.ErrorIs(t, wrapped, ErrExportJobNotFound)
	})
}
