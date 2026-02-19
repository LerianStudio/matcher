//go:build unit

package export_job

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExportJobErrors_NotNil(t *testing.T) {
	t.Parallel()

	require.Error(t, ErrRepositoryNotInitialized)
	assert.NotEmpty(t, ErrRepositoryNotInitialized.Error())
}

func TestExportJobErrors_Messages(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "export job repository not initialized", ErrRepositoryNotInitialized.Error())
}

func TestExportJobErrors_CanBeWrapped(t *testing.T) {
	t.Parallel()

	testErr := errors.New("test error")
	wrapped := errors.Join(testErr, ErrRepositoryNotInitialized)
	assert.ErrorIs(t, wrapped, ErrRepositoryNotInitialized)
}
