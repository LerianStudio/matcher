//go:build unit

package common

import (
	"testing"

	"github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyCursorPagination_IDCursor_EmptyCursor(t *testing.T) {
	t.Parallel()

	builder := squirrel.Select("*").From("transactions")

	result, dir, cursorDir, err := ApplyCursorPagination(
		builder, "", "created_at", "DESC", 10, true, "transactions",
	)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, dir)
	assert.NotEmpty(t, cursorDir)
}

func TestApplyCursorPagination_SortCursor_EmptyCursor(t *testing.T) {
	t.Parallel()

	builder := squirrel.Select("*").From("transactions")

	result, dir, cursorDir, err := ApplyCursorPagination(
		builder, "", "created_at", "ASC", 20, false, "transactions",
	)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, dir)
	assert.NotEmpty(t, cursorDir)
}

func TestApplyCursorPagination_IDCursor_InvalidCursor(t *testing.T) {
	t.Parallel()

	builder := squirrel.Select("*").From("transactions")

	_, _, _, err := ApplyCursorPagination(
		builder, "not-valid-base64!!!", "created_at", "DESC", 10, true, "transactions",
	)
	// Invalid cursor should return an error.
	assert.Error(t, err)
}

func TestApplyCursorPagination_SortCursor_InvalidCursor(t *testing.T) {
	t.Parallel()

	builder := squirrel.Select("*").From("transactions")

	_, _, _, err := ApplyCursorPagination(
		builder, "not-valid-base64!!!", "created_at", "ASC", 10, false, "transactions",
	)
	// Invalid cursor should return an error.
	assert.Error(t, err)
}
