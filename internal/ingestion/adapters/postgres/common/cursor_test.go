//go:build unit

package common

import (
	"testing"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
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
	assert.Equal(t, "DESC", dir)
	assert.Equal(t, libHTTP.CursorDirectionNext, cursorDir)
	sql, _, sqlErr := result.ToSql()
	require.NoError(t, sqlErr)
	assert.Contains(t, sql, "ORDER BY id DESC")
	assert.Contains(t, sql, "LIMIT 11")
}

func TestApplyCursorPagination_SortCursor_EmptyCursor(t *testing.T) {
	t.Parallel()

	builder := squirrel.Select("*").From("transactions")

	result, dir, cursorDir, err := ApplyCursorPagination(
		builder, "", "created_at", "ASC", 20, false, "transactions",
	)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "ASC", dir)
	assert.Equal(t, libHTTP.CursorDirectionNext, cursorDir)
	sql, _, sqlErr := result.ToSql()
	require.NoError(t, sqlErr)
	assert.Contains(t, sql, "ORDER BY transactions.created_at ASC, transactions.id ASC")
	assert.Contains(t, sql, "LIMIT 21")
}

func TestApplyCursorPagination_IDCursor_ValidCursor_AppendsBoundaryFilter(t *testing.T) {
	t.Parallel()

	cursor, err := libHTTP.EncodeCursor(libHTTP.Cursor{ID: "abc-123", Direction: libHTTP.CursorDirectionNext})
	require.NoError(t, err)

	result, dir, cursorDir, err := ApplyCursorPagination(
		squirrel.Select("*").From("transactions"), cursor, "created_at", "DESC", 10, true, "transactions",
	)
	require.NoError(t, err)
	assert.Equal(t, "DESC", dir)
	assert.Equal(t, libHTTP.CursorDirectionNext, cursorDir)
	sql, args, sqlErr := result.ToSql()
	require.NoError(t, sqlErr)
	assert.Contains(t, sql, "WHERE id < ?")
	assert.Contains(t, sql, "ORDER BY id DESC")
	require.Len(t, args, 1)
	assert.Equal(t, "abc-123", args[0])
}

func TestApplyCursorPagination_SortCursor_ValidCursor_AppendsCompositeBoundary(t *testing.T) {
	t.Parallel()

	cursor, err := libHTTP.EncodeSortCursor("created_at", "2025-01-01T00:00:00Z", "abc-123", true)
	require.NoError(t, err)

	result, dir, cursorDir, err := ApplyCursorPagination(
		squirrel.Select("*").From("transactions"), cursor, "created_at", "ASC", 20, false, "transactions",
	)
	require.NoError(t, err)
	assert.Equal(t, "ASC", dir)
	assert.Equal(t, libHTTP.CursorDirectionNext, cursorDir)
	sql, args, sqlErr := result.ToSql()
	require.NoError(t, sqlErr)
	assert.Contains(t, sql, "(transactions.created_at, transactions.id) > (?, ?)")
	assert.Contains(t, sql, "ORDER BY transactions.created_at ASC, transactions.id ASC")
	require.Len(t, args, 2)
	assert.Equal(t, "2025-01-01T00:00:00Z", args[0])
	assert.Equal(t, "abc-123", args[1])
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
