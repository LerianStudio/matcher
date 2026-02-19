//go:build unit

package common

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
)

// encodeIDCursor is a test helper that creates a valid base64-encoded ID cursor.
func encodeIDCursor(t *testing.T, id string, direction string) string {
	t.Helper()

	cursor := libHTTP.Cursor{ID: id, Direction: direction}

	data, err := json.Marshal(cursor)
	require.NoError(t, err)

	return base64.StdEncoding.EncodeToString(data)
}

func TestApplyIDCursorPagination_NoCursor(t *testing.T) {
	t.Parallel()

	builder := squirrel.Select("id").From("test_table").PlaceholderFormat(squirrel.Dollar)

	result, dir, cursorDir, err := ApplyIDCursorPagination(builder, "", "ASC", 10)

	require.NoError(t, err)
	assert.Equal(t, libHTTP.CursorDirectionNext, cursorDir)
	assert.Equal(t, "ASC", dir)

	query, _, err := result.ToSql()
	require.NoError(t, err)
	assert.Contains(t, query, "test_table")
}

func TestApplyIDCursorPagination_ValidCursorForward(t *testing.T) {
	t.Parallel()

	builder := squirrel.Select("id").From("test_table").PlaceholderFormat(squirrel.Dollar)

	cursor := encodeIDCursor(t, "550e8400-e29b-41d4-a716-446655440000", libHTTP.CursorDirectionNext)

	result, _, cursorDir, err := ApplyIDCursorPagination(builder, cursor, "ASC", 10)

	require.NoError(t, err)
	assert.Equal(t, libHTTP.CursorDirectionNext, cursorDir)

	query, _, err := result.ToSql()
	require.NoError(t, err)
	assert.Contains(t, query, "test_table")
}

func TestApplyIDCursorPagination_ValidCursorBackward(t *testing.T) {
	t.Parallel()

	builder := squirrel.Select("id").From("test_table").PlaceholderFormat(squirrel.Dollar)

	cursor := encodeIDCursor(t, "550e8400-e29b-41d4-a716-446655440000", libHTTP.CursorDirectionPrev)

	result, _, cursorDir, err := ApplyIDCursorPagination(builder, cursor, "ASC", 10)

	require.NoError(t, err)
	assert.Equal(t, libHTTP.CursorDirectionPrev, cursorDir)

	query, _, err := result.ToSql()
	require.NoError(t, err)
	assert.Contains(t, query, "test_table")
}

func TestApplyIDCursorPagination_InvalidCursor(t *testing.T) {
	t.Parallel()

	builder := squirrel.Select("id").From("test_table").PlaceholderFormat(squirrel.Dollar)

	_, _, _, err := ApplyIDCursorPagination(builder, "not-valid-base64!", "ASC", 10)

	require.Error(t, err)
	require.ErrorIs(t, err, libHTTP.ErrInvalidCursor)
}

func TestApplySortCursorPagination_NoCursor(t *testing.T) {
	t.Parallel()

	builder := squirrel.Select("id", "created_at").
		From("test_table").
		PlaceholderFormat(squirrel.Dollar)

	result, dir, cursorDir, err := ApplySortCursorPagination(
		builder, "", "created_at", "ASC", "test_table", 10,
	)

	require.NoError(t, err)
	assert.Equal(t, libHTTP.CursorDirectionNext, cursorDir)
	assert.Equal(t, "ASC", dir)

	query, _, err := result.ToSql()
	require.NoError(t, err)
	assert.Contains(t, query, "test_table.created_at ASC")
	assert.Contains(t, query, "test_table.id ASC")
}

func TestApplySortCursorPagination_ForwardCursor(t *testing.T) {
	t.Parallel()

	builder := squirrel.Select("id", "created_at").
		From("test_table").
		PlaceholderFormat(squirrel.Dollar)

	cursor := libHTTP.EncodeSortCursor("created_at", "2024-01-01T00:00:00Z", "some-id", true)

	result, dir, cursorDir, err := ApplySortCursorPagination(
		builder, cursor, "created_at", "ASC", "test_table", 10,
	)

	require.NoError(t, err)
	assert.Equal(t, libHTTP.CursorDirectionNext, cursorDir)
	assert.Equal(t, "ASC", dir)

	query, args, err := result.ToSql()
	require.NoError(t, err)
	assert.Contains(t, query, "(test_table.created_at, test_table.id)")
	assert.Contains(t, query, ">")
	assert.Len(t, args, 2)
	assert.Equal(t, "2024-01-01T00:00:00Z", args[0])
	assert.Equal(t, "some-id", args[1])
}

func TestApplySortCursorPagination_BackwardCursor(t *testing.T) {
	t.Parallel()

	builder := squirrel.Select("id", "created_at").
		From("test_table").
		PlaceholderFormat(squirrel.Dollar)

	cursor := libHTTP.EncodeSortCursor("created_at", "2024-01-01T00:00:00Z", "some-id", false)

	result, dir, cursorDir, err := ApplySortCursorPagination(
		builder, cursor, "created_at", "ASC", "test_table", 10,
	)

	require.NoError(t, err)
	assert.Equal(t, libHTTP.CursorDirectionPrev, cursorDir)
	assert.Equal(t, "DESC", dir)

	query, args, err := result.ToSql()
	require.NoError(t, err)
	assert.Contains(t, query, "(test_table.created_at, test_table.id)")
	assert.Contains(t, query, "<")
	assert.Len(t, args, 2)
	assert.Equal(t, "2024-01-01T00:00:00Z", args[0])
	assert.Equal(t, "some-id", args[1])
}

func TestApplySortCursorPagination_InvalidCursor(t *testing.T) {
	t.Parallel()

	builder := squirrel.Select("id", "created_at").
		From("test_table").
		PlaceholderFormat(squirrel.Dollar)

	_, _, _, err := ApplySortCursorPagination(
		builder, "not-valid-base64!", "created_at", "ASC", "test_table", 10,
	)

	require.Error(t, err)
	require.ErrorIs(t, err, libHTTP.ErrInvalidCursor)
}

func TestApplySortCursorPagination_CursorSortColumnMismatch(t *testing.T) {
	t.Parallel()

	builder := squirrel.Select("id", "created_at").
		From("test_table").
		PlaceholderFormat(squirrel.Dollar)

	cursor := libHTTP.EncodeSortCursor("status", "ACTIVE", "some-id", true)

	_, _, _, err := ApplySortCursorPagination(
		builder, cursor, "created_at", "ASC", "test_table", 10,
	)

	require.Error(t, err)
	require.ErrorIs(t, err, libHTTP.ErrInvalidCursor)
	assert.Contains(t, err.Error(), "does not match")
}

func TestApplySortCursorPagination_DescOrder(t *testing.T) {
	t.Parallel()

	builder := squirrel.Select("id", "status").
		From("items").
		PlaceholderFormat(squirrel.Dollar)

	cursor := libHTTP.EncodeSortCursor("status", "ACTIVE", "item-id-1", true)

	result, dir, cursorDir, err := ApplySortCursorPagination(
		builder, cursor, "status", "DESC", "items", 20,
	)

	require.NoError(t, err)
	assert.Equal(t, libHTTP.CursorDirectionNext, cursorDir)
	assert.Equal(t, "DESC", dir)

	query, args, err := result.ToSql()
	require.NoError(t, err)
	assert.Contains(t, query, "(items.status, items.id)")
	assert.Contains(t, query, "<")
	assert.Len(t, args, 2)
	assert.Equal(t, "ACTIVE", args[0])
	assert.Equal(t, "item-id-1", args[1])
}

func TestApplySortCursorPagination_LimitApplied(t *testing.T) {
	t.Parallel()

	builder := squirrel.Select("id").
		From("test_table").
		PlaceholderFormat(squirrel.Dollar)

	result, _, _, err := ApplySortCursorPagination(
		builder, "", "created_at", "ASC", "test_table", 5,
	)

	require.NoError(t, err)

	query, _, err := result.ToSql()
	require.NoError(t, err)
	// LIMIT should be limit+1 = 6
	assert.Contains(t, query, "LIMIT 6")
}

func TestValidateIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid lowercase", "test_table", false},
		{"valid single letter", "t", false},
		{"valid underscore prefix", "_private", false},
		{"valid with digits", "table_2", false},
		{"valid all lowercase", "adjustments", false},
		{"empty string", "", true},
		{"contains uppercase", "TestTable", true},
		{"starts with digit", "2table", true},
		{"contains hyphen", "test-table", true},
		{"contains space", "test table", true},
		{"contains dot", "schema.table", true},
		{"contains semicolon", "table;drop", true},
		{"contains parenthesis", "table()", true},
		{"SQL injection attempt", "id; DROP TABLE users--", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateIdentifier(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, ErrInvalidIdentifier)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestApplySortCursorPagination_InvalidTableName(t *testing.T) {
	t.Parallel()

	builder := squirrel.Select("id").
		From("test_table").
		PlaceholderFormat(squirrel.Dollar)

	_, _, _, err := ApplySortCursorPagination(
		builder, "", "created_at", "ASC", "bad-table", 10,
	)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidIdentifier)
	assert.Contains(t, err.Error(), "table name")
}

func TestApplySortCursorPagination_InvalidSortColumn(t *testing.T) {
	t.Parallel()

	builder := squirrel.Select("id").
		From("test_table").
		PlaceholderFormat(squirrel.Dollar)

	_, _, _, err := ApplySortCursorPagination(
		builder, "", "bad-column", "ASC", "test_table", 10,
	)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidIdentifier)
	assert.Contains(t, err.Error(), "sort column")
}

func TestDecodeSortCursorWithDirection_ValidCursorNext(t *testing.T) {
	t.Parallel()

	cursor := libHTTP.EncodeSortCursor("created_at", "2024-01-01T00:00:00Z", "some-id", true)

	sc, direction, err := DecodeSortCursorWithDirection(cursor, "created_at")

	require.NoError(t, err)
	require.NotNil(t, sc)
	assert.Equal(t, libHTTP.CursorDirectionNext, direction)
	assert.Equal(t, "2024-01-01T00:00:00Z", sc.SortValue)
	assert.Equal(t, "some-id", sc.ID)
	assert.True(t, sc.PointsNext)
}

func TestDecodeSortCursorWithDirection_ValidCursorPrev(t *testing.T) {
	t.Parallel()

	cursor := libHTTP.EncodeSortCursor("created_at", "2024-01-01T00:00:00Z", "some-id", false)

	sc, direction, err := DecodeSortCursorWithDirection(cursor, "created_at")

	require.NoError(t, err)
	require.NotNil(t, sc)
	assert.Equal(t, libHTTP.CursorDirectionPrev, direction)
	assert.False(t, sc.PointsNext)
}

func TestDecodeSortCursorWithDirection_InvalidCursor(t *testing.T) {
	t.Parallel()

	_, _, err := DecodeSortCursorWithDirection("not-valid-base64!", "created_at")

	require.Error(t, err)
	require.ErrorIs(t, err, libHTTP.ErrInvalidCursor)
}

func TestDecodeSortCursorWithDirection_ColumnMismatch(t *testing.T) {
	t.Parallel()

	cursor := libHTTP.EncodeSortCursor("severity", "HIGH", "some-id", true)

	_, _, err := DecodeSortCursorWithDirection(cursor, "created_at")

	require.Error(t, err)
	require.ErrorIs(t, err, libHTTP.ErrInvalidCursor)
	assert.Contains(t, err.Error(), "does not match")
}

func TestSafeIntToUint64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    int
		expected uint64
	}{
		{"positive number", 10, 10},
		{"zero", 0, 0},
		{"negative returns zero", -1, 0},
		{"large negative returns zero", -1000, 0},
		{"large positive", 1000000, 1000000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := SafeIntToUint64(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
