// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package common

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
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

	cursor, err := libHTTP.EncodeSortCursor("created_at", "2024-01-01T00:00:00Z", "some-id", true)
	require.NoError(t, err)

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

	cursor, err := libHTTP.EncodeSortCursor("created_at", "2024-01-01T00:00:00Z", "some-id", false)
	require.NoError(t, err)

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

	cursor, err := libHTTP.EncodeSortCursor("status", "ACTIVE", "some-id", true)
	require.NoError(t, err)

	_, _, _, err = ApplySortCursorPagination(
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

	cursor, err := libHTTP.EncodeSortCursor("status", "ACTIVE", "item-id-1", true)
	require.NoError(t, err)

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

	cursor, err := libHTTP.EncodeSortCursor("created_at", "2024-01-01T00:00:00Z", "some-id", true)
	require.NoError(t, err)

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

	cursor, err := libHTTP.EncodeSortCursor("created_at", "2024-01-01T00:00:00Z", "some-id", false)
	require.NoError(t, err)

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

	cursor, err := libHTTP.EncodeSortCursor("severity", "HIGH", "some-id", true)
	require.NoError(t, err)

	_, _, err = DecodeSortCursorWithDirection(cursor, "created_at")

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

func TestCalculateSortCursorPagination_NilCalculator(t *testing.T) {
	t.Parallel()

	_, err := CalculateSortCursorPagination(
		true,
		true,
		true,
		"created_at",
		"2026-01-01T00:00:00Z",
		"first-id",
		"2026-01-02T00:00:00Z",
		"last-id",
		nil,
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSortCursorCalculatorRequired)
}

func TestCalculateSortCursorPagination_PropagatesCalculatorError(t *testing.T) {
	t.Parallel()

	_, err := CalculateSortCursorPagination(
		true,
		true,
		true,
		"created_at",
		"2026-01-01T00:00:00Z",
		"first-id",
		"2026-01-02T00:00:00Z",
		"last-id",
		func(
			_ bool,
			_ bool,
			_ bool,
			_ string,
			_ string,
			_ string,
			_ string,
			_ string,
		) (string, string, error) {
			return "", "", ErrInvalidIdentifier
		},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidIdentifier)
}

func TestCalculateSortCursorPagination_Success(t *testing.T) {
	t.Parallel()

	pagination, err := CalculateSortCursorPagination(
		true,
		true,
		true,
		"created_at",
		"2026-01-01T00:00:00Z",
		"first-id",
		"2026-01-02T00:00:00Z",
		"last-id",
		func(
			_ bool,
			_ bool,
			_ bool,
			_ string,
			_ string,
			_ string,
			_ string,
			_ string,
		) (string, string, error) {
			return "next-cursor", "prev-cursor", nil
		},
	)

	require.NoError(t, err)
	assert.Equal(t, "next-cursor", pagination.Next)
	assert.Equal(t, "prev-cursor", pagination.Prev)
}

func TestCalculateSortCursorPaginationWrapped_PropagatesErrorWithContext(t *testing.T) {
	t.Parallel()

	_, err := CalculateSortCursorPaginationWrapped(
		true,
		true,
		true,
		"created_at",
		"2026-01-01T00:00:00Z",
		"first-id",
		"2026-01-02T00:00:00Z",
		"last-id",
		nil,
		"calculate domain pagination",
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSortCursorCalculatorRequired)
	assert.Contains(t, err.Error(), "calculate domain pagination")
}

func TestTrimRecordsAndEncodeTimestampNextCursor_NoTrimWhenWithinLimit(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	items := []libHTTP.TimestampCursor{
		{Timestamp: now, ID: uuid.New()},
	}

	result, cursor, err := TrimRecordsAndEncodeTimestampNextCursor(
		items,
		5,
		func(item libHTTP.TimestampCursor) (time.Time, uuid.UUID) {
			return item.Timestamp, item.ID
		},
		libHTTP.EncodeTimestampCursor,
	)

	require.NoError(t, err)
	assert.Equal(t, items, result)
	assert.Empty(t, cursor)
}

func TestTrimRecordsAndEncodeTimestampNextCursor_Success(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	item1 := libHTTP.TimestampCursor{Timestamp: now, ID: uuid.New()}
	item2 := libHTTP.TimestampCursor{Timestamp: now.Add(-time.Minute), ID: uuid.New()}
	item3 := libHTTP.TimestampCursor{Timestamp: now.Add(-2 * time.Minute), ID: uuid.New()}

	result, cursor, err := TrimRecordsAndEncodeTimestampNextCursor(
		[]libHTTP.TimestampCursor{item1, item2, item3},
		2,
		func(item libHTTP.TimestampCursor) (time.Time, uuid.UUID) {
			return item.Timestamp, item.ID
		},
		libHTTP.EncodeTimestampCursor,
	)

	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, item1, result[0])
	assert.Equal(t, item2, result[1])
	assert.NotEmpty(t, cursor)
}

func TestTrimRecordsAndEncodeTimestampNextCursor_EncoderFailure(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	item1 := libHTTP.TimestampCursor{Timestamp: now, ID: uuid.New()}
	item2 := libHTTP.TimestampCursor{Timestamp: now.Add(-time.Minute), ID: uuid.New()}

	result, cursor, err := TrimRecordsAndEncodeTimestampNextCursor(
		[]libHTTP.TimestampCursor{item1, item2},
		1,
		func(item libHTTP.TimestampCursor) (time.Time, uuid.UUID) {
			return item.Timestamp, item.ID
		},
		func(_ time.Time, _ uuid.UUID) (string, error) {
			return "", ErrCursorEncoderRequired
		},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCursorEncoderRequired)
	assert.Len(t, result, 1)
	assert.Empty(t, cursor)
}

func TestValidateSortCursorBoundaries(t *testing.T) {
	t.Parallel()

	t.Run("nil boundary returns error", func(t *testing.T) {
		t.Parallel()

		type cursorRecord struct {
			id string
		}

		first := &cursorRecord{id: "a"}
		var last *cursorRecord

		err := ValidateSortCursorBoundaries(first, last)

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSortCursorBoundaryRecordNil)
	})

	t.Run("both boundaries present", func(t *testing.T) {
		t.Parallel()

		type cursorRecord struct {
			id string
		}

		first := &cursorRecord{id: "a"}
		last := &cursorRecord{id: "b"}

		err := ValidateSortCursorBoundaries(first, last)

		require.NoError(t, err)
	})
}

func TestTrimRecordsAndEncodeTimestampNextCursor_NilExtractor(t *testing.T) {
	t.Parallel()

	items := []libHTTP.TimestampCursor{
		{Timestamp: time.Now().UTC(), ID: uuid.New()},
		{Timestamp: time.Now().UTC().Add(-time.Minute), ID: uuid.New()},
	}

	result, cursor, err := TrimRecordsAndEncodeTimestampNextCursor(
		items,
		1,
		nil,
		libHTTP.EncodeTimestampCursor,
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCursorRecordExtractorRequired)
	assert.Len(t, result, 1)
	assert.Empty(t, cursor)
}

func TestTrimRecordsAndEncodeTimestampNextCursor_NilEncoder(t *testing.T) {
	t.Parallel()

	items := []libHTTP.TimestampCursor{
		{Timestamp: time.Now().UTC(), ID: uuid.New()},
		{Timestamp: time.Now().UTC().Add(-time.Minute), ID: uuid.New()},
	}

	result, cursor, err := TrimRecordsAndEncodeTimestampNextCursor(
		items,
		1,
		func(item libHTTP.TimestampCursor) (time.Time, uuid.UUID) {
			return item.Timestamp, item.ID
		},
		nil,
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCursorEncoderRequired)
	assert.Len(t, result, 1)
	assert.Empty(t, cursor)
}
