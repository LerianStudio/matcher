// Package common provides shared utilities for postgres adapters.
package common

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
)

// ErrInvalidIdentifier is returned when a SQL identifier fails validation.
var (
	ErrInvalidIdentifier = errors.New("invalid SQL identifier")
	// ErrSortCursorCalculatorRequired is returned when the sort cursor callback is missing.
	ErrSortCursorCalculatorRequired = errors.New("sort cursor calculator is required")
	// ErrSortCursorBoundaryRecordNil is returned when first/last pagination records are nil.
	ErrSortCursorBoundaryRecordNil = errors.New("sort cursor boundary record is nil")
	// ErrCursorRecordExtractorRequired is returned when cursor field extractor callback is missing.
	ErrCursorRecordExtractorRequired = errors.New("cursor record extractor is required")
	// ErrCursorEncoderRequired is returned when cursor encoder callback is missing.
	ErrCursorEncoderRequired = errors.New("cursor encoder is required")
)

// TimestampCursorExtractor extracts timestamp and ID fields from a record for
// timestamp cursor generation.
type TimestampCursorExtractor[T any] func(record T) (time.Time, uuid.UUID)

// TimestampCursorEncoder encodes timestamp/ID cursor values.
type TimestampCursorEncoder func(time.Time, uuid.UUID) (string, error)

// TrimRecordsAndEncodeTimestampNextCursor applies the common "limit+1" pagination
// contract: trims records to limit and encodes the next cursor from the last
// returned record. When limit is non-positive or there are not enough records,
// it returns the input records and an empty cursor.
func TrimRecordsAndEncodeTimestampNextCursor[T any](
	records []T,
	limit int,
	extractCursor TimestampCursorExtractor[T],
	encodeCursor TimestampCursorEncoder,
) ([]T, string, error) {
	if limit <= 0 || len(records) <= limit {
		return records, "", nil
	}

	trimmedRecords := records[:limit]

	if extractCursor == nil {
		return trimmedRecords, "", fmt.Errorf("extract next cursor fields: %w", ErrCursorRecordExtractorRequired)
	}

	if encodeCursor == nil {
		return trimmedRecords, "", fmt.Errorf("encode next cursor: %w", ErrCursorEncoderRequired)
	}

	cursorTimestamp, cursorID := extractCursor(trimmedRecords[len(trimmedRecords)-1])

	nextCursor, err := encodeCursor(cursorTimestamp, cursorID)
	if err != nil {
		return trimmedRecords, "", fmt.Errorf("encode next cursor: %w", err)
	}

	return trimmedRecords, nextCursor, nil
}

// SortCursorCalculator computes next/prev cursor values for sort-based pagination.
type SortCursorCalculator func(
	isFirstPage bool,
	hasPagination bool,
	pointsNext bool,
	sortColumn string,
	firstSortValue string,
	firstID string,
	lastSortValue string,
	lastID string,
) (string, string, error)

// CalculateSortCursorPagination computes sort-based cursor pagination metadata.
func CalculateSortCursorPagination(
	isFirstPage, hasPagination, pointsNext bool,
	sortColumn,
	firstSortValue,
	firstID,
	lastSortValue,
	lastID string,
	calculateSortCursor SortCursorCalculator,
) (libHTTP.CursorPagination, error) {
	if calculateSortCursor == nil {
		return libHTTP.CursorPagination{}, ErrSortCursorCalculatorRequired
	}

	next, prev, err := calculateSortCursor(
		isFirstPage,
		hasPagination,
		pointsNext,
		sortColumn,
		firstSortValue,
		firstID,
		lastSortValue,
		lastID,
	)
	if err != nil {
		return libHTTP.CursorPagination{}, err
	}

	return libHTTP.CursorPagination{Next: next, Prev: prev}, nil
}

// ValidateSortCursorBoundaries ensures the first and last records used for
// sort cursor pagination are non-nil.
func ValidateSortCursorBoundaries[T any](firstRecord, lastRecord *T) error {
	if firstRecord == nil || lastRecord == nil {
		return ErrSortCursorBoundaryRecordNil
	}

	return nil
}

// CalculateSortCursorPaginationWrapped computes sort cursor pagination and wraps
// any underlying error with a stable operation context.
func CalculateSortCursorPaginationWrapped(
	isFirstPage, hasPagination, pointsNext bool,
	sortColumn,
	firstSortValue,
	firstID,
	lastSortValue,
	lastID string,
	calculateSortCursor SortCursorCalculator,
	operation string,
) (libHTTP.CursorPagination, error) {
	pagination, err := CalculateSortCursorPagination(
		isFirstPage,
		hasPagination,
		pointsNext,
		sortColumn,
		firstSortValue,
		firstID,
		lastSortValue,
		lastID,
		calculateSortCursor,
	)
	if err != nil {
		if operation == "" {
			operation = "calculate sort cursor pagination"
		}

		return libHTTP.CursorPagination{}, fmt.Errorf("%s: %w", operation, err)
	}

	return pagination, nil
}

// validIdentifier matches safe PostgreSQL unquoted identifiers: lowercase letters
// or underscore start, followed by lowercase letters, digits, or underscores.
var validIdentifier = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// validateIdentifier checks that name is a safe PostgreSQL identifier to prevent
// SQL injection when interpolating table or column names into queries.
func validateIdentifier(name string) error {
	if !validIdentifier.MatchString(name) {
		return fmt.Errorf("%w: %q", ErrInvalidIdentifier, name)
	}

	return nil
}

// ApplyIDCursorPagination decodes an ID-based cursor and applies it to a squirrel SelectBuilder.
// If the cursor string is empty, it defaults to a forward-pointing first page.
// Returns the modified builder, effective order direction, cursor direction string, and any error.
func ApplyIDCursorPagination(
	findAll squirrel.SelectBuilder,
	cursorStr, orderDirection string,
	limit int,
) (squirrel.SelectBuilder, string, string, error) {
	decodedCursor := libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext}

	if cursorStr != "" {
		cursor, err := libHTTP.DecodeCursor(cursorStr)
		if err != nil {
			return findAll, orderDirection, libHTTP.CursorDirectionNext, fmt.Errorf("%w: %w", libHTTP.ErrInvalidCursor, err)
		}

		decodedCursor = cursor
	}

	operator, effectiveOrder, err := libHTTP.CursorDirectionRules(orderDirection, decodedCursor.Direction)
	if err != nil {
		return findAll, orderDirection, decodedCursor.Direction, fmt.Errorf("cursor direction rules: %w", err)
	}

	if decodedCursor.ID != "" {
		findAll = findAll.Where(squirrel.Expr("id "+operator+" ?", decodedCursor.ID))
	}

	findAll = findAll.
		OrderBy("id " + effectiveOrder).
		Limit(SafeIntToUint64(limit + 1))

	return findAll, effectiveOrder, decodedCursor.Direction, nil
}

// ApplySortCursorPagination decodes a sort-column cursor and applies it to a squirrel SelectBuilder.
//
// SECURITY NOTE: The tableName and sortColumn parameters are interpolated into SQL.
// Callers MUST use hard-coded table/column names (never user input) to prevent SQL injection.
// Both values are validated at runtime against a safe identifier pattern as defense-in-depth.
// Returns the modified builder, effective order direction, cursor direction string, and any error.
func ApplySortCursorPagination(
	findAll squirrel.SelectBuilder,
	cursorStr, sortColumn, orderDirection, tableName string,
	limit int,
) (squirrel.SelectBuilder, string, string, error) {
	if err := validateIdentifier(tableName); err != nil {
		return findAll, orderDirection, libHTTP.CursorDirectionNext, fmt.Errorf("table name: %w", err)
	}

	if err := validateIdentifier(sortColumn); err != nil {
		return findAll, orderDirection, libHTTP.CursorDirectionNext, fmt.Errorf("sort column: %w", err)
	}

	cursorDirection := libHTTP.CursorDirectionNext

	if cursorStr != "" {
		sc, decodedDirection, err := DecodeSortCursorWithDirection(cursorStr, sortColumn)
		if err != nil {
			return findAll, orderDirection, libHTTP.CursorDirectionNext, err
		}

		cursorDirection = decodedDirection

		var op string

		orderDirection, op = libHTTP.SortCursorDirection(orderDirection, sc.PointsNext)
		findAll = findAll.Where(squirrel.Expr(
			"("+tableName+"."+sortColumn+", "+tableName+".id) "+op+" (?, ?)",
			sc.SortValue, sc.ID,
		))
	}

	findAll = findAll.
		OrderBy(tableName+"."+sortColumn+" "+orderDirection, tableName+".id "+orderDirection).
		Limit(SafeIntToUint64(limit + 1))

	return findAll, orderDirection, cursorDirection, nil
}

// DecodeSortCursorWithDirection decodes a sort cursor and validates that its sort column
// matches the expected column, returning the decoded cursor, its direction string, or an error.
// This is useful when cursor decoding must happen separately from query building
// (e.g., when the decoded cursor influences control flow before the query is constructed).
func DecodeSortCursorWithDirection(cursorStr, expectedSortColumn string) (*libHTTP.SortCursor, string, error) {
	sc, err := libHTTP.DecodeSortCursor(cursorStr)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %w", libHTTP.ErrInvalidCursor, err)
	}

	if sc.SortColumn != expectedSortColumn {
		return nil, "", fmt.Errorf(
			"%w: cursor sort column %q does not match requested sort column %q",
			libHTTP.ErrInvalidCursor, sc.SortColumn, expectedSortColumn,
		)
	}

	cursorDirection := libHTTP.CursorDirectionPrev
	if sc.PointsNext {
		cursorDirection = libHTTP.CursorDirectionNext
	}

	return sc, cursorDirection, nil
}

// SafeIntToUint64 converts an int to uint64, returning 0 for negative values.
// Used for safe conversion when setting SQL LIMIT/OFFSET.
func SafeIntToUint64(n int) uint64 {
	if n < 0 {
		return 0
	}

	return uint64(n)
}
