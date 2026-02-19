// Package common provides shared utilities for postgres adapters.
package common

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/Masterminds/squirrel"

	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
)

// ErrInvalidIdentifier is returned when a SQL identifier fails validation.
var ErrInvalidIdentifier = errors.New("invalid SQL identifier")

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
