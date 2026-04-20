// Package common provides shared PostgreSQL utilities for the ingestion context.
package common

import (
	"fmt"

	"github.com/Masterminds/squirrel"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	sharedpg "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// ApplyCursorPagination applies either ID-based or sort-column-based cursor pagination
// to the given query builder. It delegates to the appropriate shared pagination strategy
// based on the useIDCursor flag, returning the modified query, resolved direction, and cursor direction.
func ApplyCursorPagination(
	findAll squirrel.SelectBuilder,
	cursorStr, sortColumn, orderDirection string,
	limit int,
	useIDCursor bool,
	tableName string,
) (squirrel.SelectBuilder, string, string, error) {
	var (
		result    squirrel.SelectBuilder
		dir       string
		cursorDir string
		cursorErr error
	)

	if useIDCursor {
		result, dir, cursorDir, cursorErr = sharedpg.ApplyIDCursorPagination(findAll, cursorStr, orderDirection, limit)
	} else {
		result, dir, cursorDir, cursorErr = sharedpg.ApplySortCursorPagination(findAll, cursorStr, sortColumn, orderDirection, tableName, limit)
	}

	if cursorErr != nil {
		return findAll, orderDirection, libHTTP.CursorDirectionNext, fmt.Errorf("apply cursor pagination: %w", cursorErr)
	}

	return result, dir, cursorDir, nil
}
