// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package match_rule

import (
	stdctx "context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
)

func scanMatchRule(scanner interface{ Scan(dest ...any) error }) (*entities.MatchRule, error) {
	var model MatchRulePostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.ContextID,
		&model.Priority,
		&model.Type,
		&model.Config,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, err
	}

	return model.ToEntity()
}

func fetchCursorPriority(ctx stdctx.Context, tx *sql.Tx, cursor, contextID uuid.UUID) (int, error) {
	var cursorPriority int

	cursorQuery := "SELECT priority FROM match_rules WHERE id = $1 AND context_id = $2"

	if err := tx.QueryRowContext(ctx, cursorQuery, cursor.String(), contextID.String()).Scan(&cursorPriority); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrCursorNotFound
		}

		return 0, fmt.Errorf("validating cursor: %w", err)
	}

	return cursorPriority, nil
}

func executeMatchRulesQuery(
	ctx stdctx.Context,
	tx *sql.Tx,
	query string,
	args []any,
) (rules entities.MatchRules, err error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	const defaultMatchRulesCapacity = 32

	rules = make(entities.MatchRules, 0, defaultMatchRulesCapacity)

	for rows.Next() {
		rule, err := scanMatchRule(rows)
		if err != nil {
			return nil, err
		}

		rules = append(rules, rule)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return rules, nil
}

// parseCursor decodes and parses a cursor string into its components.
func parseCursor(cursor string) (libHTTP.Cursor, uuid.UUID, error) {
	decodedCursor := libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext}

	var cursorID uuid.UUID

	if cursor == "" {
		return decodedCursor, cursorID, nil
	}

	parsedCursor, err := libHTTP.DecodeCursor(cursor)
	if err != nil {
		return libHTTP.Cursor{}, uuid.Nil, fmt.Errorf("%w: %w", libHTTP.ErrInvalidCursor, err)
	}

	decodedCursor = parsedCursor

	parsedID, err := uuid.Parse(decodedCursor.ID)
	if err != nil {
		return libHTTP.Cursor{}, uuid.Nil, fmt.Errorf("%w: %w", libHTTP.ErrInvalidCursor, err)
	}

	return decodedCursor, parsedID, nil
}

// buildCursorConditions validates cursor data and returns filter conditions and ordering.
func buildCursorConditions(
	ctx stdctx.Context,
	tx *sql.Tx,
	decodedCursor libHTTP.Cursor,
	cursorID uuid.UUID,
	contextID uuid.UUID,
) (squirrel.Sqlizer, string, error) {
	cursorPriority, err := fetchCursorPriority(ctx, tx, cursorID, contextID)
	if err != nil {
		if errors.Is(err, ErrCursorNotFound) {
			return nil, "", fmt.Errorf("%w: %w", libHTTP.ErrInvalidCursor, err)
		}

		return nil, "", err
	}

	if decodedCursor.Direction == libHTTP.CursorDirectionNext {
		return squirrel.Or{
			squirrel.Gt{"priority": cursorPriority},
			squirrel.And{
				squirrel.Eq{"priority": cursorPriority},
				squirrel.Gt{"id": cursorID.String()},
			},
		}, "ASC", nil
	}

	return squirrel.Or{
		squirrel.Lt{"priority": cursorPriority},
		squirrel.And{
			squirrel.Eq{"priority": cursorPriority},
			squirrel.Lt{"id": cursorID.String()},
		},
	}, "DESC", nil
}

// safeUint64 safely converts an int to uint64, returning 0 for negative values.
func safeUint64(n int) uint64 {
	if n < 0 {
		return 0
	}

	return uint64(n)
}

// paginateAndCalculateCursor handles pagination logic and cursor calculation for match rules.
func paginateAndCalculateCursor(
	cursor string,
	decodedCursor libHTTP.Cursor,
	rules entities.MatchRules,
	limit int,
) (entities.MatchRules, libHTTP.CursorPagination, error) {
	var pagination libHTTP.CursorPagination

	hasPagination := len(rules) > limit
	isFirstPage := cursor == "" || (!hasPagination && decodedCursor.Direction == libHTTP.CursorDirectionPrev)

	rules = libHTTP.PaginateRecords(
		isFirstPage,
		hasPagination,
		decodedCursor.Direction,
		rules,
		limit,
	)

	if len(rules) > 0 {
		page, err := libHTTP.CalculateCursor(
			isFirstPage,
			hasPagination,
			decodedCursor.Direction,
			rules[0].ID.String(),
			rules[len(rules)-1].ID.String(),
		)
		if err != nil {
			return nil, libHTTP.CursorPagination{}, fmt.Errorf("calculate cursor: %w", err)
		}

		pagination = page
	}

	return rules, pagination, nil
}
