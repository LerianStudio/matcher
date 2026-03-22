package source

import (
	stdctx "context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	sharedpg "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

const (
	sourcePaginationOrderAsc  = "ASC"
	sourcePaginationOrderDesc = "DESC"
	sourcePaginationOpGreater = ">"
	sourcePaginationOpLess    = "<"
)

var (
	validSourcePaginationOrders = map[string]struct{}{
		sourcePaginationOrderAsc:  {},
		sourcePaginationOrderDesc: {},
	}
	validSourcePaginationOps = map[string]struct{}{
		sourcePaginationOpGreater: {},
		sourcePaginationOpLess:    {},
	}
)

// parseCursor decodes a cursor string into a Cursor struct for pagination.
// Returns a default cursor pointing forward if the input is empty.
func parseCursor(cursor string) (libHTTP.Cursor, error) {
	if cursor == "" {
		return libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext}, nil
	}

	parsedCursor, err := libHTTP.DecodeCursor(cursor)
	if err != nil {
		return libHTTP.Cursor{}, fmt.Errorf("%w: %w", libHTTP.ErrInvalidCursor, err)
	}

	return parsedCursor, nil
}

// buildPaginatedSourceQuery applies cursor direction and limit to the base query.
func buildPaginatedSourceQuery(
	baseQuery squirrel.SelectBuilder,
	decodedCursor libHTTP.Cursor,
	limit int,
) (squirrel.SelectBuilder, error) {
	orderDirection, err := validateSourcePaginationOrder(libHTTP.ValidateSortDirection(sourcePaginationOrderAsc))
	if err != nil {
		return baseQuery, fmt.Errorf("base pagination order: %w", err)
	}

	operator, effectiveOrder, dirErr := libHTTP.CursorDirectionRules(orderDirection, decodedCursor.Direction)
	if dirErr != nil {
		return baseQuery, fmt.Errorf("cursor direction rules: %w", dirErr)
	}

	validatedOperator, err := validateSourcePaginationOperator(operator)
	if err != nil {
		return baseQuery, fmt.Errorf("pagination operator: %w", err)
	}

	validatedOrder, err := validateSourcePaginationOrder(effectiveOrder)
	if err != nil {
		return baseQuery, fmt.Errorf("pagination order: %w", err)
	}

	baseQuery, err = applySourceCursorFilter(baseQuery, validatedOperator, decodedCursor.ID)
	if err != nil {
		return baseQuery, err
	}

	return baseQuery.
		OrderBy("id " + validatedOrder).
		Limit(sharedpg.SafeIntToUint64(limit) + 1), nil
}

// executeSourceRows runs the query and scans all source rows.
func executeSourceRows(
	ctx stdctx.Context,
	tx *sql.Tx,
	paginatedQuery squirrel.SelectBuilder,
	limit int,
) ([]*entities.ReconciliationSource, error) {
	query, args, err := paginatedQuery.ToSql()
	if err != nil {
		return nil, fmt.Errorf("build list sources query: %w", err)
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sources := make([]*entities.ReconciliationSource, 0, limit)

	for rows.Next() {
		sourceEntity, scanErr := scanSource(rows)
		if scanErr != nil {
			return nil, scanErr
		}

		sources = append(sources, sourceEntity)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return sources, nil
}

// applySourcePagination processes results to calculate cursor pagination.
func applySourcePagination(
	sources []*entities.ReconciliationSource,
	cursor string,
	decodedCursor libHTTP.Cursor,
	limit int,
	pagination *libHTTP.CursorPagination,
) ([]*entities.ReconciliationSource, error) {
	hasPagination := len(sources) > limit
	isFirstPage := cursor == "" || (!hasPagination && decodedCursor.Direction == libHTTP.CursorDirectionPrev)

	sources = libHTTP.PaginateRecords(
		isFirstPage,
		hasPagination,
		decodedCursor.Direction,
		sources,
		limit,
	)

	if len(sources) == 0 {
		return sources, nil
	}

	page, err := libHTTP.CalculateCursor(
		isFirstPage,
		hasPagination,
		decodedCursor.Direction,
		sources[0].ID.String(),
		sources[len(sources)-1].ID.String(),
	)
	if err != nil {
		return nil, fmt.Errorf("calculate cursor: %w", err)
	}

	*pagination = page

	return sources, nil
}

func scanSource(scanner interface{ Scan(dest ...any) error }) (*entities.ReconciliationSource, error) {
	var model SourcePostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.ContextID,
		&model.Name,
		&model.Type,
		&model.Side,
		&model.Config,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, err
	}

	return model.ToEntity()
}

func applySourceCursorFilter(
	baseQuery squirrel.SelectBuilder,
	operator string,
	cursorID string,
) (squirrel.SelectBuilder, error) {
	if strings.TrimSpace(cursorID) == "" {
		return baseQuery, nil
	}

	switch operator {
	case sourcePaginationOpGreater:
		return baseQuery.Where(squirrel.Gt{"id": cursorID}), nil
	case sourcePaginationOpLess:
		return baseQuery.Where(squirrel.Lt{"id": cursorID}), nil
	default:
		return baseQuery, fmt.Errorf("%w: %q", ErrInvalidPaginationOp, operator)
	}
}

func validateSourcePaginationOrder(order string) (string, error) {
	normalized := strings.ToUpper(strings.TrimSpace(order))
	if _, ok := validSourcePaginationOrders[normalized]; !ok {
		return "", fmt.Errorf("%w: %q", ErrInvalidPaginationOrder, order)
	}

	return normalized, nil
}

func validateSourcePaginationOperator(operator string) (string, error) {
	normalized := strings.TrimSpace(operator)
	if _, ok := validSourcePaginationOps[normalized]; !ok {
		return "", fmt.Errorf("%w: %q", ErrInvalidPaginationOp, operator)
	}

	return normalized, nil
}
