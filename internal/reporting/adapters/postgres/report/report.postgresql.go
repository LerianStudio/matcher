// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package report

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Reporting uses higher pagination limits than standard API endpoints.
// This is intentional for bulk export operations that need to retrieve
// large datasets for reconciliation reports.
const (
	defaultLimit  = 100
	maxLimit      = 1000
	sortOrderAsc  = "ASC"
	sortOrderDesc = "DESC"

	matchGroupStatusConfirmed = "CONFIRMED"

	MaxExportRecords = 100000

	// varianceBaseQuery is the manual SQL template for variance page queries.
	// It is used exclusively by ListVariancePage (via applyVarianceCursor) because
	// composite ROW keyset pagination cannot be expressed with squirrel.
	// Other variance methods use buildVarianceSelectBuilder() instead.
	//
	// Fixed positional args: $1=context_id, $2=date_from, $3=date_to.
	// Dynamic args are appended by callers starting at $4.
	varianceBaseQuery = `
			SELECT
				t.source_id,
				fv.currency,
				fv.fee_schedule_id,
				COALESCE(MAX(fs.name), MAX(fv.fee_schedule_name_snapshot), '') AS fee_schedule_name,
				COALESCE(SUM(fv.expected_fee_amount), 0) AS total_expected,
				COALESCE(SUM(fv.actual_fee_amount), 0) AS total_actual,
				COALESCE(SUM(fv.delta), 0) AS net_variance
			FROM match_fee_variances fv
			JOIN transactions t ON t.id = fv.transaction_id
			LEFT JOIN fee_schedules fs ON fs.id = fv.fee_schedule_id
			WHERE fv.context_id = $1
			  AND fv.created_at >= $2 AND fv.created_at <= $3`

	varianceGroupByClause = " GROUP BY t.source_id, fv.currency, fv.fee_schedule_id"
	varianceOrderByClause = " ORDER BY t.source_id, fv.currency, fv.fee_schedule_id"

	cursorPartCount = 3
)

// currencyPattern validates ISO 4217 currency codes (3 uppercase letters).
var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

// safeLimitForPage converts a non-negative int to uint64 for squirrel's Limit method.
// Returns 0 for negative values.
func safeLimitForPage(n int) uint64 {
	if n <= 0 {
		return 0
	}

	return uint64(n)
}

func safeExportLimit(maxRecords int) uint64 {
	if maxRecords <= 0 {
		return 1
	}

	return uint64(maxRecords) + 1
}

// normalizeLimit clamps the page limit to [defaultLimit, maxLimit].
func normalizeLimit(limit int) int {
	if limit <= 0 {
		return defaultLimit
	}

	if limit > maxLimit {
		return maxLimit
	}

	return limit
}

type paginationArgs struct {
	orderDirection string
	limit          int
	cursor         libHTTP.Cursor
}

type matchedListResult struct {
	items      []*entities.MatchedItem
	pagination libHTTP.CursorPagination
}

type unmatchedListResult struct {
	items      []*entities.UnmatchedItem
	pagination libHTTP.CursorPagination
}

// Repository persists report data in Postgres.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new report repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

func (repo *Repository) validateFilter(filter *entities.ReportFilter) error {
	if repo == nil || repo.provider == nil {
		return ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return ErrContextIDRequired
	}

	if filter.Limit < 0 {
		return ErrLimitMustBePositive
	}

	if filter.Limit == 0 {
		filter.Limit = defaultLimit
	}

	if filter.Limit > maxLimit {
		return ErrLimitExceedsMaximum
	}

	return nil
}

// buildGenericPaginationArgs constructs pagination args from raw cursor, sortOrder, and limit values.
func buildGenericPaginationArgs(cursorStr, sortOrder string, limit int) (paginationArgs, error) {
	orderDirection := strings.ToUpper(sortOrder)
	if orderDirection != sortOrderAsc && orderDirection != sortOrderDesc {
		orderDirection = sortOrderAsc
	}

	if limit <= 0 {
		limit = defaultLimit
	}

	cursor := libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext}

	if cursorStr != "" {
		decodedCursor, err := libHTTP.DecodeCursor(cursorStr)
		if err != nil {
			return paginationArgs{}, fmt.Errorf("%w: %w", libHTTP.ErrInvalidCursor, err)
		}

		cursor = decodedCursor
	}

	return paginationArgs{
		orderDirection: orderDirection,
		limit:          limit,
		cursor:         cursor,
	}, nil
}

func buildPaginationArgs(filter entities.ReportFilter) (paginationArgs, error) {
	return buildGenericPaginationArgs(filter.Cursor, filter.SortOrder, filter.Limit)
}

func paginateReportItems[Item any](
	filter entities.ReportFilter,
	args paginationArgs,
	items []*Item,
	idFn func(*Item) string,
) ([]*Item, libHTTP.CursorPagination, error) {
	hasPagination := len(items) > args.limit
	isFirstPage := filter.Cursor == "" || (!hasPagination && args.cursor.Direction == libHTTP.CursorDirectionPrev)

	items = libHTTP.PaginateRecords(
		isFirstPage,
		hasPagination,
		args.cursor.Direction,
		items,
		args.limit,
	)
	if len(items) == 0 {
		return items, libHTTP.CursorPagination{}, nil
	}

	pagination, err := libHTTP.CalculateCursor(
		isFirstPage,
		hasPagination,
		args.cursor.Direction,
		idFn(items[0]),
		idFn(items[len(items)-1]),
	)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, fmt.Errorf("failed to calculate cursor: %w", err)
	}

	return items, pagination, nil
}

func scanMatchedItem(scanner interface{ Scan(dest ...any) error }) (*entities.MatchedItem, error) {
	var item entities.MatchedItem
	if err := scanner.Scan(
		&item.TransactionID,
		&item.MatchGroupID,
		&item.SourceID,
		&item.Amount,
		&item.Currency,
		&item.Date,
	); err != nil {
		return nil, fmt.Errorf("scanning matched item: %w", err)
	}

	return &item, nil
}

func scanUnmatchedItem(
	scanner interface{ Scan(dest ...any) error },
) (*entities.UnmatchedItem, error) {
	var item entities.UnmatchedItem
	if err := scanner.Scan(
		&item.TransactionID,
		&item.SourceID,
		&item.Amount,
		&item.Currency,
		&item.Status,
		&item.Date,
		&item.ExceptionID,
		&item.DueAt,
	); err != nil {
		return nil, fmt.Errorf("scanning unmatched item: %w", err)
	}

	return &item, nil
}
