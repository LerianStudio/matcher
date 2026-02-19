package report

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
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
				r.structure_type AS fee_type,
				COALESCE(SUM(fv.expected_fee_amount), 0) AS total_expected,
				COALESCE(SUM(fv.actual_fee_amount), 0) AS total_actual,
				COALESCE(SUM(fv.delta), 0) AS net_variance
			FROM match_fee_variances fv
			JOIN transactions t ON t.id = fv.transaction_id
			JOIN rates r ON r.id = fv.rate_id
			WHERE fv.context_id = $1
			  AND fv.created_at >= $2 AND fv.created_at <= $3`

	varianceGroupByClause = " GROUP BY t.source_id, fv.currency, r.structure_type"
	varianceOrderByClause = " ORDER BY t.source_id, fv.currency, r.structure_type"

	cursorPartCount = 3
)

// currencyPattern validates ISO 4217 currency codes (3 uppercase letters).
var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

// validFeeStructureTypes is the set of recognized fee structure types for
// variance cursor validation.
var validFeeStructureTypes = map[string]bool{
	"FLAT":       true,
	"PERCENTAGE": true,
	"TIERED":     true,
}

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

// ListMatched retrieves matched transactions based on filter criteria.
func (repo *Repository) ListMatched(
	ctx context.Context,
	filter entities.ReportFilter,
) ([]*entities.MatchedItem, libHTTP.CursorPagination, error) {
	if err := repo.validateFilter(&filter); err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_matched")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (matchedListResult, error) {
			return repo.listMatchedQuery(ctx, qe, filter)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list matched transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list matched items", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list matched items: %v", wrappedErr))

		return nil, libHTTP.CursorPagination{}, wrappedErr
	}

	return result.items, result.pagination, nil
}

// ListUnmatched retrieves unmatched transactions based on filter criteria.
func (repo *Repository) ListUnmatched(
	ctx context.Context,
	filter entities.ReportFilter,
) ([]*entities.UnmatchedItem, libHTTP.CursorPagination, error) {
	if err := repo.validateFilter(&filter); err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_unmatched")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (unmatchedListResult, error) {
			return repo.listUnmatchedQuery(ctx, qe, filter)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list unmatched transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list unmatched items", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list unmatched items: %v", wrappedErr))

		return nil, libHTTP.CursorPagination{}, wrappedErr
	}

	return result.items, result.pagination, nil
}

func (repo *Repository) listMatchedQuery(
	ctx context.Context,
	qe pgcommon.QueryExecutor,
	filter entities.ReportFilter,
) (matchedListResult, error) {
	args, err := buildPaginationArgs(filter)
	if err != nil {
		return matchedListResult{}, err
	}

	findAll := squirrel.Select(
		"t.id",
		"mg.id",
		"t.source_id",
		"mi.allocated_amount",
		"mi.allocated_currency",
		"t.date",
	).
		From("match_items mi").
		Join("match_groups mg ON mi.match_group_id = mg.id").
		Join("transactions t ON mi.transaction_id = t.id").
		Where(squirrel.Eq{"mg.context_id": filter.ContextID, "mg.status": matchGroupStatusConfirmed}).
		Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
		Where(squirrel.Expr("t.date <= ?", filter.DateTo)).
		PlaceholderFormat(squirrel.Dollar)

	if filter.SourceID != nil {
		findAll = findAll.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
	}

	operator, effectiveOrder, dirErr := libHTTP.CursorDirectionRules(args.orderDirection, args.cursor.Direction)
	if dirErr != nil {
		return matchedListResult{}, fmt.Errorf("cursor direction rules: %w", dirErr)
	}

	if args.cursor.ID != "" {
		findAll = findAll.Where(squirrel.Expr("t.id "+operator+" ?", args.cursor.ID))
	}

	findAll = findAll.
		OrderBy("t.id " + effectiveOrder).
		Limit(uint64(args.limit + 1)) //nolint:gosec //#nosec G115 -- limit is validated positive by buildPaginationArgs

	query, queryArgs, err := findAll.ToSql()
	if err != nil {
		return matchedListResult{}, fmt.Errorf("building matched query: %w", err)
	}

	rows, err := qe.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return matchedListResult{}, fmt.Errorf("querying matched items: %w", err)
	}

	defer rows.Close()

	items := make([]*entities.MatchedItem, 0, args.limit+1)

	for rows.Next() {
		item, err := scanMatchedItem(rows)
		if err != nil {
			return matchedListResult{}, fmt.Errorf("scanning matched item: %w", err)
		}

		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return matchedListResult{}, fmt.Errorf("iterating matched items: %w", err)
	}

	paginatedItems, pagination, err := paginateReportItems(
		filter,
		args,
		items,
		func(item *entities.MatchedItem) string { return item.TransactionID.String() },
	)
	if err != nil {
		return matchedListResult{}, err
	}

	return matchedListResult{items: paginatedItems, pagination: pagination}, nil
}

func (repo *Repository) listUnmatchedQuery(
	ctx context.Context,
	qe pgcommon.QueryExecutor,
	filter entities.ReportFilter,
) (unmatchedListResult, error) {
	args, err := buildPaginationArgs(filter)
	if err != nil {
		return unmatchedListResult{}, err
	}

	findAll := squirrel.Select(
		"t.id",
		"t.source_id",
		"t.amount",
		"t.currency",
		"t.status",
		"t.date",
		"e.id",
		"e.due_at",
	).
		From("transactions t").
		Join("reconciliation_sources rs ON t.source_id = rs.id").
		LeftJoin("exceptions e ON e.transaction_id = t.id").
		Where(squirrel.Eq{"rs.context_id": filter.ContextID}).
		Where(squirrel.NotEq{"t.status": "MATCHED"}).
		Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
		Where(squirrel.Expr("t.date <= ?", filter.DateTo)).
		PlaceholderFormat(squirrel.Dollar)

	if filter.SourceID != nil {
		findAll = findAll.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
	}

	if filter.Status != nil {
		findAll = findAll.Where(squirrel.Eq{"t.status": *filter.Status})
	}

	operator, effectiveOrder, dirErr := libHTTP.CursorDirectionRules(args.orderDirection, args.cursor.Direction)
	if dirErr != nil {
		return unmatchedListResult{}, fmt.Errorf("cursor direction rules: %w", dirErr)
	}

	if args.cursor.ID != "" {
		findAll = findAll.Where(squirrel.Expr("t.id "+operator+" ?", args.cursor.ID))
	}

	findAll = findAll.
		OrderBy("t.id " + effectiveOrder).
		Limit(uint64(args.limit + 1)) //nolint:gosec //#nosec G115 -- limit is validated positive by buildPaginationArgs

	query, queryArgs, err := findAll.ToSql()
	if err != nil {
		return unmatchedListResult{}, fmt.Errorf("building unmatched query: %w", err)
	}

	rows, err := qe.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return unmatchedListResult{}, fmt.Errorf("querying unmatched items: %w", err)
	}

	defer rows.Close()

	items := make([]*entities.UnmatchedItem, 0, args.limit+1)

	for rows.Next() {
		item, err := scanUnmatchedItem(rows)
		if err != nil {
			return unmatchedListResult{}, fmt.Errorf("scanning unmatched item: %w", err)
		}

		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return unmatchedListResult{}, fmt.Errorf("iterating unmatched items: %w", err)
	}

	paginatedItems, pagination, err := paginateReportItems(
		filter,
		args,
		items,
		func(item *entities.UnmatchedItem) string { return item.TransactionID.String() },
	)
	if err != nil {
		return unmatchedListResult{}, err
	}

	return unmatchedListResult{items: paginatedItems, pagination: pagination}, nil
}

// GetSummary retrieves aggregated summary statistics.
func (repo *Repository) GetSummary(
	ctx context.Context,
	filter entities.ReportFilter,
) (*entities.SummaryReport, error) {
	if err := repo.validateFilter(&filter); err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_summary")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*entities.SummaryReport, error) {
			// Matched summary: COUNT/SUM over distinct transactions in confirmed
			// match groups. Uses FromSelect to wrap the inner DISTINCT query as a
			// subquery -- squirrel handles positional arg indexing automatically.
			innerMatched := squirrel.Select("t.id", "t.amount").Distinct().
				From("match_items mi").
				Join("match_groups mg ON mi.match_group_id = mg.id").
				Join("transactions t ON mi.transaction_id = t.id").
				Where(squirrel.Eq{
					"mg.context_id": filter.ContextID,
					"mg.status":     matchGroupStatusConfirmed,
				}).
				Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
				Where(squirrel.Expr("t.date <= ?", filter.DateTo))

			if filter.SourceID != nil {
				innerMatched = innerMatched.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
			}

			matchedQuery := squirrel.
				Select("COUNT(*) AS cnt", "COALESCE(SUM(t.amount), 0) AS total").
				FromSelect(innerMatched, "t").
				PlaceholderFormat(squirrel.Dollar)

			matchedSQL, matchedArgs, err := matchedQuery.ToSql()
			if err != nil {
				return nil, fmt.Errorf("building matched summary query: %w", err)
			}

			// Unmatched summary: COUNT/SUM over non-matched transactions in the
			// context's reconciliation sources, optionally filtered by source and
			// status.
			unmatchedQuery := squirrel.
				Select("COUNT(*) AS cnt", "COALESCE(SUM(t.amount), 0) AS total").
				From("transactions t").
				Join("reconciliation_sources rs ON t.source_id = rs.id").
				Where(squirrel.Eq{"rs.context_id": filter.ContextID}).
				Where(squirrel.NotEq{"t.status": "MATCHED"}).
				Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
				Where(squirrel.Expr("t.date <= ?", filter.DateTo)).
				PlaceholderFormat(squirrel.Dollar)

			if filter.SourceID != nil {
				unmatchedQuery = unmatchedQuery.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
			}

			if filter.Status != nil {
				unmatchedQuery = unmatchedQuery.Where(squirrel.Eq{"t.status": *filter.Status})
			}

			unmatchedSQL, unmatchedArgs, err := unmatchedQuery.ToSql()
			if err != nil {
				return nil, fmt.Errorf("building unmatched summary query: %w", err)
			}

			var matchedCount int

			var matchedTotal decimal.Decimal

			row := qe.QueryRowContext(ctx, matchedSQL, matchedArgs...)
			if err := row.Scan(&matchedCount, &matchedTotal); err != nil {
				return nil, fmt.Errorf("scanning matched summary: %w", err)
			}

			var unmatchedCount int

			var unmatchedTotal decimal.Decimal

			row = qe.QueryRowContext(ctx, unmatchedSQL, unmatchedArgs...)
			if err := row.Scan(&unmatchedCount, &unmatchedTotal); err != nil {
				return nil, fmt.Errorf("scanning unmatched summary: %w", err)
			}

			return &entities.SummaryReport{
				MatchedCount:    matchedCount,
				UnmatchedCount:  unmatchedCount,
				MatchedAmount:   matchedTotal,
				UnmatchedAmount: unmatchedTotal,
				TotalAmount:     matchedTotal.Add(unmatchedTotal),
			}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("get summary transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get summary", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get summary: %v", wrappedErr))

		return nil, wrappedErr
	}

	return result, nil
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

// applyUnmatchedExportFilters adds optional filters to the unmatched export query.
func applyUnmatchedExportFilters(
	query squirrel.SelectBuilder,
	filter entities.ReportFilter,
) squirrel.SelectBuilder {
	if filter.SourceID != nil {
		query = query.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
	}

	if filter.Status != nil {
		query = query.Where(squirrel.Eq{"t.status": *filter.Status})
	}

	return query
}

// executeUnmatchedExportQuery executes the query and scans results.
func executeUnmatchedExportQuery(
	ctx context.Context,
	qe pgcommon.QueryExecutor,
	query squirrel.SelectBuilder,
	maxRecords int,
) ([]*entities.UnmatchedItem, error) {
	sqlQuery, args, err := query.ToSql()
	if err != nil {
		return nil, fmt.Errorf("building export query: %w", err)
	}

	rows, err := qe.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("querying unmatched items for export: %w", err)
	}

	defer rows.Close()

	items := make([]*entities.UnmatchedItem, 0, maxRecords)

	for rows.Next() {
		item, err := scanUnmatchedItem(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning unmatched item: %w", err)
		}

		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating unmatched items: %w", err)
	}

	// Defensive: unreachable while query applies LIMIT, retained as safety net.
	if len(items) > maxRecords {
		return nil, fmt.Errorf(
			"%w: result set exceeds %d records",
			ErrExportLimitExceeded,
			maxRecords,
		)
	}

	return items, nil
}

func (repo *Repository) validateVarianceFilter(filter *entities.VarianceReportFilter) error {
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

// GetVarianceReport retrieves variance data aggregated by source, currency, and fee type.
func (repo *Repository) GetVarianceReport(
	ctx context.Context,
	filter entities.VarianceReportFilter,
) ([]*entities.VarianceReportRow, libHTTP.CursorPagination, error) {
	if err := repo.validateVarianceFilter(&filter); err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_variance_report")

	defer span.End()

	pagArgs, err := repo.buildVariancePaginationArgs(filter)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) ([]*entities.VarianceReportRow, error) {
			query, err := buildVariancePaginatedQuery(filter, pagArgs)
			if err != nil {
				return nil, err
			}

			sqlStr, args, err := query.ToSql()
			if err != nil {
				return nil, fmt.Errorf("building variance report query: %w", err)
			}

			rows, err := qe.QueryContext(ctx, sqlStr, args...)
			if err != nil {
				return nil, fmt.Errorf("querying variance report: %w", err)
			}

			defer rows.Close()

			items := make([]*entities.VarianceReportRow, 0, pagArgs.limit+1)

			for rows.Next() {
				item, err := scanVarianceRow(rows)
				if err != nil {
					return nil, fmt.Errorf("scanning variance row: %w", err)
				}

				items = append(items, item)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterating variance rows: %w", err)
			}

			return items, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("get variance report transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get variance report", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get variance report: %v", wrappedErr))

		return nil, libHTTP.CursorPagination{}, wrappedErr
	}

	items, pagination, err := paginateVarianceItems(filter, pagArgs, result)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	return items, pagination, nil
}

// buildVarianceSelectBuilder constructs the base squirrel query for fee variance
// reports. It performs a three-table JOIN (match_fee_variances, transactions, rates)
// with GROUP BY aggregation, producing per-source/currency/fee-type totals.
//
// The returned builder includes GROUP BY and ORDER BY clauses but no LIMIT or
// cursor filtering -- callers add those according to their pagination strategy.
//
// Columns returned:
//
//	t.source_id, fv.currency, r.structure_type (as fee_type),
//	total_expected, total_actual, net_variance
func buildVarianceSelectBuilder(
	contextID uuid.UUID,
	dateFrom, dateTo any,
	sourceID *uuid.UUID,
	orderDirection string,
) squirrel.SelectBuilder {
	query := squirrel.Select(
		"t.source_id",
		"fv.currency",
		"r.structure_type AS fee_type",
		"COALESCE(SUM(fv.expected_fee_amount), 0) AS total_expected",
		"COALESCE(SUM(fv.actual_fee_amount), 0) AS total_actual",
		"COALESCE(SUM(fv.delta), 0) AS net_variance",
	).
		From("match_fee_variances fv").
		Join("transactions t ON t.id = fv.transaction_id").
		Join("rates r ON r.id = fv.rate_id").
		Where(squirrel.Eq{"fv.context_id": contextID}).
		Where(squirrel.Expr("fv.created_at >= ?", dateFrom)).
		Where(squirrel.Expr("fv.created_at <= ?", dateTo)).
		GroupBy("t.source_id", "fv.currency", "r.structure_type").
		OrderBy(
			"t.source_id "+orderDirection,
			"fv.currency "+orderDirection,
			"r.structure_type "+orderDirection,
		).
		PlaceholderFormat(squirrel.Dollar)

	if sourceID != nil {
		query = query.Where(squirrel.Eq{"t.source_id": *sourceID})
	}

	return query
}

func buildVariancePaginatedQuery(
	filter entities.VarianceReportFilter,
	args paginationArgs,
) (squirrel.SelectBuilder, error) {
	operator, effectiveOrder, err := libHTTP.CursorDirectionRules(args.orderDirection, args.cursor.Direction)
	if err != nil {
		return squirrel.SelectBuilder{}, fmt.Errorf("cursor direction rules: %w", err)
	}

	query := buildVarianceSelectBuilder(
		filter.ContextID,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
		effectiveOrder,
	)

	if args.cursor.ID != "" {
		cursorSourceID, cursorCurrency, cursorFeeType, parseErr := parseVarianceCursorParts(args.cursor.ID)
		if parseErr != nil {
			return squirrel.SelectBuilder{}, fmt.Errorf("variance cursor: %w", parseErr)
		}

		query = query.Where(
			squirrel.Expr(
				"(t.source_id, fv.currency, r.structure_type) "+operator+" (?, ?, ?)",
				cursorSourceID,
				cursorCurrency,
				cursorFeeType,
			),
		)
	}

	query = query.Limit(safeLimitForPage(args.limit + 1))

	return query, nil
}

func (repo *Repository) buildVariancePaginationArgs(
	filter entities.VarianceReportFilter,
) (paginationArgs, error) {
	return buildGenericPaginationArgs(filter.Cursor, filter.SortOrder, filter.Limit)
}

func paginateVarianceItems(
	filter entities.VarianceReportFilter,
	args paginationArgs,
	items []*entities.VarianceReportRow,
) ([]*entities.VarianceReportRow, libHTTP.CursorPagination, error) {
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

	getVarianceRowKey := func(row *entities.VarianceReportRow) string {
		return row.SourceID.String() + ":" + row.Currency + ":" + row.FeeType
	}

	pagination, err := libHTTP.CalculateCursor(
		isFirstPage,
		hasPagination,
		args.cursor.Direction,
		getVarianceRowKey(items[0]),
		getVarianceRowKey(items[len(items)-1]),
	)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, fmt.Errorf("failed to calculate cursor: %w", err)
	}

	return items, pagination, nil
}

func scanVarianceRow(
	scanner interface{ Scan(dest ...any) error },
) (*entities.VarianceReportRow, error) {
	var sourceID uuid.UUID

	var currency, feeType string

	var totalExpected, totalActual, netVariance decimal.Decimal

	if err := scanner.Scan(
		&sourceID,
		&currency,
		&feeType,
		&totalExpected,
		&totalActual,
		&netVariance,
	); err != nil {
		return nil, fmt.Errorf("scanning variance row: %w", err)
	}

	return entities.BuildVarianceRow(
		sourceID,
		currency,
		feeType,
		totalExpected,
		totalActual,
		netVariance,
	), nil
}

// ListMatchedForExport retrieves matched transactions for export with a maximum record limit.
func (repo *Repository) ListMatchedForExport(
	ctx context.Context,
	filter entities.ReportFilter,
	maxRecords int,
) ([]*entities.MatchedItem, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return nil, ErrContextIDRequired
	}

	if maxRecords <= 0 {
		return nil, ErrMaxRecordsMustBePositive
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_matched_for_export")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) ([]*entities.MatchedItem, error) {
			query := squirrel.Select(
				"t.id",
				"mg.id",
				"t.source_id",
				"mi.allocated_amount",
				"mi.allocated_currency",
				"t.date",
			).
				From("match_items mi").
				Join("match_groups mg ON mi.match_group_id = mg.id").
				Join("transactions t ON mi.transaction_id = t.id").
				Where(squirrel.Eq{"mg.context_id": filter.ContextID, "mg.status": matchGroupStatusConfirmed}).
				Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
				Where(squirrel.Expr("t.date <= ?", filter.DateTo)).
				OrderBy("t.id ASC").
				Limit(safeExportLimit(maxRecords)).
				PlaceholderFormat(squirrel.Dollar)

			if filter.SourceID != nil {
				query = query.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
			}

			sqlQuery, args, err := query.ToSql()
			if err != nil {
				return nil, fmt.Errorf("building export query: %w", err)
			}

			rows, err := qe.QueryContext(ctx, sqlQuery, args...)
			if err != nil {
				return nil, fmt.Errorf("querying matched items for export: %w", err)
			}

			defer rows.Close()

			items := make([]*entities.MatchedItem, 0, maxRecords)

			for rows.Next() {
				item, err := scanMatchedItem(rows)
				if err != nil {
					return nil, fmt.Errorf("scanning matched item: %w", err)
				}

				items = append(items, item)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterating matched items: %w", err)
			}

			// Defensive: unreachable while query applies LIMIT, retained as safety net.
			if len(items) > maxRecords {
				return nil, fmt.Errorf(
					"%w: result set exceeds %d records",
					ErrExportLimitExceeded,
					maxRecords,
				)
			}

			return items, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list matched for export transaction: %w", err)
		libOpentelemetry.HandleSpanError(
			span,
			"failed to list matched items for export",
			wrappedErr,
		)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list matched items for export: %v", wrappedErr))

		return nil, wrappedErr
	}

	return result, nil
}

// ListUnmatchedForExport retrieves unmatched transactions for export with a maximum record limit.
func (repo *Repository) ListUnmatchedForExport(
	ctx context.Context,
	filter entities.ReportFilter,
	maxRecords int,
) ([]*entities.UnmatchedItem, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return nil, ErrContextIDRequired
	}

	if maxRecords <= 0 {
		return nil, ErrMaxRecordsMustBePositive
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_unmatched_for_export")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) ([]*entities.UnmatchedItem, error) {
			query := squirrel.Select(
				"t.id",
				"t.source_id",
				"t.amount",
				"t.currency",
				"t.status",
				"t.date",
				"e.id",
				"e.due_at",
			).
				From("transactions t").
				Join("reconciliation_sources rs ON t.source_id = rs.id").
				LeftJoin("exceptions e ON e.transaction_id = t.id").
				Where(squirrel.Eq{"rs.context_id": filter.ContextID}).
				Where(squirrel.NotEq{"t.status": "MATCHED"}).
				Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
				Where(squirrel.Expr("t.date <= ?", filter.DateTo)).
				OrderBy("t.id ASC").
				Limit(safeExportLimit(maxRecords)).
				PlaceholderFormat(squirrel.Dollar)

			query = applyUnmatchedExportFilters(query, filter)

			return executeUnmatchedExportQuery(ctx, qe, query, maxRecords)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list unmatched for export transaction: %w", err)
		libOpentelemetry.HandleSpanError(
			span,
			"failed to list unmatched items for export",
			wrappedErr,
		)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list unmatched items for export: %v", wrappedErr))

		return nil, wrappedErr
	}

	return result, nil
}

// ListVarianceForExport retrieves variance rows for export with a maximum record limit.
func (repo *Repository) ListVarianceForExport(
	ctx context.Context,
	filter entities.VarianceReportFilter,
	maxRecords int,
) ([]*entities.VarianceReportRow, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return nil, ErrContextIDRequired
	}

	if maxRecords <= 0 {
		return nil, ErrMaxRecordsMustBePositive
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_variance_for_export")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) ([]*entities.VarianceReportRow, error) {
			query := buildVarianceSelectBuilder(
				filter.ContextID,
				filter.DateFrom,
				filter.DateTo,
				filter.SourceID,
				sortOrderAsc,
			).
				Limit(safeExportLimit(maxRecords))

			sqlStr, args, err := query.ToSql()
			if err != nil {
				return nil, fmt.Errorf("building variance export query: %w", err)
			}

			rows, err := qe.QueryContext(ctx, sqlStr, args...)
			if err != nil {
				return nil, fmt.Errorf("querying variance report for export: %w", err)
			}

			defer rows.Close()

			items := make([]*entities.VarianceReportRow, 0, safeExportLimit(maxRecords))

			for rows.Next() {
				item, err := scanVarianceRow(rows)
				if err != nil {
					return nil, fmt.Errorf("scanning variance row: %w", err)
				}

				items = append(items, item)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterating variance rows: %w", err)
			}

			// Defensive: unreachable while query applies LIMIT, retained as safety net.
			if len(items) > maxRecords {
				return nil, fmt.Errorf(
					"%w: result set exceeds %d records",
					ErrExportLimitExceeded,
					maxRecords,
				)
			}

			return items, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list variance for export transaction: %w", err)
		libOpentelemetry.HandleSpanError(
			span,
			"failed to list variance rows for export",
			wrappedErr,
		)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list variance rows for export: %v", wrappedErr))

		return nil, wrappedErr
	}

	return result, nil
}

// ListMatchedPage retrieves a page of matched transactions for streaming export.
//
//nolint:cyclop // cursor pagination with multiple filter paths
func (repo *Repository) ListMatchedPage(
	ctx context.Context,
	filter entities.ReportFilter,
	afterKey string,
	limit int,
) ([]*entities.MatchedItem, string, error) {
	if repo == nil || repo.provider == nil {
		return nil, "", ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return nil, "", ErrContextIDRequired
	}

	limit = normalizeLimit(limit)

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_matched_page")

	defer span.End()

	type pageResult struct {
		items   []*entities.MatchedItem
		nextKey string
	}

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (pageResult, error) {
			query := squirrel.Select(
				"t.id",
				"mg.id",
				"t.source_id",
				"mi.allocated_amount",
				"mi.allocated_currency",
				"t.date",
			).
				From("match_items mi").
				Join("match_groups mg ON mi.match_group_id = mg.id").
				Join("transactions t ON mi.transaction_id = t.id").
				Where(squirrel.Eq{"mg.context_id": filter.ContextID, "mg.status": matchGroupStatusConfirmed}).
				Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
				Where(squirrel.Expr("t.date <= ?", filter.DateTo)).
				OrderBy("t.id ASC").
				Limit(safeLimitForPage(limit + 1)).
				PlaceholderFormat(squirrel.Dollar)

			if filter.SourceID != nil {
				query = query.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
			}

			if afterKey != "" {
				afterID, err := uuid.Parse(afterKey)
				if err != nil {
					return pageResult{}, fmt.Errorf("parsing after key: %w", err)
				}

				query = query.Where(squirrel.Gt{"t.id": afterID})
			}

			sqlQuery, args, err := query.ToSql()
			if err != nil {
				return pageResult{}, fmt.Errorf("building page query: %w", err)
			}

			rows, err := qe.QueryContext(ctx, sqlQuery, args...)
			if err != nil {
				return pageResult{}, fmt.Errorf("querying matched page: %w", err)
			}

			defer rows.Close()

			items := make([]*entities.MatchedItem, 0, limit)

			for rows.Next() {
				item, err := scanMatchedItem(rows)
				if err != nil {
					return pageResult{}, fmt.Errorf("scanning matched item: %w", err)
				}

				items = append(items, item)
			}

			if err := rows.Err(); err != nil {
				return pageResult{}, fmt.Errorf("iterating matched items: %w", err)
			}

			var nextKey string

			if len(items) > limit {
				items = items[:limit]
				nextKey = items[limit-1].TransactionID.String()
			}

			return pageResult{items: items, nextKey: nextKey}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list matched page transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list matched page", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list matched page: %v", wrappedErr))

		return nil, "", wrappedErr
	}

	return result.items, result.nextKey, nil
}

// ListUnmatchedPage retrieves a page of unmatched transactions for streaming export.
//
//nolint:cyclop // cursor pagination with multiple filter paths
func (repo *Repository) ListUnmatchedPage(
	ctx context.Context,
	filter entities.ReportFilter,
	afterKey string,
	limit int,
) ([]*entities.UnmatchedItem, string, error) {
	if repo == nil || repo.provider == nil {
		return nil, "", ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return nil, "", ErrContextIDRequired
	}

	limit = normalizeLimit(limit)

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_unmatched_page")

	defer span.End()

	type pageResult struct {
		items   []*entities.UnmatchedItem
		nextKey string
	}

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (pageResult, error) {
			query := squirrel.Select(
				"t.id",
				"t.source_id",
				"t.amount",
				"t.currency",
				"t.status",
				"t.date",
				"e.id",
				"e.due_at",
			).
				From("transactions t").
				Join("reconciliation_sources rs ON t.source_id = rs.id").
				LeftJoin("exceptions e ON t.id = e.transaction_id").
				Where(squirrel.Eq{"rs.context_id": filter.ContextID}).
				Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
				Where(squirrel.Expr("t.date <= ?", filter.DateTo)).
				Where(squirrel.NotEq{"t.status": "MATCHED"}).
				OrderBy("t.id ASC").
				Limit(safeLimitForPage(limit + 1)).
				PlaceholderFormat(squirrel.Dollar)

			if filter.SourceID != nil {
				query = query.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
			}

			if filter.Status != nil {
				query = query.Where(squirrel.Eq{"t.status": *filter.Status})
			}

			if afterKey != "" {
				afterID, err := uuid.Parse(afterKey)
				if err != nil {
					return pageResult{}, fmt.Errorf("parsing after key: %w", err)
				}

				query = query.Where(squirrel.Gt{"t.id": afterID})
			}

			sqlQuery, args, err := query.ToSql()
			if err != nil {
				return pageResult{}, fmt.Errorf("building page query: %w", err)
			}

			rows, err := qe.QueryContext(ctx, sqlQuery, args...)
			if err != nil {
				return pageResult{}, fmt.Errorf("querying unmatched page: %w", err)
			}

			defer rows.Close()

			items := make([]*entities.UnmatchedItem, 0, limit)

			for rows.Next() {
				item, err := scanUnmatchedItem(rows)
				if err != nil {
					return pageResult{}, fmt.Errorf("scanning unmatched item: %w", err)
				}

				items = append(items, item)
			}

			if err := rows.Err(); err != nil {
				return pageResult{}, fmt.Errorf("iterating unmatched items: %w", err)
			}

			var nextKey string

			if len(items) > limit {
				items = items[:limit]
				nextKey = items[limit-1].TransactionID.String()
			}

			return pageResult{items: items, nextKey: nextKey}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list unmatched page transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list unmatched page", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list unmatched page: %v", wrappedErr))

		return nil, "", wrappedErr
	}

	return result.items, result.nextKey, nil
}

// varianceCursorFilter holds the parsed cursor state for variance page queries.
type varianceCursorFilter struct {
	query  string
	args   []any
	argIdx int
}

// applyVarianceCursor parses the composite cursor key and appends the
// appropriate WHERE clause and arguments for keyset pagination.
func applyVarianceCursor(afterKey, query string, args []any, argIdx int) (varianceCursorFilter, error) {
	if afterKey == "" {
		return varianceCursorFilter{query: query, args: args, argIdx: argIdx}, nil
	}

	sourceID, currency, feeType, err := parseVarianceCursorParts(afterKey)
	if err != nil {
		return varianceCursorFilter{}, err
	}

	p1, p2, p3 := argIdx, argIdx+1, argIdx+cursorPartCount-1
	query += fmt.Sprintf(
		" AND (t.source_id, fv.currency, r.structure_type) > ($%d, $%d, $%d)",
		p1, p2, p3,
	)

	args = append(args, sourceID, currency, feeType)
	argIdx += cursorPartCount

	return varianceCursorFilter{query: query, args: args, argIdx: argIdx}, nil
}

func parseVarianceCursorParts(afterKey string) (uuid.UUID, string, string, error) {
	parts := strings.SplitN(afterKey, ":", cursorPartCount)
	if len(parts) != cursorPartCount {
		return uuid.Nil, "", "", fmt.Errorf("%w: expected %d parts, got %d", ErrInvalidVarianceCursor, cursorPartCount, len(parts))
	}

	sourceID, err := uuid.Parse(parts[0])
	if err != nil {
		return uuid.Nil, "", "", fmt.Errorf("%w: source_id is not a valid UUID: %w", ErrInvalidVarianceCursor, err)
	}

	if !currencyPattern.MatchString(parts[1]) {
		return uuid.Nil, "", "", fmt.Errorf("%w: currency is not a valid 3-letter ISO code", ErrInvalidVarianceCursor)
	}

	if !validFeeStructureTypes[parts[2]] {
		return uuid.Nil, "", "", fmt.Errorf("%w: fee type %q is not a recognized structure type", ErrInvalidVarianceCursor, parts[2])
	}

	return sourceID, parts[1], parts[2], nil
}

// ListVariancePage retrieves a page of variance rows for streaming export.
func (repo *Repository) ListVariancePage(
	ctx context.Context,
	filter entities.VarianceReportFilter,
	afterKey string,
	limit int,
) ([]*entities.VarianceReportRow, string, error) {
	if repo == nil || repo.provider == nil {
		return nil, "", ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return nil, "", ErrContextIDRequired
	}

	limit = normalizeLimit(limit)

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_variance_page")

	defer span.End()

	type pageResult struct {
		items   []*entities.VarianceReportRow
		nextKey string
	}

	// RATIONALE: ListVariancePage uses manual SQL (varianceBaseQuery) because
	// composite ROW keyset pagination -- (source_id, currency, structure_type) > ($N, $N+1, $N+2) --
	// cannot be expressed via squirrel's Where() clause. The applyVarianceCursor
	// helper appends this comparison with validated positional args. Other variance
	// methods (GetVarianceReport, ListVarianceForExport) use squirrel fully.
	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (pageResult, error) {
			// Start from the base WHERE clause template.
			// Positional args: $1=contextID, $2=dateFrom, $3=dateTo.
			querySQL := varianceBaseQuery
			queryArgs := []any{filter.ContextID, filter.DateFrom, filter.DateTo}
			nextArgIdx := 4 // next available positional arg index

			// Optional source filter: appends the next positional arg.
			if filter.SourceID != nil {
				querySQL += fmt.Sprintf(" AND t.source_id = $%d", nextArgIdx)

				queryArgs = append(queryArgs, *filter.SourceID)
				nextArgIdx++
			}

			// Keyset cursor: appends composite ROW comparison for pagination.
			// e.g., AND (t.source_id, fv.currency, r.structure_type) > ($N, $N+1, $N+2)
			cf, err := applyVarianceCursor(afterKey, querySQL, queryArgs, nextArgIdx)
			if err != nil {
				return pageResult{}, fmt.Errorf("applying variance cursor: %w", err)
			}

			querySQL = cf.query
			queryArgs = cf.args
			nextArgIdx = cf.argIdx

			// Finalize: GROUP BY + ORDER BY + LIMIT.
			querySQL += varianceGroupByClause
			querySQL += varianceOrderByClause
			querySQL += fmt.Sprintf(" LIMIT $%d", nextArgIdx)

			queryArgs = append(queryArgs, limit+1)

			rows, err := qe.QueryContext(ctx, querySQL, queryArgs...)
			if err != nil {
				return pageResult{}, fmt.Errorf("querying variance page: %w", err)
			}

			defer rows.Close()

			items := make([]*entities.VarianceReportRow, 0, limit)

			for rows.Next() {
				item, err := scanVarianceRow(rows)
				if err != nil {
					return pageResult{}, fmt.Errorf("scanning variance row: %w", err)
				}

				items = append(items, item)
			}

			if err := rows.Err(); err != nil {
				return pageResult{}, fmt.Errorf("iterating variance rows: %w", err)
			}

			var nextKey string

			if len(items) > limit {
				items = items[:limit]
				last := items[limit-1]
				nextKey = last.SourceID.String() + ":" + last.Currency + ":" + last.FeeType
			}

			return pageResult{items: items, nextKey: nextKey}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list variance page transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list variance page", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list variance page: %v", wrappedErr))

		return nil, "", wrappedErr
	}

	return result.items, result.nextKey, nil
}

// CountMatched returns the total count of matched records for a filter.
func (repo *Repository) CountMatched(
	ctx context.Context,
	filter entities.ReportFilter,
) (int64, error) {
	if repo == nil || repo.provider == nil {
		return 0, ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return 0, ErrContextIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.count_matched")

	defer span.End()

	count, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (int64, error) {
			query := squirrel.Select("COUNT(*)").
				From("match_items mi").
				Join("match_groups mg ON mi.match_group_id = mg.id").
				Join("transactions t ON mi.transaction_id = t.id").
				Where(squirrel.Eq{"mg.context_id": filter.ContextID, "mg.status": matchGroupStatusConfirmed}).
				Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
				Where(squirrel.Expr("t.date <= ?", filter.DateTo)).
				PlaceholderFormat(squirrel.Dollar)

			if filter.SourceID != nil {
				query = query.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
			}

			sqlQuery, args, err := query.ToSql()
			if err != nil {
				return 0, fmt.Errorf("building count query: %w", err)
			}

			var count int64
			if err := qe.QueryRowContext(ctx, sqlQuery, args...).Scan(&count); err != nil {
				return 0, fmt.Errorf("counting matched: %w", err)
			}

			return count, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("count matched transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to count matched", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to count matched: %v", wrappedErr))

		return 0, wrappedErr
	}

	return count, nil
}

// CountUnmatched returns the total count of unmatched records for a filter.
func (repo *Repository) CountUnmatched(
	ctx context.Context,
	filter entities.ReportFilter,
) (int64, error) {
	if repo == nil || repo.provider == nil {
		return 0, ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return 0, ErrContextIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.count_unmatched")

	defer span.End()

	count, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (int64, error) {
			query := squirrel.Select("COUNT(*)").
				From("transactions t").
				Join("reconciliation_sources rs ON t.source_id = rs.id").
				Where(squirrel.Eq{"rs.context_id": filter.ContextID}).
				Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
				Where(squirrel.Expr("t.date <= ?", filter.DateTo)).
				Where(squirrel.NotEq{"t.status": "MATCHED"}).
				PlaceholderFormat(squirrel.Dollar)

			if filter.SourceID != nil {
				query = query.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
			}

			if filter.Status != nil {
				query = query.Where(squirrel.Eq{"t.status": *filter.Status})
			}

			sqlQuery, args, err := query.ToSql()
			if err != nil {
				return 0, fmt.Errorf("building count query: %w", err)
			}

			var count int64
			if err := qe.QueryRowContext(ctx, sqlQuery, args...).Scan(&count); err != nil {
				return 0, fmt.Errorf("counting unmatched: %w", err)
			}

			return count, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("count unmatched transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to count unmatched", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to count unmatched: %v", wrappedErr))

		return 0, wrappedErr
	}

	return count, nil
}

// CountTransactions returns the total count of all transactions for a filter.
func (repo *Repository) CountTransactions(
	ctx context.Context,
	filter entities.ReportFilter,
) (int64, error) {
	if repo == nil || repo.provider == nil {
		return 0, ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return 0, ErrContextIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.count_transactions")

	defer span.End()

	count, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (int64, error) {
			query := squirrel.Select("COUNT(*)").
				From("transactions t").
				Join("reconciliation_sources rs ON t.source_id = rs.id").
				Where(squirrel.Eq{"rs.context_id": filter.ContextID}).
				Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
				Where(squirrel.Expr("t.date <= ?", filter.DateTo)).
				PlaceholderFormat(squirrel.Dollar)

			if filter.SourceID != nil {
				query = query.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
			}

			sqlQuery, args, err := query.ToSql()
			if err != nil {
				return 0, fmt.Errorf("building count query: %w", err)
			}

			var count int64
			if err := qe.QueryRowContext(ctx, sqlQuery, args...).Scan(&count); err != nil {
				return 0, fmt.Errorf("counting transactions: %w", err)
			}

			return count, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("count transactions: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to count transactions", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to count transactions: %v", wrappedErr))

		return 0, wrappedErr
	}

	return count, nil
}

// CountExceptions returns the total count of exceptions for a filter.
func (repo *Repository) CountExceptions(
	ctx context.Context,
	filter entities.ReportFilter,
) (int64, error) {
	if repo == nil || repo.provider == nil {
		return 0, ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return 0, ErrContextIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.count_exceptions")

	defer span.End()

	count, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (int64, error) {
			query := squirrel.Select("COUNT(*)").
				From("exceptions e").
				Join("transactions t ON e.transaction_id = t.id").
				Join("reconciliation_sources rs ON t.source_id = rs.id").
				Where(squirrel.Eq{"rs.context_id": filter.ContextID}).
				Where(squirrel.Expr("e.created_at >= ?", filter.DateFrom)).
				Where(squirrel.Expr("e.created_at <= ?", filter.DateTo)).
				PlaceholderFormat(squirrel.Dollar)

			if filter.SourceID != nil {
				query = query.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
			}

			sqlQuery, args, err := query.ToSql()
			if err != nil {
				return 0, fmt.Errorf("building count query: %w", err)
			}

			var count int64
			if err := qe.QueryRowContext(ctx, sqlQuery, args...).Scan(&count); err != nil {
				return 0, fmt.Errorf("counting exceptions: %w", err)
			}

			return count, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("count exceptions: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to count exceptions", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to count exceptions: %v", wrappedErr))

		return 0, wrappedErr
	}

	return count, nil
}

var _ repositories.ReportRepository = (*Repository)(nil)
