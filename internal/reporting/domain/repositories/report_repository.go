// Package repositories provides reporting persistence contracts.
package repositories

//go:generate mockgen -destination=mocks/report_repository_mock.go -package=mocks . ReportRepository

import (
	"context"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

// RowIterator provides a streaming interface for database rows.
// Implementations must be closed after use to release resources.
type RowIterator interface {
	// Next advances the iterator to the next row.
	// Returns false when no more rows or an error occurred.
	Next() bool

	// Err returns any error that occurred during iteration.
	Err() error

	// Close releases the iterator resources.
	Close() error
}

// MatchedRowIterator iterates over matched transaction rows.
type MatchedRowIterator interface {
	RowIterator
	// Scan reads the current row into a MatchedItem.
	Scan() (*entities.MatchedItem, error)
}

// UnmatchedRowIterator iterates over unmatched transaction rows.
type UnmatchedRowIterator interface {
	RowIterator
	// Scan reads the current row into an UnmatchedItem.
	Scan() (*entities.UnmatchedItem, error)
}

// VarianceRowIterator iterates over variance report rows.
type VarianceRowIterator interface {
	RowIterator
	// Scan reads the current row into a VarianceReportRow.
	Scan() (*entities.VarianceReportRow, error)
}

// ReportRepository defines read-only operations for report data.
type ReportRepository interface {
	// ListMatched retrieves matched transactions based on filter criteria.
	ListMatched(
		ctx context.Context,
		filter entities.ReportFilter,
	) ([]*entities.MatchedItem, libHTTP.CursorPagination, error)

	// ListUnmatched retrieves unmatched transactions based on filter criteria.
	ListUnmatched(
		ctx context.Context,
		filter entities.ReportFilter,
	) ([]*entities.UnmatchedItem, libHTTP.CursorPagination, error)

	// GetSummary retrieves aggregated summary statistics.
	GetSummary(ctx context.Context, filter entities.ReportFilter) (*entities.SummaryReport, error)

	// GetVarianceReport retrieves variance data aggregated by source, currency, and fee schedule.
	GetVarianceReport(
		ctx context.Context,
		filter entities.VarianceReportFilter,
	) ([]*entities.VarianceReportRow, libHTTP.CursorPagination, error)

	// ListMatchedForExport retrieves matched transactions for export with a maximum record limit.
	// Returns ErrExportLimitExceeded if the result set exceeds the configured maximum.
	ListMatchedForExport(
		ctx context.Context,
		filter entities.ReportFilter,
		maxRecords int,
	) ([]*entities.MatchedItem, error)

	// ListUnmatchedForExport retrieves unmatched transactions for export with a maximum record limit.
	// Returns ErrExportLimitExceeded if the result set exceeds the configured maximum.
	ListUnmatchedForExport(
		ctx context.Context,
		filter entities.ReportFilter,
		maxRecords int,
	) ([]*entities.UnmatchedItem, error)

	// ListVarianceForExport retrieves variance rows for export with a maximum record limit.
	// Returns ErrExportLimitExceeded if the result set exceeds the configured maximum.
	ListVarianceForExport(
		ctx context.Context,
		filter entities.VarianceReportFilter,
		maxRecords int,
	) ([]*entities.VarianceReportRow, error)

	// ListMatchedPage retrieves a page of matched transactions for streaming export.
	// Uses keyset pagination with afterKey for efficient large dataset handling.
	ListMatchedPage(
		ctx context.Context,
		filter entities.ReportFilter,
		afterKey string,
		limit int,
	) ([]*entities.MatchedItem, string, error)

	// ListUnmatchedPage retrieves a page of unmatched transactions for streaming export.
	// Uses keyset pagination with afterKey for efficient large dataset handling.
	ListUnmatchedPage(
		ctx context.Context,
		filter entities.ReportFilter,
		afterKey string,
		limit int,
	) ([]*entities.UnmatchedItem, string, error)

	// ListVariancePage retrieves a page of variance rows for streaming export.
	// Uses keyset pagination with afterKey for efficient large dataset handling.
	ListVariancePage(
		ctx context.Context,
		filter entities.VarianceReportFilter,
		afterKey string,
		limit int,
	) ([]*entities.VarianceReportRow, string, error)

	// CountMatched returns the total count of matched records for a filter.
	CountMatched(ctx context.Context, filter entities.ReportFilter) (int64, error)

	// CountUnmatched returns the total count of unmatched records for a filter.
	CountUnmatched(ctx context.Context, filter entities.ReportFilter) (int64, error)

	// CountTransactions returns the total count of all transactions for a filter.
	CountTransactions(ctx context.Context, filter entities.ReportFilter) (int64, error)

	// CountExceptions returns the total count of exceptions for a filter.
	CountExceptions(ctx context.Context, filter entities.ReportFilter) (int64, error)
}

// StreamingReportRepository extends ReportRepository with streaming export capabilities.
// Streaming methods avoid loading entire result sets into memory.
type StreamingReportRepository interface {
	ReportRepository

	// StreamMatchedForExport streams matched transactions for export.
	// The caller must close the iterator when done.
	StreamMatchedForExport(
		ctx context.Context,
		filter entities.ReportFilter,
		maxRecords int,
	) (MatchedRowIterator, error)

	// StreamUnmatchedForExport streams unmatched transactions for export.
	// The caller must close the iterator when done.
	StreamUnmatchedForExport(
		ctx context.Context,
		filter entities.ReportFilter,
		maxRecords int,
	) (UnmatchedRowIterator, error)

	// StreamVarianceForExport streams variance rows for export.
	// The caller must close the iterator when done.
	StreamVarianceForExport(
		ctx context.Context,
		filter entities.VarianceReportFilter,
		maxRecords int,
	) (VarianceRowIterator, error)
}
