// Package query provides read operations for reporting.
package query

import (
	"context"
	"errors"
	"fmt"
	"io"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	"github.com/LerianStudio/matcher/internal/reporting/services/query/exports"
)

// ErrNilReportRepository is returned when a nil repository is provided.
var ErrNilReportRepository = errors.New("report repository is required")

// ErrStreamingNotSupported is returned when streaming is requested but not available.
var ErrStreamingNotSupported = errors.New("streaming not supported by repository")

// MaxExportRecords defines the maximum number of records allowed in a single export.
// This prevents OOM errors when exporting large datasets.
const MaxExportRecords = 100000

// MaxPDFExportRecords defines a lower limit for PDF exports, which are more memory-intensive
// than CSV due to in-memory layout rendering. This reduces OOM risk for concurrent PDF exports.
const MaxPDFExportRecords = 25000

// UseCase orchestrates report queries and export generation.
type UseCase struct {
	repo          repositories.ReportRepository
	streamingRepo repositories.StreamingReportRepository
}

// NewUseCase creates a new query use case with the required repository.
func NewUseCase(repo repositories.ReportRepository) (*UseCase, error) {
	if repo == nil {
		return nil, ErrNilReportRepository
	}

	uc := &UseCase{repo: repo}

	if streamingRepo, ok := repo.(repositories.StreamingReportRepository); ok {
		uc.streamingRepo = streamingRepo
	}

	return uc, nil
}

// SupportsStreaming returns true if the repository supports streaming exports.
func (uc *UseCase) SupportsStreaming() bool {
	return uc.streamingRepo != nil
}

// GetMatchedReport retrieves matched transactions based on filter criteria.
func (uc *UseCase) GetMatchedReport(
	ctx context.Context,
	filter entities.ReportFilter,
) ([]*entities.MatchedItem, libHTTP.CursorPagination, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.get_matched_report")
	defer span.End()

	result, pagination, err := uc.repo.ListMatched(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get matched report", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get matched report: %v", err))

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("listing matched items: %w", err)
	}

	return result, pagination, nil
}

// GetUnmatchedReport retrieves unmatched transactions based on filter criteria.
func (uc *UseCase) GetUnmatchedReport(
	ctx context.Context,
	filter entities.ReportFilter,
) ([]*entities.UnmatchedItem, libHTTP.CursorPagination, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.get_unmatched_report")
	defer span.End()

	result, pagination, err := uc.repo.ListUnmatched(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get unmatched report", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get unmatched report: %v", err))

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("listing unmatched items: %w", err)
	}

	return result, pagination, nil
}

// GetSummaryReport retrieves aggregated summary statistics.
func (uc *UseCase) GetSummaryReport(
	ctx context.Context,
	filter entities.ReportFilter,
) (*entities.SummaryReport, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.get_summary_report")
	defer span.End()

	result, err := uc.repo.GetSummary(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get summary report", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get summary report: %v", err))

		return nil, fmt.Errorf("getting summary report: %w", err)
	}

	return result, nil
}

// Export functions fetch records with a maximum limit to prevent OOM errors.
// If the result set exceeds MaxExportRecords, an error is returned.
//
// For large datasets, use the async export job API (POST /v1/contexts/:contextId/export-jobs)
// which provides streaming exports via the ExportWorker background processor.
// See: internal/reporting/services/worker/export_worker.go.

// ExportMatchedCSV generates a CSV file from matched items.
func (uc *UseCase) ExportMatchedCSV(
	ctx context.Context,
	filter entities.ReportFilter,
) ([]byte, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.export_matched_csv")
	defer span.End()

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"reporting",
			struct {
				ContextID    string `json:"contextId"`
				ExportFormat string `json:"exportFormat"`
			}{
				ContextID:    filter.ContextID.String(),
				ExportFormat: "CSV",
			},
			nil,
		)
	}

	items, err := uc.repo.ListMatchedForExport(ctx, filter, MaxExportRecords)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list matched items for CSV export", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list matched items for CSV export: %v", err))

		return nil, fmt.Errorf("listing matched items for CSV export: %w", err)
	}

	data, err := exports.BuildMatchedCSV(items)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build matched CSV", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to build matched CSV: %v", err))

		return nil, fmt.Errorf("building matched CSV: %w", err)
	}

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"reporting",
			struct {
				RecordsCount int `json:"recordsCount"`
			}{
				RecordsCount: len(items),
			},
			nil,
		)
	}

	return data, nil
}

// ExportUnmatchedCSV generates a CSV file from unmatched items.
func (uc *UseCase) ExportUnmatchedCSV(
	ctx context.Context,
	filter entities.ReportFilter,
) ([]byte, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.export_unmatched_csv")
	defer span.End()

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"reporting",
			struct {
				ContextID    string `json:"contextId"`
				ExportFormat string `json:"exportFormat"`
			}{
				ContextID:    filter.ContextID.String(),
				ExportFormat: "CSV",
			},
			nil,
		)
	}

	items, err := uc.repo.ListUnmatchedForExport(ctx, filter, MaxExportRecords)
	if err != nil {
		libOpentelemetry.HandleSpanError(
			span,
			"failed to list unmatched items for CSV export",
			err,
		)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list unmatched items for CSV export: %v", err))

		return nil, fmt.Errorf("listing unmatched items for CSV export: %w", err)
	}

	data, err := exports.BuildUnmatchedCSV(items)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build unmatched CSV", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to build unmatched CSV: %v", err))

		return nil, fmt.Errorf("building unmatched CSV: %w", err)
	}

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"reporting",
			struct {
				RecordsCount int `json:"recordsCount"`
			}{
				RecordsCount: len(items),
			},
			nil,
		)
	}

	return data, nil
}

// ExportSummaryCSV generates a CSV file from a summary report.
func (uc *UseCase) ExportSummaryCSV(
	ctx context.Context,
	filter entities.ReportFilter,
) ([]byte, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.export_summary_csv")
	defer span.End()

	summary, err := uc.repo.GetSummary(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get summary for CSV export", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get summary for CSV export: %v", err))

		return nil, fmt.Errorf("getting summary for CSV export: %w", err)
	}

	data, err := exports.BuildSummaryCSV(summary)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build summary CSV", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to build summary CSV: %v", err))

		return nil, fmt.Errorf("building summary CSV: %w", err)
	}

	return data, nil
}

// ExportMatchedPDF generates a PDF file from matched items.
func (uc *UseCase) ExportMatchedPDF(
	ctx context.Context,
	filter entities.ReportFilter,
) ([]byte, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.export_matched_pdf")
	defer span.End()

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"reporting",
			struct {
				ContextID    string `json:"contextId"`
				ExportFormat string `json:"exportFormat"`
			}{
				ContextID:    filter.ContextID.String(),
				ExportFormat: "PDF",
			},
			nil,
		)
	}

	items, err := uc.repo.ListMatchedForExport(ctx, filter, MaxPDFExportRecords)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list matched items for PDF export", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list matched items for PDF export: %v", err))

		return nil, fmt.Errorf("listing matched items for PDF export: %w", err)
	}

	data, err := exports.BuildMatchedPDF(items)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build matched PDF", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to build matched PDF: %v", err))

		return nil, fmt.Errorf("building matched PDF: %w", err)
	}

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"reporting",
			struct {
				RecordsCount int `json:"recordsCount"`
			}{
				RecordsCount: len(items),
			},
			nil,
		)
	}

	return data, nil
}

// ExportUnmatchedPDF generates a PDF file from unmatched items.
func (uc *UseCase) ExportUnmatchedPDF(
	ctx context.Context,
	filter entities.ReportFilter,
) ([]byte, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.export_unmatched_pdf")
	defer span.End()

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"reporting",
			struct {
				ContextID    string `json:"contextId"`
				ExportFormat string `json:"exportFormat"`
			}{
				ContextID:    filter.ContextID.String(),
				ExportFormat: "PDF",
			},
			nil,
		)
	}

	items, err := uc.repo.ListUnmatchedForExport(ctx, filter, MaxPDFExportRecords)
	if err != nil {
		libOpentelemetry.HandleSpanError(
			span,
			"failed to list unmatched items for PDF export",
			err,
		)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list unmatched items for PDF export: %v", err))

		return nil, fmt.Errorf("listing unmatched items for PDF export: %w", err)
	}

	data, err := exports.BuildUnmatchedPDF(items)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build unmatched PDF", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to build unmatched PDF: %v", err))

		return nil, fmt.Errorf("building unmatched PDF: %w", err)
	}

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"reporting",
			struct {
				RecordsCount int `json:"recordsCount"`
			}{
				RecordsCount: len(items),
			},
			nil,
		)
	}

	return data, nil
}

// ExportSummaryPDF generates a PDF file from a summary report.
func (uc *UseCase) ExportSummaryPDF(
	ctx context.Context,
	filter entities.ReportFilter,
) ([]byte, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.export_summary_pdf")
	defer span.End()

	summary, err := uc.repo.GetSummary(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get summary for PDF export", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get summary for PDF export: %v", err))

		return nil, fmt.Errorf("getting summary for PDF export: %w", err)
	}

	data, err := exports.BuildSummaryPDF(summary)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build summary PDF", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to build summary PDF: %v", err))

		return nil, fmt.Errorf("building summary PDF: %w", err)
	}

	return data, nil
}

// GetVarianceReport retrieves variance data aggregated by source, currency, and fee schedule.
func (uc *UseCase) GetVarianceReport(
	ctx context.Context,
	filter entities.VarianceReportFilter,
) ([]*entities.VarianceReportRow, libHTTP.CursorPagination, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.get_variance_report")
	defer span.End()

	result, pagination, err := uc.repo.GetVarianceReport(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get variance report", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get variance report: %v", err))

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("getting variance report: %w", err)
	}

	return result, pagination, nil
}

// ExportVarianceCSV generates a CSV file from variance report data.
func (uc *UseCase) ExportVarianceCSV(
	ctx context.Context,
	filter entities.VarianceReportFilter,
) ([]byte, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.export_variance_csv")
	defer span.End()

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"reporting",
			struct {
				ContextID    string `json:"contextId"`
				ExportFormat string `json:"exportFormat"`
			}{
				ContextID:    filter.ContextID.String(),
				ExportFormat: "CSV",
			},
			nil,
		)
	}

	rows, err := uc.repo.ListVarianceForExport(ctx, filter, MaxExportRecords)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get variance report for CSV export", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get variance report for CSV export: %v", err))

		return nil, fmt.Errorf("getting variance report for CSV export: %w", err)
	}

	data, err := exports.BuildVarianceCSV(rows)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build variance CSV", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to build variance CSV: %v", err))

		return nil, fmt.Errorf("building variance CSV: %w", err)
	}

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"reporting",
			struct {
				RecordsCount int `json:"recordsCount"`
			}{
				RecordsCount: len(rows),
			},
			nil,
		)
	}

	return data, nil
}

// ExportVariancePDF generates a PDF file from variance report data.
func (uc *UseCase) ExportVariancePDF(
	ctx context.Context,
	filter entities.VarianceReportFilter,
) ([]byte, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.export_variance_pdf")
	defer span.End()

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"reporting",
			struct {
				ContextID    string `json:"contextId"`
				ExportFormat string `json:"exportFormat"`
			}{
				ContextID:    filter.ContextID.String(),
				ExportFormat: "PDF",
			},
			nil,
		)
	}

	rows, err := uc.repo.ListVarianceForExport(ctx, filter, MaxPDFExportRecords)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get variance report for PDF export", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get variance report for PDF export: %v", err))

		return nil, fmt.Errorf("getting variance report for PDF export: %w", err)
	}

	data, err := exports.BuildVariancePDF(rows)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build variance PDF", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to build variance PDF: %v", err))

		return nil, fmt.Errorf("building variance PDF: %w", err)
	}

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"reporting",
			struct {
				RecordsCount int `json:"recordsCount"`
			}{
				RecordsCount: len(rows),
			},
			nil,
		)
	}

	return data, nil
}

// StreamMatchedCSV streams matched items as CSV to the provided writer.
// This avoids loading all data into memory for large exports.
func (uc *UseCase) StreamMatchedCSV(
	ctx context.Context,
	filter entities.ReportFilter,
	writer io.Writer,
) error {
	if uc.streamingRepo == nil {
		return ErrStreamingNotSupported
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.stream_matched_csv")
	defer span.End()

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"reporting",
			struct {
				ContextID    string `json:"contextId"`
				ExportFormat string `json:"exportFormat"`
				Streaming    bool   `json:"streaming"`
			}{
				ContextID:    filter.ContextID.String(),
				ExportFormat: "CSV",
				Streaming:    true,
			},
			nil,
		)
	}

	iter, err := uc.streamingRepo.StreamMatchedForExport(ctx, filter, MaxExportRecords)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to stream matched items", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to stream matched items for CSV export: %v", err))

		return fmt.Errorf("streaming matched items for CSV export: %w", err)
	}

	defer iter.Close()

	if err := exports.StreamMatchedCSV(writer, iter); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to write matched CSV", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to write matched CSV: %v", err))

		return fmt.Errorf("writing matched CSV: %w", err)
	}

	return nil
}

// StreamUnmatchedCSV streams unmatched items as CSV to the provided writer.
func (uc *UseCase) StreamUnmatchedCSV(
	ctx context.Context,
	filter entities.ReportFilter,
	writer io.Writer,
) error {
	if uc.streamingRepo == nil {
		return ErrStreamingNotSupported
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.stream_unmatched_csv")
	defer span.End()

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"reporting",
			struct {
				ContextID    string `json:"contextId"`
				ExportFormat string `json:"exportFormat"`
				Streaming    bool   `json:"streaming"`
			}{
				ContextID:    filter.ContextID.String(),
				ExportFormat: "CSV",
				Streaming:    true,
			},
			nil,
		)
	}

	iter, err := uc.streamingRepo.StreamUnmatchedForExport(ctx, filter, MaxExportRecords)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to stream unmatched items", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to stream unmatched items for CSV export: %v", err))

		return fmt.Errorf("streaming unmatched items for CSV export: %w", err)
	}

	defer iter.Close()

	if err := exports.StreamUnmatchedCSV(writer, iter); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to write unmatched CSV", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to write unmatched CSV: %v", err))

		return fmt.Errorf("writing unmatched CSV: %w", err)
	}

	return nil
}

// CountMatched returns the total count of matched records.
func (uc *UseCase) CountMatched(
	ctx context.Context,
	filter entities.ReportFilter,
) (int64, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.count_matched")
	defer span.End()

	count, err := uc.repo.CountMatched(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to count matched", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to count matched: %v", err))

		return 0, fmt.Errorf("counting matched: %w", err)
	}

	return count, nil
}

// CountUnmatched returns the total count of unmatched records.
func (uc *UseCase) CountUnmatched(
	ctx context.Context,
	filter entities.ReportFilter,
) (int64, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.count_unmatched")
	defer span.End()

	count, err := uc.repo.CountUnmatched(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to count unmatched", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to count unmatched: %v", err))

		return 0, fmt.Errorf("counting unmatched: %w", err)
	}

	return count, nil
}

// CountTransactions returns the total count of all transactions.
func (uc *UseCase) CountTransactions(
	ctx context.Context,
	filter entities.ReportFilter,
) (int64, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.count_transactions")
	defer span.End()

	count, err := uc.repo.CountTransactions(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to count transactions", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to count transactions: %v", err))

		return 0, fmt.Errorf("counting transactions: %w", err)
	}

	return count, nil
}

// CountExceptions returns the total count of exceptions.
func (uc *UseCase) CountExceptions(
	ctx context.Context,
	filter entities.ReportFilter,
) (int64, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.count_exceptions")
	defer span.End()

	count, err := uc.repo.CountExceptions(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to count exceptions", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to count exceptions: %v", err))

		return 0, fmt.Errorf("counting exceptions: %w", err)
	}

	return count, nil
}

// StreamVarianceCSV streams variance rows as CSV to the provided writer.
func (uc *UseCase) StreamVarianceCSV(
	ctx context.Context,
	filter entities.VarianceReportFilter,
	writer io.Writer,
) error {
	if uc.streamingRepo == nil {
		return ErrStreamingNotSupported
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "reporting.query.stream_variance_csv")
	defer span.End()

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"reporting",
			struct {
				ContextID    string `json:"contextId"`
				ExportFormat string `json:"exportFormat"`
				Streaming    bool   `json:"streaming"`
			}{
				ContextID:    filter.ContextID.String(),
				ExportFormat: "CSV",
				Streaming:    true,
			},
			nil,
		)
	}

	iter, err := uc.streamingRepo.StreamVarianceForExport(ctx, filter, MaxExportRecords)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to stream variance rows", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to stream variance rows for CSV export: %v", err))

		return fmt.Errorf("streaming variance rows for CSV export: %w", err)
	}

	defer iter.Close()

	if err := exports.StreamVarianceCSV(writer, iter); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to write variance CSV", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to write variance CSV: %v", err))

		return fmt.Errorf("writing variance CSV: %w", err)
	}

	return nil
}
