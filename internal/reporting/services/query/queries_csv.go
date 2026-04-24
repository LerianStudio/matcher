// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package query

import (
	"context"
	"fmt"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/services/query/exports"
	sharedObservability "github.com/LerianStudio/matcher/internal/shared/observability"
)

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
			sharedObservability.NewMatcherRedactor(),
		)
	}

	items, err := uc.repo.ListMatchedForExport(ctx, filter, MaxExportRecords)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list matched items for CSV export", err)

		libLog.SafeError(logger, ctx, "failed to list matched items for CSV export", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("listing matched items for CSV export: %w", err)
	}

	data, err := exports.BuildMatchedCSV(items)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build matched CSV", err)

		libLog.SafeError(logger, ctx, "failed to build matched CSV", err, runtime.IsProductionMode())

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
			sharedObservability.NewMatcherRedactor(),
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
			sharedObservability.NewMatcherRedactor(),
		)
	}

	items, err := uc.repo.ListUnmatchedForExport(ctx, filter, MaxExportRecords)
	if err != nil {
		libOpentelemetry.HandleSpanError(
			span,
			"failed to list unmatched items for CSV export",
			err,
		)

		libLog.SafeError(logger, ctx, "failed to list unmatched items for CSV export", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("listing unmatched items for CSV export: %w", err)
	}

	data, err := exports.BuildUnmatchedCSV(items)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build unmatched CSV", err)

		libLog.SafeError(logger, ctx, "failed to build unmatched CSV", err, runtime.IsProductionMode())

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
			sharedObservability.NewMatcherRedactor(),
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

		libLog.SafeError(logger, ctx, "failed to get summary for CSV export", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("getting summary for CSV export: %w", err)
	}

	data, err := exports.BuildSummaryCSV(summary)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build summary CSV", err)

		libLog.SafeError(logger, ctx, "failed to build summary CSV", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("building summary CSV: %w", err)
	}

	return data, nil
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
			sharedObservability.NewMatcherRedactor(),
		)
	}

	rows, err := uc.repo.ListVarianceForExport(ctx, filter, MaxExportRecords)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get variance report for CSV export", err)

		libLog.SafeError(logger, ctx, "failed to get variance report for CSV export", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("getting variance report for CSV export: %w", err)
	}

	data, err := exports.BuildVarianceCSV(rows)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build variance CSV", err)

		libLog.SafeError(logger, ctx, "failed to build variance CSV", err, runtime.IsProductionMode())

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
			sharedObservability.NewMatcherRedactor(),
		)
	}

	return data, nil
}
