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
			sharedObservability.NewMatcherRedactor(),
		)
	}

	items, err := uc.repo.ListMatchedForExport(ctx, filter, MaxPDFExportRecords)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list matched items for PDF export", err)

		libLog.SafeError(logger, ctx, "failed to list matched items for PDF export", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("listing matched items for PDF export: %w", err)
	}

	data, err := exports.BuildMatchedPDF(items)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build matched PDF", err)

		libLog.SafeError(logger, ctx, "failed to build matched PDF", err, runtime.IsProductionMode())

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
			sharedObservability.NewMatcherRedactor(),
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
			sharedObservability.NewMatcherRedactor(),
		)
	}

	items, err := uc.repo.ListUnmatchedForExport(ctx, filter, MaxPDFExportRecords)
	if err != nil {
		libOpentelemetry.HandleSpanError(
			span,
			"failed to list unmatched items for PDF export",
			err,
		)

		libLog.SafeError(logger, ctx, "failed to list unmatched items for PDF export", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("listing unmatched items for PDF export: %w", err)
	}

	data, err := exports.BuildUnmatchedPDF(items)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build unmatched PDF", err)

		libLog.SafeError(logger, ctx, "failed to build unmatched PDF", err, runtime.IsProductionMode())

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
			sharedObservability.NewMatcherRedactor(),
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

		libLog.SafeError(logger, ctx, "failed to get summary for PDF export", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("getting summary for PDF export: %w", err)
	}

	data, err := exports.BuildSummaryPDF(summary)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build summary PDF", err)

		libLog.SafeError(logger, ctx, "failed to build summary PDF", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("building summary PDF: %w", err)
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
			sharedObservability.NewMatcherRedactor(),
		)
	}

	rows, err := uc.repo.ListVarianceForExport(ctx, filter, MaxPDFExportRecords)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get variance report for PDF export", err)

		libLog.SafeError(logger, ctx, "failed to get variance report for PDF export", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("getting variance report for PDF export: %w", err)
	}

	data, err := exports.BuildVariancePDF(rows)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build variance PDF", err)

		libLog.SafeError(logger, ctx, "failed to build variance PDF", err, runtime.IsProductionMode())

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
			sharedObservability.NewMatcherRedactor(),
		)
	}

	return data, nil
}
