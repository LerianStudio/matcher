// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package query

import (
	"context"
	"fmt"
	"io"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/services/query/exports"
	sharedObservability "github.com/LerianStudio/matcher/internal/shared/observability"
)

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
			sharedObservability.NewMatcherRedactor(),
		)
	}

	iter, err := uc.streamingRepo.StreamMatchedForExport(ctx, filter, MaxExportRecords)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to stream matched items", err)

		libLog.SafeError(logger, ctx, "failed to stream matched items for CSV export", err, runtime.IsProductionMode())

		return fmt.Errorf("streaming matched items for CSV export: %w", err)
	}

	defer iter.Close()

	if err := exports.StreamMatchedCSV(writer, iter); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to write matched CSV", err)

		libLog.SafeError(logger, ctx, "failed to write matched CSV", err, runtime.IsProductionMode())

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
			sharedObservability.NewMatcherRedactor(),
		)
	}

	iter, err := uc.streamingRepo.StreamUnmatchedForExport(ctx, filter, MaxExportRecords)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to stream unmatched items", err)

		libLog.SafeError(logger, ctx, "failed to stream unmatched items for CSV export", err, runtime.IsProductionMode())

		return fmt.Errorf("streaming unmatched items for CSV export: %w", err)
	}

	defer iter.Close()

	if err := exports.StreamUnmatchedCSV(writer, iter); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to write unmatched CSV", err)

		libLog.SafeError(logger, ctx, "failed to write unmatched CSV", err, runtime.IsProductionMode())

		return fmt.Errorf("writing unmatched CSV: %w", err)
	}

	return nil
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
			sharedObservability.NewMatcherRedactor(),
		)
	}

	iter, err := uc.streamingRepo.StreamVarianceForExport(ctx, filter, MaxExportRecords)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to stream variance rows", err)

		libLog.SafeError(logger, ctx, "failed to stream variance rows for CSV export", err, runtime.IsProductionMode())

		return fmt.Errorf("streaming variance rows for CSV export: %w", err)
	}

	defer iter.Close()

	if err := exports.StreamVarianceCSV(writer, iter); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to write variance CSV", err)

		libLog.SafeError(logger, ctx, "failed to write variance CSV", err, runtime.IsProductionMode())

		return fmt.Errorf("writing variance CSV: %w", err)
	}

	return nil
}
