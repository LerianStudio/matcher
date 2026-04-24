// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//nolint:wrapcheck // internal package streaming writers are tightly coupled
package worker

import (
	"context"
	"fmt"
	"io"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/services/query/exports"
)

func (worker *ExportWorker) streamUnmatched(
	ctx context.Context,
	job *entities.ExportJob,
	filter entities.ReportFilter,
	writer io.Writer,
) (int64, error) {
	switch job.Format {
	case entities.ExportFormatCSV:
		return worker.streamUnmatchedCSV(ctx, job, filter, writer)
	case entities.ExportFormatJSON:
		return worker.streamUnmatchedJSON(ctx, job, filter, writer)
	case entities.ExportFormatXML:
		return worker.streamUnmatchedXML(ctx, job, filter, writer)
	default:
		return 0, fmt.Errorf("%w: %s", ErrUnsupportedFormat, job.Format)
	}
}

func (worker *ExportWorker) streamUnmatchedCSV(
	ctx context.Context,
	job *entities.ExportJob,
	filter entities.ReportFilter,
	writer io.Writer,
) (int64, error) {
	csvWriter := exports.NewStreamingCSVWriter(writer)

	if err := csvWriter.WriteUnmatchedHeader(); err != nil {
		return 0, err
	}

	var afterKey string

	for {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("export cancelled: %w", err)
		}

		items, nextKey, err := worker.reportRepo.ListUnmatchedPage(
			ctx,
			filter,
			afterKey,
			worker.cfg.PageSize,
		)
		if err != nil {
			return 0, fmt.Errorf("fetching unmatched page: %w", err)
		}

		for _, item := range items {
			if err := csvWriter.WriteUnmatchedRow(item); err != nil {
				return 0, err
			}
		}

		if csvWriter.RecordCount()%progressUpdateEvery == 0 {
			_ = worker.jobRepo.UpdateProgress(ctx, job.ID, csvWriter.RecordCount(), 0)
		}

		if nextKey == "" {
			break
		}

		afterKey = nextKey
	}

	_ = worker.jobRepo.UpdateProgress(ctx, job.ID, csvWriter.RecordCount(), 0)

	if err := csvWriter.Flush(); err != nil {
		return 0, err
	}

	return csvWriter.RecordCount(), nil
}

func (worker *ExportWorker) streamUnmatchedJSON(
	ctx context.Context,
	job *entities.ExportJob,
	filter entities.ReportFilter,
	writer io.Writer,
) (int64, error) {
	jsonWriter := exports.NewStreamingJSONWriter(writer)

	if err := jsonWriter.WriteArrayStart(); err != nil {
		return 0, err
	}

	var afterKey string

	for {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("export cancelled: %w", err)
		}

		items, nextKey, err := worker.reportRepo.ListUnmatchedPage(
			ctx,
			filter,
			afterKey,
			worker.cfg.PageSize,
		)
		if err != nil {
			return 0, fmt.Errorf("fetching unmatched page: %w", err)
		}

		for _, item := range items {
			if err := jsonWriter.WriteRow(item); err != nil {
				return 0, err
			}
		}

		if jsonWriter.RecordCount()%progressUpdateEvery == 0 {
			_ = worker.jobRepo.UpdateProgress(ctx, job.ID, jsonWriter.RecordCount(), 0)
		}

		if nextKey == "" {
			break
		}

		afterKey = nextKey
	}

	_ = worker.jobRepo.UpdateProgress(ctx, job.ID, jsonWriter.RecordCount(), 0)

	if err := jsonWriter.WriteArrayEnd(); err != nil {
		return 0, err
	}

	return jsonWriter.RecordCount(), nil
}

func (worker *ExportWorker) streamUnmatchedXML(
	ctx context.Context,
	job *entities.ExportJob,
	filter entities.ReportFilter,
	writer io.Writer,
) (int64, error) {
	xmlWriter := exports.NewStreamingXMLWriter(writer)

	if err := xmlWriter.WriteHeader("unmatchedItems"); err != nil {
		return 0, err
	}

	var afterKey string

	for {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("export cancelled: %w", err)
		}

		items, nextKey, err := worker.reportRepo.ListUnmatchedPage(
			ctx,
			filter,
			afterKey,
			worker.cfg.PageSize,
		)
		if err != nil {
			return 0, fmt.Errorf("fetching unmatched page: %w", err)
		}

		for _, item := range items {
			if err := xmlWriter.WriteRow("item", item); err != nil {
				return 0, err
			}
		}

		if xmlWriter.RecordCount()%progressUpdateEvery == 0 {
			_ = worker.jobRepo.UpdateProgress(ctx, job.ID, xmlWriter.RecordCount(), 0)
		}

		if nextKey == "" {
			break
		}

		afterKey = nextKey
	}

	_ = worker.jobRepo.UpdateProgress(ctx, job.ID, xmlWriter.RecordCount(), 0)

	if err := xmlWriter.WriteFooter("unmatchedItems"); err != nil {
		return 0, err
	}

	if err := xmlWriter.Flush(); err != nil {
		return 0, err
	}

	return xmlWriter.RecordCount(), nil
}
