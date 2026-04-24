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

func (worker *ExportWorker) streamMatched(
	ctx context.Context,
	job *entities.ExportJob,
	filter entities.ReportFilter,
	writer io.Writer,
) (int64, error) {
	switch job.Format {
	case entities.ExportFormatCSV:
		return worker.streamMatchedCSV(ctx, job, filter, writer)
	case entities.ExportFormatJSON:
		return worker.streamMatchedJSON(ctx, job, filter, writer)
	case entities.ExportFormatXML:
		return worker.streamMatchedXML(ctx, job, filter, writer)
	default:
		return 0, fmt.Errorf("%w: %s", ErrUnsupportedFormat, job.Format)
	}
}

func (worker *ExportWorker) streamMatchedCSV(
	ctx context.Context,
	job *entities.ExportJob,
	filter entities.ReportFilter,
	writer io.Writer,
) (int64, error) {
	csvWriter := exports.NewStreamingCSVWriter(writer)

	if err := csvWriter.WriteMatchedHeader(); err != nil {
		return 0, err
	}

	var afterKey string

	for {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("export cancelled: %w", err)
		}

		items, nextKey, err := worker.reportRepo.ListMatchedPage(
			ctx,
			filter,
			afterKey,
			worker.cfg.PageSize,
		)
		if err != nil {
			return 0, fmt.Errorf("fetching matched page: %w", err)
		}

		for _, item := range items {
			if err := csvWriter.WriteMatchedRow(item); err != nil {
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

func (worker *ExportWorker) streamMatchedJSON(
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

		items, nextKey, err := worker.reportRepo.ListMatchedPage(
			ctx,
			filter,
			afterKey,
			worker.cfg.PageSize,
		)
		if err != nil {
			return 0, fmt.Errorf("fetching matched page: %w", err)
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

func (worker *ExportWorker) streamMatchedXML(
	ctx context.Context,
	job *entities.ExportJob,
	filter entities.ReportFilter,
	writer io.Writer,
) (int64, error) {
	xmlWriter := exports.NewStreamingXMLWriter(writer)

	if err := xmlWriter.WriteHeader("matchedItems"); err != nil {
		return 0, err
	}

	var afterKey string

	for {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("export cancelled: %w", err)
		}

		items, nextKey, err := worker.reportRepo.ListMatchedPage(
			ctx,
			filter,
			afterKey,
			worker.cfg.PageSize,
		)
		if err != nil {
			return 0, fmt.Errorf("fetching matched page: %w", err)
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

	if err := xmlWriter.WriteFooter("matchedItems"); err != nil {
		return 0, err
	}

	if err := xmlWriter.Flush(); err != nil {
		return 0, err
	}

	return xmlWriter.RecordCount(), nil
}
