// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"context"
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

var _ = sharedhttp.ErrorResponse{}

// exportFn produces export data for a given format.
// Return ErrInvalidExportFormat for unsupported formats.
type exportFn func(ctx context.Context, filter entities.ReportFilter, format string) ([]byte, string, string, error)

func (handler *Handlers) handleExport(
	fiberCtx *fiber.Ctx,
	spanName string,
	fn exportFn,
) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, spanName)
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handler.handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	filter, err := parseReportFilter(fiberCtx, contextID)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	format := fiberCtx.Query("format", formatCSV)

	data, contentType, filename, err := fn(ctx, filter, format)
	if err != nil {
		if errors.Is(err, ErrInvalidExportFormat) {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid format", err)
		}

		handler.logSpanError(ctx, span, logger, "failed to export report", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	fiberCtx.Set("Content-Type", contentType)
	fiberCtx.Set("Content-Disposition", contentDispoFmt+filename+"\"")

	return fiberCtx.Send(data)
}

// ExportMatchedReport handles GET /v1/reports/contexts/:contextId/matched/export
// @ID exportMatchedReport
// @Summary Export matched transactions report
// @Description Exports matched transactions report in CSV or PDF format for the specified date range.
// @Tags Reporting
// @Produce text/csv,application/pdf,application/json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Param format query string false "Export format (csv or pdf)" default(csv)
// @Success 200 {file} file
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/matched/export [get]
func (handler *Handlers) ExportMatchedReport(fiberCtx *fiber.Ctx) error {
	return handler.handleExport(
		fiberCtx,
		"handler.reporting.export_matched_report",
		func(ctx context.Context, filter entities.ReportFilter, format string) ([]byte, string, string, error) {
			switch format {
			case formatCSV:
				data, err := handler.exportUC.ExportMatchedCSV(ctx, filter)
				if err != nil {
					return nil, "", "", fmt.Errorf("export matched csv: %w", err)
				}

				return data, contentTypeCSV, "matched_report.csv", nil
			case formatPDF:
				data, err := handler.exportUC.ExportMatchedPDF(ctx, filter)
				if err != nil {
					return nil, "", "", fmt.Errorf("export matched pdf: %w", err)
				}

				return data, contentTypePDF, "matched_report.pdf", nil
			default:
				return nil, "", "", ErrInvalidExportFormat
			}
		},
	)
}

// ExportUnmatchedReport handles GET /v1/reports/contexts/:contextId/unmatched/export
// @ID exportUnmatchedReport
// @Summary Export unmatched transactions report
// @Description Exports unmatched transactions report in CSV or PDF format for the specified date range.
// @Tags Reporting
// @Produce text/csv,application/pdf,application/json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Param format query string false "Export format (csv or pdf)" default(csv)
// @Success 200 {file} file
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/unmatched/export [get]
func (handler *Handlers) ExportUnmatchedReport(fiberCtx *fiber.Ctx) error {
	return handler.handleExport(
		fiberCtx,
		"handler.reporting.export_unmatched_report",
		func(ctx context.Context, filter entities.ReportFilter, format string) ([]byte, string, string, error) {
			switch format {
			case formatCSV:
				data, err := handler.exportUC.ExportUnmatchedCSV(ctx, filter)
				if err != nil {
					return nil, "", "", fmt.Errorf("export unmatched csv: %w", err)
				}

				return data, contentTypeCSV, "unmatched_report.csv", nil
			case formatPDF:
				data, err := handler.exportUC.ExportUnmatchedPDF(ctx, filter)
				if err != nil {
					return nil, "", "", fmt.Errorf("export unmatched pdf: %w", err)
				}

				return data, contentTypePDF, "unmatched_report.pdf", nil
			default:
				return nil, "", "", ErrInvalidExportFormat
			}
		},
	)
}

// ExportSummaryReport handles GET /v1/reports/contexts/:contextId/summary/export
// @ID exportSummaryReport
// @Summary Export summary report
// @Description Exports reconciliation summary report in CSV or PDF format for the specified date range.
// @Tags Reporting
// @Produce text/csv,application/pdf,application/json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Param format query string false "Export format (csv or pdf)" default(csv)
// @Success 200 {file} file
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/summary/export [get]
func (handler *Handlers) ExportSummaryReport(fiberCtx *fiber.Ctx) error {
	return handler.handleExport(
		fiberCtx,
		"handler.reporting.export_summary_report",
		func(ctx context.Context, filter entities.ReportFilter, format string) ([]byte, string, string, error) {
			switch format {
			case formatCSV:
				data, err := handler.exportUC.ExportSummaryCSV(ctx, filter)
				if err != nil {
					return nil, "", "", fmt.Errorf("export summary csv: %w", err)
				}

				return data, contentTypeCSV, "summary_report.csv", nil
			case formatPDF:
				data, err := handler.exportUC.ExportSummaryPDF(ctx, filter)
				if err != nil {
					return nil, "", "", fmt.Errorf("export summary pdf: %w", err)
				}

				return data, contentTypePDF, "summary_report.pdf", nil
			default:
				return nil, "", "", ErrInvalidExportFormat
			}
		},
	)
}

// ExportVarianceReport handles GET /v1/reports/contexts/:contextId/variance/export
// @ID exportVarianceReport
// @Summary Export variance report
// @Description Exports variance analysis report in CSV or PDF format for the specified date range.
// @Tags Reporting
// @Produce text/csv,application/pdf,application/json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Param format query string false "Export format (csv or pdf)" default(csv)
// @Success 200 {file} file
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/variance/export [get]
func (handler *Handlers) ExportVarianceReport(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.reporting.export_variance_report")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handler.handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	filter, err := parseVarianceReportFilter(fiberCtx, contextID)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	format := fiberCtx.Query("format", formatCSV)

	switch format {
	case formatCSV:
		fiberCtx.Set("Content-Type", contentTypeCSV)
		fiberCtx.Set("Content-Disposition", contentDispoFmt+"variance_report.csv\"")

		data, err := handler.exportUC.ExportVarianceCSV(ctx, filter)
		if err != nil {
			handler.logSpanError(ctx, span, logger, "failed to export variance CSV", err)

			return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
		}

		return fiberCtx.Send(data)

	case formatPDF:
		data, err := handler.exportUC.ExportVariancePDF(ctx, filter)
		if err != nil {
			handler.logSpanError(ctx, span, logger, "failed to export variance PDF", err)

			return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
		}

		fiberCtx.Set("Content-Type", contentTypePDF)
		fiberCtx.Set("Content-Disposition", contentDispoFmt+"variance_report.pdf\"")

		return fiberCtx.Send(data)

	default:
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid format", ErrInvalidExportFormat)
	}
}
