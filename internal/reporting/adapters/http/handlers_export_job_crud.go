// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/services/command"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

var _ = sharedhttp.ErrorResponse{}

// CreateExportJob handles POST /v1/contexts/:contextId/export-jobs
// @ID createExportJob
// @Summary Create an export job
// @Description Creates an async export job for large report exports (CSV, JSON, XML).
// @Tags Export Jobs
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param request body CreateExportJobRequest true "Export job parameters"
// @Success 202 {object} CreateExportJobResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 409 {object} sharedhttp.ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 503 {object} sharedhttp.ErrorResponse "Export worker disabled"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId}/export-jobs [post]
func (handler *ExportJobHandlers) CreateExportJob(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startExportJobSpan(fiberCtx, "handler.export_job.create")

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

	runtimeConfig := handler.currentRuntimeConfigForContext(ctx)
	if runtimeConfig.Enabled != nil && !*runtimeConfig.Enabled {
		return respondError(
			fiberCtx,
			fiber.StatusServiceUnavailable,
			"export_worker_disabled",
			ErrExportWorkerDisabled.Error(),
		)
	}

	var req CreateExportJobRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return handler.badRequestBiz(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	parsed, msg, err := parseExportJobRequest(&req)
	if err != nil {
		if errors.Is(err, entities.ErrInvalidExportFormat) ||
			errors.Is(err, ErrPDFNotSupportedAsync) ||
			errors.Is(err, ErrSummaryNotSupportedAsync) ||
			errors.Is(err, ErrExceptionsNotSupportedAsync) {
			return handler.badRequestBizWithSlug(ctx, fiberCtx, span, logger, "reporting_invalid_export_format", msg, err)
		}

		return handler.badRequestBiz(ctx, fiberCtx, span, logger, msg, err)
	}

	input := command.CreateExportJobInput{
		TenantID:   tenantID,
		ContextID:  contextID,
		ReportType: parsed.reportType,
		Format:     parsed.format,
		Filter: entities.ExportJobFilter{
			DateFrom: parsed.dateFrom,
			DateTo:   parsed.dateTo,
			SourceID: parsed.sourceID,
		},
	}

	output, err := handler.exportJobUC.CreateExportJob(ctx, input)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to create export job", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	response := CreateExportJobResponse{
		JobID:     output.JobID.String(),
		Status:    string(output.Status),
		StatusURL: output.StatusURL,
	}

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusAccepted, response); writeErr != nil {
		return fmt.Errorf("write accepted response: %w", writeErr)
	}

	return nil
}
