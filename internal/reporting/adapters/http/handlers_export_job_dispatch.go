// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"errors"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/reporting/services/command"
	"github.com/LerianStudio/matcher/internal/reporting/services/query"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

var _ = sharedhttp.ErrorResponse{}

// CancelExportJob handles POST /v1/export-jobs/:jobId/cancel
// @ID cancelExportJob
// @Summary Cancel an export job
// @Description Cancels a queued or running export job.
// @Tags Export Jobs
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param jobId path string true "Export Job ID" format(uuid)
// @Success 200 {object} ExportJobResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Export job not found"
// @Failure 409 {object} sharedhttp.ErrorResponse "Job in terminal state or idempotency conflict"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/export-jobs/{jobId}/cancel [post]
func (handler *ExportJobHandlers) CancelExportJob(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startExportJobSpan(fiberCtx, "handler.export_job.cancel")

	defer span.End()

	jobIDStr := fiberCtx.Params("jobId")

	jobID, err := uuid.Parse(jobIDStr)
	if err != nil {
		return handler.badRequestBiz(ctx, fiberCtx, span, logger, "invalid job ID", ErrInvalidJobID)
	}

	existingJob, err := handler.querySvc.GetByID(ctx, jobID)
	if err != nil {
		if errors.Is(err, query.ErrExportJobNotFound) {
			return handler.notFoundBiz(ctx, fiberCtx, span, logger, err)
		}

		handler.logSpanError(ctx, span, logger, "failed to get export job for cancel", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if existingJob == nil {
		return handler.notFoundBiz(ctx, fiberCtx, span, logger, query.ErrExportJobNotFound)
	}

	if err := verifyJobTenantOwnership(ctx, existingJob); err != nil {
		return handler.notFoundBiz(ctx, fiberCtx, span, logger, err)
	}

	if err := handler.exportJobUC.CancelExportJob(ctx, jobID); err != nil {
		if errors.Is(err, command.ErrExportJobNotFound) {
			return handler.notFoundBiz(ctx, fiberCtx, span, logger, err)
		}

		if errors.Is(err, command.ErrJobInTerminalState) {
			handler.logSpanBusinessEvent(ctx, span, logger, "job already in terminal state", err)

			return respondError(
				fiberCtx,
				fiber.StatusConflict,
				"conflict",
				"job is already in a terminal state",
			)
		}

		handler.logSpanError(ctx, span, logger, "failed to cancel export job", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	job, err := handler.querySvc.GetByID(ctx, jobID)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to get cancelled job", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if job == nil {
		handler.logSpanError(ctx, span, logger, "cancelled job unexpectedly nil", nil)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	response := handler.mapJobToResponse(ctx, job)

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}

// DownloadExportJob handles GET /v1/export-jobs/:jobId/download
// @ID downloadExportJob
// @Summary Download export file
// @Description Returns a presigned URL or redirects to download the export file.
// @Tags Export Jobs
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param jobId path string true "Export Job ID" format(uuid)
// @Success 200 {object} DownloadExportJobResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Export job not found"
// @Failure 409 {object} sharedhttp.ErrorResponse "Job not ready for download"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/export-jobs/{jobId}/download [get]
func (handler *ExportJobHandlers) DownloadExportJob(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startExportJobSpan(fiberCtx, "handler.export_job.download")

	defer span.End()

	jobIDStr := fiberCtx.Params("jobId")

	jobID, err := uuid.Parse(jobIDStr)
	if err != nil {
		return handler.badRequestBiz(ctx, fiberCtx, span, logger, "invalid job ID", ErrInvalidJobID)
	}

	job, err := handler.querySvc.GetByID(ctx, jobID)
	if err != nil {
		if errors.Is(err, query.ErrExportJobNotFound) {
			return handler.notFoundBiz(ctx, fiberCtx, span, logger, err)
		}

		handler.logSpanError(ctx, span, logger, "failed to get export job", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if job == nil {
		handler.logSpanError(ctx, span, logger, "export job unexpectedly nil", nil)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if err := verifyJobTenantOwnership(ctx, job); err != nil {
		return handler.notFoundBiz(ctx, fiberCtx, span, logger, err)
	}

	if !job.IsDownloadable() {
		handler.logSpanBusinessEvent(ctx, span, logger, "job not downloadable", ErrJobNotDownloadable)

		return respondError(
			fiberCtx,
			fiber.StatusConflict,
			"not_ready",
			"export job is not ready for download",
		)
	}

	if time.Now().After(job.ExpiresAt) {
		return respondError(
			fiberCtx,
			fiber.StatusGone,
			"expired",
			"export file has expired",
		)
	}

	runtimeConfig := handler.currentRuntimeConfigForContext(ctx)

	downloadURL, err := handler.storage.GeneratePresignedURL(ctx, job.FileKey, runtimeConfig.PresignExpiry)
	if err != nil {
		if errors.Is(err, sharedPorts.ErrObjectStorageUnavailable) {
			return respondError(fiberCtx, fiber.StatusServiceUnavailable, "object_storage_unavailable", "export storage is unavailable")
		}

		handler.logSpanError(ctx, span, logger, "failed to generate download URL", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, DownloadExportJobResponse{
		DownloadURL: downloadURL,
		FileName:    job.FileName,
		SHA256:      job.SHA256,
		ExpiresIn:   int(runtimeConfig.PresignExpiry.Seconds()),
	}); err != nil {
		return fmt.Errorf("respond download export job: %w", err)
	}

	return nil
}
