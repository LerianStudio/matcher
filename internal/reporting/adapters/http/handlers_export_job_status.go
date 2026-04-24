// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	"github.com/LerianStudio/lib-commons/v5/commons/pointers"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/reporting/services/query"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

var _ = sharedhttp.ErrorResponse{}

// GetExportJob handles GET /v1/export-jobs/:jobId
// @ID getExportJob
// @Summary Get export job status
// @Description Retrieves the status of an export job.
// @Tags Export Jobs
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param jobId path string true "Export Job ID" format(uuid)
// @Success 200 {object} ExportJobResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Export job not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/export-jobs/{jobId} [get]
func (handler *ExportJobHandlers) GetExportJob(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startExportJobSpan(fiberCtx, "handler.export_job.get")

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

	response := handler.mapJobToResponse(ctx, job)

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}

// ListExportJobs handles GET /v1/export-jobs
// @ID listExportJobs
// @Summary List export jobs
// @Description Lists export jobs for the authenticated tenant using cursor-based pagination.
// @Description Use the nextCursor value from the response to fetch subsequent pages.
// @Tags Export Jobs
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param status query string false "Filter by status (QUEUED, RUNNING, SUCCEEDED, FAILED, EXPIRED, CANCELED)"
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Success 200 {object} ExportJobListResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/export-jobs [get]
func (handler *ExportJobHandlers) ListExportJobs(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startExportJobSpan(fiberCtx, "handler.export_job.list")

	defer span.End()

	cursor, limit, err := parseTimestampCursorPagination(fiberCtx)
	if err != nil {
		return handler.badRequestBiz(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
	}

	var status *string
	if s := fiberCtx.Query("status"); s != "" {
		status = pointers.String(s)
	}

	jobs, pagination, err := handler.exportJobRepo.List(ctx, status, cursor, limit)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to list export jobs", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	responses := make([]*ExportJobResponse, len(jobs))
	for i, job := range jobs {
		responses[i] = handler.mapJobToResponse(ctx, job)
	}

	response := ExportJobListResponse{
		Items:      responses,
		NextCursor: pagination.Next,
		Limit:      limit,
		HasMore:    pagination.Next != "",
	}

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}

func parseTimestampCursorPagination(fiberCtx *fiber.Ctx) (*libHTTP.TimestampCursor, int, error) {
	cursor, limit, err := libHTTP.ParseTimestampCursorPagination(fiberCtx)
	if err != nil {
		return nil, 0, fmt.Errorf("parse timestamp cursor pagination: %w", err)
	}

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	return cursor, limit, nil
}

// ListExportJobsByContext handles GET /v1/contexts/:contextId/export-jobs
// @ID listExportJobsByContext
// @Summary List export jobs by context
// @Description Lists export jobs for a specific reconciliation context using cursor-based pagination.
// @Description Use the nextCursor value from the response to fetch subsequent pages.
// @Tags Export Jobs
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param cursor query string false "Pagination cursor from previous response"
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Success 200 {object} ExportJobListResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId}/export-jobs [get]
func (handler *ExportJobHandlers) ListExportJobsByContext(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startExportJobSpan(fiberCtx, "handler.export_job.list_by_context")

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

	cursor, limit, err := parseTimestampCursorPagination(fiberCtx)
	if err != nil {
		return handler.badRequestBiz(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
	}

	jobs, pagination, err := handler.exportJobRepo.ListByContext(ctx, contextID, cursor, limit)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to list export jobs by context", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	responses := make([]*ExportJobResponse, len(jobs))
	for i, job := range jobs {
		responses[i] = handler.mapJobToResponse(ctx, job)
	}

	response := ExportJobListResponse{
		Items:      responses,
		NextCursor: pagination.Next,
		Limit:      limit,
		HasMore:    pagination.Next != "",
	}

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}
