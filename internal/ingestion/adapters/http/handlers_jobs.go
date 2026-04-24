// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/ingestion/adapters/http/dto"
	ingestionRepositories "github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	"github.com/LerianStudio/matcher/internal/ingestion/services/query"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

// GetJob handles GET /v1/imports/contexts/:contextId/jobs/:jobId
// @Summary Get ingestion job status
// @Description Retrieves the status and details of an ingestion job by its ID, including progress metrics and error information.
// @ID getIngestionJob
// @Tags Ingestion
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param jobId path string true "Job ID" format(uuid)
// @Success 200 {object} dto.JobResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Job not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/imports/contexts/{contextId}/jobs/{jobId} [get]
func (handler *Handlers) GetJob(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.ingestion.get_job")
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

	jobID, err := uuid.Parse(fiberCtx.Params("jobId"))
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid job_id", err)
	}

	job, err := handler.queryUC.GetJobByContext(ctx, contextID, jobID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || errors.Is(err, query.ErrJobNotFound) {
			return handler.notFound(ctx, fiberCtx, span, logger, "ingestion_job_not_found", "job not found", err)
		}

		handler.logSpanError(ctx, span, logger, "failed to get job", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.JobToResponse(job)); err != nil {
		return fmt.Errorf("respond get ingestion job: %w", err)
	}

	return nil
}

// ListJobsByContext handles GET /v1/imports/contexts/:contextId/jobs
// @Summary List ingestion jobs for a context
// @Description Returns a cursor-paginated list of ingestion jobs for a reconciliation context, with optional sorting.
// @ID listIngestionJobs
// @Tags Ingestion
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param sort_order query string false "Sort order" Enums(asc,desc)
// @Param sort_by query string false "Sort field" Enums(id,created_at,started_at,completed_at,status)
// @Success 200 {object} dto.ListJobsResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/imports/contexts/{contextId}/jobs [get]
func (handler *Handlers) ListJobsByContext(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.ingestion.list_jobs")
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

	cursor, limit, err := libHTTP.ParseOpaqueCursorPagination(fiberCtx)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
	}

	cursor = strings.TrimSpace(cursor)

	sortOrder := strings.TrimSpace(fiberCtx.Query("sort_order"))
	if sortOrder == "" {
		sortOrder = sortOrderDesc
	}

	sortOrder = strings.ToLower(sortOrder)
	if sortOrder != "asc" && sortOrder != sortOrderDesc {
		return handler.badRequest(
			ctx,
			fiberCtx,
			span,
			logger,
			"invalid sort_order: must be asc or desc",
			ErrInvalidSortOrder,
		)
	}

	sortBy := strings.TrimSpace(fiberCtx.Query("sort_by"))
	if sortBy != "" && !validJobSortColumns[sortBy] {
		return handler.badRequest(
			ctx,
			fiberCtx,
			span,
			logger,
			"invalid sort_by: must be one of id, created_at, started_at, completed_at, status",
			ErrInvalidSortBy,
		)
	}

	jobs, pagination, err := handler.jobRepo.FindByContextID(
		ctx,
		contextID,
		ingestionRepositories.CursorFilter{
			Limit:     limit,
			Cursor:    cursor,
			SortBy:    sortBy,
			SortOrder: sortOrder,
		},
	)
	if err != nil {
		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
		}

		handler.logSpanError(ctx, span, logger, "failed to list jobs", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	items := dto.JobsToResponse(jobs)

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ListJobsResponse{
		Items: items,
		CursorResponse: sharedhttp.CursorResponse{
			NextCursor: pagination.Next,
			PrevCursor: pagination.Prev,
			Limit:      limit,
			HasMore:    pagination.Next != "",
		},
	}); err != nil {
		return fmt.Errorf("respond list ingestion jobs: %w", err)
	}

	return nil
}

// ListTransactionsByJob handles GET /v1/imports/contexts/:contextId/jobs/:jobId/transactions
// @Summary List transactions for a job
// @Description Returns a cursor-paginated list of transactions extracted from an ingestion job, with optional sorting.
// @ID listJobTransactions
// @Tags Ingestion
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param jobId path string true "Job ID" format(uuid)
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param sort_order query string false "Sort order" Enums(asc,desc)
// @Param sort_by query string false "Sort field" Enums(id,created_at,date,status,extraction_status)
// @Success 200 {object} dto.ListTransactionsResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Job not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/imports/contexts/{contextId}/jobs/{jobId}/transactions [get]
func (handler *Handlers) ListTransactionsByJob(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.ingestion.list_transactions")
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

	jobID, err := uuid.Parse(fiberCtx.Params("jobId"))
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid job_id", err)
	}

	cursor, limit, err := libHTTP.ParseOpaqueCursorPagination(fiberCtx)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
	}

	cursor = strings.TrimSpace(cursor)

	sortOrder, err := parseSortOrder(fiberCtx)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid sort_order: must be asc or desc", err)
	}

	sortBy := strings.TrimSpace(fiberCtx.Query("sort_by"))
	if sortBy != "" && !validTransactionSortColumns[sortBy] {
		return handler.badRequest(
			ctx,
			fiberCtx,
			span,
			logger,
			"invalid sort_by: must be one of id, created_at, date, status, extraction_status",
			ErrInvalidSortBy,
		)
	}

	job, err := handler.queryUC.GetJobByContext(ctx, contextID, jobID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || errors.Is(err, query.ErrJobNotFound) {
			return handler.notFound(ctx, fiberCtx, span, logger, "ingestion_job_not_found", "job not found", err)
		}

		handler.logSpanError(ctx, span, logger, "failed to get job", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	transactions, pagination, err := handler.transactionRepo.FindByJobAndContextID(
		ctx,
		job.ID,
		contextID,
		ingestionRepositories.CursorFilter{
			Limit:     limit,
			Cursor:    cursor,
			SortBy:    sortBy,
			SortOrder: sortOrder,
		},
	)
	if err != nil {
		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
		}

		handler.logSpanError(ctx, span, logger, "failed to list transactions", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	items := dto.TransactionsToResponse(transactions, jobID, contextID)

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ListTransactionsResponse{
		Items: items,
		CursorResponse: sharedhttp.CursorResponse{
			NextCursor: pagination.Next,
			PrevCursor: pagination.Prev,
			Limit:      limit,
			HasMore:    pagination.Next != "",
		},
	}); err != nil {
		return fmt.Errorf("respond list transactions: %w", err)
	}

	return nil
}
