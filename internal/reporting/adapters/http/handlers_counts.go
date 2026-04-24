// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"context"
	"fmt"

	"github.com/gofiber/fiber/v2"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/reporting/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

var _ = sharedhttp.ErrorResponse{}

// countFn produces a count for a given report filter.
type countFn func(ctx context.Context, filter entities.ReportFilter) (int64, error)

func (handler *Handlers) handleCount(
	fiberCtx *fiber.Ctx,
	spanName string,
	fn countFn,
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

	count, err := fn(ctx, filter)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to count records", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ExportCountResponse{Count: count}); err != nil {
		return fmt.Errorf("respond export count: %w", err)
	}

	return nil
}

// CountMatched handles GET /v1/reports/contexts/:contextId/matches/count
// @ID countMatched
// @Summary Count matched records
// @Description Returns the total count of matched records for the specified filters.
// @Description Used to decide between sync download (<1000 rows) and async export job.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Success 200 {object} dto.ExportCountResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/matches/count [get]
func (handler *Handlers) CountMatched(fiberCtx *fiber.Ctx) error {
	return handler.handleCount(
		fiberCtx,
		"handler.reporting.count_matched",
		handler.reportRepo.CountMatched,
	)
}

// CountTransactions handles GET /v1/reports/contexts/:contextId/transactions/count
// @ID countTransactions
// @Summary Count all transactions
// @Description Returns the total count of all transactions for the specified filters.
// @Description Used to decide between sync download (<1000 rows) and async export job.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Success 200 {object} dto.ExportCountResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/transactions/count [get]
func (handler *Handlers) CountTransactions(fiberCtx *fiber.Ctx) error {
	return handler.handleCount(
		fiberCtx,
		"handler.reporting.count_transactions",
		handler.reportRepo.CountTransactions,
	)
}

// CountExceptions handles GET /v1/reports/contexts/:contextId/exceptions/count
// @ID countExceptions
// @Summary Count exceptions
// @Description Returns the total count of exceptions for the specified filters.
// @Description Used to decide between sync download (<1000 rows) and async export job.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Success 200 {object} dto.ExportCountResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/exceptions/count [get]
func (handler *Handlers) CountExceptions(fiberCtx *fiber.Ctx) error {
	return handler.handleCount(
		fiberCtx,
		"handler.reporting.count_exceptions",
		handler.reportRepo.CountExceptions,
	)
}

// CountUnmatched handles GET /v1/reports/contexts/:contextId/unmatched/count
// @ID countUnmatched
// @Summary Count unmatched records
// @Description Returns the total count of unmatched records for the specified filters.
// @Description Used to decide between sync download (<1000 rows) and async export job.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Success 200 {object} dto.ExportCountResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/unmatched/count [get]
func (handler *Handlers) CountUnmatched(fiberCtx *fiber.Ctx) error {
	return handler.handleCount(
		fiberCtx,
		"handler.reporting.count_unmatched",
		handler.reportRepo.CountUnmatched,
	)
}
