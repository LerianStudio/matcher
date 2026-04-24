// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/exception/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/services/query"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
)

// ListExceptions lists exceptions with optional filters and pagination.
// @Summary List exceptions
// @Description Lists all exceptions with optional filters for status, severity, assigned user, external system, and date range. Supports cursor-based pagination.
// @ID listExceptions
// @Tags Exception
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param status query string false "Filter by status" Enums(OPEN,ASSIGNED,RESOLVED)
// @Param severity query string false "Filter by severity" Enums(LOW,MEDIUM,HIGH,CRITICAL)
// @Param assigned_to query string false "Filter by assigned user"
// @Param external_system query string false "Filter by external system"
// @Param date_from query string false "Filter from date (RFC3339)"
// @Param date_to query string false "Filter to date (RFC3339)"
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param sort_by query string false "Sort by field" Enums(id,created_at,updated_at,severity,status) default(id)
// @Param sort_order query string false "Sort order" Enums(asc,desc) default(desc)
// @Success 200 {object} dto.ListExceptionsResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/exceptions [get]
func (handler *Handlers) ListExceptions(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.list")
	defer span.End()

	filter, cursorFilter, err := parseListFilters(fiberCtx)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid filter parameters", err)
	}

	exceptions, pagination, err := handler.queryUC.ListExceptions(ctx, query.ListQuery{
		Filter: filter,
		Cursor: cursorFilter,
	})
	if err != nil {
		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
		}

		return handler.internalError(ctx, fiberCtx, span, logger, "failed to list exceptions", err)
	}

	items := dto.ExceptionsToResponse(exceptions)

	response := dto.ListExceptionsResponse{
		Items: items,
		CursorResponse: sharedhttp.CursorResponse{
			NextCursor: pagination.Next,
			PrevCursor: pagination.Prev,
			Limit:      cursorFilter.Limit,
			HasMore:    pagination.Next != "",
		},
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, response); err != nil {
		return fmt.Errorf("respond list exceptions: %w", err)
	}

	return nil
}

func parseListFilters(
	fiberCtx *fiber.Ctx,
) (repositories.ExceptionFilter, repositories.CursorFilter, error) {
	filter, err := parseExceptionFilter(fiberCtx)
	if err != nil {
		return repositories.ExceptionFilter{}, repositories.CursorFilter{}, err
	}

	cursorFilter, err := parseCursorFilter(fiberCtx)
	if err != nil {
		return repositories.ExceptionFilter{}, repositories.CursorFilter{}, err
	}

	return filter, cursorFilter, nil
}

func parseExceptionFilter(fiberCtx *fiber.Ctx) (repositories.ExceptionFilter, error) {
	var filter repositories.ExceptionFilter

	if status := fiberCtx.Query("status"); status != "" {
		parsed, err := value_objects.ParseExceptionStatus(status)
		if err != nil {
			return filter, fmt.Errorf("invalid status: %w", err)
		}

		filter.Status = &parsed
	}

	if severity := fiberCtx.Query("severity"); severity != "" {
		parsed, err := sharedexception.ParseExceptionSeverity(severity)
		if err != nil {
			return filter, fmt.Errorf("invalid severity: %w", err)
		}

		filter.Severity = &parsed
	}

	if assignedTo := fiberCtx.Query("assigned_to"); assignedTo != "" {
		if err := libHTTP.ValidateQueryParamLength(assignedTo, "assigned_to", libHTTP.MaxQueryParamLengthLong); err != nil {
			return filter, fmt.Errorf("invalid assigned_to: %w", err)
		}

		filter.AssignedTo = &assignedTo
	}

	if externalSystem := fiberCtx.Query("external_system"); externalSystem != "" {
		if err := libHTTP.ValidateQueryParamLength(externalSystem, "external_system", libHTTP.MaxQueryParamLengthLong); err != nil {
			return filter, fmt.Errorf("invalid external_system: %w", err)
		}

		filter.ExternalSystem = &externalSystem
	}

	if dateFrom := fiberCtx.Query("date_from"); dateFrom != "" {
		parsed, err := time.Parse(time.RFC3339, dateFrom)
		if err != nil {
			return filter, fmt.Errorf("invalid date_from: %w", err)
		}

		filter.DateFrom = &parsed
	}

	if dateTo := fiberCtx.Query("date_to"); dateTo != "" {
		parsed, err := time.Parse(time.RFC3339, dateTo)
		if err != nil {
			return filter, fmt.Errorf("invalid date_to: %w", err)
		}

		filter.DateTo = &parsed
	}

	return filter, nil
}

// allowedSortColumns defines the whitelist of columns allowed for sorting.
var allowedSortColumns = []string{"id", "created_at", "updated_at", "severity", "status"}

// allowedSortOrders defines the whitelist of sort orders.
var allowedSortOrders = map[string]bool{
	"asc":  true,
	"desc": true,
}

func parseCursorFilter(fiberCtx *fiber.Ctx) (repositories.CursorFilter, error) {
	cursor, limit, err := libHTTP.ParseOpaqueCursorPagination(fiberCtx)
	if err != nil {
		return repositories.CursorFilter{}, fmt.Errorf("parse pagination: %w", err)
	}

	sortBy := libHTTP.ValidateSortColumn(fiberCtx.Query("sort_by"), allowedSortColumns, "id")

	sortOrder := fiberCtx.Query("sort_order")
	if sortOrder != "" && !allowedSortOrders[sortOrder] {
		return repositories.CursorFilter{}, ErrInvalidSortOrder
	}

	return repositories.CursorFilter{
		Limit:     limit,
		Cursor:    cursor,
		SortBy:    sortBy,
		SortOrder: sortOrder,
	}, nil
}

// GetException retrieves a single exception by ID.
// @Summary Get exception
// @Description Retrieves a single exception by its ID.
// @ID getException
// @Tags Exception
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param exceptionId path string true "Exception ID" format(uuid)
// @Success 200 {object} dto.ExceptionResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Exception not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/exceptions/{exceptionId} [get]
func (handler *Handlers) GetException(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.get")
	defer span.End()

	exceptionID, tenantID, err := libHTTP.ParseAndVerifyResourceScopedID(
		fiberCtx,
		"exceptionId",
		libHTTP.IDLocationParam,
		handler.exceptionVerifier,
		auth.GetTenantID,
		ErrMissingExceptionID,
		ErrInvalidExceptionID,
		ErrExceptionAccessDenied,
		"exception",
	)
	if err != nil {
		return handler.handleExceptionVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetExceptionSpanAttributes(span, tenantID, exceptionID)

	exception, err := handler.queryUC.GetException(ctx, exceptionID)
	if err != nil {
		if errors.Is(err, entities.ErrExceptionNotFound) {
			return handler.notFoundWithSlug(ctx, fiberCtx, span, logger, "exception_not_found", "exception not found", err)
		}

		return handler.internalError(ctx, fiberCtx, span, logger, "failed to get exception", err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ExceptionToResponse(exception)); err != nil {
		return fmt.Errorf("respond get exception: %w", err)
	}

	return nil
}

// GetHistory retrieves the audit history for an exception.
// @Summary Get exception history
// @Description Retrieves the audit history for an exception, showing all actions taken on it. Pagination is forward-only (no prevCursor); use the nextCursor value to fetch subsequent pages.
// @ID getExceptionHistory
// @Tags Exception
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param exceptionId path string true "Exception ID" format(uuid)
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Success 200 {object} dto.HistoryResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Exception not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/exceptions/{exceptionId}/history [get]
func (handler *Handlers) GetHistory(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.get_history")
	defer span.End()

	exceptionID, tenantID, err := libHTTP.ParseAndVerifyResourceScopedID(
		fiberCtx,
		"exceptionId",
		libHTTP.IDLocationParam,
		handler.exceptionVerifier,
		auth.GetTenantID,
		ErrMissingExceptionID,
		ErrInvalidExceptionID,
		ErrExceptionAccessDenied,
		"exception",
	)
	if err != nil {
		return handler.handleExceptionVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetExceptionSpanAttributes(span, tenantID, exceptionID)

	cursorParam := fiberCtx.Query("cursor")

	// cursorPtr validates/parses the timestamp cursor; the raw cursorParam is forwarded as-is.
	cursorPtr, limit, err := parseTimestampCursorPagination(fiberCtx)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
	}

	cursor := ""
	if cursorPtr != nil {
		cursor = cursorParam
	}

	entries, nextCursor, err := handler.queryUC.GetHistory(ctx, exceptionID, cursor, limit)
	if err != nil {
		if errors.Is(err, query.ErrTenantIDRequired) {
			return handler.badRequest(ctx, fiberCtx, span, logger, "tenant context required", err)
		}

		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
		}

		return handler.internalError(ctx, fiberCtx, span, logger, "failed to get history", err)
	}

	items := make([]dto.HistoryEntryResponse, len(entries))

	for i, entry := range entries {
		var changes any

		if len(entry.Changes) > 0 {
			if err := json.Unmarshal(entry.Changes, &changes); err != nil {
				logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf(
					"failed to unmarshal history entry changes for entry %s: %v",
					entry.ID.String(),
					err,
				))
			}
		}

		items[i] = dto.HistoryEntryResponse{
			ID:        entry.ID.String(),
			Action:    entry.Action,
			ActorID:   entry.ActorID,
			Changes:   changes,
			CreatedAt: entry.CreatedAt,
		}
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.HistoryResponse{
		Items: items,
		CursorResponse: sharedhttp.CursorResponse{
			NextCursor: nextCursor,
			Limit:      limit,
			HasMore:    nextCursor != "",
		},
	}); err != nil {
		return fmt.Errorf("respond exception history: %w", err)
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
