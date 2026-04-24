// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package http provides HTTP handlers for the matching domain.
package http

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/matching/adapters/http/dto"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/matching/services/command"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// RunMatch triggers a matching run for a context.
// @Summary Trigger a matching run
// @Description Triggers a matching run for a reconciliation context. Supports DRY_RUN mode for testing rules without committing results, or COMMIT mode for persisting matches.
// @ID runMatch
// @Tags Matching
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param request body RunMatchRequest true "Run match payload"
// @Success 202 {object} RunMatchResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/matching/contexts/{contextId}/run [post]
func (handler *Handler) RunMatch(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.matching.run")
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
	if shouldReturn, returnErr := handler.handleContextVerificationError(ctx, fiberCtx, span, logger, err); shouldReturn {
		return returnErr
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	var payload RunMatchRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &payload); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid run match payload", err)
	}

	modeValue := strings.TrimSpace(payload.Mode)
	if modeValue == "" {
		return handler.badRequest(ctx, fiberCtx, span, logger, "mode is required", ErrRunModeRequired)
	}

	mode, err := matchingVO.ParseMatchRunMode(strings.ToUpper(modeValue))
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid match run mode", err)
	}

	run, _, err := handler.command.RunMatch(
		ctx,
		command.RunMatchInput{
			TenantID:  tenantID,
			ContextID: contextID,
			Mode:      mode,
		},
	)
	if err != nil {
		return handler.handleRunMatchError(ctx, fiberCtx, span, logger, err)
	}

	if run == nil {
		return handler.writeServiceError(
			ctx,
			fiberCtx,
			span,
			logger,
			"match run response is nil",
			ErrMatchRunResponseNil,
		)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusAccepted, RunMatchResponse{RunID: run.ID, Status: run.Status.String()}); err != nil {
		return fmt.Errorf("write accepted response: %w", err)
	}

	return nil
}

// GetMatchRun retrieves a match run by ID.
// @Summary Get match run
// @Description Retrieves details of a match run by its ID, including status, timing, and aggregate statistics.
// @ID getMatchRun
// @Tags Matching
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param runId path string true "Run ID" format(uuid)
// @Param contextId query string true "Context ID" format(uuid)
// @Success 200 {object} dto.MatchRunResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Match run not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/matching/runs/{runId} [get]
func (handler *Handler) GetMatchRun(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.matching.get_run")
	defer span.End()

	runID, err := parseUUIDParam(fiberCtx, "runId")
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid run id", err)
	}

	contextID, tenantID, err := libHTTP.ParseAndVerifyResourceScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationQuery,
		handler.resourceContextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
		"context",
	)
	if shouldReturn, returnErr := handler.handleContextQueryVerificationError(ctx, fiberCtx, span, logger, err); shouldReturn {
		return returnErr
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	run, err := handler.matchRunRepo.FindByID(ctx, contextID, runID)
	if errors.Is(err, sql.ErrNoRows) {
		libOpentelemetry.HandleSpanError(span, "match run not found", err)

		return writeNotFound(fiberCtx, "match run not found")
	}

	if err != nil {
		return handler.writeServiceError(ctx, fiberCtx, span, logger, "failed to load match run", err)
	}

	if run == nil {
		libOpentelemetry.HandleSpanError(span, "match run not found", sql.ErrNoRows)

		return writeNotFound(fiberCtx, "match run not found")
	}

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.MatchRunToResponse(run)); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}

// ListMatchRuns retrieves all match runs for a context.
// @Summary List match runs
// @Description Retrieves a list of match runs for a given reconciliation context, sorted by creation time descending.
// @ID listMatchRuns
// @Tags Matching
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param sort_order query string false "Sort order" Enums(asc,desc)
// @Success 200 {object} ListMatchRunsResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/matching/contexts/{contextId}/runs [get]
func (handler *Handler) ListMatchRuns(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.matching.list_runs")
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
	if shouldReturn, returnErr := handler.handleContextVerificationError(ctx, fiberCtx, span, logger, err); shouldReturn {
		return returnErr
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

	runs, pagination, err := handler.matchRunRepo.ListByContextID(
		ctx,
		contextID,
		matchingRepos.CursorFilter{
			Limit:     limit,
			Cursor:    cursor,
			SortBy:    "id",
			SortOrder: sortOrder,
		},
	)
	if err != nil {
		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
		}

		return handler.writeServiceError(ctx, fiberCtx, span, logger, "failed to list match runs", err)
	}

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusOK, ListMatchRunsResponse{
		Items: dto.MatchRunsToResponse(runs),
		CursorResponse: sharedhttp.CursorResponse{
			NextCursor: pagination.Next,
			PrevCursor: pagination.Prev,
			Limit:      limit,
			HasMore:    pagination.Next != "",
		},
	}); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}

// GetMatchRunResults retrieves a match run's groups (paged).
// @Summary Get match run results
// @Description Returns a cursor-paginated list of match groups from a matching run, with optional sorting. Each group contains matched transaction pairs and confidence scores.
// @ID getMatchRunGroups
// @Tags Matching
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param runId path string true "Run ID" format(uuid)
// @Param contextId query string true "Context ID" format(uuid)
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param sort_order query string false "Sort order" Enums(asc,desc)
// @Param sort_by query string false "Sort field" Enums(id,created_at,status)
// @Success 200 {object} ListMatchGroupsResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Match run not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/matching/runs/{runId}/groups [get]
//
//nolint:cyclop // HTTP handler with multiple query params and validations
func (handler *Handler) GetMatchRunResults(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.matching.get_run_results")
	defer span.End()

	runID, err := parseUUIDParam(fiberCtx, "runId")
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid run id", err)
	}

	contextID, tenantID, err := libHTTP.ParseAndVerifyResourceScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationQuery,
		handler.resourceContextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
		"context",
	)
	if shouldReturn, returnErr := handler.handleContextQueryVerificationError(ctx, fiberCtx, span, logger, err); shouldReturn {
		return returnErr
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
	if sortBy != "" && sortBy != "id" && sortBy != "created_at" && sortBy != "status" {
		return handler.badRequest(
			ctx,
			fiberCtx,
			span,
			logger,
			"invalid sort_by: must be id, created_at, or status",
			ErrInvalidSortBy,
		)
	}

	groups, pagination, err := handler.query.ListMatchRunGroups(
		ctx,
		contextID,
		runID,
		matchingRepos.CursorFilter{
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

		return handler.writeServiceError(ctx, fiberCtx, span, logger, "failed to load match run results", err)
	}

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusOK, ListMatchGroupsResponse{
		Items: dto.MatchGroupsToResponse(groups),
		CursorResponse: sharedhttp.CursorResponse{
			NextCursor: pagination.Next,
			PrevCursor: pagination.Prev,
			Limit:      limit,
			HasMore:    pagination.Next != "",
		},
	}); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}

// runMatchBadRequestErrors maps bad-request sentinel errors to human-readable messages.
var runMatchBadRequestErrors = []struct {
	err  error
	slug string
	msg  string
}{
	{command.ErrNoSourcesConfigured, "matching_no_sources_configured", "no sources configured for context"},
	{command.ErrAtLeastTwoSourcesRequired, "matching_at_least_two_sources", "at least two sources are required"},
	{command.ErrSourceSideRequiredForMatching, "matching_source_side_required", "all sources must declare side LEFT or RIGHT before matching"},
	{command.ErrOneToOneRequiresExactlyOneLeftSource, "matching_invalid_one_to_one_topology", "1:1 contexts require exactly one LEFT source"},
	{command.ErrOneToOneRequiresExactlyOneRightSource, "matching_invalid_one_to_one_topology", "1:1 contexts require exactly one RIGHT source"},
	{command.ErrOneToManyRequiresExactlyOneLeftSource, "matching_invalid_one_to_many_topology", "1:N contexts require exactly one LEFT source"},
	{command.ErrAtLeastOneRightSourceRequired, "matching_invalid_one_to_many_topology", "at least one RIGHT source is required"},
	{command.ErrMatchRunModeRequired, "invalid_request", "match run mode is required"},
}

// isRunMatchBadRequestError returns true if err is a client-side (bad request) error.
func isRunMatchBadRequestError(err error) bool {
	for _, entry := range runMatchBadRequestErrors {
		if errors.Is(err, entry.err) {
			return true
		}
	}

	return false
}

// runMatchBadRequestMessage returns the message for a bad-request error, or a fallback.
func runMatchBadRequestMessage(err error) string {
	for _, entry := range runMatchBadRequestErrors {
		if errors.Is(err, entry.err) {
			return entry.msg
		}
	}

	return "bad request"
}

func runMatchBadRequestSlug(err error) string {
	for _, entry := range runMatchBadRequestErrors {
		if errors.Is(err, entry.err) {
			return entry.slug
		}
	}

	return "invalid_request"
}

// mapRunMatchErrorToResponse maps known errors to appropriate HTTP responses.
func (handler *Handler) mapRunMatchErrorToResponse(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	switch {
	case errors.Is(err, command.ErrContextNotFound):
		return writeNotFound(fiberCtx, "context not found")
	case errors.Is(err, command.ErrContextNotActive):
		return respondError(fiberCtx, fiber.StatusForbidden, "context_not_active", "context is not active")
	case isRunMatchBadRequestError(err):
		handler.logSpanError(ctx, span, logger, runMatchBadRequestMessage(err), err)
		return respondError(fiberCtx, fiber.StatusBadRequest, runMatchBadRequestSlug(err), runMatchBadRequestMessage(err))
	case errors.Is(err, command.ErrFeeRulesReferenceMissingSchedules):
		return respondError(
			fiberCtx,
			fiber.StatusUnprocessableEntity,
			"fee_rules_misconfigured",
			"fee rules reference fee schedules that do not exist",
		)
	case errors.Is(err, command.ErrFeeRulesRequiredForNormalization):
		return respondError(
			fiberCtx,
			fiber.StatusUnprocessableEntity,
			"fee_rules_missing",
			"fee normalization is enabled but no fee rules are configured for this context",
		)
	case errors.Is(err, sharedfee.ErrFeeRuleCountLimitExceeded):
		return respondError(
			fiberCtx,
			fiber.StatusUnprocessableEntity,
			"fee_rules_misconfigured",
			"fee rule count exceeds the maximum allowed per context",
		)
	case errors.Is(err, command.ErrMatchRunLocked):
		return respondError(
			fiberCtx,
			fiber.StatusConflict,
			"match_run_in_progress",
			"another match run is already in progress for this context",
		)
	default:
		return handler.writeServiceError(ctx, fiberCtx, span, logger, "failed to run match", err)
	}
}

func (handler *Handler) handleRunMatchError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	if !isRunMatchBadRequestError(err) {
		libOpentelemetry.HandleSpanError(span, "failed to run match", err)
	}

	libLog.SafeError(logger, ctx, "failed to run match", err, handler.productionMode)

	return handler.mapRunMatchErrorToResponse(ctx, fiberCtx, span, logger, err)
}
