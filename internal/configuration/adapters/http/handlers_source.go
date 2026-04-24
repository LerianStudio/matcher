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
	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/configuration/services/command"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

// CreateSource creates a reconciliation source.
//
// @ID createSource
// @Summary Create a reconciliation source
// @Description Creates a new reconciliation source under a context.
// @Tags Configuration Sources
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param source body dto.CreateSourceRequest true "Source creation payload"
// @Success 201 {object} dto.ReconciliationSourceResponse "Successfully created source"
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 409 {object} sharedhttp.ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/config/contexts/{contextId}/sources [post]
func (handler *Handler) CreateSource(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.source.create")
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

	var req dto.CreateSourceRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid source payload", err)
	}

	domainInput, err := req.ToDomainInput()
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid source payload", err)
	}

	result, err := handler.command.CreateSource(ctx, contextID, domainInput)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to create source", err)
		return writeServiceError(fiberCtx, err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusCreated, dto.ReconciliationSourceToResponse(result)); err != nil {
		return fmt.Errorf("respond create source: %w", err)
	}

	return nil
}

// ListSources lists reconciliation sources.
//
// @ID listSources
// @Summary List reconciliation sources
// @Description Returns a cursor-paginated list of reconciliation sources under a context, optionally filtered by type.
// @Tags Configuration Sources
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param type query string false "Filter by source type" Enums(LEDGER,BANK,GATEWAY,CUSTOM,FETCHER)
// @Success 200 {object} ListSourcesResponse "List of sources with cursor pagination"
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/config/contexts/{contextId}/sources [get]
func (handler *Handler) ListSources(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.source.list")
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
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid pagination", err)
	}

	var sourceType *value_objects.SourceType

	if typeParam := strings.TrimSpace(fiberCtx.Query("type")); typeParam != "" {
		parsed, err := value_objects.ParseSourceType(strings.ToUpper(typeParam))
		if err != nil {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid source type", err)
		}

		sourceType = &parsed
	}

	result, pagination, err := handler.query.ListSources(ctx, contextID, cursor, limit, sourceType)
	if err != nil {
		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid pagination", err)
		}

		handler.logSpanError(ctx, span, logger, "failed to list sources", err)

		return writeServiceError(fiberCtx, err)
	}

	if result == nil {
		result = []*entities.ReconciliationSource{}
	}

	// Check which sources have field maps
	sourceIDs := make([]uuid.UUID, len(result))
	for i, src := range result {
		sourceIDs[i] = src.ID
	}

	fieldMapsExist, err := handler.fieldMapRepo.ExistsBySourceIDs(ctx, sourceIDs)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to check field maps existence", err)
		return writeServiceError(fiberCtx, err)
	}

	response := ListSourcesResponse{
		Items: toSourceValuesWithFieldMaps(result, fieldMapsExist),
		CursorResponse: sharedhttp.CursorResponse{
			NextCursor: pagination.Next,
			PrevCursor: pagination.Prev,
			Limit:      limit,
			HasMore:    pagination.Next != "",
		},
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, response); err != nil {
		return fmt.Errorf("respond list sources: %w", err)
	}

	return nil
}

// GetSource retrieves a reconciliation source.
//
// @ID getSource
// @Summary Get a reconciliation source
// @Description Returns a reconciliation source by ID.
// @Tags Configuration Sources
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param sourceId path string true "Source ID" format(uuid)
// @Success 200 {object} dto.ReconciliationSourceResponse "Successfully retrieved source"
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid source ID format"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Source not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/config/contexts/{contextId}/sources/{sourceId} [get]
func (handler *Handler) GetSource(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.source.get")
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

	sourceID, err := parseUUIDParam(fiberCtx, "sourceId")
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid source id", err)
	}

	result, err := handler.sourceRepo.FindByID(ctx, contextID, sourceID)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to get source", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "configuration_source_not_found", "source not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ReconciliationSourceToResponse(result)); err != nil {
		return fmt.Errorf("respond get source: %w", err)
	}

	return nil
}

// UpdateSource updates a reconciliation source.
//
// @ID updateSource
// @Summary Update a reconciliation source
// @Description Updates fields on a reconciliation source by ID.
// @Tags Configuration Sources
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param sourceId path string true "Source ID" format(uuid)
// @Param source body dto.UpdateSourceRequest true "Source updates"
// @Success 200 {object} dto.ReconciliationSourceResponse "Successfully updated source"
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Source not found"
// @Failure 409 {object} sharedhttp.ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/config/contexts/{contextId}/sources/{sourceId} [patch]
func (handler *Handler) UpdateSource(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.source.update")
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

	sourceID, err := parseUUIDParam(fiberCtx, "sourceId")
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid source id", err)
	}

	var req dto.UpdateSourceRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid source payload", err)
	}

	domainInput, err := req.ToDomainInput()
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid source payload", err)
	}

	result, err := handler.command.UpdateSource(ctx, contextID, sourceID, domainInput)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to update source", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "configuration_source_not_found", "source not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ReconciliationSourceToResponse(result)); err != nil {
		return fmt.Errorf("respond update source: %w", err)
	}

	return nil
}

// DeleteSource deletes a reconciliation source.
//
// @ID deleteSource
// @Summary Delete a reconciliation source
// @Description Removes a reconciliation source by ID.
// @Tags Configuration Sources
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param sourceId path string true "Source ID" format(uuid)
// @Success 204 "Source successfully deleted"
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid source ID format"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Source not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/config/contexts/{contextId}/sources/{sourceId} [delete]
func (handler *Handler) DeleteSource(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.source.delete")
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

	sourceID, err := parseUUIDParam(fiberCtx, "sourceId")
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid source id", err)
	}

	if err := handler.command.DeleteSource(ctx, contextID, sourceID); err != nil {
		handler.logSpanError(ctx, span, logger, "failed to delete source", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "configuration_source_not_found", "source not found")
		}

		if errors.Is(err, command.ErrSourceHasFieldMap) {
			return respondError(fiberCtx, fiber.StatusConflict, "has_field_map", err.Error())
		}

		return writeServiceError(fiberCtx, err)
	}

	if err := libHTTP.RespondStatus(fiberCtx, fiber.StatusNoContent); err != nil {
		return fmt.Errorf("respond delete source: %w", err)
	}

	return nil
}
