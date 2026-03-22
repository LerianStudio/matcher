package http

import (
	"database/sql"
	"errors"

	"github.com/gofiber/fiber/v2"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
)

// CreateFieldMap creates a field map.
//
// @ID createFieldMap
// @Summary Create a field map
// @Description Creates a field map for a source within a context.
// @Tags Configuration Field Maps
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param sourceId path string true "Source ID" format(uuid)
// @Param fieldMap body dto.CreateFieldMapRequest true "Field map creation payload"
// @Success 201 {object} dto.FieldMapResponse "Successfully created field map"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Context or source not found"
// @Failure 409 {object} ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/config/contexts/{contextId}/sources/{sourceId}/field-maps [post]
func (handler *Handler) CreateFieldMap(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fieldmap.create")
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
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	sourceID, err := parseUUIDParam(fiberCtx, "sourceId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid source id", err)
	}

	var req dto.CreateFieldMapRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid field map payload", err)
	}

	if err := handler.ensureSourceAccess(ctx, fiberCtx, span, logger, contextID, sourceID); err != nil {
		return err
	}

	result, err := handler.command.CreateFieldMap(ctx, contextID, sourceID, req.ToDomainInput())
	if err != nil {
		logSpanError(ctx, span, logger, "failed to create field map", err)
		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusCreated, dto.FieldMapToResponse(result))
}

// GetFieldMapBySource retrieves a field map by source.
//
// @ID getFieldMapBySource
// @Summary Get a field map by source
// @Description Returns the field map for a source within a context.
// @Tags Configuration Field Maps
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param sourceId path string true "Source ID" format(uuid)
// @Success 200 {object} dto.FieldMapResponse "Successfully retrieved field map"
// @Failure 400 {object} ErrorResponse "Invalid source ID format"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Field map not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/config/contexts/{contextId}/sources/{sourceId}/field-maps [get]
func (handler *Handler) GetFieldMapBySource(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fieldmap.get_by_source")
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
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	sourceID, err := parseUUIDParam(fiberCtx, "sourceId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid source id", err)
	}

	if err := handler.ensureSourceAccess(ctx, fiberCtx, span, logger, contextID, sourceID); err != nil {
		return err
	}

	result, err := handler.query.GetFieldMapBySource(ctx, sourceID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "field map not found")
		}

		logSpanError(ctx, span, logger, "failed to get field map", err)

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.FieldMapToResponse(result))
}

// UpdateFieldMap updates a field map.
//
// @ID updateFieldMap
// @Summary Update a field map
// @Description Updates fields on a field map by ID.
// @Tags Configuration Field Maps
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param fieldMapId path string true "Field map ID" format(uuid)
// @Param fieldMap body dto.UpdateFieldMapRequest true "Field map updates"
// @Success 200 {object} dto.FieldMapResponse "Successfully updated field map"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Field map not found"
// @Failure 409 {object} ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/config/field-maps/{fieldMapId} [patch]
func (handler *Handler) UpdateFieldMap(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fieldmap.update")
	defer span.End()

	fieldMapID, err := parseUUIDParam(fiberCtx, "fieldMapId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid field map id", err)
	}

	var req dto.UpdateFieldMapRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid field map payload", err)
	}

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return unauthorized(ctx, fiberCtx, span, logger, err)
	}

	fieldMap, err := handler.query.GetFieldMap(ctx, fieldMapID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "resource not found")
		}

		logSpanError(ctx, span, logger, "failed to load field map", err)

		return writeServiceError(fiberCtx, err)
	}

	if err := handler.contextVerifier(ctx, tenantID, fieldMap.ContextID); err != nil {
		return handleOwnershipVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, fieldMap.ContextID)

	result, err := handler.command.UpdateFieldMap(ctx, fieldMapID, req.ToDomainInput())
	if err != nil {
		logSpanError(ctx, span, logger, "failed to update field map", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "resource not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.FieldMapToResponse(result))
}

// DeleteFieldMap deletes a field map.
//
// @ID deleteFieldMap
// @Summary Delete a field map
// @Description Removes a field map by ID.
// @Tags Configuration Field Maps
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param fieldMapId path string true "Field map ID" format(uuid)
// @Success 204 "Field map successfully deleted"
// @Failure 400 {object} ErrorResponse "Invalid field map ID format"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Field map not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/config/field-maps/{fieldMapId} [delete]
func (handler *Handler) DeleteFieldMap(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fieldmap.delete")
	defer span.End()

	fieldMapID, err := parseUUIDParam(fiberCtx, "fieldMapId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid field map id", err)
	}

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return unauthorized(ctx, fiberCtx, span, logger, err)
	}

	fieldMap, err := handler.query.GetFieldMap(ctx, fieldMapID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "resource not found")
		}

		logSpanError(ctx, span, logger, "failed to load field map", err)

		return writeServiceError(fiberCtx, err)
	}

	if err := handler.contextVerifier(ctx, tenantID, fieldMap.ContextID); err != nil {
		return handleOwnershipVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, fieldMap.ContextID)

	if err := handler.command.DeleteFieldMap(ctx, fieldMapID); err != nil {
		logSpanError(ctx, span, logger, "failed to delete field map", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "resource not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.RespondStatus(fiberCtx, fiber.StatusNoContent)
}
