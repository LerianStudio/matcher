package http

import (
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/governance/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	governanceErrors "github.com/LerianStudio/matcher/internal/governance/domain/errors"
	"github.com/LerianStudio/matcher/internal/governance/services/command"
	"github.com/LerianStudio/matcher/internal/governance/services/query"
)

// Sentinel errors for actor mapping handler validation.
var (
	ErrActorMappingCommandUCRequired = errors.New("actor mapping command use case is required")
	ErrActorMappingQueryUCRequired   = errors.New("actor mapping query use case is required")
	ErrMissingActorID                = errors.New("actor id path parameter is required")
	ErrAtLeastOneFieldRequired       = errors.New("at least one of display_name or email must be provided")
)

// ActorMappingHandler handles HTTP requests for actor mapping operations.
type ActorMappingHandler struct {
	commandUC *command.ActorMappingUseCase
	queryUC   *query.ActorMappingQueryUseCase
}

// NewActorMappingHandler creates a new actor mapping HTTP handler.
func NewActorMappingHandler(
	commandUC *command.ActorMappingUseCase,
	queryUC *query.ActorMappingQueryUseCase,
) (*ActorMappingHandler, error) {
	if commandUC == nil {
		return nil, ErrActorMappingCommandUCRequired
	}

	if queryUC == nil {
		return nil, ErrActorMappingQueryUCRequired
	}

	return &ActorMappingHandler{
		commandUC: commandUC,
		queryUC:   queryUC,
	}, nil
}

// UpsertActorMapping creates or updates an actor mapping.
// @Summary Upsert actor mapping
// @Description Creates or updates the PII mapping for an actor ID. Used to associate opaque actor identifiers with human-readable display names and emails.
// @ID upsertActorMapping
// @Tags Governance
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param actorId path string true "Actor ID"
// @Param request body dto.UpsertActorMappingRequest true "Actor mapping data"
// @Success 200 {object} dto.ActorMappingResponse
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid request"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
// @Router /v1/governance/actor-mappings/{actorId} [put]
func (ha *ActorMappingHandler) UpsertActorMapping(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.governance.upsert_actor_mapping")
	defer span.End()

	actorID := fiberCtx.Params("actorId")
	if actorID == "" {
		return badRequest(ctx, fiberCtx, span, logger, "actor id is required", ErrMissingActorID)
	}

	var req dto.UpsertActorMappingRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	// At least one field must be provided to avoid no-op upserts.
	if req.DisplayName == nil && req.Email == nil {
		return badRequest(ctx, fiberCtx, span, logger, ErrAtLeastOneFieldRequired.Error(), ErrAtLeastOneFieldRequired)
	}

	if err := ha.commandUC.UpsertActorMapping(ctx, actorID, req.DisplayName, req.Email); err != nil {
		if errors.Is(err, entities.ErrActorIDRequired) || errors.Is(err, entities.ErrActorIDExceedsMaxLen) {
			return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
		}

		return writeServiceError(ctx, fiberCtx, span, logger, "failed to upsert actor mapping", err)
	}

	// Retrieve the upserted mapping to return a complete response.
	mapping, err := ha.queryUC.GetActorMapping(ctx, actorID)
	if err != nil {
		return writeServiceError(ctx, fiberCtx, span, logger, "failed to retrieve actor mapping after upsert", err)
	}

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ActorMappingToResponse(mapping)); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}

// GetActorMapping retrieves a single actor mapping by actor ID.
// @Summary Get actor mapping
// @Description Retrieves the PII mapping for a specific actor ID.
// @ID getActorMapping
// @Tags Governance
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param actorId path string true "Actor ID"
// @Success 200 {object} dto.ActorMappingResponse
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid request"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Actor mapping not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
// @Router /v1/governance/actor-mappings/{actorId} [get]
func (ha *ActorMappingHandler) GetActorMapping(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.governance.get_actor_mapping")
	defer span.End()

	actorID := fiberCtx.Params("actorId")
	if actorID == "" {
		return badRequest(ctx, fiberCtx, span, logger, "actor id is required", ErrMissingActorID)
	}

	mapping, err := ha.queryUC.GetActorMapping(ctx, actorID)
	if err != nil {
		if errors.Is(err, governanceErrors.ErrActorMappingNotFound) {
			return writeNotFound(ctx, fiberCtx, span, logger, "actor mapping not found", err)
		}

		return writeServiceError(ctx, fiberCtx, span, logger, "failed to get actor mapping", err)
	}

	if mapping == nil {
		return writeNotFound(ctx, fiberCtx, span, logger, "actor mapping not found", governanceErrors.ErrActorMappingNotFound)
	}

	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ActorMappingToResponse(mapping)); writeErr != nil {
		return fmt.Errorf("write ok response: %w", writeErr)
	}

	return nil
}

// PseudonymizeActor replaces PII fields with [REDACTED] for GDPR compliance.
// @Summary Pseudonymize actor
// @Description Replaces display name and email with [REDACTED] for GDPR compliance. The actor mapping record is preserved with the actor ID link intact.
// @ID pseudonymizeActor
// @Tags Governance
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param actorId path string true "Actor ID"
// @Success 204 "No Content"
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid request"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Actor mapping not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
// @Router /v1/governance/actor-mappings/{actorId}/pseudonymize [post]
func (ha *ActorMappingHandler) PseudonymizeActor(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.governance.pseudonymize_actor")
	defer span.End()

	actorID := fiberCtx.Params("actorId")
	if actorID == "" {
		return badRequest(ctx, fiberCtx, span, logger, "actor id is required", ErrMissingActorID)
	}

	if err := ha.commandUC.PseudonymizeActor(ctx, actorID); err != nil {
		if errors.Is(err, governanceErrors.ErrActorMappingNotFound) {
			return writeNotFound(ctx, fiberCtx, span, logger, "actor mapping not found", err)
		}

		return writeServiceError(ctx, fiberCtx, span, logger, "failed to pseudonymize actor", err)
	}

	return fiberCtx.SendStatus(fiber.StatusNoContent)
}

// DeleteActorMapping permanently removes an actor mapping (right-to-erasure).
// @Summary Delete actor mapping
// @Description Permanently removes an actor mapping for GDPR right-to-erasure compliance.
// @ID deleteActorMapping
// @Tags Governance
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param actorId path string true "Actor ID"
// @Success 204 "No Content"
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid request"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Actor mapping not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
// @Router /v1/governance/actor-mappings/{actorId} [delete]
func (ha *ActorMappingHandler) DeleteActorMapping(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.governance.delete_actor_mapping")
	defer span.End()

	actorID := fiberCtx.Params("actorId")
	if actorID == "" {
		return badRequest(ctx, fiberCtx, span, logger, "actor id is required", ErrMissingActorID)
	}

	if err := ha.commandUC.DeleteActorMapping(ctx, actorID); err != nil {
		if errors.Is(err, governanceErrors.ErrActorMappingNotFound) {
			return writeNotFound(ctx, fiberCtx, span, logger, "actor mapping not found", err)
		}

		return writeServiceError(ctx, fiberCtx, span, logger, "failed to delete actor mapping", err)
	}

	return fiberCtx.SendStatus(fiber.StatusNoContent)
}
