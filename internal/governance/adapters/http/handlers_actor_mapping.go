package http

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/governance/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	governanceErrors "github.com/LerianStudio/matcher/internal/governance/domain/errors"
	"github.com/LerianStudio/matcher/internal/governance/services/command"
	"github.com/LerianStudio/matcher/internal/governance/services/query"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

var _ = sharedhttp.ErrorResponse{}

// Sentinel errors for actor mapping handler validation.
var (
	ErrActorMappingCommandUCRequired = errors.New("actor mapping command use case is required")
	ErrActorMappingQueryUCRequired   = errors.New("actor mapping query use case is required")
	ErrMissingActorID                = errors.New("actor id path parameter is required")
	ErrAtLeastOneFieldRequired       = errors.New("at least one of display_name or email must be provided")
	ErrNilActorMappingResponse       = errors.New("nil mapping after successful upsert")
)

// ActorMappingHandler handles HTTP requests for actor mapping operations.
//
// productionMode governs SafeError behavior (suppresses internal error
// details in client responses when true). Stored per-handler rather than
// on a package-level atomic.Bool to avoid cross-test coupling via shared
// global state.
type ActorMappingHandler struct {
	commandUC      *command.ActorMappingUseCase
	queryUC        *query.ActorMappingQueryUseCase
	productionMode bool
}

// NewActorMappingHandler creates a new actor mapping HTTP handler.
func NewActorMappingHandler(
	commandUC *command.ActorMappingUseCase,
	queryUC *query.ActorMappingQueryUseCase,
	production bool,
) (*ActorMappingHandler, error) {
	if commandUC == nil {
		return nil, ErrActorMappingCommandUCRequired
	}

	if queryUC == nil {
		return nil, ErrActorMappingQueryUCRequired
	}

	return &ActorMappingHandler{
		commandUC:      commandUC,
		queryUC:        queryUC,
		productionMode: production,
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
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/governance/actor-mappings/{actorId} [put]
func (ha *ActorMappingHandler) UpsertActorMapping(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.governance.upsert_actor_mapping")
	defer span.End()

	actorID := strings.TrimSpace(fiberCtx.Params("actorId"))
	if actorID == "" {
		return ha.badRequest(ctx, fiberCtx, span, logger, "actor id is required", ErrMissingActorID)
	}

	var req dto.UpsertActorMappingRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return ha.badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	// At least one field must be provided to avoid no-op upserts.
	if req.DisplayName == nil && req.Email == nil {
		return ha.badRequest(ctx, fiberCtx, span, logger, ErrAtLeastOneFieldRequired.Error(), ErrAtLeastOneFieldRequired)
	}

	if len(actorID) > entities.MaxActorMappingActorIDLength {
		return ha.badRequest(ctx, fiberCtx, span, logger, "actor id exceeds maximum length", entities.ErrActorIDExceedsMaxLen)
	}

	mapping, err := ha.commandUC.UpsertActorMapping(ctx, actorID, req.DisplayName, req.Email)
	if err != nil {
		if errors.Is(err, entities.ErrActorIDRequired) || errors.Is(err, entities.ErrActorIDExceedsMaxLen) {
			return ha.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
		}

		return ha.writeServiceError(ctx, fiberCtx, span, logger, "failed to upsert actor mapping", err)
	}

	if mapping == nil {
		return ha.writeServiceError(ctx, fiberCtx, span, logger, "upsert returned nil mapping", ErrNilActorMappingResponse)
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
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Actor mapping not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/governance/actor-mappings/{actorId} [get]
func (ha *ActorMappingHandler) GetActorMapping(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.governance.get_actor_mapping")
	defer span.End()

	actorID := fiberCtx.Params("actorId")
	if actorID == "" {
		return ha.badRequest(ctx, fiberCtx, span, logger, "actor id is required", ErrMissingActorID)
	}

	mapping, err := ha.queryUC.GetActorMapping(ctx, actorID)
	if err != nil {
		if errors.Is(err, governanceErrors.ErrActorMappingNotFound) {
			return ha.writeNotFound(ctx, fiberCtx, span, logger, "governance_actor_mapping_not_found", "actor mapping not found", err)
		}

		return ha.writeServiceError(ctx, fiberCtx, span, logger, "failed to get actor mapping", err)
	}

	if mapping == nil {
		return ha.writeNotFound(ctx, fiberCtx, span, logger, "governance_actor_mapping_not_found", "actor mapping not found", governanceErrors.ErrActorMappingNotFound)
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
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Actor mapping not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/governance/actor-mappings/{actorId}/pseudonymize [post]
func (ha *ActorMappingHandler) PseudonymizeActor(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.governance.pseudonymize_actor")
	defer span.End()

	actorID := fiberCtx.Params("actorId")
	if actorID == "" {
		return ha.badRequest(ctx, fiberCtx, span, logger, "actor id is required", ErrMissingActorID)
	}

	if err := ha.commandUC.PseudonymizeActor(ctx, actorID); err != nil {
		if errors.Is(err, governanceErrors.ErrActorMappingNotFound) {
			return ha.writeNotFound(ctx, fiberCtx, span, logger, "governance_actor_mapping_not_found", "actor mapping not found", err)
		}

		return ha.writeServiceError(ctx, fiberCtx, span, logger, "failed to pseudonymize actor", err)
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
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Actor mapping not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/governance/actor-mappings/{actorId} [delete]
func (ha *ActorMappingHandler) DeleteActorMapping(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.governance.delete_actor_mapping")
	defer span.End()

	actorID := fiberCtx.Params("actorId")
	if actorID == "" {
		return ha.badRequest(ctx, fiberCtx, span, logger, "actor id is required", ErrMissingActorID)
	}

	if err := ha.commandUC.DeleteActorMapping(ctx, actorID); err != nil {
		if errors.Is(err, governanceErrors.ErrActorMappingNotFound) {
			return ha.writeNotFound(ctx, fiberCtx, span, logger, "governance_actor_mapping_not_found", "actor mapping not found", err)
		}

		return ha.writeServiceError(ctx, fiberCtx, span, logger, "failed to delete actor mapping", err)
	}

	return fiberCtx.SendStatus(fiber.StatusNoContent)
}

// Response helpers — see note on *Handler methods in handlers.go for why
// these live on the receiver rather than in package-global state.

func (ha *ActorMappingHandler) logSpanError(ctx context.Context, span trace.Span, logger libLog.Logger, message string, err error) {
	sharedhttp.LogSpanError(ctx, span, logger, ha.productionMode, message, err)
}

//nolint:wrapcheck // HTTP transport response is the terminal error boundary.
func (ha *ActorMappingHandler) badRequest(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	return sharedhttp.BadRequest(ctx, fiberCtx, span, logger, ha.productionMode, message, err)
}

//nolint:wrapcheck // HTTP transport response is the terminal error boundary.
func (ha *ActorMappingHandler) writeServiceError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	return sharedhttp.InternalError(ctx, fiberCtx, span, logger, ha.productionMode, message, err)
}

func (ha *ActorMappingHandler) writeNotFound(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	slug string,
	message string,
	err error,
) error {
	sharedhttp.LogSpanError(ctx, span, logger, ha.productionMode, message, err)

	return respondError(fiberCtx, fiber.StatusNotFound, slug, message)
}
