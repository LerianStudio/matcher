package http

import (
	"errors"

	"github.com/gofiber/fiber/v2"

	"github.com/LerianStudio/matcher/internal/auth"
)

// Sentinel errors for route registration.
var (
	// ErrProtectedRouteHelperRequired indicates protected route helper is nil.
	ErrProtectedRouteHelperRequired = errors.New("protected route helper is required")
	ErrHandlerRequired              = errors.New("governance handler is required")
	ErrArchiveHandlerRequired       = errors.New("archive handler is required")
	ErrActorMappingHandlerRequired  = errors.New("actor mapping handler is required")
)

// RegisterRoutes registers the governance HTTP routes.
func RegisterRoutes(protected func(resource, action string) fiber.Router, handler *Handler) error {
	if protected == nil {
		return ErrProtectedRouteHelperRequired
	}

	if handler == nil {
		return ErrHandlerRequired
	}

	protected(
		auth.ResourceGovernance,
		auth.ActionAuditRead,
	).Get("/v1/governance/audit-logs", handler.ListAuditLogs)
	protected(
		auth.ResourceGovernance,
		auth.ActionAuditRead,
	).Get("/v1/governance/audit-logs/:id", handler.GetAuditLog)
	protected(
		auth.ResourceGovernance,
		auth.ActionAuditRead,
	).Get("/v1/governance/entities/:entityType/:entityId/audit-logs", handler.ListAuditLogsByEntity)

	return nil
}

// RegisterArchiveRoutes registers the archive retrieval HTTP routes.
func RegisterArchiveRoutes(protected func(resource, action string) fiber.Router, handler *ArchiveHandler) error {
	if protected == nil {
		return ErrProtectedRouteHelperRequired
	}

	if handler == nil {
		return ErrArchiveHandlerRequired
	}

	protected(
		auth.ResourceGovernance,
		auth.ActionArchiveRead,
	).Get("/v1/governance/archives", handler.ListArchives)
	protected(
		auth.ResourceGovernance,
		auth.ActionArchiveRead,
	).Get("/v1/governance/archives/:id/download", handler.DownloadArchive)

	return nil
}

// RegisterActorMappingRoutes registers the actor mapping HTTP routes.
func RegisterActorMappingRoutes(
	protected func(resource, action string) fiber.Router,
	handler *ActorMappingHandler,
) error {
	if protected == nil {
		return ErrProtectedRouteHelperRequired
	}

	if handler == nil {
		return ErrActorMappingHandlerRequired
	}

	protected(
		auth.ResourceGovernance,
		auth.ActionActorMappingWrite,
	).Put("/v1/governance/actor-mappings/:actorId", handler.UpsertActorMapping)
	protected(
		auth.ResourceGovernance,
		auth.ActionActorMappingRead,
	).Get("/v1/governance/actor-mappings/:actorId", handler.GetActorMapping)
	protected(
		auth.ResourceGovernance,
		auth.ActionActorMappingWrite,
	).Post("/v1/governance/actor-mappings/:actorId/pseudonymize", handler.PseudonymizeActor)
	protected(
		auth.ResourceGovernance,
		auth.ActionActorMappingDelete,
	).Delete("/v1/governance/actor-mappings/:actorId", handler.DeleteActorMapping)

	return nil
}
