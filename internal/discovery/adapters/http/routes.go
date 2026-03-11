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
	// ErrHandlerRequired indicates handler is nil.
	ErrHandlerRequired = errors.New("discovery handler is required")
	// ErrHandlerNotInitialized indicates handler dependencies are missing.
	ErrHandlerNotInitialized = errors.New("discovery handler dependencies are required")
)

// RegisterRoutes registers all discovery routes with the provided router.
func RegisterRoutes(protected func(resource, action string) fiber.Router, handler *Handler) error {
	if protected == nil {
		return ErrProtectedRouteHelperRequired
	}

	if handler == nil {
		return ErrHandlerRequired
	}

	if handler.command == nil || handler.query == nil {
		return ErrHandlerNotInitialized
	}

	// Status
	protected(
		auth.ResourceDiscovery,
		auth.ActionDiscoveryRead,
	).Get("/v1/discovery/status", handler.GetDiscoveryStatus)

	// Connections
	protected(
		auth.ResourceDiscovery,
		auth.ActionDiscoveryRead,
	).Get("/v1/discovery/connections", handler.ListConnections)
	protected(
		auth.ResourceDiscovery,
		auth.ActionDiscoveryRead,
	).Get("/v1/discovery/connections/:connectionId", handler.GetConnection)
	protected(
		auth.ResourceDiscovery,
		auth.ActionDiscoveryRead,
	).Get("/v1/discovery/connections/:connectionId/schema", handler.GetConnectionSchema)
	protected(
		auth.ResourceDiscovery,
		auth.ActionDiscoveryWrite,
	).Post("/v1/discovery/connections/:connectionId/test", handler.TestConnection)
	protected(
		auth.ResourceDiscovery,
		auth.ActionDiscoveryWrite,
	).Post("/v1/discovery/connections/:connectionId/extractions", handler.StartExtraction)
	protected(
		auth.ResourceDiscovery,
		auth.ActionDiscoveryRead,
	).Get("/v1/discovery/extractions/:extractionId", handler.GetExtraction)
	protected(
		auth.ResourceDiscovery,
		auth.ActionDiscoveryWrite,
	).Post("/v1/discovery/extractions/:extractionId/poll", handler.PollExtraction)

	// Refresh
	protected(
		auth.ResourceDiscovery,
		auth.ActionDiscoveryWrite,
	).Post("/v1/discovery/refresh", handler.RefreshDiscovery)

	return nil
}
