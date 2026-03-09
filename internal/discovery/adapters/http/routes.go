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
)

// RegisterRoutes registers all discovery routes with the provided router.
func RegisterRoutes(protected func(resource, action string) fiber.Router, handler *Handler) error {
	if protected == nil {
		return ErrProtectedRouteHelperRequired
	}

	if handler == nil {
		return ErrHandlerRequired
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

	// Refresh
	protected(
		auth.ResourceDiscovery,
		auth.ActionDiscoveryWrite,
	).Post("/v1/discovery/refresh", handler.RefreshDiscovery)

	return nil
}
