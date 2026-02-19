package auth

import (
	"github.com/gofiber/fiber/v2"

	authMiddleware "github.com/LerianStudio/lib-auth/v2/auth/middleware"
)

// ProtectedGroup creates a route group with tenant extraction and authorization middleware.
func ProtectedGroup(
	router fiber.Router,
	authClient *authMiddleware.AuthClient,
	extractor *TenantExtractor,
	resource, action string,
) fiber.Router {
	if extractor == nil {
		return router.Group("/", func(c *fiber.Ctx) error {
			return fiber.NewError(
				fiber.StatusInternalServerError,
				"tenant extractor not initialized",
			)
		})
	}

	return router.Group("/", extractor.ExtractTenant(), Authorize(authClient, resource, action))
}

// ProtectedGroupWithMiddleware creates a route group with tenant extraction, authorization,
// and additional middleware applied AFTER auth (e.g., rate limiter that uses UserID/TenantID).
// Middleware order: TenantExtract → Auth → additionalMiddleware → Handlers.
func ProtectedGroupWithMiddleware(
	router fiber.Router,
	authClient *authMiddleware.AuthClient,
	extractor *TenantExtractor,
	resource, action string,
	additionalMiddleware ...fiber.Handler,
) fiber.Router {
	if extractor == nil {
		return router.Group("/", func(c *fiber.Ctx) error {
			return fiber.NewError(
				fiber.StatusInternalServerError,
				"tenant extractor not initialized",
			)
		})
	}

	const baseHandlerCount = 2

	handlers := make([]fiber.Handler, 0, baseHandlerCount+len(additionalMiddleware))
	handlers = append(handlers, extractor.ExtractTenant(), Authorize(authClient, resource, action))
	handlers = append(handlers, additionalMiddleware...)

	return router.Group("/", handlers...)
}
