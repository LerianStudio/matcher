package auth

import (
	"github.com/gofiber/fiber/v2"

	authMiddleware "github.com/LerianStudio/lib-auth/v2/auth/middleware"
)

// ProtectedGroupWithMiddleware validates auth-enabled tokens locally before
// authorization, then extracts tenant context and finally applies additional
// middleware (e.g., rate limiter that uses UserID/TenantID).
// Middleware order: ValidateTokenClaims? → Auth → TenantExtract → additionalMiddleware → Handlers.
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
	if authClient != nil && extractor.authEnabled {
		handlers = append(handlers, extractor.validateTenantClaims())
	}

	handlers = append(handlers, Authorize(authClient, resource, action), extractor.ExtractTenant())
	handlers = append(handlers, additionalMiddleware...)

	return router.Group("/", handlers...)
}
