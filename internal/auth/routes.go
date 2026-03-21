package auth

import (
	"strings"

	"github.com/gofiber/fiber/v2"

	authMiddleware "github.com/LerianStudio/lib-auth/v2/auth/middleware"
)

// ProtectedGroupWithActionsWithMiddleware validates auth-enabled tokens locally before
// applying all requested authorization checks, then extracts tenant context and
// finally applies additional middleware.
func ProtectedGroupWithActionsWithMiddleware(
	router fiber.Router,
	authClient *authMiddleware.AuthClient,
	extractor *TenantExtractor,
	resource string,
	actions []string,
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

	if len(actions) == 0 {
		return router.Group("/", func(c *fiber.Ctx) error {
			return fiber.NewError(
				fiber.StatusInternalServerError,
				"authorization actions not configured",
			)
		})
	}

	for _, action := range actions {
		if strings.TrimSpace(action) == "" {
			return router.Group("/", func(c *fiber.Ctx) error {
				return fiber.NewError(
					fiber.StatusInternalServerError,
					"authorization actions contain empty entry",
				)
			})
		}
	}

	handlers := make([]fiber.Handler, 0, len(actions)+2+len(additionalMiddleware))
	if authClient != nil && extractor.authEnabled {
		handlers = append(handlers, extractor.validateTenantClaims())
	}

	for _, action := range actions {
		handlers = append(handlers, Authorize(authClient, resource, action))
	}

	handlers = append(handlers, extractor.ExtractTenant())
	handlers = append(handlers, additionalMiddleware...)

	return router.Group("/", handlers...)
}

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
	return ProtectedGroupWithActionsWithMiddleware(
		router,
		authClient,
		extractor,
		resource,
		[]string{action},
		additionalMiddleware...,
	)
}
