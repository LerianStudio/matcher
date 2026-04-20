package auth

import (
	"errors"

	"github.com/gofiber/fiber/v2"

	authMiddleware "github.com/LerianStudio/lib-auth/v3/auth/middleware"
)

// Sentinel errors returned at startup when route configuration is invalid.
var (
	ErrNilTenantExtractor = errors.New("tenant extractor not initialized")
	ErrNoActions          = errors.New("authorization actions not configured")
	ErrEmptyAction        = errors.New("authorization actions contain empty entry")
	ErrNilAuthClient      = errors.New("auth client required when authentication is enabled")
)

// ProtectedGroupWithActionsWithMiddleware validates auth-enabled tokens locally before
// applying all requested authorization checks, then extracts tenant context and
// finally applies additional middleware.
//
// Deprecated: this function wraps the built handler slice in a fiber.Router via
// router.Group("/", handlers...), which Fiber v2 implements as an app-level USE
// entry. Once matcher migrated to the bootstrap.protectedRouter surface (which
// registers each route directly via app.Get/Post/... with the composed chain),
// this helper is retained only as a thin shim around BuildProtectedAuthChain
// for tests and any remaining callers that still depend on the Group shape.
// New code should call BuildProtectedAuthChain directly and attach the returned
// handlers to individual routes.
//
// Validation errors (nil extractor, empty/blank actions) are returned at startup
// so misconfiguration is caught before the server accepts traffic.
func ProtectedGroupWithActionsWithMiddleware(
	router fiber.Router,
	authClient *authMiddleware.AuthClient,
	extractor *TenantExtractor,
	resource string,
	actions []string,
	additionalMiddleware ...fiber.Handler,
) (fiber.Router, error) {
	chain, err := BuildProtectedAuthChain(authClient, extractor, resource, actions)
	if err != nil {
		return nil, err
	}

	handlers := make([]fiber.Handler, 0, len(chain)+len(additionalMiddleware))
	handlers = append(handlers, chain...)
	handlers = append(handlers, additionalMiddleware...)

	return router.Group("/", handlers...), nil
}
