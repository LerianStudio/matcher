package auth

import (
	"errors"
	"strings"

	"github.com/gofiber/fiber/v2"

	authMiddleware "github.com/LerianStudio/lib-auth/v3/auth/middleware"
)

// Sentinel errors returned at startup when route configuration is invalid.
var (
	ErrNilTenantExtractor = errors.New("tenant extractor not initialized")
	ErrNoActions          = errors.New("authorization actions not configured")
	ErrEmptyAction        = errors.New("authorization actions contain empty entry")
)

// ProtectedGroupWithActionsWithMiddleware validates auth-enabled tokens locally before
// applying all requested authorization checks, then extracts tenant context and
// finally applies additional middleware.
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
	if extractor == nil {
		return nil, ErrNilTenantExtractor
	}

	if len(actions) == 0 {
		return nil, ErrNoActions
	}

	for _, action := range actions {
		if strings.TrimSpace(action) == "" {
			return nil, ErrEmptyAction
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

	return router.Group("/", handlers...), nil
}
