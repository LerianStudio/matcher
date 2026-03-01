package bootstrap

import (
	"context"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
	fiberSwagger "github.com/swaggo/fiber-swagger"

	authMiddleware "github.com/LerianStudio/lib-auth/v2/auth/middleware"
	"github.com/LerianStudio/lib-uncommons/v2/uncommons/assert"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"

	swagger "github.com/LerianStudio/matcher/docs/swagger"
	"github.com/LerianStudio/matcher/internal/auth"
	sharedHTTP "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// TenantMiddlewareProvider is an interface for tenant middleware that can provide
// a Fiber handler for tenant database resolution.
type TenantMiddlewareProvider interface {
	// WithTenantDB returns a Fiber handler that extracts tenant context and resolves DB connection.
	WithTenantDB(c *fiber.Ctx) error
	// Enabled returns whether the middleware is enabled.
	Enabled() bool
}

// Routes holds the configured API route groups.
type Routes struct {
	API       fiber.Router
	Protected func(resource, action string) fiber.Router
}

// RegisterRoutesOptions contains optional configuration for RegisterRoutes.
type RegisterRoutesOptions struct {
	// TenantMiddleware is the optional lib-commons v3 tenant middleware for multi-tenant mode.
	// When set and enabled, it resolves tenant-specific database connections via Tenant Manager API.
	TenantMiddleware TenantMiddlewareProvider
}

// RegisterRoutes configures health endpoints and API route groups with authentication.
// Middleware order: Auth -> TenantExtract -> TenantDB (optional) -> Idempotency -> RateLimiter -> Handlers
// Idempotency middleware is applied before rate limiting to ensure duplicate requests
// are handled correctly even if rate limiting would otherwise block them.
// If rateLimitStorage is provided, uses it for distributed rate limiting across multiple instances.
func RegisterRoutes(
	app *fiber.App,
	cfg *Config,
	deps *HealthDependencies,
	logger libLog.Logger,
	authClient *authMiddleware.AuthClient,
	tenantExtractor *auth.TenantExtractor,
	rateLimitStorage fiber.Storage,
	idempotencyRepo sharedHTTP.IdempotencyRepository,
	opts ...RegisterRoutesOptions,
) (*Routes, error) {
	asserter := assert.New(
		context.Background(),
		logger,
		constants.ApplicationName,
		"bootstrap.register_routes",
	)

	if err := asserter.NotNil(context.Background(), app, "fiber app is required"); err != nil {
		return nil, fmt.Errorf("register routes: %w", err)
	}

	if err := asserter.NotNil(context.Background(), cfg, "config is required"); err != nil {
		return nil, fmt.Errorf("register routes: %w", err)
	}

	if err := asserter.NotNil(context.Background(), tenantExtractor, "tenant extractor is required"); err != nil {
		return nil, fmt.Errorf("register routes: %w", err)
	}

	app.Get("/health", healthHandler)
	app.Get("/ready", readinessHandler(cfg, deps, logger))

	if cfg.Swagger.Enabled && !IsProductionEnvironment(cfg.App.EnvName) {
		// Override the generated spec host when SWAGGER_HOST is set.
		// When empty, the spec omits the host field and Swagger UI
		// defaults to the request's own host (works for most setups).
		if cfg.Swagger.Host != "" {
			swagger.SwaggerInfo.Host = cfg.Swagger.Host
		}

		// Override the generated spec schemes at runtime.
		// The static annotation defaults to ["https"]; this allows
		// development/staging environments to use ["http"] for local testing.
		if cfg.Swagger.Schemes != "" {
			swagger.SwaggerInfo.Schemes = parseSchemes(cfg.Swagger.Schemes)
		}

		app.Get("/swagger/*", fiberSwagger.WrapHandler)
	}

	idempotencyMiddleware := sharedHTTP.NewIdempotencyMiddleware(
		sharedHTTP.IdempotencyMiddlewareConfig{
			Repository: idempotencyRepo,
			KeyPrefix:  "matcher",
			SkipPaths: []string{
				"/health",
				"/ready",
			},
		},
	)

	// Extract tenant middleware from options if provided
	var tenantDBMiddleware fiber.Handler
	if len(opts) > 0 && opts[0].TenantMiddleware != nil && opts[0].TenantMiddleware.Enabled() {
		tenantDBMiddleware = opts[0].TenantMiddleware.WithTenantDB
	}

	var protected func(resource, action string) fiber.Router

	if cfg.RateLimit.Enabled {
		rateLimiter := NewRateLimiter(cfg, rateLimitStorage)
		protected = func(resource, action string) fiber.Router {
			middlewares := buildProtectedMiddlewares(idempotencyMiddleware, rateLimiter, tenantDBMiddleware)

			return auth.ProtectedGroupWithMiddleware(
				app,
				authClient,
				tenantExtractor,
				resource,
				action,
				middlewares...,
			)
		}
	} else {
		protected = func(resource, action string) fiber.Router {
			middlewares := buildProtectedMiddlewares(idempotencyMiddleware, nil, tenantDBMiddleware)

			return auth.ProtectedGroupWithMiddleware(app, authClient, tenantExtractor, resource, action, middlewares...)
		}
	}

	return &Routes{
		API:       app.Group(""),
		Protected: protected,
	}, nil
}

// buildProtectedMiddlewares constructs the middleware slice for protected routes.
// Order: TenantDB (optional) -> Idempotency -> RateLimiter (optional)
// TenantDB middleware is applied first (after auth) to resolve tenant-specific connections
// before other middleware that may need database access.
func buildProtectedMiddlewares(
	idempotency fiber.Handler,
	rateLimiter fiber.Handler,
	tenantDB fiber.Handler,
) []fiber.Handler {
	// Estimate capacity: idempotency always, rateLimiter optional, tenantDB optional
	capacity := 1
	if rateLimiter != nil {
		capacity++
	}

	if tenantDB != nil {
		capacity++
	}

	middlewares := make([]fiber.Handler, 0, capacity)

	// TenantDB middleware comes first (after auth) to resolve connections
	if tenantDB != nil {
		middlewares = append(middlewares, tenantDB)
	}

	// Idempotency middleware
	middlewares = append(middlewares, idempotency)

	// Rate limiter last
	if rateLimiter != nil {
		middlewares = append(middlewares, rateLimiter)
	}

	return middlewares
}

// parseSchemes splits a comma-separated schemes string into a trimmed slice.
// Only "http" and "https" values are accepted; unknown values are silently dropped.
func parseSchemes(raw string) []string {
	parts := strings.Split(raw, ",")
	schemes := make([]string, 0, len(parts))

	for _, p := range parts {
		s := strings.TrimSpace(strings.ToLower(p))
		if s == "http" || s == "https" {
			schemes = append(schemes, s)
		}
	}

	// Fall back to https-only if the input produced no valid schemes.
	if len(schemes) == 0 {
		return []string{"https"}
	}

	return schemes
}
