// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
	fiberSwagger "github.com/swaggo/fiber-swagger"

	authMiddleware "github.com/LerianStudio/lib-auth/v2/auth/middleware"
	"github.com/LerianStudio/lib-commons/v4/commons/assert"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	swagger "github.com/LerianStudio/matcher/docs/swagger"
	"github.com/LerianStudio/matcher/internal/auth"
	sharedHTTP "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// Routes holds the configured API route groups.
type Routes struct {
	API       fiber.Router
	Protected func(resource string, actions ...string) fiber.Router
}

// RegisterRoutes configures health endpoints and API route groups with authentication.
// Middleware order: Auth -> TenantExtract -> Idempotency -> RateLimiter -> Handlers
// Idempotency middleware is applied before rate limiting to ensure duplicate requests
// are handled correctly even if rate limiting would otherwise block them.
// If rateLimitStorage is provided, uses it for distributed rate limiting across multiple instances.
func RegisterRoutes(
	app *fiber.App,
	cfg *Config,
	configGetter func() *Config,
	readiness *readinessState,
	deps *HealthDependencies,
	logger libLog.Logger,
	authClient *authMiddleware.AuthClient,
	tenantExtractor *auth.TenantExtractor,
	rateLimitStorage fiber.Storage,
	idempotencyRepo sharedHTTP.IdempotencyRepository,
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

	var drainingGetter func() bool
	if readiness != nil {
		drainingGetter = readiness.isDraining
	}

	app.Get("/health", healthHandler)
	app.Get("/ready", readinessHandler(cfg, configGetter, drainingGetter, deps, logger))

	if cfg.Swagger.Enabled && !IsProductionEnvironment(cfg.App.EnvName) {
		applySwaggerInfo(cfg)
		app.Get("/swagger/*", runtimeSwaggerHandler(cfg, configGetter, fiberSwagger.WrapHandler))
	}

	if configGetter != nil && !IsProductionEnvironment(cfg.App.EnvName) && !cfg.Swagger.Enabled {
		app.Get("/swagger/*", runtimeSwaggerHandler(cfg, configGetter, fiberSwagger.WrapHandler))
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

	var protected func(resource string, actions ...string) fiber.Router

	rateLimiter := NewRateLimiter(cfg, rateLimitStorage)
	if configGetter != nil {
		rateLimiter = NewDynamicRateLimiter(configGetter, rateLimitStorage)
	}

	protected = func(resource string, actions ...string) fiber.Router {
		return auth.ProtectedGroupWithActionsWithMiddleware(
			app,
			authClient,
			tenantExtractor,
			resource,
			actions,
			idempotencyMiddleware,
			rateLimiter,
		)
	}

	return &Routes{
		API:       app.Group(""),
		Protected: protected,
	}, nil
}

func runtimeSwaggerHandler(initialCfg *Config, configGetter func() *Config, next fiber.Handler) fiber.Handler {
	return func(fiberCtx *fiber.Ctx) error {
		cfg := initialCfg

		if configGetter != nil {
			if runtimeCfg := configGetter(); runtimeCfg != nil {
				cfg = runtimeCfg
			}
		}

		if cfg == nil || !cfg.Swagger.Enabled || IsProductionEnvironment(cfg.App.EnvName) {
			return fiber.ErrNotFound
		}

		applySwaggerInfo(cfg)

		return next(fiberCtx)
	}
}

func applySwaggerInfo(cfg *Config) {
	if cfg == nil {
		return
	}

	if cfg.Swagger.Host != "" {
		swagger.SwaggerInfo.Host = cfg.Swagger.Host
	} else {
		swagger.SwaggerInfo.Host = ""
	}

	if cfg.Swagger.Schemes != "" {
		swagger.SwaggerInfo.Schemes = parseSchemes(cfg.Swagger.Schemes)
	} else {
		swagger.SwaggerInfo.Schemes = []string{"https"}
	}
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
