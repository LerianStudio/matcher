// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
	fiberSwagger "github.com/swaggo/fiber-swagger"

	authMiddleware "github.com/LerianStudio/lib-auth/v3/auth/middleware"
	"github.com/LerianStudio/lib-commons/v5/commons/assert"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/net/http/ratelimit"

	swagger "github.com/LerianStudio/matcher/docs/swagger"
	"github.com/LerianStudio/matcher/internal/auth"
	sharedHTTP "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Routes holds the configured API route groups.
type Routes struct {
	API       fiber.Router
	Protected func(resource string, actions ...string) fiber.Router

	// registrationErrs accumulates errors from the Protected closure.
	// Since Protected returns fiber.Router (no error), failures during
	// auth group creation are collected here and surfaced via RegistrationErr()
	// after all modules have registered their routes.
	registrationErrs []error
}

// RegistrationErr returns any errors that occurred during Protected route
// registration. Callers must check this after all route registrations complete
// to ensure no auth group creations failed silently.
func (r *Routes) RegistrationErr() error {
	return errors.Join(r.registrationErrs...)
}

// WhenEnabled returns a Fiber handler that delegates to the given middleware when
// it is non-nil, or calls c.Next() (pass-through) when nil. This enables conditional
// middleware registration: in single-tenant mode the tenant DB middleware is nil,
// so it becomes a no-op without affecting the handler chain.
func WhenEnabled(middleware fiber.Handler) fiber.Handler {
	return func(c *fiber.Ctx) error {
		if middleware == nil {
			return c.Next()
		}

		return middleware(c)
	}
}

// RegisterRoutes configures health endpoints and API route groups with authentication.
// Middleware order: Auth -> TenantExtract -> TenantDB (multi-tenant) -> Idempotency -> RateLimiter -> Handlers
// When multi-tenant mode is enabled, the TenantDB middleware resolves the per-tenant
// database connection from the canonical lib-commons tenant-manager and stores it in context.
// Idempotency middleware is applied before rate limiting to ensure duplicate requests
// are handled correctly even if rate limiting would otherwise block them.
// The rateLimiter uses lib-commons ratelimit for distributed Redis-backed rate limiting.
func RegisterRoutes(
	app *fiber.App,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	readiness *readinessState,
	deps *HealthDependencies,
	logger libLog.Logger,
	authClient *authMiddleware.AuthClient,
	tenantExtractor *auth.TenantExtractor,
	rateLimiterGetter func() *ratelimit.RateLimiter,
	idempotencyRepo sharedPorts.IdempotencyRepository,
	tenantDBHandler fiber.Handler,
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

	app.Get("/health", livenessHandler)
	app.Get("/ready", readinessHandler(cfg, configGetter, drainingGetter, deps, logger))

	// /version is registered unconditionally (including production).
	// This is intentional: operational tooling (Kubernetes probes, deployment
	// verification scripts, CI/CD pipelines) relies on /version to confirm
	// which build is running.  The endpoint returns only the build tag and
	// timestamp — no sensitive configuration, secrets, or internal topology.
	// If stricter control is needed in the future, gate it behind auth
	// rather than removing it, so that automated tooling can still reach it
	// with a service account token.
	app.Get("/version", versionHandler)

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
				"/version",
				// /system/* is the v5 systemplane admin plane. Its PUTs are the
				// canonical "write runtime config" mutation — callers are
				// expected to be able to flip a value back and forth by
				// re-sending the same body (e.g., PUT true, ops review, PUT
				// false, PUT true again). Caching on body-hash would replay
				// the first response indefinitely and make the admin API
				// silently non-functional after the first successful write to
				// a given (key, value) pair. The admin plane has its own
				// dedicated rate limiter for DoS protection (NewAdminRateLimit),
				// so idempotency caching adds no safety here — only breakage.
				"/system",
			},
		},
	)

	globalRateLimit := NewGlobalRateLimit(rateLimiterGetter, cfg, configGetter, settingsResolver)

	routes := &Routes{
		API: app.Group(""),
	}

	// sharedChain is the per-protected-route middleware slice applied AFTER
	// the auth chain (validateTenantClaims → Authorize → ExtractTenant) and
	// BEFORE the user-supplied handlers. Order matters:
	//
	//   1. WhenEnabled(tenantDBHandler) — multi-tenant DB pinning. Runs first
	//      so idempotency lookups and rate-limit counters are keyed against
	//      the correctly-scoped tenant DB.
	//   2. idempotencyMiddleware       — before rate limiting, so a duplicate
	//      request that should replay a cached response is not rejected as
	//      429 before the middleware gets a chance to answer from the cache.
	//   3. globalRateLimit             — last in the shared chain, first line
	//      of defense against handler-overload.
	//
	// Previously this slice was passed as the additionalMiddleware varargs of
	// auth.ProtectedGroupWithActionsWithMiddleware, which in turn called
	// router.Group("/", handlers...) and thereby installed every handler as
	// an app-level USE entry. Each of the 114 protected(...) invocations in
	// the bounded contexts stacked another copy, so a single HTTP request
	// ran every middleware 114 times. The protectedRouter wiring below
	// registers each route directly on the app with the composed chain, so
	// each middleware runs EXACTLY ONCE per matching request.
	sharedChain := []fiber.Handler{
		WhenEnabled(tenantDBHandler),
		idempotencyMiddleware,
		globalRateLimit,
	}

	routes.Protected = func(resource string, actions ...string) fiber.Router {
		authChain, err := auth.BuildProtectedAuthChain(
			authClient,
			tenantExtractor,
			resource,
			actions,
		)
		if err != nil {
			// This closure is called during route registration at startup,
			// not at request time. tenantExtractor is validated non-nil above
			// and actions are hardcoded string literals in every call site,
			// so an error here indicates a programmer bug in route definitions.
			// The error is collected and surfaced via RegistrationErr() after
			// all modules complete registration; the stub router prevents nil
			// dereferences on chained .Post()/.Get() calls.
			routes.registrationErrs = append(routes.registrationErrs, fmt.Errorf(
				"protected route registration failed for resource=%q actions=%v: %w",
				resource, actions, err,
			))

			// Return a no-op router so downstream .Get()/.Post() calls do
			// not nil-dereference. The router has no authChain and records
			// every verb call as a no-op (route is never registered on the
			// app), so any hit against the misconfigured path yields a
			// Fiber default 404 rather than a privileged handler running.
			return &protectedRouter{
				app:       app,
				logger:    logger,
				label:     fmt.Sprintf("resource=%q actions=%v (registration failed)", resource, actions),
				recordErr: func(e error) { routes.registrationErrs = append(routes.registrationErrs, e) },
			}
		}

		return &protectedRouter{
			app:         app,
			authChain:   authChain,
			sharedChain: sharedChain,
			logger:      logger,
			label:       fmt.Sprintf("resource=%q actions=%v", resource, actions),
			recordErr: func(e error) {
				routes.registrationErrs = append(routes.registrationErrs, e)
			},
		}
	}

	return routes, nil
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
