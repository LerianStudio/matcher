// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"

	"github.com/gofiber/fiber/v2"

	authMiddleware "github.com/LerianStudio/lib-auth/v3/auth/middleware"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/net/http/ratelimit"
	"github.com/LerianStudio/lib-commons/v5/commons/systemplane"
	"github.com/LerianStudio/lib-commons/v5/commons/systemplane/admin"

	"github.com/LerianStudio/matcher/internal/auth"
)

var (
	errMountSystemplaneAppRequired           = errors.New("mount systemplane api: app is required")
	errMountSystemplaneAuthorizationRequired = errors.New("mount systemplane api: authorization required")
)

// MountSystemplaneAPI mounts the v5 systemplane admin HTTP routes on the
// Fiber app. Routes are mounted at /system/:namespace and /system/:namespace/:key.
//
// Authorization: when auth is enabled (authClient.Enabled), every /system
// request must present a valid JWT that clears the system:admin RBAC check. The
// middleware chain installed on the /system prefix runs BEFORE admin.Mount's
// handlers, so any unauthenticated or unauthorized caller is rejected with
// 401/403 before any systemplane code executes. When auth is disabled, the
// chain collapses to just the tenant extractor so the default-tenant context is
// still populated and actor extraction stays consistent.
//
// The admin.WithAuthorizer callback provides a defense-in-depth check that the
// upstream middleware populated the user identity when auth is enabled.
func MountSystemplaneAPI(
	app *fiber.App,
	client *systemplane.Client,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	authClient *authMiddleware.AuthClient,
	tenantExtractor *auth.TenantExtractor,
	rateLimiterGetter func() *ratelimit.RateLimiter,
	logger libLog.Logger,
) error {
	if app == nil {
		return errMountSystemplaneAppRequired
	}

	if client == nil {
		return nil // graceful no-op: systemplane not initialized
	}

	// Guard all /system requests behind JWT + RBAC. app.Use accepts a variadic
	// list of handlers, which Fiber executes sequentially as middleware; any
	// error short-circuits the chain. Handlers are registered BEFORE admin.Mount
	// wires the actual systemplane routes, so they run first on every request.
	handlers := buildSystemplaneAuthChain(authClient, tenantExtractor)

	useArgs := make([]any, 0, len(handlers)+2)
	useArgs = append(useArgs, "/system")

	for _, h := range handlers {
		useArgs = append(useArgs, h)
	}

	useArgs = append(useArgs, NewGlobalRateLimit(rateLimiterGetter, cfg, configGetter, settingsResolver))

	app.Use(useArgs...)

	opts := []admin.MountOption{
		admin.WithPathPrefix("/system"),
		admin.WithActorExtractor(func(c *fiber.Ctx) string {
			userID := auth.GetUserID(c.UserContext())
			if userID == "" {
				return "anonymous"
			}

			return userID
		}),
		admin.WithAuthorizer(func(c *fiber.Ctx, _ string) error { //nolint:varnamelen // fiber convention: *fiber.Ctx is idiomatically named 'c' across the codebase.
			// Defense-in-depth: upstream middleware should have populated UserID.
			// When auth is disabled there is no JWT to derive an identity from,
			// so skip the check in that mode (the middleware already permits all).
			if authClient == nil || !authClient.Enabled {
				return nil
			}

			if auth.GetUserID(c.UserContext()) == "" {
				return errMountSystemplaneAuthorizationRequired
			}

			return nil
		}),
	}

	admin.Mount(app, client, opts...)

	if logger != nil {
		logger.Log(context.Background(), libLog.LevelInfo,
			"systemplane admin API mounted on /system/:namespace/:key")
	}

	return nil
}

// buildSystemplaneAuthChain returns the ordered list of Fiber middleware that
// guards /system requests before they reach the admin router.
//
// When auth is enabled the chain is:
//  1. Authorize(system, admin) — RBAC check via the lib-auth plugin, which
//     extracts and validates the bearer token and enforces the permission
//  2. ExtractTenant           — populate tenant/user context for downstream
//     handlers and the admin authorizer callback
//
// When auth is disabled the chain collapses to a single ExtractTenant call so
// the default tenant + any dev X-User-ID header are still materialized on
// c.UserContext(). ExtractTenant short-circuits internally when auth is off.
func buildSystemplaneAuthChain(
	authClient *authMiddleware.AuthClient,
	extractor *auth.TenantExtractor,
) []fiber.Handler {
	if extractor == nil {
		// No extractor configured — fall back to a no-op so the mount still
		// succeeds. Without an extractor we cannot populate tenant context, but
		// the admin authorizer below will still reject requests missing a user
		// ID when auth is enabled.
		return []fiber.Handler{func(c *fiber.Ctx) error { return c.Next() }}
	}

	if authClient == nil || !authClient.Enabled {
		return []fiber.Handler{extractor.ExtractTenant()}
	}

	return []fiber.Handler{
		auth.Authorize(authClient, auth.ResourceSystem, auth.ActionAdmin),
		extractor.ExtractTenant(),
	}
}
