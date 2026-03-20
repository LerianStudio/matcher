// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"

	authMiddleware "github.com/LerianStudio/lib-auth/v2/auth/middleware"
	"github.com/gofiber/fiber/v2"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/auth"
	fiberhttp "github.com/LerianStudio/matcher/pkg/systemplane/adapters/http/fiber"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/service"
)

var (
	errMountSystemplaneAppRequired     = errors.New("mount systemplane api: app is required")
	errMountSystemplaneManagerRequired = errors.New("mount systemplane api: manager is required")
)

// MountSystemplaneAPI creates and mounts the systemplane HTTP transport on
// the given Fiber app behind the standard auth middleware chain. The protected
// function must wrap routes with JWT validation, tenant extraction, and
// permission checking — the same chain used by all other Matcher API routes.
//
// When protected is nil (auth disabled), routes are mounted directly on the app
// with only the handler's own requireAuth middleware as a secondary gate.
func MountSystemplaneAPI(
	app *fiber.App,
	authClient *authMiddleware.AuthClient,
	protected func(resource, action string) fiber.Router,
	manager service.Manager,
	authEnabled bool,
	logger libLog.Logger,
) error {
	if app == nil {
		return errMountSystemplaneAppRequired
	}

	if domain.IsNilValue(manager) {
		return errMountSystemplaneManagerRequired
	}

	identity := &MatcherIdentityResolver{}
	authorizer := NewMatcherAuthorizer(authEnabled)

	handler, err := fiberhttp.NewHandler(manager, identity, authorizer)
	if err != nil {
		return fmt.Errorf("mount systemplane api: create handler: %w", err)
	}

	if protected != nil {
		// Mount each route behind the full auth middleware chain (JWT validation,
		// tenant extraction, permission check, idempotency, rate limiting).
		// The handler's own requireAuth/settingsAuth still runs as defense-in-depth.
		mountSystemplaneRoutesProtected(authClient, protected, handler)
	} else {
		// Auth disabled — mount directly. The handler's own requireAuth middleware
		// provides a secondary gate (MatcherAuthorizer permits all when auth disabled).
		handler.Mount(app)
	}

	if logger != nil {
		logger.Log(context.Background(), libLog.LevelInfo,
			"systemplane API mounted on /v1/system/configs and /v1/system/settings")
	}

	return nil
}

// mountSystemplaneRoutesProtected registers each systemplane route behind
// the auth middleware chain with the appropriate resource+action permission.
// This ensures JWT validation and tenant extraction happen before any handler
// code runs, matching the security model of all other Matcher API routes.
func mountSystemplaneRoutesProtected(
	authClient *authMiddleware.AuthClient,
	protected func(resource, action string) fiber.Router,
	handler *fiberhttp.Handler,
) {
	// Configs routes — admin/system runtime configuration.
	protected(auth.ResourceSystem, auth.ActionConfigRead).
		Get("/v1/system/configs", handler.GetConfigs)
	protected(auth.ResourceSystem, auth.ActionConfigWrite).
		Patch("/v1/system/configs", handler.PatchConfigs)
	protected(auth.ResourceSystem, auth.ActionConfigSchemaRead).
		Get("/v1/system/configs/schema", handler.GetConfigSchema)
	protected(auth.ResourceSystem, auth.ActionConfigHistoryRead).
		Get("/v1/system/configs/history", handler.GetConfigHistory)
	protected(auth.ResourceSystem, auth.ActionConfigReloadWrite).
		Post("/v1/system/configs/reload", handler.Reload)

	// Settings routes — operator-safe tenant/runtime settings.
	// Base permission covers tenant scope; the handler's settingsAuth middleware
	// elevates to global permission when ?scope=global is requested.
	protected(auth.ResourceSystem, auth.ActionSettingsRead).
		Get("/v1/system/settings", settingsScopeAuthorization(authClient, auth.ActionSettingsGlobalRead), handler.GetSettings)
	protected(auth.ResourceSystem, auth.ActionSettingsWrite).
		Patch("/v1/system/settings", settingsScopeAuthorization(authClient, auth.ActionSettingsGlobalWrite), handler.PatchSettings)
	protected(auth.ResourceSystem, auth.ActionSettingsSchemaRead).
		Get("/v1/system/settings/schema", handler.GetSettingSchema)
	protected(auth.ResourceSystem, auth.ActionSettingsHistoryRead).
		Get("/v1/system/settings/history", settingsScopeAuthorization(authClient, auth.ActionSettingsGlobalRead), handler.GetSettingHistory)
}

func settingsScopeAuthorization(authClient *authMiddleware.AuthClient, globalAction string) fiber.Handler {
	if authClient == nil {
		return func(fiberCtx *fiber.Ctx) error {
			return fiberCtx.Next()
		}
	}

	globalAuthorize := auth.Authorize(authClient, auth.ResourceSystem, globalAction)

	return func(fiberCtx *fiber.Ctx) error {
		if fiberCtx.Query("scope") != "global" {
			return fiberCtx.Next()
		}

		return globalAuthorize(fiberCtx)
	}
}
