// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"

	authMiddleware "github.com/LerianStudio/lib-auth/v2/auth/middleware"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	fiberhttp "github.com/LerianStudio/lib-commons/v4/commons/systemplane/adapters/http/fiber"
	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/service"

	"github.com/LerianStudio/matcher/internal/auth"
)

var (
	errMountSystemplaneAppRequired       = errors.New("mount systemplane api: app is required")
	errMountSystemplaneProtectedRequired = errors.New("mount systemplane api: protected router is required")
	errMountSystemplaneManagerRequired   = errors.New("mount systemplane api: manager is required")
)

// MountSystemplaneAPI creates and mounts the systemplane HTTP transport on
// the given Fiber app behind the standard auth middleware chain. The protected
// function must wrap routes with JWT validation, tenant extraction, and
// permission checking — the same chain used by all other Matcher API routes.
func MountSystemplaneAPI(
	app *fiber.App,
	authClient *authMiddleware.AuthClient,
	protected func(resource string, actions ...string) fiber.Router,
	manager service.Manager,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	authEnabled bool,
	logger libLog.Logger,
) error {
	if app == nil {
		return errMountSystemplaneAppRequired
	}

	if protected == nil {
		return errMountSystemplaneProtectedRequired
	}

	if domain.IsNilValue(manager) {
		return errMountSystemplaneManagerRequired
	}

	identity := &MatcherIdentityResolver{}
	authorizer := NewMatcherAuthorizer(authEnabled)
	aliasAwareManager := newAliasAwareSystemplaneManager(manager)

	handler, err := fiberhttp.NewHandler(aliasAwareManager, identity, authorizer)
	if err != nil {
		return fmt.Errorf("mount systemplane api: create handler: %w", err)
	}

	runtimeManager, hasRuntimeAccess := manager.(systemplaneRuntimeManager)
	if hasRuntimeAccess && !hasCompleteRuntimeAccess(runtimeManager) {
		hasRuntimeAccess = false
		runtimeManager = nil
	}

	customHandler := newMatcherSystemplaneHandler(aliasAwareManager, runtimeManager, configGetter, settingsResolver)

	// Mount each route behind the full auth middleware chain (JWT validation,
	// tenant extraction, permission check, idempotency, rate limiting).
	// The handler's own requireAuth/settingsAuth still runs as defense-in-depth.
	mountSystemplaneRoutesProtected(authClient, protected, handler, customHandler, hasRuntimeAccess)

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
	protected func(resource string, actions ...string) fiber.Router,
	handler *fiberhttp.Handler,
	customHandler *matcherSystemplaneHandler,
	hasRuntimeAccess bool,
) {
	// Configs routes — admin/system runtime configuration.
	configsGet := handler.GetConfigs
	configsPatch := handler.PatchConfigs
	settingsGet := handler.GetSettings
	settingsPatch := handler.PatchSettings

	if hasRuntimeAccess && customHandler != nil {
		configsGet = customHandler.getConfigs
		configsPatch = customHandler.patchConfigs
		settingsGet = customHandler.getSettings
		settingsPatch = customHandler.patchSettings
	}

	protected(auth.ResourceSystem, auth.ActionConfigRead).
		Get("/v1/system/configs", configsGet)
	protected(auth.ResourceSystem, auth.ActionConfigWrite).
		Patch("/v1/system/configs", configsPatch)
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
		Get("/v1/system/settings", settingsScopeAuthorization(authClient, auth.ActionSettingsGlobalRead), settingsGet)
	protected(auth.ResourceSystem, auth.ActionSettingsWrite).
		Patch("/v1/system/settings", settingsScopeAuthorization(authClient, auth.ActionSettingsGlobalWrite), settingsPatch)
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

func hasCompleteRuntimeAccess(manager systemplaneRuntimeManager) bool {
	if manager == nil {
		return false
	}

	return !domain.IsNilValue(manager.registry()) &&
		!domain.IsNilValue(manager.store()) &&
		!domain.IsNilValue(manager.supervisor())
}
