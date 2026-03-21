// Copyright 2025 Lerian Studio.

// Package fiberhttp provides a Fiber-based HTTP transport adapter for the
// systemplane configuration API. It translates HTTP requests/responses into
// Manager service calls and maps domain errors to appropriate HTTP status codes.
package fiberhttp

import (
	"errors"

	"github.com/gofiber/fiber/v2"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/service"
)

var (
	errHandlerManagerRequired  = errors.New("new handler: manager is required")
	errHandlerIdentityRequired = errors.New("new handler: identity resolver is required")
	errHandlerAuthRequired     = errors.New("new handler: authorizer is required")
)

// Handler serves the systemplane HTTP API for configs and settings.
type Handler struct {
	manager  service.Manager
	identity ports.IdentityResolver
	auth     ports.Authorizer
}

// NewHandler creates a new Handler with the given dependencies.
// All three dependencies are required; a nil value causes a construction-time
// error rather than a runtime panic on first use.
func NewHandler(manager service.Manager, identity ports.IdentityResolver, auth ports.Authorizer) (*Handler, error) {
	if domain.IsNilValue(manager) {
		return nil, errHandlerManagerRequired
	}

	if domain.IsNilValue(identity) {
		return nil, errHandlerIdentityRequired
	}

	if domain.IsNilValue(auth) {
		return nil, errHandlerAuthRequired
	}

	return &Handler{manager: manager, identity: identity, auth: auth}, nil
}

// Mount registers all systemplane routes on the given Fiber router.
// The route layout is:
//
//	/v1/system/configs          GET   — list resolved configs
//	/v1/system/configs          PATCH — patch config values
//	/v1/system/configs/schema   GET   — key metadata
//	/v1/system/configs/history  GET   — change audit trail
//	/v1/system/configs/reload   POST  — trigger full reload
//	/v1/system/settings         GET   — list resolved settings
//	/v1/system/settings         PATCH — patch setting values
//	/v1/system/settings/schema  GET   — key metadata
//	/v1/system/settings/history GET   — change audit trail
func (handler *Handler) Mount(router fiber.Router) {
	system := router.Group("/v1/system")

	configs := system.Group("/configs")
	configs.Get("/", handler.requireAuth("system/configs:read"), handler.GetConfigs)
	configs.Patch("/", handler.requireAuth("system/configs:write"), handler.PatchConfigs)
	configs.Get("/schema", handler.requireAuth("system/configs/schema:read"), handler.GetConfigSchema)
	configs.Get("/history", handler.requireAuth("system/configs/history:read"), handler.GetConfigHistory)
	configs.Post("/reload", handler.requireAuth("system/configs/reload:write"), handler.Reload)

	settings := system.Group("/settings")
	settings.Get("/", handler.settingsAuth("read"), handler.GetSettings)
	settings.Patch("/", handler.settingsAuth("write"), handler.PatchSettings)
	settings.Get("/schema", handler.requireAuth("system/settings/schema:read"), handler.GetSettingSchema)
	settings.Get("/history", handler.settingsAuth("history:read"), handler.GetSettingHistory)
}
