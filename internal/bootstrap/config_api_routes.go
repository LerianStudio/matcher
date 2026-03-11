// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"errors"

	"github.com/gofiber/fiber/v2"

	"github.com/LerianStudio/matcher/internal/auth"
)

// Sentinel errors for config API route registration.
var (
	ErrConfigAPIProtectedRequired = errors.New("protected route helper is required for config API")
	ErrConfigAPIHandlerRequired   = errors.New("config API handler is required")
)

// RegisterConfigAPIRoutes registers the system config API HTTP routes.
// Read endpoints use ActionConfigRead; write endpoints use ActionConfigWrite.
func RegisterConfigAPIRoutes(
	protected func(resource, action string) fiber.Router,
	handler *ConfigAPIHandler,
) error {
	if protected == nil {
		return ErrConfigAPIProtectedRequired
	}

	if handler == nil {
		return ErrConfigAPIHandlerRequired
	}

	// Read endpoints — require config:read permission.
	protected(
		auth.ResourceSystem,
		auth.ActionConfigRead,
	).Get("/v1/system/config", handler.GetConfig)

	protected(
		auth.ResourceSystem,
		auth.ActionConfigRead,
	).Get("/v1/system/config/schema", handler.GetSchema)

	protected(
		auth.ResourceSystem,
		auth.ActionConfigRead,
	).Get("/v1/system/config/history", handler.GetConfigHistory)

	// Write endpoints — require config:write permission.
	protected(
		auth.ResourceSystem,
		auth.ActionConfigWrite,
	).Patch("/v1/system/config", handler.UpdateConfig)

	protected(
		auth.ResourceSystem,
		auth.ActionConfigWrite,
	).Post("/v1/system/config/reload", handler.ReloadConfig)

	return nil
}
