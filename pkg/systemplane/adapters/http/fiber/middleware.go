// Copyright 2025 Lerian Studio.

package fiberhttp

import "github.com/gofiber/fiber/v2"

// requireAuth returns a Fiber middleware that checks a specific permission
// via the configured Authorizer. If the check fails, it writes an error
// response and halts the chain.
func (handler *Handler) requireAuth(permission string) fiber.Handler {
	return func(fiberCtx *fiber.Ctx) error {
		if err := handler.auth.Authorize(fiberCtx.UserContext(), permission); err != nil {
			return writeError(fiberCtx, err)
		}

		return fiberCtx.Next()
	}
}

// settingsAuth returns middleware that checks permission based on the scope
// query parameter. A ?scope=global request requires elevated "global" permission
// while the default tenant scope uses the tenant-scoped settings permission.
func (handler *Handler) settingsAuth(action string) fiber.Handler {
	return func(fiberCtx *fiber.Ctx) error {
		permission := "system/settings:" + action

		if fiberCtx.Query("scope") == "global" {
			permission = "system/settings/global:" + action
		}

		if err := handler.auth.Authorize(fiberCtx.UserContext(), permission); err != nil {
			return writeError(fiberCtx, err)
		}

		return fiberCtx.Next()
	}
}
