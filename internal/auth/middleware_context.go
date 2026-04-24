// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package auth

import (
	"context"
	"strings"

	"github.com/gofiber/fiber/v2"

	"github.com/LerianStudio/lib-auth/v3/auth/middleware"
	"github.com/LerianStudio/lib-commons/v5/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// GetTenantID retrieves the tenant ID from context, returning the default if not set.
func GetTenantID(ctx context.Context) string {
	if ctx == nil {
		return getDefaultTenantID()
	}

	if tid, ok := ctx.Value(TenantIDKey).(string); ok && tid != "" {
		return tid
	}

	return getDefaultTenantID()
}

// LookupTenantID retrieves the tenant ID only when it is explicitly present in
// context. Unlike GetTenantID, it does not fall back to the configured default.
func LookupTenantID(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}

	tid, ok := ctx.Value(TenantIDKey).(string)
	if !ok {
		return "", false
	}

	tid = strings.TrimSpace(tid)
	if tid == "" {
		return "", false
	}

	return tid, true
}

// GetTenantSlug retrieves the tenant slug from context, returning the default if not set.
func GetTenantSlug(ctx context.Context) string {
	if ctx == nil {
		return getDefaultTenantSlug()
	}

	if slug, ok := ctx.Value(TenantSlugKey).(string); ok && slug != "" {
		return slug
	}

	return getDefaultTenantSlug()
}

// GetUserID retrieves the user ID from context, returning empty string if not set.
func GetUserID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	if uid, ok := ctx.Value(UserIDKey).(string); ok {
		return uid
	}

	return ""
}

// Authorize returns a Fiber middleware that checks authorization for the given resource and action.
func Authorize(authClient *middleware.AuthClient, resource, action string) fiber.Handler {
	if authClient == nil {
		// Emit observability for this programming error
		ctx := context.Background()
		asserter := assert.New(ctx, nil, constants.ApplicationName, "auth.authorize")
		_ = asserter.Never(ctx, "auth client not initialized", "resource", resource, "action", action)

		return func(c *fiber.Ctx) error {
			return fiber.NewError(
				fiber.StatusInternalServerError,
				"auth client not initialized",
			)
		}
	}

	return authClient.Authorize(constants.ApplicationName, resource, action)
}
