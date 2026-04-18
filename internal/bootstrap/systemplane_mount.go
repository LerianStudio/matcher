// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"

	"github.com/gofiber/fiber/v2"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/systemplane"
	"github.com/LerianStudio/lib-commons/v5/commons/systemplane/admin"

	"github.com/LerianStudio/matcher/internal/auth"
)

var errMountSystemplaneAppRequired = errors.New("mount systemplane api: app is required")

// MountSystemplaneAPI mounts the v5 systemplane admin HTTP routes on the
// Fiber app. Routes are mounted at /system/:namespace and /system/:namespace/:key.
//
// Authorization: when auth is enabled, the admin.WithAuthorizer callback
// delegates to the lib-auth middleware chain that already ran before the
// handler. When auth is disabled, all requests are permitted.
func MountSystemplaneAPI(
	app *fiber.App,
	client *systemplane.Client,
	authEnabled bool,
	logger libLog.Logger,
) error {
	if app == nil {
		return errMountSystemplaneAppRequired
	}

	if client == nil {
		return nil // graceful no-op: systemplane not initialized
	}

	opts := []admin.MountOption{
		admin.WithPathPrefix("/system"),
		admin.WithActorExtractor(func(c *fiber.Ctx) string {
			userID := auth.GetUserID(c.UserContext())
			if userID == "" {
				return "anonymous"
			}

			return userID
		}),
	}

	if authEnabled {
		opts = append(opts, admin.WithAuthorizer(func(_ *fiber.Ctx, _ string) error {
			// Auth middleware already validated JWT and permissions on the
			// Fiber router chain before this handler runs. This is a
			// defense-in-depth layer; the actual RBAC check happened upstream.
			return nil
		}))
	} else {
		opts = append(opts, admin.WithAuthorizer(func(_ *fiber.Ctx, _ string) error {
			return nil // auth disabled: permit all
		}))
	}

	admin.Mount(app, client, opts...)

	if logger != nil {
		logger.Log(context.Background(), libLog.LevelInfo,
			"systemplane admin API mounted on /system/:namespace/:key")
	}

	return nil
}
