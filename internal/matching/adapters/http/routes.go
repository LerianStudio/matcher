// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package http provides HTTP route adapters for matching operations.
// It registers protected routes and exposes matching handlers.
package http

import (
	"errors"

	"github.com/gofiber/fiber/v2"

	"github.com/LerianStudio/matcher/internal/auth"
)

// Sentinel errors for route registration.
var (
	ErrProtectedRouteHelperRequired = errors.New("protected route helper is required")
	ErrHandlerRequired              = errors.New("handler is required")
)

// RegisterRoutes registers the matching HTTP routes.
func RegisterRoutes(protected func(resource string, actions ...string) fiber.Router, handler *Handler) error {
	if protected == nil {
		return ErrProtectedRouteHelperRequired
	}

	if handler == nil {
		return ErrHandlerRequired
	}

	protected(
		auth.ResourceMatching,
		auth.ActionMatchRun,
	).Post("/v1/matching/contexts/:contextId/run", handler.RunMatch)
	protected(
		auth.ResourceMatching,
		auth.ActionMatchRead,
	).Get("/v1/matching/contexts/:contextId/runs", handler.ListMatchRuns)
	protected(
		auth.ResourceMatching,
		auth.ActionMatchRead,
	).Get("/v1/matching/runs/:runId", handler.GetMatchRun)
	protected(
		auth.ResourceMatching,
		auth.ActionMatchRead,
	).Get("/v1/matching/runs/:runId/groups", handler.GetMatchRunResults)
	protected(
		auth.ResourceMatching,
		auth.ActionMatchDelete,
	).Delete("/v1/matching/groups/:matchGroupId", handler.Unmatch)
	protected(
		auth.ResourceMatching,
		auth.ActionManualMatch,
	).Post("/v1/matching/manual", handler.CreateManualMatch)
	protected(
		auth.ResourceMatching,
		auth.ActionAdjustmentCreate,
	).Post("/v1/matching/adjustments", handler.CreateAdjustment)

	return nil
}
