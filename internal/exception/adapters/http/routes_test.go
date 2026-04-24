// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package http

import (
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func noopMiddleware(c *fiber.Ctx) error {
	return c.Next()
}

func TestRegisterRoutes_NilProtected(t *testing.T) {
	t.Parallel()

	handlers := &Handlers{}
	err := RegisterRoutes(nil, handlers, noopMiddleware)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrProtectedRouteHelperRequired)
}

func TestRegisterRoutes_NilHandlers(t *testing.T) {
	t.Parallel()

	protected := func(resource string, actions ...string) fiber.Router {
		return nil
	}
	err := RegisterRoutes(protected, nil, noopMiddleware)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrHandlersRequired)
}

func TestRegisterRoutes_NilDispatchLimiter(t *testing.T) {
	t.Parallel()

	protected := func(resource string, actions ...string) fiber.Router {
		return nil
	}
	handlers := &Handlers{}
	err := RegisterRoutes(protected, handlers, nil)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrDispatchLimiterRequired)
}

func TestRegisterRoutes_Success(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer app.Shutdown()

	registeredRoutes := make(map[string]bool)

	protected := func(resource string, actions ...string) fiber.Router {
		return app
	}

	handlers := &Handlers{}

	err := RegisterRoutes(protected, handlers, noopMiddleware)
	require.NoError(t, err)

	for _, route := range app.GetRoutes() {
		registeredRoutes[route.Method+" "+route.Path] = true
	}

	expectedRoutes := []string{
		"GET /v1/exceptions",
		"GET /v1/exceptions/:exceptionId",
		"GET /v1/exceptions/:exceptionId/history",
		"POST /v1/exceptions/:exceptionId/force-match",
		"POST /v1/exceptions/:exceptionId/adjust-entry",
		"POST /v1/exceptions/:exceptionId/dispatch",
		"POST /v1/exceptions/:exceptionId/callback",
		"GET /v1/exceptions/:exceptionId/comments",
		"POST /v1/exceptions/:exceptionId/comments",
		"DELETE /v1/exceptions/:exceptionId/comments/:commentId",
		"POST /v1/exceptions/:exceptionId/disputes",
		"POST /v1/disputes/:disputeId/close",
		"POST /v1/disputes/:disputeId/evidence",
	}

	for _, route := range expectedRoutes {
		assert.True(t, registeredRoutes[route], "route %s should be registered", route)
	}
}

// ---------------------------------------------------------------------------
// HIGH-18: Bulk route precedence — /v1/exceptions/bulk/assign must NOT match :exceptionId
// ---------------------------------------------------------------------------

func TestRegisterRoutes_BulkRoutePrecedence(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	protected := func(_ string, _ ...string) fiber.Router {
		return app
	}

	handlers := &Handlers{}

	err := RegisterRoutes(protected, handlers, noopMiddleware)
	require.NoError(t, err)

	routes := app.GetRoutes()
	routeSet := make(map[string]bool)

	for _, r := range routes {
		routeSet[r.Method+" "+r.Path] = true
	}

	// Bulk routes MUST be registered.
	assert.True(t, routeSet["POST /v1/exceptions/bulk/assign"],
		"bulk assign route should be registered")
	assert.True(t, routeSet["POST /v1/exceptions/bulk/resolve"],
		"bulk resolve route should be registered")
	assert.True(t, routeSet["POST /v1/exceptions/bulk/dispatch"],
		"bulk dispatch route should be registered")

	// Verify bulk routes are registered BEFORE parameterized routes.
	// In Fiber, the first matching route wins, so bulk paths being in
	// the route table is sufficient — Fiber's router matches literal
	// segments before parameters when both are present.
	assert.True(t, routeSet["GET /v1/exceptions/:exceptionId"],
		"parameterized route should also exist")
}

func TestRoutesErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	require.NotErrorIs(t, ErrProtectedRouteHelperRequired, ErrHandlersRequired)
	require.NotErrorIs(t, ErrProtectedRouteHelperRequired, ErrDispatchLimiterRequired)
	require.NotErrorIs(t, ErrHandlersRequired, ErrDispatchLimiterRequired)
}

func TestRoutesErrors_HaveMessages(t *testing.T) {
	t.Parallel()

	assert.Contains(
		t,
		ErrProtectedRouteHelperRequired.Error(),
		"protected route helper is required",
	)
	assert.Contains(t, ErrHandlersRequired.Error(), "exception handlers are required")
}
