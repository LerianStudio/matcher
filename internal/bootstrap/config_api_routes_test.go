// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"net/http"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/auth"
)

type protectedCall struct {
	resource string
	action   string
}

func TestRegisterConfigAPIRoutes_Success(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{}, false)
	require.NoError(t, err)

	app := fiber.New()
	protectedCalls := make([]protectedCall, 0)

	protected := func(resource, action string) fiber.Router {
		protectedCalls = append(protectedCalls, protectedCall{resource: resource, action: action})

		// Return a group that mimics auth middleware without enforcing it.
		return app.Group("")
	}

	err = RegisterConfigAPIRoutes(protected, handler)
	assert.NoError(t, err)

	expectedCalls := []protectedCall{
		{resource: auth.ResourceSystem, action: auth.ActionConfigRead},
		{resource: auth.ResourceSystem, action: auth.ActionConfigRead},
		{resource: auth.ResourceSystem, action: auth.ActionConfigRead},
		{resource: auth.ResourceSystem, action: auth.ActionConfigWrite},
		{resource: auth.ResourceSystem, action: auth.ActionConfigWrite},
	}
	require.Equal(t, expectedCalls, protectedCalls)

	registeredRoutes := make(map[string]bool)
	for _, routesByMethod := range app.Stack() {
		for _, route := range routesByMethod {
			registeredRoutes[route.Method+" "+route.Path] = true
		}
	}

	for _, routeKey := range []string{
		http.MethodGet + " /v1/system/config",
		http.MethodGet + " /v1/system/config/schema",
		http.MethodGet + " /v1/system/config/history",
		http.MethodPatch + " /v1/system/config",
		http.MethodPost + " /v1/system/config/reload",
	} {
		assert.True(t, registeredRoutes[routeKey], "expected route %s to be registered", routeKey)
	}
}

func TestRegisterConfigAPIRoutes_NilProtected_ReturnsError(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{}, false)
	require.NoError(t, err)

	err = RegisterConfigAPIRoutes(nil, handler)
	assert.ErrorIs(t, err, ErrConfigAPIProtectedRequired)
}

func TestRegisterConfigAPIRoutes_NilHandler_ReturnsError(t *testing.T) {
	t.Parallel()

	protected := func(_, _ string) fiber.Router {
		return fiber.New().Group("")
	}

	err := RegisterConfigAPIRoutes(protected, nil)
	assert.ErrorIs(t, err, ErrConfigAPIHandlerRequired)
}

func TestRegisterConfigAPIRoutes_SentinelErrors(t *testing.T) {
	t.Parallel()

	// Verify sentinel error messages are stable — they may appear in logs or API responses.
	assert.Equal(t, "protected route helper is required for config API", ErrConfigAPIProtectedRequired.Error())
	assert.Equal(t, "config API handler is required", ErrConfigAPIHandlerRequired.Error())
}
