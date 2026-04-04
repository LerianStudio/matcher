// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authMiddleware "github.com/LerianStudio/lib-auth/v2/auth/middleware"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/ports"
	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/service"
	"github.com/LerianStudio/matcher/internal/auth"
)

type mockManagerForMount struct {
	getConfigsFn      func(context.Context) (service.ResolvedSet, error)
	patchConfigsFn    func(context.Context, service.PatchRequest) (service.WriteResult, error)
	getConfigSchemaFn func(context.Context) ([]service.SchemaEntry, error)
	getHistoryFn      func(context.Context, ports.HistoryFilter) ([]ports.HistoryEntry, error)
}

// mockManagerForMount implements service.Manager with no-op methods,
// sufficient for verifying that MountSystemplaneAPI wires and mounts routes
// without requiring a real backend.
func (m *mockManagerForMount) GetConfigs(ctx context.Context) (service.ResolvedSet, error) {
	if m.getConfigsFn != nil {
		return m.getConfigsFn(ctx)
	}

	return service.ResolvedSet{}, nil
}

func (m *mockManagerForMount) GetSettings(_ context.Context, _ service.Subject) (service.ResolvedSet, error) {
	return service.ResolvedSet{}, nil
}

func (m *mockManagerForMount) PatchConfigs(ctx context.Context, req service.PatchRequest) (service.WriteResult, error) {
	if m.patchConfigsFn != nil {
		return m.patchConfigsFn(ctx, req)
	}

	return service.WriteResult{}, nil
}

func (m *mockManagerForMount) PatchSettings(_ context.Context, _ service.Subject, _ service.PatchRequest) (service.WriteResult, error) {
	return service.WriteResult{}, nil
}

func (m *mockManagerForMount) GetConfigSchema(ctx context.Context) ([]service.SchemaEntry, error) {
	if m.getConfigSchemaFn != nil {
		return m.getConfigSchemaFn(ctx)
	}

	return nil, nil
}

func (m *mockManagerForMount) GetSettingSchema(_ context.Context) ([]service.SchemaEntry, error) {
	return nil, nil
}

func (m *mockManagerForMount) GetConfigHistory(ctx context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error) {
	if m.getHistoryFn != nil {
		return m.getHistoryFn(ctx, filter)
	}

	return nil, nil
}

func (m *mockManagerForMount) GetSettingHistory(_ context.Context, _ ports.HistoryFilter) ([]ports.HistoryEntry, error) {
	return nil, nil
}

func (m *mockManagerForMount) ApplyChangeSignal(_ context.Context, _ ports.ChangeSignal) error {
	return nil
}

func (m *mockManagerForMount) Resync(_ context.Context) error {
	return nil
}

// Compile-time interface check.
var _ service.Manager = (*mockManagerForMount)(nil)

func TestMountSystemplaneAPI_NilApp(t *testing.T) {
	t.Parallel()

	err := MountSystemplaneAPI(nil, nil, nil, &mockManagerForMount{}, nil, nil, false, &libLog.NopLogger{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "app is required")
}

func TestMountSystemplaneAPI_NilManager(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	protected := func(_ string, _ ...string) fiber.Router { return app.Group("") }
	err := MountSystemplaneAPI(app, nil, protected, nil, nil, nil, false, &libLog.NopLogger{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "manager is required")
}

func TestMountSystemplaneAPI_NilProtected(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	err := MountSystemplaneAPI(app, nil, nil, &mockManagerForMount{}, nil, nil, false, &libLog.NopLogger{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "protected router is required")
}

func TestMountSystemplaneAPI_Success(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	protected := func(_ string, _ ...string) fiber.Router { return app.Group("") }
	err := MountSystemplaneAPI(app, nil, protected, &mockManagerForMount{}, nil, nil, false, &libLog.NopLogger{})
	require.NoError(t, err)
}

func TestMountSystemplaneAPI_NilLogger(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	protected := func(_ string, _ ...string) fiber.Router { return app.Group("") }

	// A nil logger should not cause a panic — the function guards it.
	err := MountSystemplaneAPI(app, nil, protected, &mockManagerForMount{}, nil, nil, false, nil)
	require.NoError(t, err)
}

func TestMountSystemplaneAPI_RoutesRegistered(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	protected := func(_ string, _ ...string) fiber.Router { return app.Group("") }
	err := MountSystemplaneAPI(app, nil, protected, &mockManagerForMount{}, nil, nil, false, &libLog.NopLogger{})
	require.NoError(t, err)

	// Build a set of registered routes from the Fiber app stack.
	registeredRoutes := make(map[string]bool)
	for _, routesByMethod := range app.Stack() {
		for _, route := range routesByMethod {
			registeredRoutes[route.Method+" "+route.Path] = true
		}
	}

	// Verify the expected systemplane routes are present.
	expectedRoutes := []string{
		http.MethodGet + " /v1/system/configs",
		http.MethodPatch + " /v1/system/configs",
		http.MethodGet + " /v1/system/configs/schema",
		http.MethodGet + " /v1/system/configs/history",
		http.MethodPost + " /v1/system/configs/reload",
		http.MethodGet + " /v1/system/settings",
		http.MethodPatch + " /v1/system/settings",
		http.MethodGet + " /v1/system/settings/schema",
		http.MethodGet + " /v1/system/settings/history",
	}

	for _, routeKey := range expectedRoutes {
		assert.True(t, registeredRoutes[routeKey],
			"expected route %q to be registered; registered routes: %v", routeKey, registeredRoutes)
	}
}

func TestSettingsScopeAuthorization_OnlyChecksGlobalScope(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	authClient := authMiddleware.NewAuthClient("http://auth.example", true, nil)
	app.Get("/test", settingsScopeAuthorization(authClient, auth.ActionSettingsGlobalRead), func(c *fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})

	t.Run("tenant scope skips elevated auth", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		resp, err := app.Test(req, -1)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("global scope enforces elevated auth", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test?scope=global", nil)
		resp, err := app.Test(req, -1)
		require.NoError(t, err)
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})
}

func TestMountSystemplaneAPI_ProtectedRoutesServeRequests(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	manager := &mockManagerForMount{}
	managerGetCalled := false
	manager.getConfigsFn = func(_ context.Context) (service.ResolvedSet, error) {
		managerGetCalled = true
		return service.ResolvedSet{
			Values: map[string]domain.EffectiveValue{
				"app.log_level": {Key: "app.log_level", Value: "info", Source: "default"},
			},
			Revision: 1,
		}, nil
	}

	protected := func(_ string, _ ...string) fiber.Router { return app.Group("") }
	err := MountSystemplaneAPI(app, nil, protected, manager, nil, nil, false, &libLog.NopLogger{})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/v1/system/configs", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, managerGetCalled)

	var payload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	assert.Equal(t, float64(1), payload["revision"])
}

func TestMountSystemplaneAPI_IncompleteRuntimeManagerFallsBackToBaseHandler(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	manager := &runtimeHandlerManagerStub{
		getConfigs: func(_ context.Context) (service.ResolvedSet, error) {
			return service.ResolvedSet{
				Values: map[string]domain.EffectiveValue{
					"app.log_level": {Key: "app.log_level", Value: "info", Source: "default"},
				},
				Revision: 1,
			}, nil
		},
		// Intentionally nil runtime dependencies. If the custom runtime handler is
		// mounted, these requests would panic when it tries to access them.
		runtimeRegistry:   nil,
		runtimeStore:      nil,
		runtimeSupervisor: nil,
	}

	protected := func(_ string, _ ...string) fiber.Router { return app.Group("") }
	err := MountSystemplaneAPI(app, nil, protected, manager, nil, nil, false, &libLog.NopLogger{})
	require.NoError(t, err)

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/v1/system/configs", nil), -1)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestLegacyConfigRoutes_NotMounted(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	protected := func(_ string, _ ...string) fiber.Router { return app.Group("") }
	err := MountSystemplaneAPI(app, nil, protected, &mockManagerForMount{}, nil, nil, false, &libLog.NopLogger{})
	require.NoError(t, err)

	for _, path := range []string{"/v1/configs", "/api/v1/configs", "/v1/config", "/api/v1/config"} {
		req := httptest.NewRequest(http.MethodGet, path, http.NoBody)
		resp, testErr := app.Test(req)
		require.NoError(t, testErr)
		resp.Body.Close()
		assert.Equalf(t, http.StatusNotFound, resp.StatusCode, "legacy path %s should be absent", path)
	}
}

func TestMountSystemplaneAPI_HistoryRoutesUseExpectedPermissions(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	type routeBinding struct {
		resource string
		action   string
	}

	var bindings []routeBinding
	protected := func(resource string, actions ...string) fiber.Router {
		action := ""
		if len(actions) > 0 {
			action = actions[0]
		}
		bindings = append(bindings, routeBinding{resource: resource, action: action})
		return app.Group("")
	}

	err := MountSystemplaneAPI(app, nil, protected, &mockManagerForMount{}, nil, nil, false, &libLog.NopLogger{})
	require.NoError(t, err)

	assert.Contains(t, bindings, routeBinding{resource: auth.ResourceSystem, action: auth.ActionConfigHistoryRead})
	assert.Contains(t, bindings, routeBinding{resource: auth.ResourceSystem, action: auth.ActionSettingsHistoryRead})
}

func TestMountSystemplaneAPI_GlobalSettingsHistoryRequiresElevatedAuth(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	authClient := authMiddleware.NewAuthClient("http://auth.example", true, nil)
	protected := func(_ string, _ ...string) fiber.Router { return app.Group("") }
	err := MountSystemplaneAPI(app, authClient, protected, &mockManagerForMount{}, nil, nil, true, &libLog.NopLogger{})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/v1/system/settings/history?scope=global", http.NoBody)
	resp, err := app.Test(req, -1)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}
