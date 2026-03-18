// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"net/http"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/service"
)

// mockManagerForMount implements service.Manager with no-op methods,
// sufficient for verifying that MountSystemplaneAPI wires and mounts routes
// without requiring a real backend.
type mockManagerForMount struct{}

func (m *mockManagerForMount) GetConfigs(_ context.Context) (service.ResolvedSet, error) {
	return service.ResolvedSet{}, nil
}

func (m *mockManagerForMount) GetSettings(_ context.Context, _ service.Subject) (service.ResolvedSet, error) {
	return service.ResolvedSet{}, nil
}

func (m *mockManagerForMount) PatchConfigs(_ context.Context, _ service.PatchRequest) (service.WriteResult, error) {
	return service.WriteResult{}, nil
}

func (m *mockManagerForMount) PatchSettings(_ context.Context, _ service.Subject, _ service.PatchRequest) (service.WriteResult, error) {
	return service.WriteResult{}, nil
}

func (m *mockManagerForMount) GetConfigSchema(_ context.Context) ([]service.SchemaEntry, error) {
	return nil, nil
}

func (m *mockManagerForMount) GetSettingSchema(_ context.Context) ([]service.SchemaEntry, error) {
	return nil, nil
}

func (m *mockManagerForMount) GetConfigHistory(_ context.Context, _ ports.HistoryFilter) ([]ports.HistoryEntry, error) {
	return nil, nil
}

func (m *mockManagerForMount) GetSettingHistory(_ context.Context, _ ports.HistoryFilter) ([]ports.HistoryEntry, error) {
	return nil, nil
}

func (m *mockManagerForMount) Resync(_ context.Context) error {
	return nil
}

// Compile-time interface check.
var _ service.Manager = (*mockManagerForMount)(nil)

func TestMountSystemplaneAPI_NilApp(t *testing.T) {
	t.Parallel()

	err := MountSystemplaneAPI(nil, nil, &mockManagerForMount{}, false, &libLog.NopLogger{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "app is required")
}

func TestMountSystemplaneAPI_NilManager(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	err := MountSystemplaneAPI(app, nil, nil, false, &libLog.NopLogger{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "manager is required")
}

func TestMountSystemplaneAPI_Success(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	err := MountSystemplaneAPI(app, nil, &mockManagerForMount{}, false, &libLog.NopLogger{})
	require.NoError(t, err)
}

func TestMountSystemplaneAPI_NilLogger(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	// A nil logger should not cause a panic — the function guards it.
	err := MountSystemplaneAPI(app, nil, &mockManagerForMount{}, false, nil)
	require.NoError(t, err)
}

func TestMountSystemplaneAPI_RoutesRegistered(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	err := MountSystemplaneAPI(app, nil, &mockManagerForMount{}, false, &libLog.NopLogger{})
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
		http.MethodGet + " /v1/system/configs/",
		http.MethodPatch + " /v1/system/configs/",
		http.MethodGet + " /v1/system/configs/schema",
		http.MethodGet + " /v1/system/configs/history",
		http.MethodPost + " /v1/system/configs/reload",
		http.MethodGet + " /v1/system/settings/",
		http.MethodPatch + " /v1/system/settings/",
		http.MethodGet + " /v1/system/settings/schema",
		http.MethodGet + " /v1/system/settings/history",
	}

	for _, routeKey := range expectedRoutes {
		assert.True(t, registeredRoutes[routeKey],
			"expected route %q to be registered; registered routes: %v", routeKey, registeredRoutes)
	}
}

