//go:build unit

// Copyright 2025 Lerian Studio.

package fiberhttp

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/service"
)

// --- Test mocks ---

type mockManager struct {
	getConfigsFn        func(ctx context.Context) (service.ResolvedSet, error)
	getSettingsFn       func(ctx context.Context, subject service.Subject) (service.ResolvedSet, error)
	patchConfigsFn      func(ctx context.Context, req service.PatchRequest) (service.WriteResult, error)
	patchSettingsFn     func(ctx context.Context, subject service.Subject, req service.PatchRequest) (service.WriteResult, error)
	getConfigSchemaFn   func(ctx context.Context) ([]service.SchemaEntry, error)
	getSettingSchemaFn  func(ctx context.Context) ([]service.SchemaEntry, error)
	getConfigHistoryFn  func(ctx context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error)
	getSettingHistoryFn func(ctx context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error)
	applyChangeSignalFn func(ctx context.Context, signal ports.ChangeSignal) error
	resyncFn            func(ctx context.Context) error
}

func (m *mockManager) GetConfigs(ctx context.Context) (service.ResolvedSet, error) {
	if m.getConfigsFn != nil {
		return m.getConfigsFn(ctx)
	}

	return service.ResolvedSet{}, nil
}

func (m *mockManager) GetSettings(ctx context.Context, subject service.Subject) (service.ResolvedSet, error) {
	if m.getSettingsFn != nil {
		return m.getSettingsFn(ctx, subject)
	}

	return service.ResolvedSet{}, nil
}

func (m *mockManager) PatchConfigs(ctx context.Context, req service.PatchRequest) (service.WriteResult, error) {
	if m.patchConfigsFn != nil {
		return m.patchConfigsFn(ctx, req)
	}

	return service.WriteResult{}, nil
}

func (m *mockManager) PatchSettings(ctx context.Context, subject service.Subject, req service.PatchRequest) (service.WriteResult, error) {
	if m.patchSettingsFn != nil {
		return m.patchSettingsFn(ctx, subject, req)
	}

	return service.WriteResult{}, nil
}

func (m *mockManager) GetConfigSchema(ctx context.Context) ([]service.SchemaEntry, error) {
	if m.getConfigSchemaFn != nil {
		return m.getConfigSchemaFn(ctx)
	}

	return nil, nil
}

func (m *mockManager) GetSettingSchema(ctx context.Context) ([]service.SchemaEntry, error) {
	if m.getSettingSchemaFn != nil {
		return m.getSettingSchemaFn(ctx)
	}

	return nil, nil
}

func (m *mockManager) GetConfigHistory(ctx context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error) {
	if m.getConfigHistoryFn != nil {
		return m.getConfigHistoryFn(ctx, filter)
	}

	return nil, nil
}

func (m *mockManager) GetSettingHistory(ctx context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error) {
	if m.getSettingHistoryFn != nil {
		return m.getSettingHistoryFn(ctx, filter)
	}

	return nil, nil
}

func (m *mockManager) ApplyChangeSignal(ctx context.Context, signal ports.ChangeSignal) error {
	if m.applyChangeSignalFn != nil {
		return m.applyChangeSignalFn(ctx, signal)
	}

	return nil
}

func (m *mockManager) Resync(ctx context.Context) error {
	if m.resyncFn != nil {
		return m.resyncFn(ctx)
	}

	return nil
}

type mockIdentity struct {
	actorFn    func(ctx context.Context) (domain.Actor, error)
	tenantIDFn func(ctx context.Context) (string, error)
}

func (m *mockIdentity) Actor(ctx context.Context) (domain.Actor, error) {
	if m.actorFn != nil {
		return m.actorFn(ctx)
	}

	return domain.Actor{ID: "test-actor"}, nil
}

func (m *mockIdentity) TenantID(ctx context.Context) (string, error) {
	if m.tenantIDFn != nil {
		return m.tenantIDFn(ctx)
	}

	return "test-tenant-id", nil
}

type mockAuthorizer struct {
	authorizeFn func(ctx context.Context, permission string) error
}

func (m *mockAuthorizer) Authorize(ctx context.Context, permission string) error {
	if m.authorizeFn != nil {
		return m.authorizeFn(ctx, permission)
	}

	return nil
}

// --- Helper ---

func newTestDeps() (*mockManager, *mockIdentity, *mockAuthorizer) {
	return &mockManager{}, &mockIdentity{}, &mockAuthorizer{}
}

func mustNewHandler(t *testing.T) (*Handler, *mockManager, *mockIdentity, *mockAuthorizer) {
	t.Helper()

	mgr, id, auth := newTestDeps()

	h, err := NewHandler(mgr, id, auth)
	require.NoError(t, err)

	return h, mgr, id, auth
}

func newTestApp(t *testing.T) (*fiber.App, *Handler, *mockManager, *mockIdentity, *mockAuthorizer) {
	t.Helper()

	h, mgr, id, auth := mustNewHandler(t)

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})
	h.Mount(app)

	return app, h, mgr, id, auth
}

func doRequest(t *testing.T, app *fiber.App, req *http.Request) *http.Response {
	t.Helper()

	resp, err := app.Test(req)
	require.NoError(t, err)

	return resp
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()

	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(body)
}

// --- Constructor tests ---

func TestNewHandler_NilManager(t *testing.T) {
	t.Parallel()

	_, id, auth := newTestDeps()

	h, err := NewHandler(nil, id, auth)
	assert.Nil(t, h)
	assert.ErrorIs(t, err, errHandlerManagerRequired)
}

func TestNewHandler_NilIdentity(t *testing.T) {
	t.Parallel()

	mgr, _, auth := newTestDeps()

	h, err := NewHandler(mgr, nil, auth)
	assert.Nil(t, h)
	assert.ErrorIs(t, err, errHandlerIdentityRequired)
}

func TestNewHandler_NilAuth(t *testing.T) {
	t.Parallel()

	mgr, id, _ := newTestDeps()

	h, err := NewHandler(mgr, id, nil)
	assert.Nil(t, h)
	assert.ErrorIs(t, err, errHandlerAuthRequired)
}

func TestNewHandler_Success(t *testing.T) {
	t.Parallel()

	mgr, id, auth := newTestDeps()

	h, err := NewHandler(mgr, id, auth)
	assert.NotNil(t, h)
	assert.NoError(t, err)
}

func TestHandler_Mount_RegistersRoutes(t *testing.T) {
	t.Parallel()

	h, _, _, _ := mustNewHandler(t)

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})
	h.Mount(app)

	routes := app.GetRoutes()

	// We expect 10 routes:
	// GET /v1/system/configs/
	// PATCH /v1/system/configs/
	// GET /v1/system/configs/schema
	// GET /v1/system/configs/history
	// POST /v1/system/configs/reload
	// GET /v1/system/settings/
	// PATCH /v1/system/settings/
	// GET /v1/system/settings/schema
	// GET /v1/system/settings/history
	// Plus HEAD routes for each GET (Fiber adds them automatically)
	expectedPaths := map[string][]string{
		"/v1/system/configs/":         {"GET", "PATCH"},
		"/v1/system/configs/schema":   {"GET"},
		"/v1/system/configs/history":  {"GET"},
		"/v1/system/configs/reload":   {"POST"},
		"/v1/system/settings/":        {"GET", "PATCH"},
		"/v1/system/settings/schema":  {"GET"},
		"/v1/system/settings/history": {"GET"},
	}

	routeMap := make(map[string]map[string]bool)

	for _, r := range routes {
		if r.Method == "HEAD" {
			continue // Fiber auto-adds HEAD for GET
		}

		if _, ok := routeMap[r.Path]; !ok {
			routeMap[r.Path] = make(map[string]bool)
		}

		routeMap[r.Path][r.Method] = true
	}

	for path, methods := range expectedPaths {
		for _, method := range methods {
			assert.True(t, routeMap[path][method],
				"expected route %s %s to be registered", method, path)
		}
	}
}

// --- Helpers for test timestamps ---

func testTime() time.Time {
	return time.Date(2026, 3, 17, 12, 0, 0, 0, time.UTC)
}

// --- Verify mocks implement interfaces ---

var (
	_ service.Manager        = (*mockManager)(nil)
	_ ports.IdentityResolver = (*mockIdentity)(nil)
	_ ports.Authorizer       = (*mockAuthorizer)(nil)
)
