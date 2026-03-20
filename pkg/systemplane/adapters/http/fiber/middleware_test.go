//go:build unit

// Copyright 2025 Lerian Studio.

package fiberhttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

func TestRequireAuth_Authorized(t *testing.T) {
	t.Parallel()

	h, _, _, auth := mustNewHandler(t)

	auth.authorizeFn = func(_ context.Context, permission string) error {
		assert.Equal(t, "system/configs:read", permission)

		return nil
	}

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	app.Get("/test", h.requireAuth("system/configs:read"), func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "ok", readBody(t, resp))
}

func TestRequireAuth_Denied(t *testing.T) {
	t.Parallel()

	h, _, _, auth := mustNewHandler(t)

	auth.authorizeFn = func(_ context.Context, _ string) error {
		return domain.ErrPermissionDenied
	}

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	handlerCalled := false

	app.Get("/test", h.requireAuth("system/configs:read"), func(c *fiber.Ctx) error {
		handlerCalled = true

		return c.SendString("should not reach")
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.False(t, handlerCalled)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_permission_denied", errResp.Code)
}

func TestSettingsAuth_TenantScope(t *testing.T) {
	t.Parallel()

	h, _, _, auth := mustNewHandler(t)

	var capturedPermission string

	auth.authorizeFn = func(_ context.Context, permission string) error {
		capturedPermission = permission

		return nil
	}

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	app.Get("/test", h.settingsAuth("read"), func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	// No scope param = defaults to tenant
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "system/settings:read", capturedPermission)
}

func TestSettingsAuth_GlobalScope(t *testing.T) {
	t.Parallel()

	h, _, _, auth := mustNewHandler(t)

	var capturedPermission string

	auth.authorizeFn = func(_ context.Context, permission string) error {
		capturedPermission = permission

		return nil
	}

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	app.Get("/test", h.settingsAuth("write"), func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/test?scope=global", nil)
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "system/settings/global:write", capturedPermission)
}

func TestSettingsAuth_Denied(t *testing.T) {
	t.Parallel()

	h, _, _, auth := mustNewHandler(t)

	auth.authorizeFn = func(_ context.Context, _ string) error {
		return domain.ErrPermissionDenied
	}

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	app.Get("/test", h.settingsAuth("read"), func(c *fiber.Ctx) error {
		return c.SendString("should not reach")
	})

	req := httptest.NewRequest(http.MethodGet, "/test?scope=global", nil)
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestRequireAuth_NonPermissionError(t *testing.T) {
	t.Parallel()

	h, _, _, auth := mustNewHandler(t)

	auth.authorizeFn = func(_ context.Context, _ string) error {
		return domain.ErrSupervisorStopped
	}

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	app.Get("/test", h.requireAuth("anything"), func(c *fiber.Ctx) error {
		return c.SendString("should not reach")
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	resp := doRequest(t, app, req)

	// ErrSupervisorStopped maps to 503 through writeError
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}
