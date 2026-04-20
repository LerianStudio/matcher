// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fireRequest runs a synthetic request through a fresh Fiber app mounted
// with the v4 shim and returns the HTTP status + decoded JSON body.
func fireRequest(t *testing.T, method, path string) (int, v4DeprecationResponseBody, bool) {
	t.Helper()

	app := fiber.New()
	app.Use(v4DeprecationShim(&recordingLogger{}))
	app.Get("/v1/ping", func(fiberCtx *fiber.Ctx) error {
		return fiberCtx.SendString("pong")
	})
	app.Get("/system/matcher", func(fiberCtx *fiber.Ctx) error {
		return fiberCtx.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(method, path, nil)

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	if resp.StatusCode != fiber.StatusGone {
		return resp.StatusCode, v4DeprecationResponseBody{}, false
	}

	var body v4DeprecationResponseBody
	if err := json.Unmarshal(bodyBytes, &body); err != nil {
		return resp.StatusCode, v4DeprecationResponseBody{}, false
	}

	return resp.StatusCode, body, true
}

// TestV4DeprecationShim_RejectsKnownRemovedPaths asserts every removed v4
// admin API route (configs, configs/:key, configs/schema, configs/history,
// configs/reload, settings, settings/:key, and sub-paths) returns 410 Gone
// with the canonical JSON body shape. The test doubles as the contract
// documentation for migration guides.
func TestV4DeprecationShim_RejectsKnownRemovedPaths(t *testing.T) {
	t.Parallel()

	removedPaths := []string{
		"/v1/system/configs",
		"/v1/system/configs/",
		"/v1/system/configs/rate_limit.max",
		"/v1/system/configs/schema",
		"/v1/system/configs/history",
		"/v1/system/configs/reload",
		"/v1/system/settings",
		"/v1/system/settings/",
		"/v1/system/settings/tenant.id",
		"/v1/system/settings/schema",
		"/v1/system/settings/history",
		"/v1/system/settings/global",
	}

	for _, removed := range removedPaths {
		t.Run(removed, func(t *testing.T) {
			t.Parallel()

			status, body, ok := fireRequest(t, http.MethodGet, removed)

			require.Equal(t, fiber.StatusGone, status, "%s must respond with 410 Gone", removed)
			require.True(t, ok, "%s must return the canonical JSON body", removed)

			assert.Equal(t, "GONE", body.Code)
			assert.Equal(t, "endpoint_removed", body.Title)
			assert.Contains(t, body.Message, "v4 admin API paths removed")
			assert.Contains(t, body.Hint, "/system/matcher")
			assert.NotEmpty(t, body.Removal)
		})
	}
}

// TestV4DeprecationShim_AllMethods asserts every HTTP verb against a
// removed v4 path is rejected — the shim must not discriminate between
// GET/PATCH/PUT because clients may still be hitting the old bulk PATCH
// entrypoint or admin-panel GETs interchangeably.
func TestV4DeprecationShim_AllMethods(t *testing.T) {
	t.Parallel()

	verbs := []string{
		http.MethodGet, http.MethodPost, http.MethodPut,
		http.MethodPatch, http.MethodDelete, http.MethodOptions, http.MethodHead,
	}

	for _, verb := range verbs {
		t.Run(verb, func(t *testing.T) {
			t.Parallel()

			status, _, _ := fireRequest(t, verb, "/v1/system/configs")

			assert.Equal(t, fiber.StatusGone, status, "verb %s must still be rejected", verb)
		})
	}
}

// TestV4DeprecationShim_DoesNotAffectAdjacentRoutes asserts the shim is a
// prefix-scoped no-op for non-admin paths and for the canonical v5 admin
// layout. A too-greedy prefix match would break business traffic.
func TestV4DeprecationShim_DoesNotAffectAdjacentRoutes(t *testing.T) {
	t.Parallel()

	t.Run("business route under /v1 is untouched", func(t *testing.T) {
		t.Parallel()

		status, _, _ := fireRequest(t, http.MethodGet, "/v1/ping")

		assert.Equal(t, fiber.StatusOK, status)
	})

	t.Run("v5 admin route is untouched", func(t *testing.T) {
		t.Parallel()

		status, _, _ := fireRequest(t, http.MethodGet, "/system/matcher")

		assert.Equal(t, fiber.StatusOK, status)
	})
}

// TestIsV4SystemplanePath_ClassificationTable lets the shim stay
// declarative about which paths it claims — a single table covers the
// positive and negative cases, and future additions only add a row.
func TestIsV4SystemplanePath_ClassificationTable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		path     string
		expected bool
	}{
		{"/v1/system/configs", true},
		{"/v1/system/configs/", true},
		{"/v1/system/configs/rate_limit.max", true},
		{"/v1/system/configs/schema", true},
		{"/v1/system/configs/history", true},
		{"/v1/system/configs/reload", true},
		{"/v1/system/settings", true},
		{"/v1/system/settings/global", true},

		// Near-miss paths that MUST NOT trigger the shim.
		{"/v1/system", false},
		{"/v1/system/", false},
		{"/v1/system/other", false},
		{"/v1/systems/configs", false},
		{"/v1/config", false},
		{"/system/matcher", false},
		{"/system/matcher/any.key", false},
		{"/health", false},
		{"/", false},
	}

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.expected, isV4SystemplanePath(tc.path))
		})
	}
}

// TestV4DeprecationShim_EmitsWarnPerHit ensures each intercepted call
// produces a structured WARN so ops can grep for the literal message and
// identify migration-lagging clients.
func TestV4DeprecationShim_EmitsWarnPerHit(t *testing.T) {
	t.Parallel()

	logger := &recordingLogger{}

	app := fiber.New()
	app.Use(v4DeprecationShim(logger))

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/v1/system/configs/rate_limit.max", nil))
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	require.Len(t, logger.records, 1, "exactly one warn per hit")

	assert.Contains(t, logger.records[0].msg, "410 Gone")
}

// TestV4DeprecationShim_NilLoggerDoesNotPanic covers the degenerate boot
// path where the shim is mounted before the logger is wired (defensive
// — should not happen in the real pipeline, but we want the contract
// explicit).
func TestV4DeprecationShim_NilLoggerDoesNotPanic(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Use(v4DeprecationShim(nil))

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/v1/system/configs", nil))
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, fiber.StatusGone, resp.StatusCode)
}
