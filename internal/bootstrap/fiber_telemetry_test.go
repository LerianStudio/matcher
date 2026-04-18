//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
)

// --- telemetryMiddleware ---

func TestTelemetryMiddleware_SetsRequestIDHeader(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")

	app := fiber.New()
	app.Use(telemetryMiddleware(&libLog.NopLogger{}, tracer, nil))
	app.Get("/ping", func(c *fiber.Ctx) error {
		return c.SendString("pong")
	})

	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	req.Header.Set("X-Request-ID", "test-request-123")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotEmpty(t, resp.Header.Get("X-Request-ID"))
}

func TestTelemetryMiddleware_GeneratesRequestIDWhenMissing(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")

	app := fiber.New()
	app.Use(telemetryMiddleware(&libLog.NopLogger{}, tracer, nil))
	app.Get("/ping", func(c *fiber.Ctx) error {
		return c.SendString("pong")
	})

	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	// No X-Request-ID header.

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestTelemetryMiddleware_HandlerError(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")

	app := fiber.New()
	app.Use(telemetryMiddleware(&libLog.NopLogger{}, tracer, nil))
	app.Get("/fail", func(_ *fiber.Ctx) error {
		return fiber.NewError(http.StatusBadRequest, "bad")
	})

	req := httptest.NewRequest(http.MethodGet, "/fail", nil)

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestTelemetryMiddleware_ServerError(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")

	app := fiber.New()
	app.Use(telemetryMiddleware(&libLog.NopLogger{}, tracer, nil))
	app.Get("/error", func(_ *fiber.Ctx) error {
		return fiber.NewError(http.StatusInternalServerError, "oops")
	})

	req := httptest.NewRequest(http.MethodGet, "/error", nil)

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}
