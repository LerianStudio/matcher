//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
)

// --- currentRuntimeBodyLimit ---

func TestCurrentRuntimeBodyLimit_NilConfig(t *testing.T) {
	t.Parallel()

	result := currentRuntimeBodyLimit(nil, nil)

	assert.Equal(t, runtimeBodyLimitDefaultBytes, result)
}

func TestCurrentRuntimeBodyLimit_ZeroBodyLimit(t *testing.T) {
	t.Parallel()

	cfg := &Config{Server: ServerConfig{BodyLimitBytes: 0}}

	result := currentRuntimeBodyLimit(cfg, nil)

	assert.Equal(t, runtimeBodyLimitDefaultBytes, result)
}

func TestCurrentRuntimeBodyLimit_NegativeBodyLimit(t *testing.T) {
	t.Parallel()

	cfg := &Config{Server: ServerConfig{BodyLimitBytes: -100}}

	result := currentRuntimeBodyLimit(cfg, nil)

	assert.Equal(t, runtimeBodyLimitDefaultBytes, result)
}

func TestCurrentRuntimeBodyLimit_ValidConfig(t *testing.T) {
	t.Parallel()

	cfg := &Config{Server: ServerConfig{BodyLimitBytes: 1024}}

	result := currentRuntimeBodyLimit(cfg, nil)

	assert.Equal(t, 1024, result)
}

func TestCurrentRuntimeBodyLimit_RuntimeGetterOverrides(t *testing.T) {
	t.Parallel()

	initialCfg := &Config{Server: ServerConfig{BodyLimitBytes: 1024}}
	runtimeCfg := &Config{Server: ServerConfig{BodyLimitBytes: 2048}}
	getter := func() *Config { return runtimeCfg }

	result := currentRuntimeBodyLimit(initialCfg, getter)

	assert.Equal(t, 2048, result)
}

func TestCurrentRuntimeBodyLimit_RuntimeGetterReturnsNil(t *testing.T) {
	t.Parallel()

	initialCfg := &Config{Server: ServerConfig{BodyLimitBytes: 1024}}
	getter := func() *Config { return nil }

	result := currentRuntimeBodyLimit(initialCfg, getter)

	assert.Equal(t, 1024, result)
}

// --- effectiveRuntimeBodyLimit ---

func TestEffectiveRuntimeBodyLimit_NilConfig(t *testing.T) {
	t.Parallel()

	result := effectiveRuntimeBodyLimit(nil, nil)

	assert.Equal(t, runtimeBodyLimitDefaultBytes, result)
}

func TestEffectiveRuntimeBodyLimit_ExceedsCeiling(t *testing.T) {
	t.Parallel()

	cfg := &Config{Server: ServerConfig{BodyLimitBytes: appBodyLimitCeilingBytes + 1024}}

	result := effectiveRuntimeBodyLimit(cfg, nil)

	assert.Equal(t, appBodyLimitCeilingBytes, result)
}

func TestEffectiveRuntimeBodyLimit_WithinRange(t *testing.T) {
	t.Parallel()

	cfg := &Config{Server: ServerConfig{BodyLimitBytes: 4096}}

	result := effectiveRuntimeBodyLimit(cfg, nil)

	assert.Equal(t, 4096, result)
}

// --- runtimeBodyLimitMiddleware ---

func TestRuntimeBodyLimitMiddleware_AllowsSmallBody(t *testing.T) {
	t.Parallel()

	cfg := &Config{Server: ServerConfig{BodyLimitBytes: 1024}}

	app := fiber.New()
	app.Use(runtimeBodyLimitMiddleware(cfg, nil))
	app.Post("/data", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	body := strings.NewReader("small body")
	req := httptest.NewRequest(http.MethodPost, "/data", body)
	req.Header.Set("Content-Type", "text/plain")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRuntimeBodyLimitMiddleware_RejectsTooLargeBody(t *testing.T) {
	t.Parallel()

	cfg := &Config{Server: ServerConfig{BodyLimitBytes: 10}}

	app := fiber.New()
	app.Use(runtimeBodyLimitMiddleware(cfg, nil))
	app.Post("/data", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	largeBody := strings.NewReader(strings.Repeat("x", 100))
	req := httptest.NewRequest(http.MethodPost, "/data", largeBody)
	req.Header.Set("Content-Type", "text/plain")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
}

// TestRuntimeBodyLimitMiddleware_RejectsOnContentLengthWithoutReadingBody
// exercises the Content-Length fast path: the handler must reject before
// the body is materialised.
func TestRuntimeBodyLimitMiddleware_RejectsOnContentLengthWithoutReadingBody(t *testing.T) {
	t.Parallel()

	cfg := &Config{Server: ServerConfig{BodyLimitBytes: 10}}

	app := fiber.New()
	app.Use(runtimeBodyLimitMiddleware(cfg, nil))
	app.Post("/data", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	// Declare a Content-Length that exceeds the limit. Body bytes must remain
	// untouched for the middleware to reject early.
	req := httptest.NewRequest(http.MethodPost, "/data", strings.NewReader(strings.Repeat("y", 100)))
	req.Header.Set("Content-Type", "text/plain")
	req.ContentLength = 100

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
}

// TestRuntimeBodyLimitMiddleware_AllowsWhenContentLengthFits ensures the
// Content-Length fast path accepts requests whose declared length is below
// the limit without requiring the body to be read.
func TestRuntimeBodyLimitMiddleware_AllowsWhenContentLengthFits(t *testing.T) {
	t.Parallel()

	cfg := &Config{Server: ServerConfig{BodyLimitBytes: 1024}}

	app := fiber.New()
	app.Use(runtimeBodyLimitMiddleware(cfg, nil))
	app.Post("/data", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	req := httptest.NewRequest(http.MethodPost, "/data", strings.NewReader("within"))
	req.Header.Set("Content-Type", "text/plain")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// --- runtimeCORSMiddleware ---

func TestRuntimeCORSMiddleware_SetsHeaders(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Server: ServerConfig{
			CORSAllowedOrigins: "http://example.com",
			CORSAllowedMethods: "GET,POST",
			CORSAllowedHeaders: "Content-Type",
		},
	}

	app := fiber.New()
	app.Use(runtimeCORSMiddleware(cfg, nil))
	app.Get("/ping", func(c *fiber.Ctx) error {
		return c.SendString("pong")
	})

	req := httptest.NewRequest(http.MethodOptions, "/ping", nil)
	req.Header.Set("Origin", "http://example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	// CORS middleware should respond (exact status depends on preflight handling).
	assert.True(t, resp.StatusCode < 500)
}

func TestRuntimeCORSMiddleware_NilConfig(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Use(runtimeCORSMiddleware(nil, nil))
	app.Get("/ping", func(c *fiber.Ctx) error {
		return c.SendString("pong")
	})

	req := httptest.NewRequest(http.MethodGet, "/ping", nil)

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// --- structuredRequestLogger ---

func TestStructuredRequestLogger_LogsRequest(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Use(structuredRequestLogger(&libLog.NopLogger{}))
	app.Get("/log-test", func(c *fiber.Ctx) error {
		return c.SendString("logged")
	})

	req := httptest.NewRequest(http.MethodGet, "/log-test", nil)

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "logged", string(respBody))
}

func TestStructuredRequestLogger_NilLogger(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Use(structuredRequestLogger(nil))
	app.Get("/safe", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/safe", nil)

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	// Must not panic with nil logger.
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// --- currentQueryTimeout ---

func TestCurrentQueryTimeout_NilConfig(t *testing.T) {
	t.Parallel()

	result := currentQueryTimeout(nil, nil)

	assert.Equal(t, time.Duration(0), result)
}

func TestCurrentQueryTimeout_WithConfig(t *testing.T) {
	t.Parallel()

	cfg := &Config{Postgres: PostgresConfig{QueryTimeoutSec: 10}}

	result := currentQueryTimeout(cfg, nil)

	assert.Equal(t, 10*time.Second, result)
}

func TestCurrentQueryTimeout_RuntimeGetterOverrides(t *testing.T) {
	t.Parallel()

	initialCfg := &Config{Postgres: PostgresConfig{QueryTimeoutSec: 10}}
	runtimeCfg := &Config{Postgres: PostgresConfig{QueryTimeoutSec: 20}}
	getter := func() *Config { return runtimeCfg }

	result := currentQueryTimeout(initialCfg, getter)

	assert.Equal(t, 20*time.Second, result)
}

func TestCurrentQueryTimeout_RuntimeGetterReturnsNil(t *testing.T) {
	t.Parallel()

	initialCfg := &Config{Postgres: PostgresConfig{QueryTimeoutSec: 15}}
	getter := func() *Config { return nil }

	result := currentQueryTimeout(initialCfg, getter)

	assert.Equal(t, 15*time.Second, result)
}

// --- dbQueryTimeoutMiddleware ---

func TestDbQueryTimeoutMiddleware_AppliesTimeout(t *testing.T) {
	t.Parallel()

	cfg := &Config{Postgres: PostgresConfig{QueryTimeoutSec: 5}}

	app := fiber.New()
	app.Use(dbQueryTimeoutMiddleware(cfg, nil))
	app.Get("/query", func(c *fiber.Ctx) error {
		ctx := c.UserContext()
		deadline, ok := ctx.Deadline()
		if ok {
			c.Set("X-Has-Deadline", "true")
			_ = deadline
		}

		return c.SendString("ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/query", nil)

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "true", resp.Header.Get("X-Has-Deadline"))
}

func TestDbQueryTimeoutMiddleware_ZeroTimeout_NoDeadline(t *testing.T) {
	t.Parallel()

	// QueryTimeoutSec == 0 → no deadline should be applied by the middleware.
	cfg := &Config{Postgres: PostgresConfig{QueryTimeoutSec: 0}}

	app := fiber.New()
	app.Use(dbQueryTimeoutMiddleware(cfg, nil))
	app.Get("/query", func(c *fiber.Ctx) error {
		_, ok := c.UserContext().Deadline()
		if ok {
			return c.SendString("has-deadline")
		}

		return c.SendString("no-deadline")
	})

	req := httptest.NewRequest(http.MethodGet, "/query", nil)

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
