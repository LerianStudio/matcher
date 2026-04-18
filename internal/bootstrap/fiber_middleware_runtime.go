// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
)

func runtimeBodyLimitMiddleware(initialCfg *Config, configGetter func() *Config) fiber.Handler {
	return func(fiberCtx *fiber.Ctx) error {
		limit := effectiveRuntimeBodyLimit(initialCfg, configGetter)

		if len(fiberCtx.Body()) > limit {
			return fiber.ErrRequestEntityTooLarge
		}

		return fiberCtx.Next()
	}
}

func currentRuntimeBodyLimit(initialCfg *Config, configGetter func() *Config) int {
	cfg := initialCfg

	if configGetter != nil {
		if runtimeCfg := configGetter(); runtimeCfg != nil {
			cfg = runtimeCfg
		}
	}

	if cfg == nil || cfg.Server.BodyLimitBytes <= 0 {
		return runtimeBodyLimitDefaultBytes
	}

	return cfg.Server.BodyLimitBytes
}

func effectiveRuntimeBodyLimit(initialCfg *Config, configGetter func() *Config) int {
	limit := currentRuntimeBodyLimit(initialCfg, configGetter)
	if limit <= 0 {
		return runtimeBodyLimitDefaultBytes
	}

	if limit > appBodyLimitCeilingBytes {
		return appBodyLimitCeilingBytes
	}

	return limit
}

func runtimeCORSMiddleware(initialCfg *Config, configGetter func() *Config) fiber.Handler {
	var (
		mu            sync.RWMutex
		activeHandler fiber.Handler
		activeOrigins string
		activeMethods string
		activeHeaders string
	)

	buildHandler := func(origins, methods, headers string) fiber.Handler {
		return cors.New(cors.Config{
			AllowOrigins: origins,
			AllowMethods: methods,
			AllowHeaders: headers,
		})
	}

	resolve := func() (string, string, string) {
		cfg := initialCfg

		if configGetter != nil {
			if runtimeCfg := configGetter(); runtimeCfg != nil {
				cfg = runtimeCfg
			}
		}

		if cfg == nil {
			return "http://localhost:3000", "GET,POST,PUT,PATCH,DELETE,OPTIONS", "Origin,Content-Type,Accept,Authorization,X-Request-ID"
		}

		return cfg.Server.CORSAllowedOrigins, cfg.Server.CORSAllowedMethods, cfg.Server.CORSAllowedHeaders
	}

	return func(fiberCtx *fiber.Ctx) error {
		origins, methods, headers := resolve()

		mu.RLock()

		handler := activeHandler
		currentOrigins := activeOrigins
		currentMethods := activeMethods
		currentHeaders := activeHeaders

		mu.RUnlock()

		if handler == nil || currentOrigins != origins || currentMethods != methods || currentHeaders != headers {
			mu.Lock()
			if activeHandler == nil || activeOrigins != origins || activeMethods != methods || activeHeaders != headers {
				activeHandler = buildHandler(origins, methods, headers)
				activeOrigins = origins
				activeMethods = methods
				activeHeaders = headers
			}

			handler = activeHandler
			mu.Unlock()
		}

		return handler(fiberCtx)
	}
}

// structuredRequestLogger creates a middleware that logs HTTP requests using the structured application logger.
// This ensures consistent log format across all application logs.
func structuredRequestLogger(logger libLog.Logger) fiber.Handler {
	return func(fiberCtx *fiber.Ctx) error {
		start := time.Now().UTC()

		err := fiberCtx.Next()

		latency := time.Since(start)
		status := fiberCtx.Response().StatusCode()
		requestID := fiberCtx.Locals("requestid")

		if logger != nil {
			reqCtx := fiberCtx.UserContext()
			if reqCtx == nil {
				reqCtx = context.Background()
			}

			logger.With(
				libLog.String("http.method", fiberCtx.Method()),
				libLog.String("http.path", fiberCtx.Path()),
				libLog.Int("http.status_code", status),
				libLog.Int("http.duration_ms", int(latency.Milliseconds())),
				libLog.String("http.request_id", fmt.Sprint(requestID)),
			).Log(reqCtx, libLog.LevelInfo, "HTTP request")
		}

		return err
	}
}

// dbQueryTimeoutMiddleware creates a middleware that applies a context deadline to
// each HTTP request. This bounds the total time any request can spend waiting for
// database connections, executing queries, or performing other context-aware operations.
//
// Without this middleware, requests can block indefinitely when the sql.DB connection
// pool is exhausted, since sql.DB has no built-in pool acquisition timeout.
// The cancel function is deferred to ensure resources are released after the handler completes.
func dbQueryTimeoutMiddleware(initialCfg *Config, configGetter func() *Config) fiber.Handler {
	return func(fiberCtx *fiber.Ctx) error {
		timeout := currentQueryTimeout(initialCfg, configGetter)
		if timeout <= 0 {
			return fiberCtx.Next()
		}

		ctx := fiberCtx.UserContext()

		// Only apply timeout if the context does not already have a tighter deadline.
		if deadline, ok := ctx.Deadline(); ok {
			if time.Until(deadline) <= timeout {
				return fiberCtx.Next()
			}
		}

		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		fiberCtx.SetUserContext(ctx)

		return fiberCtx.Next()
	}
}

func currentQueryTimeout(initialCfg *Config, configGetter func() *Config) time.Duration {
	cfg := initialCfg

	if configGetter != nil {
		if runtimeCfg := configGetter(); runtimeCfg != nil {
			cfg = runtimeCfg
		}
	}

	if cfg == nil {
		return 0
	}

	return cfg.QueryTimeout()
}
