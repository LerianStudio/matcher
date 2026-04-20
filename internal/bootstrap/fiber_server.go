// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"errors"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/helmet"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/fiber/v2/middleware/requestid"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

const (
	runtimeBodyLimitDefaultBytes = 32 * 1024 * 1024
	appBodyLimitCeilingBytes     = 128 * 1024 * 1024
	// maxHeaderIDLength limits X-Request-ID / X-Header-ID to 128 chars.
	// Rationale: UUID is 36 chars; 128 allows longer correlation IDs while
	// preventing log injection and memory exhaustion from malicious headers.
	maxHeaderIDLength   = 128
	defaultReadTimeout  = 30
	defaultWriteTimeout = 30
	defaultIdleTimeout  = 120
)

var (
	// errRabbitMQUnhealthy indicates the RabbitMQ health check returned a non-OK status.
	errRabbitMQUnhealthy = errors.New("rabbitmq health check: unhealthy status")
	// errServerNotInitialized indicates Run was called on a nil Server receiver.
	errServerNotInitialized = errors.New("server run: server not initialized")
	// errConfigNotInitialized indicates Run was called before config was set.
	errConfigNotInitialized = errors.New("server run: config not initialized")
	// errPostgresPrimaryNil indicates the primary database handle resolved to nil.
	errPostgresPrimaryNil = errors.New("postgres health check: primary db is nil")
	// errReplicaResolverNil indicates the replica resolver resolved to nil.
	errReplicaResolverNil = errors.New("postgres replica health check: resolver is nil")
	// errNoReplicasConfigured indicates no replica databases were returned by the resolver.
	errNoReplicasConfigured = errors.New("postgres replica health check: no replica databases configured")
	// errNoNonNilReplicas indicates all replica database handles in the slice were nil.
	errNoNonNilReplicas = errors.New("postgres replica health check: no non-nil replica databases configured")
	// errRedisClientNil indicates the Redis client resolved to nil.
	errRedisClientNil = errors.New("redis health check: client is nil")
)

// NewFiberApp creates and configures a new Fiber application with standard middleware.
func NewFiberApp(
	cfg *Config,
	logger libLog.Logger,
	telemetry *libOpentelemetry.Telemetry,
	configGetter func() *Config,
) *fiber.App {
	if cfg == nil {
		cfg = &Config{
			Server: ServerConfig{
				BodyLimitBytes:     runtimeBodyLimitDefaultBytes,
				CORSAllowedOrigins: "http://localhost:3000",
				CORSAllowedMethods: "GET,POST,PUT,PATCH,DELETE,OPTIONS",
				CORSAllowedHeaders: "Origin,Content-Type,Accept,Authorization,X-Request-ID",
			},
		}
	}

	envName := cfg.App.EnvName

	fiberCfg := fiber.Config{
		AppName:               constants.ApplicationName,
		DisableStartupMessage: true,
		ReadTimeout:           defaultReadTimeout * time.Second,
		WriteTimeout:          defaultWriteTimeout * time.Second,
		IdleTimeout:           defaultIdleTimeout * time.Second,
		BodyLimit:             appBodyLimitCeilingBytes,
		ErrorHandler:          customErrorHandlerWithEnv(logger, envName),
	}

	// Enable trusted proxy checking when configured. Without this, c.IP() trusts
	// X-Forwarded-For from any client, allowing IP spoofing to bypass rate limits.
	if cfg.Server.TrustedProxies != "" {
		proxies := strings.Split(cfg.Server.TrustedProxies, ",")
		trimmedProxies := make([]string, 0, len(proxies))

		for _, p := range proxies {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				trimmedProxies = append(trimmedProxies, trimmed)
			}
		}

		if len(trimmedProxies) > 0 {
			fiberCfg.EnableTrustedProxyCheck = true
			fiberCfg.TrustedProxies = trimmedProxies
			fiberCfg.ProxyHeader = fiber.HeaderXForwardedFor
		}
	}

	app := fiber.New(fiberCfg)

	app.Use(recover.New(recover.Config{
		EnableStackTrace: true,
		StackTraceHandler: func(c *fiber.Ctx, panicValue any) {
			ctx := libOpentelemetry.ExtractHTTPContext(c.UserContext(), c)
			runtime.HandlePanicValue(
				ctx,
				logger,
				panicValue,
				constants.ApplicationName,
				"http_handler",
			)
		},
	}))

	app.Use(requestid.New())

	app.Use(v4DeprecationShim(logger))

	app.Use(runtimeBodyLimitMiddleware(cfg, configGetter))

	if configGetter != nil {
		app.Use(runtimeCORSMiddleware(cfg, configGetter))
	} else {
		app.Use(cors.New(cors.Config{
			AllowOrigins: cfg.Server.CORSAllowedOrigins,
			AllowMethods: cfg.Server.CORSAllowedMethods,
			AllowHeaders: cfg.Server.CORSAllowedHeaders,
		}))
	}

	helmetCfg := helmet.Config{
		XSSProtection:             "1; mode=block",
		ContentTypeNosniff:        "nosniff",
		XFrameOptions:             "DENY",
		ReferrerPolicy:            "strict-origin-when-cross-origin",
		CrossOriginEmbedderPolicy: "require-corp",
		CrossOriginOpenerPolicy:   "same-origin",
		CrossOriginResourcePolicy: "same-origin",
		PermissionPolicy:          "geolocation=(), microphone=(), camera=()",
		ContentSecurityPolicy:     "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'; object-src 'none'",
	}

	if strings.TrimSpace(cfg.Server.TLSCertFile) != "" || cfg.Server.TLSTerminatedUpstream {
		helmetCfg.HSTSMaxAge = 31536000
		helmetCfg.HSTSPreloadEnabled = true
		helmetCfg.HSTSExcludeSubdomains = false
	}

	app.Use(helmet.New(helmetCfg))

	if telemetry != nil {
		tracer := telemetry.TracerProvider.Tracer(constants.ApplicationName)
		app.Use(telemetryMiddleware(logger, tracer, telemetry.MetricsFactory))
	}

	// Apply query timeout to bound the request context.
	// This ensures all downstream operations (including database calls) have a deadline,
	// preventing indefinite hangs when the connection pool is exhausted.
	// Must be applied AFTER telemetry middleware so the enriched context gets the deadline.
	queryTimeout := cfg.QueryTimeout()
	if queryTimeout > 0 || configGetter != nil {
		app.Use(dbQueryTimeoutMiddleware(cfg, configGetter))
	}

	if !IsProductionEnvironment(envName) {
		app.Use(structuredRequestLogger(logger))
	}

	return app
}
