package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/helmet"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/fiber/v2/middleware/requestid"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	"github.com/LerianStudio/lib-commons/v4/commons/assert"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	sharedhttp "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"
	libMetrics "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry/metrics"
	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v4/commons/rabbitmq"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

const (
	defaultBodyLimitBytes = 10 * 1024 * 1024
	// maxHeaderIDLength limits X-Request-ID / X-Header-ID to 128 chars.
	// Rationale: UUID is 36 chars; 128 allows longer correlation IDs while
	// preventing log injection and memory exhaustion from malicious headers.
	maxHeaderIDLength   = 128
	defaultReadTimeout  = 30
	defaultWriteTimeout = 30
	defaultIdleTimeout  = 120
	statusUnknown       = "unknown"
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

// Server encapsulates the Fiber HTTP server with its dependencies.
type Server struct {
	app       *fiber.App
	cfg       *Config
	logger    libLog.Logger
	telemetry *libOpentelemetry.Telemetry
	postgres  *libPostgres.Client
	redis     *libRedis.Client
	rabbitmq  *libRabbitmq.RabbitMQConnection
}

// HealthCheckFunc is a function type for performing health checks on dependencies.
type HealthCheckFunc func(ctx context.Context) error

// HealthResponse represents the liveness check response.
// @Description Service liveness status
type HealthResponse struct {
	// Status indicates the service health (always "healthy" if responding)
	Status string `json:"status" example:"healthy"`
}

// ReadinessResponse represents the readiness check response.
// @Description Service readiness status with optional dependency checks
type ReadinessResponse struct {
	// Status is "ok" when all required dependencies are available, "degraded" otherwise
	Status string `json:"status"           example:"ok" enums:"ok,degraded"`
	// Checks contains individual dependency status (only in non-production environments)
	Checks *DependencyChecks `json:"checks,omitempty"`
}

// DependencyChecks contains the status of each infrastructure dependency.
// @Description Individual dependency health status
type DependencyChecks struct {
	// Database check status: ok, down, or unknown
	Database string `json:"database"        example:"ok" enums:"ok,down,unknown"`
	// DatabaseReplica check status: ok, down, or unknown
	DatabaseReplica string `json:"databaseReplica" example:"ok" enums:"ok,down,unknown"`
	// Redis check status: ok, down, or unknown
	Redis string `json:"redis"           example:"ok" enums:"ok,down,unknown"`
	// RabbitMQ check status: ok, down, or unknown
	RabbitMQ string `json:"rabbitmq"        example:"ok" enums:"ok,down,unknown"`
	// ObjectStorage check status: ok, down, or unknown
	ObjectStorage string `json:"objectStorage"   example:"ok" enums:"ok,down,unknown"`
}

// HealthDependencies holds references to infrastructure components for health checks.
type HealthDependencies struct {
	Postgres        *libPostgres.Client
	PostgresReplica *libPostgres.Client
	Redis           *libRedis.Client
	RabbitMQ        *libRabbitmq.RabbitMQConnection
	ObjectStorage   ObjectStorageHealthChecker

	PostgresCheck        HealthCheckFunc
	PostgresReplicaCheck HealthCheckFunc
	RedisCheck           HealthCheckFunc
	RabbitMQCheck        HealthCheckFunc
	ObjectStorageCheck   HealthCheckFunc

	// Optional dependencies do not impact overall readiness status when unavailable
	// or failing their readiness checks.
	PostgresOptional        bool
	PostgresReplicaOptional bool
	RedisOptional           bool
	RabbitMQOptional        bool
	ObjectStorageOptional   bool
}

// ObjectStorageHealthChecker is an interface for checking object storage health.
type ObjectStorageHealthChecker interface {
	// Exists checks if an object exists at the given key (used for health check).
	Exists(ctx context.Context, key string) (bool, error)
}

// NewHealthDependencies creates a new HealthDependencies with default settings.
func NewHealthDependencies(
	postgres *libPostgres.Client,
	postgresReplica *libPostgres.Client,
	redis *libRedis.Client,
	rabbitmq *libRabbitmq.RabbitMQConnection,
	objectStorage ObjectStorageHealthChecker,
) *HealthDependencies {
	return &HealthDependencies{
		Postgres:        postgres,
		PostgresReplica: postgresReplica,
		Redis:           redis,
		RabbitMQ:        rabbitmq,
		ObjectStorage:   objectStorage,

		// Redis, replica, and object storage are treated as optional dependencies by default.
		RedisOptional:           true,
		PostgresReplicaOptional: true,
		ObjectStorageOptional:   true,
	}
}

// NewServer creates a new Server instance with all required dependencies.
func NewServer(
	cfg *Config,
	app *fiber.App,
	logger libLog.Logger,
	telemetry *libOpentelemetry.Telemetry,
	postgres *libPostgres.Client,
	redis *libRedis.Client,
	rabbitmq *libRabbitmq.RabbitMQConnection,
) *Server {
	return &Server{
		app:       app,
		cfg:       cfg,
		logger:    logger,
		telemetry: telemetry,
		postgres:  postgres,
		redis:     redis,
		rabbitmq:  rabbitmq,
	}
}

// GetApp returns the underlying Fiber application for testing purposes.
// This allows integration tests to call app.Test() for in-process HTTP testing
// without starting a real network listener.
func (srv *Server) GetApp() *fiber.App {
	if srv == nil {
		return nil
	}

	return srv.app
}

// Run starts the HTTP server, implementing the libCommons.App interface.
func (srv *Server) Run(_ *libCommons.Launcher) error {
	if srv == nil {
		return errServerNotInitialized
	}

	if srv.cfg == nil {
		return errConfigNotInitialized
	}

	asserter := assert.New(
		context.Background(),
		srv.logger,
		constants.ApplicationName,
		"bootstrap.server_run",
	)

	if err := asserter.NotNil(context.Background(), srv.app, "server not initialized"); err != nil {
		return fmt.Errorf("server run: %w", err)
	}

	if strings.TrimSpace(srv.cfg.Server.TLSCertFile) != "" ||
		strings.TrimSpace(srv.cfg.Server.TLSKeyFile) != "" {
		if err := srv.app.ListenTLS(srv.cfg.Server.Address, srv.cfg.Server.TLSCertFile, srv.cfg.Server.TLSKeyFile); err != nil {
			return fmt.Errorf("server listen tls: %w", err)
		}

		return nil
	}

	if err := srv.app.Listen(srv.cfg.Server.Address); err != nil {
		return fmt.Errorf("server listen: %w", err)
	}

	return nil
}

// Shutdown gracefully shuts down the HTTP server and flushes telemetry.
func (srv *Server) Shutdown(ctx context.Context) error {
	logger := libLog.Logger(&libLog.NopLogger{})
	if srv != nil && srv.logger != nil {
		logger = srv.logger
	}

	asserter := assert.New(ctx, logger, constants.ApplicationName, "bootstrap.server_shutdown")

	if err := asserter.NotNil(ctx, srv, "server not initialized"); err != nil {
		return fmt.Errorf("server shutdown: %w", err)
	}

	if err := asserter.NotNil(ctx, srv.app, "server not initialized"); err != nil {
		return fmt.Errorf("server shutdown: %w", err)
	}

	if err := srv.app.ShutdownWithContext(ctx); err != nil {
		return fmt.Errorf("server shutdown: %w", err)
	}

	if srv.telemetry != nil {
		srv.telemetry.ShutdownTelemetry()
	}

	return nil
}

// NewFiberApp creates and configures a new Fiber application with standard middleware.
func NewFiberApp(
	cfg *Config,
	logger libLog.Logger,
	telemetry *libOpentelemetry.Telemetry,
) *fiber.App {
	if cfg == nil {
		cfg = &Config{
			Server: ServerConfig{
				BodyLimitBytes:     defaultBodyLimitBytes,
				CORSAllowedOrigins: "http://localhost:3000",
				CORSAllowedMethods: "GET,POST,PUT,PATCH,DELETE,OPTIONS",
				CORSAllowedHeaders: "Origin,Content-Type,Accept,Authorization,X-Request-ID",
			},
		}
	}

	bodyLimit := cfg.Server.BodyLimitBytes
	if bodyLimit <= 0 {
		bodyLimit = defaultBodyLimitBytes
	}

	envName := cfg.App.EnvName

	app := fiber.New(fiber.Config{
		AppName:               constants.ApplicationName,
		DisableStartupMessage: true,
		ReadTimeout:           defaultReadTimeout * time.Second,
		WriteTimeout:          defaultWriteTimeout * time.Second,
		IdleTimeout:           defaultIdleTimeout * time.Second,
		BodyLimit:             bodyLimit,
		ErrorHandler:          customErrorHandlerWithEnv(logger, envName),
	})

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

	app.Use(cors.New(cors.Config{
		AllowOrigins: cfg.Server.CORSAllowedOrigins,
		AllowMethods: cfg.Server.CORSAllowedMethods,
		AllowHeaders: cfg.Server.CORSAllowedHeaders,
	}))

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
	if queryTimeout > 0 {
		app.Use(dbQueryTimeoutMiddleware(queryTimeout))
	}

	if !IsProductionEnvironment(envName) {
		app.Use(structuredRequestLogger(logger))
	}

	return app
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
func dbQueryTimeoutMiddleware(timeout time.Duration) fiber.Handler {
	return func(fiberCtx *fiber.Ctx) error {
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

func customErrorHandlerWithEnv(logger libLog.Logger, envName string) fiber.ErrorHandler {
	isProduction := IsProductionEnvironment(envName)

	return func(fiberCtx *fiber.Ctx, err error) error {
		code := fiber.StatusInternalServerError

		var fe *fiber.Error
		if errors.As(err, &fe) {
			code = fe.Code
		}

		if logger != nil {
			reqCtx := fiberCtx.UserContext()
			if reqCtx == nil {
				reqCtx = context.Background()
			}

			if isProduction {
				// In production, log sanitized error details to avoid leaking PII
				logger.Log(reqCtx, libLog.LevelError, fmt.Sprintf(
					"HTTP error: status=%d path=%s method=%s",
					code,
					fiberCtx.Path(),
					fiberCtx.Method(),
				))
			} else {
				// In non-production, sanitize error message to prevent secret leakage
				sanitizedErr := sanitizeErrorForLogging(err)
				logger.Log(reqCtx, libLog.LevelError, fmt.Sprintf("HTTP error: status=%d error=%s path=%s method=%s", code, sanitizedErr, fiberCtx.Path(), fiberCtx.Method()))
			}
		}

		title := "internal_error"
		message := "internal server error"

		if code < fiber.StatusInternalServerError {
			title = clientErrorMessageForStatus(code)
			message = title
		}

		return sharedhttp.RespondError(fiberCtx, code, title, message)
	}
}

// sanitizeErrorForLogging redacts potential secrets from error messages.
// Matches common patterns for passwords, tokens, keys, and connection strings.
// Matching is case-insensitive so that "Password=", "PASSWORD=", and "password="
// are all caught by a single canonical (lower-case) pattern.
func sanitizeErrorForLogging(err error) string {
	if err == nil {
		return ""
	}

	msg := err.Error()
	msgLower := strings.ToLower(msg)

	// Patterns that may contain secrets (canonical lower-case form).
	patterns := []struct {
		pattern     string
		replacement string
	}{
		{"password=", "password=***REDACTED***"},
		{"secret=", "secret=***REDACTED***"},
		{"token=", "token=***REDACTED***"},
		{"api_key=", "api_key=***REDACTED***"},
		{"apikey=", "apikey=***REDACTED***"},
		{"bearer ", "Bearer ***REDACTED***"},
		{"basic ", "Basic ***REDACTED***"},
	}

	for _, pat := range patterns {
		// Repeatedly search-and-replace until no more occurrences remain.
		// Track offset to avoid infinite loop when replacement contains pattern.
		offset := 0

		for {
			idx := strings.Index(msgLower[offset:], pat.pattern)
			if idx == -1 {
				break
			}
			// Adjust idx to be relative to the full string
			idx += offset
			// Find end of value (space, quote, or end of string) using original msg
			endIdx := findValueEnd(msg, idx+len(pat.pattern))
			// Replace the slice in both msg and msgLower so future searches stay aligned
			msg = msg[:idx] + pat.replacement + msg[endIdx:]
			msgLower = msgLower[:idx] + strings.ToLower(pat.replacement) + msgLower[endIdx:]
			// Move offset past the replacement to avoid re-matching
			offset = idx + len(pat.replacement)
		}
	}

	return msg
}

// findValueEnd finds the end of a secret value in an error message.
func findValueEnd(msg string, start int) int {
	for i := start; i < len(msg); i++ {
		switch msg[i] {
		case ' ', '"', '\'', '\n', '\r', '\t', ';', '&':
			return i
		}
	}

	return len(msg)
}

func clientErrorMessageForStatus(code int) string {
	switch code {
	case fiber.StatusBadRequest:
		return "invalid_request"
	case fiber.StatusUnauthorized:
		return "unauthorized"
	case fiber.StatusForbidden:
		return "forbidden"
	case fiber.StatusNotFound:
		return "not_found"
	default:
		return "request_failed"
	}
}

func sanitizeHeaderID(headerID string) string {
	trimmed := strings.TrimSpace(headerID)

	if trimmed == "" {
		return uuid.NewString()
	}

	if len(trimmed) > maxHeaderIDLength {
		return truncateHeaderID(trimmed)
	}

	for _, char := range trimmed {
		if !isSafeHeaderChar(char) {
			sanitized := strings.Map(func(r rune) rune {
				if !isSafeHeaderChar(r) {
					return -1
				}

				return r
			}, trimmed)

			if strings.TrimSpace(sanitized) == "" {
				return uuid.NewString()
			}

			if len(sanitized) > maxHeaderIDLength {
				return truncateHeaderID(sanitized)
			}

			return sanitized
		}
	}

	return trimmed
}

// isSafeHeaderChar returns true if the rune is safe for use in header IDs.
// Filters out non-printable characters and control characters that could
// be used for log injection attacks (\r, \n, \t, ;, |).
func isSafeHeaderChar(r rune) bool {
	if !unicode.IsPrint(r) {
		return false
	}

	switch r {
	case '\r', '\n', '\t', ';', '|':
		return false
	default:
		return true
	}
}

func truncateHeaderID(value string) string {
	runes := []rune(value)
	if len(runes) > maxHeaderIDLength {
		return string(runes[:maxHeaderIDLength])
	}

	return value
}

func telemetryMiddleware(
	logger libLog.Logger,
	tracer trace.Tracer,
	metricFactory *libMetrics.MetricsFactory,
) fiber.Handler {
	return func(fiberCtx *fiber.Ctx) error {
		ctx := libOpentelemetry.ExtractHTTPContext(fiberCtx.UserContext(), fiberCtx)
		localRequestID, _ := fiberCtx.Locals(requestid.ConfigDefault.ContextKey).(string)

		requestID := strings.TrimSpace(localRequestID)

		if requestID == "" {
			requestID = strings.TrimSpace(fiberCtx.Get("X-Request-ID"))
		}

		headerID := sanitizeHeaderID(requestID)
		fiberCtx.Set("X-Request-ID", headerID)

		// Start a span for the HTTP request with semantic convention attributes
		method := fiberCtx.Method()

		var route string
		if r := fiberCtx.Route(); r != nil {
			route = r.Path
		}

		if route == "" {
			route = fiberCtx.Path()
		}

		spanName := fmt.Sprintf("%s %s", method, route)

		ctx, span := tracer.Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
		)
		defer span.End()

		// Set HTTP semantic convention attributes (required for spanmetrics connector)
		span.SetAttributes(
			semconv.HTTPMethod(method),
			semconv.HTTPRoute(route),
			semconv.HTTPScheme(fiberCtx.Protocol()),
			semconv.HTTPTarget(fiberCtx.OriginalURL()),
			semconv.NetHostName(fiberCtx.Hostname()),
		)

		if headerID != "" {
			span.SetAttributes(attribute.String("request_id", headerID))
		}

		ctx = libCommons.ContextWithLogger(ctx, logger)
		ctx = libCommons.ContextWithTracer(ctx, tracer)
		ctx = libCommons.ContextWithHeaderID(ctx, headerID)
		ctx = libCommons.ContextWithMetricFactory(ctx, metricFactory)
		fiberCtx.SetUserContext(ctx)

		// Execute the request handler
		err := fiberCtx.Next()

		// Set HTTP status code attribute after handler completes (required for spanmetrics)
		statusCode := fiberCtx.Response().StatusCode()
		span.SetAttributes(semconv.HTTPStatusCode(statusCode))

		// Record error on span if handler returned an error
		if err != nil {
			libOpentelemetry.HandleSpanError(span, "request handler error", err)
		}

		// Mark span as error if status code >= 400
		if statusCode >= http.StatusBadRequest {
			span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", statusCode))
		}

		return err
	}
}

// healthHandler responds to liveness probe requests.
//
//	@Summary		Liveness check
//	@Description	Returns a simple health check response to indicate the service is alive.
//	@Description	Used by Kubernetes liveness probes to detect if the service needs to be restarted.
//	@Tags			Health
//	@Produce		plain
//	@Success		200	{string}	string	"healthy"
//	@Router			/health [get]
//	@ID				getHealth
func healthHandler(c *fiber.Ctx) error {
	c.Type("txt")

	return c.SendString("healthy")
}

// readinessHandler creates a handler that responds to readiness probe requests.
//
//	@Summary		Readiness check
//	@Description	Checks if the service is ready to accept traffic by verifying all required dependencies.
//	@Description	Used by Kubernetes readiness probes to control traffic routing.
//	@Description	Returns 200 OK when all required dependencies are healthy, 503 Service Unavailable otherwise.
//	@Description	Dependency check details are only included in non-production environments.
//	@Tags			Health
//	@Produce		json
//	@Success		200	{object}	ReadinessResponse	"Service is ready to accept traffic"
//	@Failure		503	{object}	ReadinessResponse	"Service is not ready (degraded state)"
//	@Router			/ready [get]
//	@ID				getReady
func readinessHandler(cfg *Config, deps *HealthDependencies, logger libLog.Logger) fiber.Handler {
	return func(fiberCtx *fiber.Ctx) error {
		ctx := fiberCtx.UserContext()
		if ctx == nil {
			ctx = context.Background()
		}

		status, readyStatus, checks := evaluateReadinessChecks(ctx, deps, logger)

		response := ReadinessResponse{
			Status: readyStatus,
		}

		if shouldIncludeReadinessDetails(cfg) {
			response.Checks = &DependencyChecks{
				Database:        checksToString(checks, "database", logger),
				DatabaseReplica: checksToString(checks, "databaseReplica", logger),
				Redis:           checksToString(checks, "redis", logger),
				RabbitMQ:        checksToString(checks, "rabbitmq", logger),
				ObjectStorage:   checksToString(checks, "objectStorage", logger),
			}
		}

		return fiberCtx.Status(status).JSON(response)
	}
}

func checksToString(checks fiber.Map, key string, logger libLog.Logger) string {
	if checks == nil {
		return statusUnknown
	}

	val, ok := checks[key]
	if !ok {
		return statusUnknown
	}

	stringVal, ok := val.(string)
	if !ok {
		if logger != nil {
			logger.Log(context.Background(), libLog.LevelDebug, "Readiness check value not a string: "+key)
		}

		return statusUnknown
	}

	return stringVal
}

func evaluateReadinessChecks(
	ctx context.Context,
	deps *HealthDependencies,
	logger libLog.Logger,
) (int, string, fiber.Map) {
	checks := fiber.Map{}
	allOk := true

	postgresOptional := deps != nil && deps.PostgresOptional
	postgresCheck, postgresAvailable := resolvePostgresCheck(deps)
	postgresOk := applyReadinessCheck(
		ctx,
		"database",
		checks,
		postgresCheck,
		postgresAvailable,
		postgresOptional,
		logger,
	)
	allOk = allOk && postgresOk

	replicaOptional := deps != nil && deps.PostgresReplicaOptional
	replicaCheck, replicaAvailable := resolvePostgresReplicaCheck(deps)
	replicaOk := applyReadinessCheck(
		ctx,
		"databaseReplica",
		checks,
		replicaCheck,
		replicaAvailable,
		replicaOptional,
		logger,
	)
	allOk = allOk && replicaOk

	redisOptional := deps != nil && deps.RedisOptional
	redisCheck, redisAvailable := resolveRedisCheck(deps)
	redisOk := applyReadinessCheck(
		ctx,
		"redis",
		checks,
		redisCheck,
		redisAvailable,
		redisOptional,
		logger,
	)
	allOk = allOk && redisOk

	rabbitOptional := deps != nil && deps.RabbitMQOptional
	rabbitCheck, rabbitAvailable := resolveRabbitMQCheck(deps)
	rabbitOk := applyReadinessCheck(
		ctx,
		"rabbitmq",
		checks,
		rabbitCheck,
		rabbitAvailable,
		rabbitOptional,
		logger,
	)
	allOk = allOk && rabbitOk

	objectStorageOptional := deps != nil && deps.ObjectStorageOptional
	objectStorageCheck, objectStorageAvailable := resolveObjectStorageCheck(deps)
	objectStorageOk := applyReadinessCheck(
		ctx,
		"objectStorage",
		checks,
		objectStorageCheck,
		objectStorageAvailable,
		objectStorageOptional,
		logger,
	)
	allOk = allOk && objectStorageOk

	status := fiber.StatusOK
	readyStatus := "ok"

	if !allOk {
		status = fiber.StatusServiceUnavailable
		readyStatus = "degraded"
	}

	return status, readyStatus, checks
}

func resolvePostgresCheck(deps *HealthDependencies) (HealthCheckFunc, bool) {
	if deps == nil {
		return nil, false
	}

	if deps.PostgresCheck != nil {
		return deps.PostgresCheck, true
	}

	if deps.Postgres == nil {
		return nil, false
	}

	return func(ctx context.Context) error {
		db, err := deps.Postgres.Primary()
		if err != nil {
			return fmt.Errorf("postgres health check: get primary db failed: %w", err)
		}

		if db == nil {
			return errPostgresPrimaryNil
		}

		if err := db.PingContext(ctx); err != nil {
			return fmt.Errorf("postgres health check: ping failed: %w", err)
		}

		return nil
	}, true
}

func resolvePostgresReplicaCheck(deps *HealthDependencies) (HealthCheckFunc, bool) {
	if deps == nil {
		return nil, false
	}

	if deps.PostgresReplicaCheck != nil {
		return deps.PostgresReplicaCheck, true
	}

	if deps.PostgresReplica == nil {
		return nil, false
	}

	return func(ctx context.Context) error {
		resolver, err := deps.PostgresReplica.Resolver(ctx)
		if err != nil {
			return fmt.Errorf("postgres replica health check: get resolver failed: %w", err)
		}

		if resolver == nil {
			return errReplicaResolverNil
		}

		replicas := resolver.ReplicaDBs()
		if len(replicas) == 0 {
			return errNoReplicasConfigured
		}

		checked := false

		for i, replica := range replicas {
			if replica == nil {
				continue
			}

			if err := replica.PingContext(ctx); err != nil {
				return fmt.Errorf("postgres replica health check: ping replica[%d] failed: %w", i, err)
			}

			checked = true
		}

		if !checked {
			return errNoNonNilReplicas
		}

		return nil
	}, true
}

func resolveRedisCheck(deps *HealthDependencies) (HealthCheckFunc, bool) {
	if deps == nil {
		return nil, false
	}

	if deps.RedisCheck != nil {
		return deps.RedisCheck, true
	}

	if deps.Redis == nil {
		return nil, false
	}

	return func(ctx context.Context) error {
		client, err := deps.Redis.GetClient(ctx)
		if err != nil {
			return fmt.Errorf("redis health check: get client failed: %w", err)
		}

		if client == nil {
			return errRedisClientNil
		}

		if err := client.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("redis health check: ping failed: %w", err)
		}

		return nil
	}, true
}

func resolveRabbitMQCheck(deps *HealthDependencies) (HealthCheckFunc, bool) {
	if deps == nil {
		return nil, false
	}

	if deps.RabbitMQCheck != nil {
		return deps.RabbitMQCheck, true
	}

	if deps.RabbitMQ == nil {
		return nil, false
	}

	return func(ctx context.Context) error {
		if deps.RabbitMQ.HealthCheckURL != "" {
			if err := checkRabbitMQHTTPHealth(ctx, deps.RabbitMQ.HealthCheckURL); err == nil {
				return nil
			}
		}

		return deps.RabbitMQ.EnsureChannel()
	}, true
}

func resolveObjectStorageCheck(deps *HealthDependencies) (HealthCheckFunc, bool) {
	if deps == nil {
		return nil, false
	}

	if deps.ObjectStorageCheck != nil {
		return deps.ObjectStorageCheck, true
	}

	if deps.ObjectStorage == nil {
		return nil, false
	}

	return func(ctx context.Context) error {
		// We just check that we can reach the storage by checking for a non-existent key.
		// The Exists call will return false if the key doesn't exist (expected),
		// but will error if the storage is unreachable.
		_, err := deps.ObjectStorage.Exists(ctx, ".health-check")
		if err != nil {
			return fmt.Errorf("object storage health check: %w", err)
		}

		return nil
	}, true
}

const rabbitMQHealthCheckTimeout = 5 * time.Second

// rabbitMQHTTPClient is a reusable HTTP client for RabbitMQ health checks.
// http.Client is safe for concurrent use, so a single package-level instance
// avoids per-call allocations and connection pool churn.
var rabbitMQHTTPClient = &http.Client{Timeout: rabbitMQHealthCheckTimeout}

func checkRabbitMQHTTPHealth(ctx context.Context, healthURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, http.NoBody)
	if err != nil {
		return fmt.Errorf("rabbitmq health check: create request: %w", err)
	}

	resp, err := rabbitMQHTTPClient.Do(req) // #nosec G704 -- internal RabbitMQ health check, URL is from application config
	if err != nil {
		return fmt.Errorf("rabbitmq health check: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: %d", errRabbitMQUnhealthy, resp.StatusCode)
	}

	return nil
}

// applyReadinessCheck performs a readiness check and returns true if the check passed
// or the dependency is optional (allowing the service to remain ready despite optional failures).
func applyReadinessCheck(
	ctx context.Context,
	name string,
	checks fiber.Map,
	checkFunc HealthCheckFunc,
	available, optional bool,
	logger libLog.Logger,
) bool {
	if !available || checkFunc == nil {
		checks[name] = statusUnknown

		return optional
	}

	// Apply a per-check timeout to prevent a single hung dependency
	// from blocking the entire readiness probe.
	const perCheckTimeout = 5 * time.Second

	checkCtx, checkCancel := context.WithTimeout(ctx, perCheckTimeout)
	defer checkCancel()

	if err := checkFunc(checkCtx); err != nil {
		checks[name] = "down"
		if logger != nil {
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("Readiness check failed: %s %v", name, err))
		}

		return optional
	}

	checks[name] = "ok"

	return true
}

func shouldIncludeReadinessDetails(cfg *Config) bool {
	if cfg == nil {
		return false
	}

	return !IsProductionEnvironment(cfg.App.EnvName)
}

// NewRateLimiter creates a rate limiter middleware that uses UserID/TenantID from context.
// This middleware MUST be applied AFTER auth middleware to access user context.
// Order: Auth → RateLimiter → Handlers
// If storage is provided, uses it for distributed rate limiting across multiple instances.
// Returns a no-op middleware if rate limiting is disabled via RateLimitEnabled config.
func NewRateLimiter(cfg *Config, storage fiber.Storage) fiber.Handler {
	if !cfg.RateLimit.Enabled {
		return func(c *fiber.Ctx) error {
			return c.Next()
		}
	}

	limiterCfg := limiter.Config{
		Max:        cfg.RateLimit.Max,
		Expiration: time.Duration(cfg.RateLimit.ExpirySec) * time.Second,
		KeyGenerator: func(fiberCtx *fiber.Ctx) string {
			ctx := fiberCtx.UserContext()
			if ctx != nil {
				if userID, ok := ctx.Value(auth.UserIDKey).(string); ok && userID != "" {
					return userID
				}

				if tenantID, ok := ctx.Value(auth.TenantIDKey).(string); ok && tenantID != "" {
					return tenantID + ":" + fiberCtx.IP()
				}
			}

			return fiberCtx.IP()
		},
		LimitReached: func(fiberCtx *fiber.Ctx) error {
			fiberCtx.Set("Retry-After", strconv.Itoa(cfg.RateLimit.ExpirySec))

			return sharedhttp.RespondError(
				fiberCtx,
				fiber.StatusTooManyRequests,
				"rate_limit_exceeded",
				"rate limit exceeded",
			)
		},
	}

	if storage != nil {
		limiterCfg.Storage = storage
	}

	return limiter.New(limiterCfg)
}

// NewExportRateLimiter creates a rate limiter middleware for export endpoints.
// It applies stricter limits than the global rate limiter to protect resource-intensive
// report generation operations.
// If storage is provided, uses it for distributed rate limiting across multiple instances.
// Returns a no-op middleware if rate limiting is disabled via RateLimitEnabled config.
func NewExportRateLimiter(cfg *Config, storage fiber.Storage) fiber.Handler {
	if !cfg.RateLimit.Enabled {
		return func(c *fiber.Ctx) error {
			return c.Next()
		}
	}

	limiterCfg := limiter.Config{
		Max:        cfg.RateLimit.ExportMax,
		Expiration: time.Duration(cfg.RateLimit.ExportExpirySec) * time.Second,
		KeyGenerator: func(fiberCtx *fiber.Ctx) string {
			ctx := fiberCtx.UserContext()
			if ctx != nil {
				if userID, ok := ctx.Value(auth.UserIDKey).(string); ok && userID != "" {
					return "export:" + userID
				}

				if tenantID, ok := ctx.Value(auth.TenantIDKey).(string); ok && tenantID != "" {
					return "export:" + tenantID + ":" + fiberCtx.IP()
				}
			}

			return "export:" + fiberCtx.IP()
		},
		LimitReached: func(fiberCtx *fiber.Ctx) error {
			fiberCtx.Set("Retry-After", strconv.Itoa(cfg.RateLimit.ExportExpirySec))

			return sharedhttp.RespondError(
				fiberCtx,
				fiber.StatusTooManyRequests,
				"export_rate_limit_exceeded",
				"too many export requests, please try again later",
			)
		},
	}

	if storage != nil {
		limiterCfg.Storage = storage
	}

	return limiter.New(limiterCfg)
}

// NewDispatchRateLimiter creates a rate limiter middleware for exception dispatch endpoints.
// It applies moderate limits to protect external system integrations from overload.
// If storage is provided, uses it for distributed rate limiting across multiple instances.
// Returns a no-op middleware if rate limiting is disabled via RateLimitEnabled config.
func NewDispatchRateLimiter(cfg *Config, storage fiber.Storage) fiber.Handler {
	if !cfg.RateLimit.Enabled {
		return func(c *fiber.Ctx) error {
			return c.Next()
		}
	}

	limiterCfg := limiter.Config{
		Max:        cfg.RateLimit.DispatchMax,
		Expiration: time.Duration(cfg.RateLimit.DispatchExpirySec) * time.Second,
		KeyGenerator: func(fiberCtx *fiber.Ctx) string {
			ctx := fiberCtx.UserContext()
			if ctx != nil {
				if userID, ok := ctx.Value(auth.UserIDKey).(string); ok && userID != "" {
					return "dispatch:" + userID
				}

				if tenantID, ok := ctx.Value(auth.TenantIDKey).(string); ok && tenantID != "" {
					return "dispatch:" + tenantID + ":" + fiberCtx.IP()
				}
			}

			return "dispatch:" + fiberCtx.IP()
		},
		LimitReached: func(fiberCtx *fiber.Ctx) error {
			fiberCtx.Set("Retry-After", strconv.Itoa(cfg.RateLimit.DispatchExpirySec))

			return sharedhttp.RespondError(
				fiberCtx,
				fiber.StatusTooManyRequests,
				"dispatch_rate_limit_exceeded",
				"too many dispatch requests, please try again later",
			)
		},
	}

	if storage != nil {
		limiterCfg.Storage = storage
	}

	return limiter.New(limiterCfg)
}
