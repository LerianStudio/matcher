# Bootstrap Package

The `internal/bootstrap` package initializes the Matcher service. It loads configuration, creates infrastructure connections, wires application modules, configures HTTP routes/middleware, and starts the service lifecycle via the lib-commons launcher.

## Overview

This package handles:

1. **Configuration**: loading and validating environment variables.
2. **Infrastructure**: PostgreSQL (primary/replica), Redis, RabbitMQ connections.
3. **Observability**: OpenTelemetry initialization plus request-scoped tracking helpers.
4. **Server Setup**: Fiber server with standardized middleware and error handling.
5. **Routing & Auth**: `/health`, `/ready`, and protected `/api` routes.
6. **Lifecycle**: service startup/shutdown via `lib-commons` launcher.

## Components

### Configuration (`Config`)

`Config` is loaded via `LoadConfig()` and validated with `Validate()`:

- **Validation**: enforces production constraints (TLS, auth enabled, non-default credentials).
- **DSN generation**: `PrimaryDSN()`, `ReplicaDSN()`, `RabbitMQDSN()` construct connection strings.

### Server & Middleware (`NewFiberApp`)

`NewFiberApp` builds the Fiber server with:

- **Error handler**: `customErrorHandler` standardizes JSON errors and logging.
- **Recover**: catches panics and reports via `runtime.RecoverAndLogWithContext`.
- **Request ID**: `requestid` middleware plus sanitization in telemetry middleware.
- **CORS**: driven by `CORS_ALLOWED_*` settings.
- **Telemetry middleware**: injects logger/tracer/header ID into request context and ensures `X-Request-ID` is set.

### Routes & Health

`RegisterRoutes` configures:

- `GET /health`: returns `healthy`.
- `GET /ready`: returns JSON readiness status; includes detailed dependency checks outside production.

Readiness uses `HealthDependencies`. Redis is optional by default; dependencies can be marked optional via `HealthDependencies.{Postgres,Redis,RabbitMQ}Optional`.

### Infrastructure Wiring (`InitServers` / `InitServersWithOptions`)

`InitServersWithOptions` is the composition root:

1. Load config (`LoadConfig()`).
2. Initialize logger + telemetry.
3. Create Postgres/Redis/RabbitMQ connections.
4. Connect infrastructure and build health dependencies.
5. Create the Fiber app and register routes.
6. Initialize configuration module.
7. Create shared outbox repository (owned by the Outbox context).
8. Initialize ingestion module (requires outbox repo + ingestion publisher).
9. Initialize matching module (requires outbox repo + matching publisher).
10. Initialize reporting/governance/exception modules.
11. Create the outbox dispatcher (requires outbox repo + publishers).
12. Construct the `Service` (server + outbox runner).

The init order is explicit because several modules share infrastructure or publish to the outbox:
- `outboxRepo` is instantiated once from `internal/outbox/adapters/postgres` and shared across modules.
- Ingestion and matching both depend on the same outbox repository instance for transactional event storage.
- The dispatcher depends on the outbox repo plus both RabbitMQ publishers.

If you add a module with cross-context dependencies, update this list to keep the wiring order visible.

### Observability Helpers

`TrackingContext` wraps `lib-commons` tracking components (logger, tracer, header ID). `InitTelemetry` configures OpenTelemetry exporters with `lib-commons`.

### Database Metrics (`db_metrics.go`)

Exports PostgreSQL connection pool metrics (open connections, in-use, idle, wait count) via OpenTelemetry gauges. Initialized during server startup.

### Migration Management (`migrations.go`)

`RunMigrations` applies pending schema migrations using golang-migrate. Supports dirty state recovery (`RecoverDirtyMigrationState`) for environments where a previous migration was interrupted.

## Usage

### Application Entry Point

```go
func main() {
    service, err := bootstrap.InitServers()
    if err != nil {
        log.Fatalf("Failed to initialize: %v", err)
    }

    service.Run()
}
```

Use `InitServersWithOptions(&bootstrap.Options{Logger: ...})` when you need a custom logger.

### Adding a New Domain

Wire new modules the same way as `initConfigurationModule`:

```go
configContextRepository := configContextRepo.NewRepository(postgresConnection)

configSourceRepository, err := configSourceRepo.NewRepository(postgresConnection)
if err != nil {
    return fmt.Errorf("create source repository: %w", err)
}

configFieldMapRepository := configFieldMapRepo.NewRepository(postgresConnection)
configMatchRuleRepository := configMatchRuleRepo.NewRepository(postgresConnection)

configCommandUseCase, err := configCommand.NewUseCase(
    configContextRepository,
    configSourceRepository,
    configFieldMapRepository,
    configMatchRuleRepository,
)
if err != nil {
    return fmt.Errorf("create config command use case: %w", err)
}

configQueryUseCase, err := configQuery.NewUseCase(
    configContextRepository,
    configSourceRepository,
    configFieldMapRepository,
    configMatchRuleRepository,
)
if err != nil {
    return fmt.Errorf("create config query use case: %w", err)
}

configHandler, err := configHTTP.NewHandler(configCommandUseCase, configQueryUseCase)
if err != nil {
    return fmt.Errorf("create config handler: %w", err)
}

if err := configHTTP.RegisterRoutes(routes.Protected, configHandler); err != nil {
    return fmt.Errorf("register configuration routes: %w", err)
}
```

## Configuration Reference

Key environment variables:

| Category | Variable | Description |
|----------|----------|-------------|
| **Server** | `SERVER_ADDRESS` | Bind address (default `:4018`) |
| | `HTTP_BODY_LIMIT_BYTES` | Max HTTP body size |
| | `CORS_ALLOWED_ORIGINS` | Allowed CORS origins |
| | `CORS_ALLOWED_METHODS` | Allowed CORS methods |
| | `CORS_ALLOWED_HEADERS` | Allowed CORS headers |
| | `SERVER_TLS_CERT_FILE` / `SERVER_TLS_KEY_FILE` | TLS configuration |
| **Auth** | `AUTH_ENABLED` | Toggle JWT validation |
| | `AUTH_SERVICE_ADDRESS` | Auth service host |
| | `AUTH_JWT_SECRET` | JWT secret when auth enabled |
| | `DEFAULT_TENANT_ID` / `DEFAULT_TENANT_SLUG` | Fallback tenant settings |
| **DB** | `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_SSLMODE` | Primary database configuration |
| | `POSTGRES_REPLICA_*` | Replica overrides |
| **Redis** | `REDIS_HOST`, `REDIS_PASSWORD`, `REDIS_DB`, `REDIS_PROTOCOL`, `REDIS_TLS`, `REDIS_CA_CERT` | Redis connection settings |
| | `REDIS_POOL_SIZE`, `REDIS_MIN_IDLE_CONNS` | Redis pool tuning |
| | `REDIS_READ_TIMEOUT_MS`, `REDIS_WRITE_TIMEOUT_MS`, `REDIS_DIAL_TIMEOUT_MS` | Redis timeouts |
| | `REDIS_MASTER_NAME` | Redis sentinel master |
| **RabbitMQ** | `RABBITMQ_URI`, `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD`, `RABBITMQ_VHOST`, `RABBITMQ_HEALTH_URL`, `RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK` | RabbitMQ configuration |
| **Telemetry** | `ENABLE_TELEMETRY`, `OTEL_LIBRARY_NAME`, `OTEL_RESOURCE_SERVICE_NAME`, `OTEL_RESOURCE_SERVICE_VERSION`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT` | OpenTelemetry settings |

See `Config` in `config.go` for the complete list.
