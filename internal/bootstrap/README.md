# Bootstrap Package

The `internal/bootstrap` package initializes the Matcher service. It loads configuration, creates infrastructure connections, wires application modules, configures HTTP routes/middleware, and starts the service lifecycle via the lib-commons launcher.

## Overview

This package handles:

1. **Configuration**: loading and validating environment variables.
2. **Infrastructure**: PostgreSQL (primary/replica), Redis, RabbitMQ connections.
3. **Observability**: OpenTelemetry initialization plus request-scoped tracking helpers.
4. **Server Setup**: Fiber server with standardized middleware and error handling.
5. **Routing & Auth**: `/health`, `/readyz`, and protected `/api` routes.
6. **Lifecycle**: service startup/shutdown via `lib-commons` launcher.
7. **Systemplane**: Runtime configuration authority with hot-reloadable settings, change history, and schema.
8. **Dynamic Infrastructure**: Runtime switching of database connections, Redis, object storage, and partition management.
9. **Worker Management**: Lifecycle management for background workers (outbox dispatcher, archival, scheduling, export, cleanup, discovery).
10. **Rate Limiting**: Static and dynamic rate limiting with configurable policies.

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

- `GET /health`: liveness endpoint, returns 503 until startup self-probe succeeds, then returns `healthy`.
- `GET /readyz`: returns JSON readiness status using the canonical contract; always includes detailed dependency checks.

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
11. Initialize discovery module (requires fetcher client + schema cache).
12. Create the outbox dispatcher (requires outbox repo + publishers).
13. Construct the `Service` (server + outbox runner).

The init order is explicit because several modules share infrastructure or publish to the outbox:
- The outbox repository is built from `lib-commons/v5/commons/outbox/postgres` (once per process) and shared across modules via `sharedPorts.OutboxRepository`.
- Ingestion and matching both depend on the same outbox repository instance for transactional event storage.
- The dispatcher (provided by `lib-commons/v5/commons/outbox`) depends on the outbox repo plus both RabbitMQ publishers.

If you add a module with cross-context dependencies, update this list to keep the wiring order visible.

### Observability Helpers

`TrackingContext` wraps `lib-commons` tracking components (logger, tracer, header ID). `InitTelemetry` configures OpenTelemetry exporters with `lib-commons`.

### Database Metrics (`db_metrics.go`)

Exports PostgreSQL connection pool metrics (open connections, in-use, idle, wait count) via OpenTelemetry gauges. Initialized during server startup.

### Migration Management (`migrations.go`)

`RunMigrations` applies pending schema migrations using golang-migrate. Supports dirty state recovery (`RecoverDirtyMigrationState`) for environments where a previous migration was interrupted.

### Systemplane Integration

The systemplane (`lib-commons/v5/commons/systemplane`) is integrated during bootstrap to provide runtime configuration authority. The admin HTTP surface is mounted separately via `MountSystemplaneAPI` at `/system/:namespace/:key`:

- **Config Manager**: Wraps the systemplane service to provide `configManager.Get()` for runtime config reads.
- **Key Registry**: All configurable keys are registered with types, defaults, scopes, and mutability metadata.
- **Reconcilers**: HTTP, publisher, and worker reconcilers apply config changes at runtime without restart.
- **Change Feed**: PostgreSQL LISTEN/NOTIFY or MongoDB change streams propagate config changes.

Key categories registered: application/server, archival, messaging, PostgreSQL, runtime HTTP, runtime services, storage/export, tenancy, and workers.

### Dynamic Infrastructure

Runtime-switchable infrastructure adapters:
- `dynamic_infrastructure_provider.go`: Database connection switching.
- `dynamic_redis_storage.go`: Redis connection management.
- `dynamic_object_storage.go`: Object storage (S3/SeaweedFS) switching.
- `dynamic_partition_manager.go`: Partition management.
- `dynamic_discovery_runtime.go`: Discovery context runtime.
- `dynamic_fetcher_client.go`: Fetcher client configuration.

### Worker Manager

`worker_manager.go` manages the lifecycle of all background workers:
- Outbox dispatcher
- Archival worker (governance)
- Scheduler worker (configuration)
- Export worker (reporting)
- Cleanup worker (reporting)
- Discovery worker and extraction poller

Workers can be started, stopped, and reconfigured at runtime through systemplane.

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
| **Auth** | `PLUGIN_AUTH_ENABLED` | Toggle JWT validation |
| | `PLUGIN_AUTH_ADDRESS` | Auth service host |
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
