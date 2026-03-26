# Multi-Tenant Activation Guide

## 1. Overview

| Field | Value |
|-------|-------|
| **Service** | matcher (transaction reconciliation engine) |
| **Service type** | Product (not plugin) |
| **Stack** | PostgreSQL, Redis, RabbitMQ, S3/Object Storage |
| **Multi-tenant model** | `tenantId` from JWT &rarr; TenantMiddleware &rarr; database-per-tenant via `tmpostgres.Manager` |

In single-tenant mode (default), matcher uses a singleton database connection and the `public` schema. When multi-tenant mode is enabled, each tenant gets an isolated database provisioned and managed by the Tenant Manager service.

## 2. Components

| Component | Service Const | Module Const | Resources | What Was Adapted |
|-----------|--------------|-------------|-----------|-----------------|
| matcher | `constants.ApplicationName` (`"matcher"`) | N/A (single-module) | PostgreSQL, Redis, RabbitMQ, S3 | **PG:** `tmpostgres.Manager` &mdash; per-tenant connection pool with circuit breaker. **Redis:** Valkey key prefixing (`tenant:{tenantID}:{key}`). **RabbitMQ:** `tmrabbitmq.Manager` (Layer 1 vhost isolation + Layer 2 `X-Tenant-ID` header). **S3:** `s3.GetObjectStorageKey` prefixes objects with `{tenantID}/`. |

## 3. Environment Variables

All variables below are in the `TenancyConfig` struct (`internal/bootstrap/config.go`).

| Name | Type | Default | Required | Description |
|------|------|---------|----------|-------------|
| `MULTI_TENANT_ENABLED` | `bool` | `false` | No | Master switch. Enables multi-tenant infrastructure when `true`. |
| `MULTI_TENANT_URL` | `string` | _(empty)_ | **Yes** (when enabled) | Base URL of the Tenant Manager service (e.g., `https://tenant-manager.example.com`). |
| `MULTI_TENANT_ENVIRONMENT` | `string` | _(empty)_ | No | Environment label sent to Tenant Manager for environment-scoped tenant resolution. |
| `MULTI_TENANT_MAX_TENANT_POOLS` | `int` | `100` | No | Maximum number of concurrent tenant connection pools. Pools beyond this limit are evicted LRU. |
| `MULTI_TENANT_IDLE_TIMEOUT_SEC` | `int` | `300` | No | Seconds a tenant pool can remain idle before being closed and evicted. |
| `MULTI_TENANT_CIRCUIT_BREAKER_THRESHOLD` | `int` | `5` | No | Consecutive Tenant Manager failures before the circuit breaker opens. |
| `MULTI_TENANT_CIRCUIT_BREAKER_TIMEOUT_SEC` | `int` | `30` | No | Seconds the circuit breaker stays open before allowing a probe request. |
| `MULTI_TENANT_SERVICE_API_KEY` | `string` | _(empty)_ | **Yes** (when enabled) | API key for authenticating with the Tenant Manager service. Excluded from JSON serialization. |

Additionally, the following default-tenant variables remain relevant in both modes:

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `DEFAULT_TENANT_ID` | `string` | `11111111-1111-1111-1111-111111111111` | UUID of the default (fallback) tenant. |
| `DEFAULT_TENANT_SLUG` | `string` | `default` | Slug of the default tenant. |

## 4. How to Activate

1. **Ensure the Tenant Manager service is running** and reachable from the matcher network. Confirm with a health check against the Tenant Manager's `/health` endpoint.

2. **Set the required environment variables:**
   ```bash
   export MULTI_TENANT_ENABLED=true
   export MULTI_TENANT_URL=https://tenant-manager.example.com
   export MULTI_TENANT_SERVICE_API_KEY=your-api-key
   ```

3. **Optionally tune pool and circuit breaker settings:**
   ```bash
   export MULTI_TENANT_MAX_TENANT_POOLS=200
   export MULTI_TENANT_IDLE_TIMEOUT_SEC=600
   export MULTI_TENANT_CIRCUIT_BREAKER_THRESHOLD=3
   export MULTI_TENANT_CIRCUIT_BREAKER_TIMEOUT_SEC=60
   ```

4. **Optionally set the environment label** (if the Tenant Manager uses environment-scoped resolution):
   ```bash
   export MULTI_TENANT_ENVIRONMENT=production
   ```

5. **Start the matcher service.** On startup, the bootstrap sequence initializes `tmpostgres.Manager`, `tmrabbitmq.Manager`, and configures Redis key prefixing.

6. **Verify logs** show multi-tenant initialization messages (see next section).

## 5. How to Verify

1. **Check startup logs.** Look for multi-tenant initialization messages indicating that `tmpostgres.Manager` and `tmrabbitmq.Manager` were created successfully.

2. **Send a request with a JWT containing a `tenantId` claim.** The Auth Middleware extracts the tenant ID and the TenantMiddleware resolves the tenant's dedicated database connection.

3. **Verify tenant isolation.** Query the matcher API with JWTs for two different tenants. Confirm that data created under one tenant is not visible to the other.

4. **Check OpenTelemetry metrics.** If telemetry is enabled (`ENABLE_TELEMETRY=true`), the `tenant_connections_total` metric should increment as new tenant pools are created.

5. **Monitor connection pools.** With `DB_METRICS_INTERVAL_SEC` (default: 15s), database pool metrics are reported per-tenant. Verify pool counts align with the number of active tenants.

## 6. How to Deactivate

1. Set `MULTI_TENANT_ENABLED=false` (or remove the variable entirely -- the default is `false`).
2. Restart the matcher service.
3. The service operates in single-tenant mode using the singleton database connection and `public` schema. The default tenant ID/slug (`DEFAULT_TENANT_ID`, `DEFAULT_TENANT_SLUG`) apply to all requests.

## 7. Deployment Considerations

### Redis key format

Keys use `tenant:{tenantID}:{key}` format in multi-tenant mode. During a rolling deployment from single-tenant to multi-tenant, old-format keys are treated as cache misses until their TTL expires. This is self-healing and typically resolves within 1-5 minutes depending on cache TTLs (idempotency, deduplication, rate limiting).

### S3 object key format

New objects are stored with a `{tenantID}/path` prefix. Existing objects created before multi-tenant activation remain at their original paths. If historical data must be accessible per-tenant, consider a one-time migration script to relocate objects under the appropriate tenant prefix.

### RabbitMQ

Each tenant gets a dedicated vhost managed by the Tenant Manager (`tmrabbitmq.Manager` Layer 1). No manual vhost creation is needed. The `X-Tenant-ID` header (Layer 2) is set on every published message for downstream consumers that share a vhost.

### Connection pool sizing

With `MULTI_TENANT_MAX_TENANT_POOLS=100` (default) and PostgreSQL's `POSTGRES_MAX_OPEN_CONNS=25` (default), the worst-case total connection count is `100 * 25 = 2500`. Size your PostgreSQL `max_connections` accordingly. Use `MULTI_TENANT_IDLE_TIMEOUT_SEC` to reclaim pools for inactive tenants.

### Circuit breaker

If the Tenant Manager becomes unreachable, the circuit breaker opens after `MULTI_TENANT_CIRCUIT_BREAKER_THRESHOLD` (default: 5) consecutive failures. While open, all new-tenant connection requests fail fast for `MULTI_TENANT_CIRCUIT_BREAKER_TIMEOUT_SEC` (default: 30s), then a single probe request is allowed. Existing tenant pools remain usable during an outage.

## 8. Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `MULTI_TENANT_URL is required` | Multi-tenant enabled but `MULTI_TENANT_URL` not set. | Set `MULTI_TENANT_URL` to the Tenant Manager base URL. |
| `MULTI_TENANT_SERVICE_API_KEY is required` | API key not configured. | Set `MULTI_TENANT_SERVICE_API_KEY` with the key from the Tenant Manager. |
| `tenant connection returned nil db resolver` | Tenant not provisioned in Tenant Manager. | Provision the tenant via the Tenant Manager API before sending requests. |
| `circuit breaker open` | Tenant Manager unreachable after threshold failures. | Check Tenant Manager health. Requests resume automatically after the timeout. |
| `errTenantIDRequired` | RabbitMQ publish attempted without tenant context. | Ensure the JWT contains a `tenantId` claim and the Auth Middleware is active (`PLUGIN_AUTH_ENABLED=true`). |
| `context deadline exceeded` (on startup) | Tenant Manager URL is wrong or network unreachable. | Verify `MULTI_TENANT_URL` is correct and the service is reachable. Check `INFRA_CONNECT_TIMEOUT_SEC` (default: 30s). |

## 9. Architecture Diagram

```
Request Flow (multi-tenant):

  Client
    |
    v
  JWT (tenantId claim)
    |
    v
  Auth Middleware ──> extracts tenantID, tenantSlug into context
    |
    v
  TenantMiddleware ──> tmpostgres.Manager.GetConnection(tenantID)
    |                   tmrabbitmq.Manager.GetChannel(tenantID)
    |                   Redis key prefix: tenant:{tenantID}:*
    v
  Handler ──> Service ──> Repository ──> InfrastructureProvider ──> Tenant DB
                                                                     (isolated)

Request Flow (single-tenant, default):

  Client
    |
    v
  Auth Middleware (optional)
    |
    v
  Handler ──> Service ──> Repository ──> InfrastructureProvider ──> Singleton DB
                                                                     (public schema)

Background Workers (multi-tenant):

  Scheduler / Outbox Dispatcher / Archival Worker
    |
    v
  ListTenants() ──> iterates all tenant schemas (including default tenant)
    |
    v
  Per-tenant: tmpostgres.Manager.GetConnection(tenantID) ──> Tenant DB
```
