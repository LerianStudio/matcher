# Multi-Tenant Activation Guide

Operational guide for enabling multi-tenant mode in the Matcher service.

## 1. Components Table

| Component | Service Const | Module Const | Resources | What Was Adapted |
|-----------|---------------|--------------|-----------|------------------|
| Ingestion | `matcher` | `ingestion` | Redis dedupe, RabbitMQ publisher | Redis key prefixing via `valkey.GetKeyFromContext`, RabbitMQ per-tenant vhosts via `tmrabbitmq.Manager`, X-Tenant-ID headers |
| Matching | `matcher` | `matching` | RabbitMQ publisher | RabbitMQ per-tenant vhosts via `tmrabbitmq.Manager`, X-Tenant-ID headers |
| Reporting | `matcher` | `reporting` | Redis cache, S3 storage | Redis key prefixing via `valkey.GetKeyFromContext`, S3 key prefixing via `tms3.GetObjectStorageKeyForTenant` |
| Configuration | `matcher` | `configuration` | PostgreSQL (via middleware) | Tenant middleware resolves DB connections |
| Governance | `matcher` | `governance` | PostgreSQL (via middleware) | Tenant middleware resolves DB connections |
| Bootstrap | `matcher` | N/A | TenantMiddleware, PostgresManager, RabbitMQManager | lib-commons v3 tenant-manager components |

## 2. Environment Variables

| Variable | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `MULTI_TENANT_ENABLED` | bool | `false` | No | Master switch for multi-tenant mode |
| `MULTI_TENANT_URL` | string | — | When enabled | Tenant Manager API URL |
| `MULTI_TENANT_ENVIRONMENT` | string | `staging` | No | Environment name for Tenant Manager |
| `MULTI_TENANT_MAX_TENANT_POOLS` | int | `100` | No | Max PostgreSQL connection pools |
| `MULTI_TENANT_IDLE_TIMEOUT_SEC` | int | `300` | No | Idle pool eviction timeout (seconds) |
| `MULTI_TENANT_CIRCUIT_BREAKER_THRESHOLD` | int | `5` | No | Circuit breaker failure threshold |
| `MULTI_TENANT_CIRCUIT_BREAKER_TIMEOUT_SEC` | int | `30` | No | Circuit breaker recovery timeout (seconds) |

## 3. How to Activate

```bash
export MULTI_TENANT_ENABLED=true
export MULTI_TENANT_URL=http://tenant-manager:8080
export MULTI_TENANT_ENVIRONMENT=production
```

Start the service alongside the Tenant Manager. The service will:

1. Create a Tenant Manager HTTP client with circuit breaker
2. Create PostgreSQL Manager for per-tenant connection pools
3. Create RabbitMQ Manager for per-tenant vhost isolation
4. Register TenantMiddleware in the HTTP chain (after auth, before handlers)

Middleware order: `Auth -> TenantExtract -> TenantDB -> Idempotency -> RateLimiter -> Handlers`

## 4. How to Verify

1. Check startup logs for: `Multi-tenant mode enabled via Tenant Manager: url=...`
2. Send a request with a JWT containing `tenantId` claim
3. Verify Redis keys are prefixed: `tenant:{tenantID}:matcher:dedupe:...`
4. Verify S3 objects are prefixed: `{tenantID}/exports/...`
5. Verify RabbitMQ messages have `X-Tenant-ID` header

## 5. How to Deactivate

```bash
export MULTI_TENANT_ENABLED=false
# or simply unset:
unset MULTI_TENANT_ENABLED
```

The service will log: `Running in SINGLE-TENANT MODE (MULTI_TENANT_ENABLED=false)` and operate with static connections.

## 6. Migration Notes

When enabling multi-tenant on an existing deployment:

- **Redis cache**: Existing unprefixed cache keys will be orphaned until TTL expiry (1-5 minutes). Cache data is automatically rebuilt on next access. No action needed.
- **Redis dedupe**: Existing dedupe keys expire based on configured TTL. During the transition, previously seen transactions may be re-processed once.
- **S3 objects**: Existing export files at unprefixed paths will NOT be accessible in multi-tenant mode. If you need access to old exports, either migrate objects with a script or keep a single-tenant instance for legacy access.
- **PostgreSQL**: No migration needed -- the TenantMiddleware resolves connections dynamically via Tenant Manager API.
- **RabbitMQ**: No migration needed -- the RabbitMQ Manager creates per-tenant vhost connections on demand.

## 7. Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `MULTI_TENANT_URL is required when MULTI_TENANT_ENABLED=true` | Missing Tenant Manager URL | Set `MULTI_TENANT_URL` |
| `failed to initialize lib-commons v3 logger` | Logger initialization failed | Check OTEL configuration |
| `get tenant channel: connection refused` | Tenant Manager unreachable | Verify `MULTI_TENANT_URL` and network |
| `circuit breaker open` | Too many Tenant Manager failures | Check Tenant Manager health, wait for recovery timeout |
| `tenant ID is required in multi-tenant mode` | JWT missing `tenantId` claim | Ensure auth is enabled and JWT contains tenant claims |

## 8. Architecture Overview

```
Request + JWT -> Auth Middleware -> TenantExtractor -> TenantMiddleware -> Handler
                                                           |
                                                   Tenant Manager API
                                                           |
                                       Per-tenant PostgreSQL / RabbitMQ / Redis / S3
```

When `MULTI_TENANT_ENABLED=false`, TenantMiddleware is not registered and the service uses static connections.
