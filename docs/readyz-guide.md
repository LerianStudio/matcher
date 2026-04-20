# /readyz & /health — Operator Guide

Kubernetes readiness and liveness probes for the matcher service.

Aligned to the `ring:dev-readyz` canonical contract. Two responsibilities: `/readyz` tells K8s *whether to route traffic*; `/health` tells K8s *whether to restart the pod*. They are separate concerns with different failure semantics — do not merge them.

## Overview

`/readyz` is a **runtime dependency probe** consumed by the K8s `readinessProbe`. Every request runs live checks against Postgres (primary + replica), Redis, RabbitMQ, and the S3-compatible object store, and reports a strict JSON contract. No caching; live checks are O(ms).

`/health` is a **liveness probe** gated by a startup self-probe. It returns 200 only after `RunSelfProbe` has confirmed every required dependency at least once. If the self-probe fails (or has not yet run), `/health` returns 503 and K8s restarts the pod.

Both endpoints are mounted **before** any authentication middleware. K8s probes are unauthenticated.

## Endpoints

| Path | Purpose | Response | Authenticated |
|------|---------|----------|---------------|
| `GET /readyz` | K8s readinessProbe target | JSON (contract below) | No |
| `GET /health` | K8s livenessProbe target | Plain `"healthy"` or 503 body | No |
| `GET /version` | Build/version check | `{"version":"…","requestDate":"…"}` | No |

There is **no `/metrics` endpoint**. Metrics flow to the collector via OTLP (`OTEL_EXPORTER_OTLP_ENDPOINT`).

There is **no `/readyz/tenant/:id` endpoint**. Matcher uses shared-cluster multi-tenancy — global `/readyz` is sufficient.

## Environment Variables

| Variable | Required | Default | Values | Reload |
|----------|----------|---------|--------|--------|
| `DEPLOYMENT_MODE` | No | `local` | `saas` / `byoc` / `local` | Bootstrap |
| `SHUTDOWN_GRACE_PERIOD_SEC` | No | `12` | integer seconds | Bootstrap |
| `VERSION` | No | `0.0.0` | any string | Bootstrap |
| `POSTGRES_*`, `POSTGRES_REPLICA_*` | Yes | — | see `.config-map.example` | Bootstrap |
| `REDIS_*`, `RABBITMQ_*` | Yes | — | see `.config-map.example` | Bootstrap |
| `OBJECT_STORAGE_*` | Conditional | — | required when `EXPORT_WORKER_ENABLED=true` | Bootstrap |
| `POSTGRES_TLS_REQUIRED` | No | `false` | `true` / `false` | Bootstrap |
| `POSTGRES_REPLICA_TLS_REQUIRED` | No | `false` | `true` / `false` | Bootstrap |
| `REDIS_TLS_REQUIRED` | No | `false` | `true` / `false` | Bootstrap |
| `RABBITMQ_TLS_REQUIRED` | No | `false` | `true` / `false` | Bootstrap |
| `OBJECT_STORAGE_TLS_REQUIRED` | No | `false` | `true` / `false` | Bootstrap |

**`DEPLOYMENT_MODE`** is informational. It surfaces in the `/readyz` response envelope's `deployment_mode` field and seeds logger configuration defaults. It does NOT gate TLS enforcement.

**TLS enforcement is explicit per stack.** Each infra stack has its own `X_TLS_REQUIRED` boolean flag. When `true`, `ValidateRequiredTLS` runs at startup BEFORE any infrastructure connection opens and verifies that the stack declares TLS — otherwise it aborts bootstrap with `ErrTLSRequiredButNotDeclared` and the stack name. When `false` (default), the stack is unenforced regardless of its configured posture. Stacks are orthogonal: enable only the ones you need enforced.

**Example production posture** (every stack enforced):

```bash
POSTGRES_TLS_REQUIRED=true
POSTGRES_SSLMODE=require                 # matches the flag
POSTGRES_REPLICA_TLS_REQUIRED=true
POSTGRES_REPLICA_SSLMODE=require         # matches the flag
REDIS_TLS_REQUIRED=true
REDIS_TLS=true                           # matches the flag
RABBITMQ_TLS_REQUIRED=true
RABBITMQ_URI=amqps                       # matches the flag
OBJECT_STORAGE_TLS_REQUIRED=true
OBJECT_STORAGE_ENDPOINT=https://s3.example.com  # or empty for AWS default HTTPS
```

**`SHUTDOWN_GRACE_PERIOD_SEC`** is the delay between SIGTERM and Fiber shutdown. `/readyz` starts returning 503 immediately on SIGTERM; grace window lets K8s observe the 503 and stop routing before connections close. Default 12s is calibrated for K8s `readinessProbe periodSeconds=5 × failureThreshold=2 + 2s buffer`. Tune up for longer-running request types or higher probe intervals.

## Canonical Response Contract

```json
{
  "status": "healthy",
  "checks": {
    "postgres":         { "status": "up", "latency_ms": 2, "tls": true },
    "postgres_replica": { "status": "up", "latency_ms": 2, "tls": true },
    "redis":            { "status": "up", "latency_ms": 1, "tls": true },
    "rabbitmq":         { "status": "up", "latency_ms": 3, "tls": true },
    "object_storage":   { "status": "up", "latency_ms": 12, "tls": true }
  },
  "version": "1.3.0",
  "deployment_mode": "saas"
}
```

- `status` — `healthy` iff every check is in `{up, skipped, n/a}`; otherwise `unhealthy`. HTTP 200 when healthy, **503 when unhealthy**.
- `checks` — always present. One entry per configured dep. An optional dep whose probe fails is reported as `down` (with `error`) in the map but does NOT flip the top-level to `unhealthy`.
- `version` — from `VERSION` env, defaults to `"0.0.0"`.
- `deployment_mode` — from `DEPLOYMENT_MODE`, defaults to `"local"`.

### Per-check fields

| Field | When present | Meaning |
|-------|--------------|---------|
| `status` | Always | `up` / `down` / `degraded` / `skipped` / `n/a` |
| `latency_ms` | `up`, `down` with probe executed | Integer milliseconds |
| `tls` | Deps with TLS concern (all five for matcher) | Reflects *configured* TLS posture, not runtime cert validity |
| `error` | `down` or `degraded` | Bounded category token: `connection refused`, `timeout`, `dns failure`, `tls handshake failed`, or `check failed`. Full error goes to server logs only — response body never contains DSN / credentials. |
| `reason` | `skipped` or `n/a` | Human-readable explanation |

## Status Vocabulary

| Value | Meaning | Counts as healthy |
|-------|---------|-------------------|
| `up` | Dep reachable, probe passed | Yes |
| `down` | Dep unreachable or probe failed | **No (→ 503)** unless the dep is optional |
| `degraded` | Circuit breaker half-open or partial failure | **No (→ 503)** (matcher does not use this today) |
| `skipped` | Optional dep explicitly disabled via config | Yes (ignored in aggregation) |
| `n/a` | Not applicable in current mode | Yes (matcher does not use this today) |

## Kubernetes Probe Configuration

```yaml
readinessProbe:
  httpGet:
    path: /readyz
    port: http
  initialDelaySeconds: 5
  periodSeconds: 5
  timeoutSeconds: 2          # handler wall-clock cap is 900ms + network headroom
  failureThreshold: 2        # 2 × 5s = 10s ≤ drain grace (12s)

livenessProbe:
  httpGet:
    path: /health
    port: http
  initialDelaySeconds: 30    # allow self-probe to run
  periodSeconds: 10
  failureThreshold: 3

terminationGracePeriodSeconds: 30   # > SHUTDOWN_GRACE_PERIOD_SEC (12s) + Fiber close margin
```

Tune `readinessProbe.failureThreshold × periodSeconds` to stay ≤ `SHUTDOWN_GRACE_PERIOD_SEC`. Otherwise K8s may kill in-flight requests because the grace window ends before the probe observes the drain 503.

`timeoutSeconds: 2` is the recommended value: the handler caps its own wall-clock at 900ms (see `readyzHandlerWallClockCap`), so 2s gives ≥1.1s of kubelet + network headroom. Kubelet's default `timeoutSeconds=1` is too tight under load — bump it.

## Metrics

Three instruments are registered on meter `matcher.readyz` and exported via OTLP:

| Name | Type | Labels | Emitted |
|------|------|--------|---------|
| `readyz_check_duration_ms` | Histogram (ms buckets: 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000) | `dep`, `status` | Every `/readyz` per-dep check |
| `readyz_check_status` | Counter | `dep`, `status` | Every `/readyz` per-dep check outcome |
| `selfprobe_result` | Gauge (0 = down, 1 = up) | `dep` | Once per dep after startup self-probe completes |

Use these to alert on:

- Non-zero rate of `readyz_check_status{status="down"}` per dep.
- `selfprobe_result == 0` after boot (pod started but self-probe didn't clear — pod will be restarted on first liveness probe).
- `readyz_check_duration_ms` p99 approaching the 5s per-dep timeout (dep is slow but not yet down).

## Operational Runbook

### `/readyz` returns 503

1. Fetch the body: `curl -s http://<pod>:<port>/readyz | jq`.
2. Inspect `checks[*].status`. Any check that is `down` or `degraded` explains the 503.
3. For a `down` check, look at `error` — it is a bounded category token (`connection refused`, `timeout`, `dns failure`, `tls handshake failed`, `check failed`). Use pod logs for full dial/ping detail. Common causes:
   - Wrong DSN or endpoint host → connection refused / name resolution failure
   - Missing network policy / firewall rule
   - Dependency restarted and pool still reconnecting — usually self-heals within seconds
4. If `status=="healthy"` but the response is 503, inspect logs for the drain log line `"shutdown wait for background workers"` — pod is draining.

### `/health` returns 503

Self-probe has not flipped `selfProbeOK` true, or there is a drain in progress.

1. Inspect startup logs for `"startup self-probe failed"`. The error names the failing dep.
2. If the failure is transient (dep restarted during boot), K8s will restart the pod via livenessProbe — typically recovers within two failureThresholds (≈20s) once the dep is reachable.
3. If the failure is persistent (wrong credentials, wrong network), the pod will CrashLoopBackOff. Fix config and redeploy.

### Service refuses to start with a TLS_REQUIRED error

`ValidateRequiredTLS` aborts bootstrap with an error like `tls required but not declared: postgres (TLS_REQUIRED=true but configuration does not declare TLS)`.

1. The named stack has `X_TLS_REQUIRED=true` but its connection configuration does not declare TLS — e.g., `POSTGRES_SSLMODE=disable`, `REDIS_TLS=false`, `RABBITMQ_URI=amqp`, `OBJECT_STORAGE_ENDPOINT=http://…`.
2. Either fix the connection configuration to declare TLS (Postgres: `POSTGRES_SSLMODE=require` or stricter; Redis: `REDIS_TLS=true`; RabbitMQ: `RABBITMQ_URI=amqps`; S3: `OBJECT_STORAGE_ENDPOINT=https://…` or empty for AWS default), OR unset the `X_TLS_REQUIRED` flag for that stack if plaintext is intentional (e.g., customer-hosted BYOC with internal TLS termination).
3. A malformed configuration surfaces as `malformed dependency configuration under tls_required`. Check the stack's host/URL syntax.
4. The flags are orthogonal: enforcing Postgres does not imply enforcing Redis. Set each flag intentionally.

### In-flight requests killed during deploy

Drain grace period is too short for typical request durations.

1. Check `SHUTDOWN_GRACE_PERIOD_SEC`. Default is 12s.
2. Raise it to exceed the p99 of your slowest request type (exports, large match runs). 30–60s is reasonable for services that do batch work.
3. Also raise K8s `terminationGracePeriodSeconds` to at least `SHUTDOWN_GRACE_PERIOD_SEC + 5s` (otherwise K8s SIGKILLs before Fiber finishes its own shutdown).

### Metric `selfprobe_result{dep}=0` alerts keep firing

Self-probe only runs once at startup. The gauge records that single result and never updates. If a dep goes down after boot, `selfprobe_result` stays at 1 — use `readyz_check_status{status="down"}` for runtime alerting instead.

## Self-probe semantics & pod restart behavior

- `/health` gates on `selfProbeOK`, which flips `true` exactly once after the startup self-probe succeeds. It never flips back — the liveness endpoint deliberately ignores post-startup dep failures.
- Post-startup dep failures do NOT flip `selfProbeOK` back to false. `/health` stays 200 even while `/readyz` returns 503. That separation is intentional: steady-state dep detection is `/readyz`'s job (K8s removes the endpoint from routing), while `/health` only reports bootstrap state.
- K8s livenessProbe restart only happens when the startup self-probe never succeeded AND K8s retries exhaust. A healthy pod whose Postgres goes down mid-operation will NOT be restarted via liveness; it just stops serving traffic until Postgres recovers.
- If deps were flaky at startup (e.g., transient DNS issue during boot), the pod may CrashLoopBackOff even after deps recover. This is intentional: a restart picks up the healthy state and avoids a long-lived pod that booted in a degraded shape.

## RabbitMQ — what "ready" actually guarantees

`/readyz`-healthy for RabbitMQ means the AMQP connection is reachable and the health channel can be opened. It does NOT guarantee that matcher's queues, exchanges, or bindings exist — those are declared lazily by the first publisher / consumer that touches them. Consumers observing `/readyz` ready and then immediately publishing may hit `no queue` errors until matcher's first publisher warms up (sub-second, but non-zero). If you are building a readiness-gated consumer, either (a) retry-on-NoQueue at the consumer, or (b) gate on a matcher-specific synthetic probe that verifies your exchange/binding exists.

## Redis `tls` field — what it means

`tls: true` in the Redis check response reflects the *configured* TLS intent (`REDIS_TLS=true` or `rediss://` URL scheme). It does NOT verify any of:

- the client-side TLS configuration is complete (CA cert valid, hostname matches, etc.)
- the TLS handshake actually succeeded against the server
- the server's certificate is valid or unexpired

A misconfigured Redis TLS client will show `tls: true` alongside a `down` status with `error: "tls handshake failed"`. The field is honest about posture, not about runtime TLS health.

## Probe-vs-pool sizing (operator note)

Each `/readyz` request consumes: 1 PG primary connection + 1 PG replica connection (if configured) + 1 Redis connection + 1 S3 request from the shared connection pools. Under K8s `periodSeconds=5 × N pods`, baseline probe load is `N/5` rps per dep.

Ensure `POSTGRES_MAX_OPEN_CONNS × pod_replicas ≤ db_max_connections × 0.8` has headroom for probe load. The default pool size of 25 supports ~125 pods at `periodSeconds=5` before competing with query traffic; the 250ms TTL cache absorbs intra-second bursts so actual pool pressure is bounded at 4 probe-rounds/sec regardless of probe client count.

## Scope Fence — What `/readyz` is NOT

| Not in scope | Why | Lives instead |
|--------------|-----|---------------|
| Synthetic business-logic probes ("can run a match?") | `/readyz` is infra, not app | Not implemented; add a separate endpoint if ever needed |
| Certificate validity / expiry | `/readyz` reports configured posture, not cert health | External cert-monitoring tool |
| p99 latency / SLI thresholds | `/readyz` is binary healthy/unhealthy | Telemetry dashboards via OTLP |
| Per-tenant health | Shared-cluster multi-tenancy; global probe suffices | N/A — no `/readyz/tenant/:id` for matcher |
| Tenant Manager API reachability | Upstream is breaker-wrapped with fail-open cache for already-resolved tenants | External upstream monitoring |

## Common Errors

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `GET /readyz` returns 401 | Mounted behind auth middleware by mistake | Register `/readyz` before `routes.Protected` group (already correct in `routes.go:107`) |
| `readyz_*` metrics not in OTLP | Collector not configured or `OTEL_EXPORTER_OTLP_ENDPOINT` unset | Check `OTEL_*` env + collector receiver |
| `selfprobe_result` never observed | `RunSelfProbe` errored before any dep was probed | Inspect boot logs; usually means nil `HealthDependencies` — code path shouldn't reach it in production |
| `/readyz` response contains stale/cached data | A proxy or CDN is caching | Ensure no cache layer in front of the pod IP. The service itself does **not** cache. |
| Top-level `"deployment_mode": ""` | `defaultConfig()` not applied or override path bypassed it | Set `DEPLOYMENT_MODE` explicitly or debug config loading |
| Breaking deploy: external probes still hit `/ready` and get 404 | `/ready` was renamed to `/readyz` in this release | Update Helm/K8s manifests, monitoring, runbooks to target `/readyz` |

## Links

- Canonical contract: `ring:dev-readyz` skill (LerianStudio/ring repo)
- Handler source: `internal/bootstrap/health_check.go`
- TLS detection helpers: `internal/bootstrap/tls_detection.go`
- Per-stack TLS enforcement: `internal/bootstrap/tls_enforcement.go`
- Startup self-probe: `internal/bootstrap/selfprobe.go`
- Metrics registration: `internal/bootstrap/readyz_metrics.go`
