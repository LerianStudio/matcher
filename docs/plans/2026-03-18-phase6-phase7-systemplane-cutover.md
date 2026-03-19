# Phase 6 & Phase 7: Systemplane Configuration Cutover

## Goal

Complete the systemplane configuration cutover for Matcher by: (1) bridging the systemplane supervisor's snapshot back into the ConfigManager's atomic pointer so all existing per-request consumers (rate limiters, health checks, readiness probes) automatically read from the systemplane-backed runtime state without code changes at every call site; (2) wiring the remaining consumer seams that still read from bootstrap-time Config values instead of the live snapshot; (3) verifying that all `bundle-rebuild+worker-reconcile` keys (fetcher.enabled, export_worker.enabled, cleanup_worker.enabled, archival.enabled, archival.storage_*) properly toggle worker lifecycle through the supervisor; (4) renaming all 30+ domain configuration routes from `/v1/config/...` to domain-native paths (`/v1/contexts/...`, `/v1/fee-schedules/...`, etc.); (5) updating all tests, E2E clients, integration tests, chaos tests, and Swagger annotations to the new route paths; (6) removing YAML/Viper runtime authority, the file watcher, and all non-bootstrap env var runtime reads; and (7) cleaning up legacy ConfigManager internals so there is exactly one runtime authority.

## Architecture

**Key Architectural Decisions:**

1. **ConfigManager.UpdateFromSystemplane()** is the bridge method. Rather than replacing `configManager.Get()` at 15+ call sites, we add a single method that the supervisor calls after each successful snapshot publish. This method converts the snapshot into a `*Config`, stores it atomically, and does NOT notify subscribers (the supervisor handles change propagation through reconcilers).

2. **Rate limiters remain unchanged.** They currently call `configManager.Get()` per-request through a closure. Once `UpdateFromSystemplane()` keeps the atomic pointer in sync with the systemplane snapshot, rate limiters automatically read the latest values with zero code changes.

3. **Clean cutover for routes.** Since Matcher is not public, `/v1/config/...` routes are replaced entirely with domain-native paths. No backward-compatibility aliases.

4. **Viper removal is safe** because after seed mode, the ConfigManager no longer uses Viper for reads. The `loadConfigFromYAML` path is only used during initial bootstrap and can be replaced with a thin env-only loader.

5. **ConfigBridgeReconciler runs first** in the reconciler chain so that `configManager.Get()` returns updated values when later reconcilers (HTTP policy, worker) execute.

## Tech Stack

- Go 1.25.6
- Fiber v2 (HTTP framework)
- lib-commons/v4 (Postgres, Redis, RabbitMQ, Logger, OpenTelemetry)
- lib-auth/v2 (JWT, authorization middleware)
- pkg/systemplane (runtime configuration plane — fully built in phases 1-5)
- Viper (TO BE REMOVED from runtime path — kept only in config_yaml.go for initial bootstrap)
- fsnotify (TO BE REMOVED — replaced by systemplane change feed)

## Prerequisites

1. **Phases 1-5 COMPLETE**: `pkg/systemplane/` is fully built (112 Go files), `InitSystemplane()` works, `MatcherBundleFactory.Build()` constructs all infra clients from snapshots, `MountSystemplaneAPI()` serves 9 endpoints, `WorkerReconciler` and `HTTPPolicyReconciler` are wired.
2. **ConfigManager seed mode works**: `SeedStore()` seeds non-default values, `enterSeedMode()` disables file watching and subscriber callbacks.
3. **All existing tests pass**: `make test` and `make lint` both green on the `feat/systemplane` branch.
4. **Feature branch**: All work happens on `feat/systemplane` branch.

---

## Epics and Tasks

### Epic 1: Complete Systemplane-to-ConfigManager Bridge

This epic creates the `UpdateFromSystemplane()` method that keeps the ConfigManager's atomic `*Config` pointer in sync with the systemplane snapshot. This is the single most important change because it enables ALL existing consumers (rate limiters, health checks, readiness probes) to transparently read from the systemplane-backed state.

---

#### Task 1.1: Add UpdateFromSystemplane() to ConfigManager

**File paths:**
- `internal/bootstrap/config_manager.go`

**What to do:** Add a new method `UpdateFromSystemplane(snap domain.Snapshot) error` to ConfigManager. This method:
1. Converts the snapshot into a `*Config` using a new `snapshotToFullConfig()` function
2. Merges bootstrap-only fields from the existing config (they are not in the snapshot)
3. Validates the result
4. Atomically stores the new Config
5. Increments version
6. Does NOT notify subscribers (supervisor handles propagation through reconcilers)
7. Only works when ConfigManager is in seed mode (returns error otherwise)

**Code example:**
```go
// UpdateFromSystemplane converts a systemplane snapshot into a *Config and
// atomically updates the config pointer. This is the bridge that allows all
// existing per-request consumers (rate limiters, health checks) to read
// systemplane-backed values through the existing configManager.Get() path.
//
// This method only works in seed mode. It does NOT notify subscribers because
// the systemplane supervisor handles change propagation through reconcilers.
func (cm *ConfigManager) UpdateFromSystemplane(snap domain.Snapshot) error {
	if !cm.InSeedMode() {
		return fmt.Errorf("update from systemplane: config manager is not in seed mode")
	}

	oldCfg := cm.config.Load()
	if oldCfg == nil {
		return fmt.Errorf("update from systemplane: %w", errConfigNilAtomicLoad)
	}

	newCfg := snapshotToFullConfig(snap, oldCfg)

	if err := newCfg.Validate(); err != nil {
		return fmt.Errorf("update from systemplane: validation: %w", err)
	}

	cm.config.Store(newCfg)
	cm.version.Add(1)
	cm.lastReload.Store(time.Now().UTC())

	return nil
}
```

**Verification command:**
```bash
go build ./internal/bootstrap/... && go vet ./internal/bootstrap/...
```

**Failure recovery:** If build fails, check that `domain.Snapshot` import is present and that `snapshotToFullConfig` exists (Task 1.2).

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 1.2: Implement snapshotToFullConfig() conversion

**File paths:**
- `internal/bootstrap/config_manager_systemplane.go` (new file)

**What to do:** Create a new file that contains `snapshotToFullConfig(snap domain.Snapshot, oldCfg *Config) *Config`. This function reads every runtime-managed key from the snapshot using `snapString/snapInt/snapBool` helpers (already in `systemplane_factory.go`) and populates a `*Config` struct. Bootstrap-only fields are copied from `oldCfg`.

The function must cover ALL runtime-managed key families from `systemplane_keys.go`:
- App (log_level)
- Server (body_limit_bytes, cors_*)
- Tenancy (all 10 keys)
- Postgres (all 17 keys)
- Redis (all 13 keys)
- RabbitMQ (all 8 keys)
- ObjectStorage (all 6 keys)
- Swagger (3 keys)
- RateLimit (7 keys)
- Infrastructure (2 keys)
- Idempotency (3 keys)
- Deduplication (1 key)
- CallbackRateLimit (1 key)
- Webhook (1 key)
- Fetcher (all 9 keys)
- ExportWorker (4 keys)
- CleanupWorker (4 keys)
- Scheduler (1 key)
- Archival (all 11 keys)

**Code example:**
```go
package bootstrap

import "github.com/LerianStudio/matcher/pkg/systemplane/domain"

// snapshotToFullConfig builds a complete *Config from a systemplane snapshot,
// merging runtime-managed values from the snapshot with bootstrap-only values
// from the old config. This is the inverse of the seed operation — it converts
// systemplane state back into the Config struct so that all existing consumers
// work transparently.
func snapshotToFullConfig(snap domain.Snapshot, oldCfg *Config) *Config {
	cfg := defaultConfig()

	// --- Bootstrap-only fields (from old config, never in snapshot) ---
	cfg.App.EnvName = oldCfg.App.EnvName
	cfg.Server.Address = oldCfg.Server.Address
	cfg.Server.TLSCertFile = oldCfg.Server.TLSCertFile
	cfg.Server.TLSKeyFile = oldCfg.Server.TLSKeyFile
	cfg.Server.TLSTerminatedUpstream = oldCfg.Server.TLSTerminatedUpstream
	cfg.Server.TrustedProxies = oldCfg.Server.TrustedProxies
	cfg.Auth = oldCfg.Auth
	cfg.Telemetry = oldCfg.Telemetry
	cfg.Logger = oldCfg.Logger
	cfg.ShutdownGracePeriod = oldCfg.ShutdownGracePeriod

	// --- Runtime-managed fields (from snapshot) ---
	// App
	cfg.App.LogLevel = snapString(snap, "app.log_level", defaultLogLevel)

	// Server (HTTP policy)
	cfg.Server.BodyLimitBytes = snapInt(snap, "server.body_limit_bytes", defaultKeyBodyLimitBytes)
	cfg.Server.CORSAllowedOrigins = snapString(snap, "server.cors_allowed_origins", defaultCORSAllowedOrigins)
	cfg.Server.CORSAllowedMethods = snapString(snap, "server.cors_allowed_methods", defaultCORSAllowedMethods)
	cfg.Server.CORSAllowedHeaders = snapString(snap, "server.cors_allowed_headers", defaultCORSAllowedHeaders)

	// Tenancy
	cfg.Tenancy.DefaultTenantID = snapString(snap, "tenancy.default_tenant_id", defaultTenantID)
	cfg.Tenancy.DefaultTenantSlug = snapString(snap, "tenancy.default_tenant_slug", defaultTenantSlug)
	cfg.Tenancy.MultiTenantEnabled = snapBool(snap, "tenancy.multi_tenant_enabled", false)
	cfg.Tenancy.MultiTenantURL = snapString(snap, "tenancy.multi_tenant_url", "")
	// ... (all remaining tenancy keys)

	// Postgres
	cfg.Postgres.PrimaryHost = snapString(snap, "postgres.primary_host", defaultPostgresHost)
	cfg.Postgres.PrimaryPort = snapInt(snap, "postgres.primary_port", defaultPostgresPort)
	// ... (all remaining postgres keys)

	// Redis
	cfg.Redis.Host = snapString(snap, "redis.host", defaultRedisHost)
	// ... (all remaining redis keys)

	// RabbitMQ
	cfg.RabbitMQ.URI = snapString(snap, "rabbitmq.uri", "")
	// ... (all remaining rabbitmq keys)

	// ObjectStorage
	cfg.ObjectStorage.Endpoint = snapString(snap, "object_storage.endpoint", "")
	// ... (all remaining object_storage keys)

	// Swagger
	cfg.Swagger.Enabled = snapBool(snap, "swagger.enabled", defaultSwaggerEnabled)
	cfg.Swagger.Host = snapString(snap, "swagger.host", "")
	cfg.Swagger.Schemes = snapString(snap, "swagger.schemes", "")

	// Rate Limiting
	cfg.RateLimit.Enabled = snapBool(snap, "rate_limit.enabled", defaultRateLimitEnabled)
	cfg.RateLimit.Max = snapInt(snap, "rate_limit.max", defaultRateLimitMax)
	cfg.RateLimit.ExpirySec = snapInt(snap, "rate_limit.expiry_sec", defaultRateLimitExpirySec)
	cfg.RateLimit.ExportMax = snapInt(snap, "rate_limit.export_max", defaultRateLimitExportMax)
	cfg.RateLimit.ExportExpirySec = snapInt(snap, "rate_limit.export_expiry_sec", defaultRateLimitExportExpiry)
	cfg.RateLimit.DispatchMax = snapInt(snap, "rate_limit.dispatch_max", defaultRateLimitDispatchMax)
	cfg.RateLimit.DispatchExpirySec = snapInt(snap, "rate_limit.dispatch_expiry_sec", defaultRateLimitDispatchExp)

	// Infrastructure
	cfg.Infra.ConnectTimeoutSec = snapInt(snap, "infrastructure.connect_timeout_sec", defaultInfraConnectTimeout)
	cfg.Infra.HealthCheckTimeoutSec = snapInt(snap, "infrastructure.health_check_timeout_sec", defaultInfraHealthCheckTimeout)

	// Idempotency
	cfg.Idempotency.RetryWindowSec = snapInt(snap, "idempotency.retry_window_sec", defaultIdempotencyRetryWindow)
	cfg.Idempotency.SuccessTTLHours = snapInt(snap, "idempotency.success_ttl_hours", defaultIdempotencySuccessTTL)
	cfg.Idempotency.HMACSecret = snapString(snap, "idempotency.hmac_secret", "")

	// Deduplication
	cfg.Dedupe.TTLSec = snapInt(snap, "deduplication.ttl_sec", defaultDedupeTTLSec)

	// Callback Rate Limit
	cfg.CallbackRateLimit.PerMinute = snapInt(snap, "callback_rate_limit.per_minute", defaultCallbackPerMinute)

	// Webhook
	cfg.Webhook.TimeoutSec = snapInt(snap, "webhook.timeout_sec", defaultWebhookTimeout)

	// Fetcher
	cfg.Fetcher.Enabled = snapBool(snap, "fetcher.enabled", false)
	cfg.Fetcher.URL = snapString(snap, "fetcher.url", "")
	cfg.Fetcher.AllowPrivateIPs = snapBool(snap, "fetcher.allow_private_ips", false)
	cfg.Fetcher.HealthTimeoutSec = snapInt(snap, "fetcher.health_timeout_sec", defaultFetcherHealthTimeout)
	cfg.Fetcher.RequestTimeoutSec = snapInt(snap, "fetcher.request_timeout_sec", defaultFetcherRequestTimeout)
	cfg.Fetcher.DiscoveryIntervalSec = snapInt(snap, "fetcher.discovery_interval_sec", defaultFetcherDiscoveryInt)
	cfg.Fetcher.SchemaCacheTTLSec = snapInt(snap, "fetcher.schema_cache_ttl_sec", defaultFetcherSchemaCacheTTL)
	cfg.Fetcher.ExtractionPollSec = snapInt(snap, "fetcher.extraction_poll_sec", defaultFetcherExtractionPoll)
	cfg.Fetcher.ExtractionTimeoutSec = snapInt(snap, "fetcher.extraction_timeout_sec", defaultFetcherExtractionTimeout)

	// Export Worker
	cfg.ExportWorker.Enabled = snapBool(snap, "export_worker.enabled", defaultExportWorkerEnabled)
	cfg.ExportWorker.PollIntervalSec = snapInt(snap, "export_worker.poll_interval_sec", defaultExportPollInterval)
	cfg.ExportWorker.PageSize = snapInt(snap, "export_worker.page_size", defaultExportPageSize)
	cfg.ExportWorker.PresignExpirySec = snapInt(snap, "export_worker.presign_expiry_sec", defaultExportPresignExpiry)

	// Cleanup Worker
	cfg.CleanupWorker.Enabled = snapBool(snap, "cleanup_worker.enabled", defaultCleanupWorkerEnabled)
	cfg.CleanupWorker.IntervalSec = snapInt(snap, "cleanup_worker.interval_sec", defaultCleanupInterval)
	cfg.CleanupWorker.BatchSize = snapInt(snap, "cleanup_worker.batch_size", defaultCleanupBatchSize)
	cfg.CleanupWorker.GracePeriodSec = snapInt(snap, "cleanup_worker.grace_period_sec", defaultCleanupGracePeriod)

	// Scheduler
	cfg.Scheduler.IntervalSec = snapInt(snap, "scheduler.interval_sec", defaultSchedulerInterval)

	// Archival
	cfg.Archival.Enabled = snapBool(snap, "archival.enabled", defaultArchivalEnabled)
	cfg.Archival.IntervalHours = snapInt(snap, "archival.interval_hours", defaultArchivalIntervalHours)
	cfg.Archival.HotRetentionDays = snapInt(snap, "archival.hot_retention_days", defaultArchivalHotRetention)
	cfg.Archival.WarmRetentionMonths = snapInt(snap, "archival.warm_retention_months", defaultArchivalWarmRetention)
	cfg.Archival.ColdRetentionMonths = snapInt(snap, "archival.cold_retention_months", defaultArchivalColdRetention)
	cfg.Archival.BatchSize = snapInt(snap, "archival.batch_size", defaultArchivalBatchSize)
	cfg.Archival.PartitionLookahead = snapInt(snap, "archival.partition_lookahead", defaultArchivalPartitionLookahead)
	cfg.Archival.StorageBucket = snapString(snap, "archival.storage_bucket", defaultArchivalStorageBucket)
	cfg.Archival.StoragePrefix = snapString(snap, "archival.storage_prefix", defaultArchivalStoragePrefix)
	cfg.Archival.StorageClass = snapString(snap, "archival.storage_class", defaultArchivalStorageClass)
	cfg.Archival.PresignExpirySec = snapInt(snap, "archival.presign_expiry_sec", defaultArchivalPresignExpiry)

	return cfg
}
```

**Verification command:**
```bash
go build ./internal/bootstrap/... && go vet ./internal/bootstrap/...
```

**Failure recovery:** Ensure all `snapString/snapInt/snapBool` functions are accessible (they live in `systemplane_factory.go` in the same package). Ensure all `default*` constants match those in `systemplane_keys.go`.

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 1.3: Create ConfigBridgeReconciler

**File paths:**
- `internal/bootstrap/systemplane_reconciler_config.go` (new file)

**What to do:** Create a new `ConfigBridgeReconciler` that implements `ports.BundleReconciler`. Its `Reconcile()` method calls `configManager.UpdateFromSystemplane(snap)` after each successful bundle swap. This reconciler must run FIRST in the chain.

**Code example:**
```go
package bootstrap

import (
	"context"
	"errors"
	"fmt"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

var _ ports.BundleReconciler = (*ConfigBridgeReconciler)(nil)

var errConfigBridgeManagerRequired = errors.New("config bridge reconciler: config manager is required")

// ConfigBridgeReconciler keeps the ConfigManager's atomic *Config pointer in
// sync with the systemplane snapshot. This ensures all existing per-request
// consumers (rate limiters, health checks, readiness probes) transparently
// read systemplane-backed values through configManager.Get().
type ConfigBridgeReconciler struct {
	configManager *ConfigManager
}

func NewConfigBridgeReconciler(cm *ConfigManager) (*ConfigBridgeReconciler, error) {
	if cm == nil {
		return nil, errConfigBridgeManagerRequired
	}

	return &ConfigBridgeReconciler{configManager: cm}, nil
}

func (r *ConfigBridgeReconciler) Name() string {
	return "config-bridge-reconciler"
}

func (r *ConfigBridgeReconciler) Reconcile(_ context.Context, _, _ domain.RuntimeBundle, snap domain.Snapshot) error {
	if err := r.configManager.UpdateFromSystemplane(snap); err != nil {
		return fmt.Errorf("config bridge reconciler: %w", err)
	}

	return nil
}
```

**Verification command:**
```bash
go build ./internal/bootstrap/... && go vet ./internal/bootstrap/...
```

**Failure recovery:** Ensure `ConfigManager.UpdateFromSystemplane` from Task 1.1 is implemented.

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 1.4: Register ConfigBridgeReconciler in buildReconcilers()

**File paths:**
- `internal/bootstrap/systemplane_init.go`

**What to do:** Update `buildReconcilers()` to accept a `*ConfigManager` parameter and create+append a `ConfigBridgeReconciler` as the FIRST reconciler. Update the `InitSystemplane()` call chain to pass the ConfigManager.

**Code example:**
```go
func buildReconcilers(configManager *ConfigManager, workerManager *WorkerManager) ([]ports.BundleReconciler, error) {
	var reconcilers []ports.BundleReconciler

	// Config bridge runs first so configManager.Get() is up-to-date
	// for any downstream reconciler that reads it.
	if configManager != nil {
		configBridge, err := NewConfigBridgeReconciler(configManager)
		if err != nil {
			return nil, fmt.Errorf("create config bridge reconciler: %w", err)
		}

		reconcilers = append(reconcilers, configBridge)
	}

	reconcilers = append(reconcilers, NewHTTPPolicyReconciler())

	if workerManager != nil {
		workerReconciler, err := NewWorkerReconciler(workerManager)
		if err != nil {
			return nil, fmt.Errorf("create worker reconciler: %w", err)
		}

		reconcilers = append(reconcilers, workerReconciler)
	}

	return reconcilers, nil
}
```

**Verification command:**
```bash
go build ./internal/bootstrap/... && go vet ./internal/bootstrap/...
```

**Failure recovery:** Update InitSystemplane() signature and caller in init.go to pass configManager.

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 1.5: Write unit tests for UpdateFromSystemplane and ConfigBridgeReconciler

**File paths:**
- `internal/bootstrap/config_manager_systemplane_test.go` (new file)
- `internal/bootstrap/systemplane_reconciler_config_test.go` (new file)

**What to do:** Write unit tests covering:
- `UpdateFromSystemplane` rejects calls when not in seed mode
- `UpdateFromSystemplane` atomically stores new Config with values from snapshot
- `UpdateFromSystemplane` preserves bootstrap-only fields from old config
- `UpdateFromSystemplane` increments version
- `ConfigBridgeReconciler.Reconcile` calls UpdateFromSystemplane
- `ConfigBridgeReconciler.Name()` returns expected name
- `NewConfigBridgeReconciler(nil)` returns error

**Verification command:**
```bash
go test -tags unit -v -run "TestUpdateFromSystemplane|TestConfigBridgeReconciler" ./internal/bootstrap/...
```

**Failure recovery:** If snapshot mock is hard to build, use `domain.Snapshot{Configs: map[string]domain.EffectiveValue{...}}` with known test values.

**Recommended agent:** `ring:qa-analyst`

---

> **Code Review Checkpoint 1:** After Tasks 1.1-1.5, verify:
> - `go build ./...` succeeds
> - `go vet ./...` clean
> - `go test -tags unit ./internal/bootstrap/...` passes
> - Rate limiters work through configManager.Get() → systemplane-backed values

---

### Epic 2: Wire Remaining Consumer Seams

These are the consumer seams identified in the master plan that currently read from stale bootstrap Config instead of the live snapshot. With the ConfigBridgeReconciler in place (Epic 1), most of these are auto-fixed.

---

#### Task 2.1: Verify rate limiter seam (no code change — auto-fixed by Epic 1)

**File paths:**
- `internal/bootstrap/rate_limiter_dynamic.go`
- `internal/bootstrap/init.go`

**What to do:** Verify (read-only) that `NewDynamicRateLimiter`, `NewDynamicExportRateLimiter`, and `NewDynamicDispatchRateLimiter` all use `configGetter func() *Config` which is wired to `configManager.Get()`. Once Epic 1 is complete, these automatically read systemplane-backed values. No code change needed — just add a brief comment documenting the bridge.

**Verification command:**
```bash
grep -n "configManager.Get" internal/bootstrap/init.go | head -5
```

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 2.2: Verify callback_rate_limit.per_minute seam (auto-fixed by Epic 1)

**File paths:**
- `internal/exception/adapters/http/connectors/` (check where CallbackRateLimit.PerMinute is consumed)

**What to do:** Grep for `CallbackRateLimit` usage. If it reads from a captured Config at construction time, it will get the correct value after the next bundle rebuild (since ConfigBridgeReconciler updates the pointer). If it reads from `configManager.Get()` per-invocation, it is already dynamic. Document which case applies and whether a runtime change requires bundle rebuild.

**Verification command:**
```bash
grep -rn "CallbackRateLimit\|PerMinute" internal/exception/
```

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 2.3: Verify webhook.timeout_sec seam

**File paths:**
- `internal/exception/adapters/http/connectors/`

**What to do:** Check how webhook timeout is consumed. If captured at construction time, it gets the updated value after bundle rebuild. If per-request, it's already dynamic via `configManager.Get()`. Document which case applies.

**Verification command:**
```bash
grep -rn "TimeoutSec\|webhook.*timeout" internal/exception/
```

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 2.4: Verify deduplication.ttl_sec seam

**File paths:**
- `internal/ingestion/adapters/redis/`

**What to do:** Check how the dedup TTL is consumed. If captured at construction time in the Redis adapter, it gets the updated value after bundle rebuild (adapter is reconstructed by MatcherBundleFactory.Build()). If it needs to be dynamic within a single bundle lifecycle, refactor to accept a TTL getter.

**Verification command:**
```bash
grep -rn "TTLSec\|dedup.*ttl\|DedupTTL" internal/ingestion/
```

**Recommended agent:** `ring:backend-engineer-golang`

---

> **Code Review Checkpoint 2:** After Tasks 2.1-2.4, verify:
> - All consumer seams documented or fixed
> - `go build ./...` succeeds
> - `go test -tags unit ./...` passes

---

### Epic 3: Verify Bundle-Rebuild+Worker-Reconcile Keys

These keys require both bundle reconstruction AND worker lifecycle reconciliation. The supervisor mechanics are already built; this epic verifies end-to-end correctness.

---

#### Task 3.1: Add integration test for fetcher.enabled toggle

**File paths:**
- `internal/bootstrap/systemplane_reconciler_worker_test.go`

**What to do:** Write a test that creates a WorkerReconciler with a mock WorkerManager, builds two snapshots (fetcher.enabled=false then fetcher.enabled=true), and verifies that `ApplyConfig` is called with the correct Config transitions. Verify that `snapshotToWorkerConfig` produces correct Fetcher config with all 9 fetcher keys.

**Verification command:**
```bash
go test -tags unit -v -run "TestWorkerReconciler_FetcherToggle" ./internal/bootstrap/...
```

**Recommended agent:** `ring:qa-analyst`

---

#### Task 3.2: Verify export_worker.enabled and cleanup_worker.enabled reconciliation

**File paths:**
- `internal/bootstrap/systemplane_reconciler_worker_test.go`

**What to do:** Write tests verifying that `snapshotToWorkerConfig` correctly populates ExportWorker and CleanupWorker config from snapshots, and that the WorkerReconciler properly delegates to WorkerManager.ApplyConfig.

**Verification command:**
```bash
go test -tags unit -v -run "TestWorkerReconciler_ExportCleanup" ./internal/bootstrap/...
```

**Recommended agent:** `ring:qa-analyst`

---

#### Task 3.3: Verify archival.enabled and archival.storage_* reconciliation

**File paths:**
- `internal/bootstrap/systemplane_reconciler_worker_test.go`

**What to do:** Write tests verifying that archival config (enabled, storage_bucket, storage_prefix, storage_class) is correctly extracted from the snapshot and that toggling archival.enabled triggers the correct WorkerManager reconciliation.

**Verification command:**
```bash
go test -tags unit -v -run "TestWorkerReconciler_Archival" ./internal/bootstrap/...
```

**Recommended agent:** `ring:qa-analyst`

---

> **Code Review Checkpoint 3:** After Tasks 3.1-3.3, verify:
> - All bundle-rebuild+worker-reconcile keys have test coverage
> - `go test -tags unit ./internal/bootstrap/...` passes

---

### Epic 4: Domain Route Rename

Rename all 30+ business configuration routes from `/v1/config/...` to domain-native paths. This is a mechanical find-and-replace across a known set of files. Since Matcher is not public, no backward-compatibility aliases are needed.

**Route Rename Map:**
| Old Path | New Path |
|----------|----------|
| `/v1/config/contexts` | `/v1/contexts` |
| `/v1/config/contexts/:contextId` | `/v1/contexts/:contextId` |
| `/v1/config/contexts/:contextId/clone` | `/v1/contexts/:contextId/clone` |
| `/v1/config/contexts/:contextId/sources` | `/v1/contexts/:contextId/sources` |
| `/v1/config/contexts/:contextId/sources/:sourceId` | `/v1/contexts/:contextId/sources/:sourceId` |
| `/v1/config/contexts/:contextId/sources/:sourceId/field-maps` | `/v1/contexts/:contextId/sources/:sourceId/field-maps` |
| `/v1/config/field-maps/:fieldMapId` | `/v1/field-maps/:fieldMapId` |
| `/v1/config/contexts/:contextId/rules` | `/v1/contexts/:contextId/rules` |
| `/v1/config/contexts/:contextId/rules/:ruleId` | `/v1/contexts/:contextId/rules/:ruleId` |
| `/v1/config/contexts/:contextId/rules/reorder` | `/v1/contexts/:contextId/rules/reorder` |
| `/v1/config/fee-schedules` | `/v1/fee-schedules` |
| `/v1/config/fee-schedules/:scheduleId` | `/v1/fee-schedules/:scheduleId` |
| `/v1/config/fee-schedules/:scheduleId/simulate` | `/v1/fee-schedules/:scheduleId/simulate` |
| `/v1/config/contexts/:contextId/schedules` | `/v1/contexts/:contextId/schedules` |
| `/v1/config/contexts/:contextId/schedules/:scheduleId` | `/v1/contexts/:contextId/schedules/:scheduleId` |

---

#### Task 4.1: Update route registration in routes.go

**File paths:**
- `internal/configuration/adapters/http/routes.go`

**What to do:** Replace every `/v1/config/` prefix with the domain-native path. This is a string replacement:
- `"/v1/config/contexts"` → `"/v1/contexts"`
- `"/v1/config/fee-schedules"` → `"/v1/fee-schedules"`
- `"/v1/config/field-maps"` → `"/v1/field-maps"`

**Verification command:**
```bash
grep -c "/v1/config/" internal/configuration/adapters/http/routes.go
# Expected: 0
```

**Failure recovery:** If count > 0, missed a route. Check the grep output for remaining occurrences.

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 4.2: Update Swagger annotations in handlers.go

**File paths:**
- `internal/configuration/adapters/http/handlers.go`

**What to do:** Replace all `@Router /v1/config/...` annotations with the new domain paths. There are ~18 `@Router` annotations in this file (contexts CRUD, sources CRUD, field-maps, rules).

**Verification command:**
```bash
grep -c "@Router.*/v1/config/" internal/configuration/adapters/http/handlers.go
# Expected: 0
```

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 4.3: Update Swagger annotations in handlers_fee_schedule.go

**File paths:**
- `internal/configuration/adapters/http/handlers_fee_schedule.go`

**What to do:** Replace all 6 `@Router /v1/config/fee-schedules...` annotations with `@Router /v1/fee-schedules...`.

**Verification command:**
```bash
grep -c "@Router.*/v1/config/" internal/configuration/adapters/http/handlers_fee_schedule.go
# Expected: 0
```

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 4.4: Update Swagger annotations in handlers_schedule.go

**File paths:**
- `internal/configuration/adapters/http/handlers_schedule.go`

**What to do:** Replace all 5 `@Router /v1/config/contexts/:contextId/schedules...` annotations with `@Router /v1/contexts/:contextId/schedules...`.

**Verification command:**
```bash
grep -c "@Router.*/v1/config/" internal/configuration/adapters/http/handlers_schedule.go
# Expected: 0
```

**Recommended agent:** `ring:backend-engineer-golang`

---

> **Code Review Checkpoint 4:** After Tasks 4.1-4.4, verify:
> - `grep -r "@Router.*/v1/config/" internal/configuration/adapters/http/` returns 0 matches
> - `grep "/v1/config/" internal/configuration/adapters/http/routes.go` returns 0 matches
> - `go build ./internal/configuration/...` succeeds

---

### Epic 5: Update All Tests and Clients

Update all test files, E2E clients, integration tests, and chaos tests to use the new route paths.

---

#### Task 5.1: Update E2E configuration client

**File paths:**
- `tests/e2e/client/configuration.go`

**What to do:** Replace all ~21 occurrences of `/v1/config/` with domain-native paths:
- `"/v1/config/contexts"` → `"/v1/contexts"`
- `"/v1/config/contexts/%s/sources"` → `"/v1/contexts/%s/sources"`
- `"/v1/config/contexts/%s/sources/%s/field-maps"` → `"/v1/contexts/%s/sources/%s/field-maps"`
- `"/v1/config/field-maps/%s"` → `"/v1/field-maps/%s"`
- `"/v1/config/contexts/%s/rules"` → `"/v1/contexts/%s/rules"`
- `"/v1/config/contexts/%s/clone"` → `"/v1/contexts/%s/clone"`
- `"/v1/config/contexts/%s/schedules"` → `"/v1/contexts/%s/schedules"`

**Verification command:**
```bash
grep -c "/v1/config/" tests/e2e/client/configuration.go
# Expected: 0
```

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 5.2: Update E2E fee schedule client

**File paths:**
- `tests/e2e/client/fee_schedule.go`

**What to do:** Replace all ~7 occurrences of `/v1/config/fee-schedules` with `/v1/fee-schedules`.

**Verification command:**
```bash
grep -c "/v1/config/" tests/e2e/client/fee_schedule.go
# Expected: 0
```

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 5.3: Update E2E client unit tests

**File paths:**
- `tests/e2e/client/configuration_test.go`

**What to do:** Replace all ~11 `assert.Equal` path assertions from `/v1/config/...` to domain-native paths.

**Verification command:**
```bash
grep -c "/v1/config/" tests/e2e/client/configuration_test.go
# Expected: 0
```

**Recommended agent:** `ring:qa-analyst`

---

#### Task 5.4: Update handler unit tests

**File paths:**
- `internal/configuration/adapters/http/handlers_test.go`
- `internal/configuration/adapters/http/handlers_fee_schedule_test.go`
- `internal/configuration/adapters/http/handlers_schedule_test.go`
- `internal/configuration/adapters/http/handlers_coverage_test.go`
- `internal/configuration/adapters/http/handlers_auth_test.go`

**What to do:** In each test file, replace all route path strings from `/v1/config/...` to the new domain paths. The pattern is the same mechanical replacement as routes.go.

**Verification command:**
```bash
grep -rc "/v1/config/" internal/configuration/adapters/http/*_test.go
# Expected: 0 for every file
go test -tags unit ./internal/configuration/adapters/http/...
```

**Recommended agent:** `ring:qa-analyst`

---

#### Task 5.5: Update integration tests

**File paths:**
- `tests/integration/configuration_flow_test.go`

**What to do:** Replace all ~14 occurrences of `/v1/config/` in URL format strings.

**Verification command:**
```bash
grep -c "/v1/config/" tests/integration/configuration_flow_test.go
# Expected: 0
```

**Recommended agent:** `ring:qa-analyst`

---

#### Task 5.6: Update chaos tests

**File paths:**
- `tests/chaos/server_harness.go`
- `tests/chaos/idempotency_chaos_test.go`

**What to do:** Replace all ~6 occurrences of `/v1/config/` in URL strings.

**Verification command:**
```bash
grep -c "/v1/config/" tests/chaos/server_harness.go tests/chaos/idempotency_chaos_test.go
# Expected: 0 for both
```

**Recommended agent:** `ring:qa-analyst`

---

#### Task 5.7: Update configuration README

**File paths:**
- `internal/configuration/README.md`

**What to do:** Replace all `/v1/config/` route references in the API endpoint table with the new domain-native paths.

**Verification command:**
```bash
grep -c "/v1/config/" internal/configuration/README.md
# Expected: 0
```

**Recommended agent:** `general-purpose`

---

> **Code Review Checkpoint 5:** After Tasks 5.1-5.7, verify:
> - `grep -rn "/v1/config/" tests/ internal/configuration/ --include="*.go" | grep -v plan` returns 0 matches
> - `go build -tags e2e ./tests/e2e/...` succeeds
> - `go build -tags integration ./tests/integration/...` succeeds
> - `go build -tags chaos ./tests/chaos/...` succeeds
> - `go test -tags unit ./internal/configuration/adapters/http/...` passes

---

### Epic 6: Swagger/OpenAPI Regeneration

---

#### Task 6.1: Regenerate Swagger documentation

**File paths:**
- `docs/swagger/swagger.yaml`
- `docs/swagger/swagger.json`
- `docs/swagger/docs.go`

**What to do:** Run `make generate-docs` to regenerate the Swagger documentation from the updated `@Router` annotations. Verify the output contains `/v1/contexts/`, `/v1/fee-schedules/`, `/v1/field-maps/` and does NOT contain `/v1/config/`.

**Verification command:**
```bash
make generate-docs
grep -c "/v1/config/" docs/swagger/swagger.yaml
# Expected: 0
grep -c "/v1/contexts/" docs/swagger/swagger.yaml
# Expected: > 0
grep -c "/v1/fee-schedules/" docs/swagger/swagger.yaml
# Expected: > 0
```

**Failure recovery:** If `make generate-docs` fails, install swag: `go install github.com/swaggo/swag/cmd/swag@latest`.

**Recommended agent:** `ring:backend-engineer-golang`

---

> **Code Review Checkpoint 6:** After Task 6.1, verify:
> - `docs/swagger/swagger.yaml` has no `/v1/config/` references
> - `docs/swagger/swagger.json` has no `/v1/config/` references
> - `docs/swagger/docs.go` has no `/v1/config/` references

---

### Epic 7: Remove YAML/Viper Runtime Authority (Phase 7)

This epic removes Viper, fsnotify, and the file watcher from the runtime path. After this, the ConfigManager is a thin atomic pointer holder that gets its values from the systemplane supervisor via `UpdateFromSystemplane()`.

**Important distinction:** Viper/YAML is kept for the INITIAL bootstrap load (first boot reads config file once). It is removed from the RUNTIME path (ConfigManager no longer reloads from file).

---

#### Task 7.1: Remove Viper from ConfigManager runtime path

**File paths:**
- `internal/bootstrap/config_manager.go`
- `internal/bootstrap/config_manager_reload.go`

**What to do:**
1. Remove the `viper` field from `ConfigManager` struct (if present)
2. Remove any Viper initialization in `NewConfigManager`
3. Simplify `reload()` — since ConfigManager is always in seed mode after systemplane init, the reload path already returns early with "superseded by systemplane". Remove the Viper-dependent reload logic (unreachable after seed mode). Keep the method signature for interface compatibility.
4. Remove `rollbackViperToConfigLocked()` if it exists
5. Remove Viper imports from config_manager*.go files

**Verification command:**
```bash
grep -c "viper\|Viper" internal/bootstrap/config_manager.go internal/bootstrap/config_manager_reload.go
# Expected: 0 (excluding comments explaining removal)
go build ./internal/bootstrap/...
```

**Failure recovery:** If tests fail because they test Viper reload behavior, update them to test seed-mode behavior instead.

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 7.2: Remove file watcher

**File paths:**
- `internal/bootstrap/config_manager_watch.go` (DELETE entire file)
- `internal/bootstrap/config_manager_watch_test.go` (DELETE entire file)

**What to do:** Delete the file watcher implementation and its tests. The systemplane change feed (LISTEN/NOTIFY or change streams) replaces file watching. Also remove `StartWatcher()` calls from `init.go` and any test setup.

**Verification command:**
```bash
ls internal/bootstrap/config_manager_watch.go 2>&1
# Expected: No such file or directory
grep -rn "StartWatcher\|fsnotify" internal/bootstrap/ --include="*.go" | grep -v _test.go
# Expected: no references in production code
go build ./internal/bootstrap/...
```

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 7.3: Isolate Viper to initial bootstrap only

**File paths:**
- `internal/bootstrap/config_yaml.go`
- `internal/bootstrap/config_loading.go`

**What to do:** The `loadConfigFromYAML()`, `resolveConfigFilePath()`, and `bindDefaults()` functions are needed for the initial bootstrap load (before systemplane is initialized). Keep them in `config_yaml.go` but ensure they are ONLY called once during startup — never from ConfigManager or any runtime path. Add a comment documenting this constraint.

**Verification command:**
```bash
# Viper should ONLY appear in config_yaml.go and config_loading.go
grep -l "viper\|Viper" internal/bootstrap/*.go | sort
# Expected: config_yaml.go and config_loading.go ONLY
go build ./internal/bootstrap/...
```

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 7.4: Update tests for Viper removal from ConfigManager

**File paths:**
- `internal/bootstrap/config_manager_test.go`
- `internal/bootstrap/config_manager_reload_test.go`

**What to do:** Remove or update tests that test Viper-based reload behavior. Replace with tests that verify seed-mode behavior (reload returns "superseded by systemplane"). Ensure all ConfigManager tests work without Viper.

**Verification command:**
```bash
go test -tags unit ./internal/bootstrap/... -count=1
```

**Recommended agent:** `ring:qa-analyst`

---

> **Code Review Checkpoint 7:** After Tasks 7.1-7.4, verify:
> - Viper is NOT imported in any `config_manager*.go` file
> - `config_manager_watch.go` is deleted
> - fsnotify is NOT imported in any `config_manager*.go` file
> - Viper only appears in `config_yaml.go` and `config_loading.go`
> - `go build ./...` succeeds
> - `go test -tags unit ./internal/bootstrap/...` passes

---

### Epic 8: Shrink Env to Bootstrap-Only (Phase 7)

---

#### Task 8.1: Remove mutableConfigKeys whitelist

**File paths:**
- `internal/bootstrap/config_manager.go`

**What to do:** The `mutableConfigKeys` map was used to mark which keys are safe to change via YAML hot-reload. Since the systemplane registry now owns mutability metadata (`MutableAtRuntime` field in `KeyDef`), this map is redundant. Remove it and any references to it.

**Verification command:**
```bash
grep -n "mutableConfigKeys" internal/bootstrap/
# Expected: 0 matches
go build ./internal/bootstrap/...
```

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 8.2: Remove blockedFileReloadKeys and rejectImmutableReloadChanges

**File paths:**
- `internal/bootstrap/config_manager_reload.go`

**What to do:** The `blockedFileReloadKeys` map and `rejectImmutableReloadChanges()` function are no longer needed since YAML file reload is removed. Delete them and any references.

**Verification command:**
```bash
grep -n "blockedFileReloadKeys\|rejectImmutableReloadChanges" internal/bootstrap/
# Expected: 0 matches
go build ./internal/bootstrap/...
```

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 8.3: Remove preserveStartupOnlyRuntimeSettings

**File paths:**
- `internal/bootstrap/config_manager_reload.go`

**What to do:** The `preserveStartupOnlyRuntimeSettings()` function prevented YAML reloads from changing startup-frozen values. Since Viper reload is removed, this function is dead code. Delete it and any references.

**Verification command:**
```bash
grep -n "preserveStartupOnlyRuntimeSettings" internal/bootstrap/
# Expected: 0 matches
go build ./internal/bootstrap/...
```

**Recommended agent:** `ring:backend-engineer-golang`

---

> **Code Review Checkpoint 8:** After Tasks 8.1-8.3, verify:
> - No references to mutableConfigKeys, blockedFileReloadKeys, or preserveStartupOnlyRuntimeSettings
> - `go build ./...` succeeds
> - `go test -tags unit ./internal/bootstrap/...` passes

---

### Epic 9: Cleanup and Hardening (Phase 7)

---

#### Task 9.1: Remove config_schema.go and config_audit.go if superseded

**File paths:**
- `internal/bootstrap/config_schema.go`
- `internal/bootstrap/config_schema_metadata.go`
- `internal/bootstrap/config_audit.go`

**What to do:** Check if these files are still referenced. The systemplane owns schema output (`/v1/system/configs/schema`) and history (`/v1/system/configs/history`). If these files were only used by the old config API (already replaced by systemplane mount), delete them. If they're used elsewhere, keep them.

**Verification command:**
```bash
# Check for references in production code (excluding tests and self-references)
grep -rn "buildConfigSchema\|configSchema\|ConfigAudit\|SchemaMetadata" internal/bootstrap/*.go | grep -v _test.go | grep -v config_schema | grep -v config_audit
# If 0 matches outside these files, safe to delete
```

**Failure recovery:** If references exist, keep the files and document why they're still needed.

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 9.2: Remove config_redact.go if superseded

**File paths:**
- `internal/bootstrap/config_redact.go`

**What to do:** The systemplane registry has `RedactPolicy` on every `KeyDef`. Check if `config_redact.go` is still used. If only by the old config API, delete it. If used by logging or other runtime code, keep it.

**Verification command:**
```bash
grep -rn "Redact\|redactConfig" internal/bootstrap/*.go | grep -v _test.go | grep -v config_redact
```

**Recommended agent:** `ring:backend-engineer-golang`

---

#### Task 9.3: Full codebase grep for stale /v1/config/ references

**File paths:** (all Go files)

**What to do:** Run a comprehensive grep to find any remaining `/v1/config/` references across the entire codebase. Fix any that were missed in previous epics.

**Verification command:**
```bash
grep -rn "/v1/config/" --include="*.go" --include="*.yaml" --include="*.json" --include="*.md" . | grep -v "docs/plans/" | grep -v "node_modules" | grep -v ".git/"
# Expected: 0 matches (excluding plan documents)
```

**Failure recovery:** Fix any remaining references found.

**Recommended agent:** `general-purpose`

---

#### Task 9.4: Update CLAUDE.md to reflect new architecture

**File paths:**
- `CLAUDE.md`

**What to do:**
1. Update the "Architecture Quick Start" bounded context descriptions to note that configuration routes are now at `/v1/contexts/...`, `/v1/fee-schedules/...`, etc.
2. Update the "Configuration" section to document systemplane as the runtime authority
3. Remove references to YAML/Viper as runtime config sources
4. Update the "Adding a New API Endpoint" section with new route paths
5. Add a note about `pkg/systemplane` as the runtime configuration library
6. Update env var documentation to distinguish bootstrap-only from runtime-managed

**Verification command:**
```bash
grep -c "/v1/config/" CLAUDE.md
# Expected: 0 (or only in historical/migration context)
```

**Recommended agent:** `general-purpose`

---

#### Task 9.5: Update docs/pre-dev references

**File paths:**
- `docs/pre-dev/matcher/api-design.md`
- `docs/pre-dev/matcher/tasks.md`

**What to do:** If these files reference `/v1/config/...` routes, update them to reflect the new domain-native paths. These docs may be historical but should reflect the current state.

**Verification command:**
```bash
grep -c "/v1/config/" docs/pre-dev/matcher/api-design.md docs/pre-dev/matcher/tasks.md
```

**Recommended agent:** `general-purpose`

---

#### Task 9.6: Run full verification suite

**File paths:** (none — verification only)

**What to do:** Run the complete pre-commit checklist:

```bash
# 1. Build
go build ./...

# 2. Vet
go vet ./...

# 3. Lint
make lint

# 4. Security
make sec

# 5. Unit tests
make test

# 6. Test coverage
make check-tests

# 7. Test build tags
make check-test-tags

# 8. Swagger regeneration
make generate-docs

# 9. No stale route references
grep -rn "/v1/config/" --include="*.go" . | grep -v "docs/plans/" | grep -v ".git/"

# 10. No Viper in ConfigManager
grep -l "viper\|Viper" internal/bootstrap/config_manager*.go

# 11. No fsnotify in ConfigManager
grep -l "fsnotify" internal/bootstrap/config_manager*.go
```

All commands must pass/return empty.

**Recommended agent:** `ring:qa-analyst`

---

> **Final Code Review Checkpoint:** After all 9 epics, verify the non-negotiable outcomes:
> - **One runtime authority**: systemplane supervisor
> - **One canonical API naming**: `/v1/contexts`, `/v1/fee-schedules`, `/v1/system/configs`, `/v1/system/settings`
> - **One supervisor-controlled bundle swap mechanism**: atomic swap with last-known-good safety
> - **ConfigManager.Get()** returns systemplane-backed values via ConfigBridgeReconciler
> - **No YAML/Viper** in the runtime path (only initial bootstrap)
> - **No file watcher** (replaced by systemplane change feed)
> - **All tests pass**: unit, integration build, E2E build, chaos build
> - **All routes** use domain-native paths (zero `/v1/config/` in production code)

---

## Execution Order Summary

| Epic | Phase | Tasks | Dependencies | Parallelizable |
|------|-------|-------|-------------|----------------|
| 1. ConfigManager Bridge | 6 | 1.1-1.5 | None | Sequential |
| 2. Consumer Seams | 6 | 2.1-2.4 | Epic 1 | Yes (all 4 in parallel) |
| 3. Worker Reconcile Tests | 6 | 3.1-3.3 | Epic 1 | Yes (all 3 in parallel) |
| 4. Domain Route Rename | 6 | 4.1-4.4 | None | Yes (all 4 in parallel) |
| 5. Test/Client Updates | 6 | 5.1-5.7 | Epic 4 | Yes (all 7 in parallel) |
| 6. Swagger Regeneration | 6 | 6.1 | Epics 4+5 | No |
| 7. Remove YAML/Viper | 7 | 7.1-7.4 | Epics 1-6 | Sequential |
| 8. Shrink Env | 7 | 8.1-8.3 | Epic 7 | Yes (all 3 in parallel) |
| 9. Cleanup/Hardening | 7 | 9.1-9.6 | All above | Partially |

**Optimal parallelization:**
- Wave 1: Epics 1 + 4 (independent)
- Wave 2: Epics 2 + 3 + 5 (after their dependencies)
- Wave 3: Epic 6 (after Epics 4+5)
- Wave 4: Epic 7 (sequential)
- Wave 5: Epics 8 + 9 (after Epic 7)

---

## Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| Stale Config between seed and first systemplane update | Rate limiters use bootstrap values briefly | ConfigBridgeReconciler runs on initial supervisor reload during InitSystemplane() |
| Fiber body_limit_bytes immutable at runtime | Known limitation | Document as bootstrap-only; requires server restart to change |
| Route rename breaks external consumers | None (Matcher not public) | Clean cutover, no dual-route surface |
| Viper removal breaks initial config loading | Cannot start without config | Keep Viper in config_yaml.go for initial bootstrap only |
| snapshotToFullConfig missing a key | Runtime value stuck at default | Cross-reference against systemplane_keys.go matcherKeyDefs(); test with known values |
