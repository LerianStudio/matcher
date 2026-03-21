# Systemplane Migration Skill

> Migrate any Lerian Go service from `.env`/YAML-based configuration to the
> **systemplane** — a database-backed, hot-reloadable runtime configuration and
> settings management plane with full audit history, optimistic concurrency,
> change feeds, and atomic infrastructure replacement.

## When to Use This Skill

Use this skill when:
- Migrating a Lerian Go service (midaz, tracer, plugin-*, etc.) to systemplane
- Adding runtime-managed configuration to a new Lerian service
- Understanding the systemplane architecture for code review or troubleshooting
- Building a new `BundleFactory`, `Reconciler`, or `SnapshotReader` for a service

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Package Reference: `pkg/systemplane/`](#2-package-reference)
3. [The Three Configuration Authorities](#3-the-three-configuration-authorities)
4. [Apply Behavior Taxonomy](#4-apply-behavior-taxonomy)
5. [Migration Methodology (10 Steps)](#5-migration-methodology)
6. [Step-by-Step: Screening a Target Service](#6-screening-a-target-service)
7. [Step-by-Step: Implementing the Migration](#7-implementing-the-migration)
8. [HTTP API Endpoints](#8-http-api-endpoints)
9. [Testing Patterns](#9-testing-patterns)
10. [Operational Guide](#10-operational-guide)

---

## 1. Architecture Overview

The systemplane replaces traditional env-var-only / YAML-based configuration with a
three-tier architecture:

```
┌─ TIER 1: Bootstrap-Only ──────────────────────────────────────────┐
│  Env vars read ONCE at startup. Immutable for process lifetime.   │
│  Examples: SERVER_ADDRESS, AUTH_ENABLED, OTEL_* (telemetry)       │
│  Stored in: BootstrapOnlyConfig struct (frozen at init)           │
└───────────────────────────────────────────────────────────────────┘

┌─ TIER 2: Runtime-Managed (Hot-Reload) ────────────────────────────┐
│  Stored in database (PostgreSQL or MongoDB).                      │
│  Changed via PATCH /v1/system/configs or /v1/system/settings.     │
│  Propagation: ChangeFeed → Supervisor → Snapshot → Bundle/Reconcile│
│  Examples: rate limits, worker intervals, DB pool sizes, CORS     │
└───────────────────────────────────────────────────────────────────┘

┌─ TIER 3: Live-Read (Zero-Cost Per-Request) ───────────────────────┐
│  Read directly from Supervisor.Snapshot() on every request.       │
│  No rebuild, no reconciler, no locking.                           │
│  Examples: rate_limit.max, health_check_timeout_sec               │
└───────────────────────────────────────────────────────────────────┘
```

### Data Flow: Startup → Hot-Reload → Shutdown

```
STARTUP:
  ENV VARS → defaultConfig() → loadConfigFromEnv() → *Config → ConfigManager
  InitSystemplane():
    1. ExtractBootstrapOnlyConfig(cfg)         → BootstrapOnlyConfig (immutable)
    2. LoadSystemplaneBackendConfig(cfg)        → BootstrapConfig (PG/Mongo DSN)
    3. builtin.NewBackendFromConfig(ctx, cfg)   → Store + History + ChangeFeed
    4. registry.New() + Register{Service}Keys() → Registry (100+ key definitions)
    5. NewSnapshotBuilder(registry, store)       → SnapshotBuilder
    6. New{Service}BundleFactory(bootstrapCfg)   → BundleFactory
    7. seedStoreForInitialReload()               → Env overrides → Store
    8. buildReconcilers()                        → [ConfigBridge, HTTP, Worker]
    9. NewSupervisor → Reload("initial-bootstrap") → First bundle
   10. NewManager(config)                         → HTTP API handler
  StartChangeFeed() → goroutine: subscribe(store changes)
  MountSystemplaneAPI() → 9 endpoints on /v1/system/*
  NewSnapshotReader() → live-read accessor

HOT-RELOAD (on API PATCH or ChangeFeed signal):
  Signal → Supervisor.Reload(ctx, reason) →
    1. SnapshotBuilder builds new Snapshot (defaults + store overrides)
    2. BundleFactory.Build(snapshot) → NEW infrastructure clients
    3. Reconcilers run IN ORDER:
       a) ConfigBridge: snapshot → *Config → configManager.Get()
       b) HTTPPolicy: validates body limit, CORS
       c) Worker: snapshot → partial Config → WorkerManager.ApplyConfig()
    4. Supervisor atomically swaps current bundle
    5. Old bundle.Close() tears down previous connections

SHUTDOWN:
  1. ConfigManager.Stop()         (prevent mutations)
  2. cancelChangeFeed()           (stop reload triggers)
  3. Supervisor.Stop()            (stop supervisory loop)
  4. Backend.Close()              (close store connection)
  5. WorkerManager.Stop()         (stop all workers)
```

---

## 2. Package Reference

The systemplane lives in `pkg/systemplane/` — a self-contained, backend-agnostic
library with zero imports of internal application packages. After stabilization,
it will migrate to `lib-commons`.

### 2.1 Domain Layer (`pkg/systemplane/domain/`)

Pure value objects. No infrastructure dependencies.

| Type | Purpose | Key Fields |
|------|---------|------------|
| `Entry` | Persisted config override | `Kind`, `Scope`, `Subject`, `Key`, `Value any`, `Revision` |
| `Kind` | Config vs Setting | `"config"` (admin) or `"setting"` (operator/tenant) |
| `Scope` | Visibility | `"global"` or `"tenant"` |
| `Target` | Coordinate for a group of entries | `Kind` + `Scope` + `SubjectID` |
| `Revision` | Monotonic version counter | `uint64`, methods: `Next()`, `Uint64()` |
| `Actor` | Who made the change | `ID string` |
| `KeyDef` | Registry metadata per key | `Key`, `Kind`, `AllowedScopes`, `DefaultValue`, `ValueType`, `Validator`, `Secret`, `RedactPolicy`, `ApplyBehavior`, `MutableAtRuntime`, `Description`, `Group`, `Component` |
| `ReconcilerPhase` | Reconciler execution ordering | `PhaseStateSync` (0), `PhaseValidation` (1), `PhaseSideEffect` (2) |
| `Snapshot` | Immutable point-in-time view | `Configs`, `GlobalSettings`, `TenantSettings`, `Revision`, `BuiltAt` |
| `EffectiveValue` | Resolved value with override info | `Key`, `Value`, `Default`, `Override`, `Source`, `Revision`, `Redacted` |
| `RuntimeBundle` | App-defined resource container | Interface: `Close(ctx) error` |
| `ApplyBehavior` | How changes propagate | See [Section 4](#4-apply-behavior-taxonomy) |
| `ValueType` | Type constraint | `"string"`, `"int"`, `"bool"`, `"float"`, `"object"`, `"array"` |
| `RedactPolicy` | Secret handling | `"none"`, `"full"`, `"mask"` |
| `BackendKind` | Storage backend | `"postgres"` or `"mongodb"` |

**Sentinel errors** (12): `ErrUnknownKey`, `ErrInvalidValue`, `ErrRevisionMismatch`,
`ErrScopeInvalid`, `ErrPermissionDenied`, `ErrReloadFailed`, `ErrNotMutable`,
`ErrSnapshotBuildFailed`, `ErrBundleBuildFailed`, `ErrBundleSwapFailed`,
`ErrBundleReconcileFailed`, `ErrNoCurrentBundle`, `ErrSupervisorStopped`.

### 2.2 Ports Layer (`pkg/systemplane/ports/`)

Six interfaces defining all external dependencies:

```go
// Persistence — read/write config entries
type Store interface {
    Get(ctx context.Context, target domain.Target) (ReadResult, error)
    Put(ctx context.Context, target domain.Target, ops []WriteOp,
        expected domain.Revision, actor domain.Actor, source string) (domain.Revision, error)
}

// Audit trail — change history
type HistoryStore interface {
    ListHistory(ctx context.Context, filter HistoryFilter) ([]HistoryEntry, error)
}

// Real-time change notifications
type ChangeFeed interface {
    Subscribe(ctx context.Context, handler func(ChangeSignal)) error
}

// Permission checking
type Authorizer interface {
    Authorize(ctx context.Context, permission string) error
}

// Identity extraction from request context
type IdentityResolver interface {
    Actor(ctx context.Context) (domain.Actor, error)
    TenantID(ctx context.Context) (string, error)
}

// Application-specific runtime dependency builder
type BundleFactory interface {
    Build(ctx context.Context, snap domain.Snapshot) (domain.RuntimeBundle, error)
}

// Side-effect applier when bundles change.
// Reconcilers are sorted by Phase before execution:
//   PhaseStateSync  → update shared state (ConfigManager, caches)
//   PhaseValidation → gates that can reject the change
//   PhaseSideEffect → external side effects (worker restarts)
type BundleReconciler interface {
    Name() string
    Phase() domain.ReconcilerPhase
    Reconcile(ctx context.Context, previous, candidate domain.RuntimeBundle,
              snap domain.Snapshot) error
}
```

### 2.3 Registry (`pkg/systemplane/registry/`)

Thread-safe in-memory registry of key definitions:

```go
type Registry interface {
    Register(def domain.KeyDef) error
    MustRegister(def domain.KeyDef)         // panics — startup only
    Get(key string) (domain.KeyDef, bool)
    List(kind domain.Kind) []domain.KeyDef
    Validate(key string, value any) error   // type + custom validation
}
```

Validation handles JSON coercion: `float64` with no fractional part accepted as
`int`, `int`/`int64` widened to `float` for float keys, etc.

### 2.4 Service Layer (`pkg/systemplane/service/`)

| Component | Purpose |
|-----------|---------|
| `Manager` | Public read/write API: GetConfigs, PatchConfigs, GetSchema, GetHistory, Resync |
| `Supervisor` | Lifecycle: Reload → build snapshot → build bundle → reconcile → atomic swap |
| `SnapshotBuilder` | Merges registry defaults + store overrides. 3-layer cascade for tenant settings: `default → global → per-tenant` |
| `Escalation` | Determines strongest `ApplyBehavior` across a batch of writes |

### 2.5 Bootstrap (`pkg/systemplane/bootstrap/`)

| File | Purpose |
|------|---------|
| `config.go` | `BootstrapConfig`, `PostgresBootstrapConfig`, `MongoBootstrapConfig` |
| `backend.go` | `BackendFactory` registry, `RegisterBackendFactory()`, `NewBackendFromConfig()` |
| `builtin/backend.go` | `init()` registers Postgres + MongoDB factories |
| `env.go` | `LoadFromEnv()` reads `SYSTEMPLANE_*` env vars |
| `classifier.go` | `IsBootstrapOnly(def)` / `IsRuntimeManaged(def)` |
| `defaults.go` | Default table/collection names |

**Environment variables** for standalone systemplane backend:

| Variable | Default | Description |
|----------|---------|-------------|
| `SYSTEMPLANE_BACKEND` | — | `postgres` or `mongodb` |
| `SYSTEMPLANE_POSTGRES_DSN` | — | PostgreSQL DSN |
| `SYSTEMPLANE_POSTGRES_SCHEMA` | `system` | Schema name |
| `SYSTEMPLANE_POSTGRES_ENTRIES_TABLE` | `runtime_entries` | Entries table |
| `SYSTEMPLANE_POSTGRES_HISTORY_TABLE` | `runtime_history` | History table |
| `SYSTEMPLANE_POSTGRES_REVISION_TABLE` | `runtime_revisions` | Revisions table |
| `SYSTEMPLANE_POSTGRES_NOTIFY_CHANNEL` | `systemplane_changes` | PG LISTEN/NOTIFY channel |
| `SYSTEMPLANE_MONGODB_URI` | — | MongoDB connection URI |
| `SYSTEMPLANE_MONGODB_DATABASE` | `systemplane` | MongoDB database |
| `SYSTEMPLANE_MONGODB_ENTRIES_COLLECTION` | `runtime_entries` | Entries collection |
| `SYSTEMPLANE_MONGODB_HISTORY_COLLECTION` | `runtime_history` | History collection |
| `SYSTEMPLANE_MONGODB_WATCH_MODE` | `change_stream` | `change_stream` or `poll` |
| `SYSTEMPLANE_MONGODB_POLL_INTERVAL_SEC` | `5` | Poll interval (poll mode only) |

### 2.6 Adapters

| Adapter | Location | Key Feature |
|---------|----------|-------------|
| PostgreSQL Store | `adapters/store/postgres/` | 3 tables, optimistic concurrency, `pg_notify` |
| MongoDB Store | `adapters/store/mongodb/` | Sentinel revision doc, multi-doc transactions |
| PostgreSQL ChangeFeed | `adapters/changefeed/postgres/` | LISTEN/NOTIFY, auto-reconnect, revision resync |
| MongoDB ChangeFeed | `adapters/changefeed/mongodb/` | Change stream or poll mode |
| DebouncedFeed | `adapters/changefeed/debounce.go` | Per-target trailing-edge debounce decorator |
| Fiber HTTP | `adapters/http/fiber/` | 9 endpoints, DTOs, middleware, error mapping |
| Contract Tests | `adapters/store/storetest/`, `adapters/changefeed/feedtest/` | Backend-agnostic test suites |

### 2.7 Test Utilities (`pkg/systemplane/testutil/`)

| Fake | Implements | Usage |
|------|-----------|-------|
| `FakeStore` | `ports.Store` | In-memory with optimistic concurrency |
| `FakeHistoryStore` | `ports.HistoryStore` | In-memory, newest-first retrieval |
| `FakeBundle` / `FakeBundleFactory` | `RuntimeBundle` / `BundleFactory` | Tracks Close state |
| `FakeReconciler` | `BundleReconciler` | Records all calls |

---

## 3. The Three Configuration Authorities

| Phase | Authority | Scope | Mutability |
|-------|-----------|-------|------------|
| **Bootstrap** | Env vars → `defaultConfig()` + `loadConfigFromEnv()` | Server address, TLS, auth, telemetry | Immutable after startup |
| **Runtime** | Systemplane Store + Supervisor | Rate limits, workers, timeouts, DB pools, CORS | Hot-reloadable via API |
| **Legacy bridge** | `ConfigManager.Get()` | Backward-compat for existing code | Updated by ConfigBridge reconciler |

**Single source of truth**: `matcherKeyDefs()` is THE canonical source of all
default values. The `defaultConfig()` function derives its values from KeyDefs
via `defaultSnapshotFromKeyDefs()` → `snapshotToFullConfig()`. No manual sync
required between defaults, key definitions, or struct tags.

---

## 4. Apply Behavior Taxonomy

Every config key MUST be classified with exactly one `ApplyBehavior`:

| ApplyBehavior | Code Constant | Runtime Effect | Use When |
|---------------|---------------|----------------|----------|
| **bootstrap-only** | `domain.ApplyBootstrapOnly` | Immutable after startup. Never changes. | Server listen address, TLS, auth enable, telemetry endpoints |
| **live-read** | `domain.ApplyLiveRead` | Read from snapshot on every request. Zero cost. | Rate limits, timeouts, cache TTLs — anything read per-request |
| **worker-reconcile** | `domain.ApplyWorkerReconcile` | Reconciler restarts affected workers | Worker intervals, scheduler periods |
| **bundle-rebuild** | `domain.ApplyBundleRebuild` | Full bundle swap: new PG/Redis/RMQ/S3 clients | Connection strings, pool sizes, credentials |
| **bundle-rebuild+worker-reconcile** | `domain.ApplyBundleRebuildAndReconcile` | Bundle swap AND worker restart | Worker enable/disable (needs new connections + restart) |

**Classification decision tree**:

```
Is this key needed BEFORE the systemplane itself can start?
  YES → ApplyBootstrapOnly (server address, auth enable, telemetry)
  NO ↓
    
Can this key be read per-request from a snapshot without side effects?
  YES → ApplyLiveRead (rate limits, timeouts, TTLs)
  NO ↓
    
Does changing this key require rebuilding infrastructure clients?
  YES → Does it ALSO require restarting background workers?
    YES → ApplyBundleRebuildAndReconcile (worker enable + storage changes)
    NO  → ApplyBundleRebuild (DB connections, pool sizes, credentials)
  NO ↓
    
Does changing this key require restarting background workers?
  YES → ApplyWorkerReconcile (worker intervals, scheduler periods)
  NO  → ApplyLiveRead (safe default for read-only configs)
```

---

## 5. Migration Methodology (10 Steps)

### Step 1: Audit Current Configuration

Inventory ALL configuration in the target service:

```bash
# Find all env var reads
grep -rn 'os.Getenv\|viper\.\|cfg\.\|config\.' internal/ --include='*.go' | grep -v _test.go

# Find all .env / YAML references  
find . -name '.env*' -o -name '*.yaml' -o -name '*.yml' | grep -i config

# Find all struct tags with envDefault
grep -rn 'envDefault:' internal/ --include='*.go'
```

### Step 2: Classify Every Key

For each configuration key, assign:

| Field | Decision |
|-------|----------|
| **Kind** | `config` (admin-only, infrastructure) or `setting` (tenant-facing, feature flags) |
| **Scope** | `global` (all tenants) or `tenant` (per-tenant override possible) |
| **ApplyBehavior** | Use the decision tree from Section 4 |
| **ValueType** | `string`, `int`, `bool`, `float`, `object`, `array` |
| **Secret** | `true` if contains credentials, tokens, keys |
| **RedactPolicy** | `RedactFull` for secrets, `RedactNone` otherwise |
| **MutableAtRuntime** | `false` for bootstrap-only, `true` for everything else |
| **DefaultValue** | Current default from code/env |
| **Validator** | Custom validation function if needed (e.g., `validatePositiveInt`) |
| **Group** | Logical grouping (e.g., `postgres`, `redis`, `rate_limit`) |

### Step 3: Derive defaultConfig from KeyDefs (Single Source of Truth)

The `matcherKeyDefs()` function IS the canonical source of all defaults. The
`defaultConfig()` function derives from it — no manual struct literal:

```go
// config_defaults.go — derived from KeyDefs, not manually maintained
func defaultConfig() *Config {
    snap := defaultSnapshotFromKeyDefs({service}KeyDefs())
    return snapshotToFullConfig(snap, &Config{})
}

func defaultSnapshotFromKeyDefs(defs []domain.KeyDef) domain.Snapshot {
    configs := make(map[string]domain.EffectiveValue, len(defs))
    for _, def := range defs {
        configs[def.Key] = domain.EffectiveValue{
            Key: def.Key, Value: def.DefaultValue,
            Default: def.DefaultValue, Source: "registry-default",
        }
    }
    return domain.Snapshot{Configs: configs, BuiltAt: time.Now().UTC()}
}
```

This eliminates drift between defaults and key definitions. If you change a
default in `{service}KeyDefs()`, the Config struct picks it up automatically.

### Step 4: Define BootstrapOnlyConfig

Create a struct for keys that CANNOT change at runtime:

```go
// bootstrap_only_config.go
type BootstrapOnlyConfig struct {
    EnvName           string
    ServerAddress     string
    TLSCertFile       string
    TLSKeyFile        string
    AuthEnabled       bool
    AuthHost          string
    AuthTokenSecret   string
    TelemetryEnabled  bool
    OTELServiceName   string
    OTELEndpoint      string
    // ... other immutable keys
}

func ExtractBootstrapOnlyConfig(cfg *Config) BootstrapOnlyConfig {
    return BootstrapOnlyConfig{
        EnvName:       cfg.EnvName,
        ServerAddress: cfg.ServerAddress,
        // ...
    }
}
```

### Step 4: Register All Keys

Create `systemplane_keys.go` with ALL key definitions:

```go
func Register{Service}Keys(reg registry.Registry) error {
    for _, def := range {service}KeyDefs() {
        if err := reg.Register(def); err != nil {
            return fmt.Errorf("register key %q: %w", def.Key, err)
        }
    }
    return nil
}

func {service}KeyDefs() []domain.KeyDef {
    return []domain.KeyDef{
        // ── Postgres ─────────────────────────────────────────
        {
            Key: "postgres.primary_host", Kind: domain.KindConfig,
            AllowedScopes: []domain.Scope{domain.ScopeGlobal},
            DefaultValue: "localhost", ValueType: domain.ValueString,
            ApplyBehavior: domain.ApplyBundleRebuild,
            MutableAtRuntime: true, Secret: false,
            Description: "PostgreSQL primary host address",
            Group: "postgres", Component: "postgres",
        },
        {
            Key: "postgres.primary_password", Kind: domain.KindConfig,
            AllowedScopes: []domain.Scope{domain.ScopeGlobal},
            DefaultValue: "", ValueType: domain.ValueString,
            ApplyBehavior: domain.ApplyBundleRebuild,
            MutableAtRuntime: true, Secret: true,
            RedactPolicy: domain.RedactFull,
            Description: "PostgreSQL primary password",
            Group: "postgres", Component: "postgres",
        },
        // ... continue for ALL keys
    }
}
```

### Step 5: Build the Bundle and BundleFactory

Define what runtime resources the service manages:

```go
// systemplane_bundle.go
type {Service}Bundle struct {
    Infra  *InfraBundle
    HTTP   *HTTPPolicyBundle
    Logger *LoggerBundle
}

type InfraBundle struct {
    Postgres      *libPostgres.Client
    Redis         *libRedis.Client
    RabbitMQ      *libRabbitmq.RabbitMQConnection
    // ... other infra clients
}

func (b *{Service}Bundle) Close(ctx context.Context) error {
    // Close in REVERSE dependency order
    // Logger → ObjectStorage → RabbitMQ → Redis → Postgres
}

// systemplane_factory.go
type {Service}BundleFactory struct {
    bootstrapCfg BootstrapOnlyConfig
}

func (f *{Service}BundleFactory) Build(ctx context.Context, snap domain.Snapshot) (domain.RuntimeBundle, error) {
    // Extract config values from snapshot
    // Build new infrastructure clients
    // Return *{Service}Bundle
}
```

### Step 6: Implement Reconcilers

Build reconcilers for side effects on config changes. Each reconciler declares
its `Phase()` — the supervisor sorts by phase before execution:

```go
// ConfigBridge — PhaseStateSync (runs first, updates shared state)
type ConfigBridgeReconciler struct {
    configManager *ConfigManager
}
func (r *ConfigBridgeReconciler) Phase() domain.ReconcilerPhase { return domain.PhaseStateSync }
func (r *ConfigBridgeReconciler) Reconcile(ctx context.Context, _, _ domain.RuntimeBundle, snap domain.Snapshot) error {
    return r.configManager.UpdateFromSystemplane(snap)
}

// HTTP Policy — PhaseValidation (gate that can reject changes)
type HTTPPolicyReconciler struct{}
func (r *HTTPPolicyReconciler) Phase() domain.ReconcilerPhase { return domain.PhaseValidation }
func (r *HTTPPolicyReconciler) Reconcile(ctx context.Context, _, candidate domain.RuntimeBundle, _ domain.Snapshot) error {
    bundle := candidate.(*{Service}Bundle)
    // Validate HTTP policy fields
}

// Worker — PhaseSideEffect (external side effects, runs last)
type WorkerReconciler struct {
    workerManager *WorkerManager
}
func (r *WorkerReconciler) Phase() domain.ReconcilerPhase { return domain.PhaseSideEffect }
func (r *WorkerReconciler) Reconcile(ctx context.Context, _, _ domain.RuntimeBundle, snap domain.Snapshot) error {
    cfg := snapshotToWorkerConfig(snap)
    return r.workerManager.ApplyConfig(cfg)
}
```

**Phase ordering is enforced by the type system** — you cannot register a
reconciler without declaring its phase. The supervisor stable-sorts by phase,
so reconcilers within the same phase retain their registration order.

### Step 7: Wire Identity and Authorization

```go
// systemplane_identity.go
type {Service}IdentityResolver struct{}
func (r *{Service}IdentityResolver) Actor(ctx context.Context) (domain.Actor, error) {
    uid := auth.GetUserID(ctx)
    if uid == "" { uid = "anonymous" }
    return domain.Actor{ID: uid}, nil
}
func (r *{Service}IdentityResolver) TenantID(ctx context.Context) (string, error) {
    return auth.GetTenantID(ctx)
}

// systemplane_authorizer.go
type {Service}Authorizer struct {
    authEnabled bool
    // Permission map: "system/<suffix>" → RBAC action constant
}
func (a *{Service}Authorizer) Authorize(ctx context.Context, permission string) error {
    if !a.authEnabled { return nil }
    // Map permission to RBAC action, call auth.Authorize()
}
```

### Step 8: Build the Init Function

```go
// systemplane_init.go
func Init{Service}Systemplane(ctx context.Context, cfg *Config, configManager *ConfigManager,
    workerManager *WorkerManager, logger log.Logger) (*SystemplaneComponents, error) {
    
    // 1. Extract bootstrap-only config
    bootstrapCfg := ExtractBootstrapOnlyConfig(cfg)
    
    // 2. Load backend config (default: reuse app's Postgres DSN)
    backendCfg := Load{Service}BackendConfig(cfg)
    
    // 3. Create backend (Store + History + ChangeFeed)
    backend, err := builtin.NewBackendFromConfig(ctx, backendCfg)
    if err != nil { return nil, fmt.Errorf("systemplane backend: %w", err) }
    
    // 4. Create registry + register all keys
    reg := registry.New()
    if err := Register{Service}Keys(reg); err != nil {
        backend.Close(); return nil, err
    }
    
    // 5. Create snapshot builder
    snapBuilder := service.NewSnapshotBuilder(reg, backend.Store)
    
    // 6. Create bundle factory
    bundleFactory := New{Service}BundleFactory(bootstrapCfg)
    
    // 7. Seed store from current env-var config
    if err := seedStoreForInitialReload(ctx, configManager, reg, backend.Store); err != nil {
        backend.Close(); return nil, err
    }
    
    // 8. Build reconcilers (order matters!)
    reconcilers := []ports.BundleReconciler{
        NewConfigBridgeReconciler(configManager),  // MUST be first
        NewHTTPPolicyReconciler(),
        NewWorkerReconciler(workerManager),
    }
    
    // 9. Create supervisor + initial reload
    supervisor := service.NewSupervisor(service.SupervisorConfig{
        SnapshotBuilder: snapBuilder,
        BundleFactory:   bundleFactory,
        Reconcilers:     reconcilers,
        Logger:          logger,
    })
    if err := supervisor.Reload(ctx, "initial-bootstrap"); err != nil {
        backend.Close(); return nil, err
    }
    
    // 10. Create manager (HTTP API handler)
    manager := service.NewManager(service.ManagerConfig{
        Registry:   reg,
        Store:      backend.Store,
        History:    backend.History,
        Supervisor: supervisor,
        Logger:     logger,
    })
    
    return &SystemplaneComponents{
        Backend:    backend,
        Registry:   reg,
        Supervisor: supervisor,
        Manager:    manager,
    }, nil
}
```

### Step 9: Mount HTTP API and Start ChangeFeed

```go
// systemplane_mount.go
func MountSystemplaneAPI(app *fiber.App, manager service.Manager,
    supervisor service.Supervisor, registry registry.Registry,
    authEnabled bool) {
    
    authorizer := New{Service}Authorizer(authEnabled)
    identity := &{Service}IdentityResolver{}
    
    handler := fiberhttp.NewHandler(manager, supervisor, registry, authorizer, identity)
    handler.Mount(app.Group("/v1/system"))
}

// In init.go — start change feed subscriber
func StartChangeFeed(ctx context.Context, changeFeed ports.ChangeFeed,
    supervisor service.Supervisor) context.CancelFunc {
    
    feedCtx, cancel := context.WithCancel(ctx)
    go func() {
        _ = changeFeed.Subscribe(feedCtx, func(signal ports.ChangeSignal) {
            _ = supervisor.Reload(feedCtx, "changefeed-signal")
        })
    }()
    return cancel
}
```

### Step 10: Update Shutdown Sequence

```go
// service.go — shutdown in correct order
func (s *Service) Stop(ctx context.Context) {
    s.configManager.Stop()           // 1. Prevent mutations
    s.cancelChangeFeed()             // 2. Stop change feed BEFORE supervisor
    s.spComponents.Supervisor.Stop(ctx) // 3. Stop supervisor
    s.spComponents.Backend.Close()   // 4. Close store connection
    s.workerManager.Stop()           // 5. Stop workers
}
```

---

## 6. Screening a Target Service

Before implementing, screen the target service to build the key inventory:

### 6.1 Identify All Configuration Sources

```bash
# In the target repo:

# 1. Find the Config struct
grep -rn 'type Config struct' internal/ --include='*.go'

# 2. Find all envDefault tags
grep -rn 'envDefault:' internal/ --include='*.go' | sort

# 3. Find env var reads outside Config struct
grep -rn 'os.Getenv\|viper.Get' internal/ --include='*.go' | grep -v _test.go

# 4. Find .env files
find . -name '.env*' -o -name '*.yaml.example' | head -20

# 5. Find file-based config loading
grep -rn 'viper\.\|yaml.Unmarshal\|json.Unmarshal.*config' internal/ --include='*.go'
```

### 6.2 Classify Infrastructure vs Application Config

**Infrastructure (stays as env-var with defaults in code)**:
- Database connection strings (PG host, port, user, password)
- Redis connection (host, master name, password)
- RabbitMQ connection (URI, host, port, user, password)
- Object storage (endpoint, bucket, credentials)
- These become `ApplyBundleRebuild` keys in systemplane

**Bootstrap-Only (env-var, immutable after startup)**:
- Server listen address and port
- TLS certificate paths
- Auth enable/disable
- Auth service address
- Telemetry endpoints (OTEL collector)
- These become `ApplyBootstrapOnly` keys

**Application Runtime (hot-reloadable)**:
- Rate limits
- Worker intervals and enable/disable flags
- Timeouts (webhook, health check)
- Feature flags
- Cache TTLs
- These become `ApplyLiveRead` or `ApplyWorkerReconcile` keys

### 6.3 Identify Runtime Dependencies (Bundle Candidates)

List all infrastructure clients that the service creates at startup:

```bash
# Find client constructors
grep -rn 'libPostgres.New\|libRedis.New\|libRabbitmq.New\|storage.New' internal/ --include='*.go'

# Find connection pools
grep -rn 'sql.Open\|pgx\|redis.New\|amqp.Dial' internal/ --include='*.go'
```

Each of these becomes a field in the `InfraBundle`.

### 6.4 Identify Background Workers

```bash
# Find worker patterns
grep -rn 'ticker\|time.NewTicker\|cron\|worker\|scheduler' internal/ --include='*.go' | grep -v _test.go
```

Each worker with configurable intervals becomes a `WorkerReconciler` candidate.

### 6.5 Generate the Key Inventory

Create a spreadsheet or table with columns:
- Key name (dotted: `postgres.primary_host`)
- Current env var (`POSTGRES_HOST`)
- Default value
- Type (`string`, `int`, `bool`, `float`)
- Kind (`config` or `setting`)
- Scope (`global` or `tenant`)
- ApplyBehavior
- Secret (yes/no)
- MutableAtRuntime (yes/no)
- Group
- Validator (if any)

---

## 7. Implementing the Migration

### 7.1 Files to Create (per service)

| File | Purpose | Reference in Matcher |
|------|---------|---------------------|
| `systemplane_init.go` | Init function with 10-step boot | `internal/bootstrap/systemplane_init.go` |
| `systemplane_keys.go` | Key definitions (~50-200 keys) | `internal/bootstrap/systemplane_keys.go` |
| `systemplane_bundle.go` | Bundle struct + Close | `internal/bootstrap/systemplane_bundle.go` |
| `systemplane_factory.go` | BundleFactory implementation | `internal/bootstrap/systemplane_factory.go` |
| `systemplane_reconciler_config.go` | ConfigBridge reconciler | `internal/bootstrap/systemplane_reconciler_config.go` |
| `systemplane_reconciler_http.go` | HTTP policy validation | `internal/bootstrap/systemplane_reconciler_http.go` |
| `systemplane_reconciler_worker.go` | Worker restart reconciler | `internal/bootstrap/systemplane_reconciler_worker.go` |
| `systemplane_identity.go` | JWT identity resolver | `internal/bootstrap/systemplane_identity.go` |
| `systemplane_authorizer.go` | Permission mapping | `internal/bootstrap/systemplane_authorizer.go` |
| `systemplane_mount.go` | HTTP route registration | `internal/bootstrap/systemplane_mount.go` |
| `systemplane_snapshot_reader.go` | Live-read accessors | `internal/bootstrap/systemplane_snapshot_reader.go` |
| `config_manager_systemplane.go` | Snapshot→Config hydration | `internal/bootstrap/config_manager_systemplane.go` |
| `config_manager_seed.go` | Env→Store one-time seed | `internal/bootstrap/config_manager_seed.go` |
| `config_validation.go` | Production config guards | `internal/bootstrap/config_validation.go` |
| `config/.config-map.example` | Bootstrap-only key reference | `config/.config-map.example` |

### 7.2 Files to Delete

| File | Reason |
|------|--------|
| `config/.env.example` | Replaced by code defaults + `.config-map.example` |
| `config/*.yaml.example` | No more YAML config |
| Config API handlers (old) | Replaced by systemplane HTTP adapter |
| Config file watcher | Replaced by change feed |
| Config audit publisher (old) | Replaced by systemplane history |
| Config YAML loader | No more YAML |
| `docker-compose.prod.yml` | Use single docker-compose with inline defaults |

### 7.3 Files to Modify

| File | Change |
|------|--------|
| `config_loading.go` | Remove YAML loading, keep env-only |
| `config_defaults.go` | Ensure defaults match systemplane key defs |
| `config_manager.go` | Add `UpdateFromSystemplane()`, seed mode |
| `init.go` | Wire systemplane init after workers, before HTTP mount |
| `service.go` | Add systemplane shutdown sequence |
| `docker-compose.yml` | Remove `env_file`, inline defaults with `${VAR:-default}` |
| `Makefile` | Remove `set-env`, `clear-envs` targets |

### 7.4 The Snapshot→Config Hydration Function

This is the most labor-intensive part — mapping every snapshot key back to the
Config struct. Pattern from Matcher:

```go
// config_manager_systemplane.go
func snapshotToFullConfig(snap domain.Snapshot, old *Config) *Config {
    cfg := &Config{}
    
    // Copy bootstrap-only fields from old config (they don't change)
    cfg.EnvName = old.EnvName
    cfg.ServerAddress = old.ServerAddress
    cfg.AuthEnabled = old.AuthEnabled
    // ...
    
    // Read runtime fields from snapshot
    cfg.Postgres.PrimaryHost = snapString(snap, "postgres.primary_host", defaultPostgresHost)
    cfg.Postgres.PrimaryPort = snapInt(snap, "postgres.primary_port", defaultPostgresPort)
    cfg.Postgres.MaxOpenConns = snapInt(snap, "postgres.max_open_connections", defaultMaxOpenConns)
    cfg.Redis.Host = snapString(snap, "redis.host", defaultRedisHost)
    cfg.RateLimitMax = snapInt(snap, "rate_limit.max", defaultRateLimitMax)
    // ... every runtime key
    
    return cfg
}
```

**Helper functions** for type-safe extraction:

```go
func snapString(snap domain.Snapshot, key, fallback string) string {
    if ev, ok := snap.Configs[key]; ok {
        if s, ok := ev.Value.(string); ok { return s }
    }
    return fallback
}

func snapInt(snap domain.Snapshot, key string, fallback int) int {
    if ev, ok := snap.Configs[key]; ok {
        switch v := ev.Value.(type) {
        case int:     return v
        case int64:   return int(v)
        case float64: return int(v) // JSON deserialization
        }
    }
    return fallback
}

func snapBool(snap domain.Snapshot, key string, fallback bool) bool {
    if ev, ok := snap.Configs[key]; ok {
        if b, ok := ev.Value.(bool); ok { return b }
    }
    return fallback
}
```

---

## 8. HTTP API Endpoints

After migration, the service exposes these endpoints:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/system/configs` | View all resolved config values |
| `PATCH` | `/v1/system/configs` | Update config values (with `If-Match` for concurrency) |
| `GET` | `/v1/system/configs/schema` | View all key definitions (types, defaults, mutability) |
| `GET` | `/v1/system/configs/history` | Audit trail of config changes |
| `POST` | `/v1/system/configs/reload` | Force a full reload |
| `GET` | `/v1/system/settings` | View resolved settings (`?scope=global\|tenant`) |
| `PATCH` | `/v1/system/settings` | Update settings |
| `GET` | `/v1/system/settings/schema` | View setting key definitions |
| `GET` | `/v1/system/settings/history` | Settings audit trail |

### PATCH Request Format

```json
PATCH /v1/system/configs
If-Match: "42"
Content-Type: application/json

{
  "values": {
    "rate_limit.max": 200,
    "rate_limit.expiry_sec": 120,
    "export_worker.enabled": false
  }
}
```

### PATCH Response Format

<!-- Note: These examples match the actual DTOs in pkg/systemplane/adapters/http/fiber/dto.go -->

```json
HTTP/1.1 200 OK
ETag: "43"

{
  "revision": 43
}
```

### Schema Response Format

```json
{
  "keys": [
    {
      "key": "rate_limit.max",
      "kind": "config",
      "valueType": "int",
      "defaultValue": 100,
      "mutableAtRuntime": true,
      "applyBehavior": "live-read",
      "secret": false,
      "description": "Maximum requests per window",
      "group": "rate_limit",
      "allowedScopes": ["global"]
    }
  ]
}
```

---

## 9. Testing Patterns

### 9.1 Key Registration Tests

```go
func TestRegister{Service}Keys_AllKeysValid(t *testing.T) {
    reg := registry.New()
    err := Register{Service}Keys(reg)
    require.NoError(t, err)
    
    // Verify count
    configs := reg.List(domain.KindConfig)
    settings := reg.List(domain.KindSetting)
    assert.Greater(t, len(configs)+len(settings), 0)
}

func TestRegister{Service}Keys_DefaultsMatchConfig(t *testing.T) {
    // Verify triple source-of-truth invariant
    reg := registry.New()
    _ = Register{Service}Keys(reg)
    defaults := defaultConfig()
    
    for _, def := range reg.List(domain.KindConfig) {
        // Compare def.DefaultValue against defaults struct field
    }
}
```

### 9.2 Bundle Factory Tests

```go
func TestBundleFactory_Build_Success(t *testing.T) {
    snap := buildTestSnapshot(t)
    factory := New{Service}BundleFactory(testBootstrapConfig())
    
    bundle, err := factory.Build(context.Background(), snap)
    require.NoError(t, err)
    defer bundle.Close(context.Background())
    
    b := bundle.(*{Service}Bundle)
    assert.NotNil(t, b.Infra.Postgres)
    assert.NotNil(t, b.Infra.Redis)
}
```

### 9.3 Reconciler Tests

```go
func TestConfigBridgeReconciler_UpdatesConfigManager(t *testing.T) {
    cm := NewConfigManager(defaultConfig(), logger)
    reconciler := NewConfigBridgeReconciler(cm)
    
    snap := buildModifiedSnapshot(t, "rate_limit.max", 500)
    err := reconciler.Reconcile(context.Background(), nil, nil, snap)
    require.NoError(t, err)
    
    assert.Equal(t, 500, cm.Get().RateLimitMax)
}
```

### 9.4 Contract Tests

Run the `storetest` and `feedtest` contract suites against your backend:

```go
func TestPostgresStore_ContractSuite(t *testing.T) {
    store, history := setupPostgresStore(t)
    storetest.Run(t, store, history)
}
```

---

## 10. Operational Guide

### 10.1 For Operators: What Changes

| Before | After |
|--------|-------|
| Edit `.env` + restart | `PATCH /v1/system/configs` (no restart) |
| Edit YAML + wait for fsnotify | `PATCH /v1/system/configs` (instant) |
| No audit trail | `GET /v1/system/configs/history` |
| No schema discovery | `GET /v1/system/configs/schema` |
| No concurrency protection | `If-Match` / `ETag` headers |
| Manual rollback | Change feed propagates across replicas |

### 10.2 Bootstrap-Only Keys (Require Restart)

Document in `config/.config-map.example`:

```
# {Service} — Bootstrap-Only Configuration (requires restart)
#
# These are the ONLY settings that require a container/pod restart.
# Everything else is hot-reloadable via:
#
#   GET  /v1/system/configs          — view current runtime config
#   PATCH /v1/system/configs         — change any runtime-managed key
#   GET  /v1/system/configs/schema   — see all keys, types, and mutability
#   GET  /v1/system/configs/history  — audit trail of changes

ENV_NAME=development
SERVER_ADDRESS=:8080
AUTH_ENABLED=false
ENABLE_TELEMETRY=false
# ... etc
```

### 10.3 Docker Compose (Zero-Config)

```yaml
services:
  myservice:
    build: .
    ports:
      - "${SERVER_PORT:-8080}:8080"
    environment:
      - POSTGRES_HOST=${POSTGRES_HOST:-postgres}
      - POSTGRES_PORT=${POSTGRES_PORT:-5432}
      - REDIS_HOST=${REDIS_HOST:-redis}
    # NO env_file directive — defaults baked into binary
    depends_on:
      postgres:
        condition: service_healthy
```

### 10.4 Systemplane Backend Config

By default, the systemplane reuses the application's primary PostgreSQL connection.
Override with `SYSTEMPLANE_*` env vars for a separate backend:

```bash
# Use app's Postgres (default — no extra config needed)
# The init function builds the DSN from the app's POSTGRES_* env vars

# Or use a dedicated backend:
SYSTEMPLANE_BACKEND=postgres
SYSTEMPLANE_POSTGRES_DSN=postgres://user:pass@host:5432/systemplane?sslmode=require
```

### 10.5 Graceful Degradation

If the systemplane fails to initialize, the service continues without it:
- Config values from env vars still work
- No runtime mutation API available
- No hot-reload capability
- Workers run with static config

This is by design — the service never fails to start due to systemplane issues.

---

## Appendix A: Matcher Key Inventory (Reference)

The Matcher service registers **100+ keys** across 20 groups. Here's a summary:

| Group | Keys | Bootstrap | BundleRebuild | LiveRead | WorkerReconcile | Rebuild+Reconcile |
|-------|------|-----------|---------------|----------|-----------------|-------------------|
| `app` | 2 | 1 | 1 | - | - | - |
| `server` | 8 | 4 | 4 | - | - | - |
| `tenancy` | 11 | - | 11 | - | - | - |
| `postgres` | 17 | - | 17 | - | - | - |
| `redis` | 12 | - | 12 | - | - | - |
| `rabbitmq` | 8 | - | 8 | - | - | - |
| `auth` | 3 | 3 | - | - | - | - |
| `swagger` | 3 | - | 3 | - | - | - |
| `telemetry` | 7 | 7 | - | - | - | - |
| `rate_limit` | 7 | - | - | 7 | - | - |
| `infrastructure` | 2 | - | 1 | 1 | - | - |
| `idempotency` | 3 | - | 3 | - | - | - |
| `callback_rate_limit` | 1 | - | - | 1 | - | - |
| `fetcher` | 9 | - | 4 | - | 3 | 2 |
| `deduplication` | 1 | - | 1 | - | - | - |
| `object_storage` | 6 | - | 6 | - | - | - |
| `export_worker` | 4 | - | - | 1 | 1 | 2 |
| `webhook` | 1 | - | 1 | - | - | - |
| `cleanup_worker` | 4 | - | - | - | 2 | 2 |
| `scheduler` | 1 | - | - | - | 1 | - |
| `archival` | 12 | - | - | 1 | 3 | 8 |

**Secrets**: 10 keys with `RedactFull` policy (passwords, tokens, certificates, access keys).

---

## Appendix B: Quick Reference Commands

```bash
# LOCAL DEV ONLY — requires AUTH_ENABLED=false

# View current runtime config
curl -s http://localhost:4018/v1/system/configs | jq

# View schema (all keys, types, mutability)
curl -s http://localhost:4018/v1/system/configs/schema | jq

# Change a runtime key
curl -X PATCH http://localhost:4018/v1/system/configs \
  -H 'Content-Type: application/json' \
  -H 'If-Match: "current-revision"' \
  -d '{"values": {"rate_limit.max": 200}}'

# View change history
curl -s http://localhost:4018/v1/system/configs/history | jq

# Force full reload
curl -X POST http://localhost:4018/v1/system/configs/reload

# View settings
curl -s http://localhost:4018/v1/system/settings?scope=global | jq

# Change a setting
curl -X PATCH http://localhost:4018/v1/system/settings \
  -H 'Content-Type: application/json' \
  -H 'If-Match: "current-revision"' \
  -d '{"scope": "global", "values": {"feature.enabled": true}}'
```
