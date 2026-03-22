# Systemplane Migration Skill

> Migrate any Lerian Go service from `.env`/YAML-based configuration to the
> **systemplane** — a database-backed, hot-reloadable runtime configuration and
> settings management plane with full audit history, optimistic concurrency,
> change feeds, component-granular bundle rebuilds, and atomic infrastructure
> replacement.

## When to Use This Skill

Use this skill when:
- Migrating a Lerian Go service (midaz, tracer, plugin-*, etc.) to systemplane
- Adding runtime-managed configuration to a new Lerian service
- Understanding the systemplane architecture for code review or troubleshooting
- Building a new `BundleFactory`, `Reconciler`, or `SnapshotReader` for a service

## Import Paths

The systemplane is a shared library in lib-commons (since v4.3.0). All services
import it via:

```go
import (
    "github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
    "github.com/LerianStudio/lib-commons/v4/commons/systemplane/ports"
    "github.com/LerianStudio/lib-commons/v4/commons/systemplane/registry"
    "github.com/LerianStudio/lib-commons/v4/commons/systemplane/service"
    "github.com/LerianStudio/lib-commons/v4/commons/systemplane/bootstrap"
    "github.com/LerianStudio/lib-commons/v4/commons/systemplane/bootstrap/builtin"
    "github.com/LerianStudio/lib-commons/v4/commons/systemplane/adapters/changefeed"
    fiberhttp "github.com/LerianStudio/lib-commons/v4/commons/systemplane/adapters/http/fiber"
)
```

Requires `lib-commons v4.3.0+` in your `go.mod`.

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Package Reference: `lib-commons/v4/commons/systemplane/`](#2-package-reference)
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
    5. configureBackendWithRegistry()            → pass secret keys + apply behaviors
    6. NewSnapshotBuilder(registry, store)       → SnapshotBuilder
    7. New{Service}BundleFactory(bootstrapCfg)   → BundleFactory (full + incremental)
    8. seedStoreForInitialReload()               → Env overrides → Store
    9. buildReconcilers()                        → [HTTP, Publisher, Worker] (phased)
   10. NewSupervisor → Reload("initial-bootstrap") → First bundle
   11. NewManager(ManagerConfig{                  → HTTP API handler
           ConfigWriteValidator: productionGuards,
           StateSync: configManagerSync,
       })
  StartChangeFeed() → DebouncedFeed(200ms) → goroutine: subscribe(store changes)
  MountSystemplaneAPI() → 9 endpoints on /v1/system/*

HOT-RELOAD (on API PATCH or ChangeFeed signal):
  Signal → Supervisor.Reload(ctx, reason) →
    1. SnapshotBuilder.BuildFull() → new Snapshot (defaults + store overrides)
    2. BundleFactory.BuildIncremental(snap, prev, prevSnap) →
       a) Diff changed keys via keyComponentMap (postgres/redis/rabbitmq/s3/http/logger)
       b) Rebuild ONLY changed components
       c) Reuse unchanged components from previous bundle (pointer transfer)
       d) Falls back to full Build() when all components are affected
    3. Reconcilers run IN PHASE ORDER:
       a) PhaseStateSync:    (reserved — no current matcher reconcilers)
       b) PhaseValidation:   HTTPPolicy, Publisher
       c) PhaseSideEffect:   Worker → WorkerManager.ApplyConfig()
    4. On success: atomic swap — snapshot.Store() + bundle.Store()
    5. AdoptResourcesFrom(previous) → nil-out transferred pointers
    6. Observer callback → bundleState.Update() + ConfigManager.UpdateFromSystemplane()
       + SwappableLogger.Swap() + SwapRuntimePublishers()
    7. previous.Close() → only tears down REPLACED components

SHUTDOWN:
  1. ConfigManager.Stop()         (prevent mutations)
  2. cancelChangeFeed()           (stop reload triggers)
  3. Supervisor.Stop()            (stop supervisory loop + close bundle)
  4. Backend.Close()              (close store connection)
  5. WorkerManager.Stop()         (stop all workers)
```

---

## 2. Package Reference

The systemplane lives in `lib-commons/v4/commons/systemplane/` — a self-contained,
backend-agnostic library with **zero imports of internal application packages**.
It was extracted from Matcher's `pkg/systemplane/` into lib-commons v4.3.0 to be
shared across all Lerian services.

### 2.1 Directory Structure

```
commons/systemplane/                            # in lib-commons repo
├── doc.go                                      # Package doc
├── domain/                                     # Pure value objects, no infra deps
│   ├── actor.go                                # Actor{ID}
│   ├── apply_behavior.go                       # ApplyBehavior enum + Strength()
│   ├── backend_kind.go                         # BackendKind enum (postgres, mongodb)
│   ├── bundle.go                               # RuntimeBundle interface
│   ├── entry.go                                # Entry (persisted override record)
│   ├── errors.go                               # 14 sentinel errors
│   ├── key_def.go                              # KeyDef, ValueType, RedactPolicy, ValidatorFunc, ComponentNone
│   ├── kind.go                                 # Kind enum (config, setting)
│   ├── nil_value.go                            # IsNilValue() — reflect-based typed-nil detection
│   ├── reconciler_phase.go                     # ReconcilerPhase enum (3 phases)
│   ├── revision.go                             # Revision type (uint64 wrapper)
│   ├── scope.go                                # Scope enum (global, tenant)
│   ├── snapshot.go                             # Snapshot + EffectiveValue
│   └── target.go                               # Target (kind/scope/subject coordinate)
├── ports/                                      # Interface definitions (hex boundary)
│   ├── authorizer.go                           # Authorizer
│   ├── bundle_factory.go                       # BundleFactory + IncrementalBundleFactory
│   ├── changefeed.go                           # ChangeFeed + ChangeSignal
│   ├── history.go                              # HistoryStore + HistoryEntry + HistoryFilter
│   ├── identity.go                             # IdentityResolver
│   ├── reconciler.go                           # BundleReconciler (phased)
│   └── store.go                                # Store + WriteOp + ReadResult
├── registry/                                   # Thread-safe key definition registry
│   ├── registry.go                             # Registry interface + inMemoryRegistry
│   └── validation.go                           # Type validation with JSON coercion
├── service/                                    # Use-case orchestration
│   ├── escalation.go                           # Escalate() — strongest ApplyBehavior
│   ├── manager.go                              # Manager interface + ManagerConfig
│   ├── manager_helpers.go                      # Redaction, schema building
│   ├── manager_reads.go                        # GetConfigs, GetSettings, GetSchema, GetHistory, Resync
│   ├── manager_writes.go                       # PatchConfigs, PatchSettings, ApplyChangeSignal
│   ├── manager_writes_helpers.go               # Validation, escalation, snapshot preview
│   ├── snapshot_builder.go                     # 3-layer cascade: default → global → tenant
│   ├── supervisor.go                           # Supervisor interface + Reload lifecycle
│   └── supervisor_helpers.go                   # Build strategy, phase sort, resource adoption
├── bootstrap/                                  # Backend wiring from env/config
│   ├── backend.go                              # BackendFactory registry + NewBackendFromConfig
│   ├── classifier.go                           # IsBootstrapOnly / IsRuntimeManaged
│   ├── config.go                               # BootstrapConfig, PostgresBootstrapConfig, MongoBootstrapConfig
│   ├── defaults.go                             # Default table/collection names
│   ├── env.go                                  # LoadFromEnv() — SYSTEMPLANE_* env vars
│   ├── postgres_identifiers.go                 # Regex validation for PG identifiers
│   └── builtin/
│       └── backend.go                          # init()-registered factories (PG + Mongo)
├── adapters/
│   ├── store/
│   │   ├── secretcodec/
│   │   │   └── codec.go                        # AES-256-GCM encryption for secret values
│   │   ├── storetest/
│   │   │   └── contract.go                     # 15 shared contract tests (Store + History)
│   │   ├── postgres/                           # 3 tables, optimistic concurrency, pg_notify
│   │   │   ├── ddl.go                          # CREATE TABLE templates
│   │   │   ├── postgres.go                     # New(), NewFromDB() constructors
│   │   │   ├── store.go                        # Get(), Put()
│   │   │   ├── store_mutation.go               # applyOp, upsert, delete, insertHistory
│   │   │   ├── store_revision.go               # lock/read/update revision
│   │   │   ├── store_runtime.go                # encrypt/decrypt, notify, escalate
│   │   │   ├── json_decode.go                  # Integer-preserving JSON decoder
│   │   │   ├── history.go                      # ListHistory with dynamic builder
│   │   │   └── identifiers.go                  # qualify() helper
│   │   └── mongodb/                            # Sentinel revision doc, multi-doc txns
│   │       ├── mongodb.go                      # New(), ensureIndexes, txn support check
│   │       ├── store.go                        # Get(), Put() with sessions
│   │       ├── store_helpers.go                # applyOperation, encrypt, CRUD
│   │       ├── models.go                       # BSON models + normalization
│   │       └── history.go                      # ListHistory with BSON filter
│   ├── changefeed/
│   │   ├── debounce.go                         # Trailing-edge per-target debounce
│   │   ├── debounce_helpers.go                 # Timer management, jitter
│   │   ├── safe_handler.go                     # Panic-to-error conversion
│   │   ├── feedtest/
│   │   │   └── contract.go                     # 3 shared contract tests for ChangeFeed
│   │   ├── postgres/                           # LISTEN/NOTIFY, auto-reconnect, revision resync
│   │   │   ├── feed.go                         # Feed struct + Subscribe
│   │   │   ├── feed_subscribe.go               # subscribeLoop, reconnect, listenLoop
│   │   │   ├── feed_runtime.go                 # backoff, jitter, revision validation
│   │   │   └── options.go                      # WithReconnectBounds, WithRevisionSource
│   │   └── mongodb/                            # change_stream or poll mode
│   │       ├── feed.go                         # Feed struct + Subscribe
│   │       ├── feed_stream.go                  # subscribeChangeStream
│   │       ├── feed_poll.go                    # subscribePoll, pollRevisions
│   │       └── feed_bson.go                    # BSON helpers
│   └── http/
│       └── fiber/                              # 9 endpoints, DTOs, middleware
│           ├── handler.go                      # Handler struct, NewHandler, Mount()
│           ├── handler_configs.go              # Config CRUD + reload
│           ├── handler_settings.go             # Settings CRUD
│           ├── dto.go                          # All request/response DTOs
│           ├── middleware.go                   # requireAuth, settingsAuth
│           └── errors.go                       # Domain error → HTTP status mapping
└── testutil/                                   # Test doubles
    ├── fake_store.go                           # In-memory Store with concurrency
    ├── fake_history.go                         # In-memory HistoryStore
    ├── fake_bundle.go                          # FakeBundle + FakeBundleFactory
    ├── fake_reconciler.go                      # Records all calls, configurable phase
    └── fake_incremental_bundle.go              # FakeIncrementalBundleFactory
```

### 2.2 Domain Layer (`commons/systemplane/domain/`)

Pure value objects. No infrastructure dependencies.

| Type | Purpose | Key Fields |
|------|---------|------------|
| `Entry` | Persisted config override | `Kind`, `Scope`, `Subject`, `Key`, `Value any`, `Revision`, `UpdatedAt`, `UpdatedBy`, `Source` |
| `Kind` | Config vs Setting | `KindConfig` (`"config"`) or `KindSetting` (`"setting"`) |
| `Scope` | Visibility | `ScopeGlobal` (`"global"`) or `ScopeTenant` (`"tenant"`) |
| `Target` | Coordinate for a group of entries | `Kind` + `Scope` + `SubjectID`; constructor `NewTarget()` validates |
| `Revision` | Monotonic version counter | `uint64`; `RevisionZero = 0`; methods: `Next()`, `Uint64()` |
| `Actor` | Who made the change | `ID string` |
| `KeyDef` | Registry metadata per key | `Key`, `Kind`, `AllowedScopes`, `DefaultValue`, `ValueType`, `Validator ValidatorFunc`, `Secret`, `RedactPolicy`, `ApplyBehavior`, `MutableAtRuntime`, `Description`, `Group`, `Component` |
| `ReconcilerPhase` | Reconciler execution ordering | `PhaseStateSync` (0), `PhaseValidation` (1), `PhaseSideEffect` (2) |
| `Snapshot` | Immutable point-in-time view | `Configs`, `GlobalSettings`, `TenantSettings map[string]map[string]EffectiveValue`, `Revision`, `BuiltAt` |
| `EffectiveValue` | Resolved value with override info | `Key`, `Value`, `Default`, `Override`, `Source`, `Revision`, `Redacted` |
| `RuntimeBundle` | App-defined resource container | Interface: `Close(ctx) error` |
| `ApplyBehavior` | How changes propagate | See [Section 4](#4-apply-behavior-taxonomy); has `Strength() int` (0–4 scale) |
| `ValueType` | Type constraint | `"string"`, `"int"`, `"bool"`, `"float"`, `"object"`, `"array"` |
| `RedactPolicy` | Secret handling | `RedactNone`, `RedactFull`, `RedactMask` |
| `BackendKind` | Storage backend | `BackendPostgres` or `BackendMongoDB` |
| `ValidatorFunc` | Custom value validation | `func(value any) error` |
| `ComponentNone` | Sentinel for no-rebuild keys | `"_none"` — for pure business-logic keys |

**Snapshot accessor methods** (nil-safe):
- `GetConfig(key)`, `GetGlobalSetting(key)`, `GetTenantSetting(tenantID, key)` → `(EffectiveValue, bool)`
- `ConfigValue(key, fallback)`, `GlobalSettingValue(key, fallback)`, `TenantSettingValue(tenantID, key, fallback)` → `any`

**Sentinel errors** (14):

```go
var (
    ErrKeyUnknown          = errors.New("unknown configuration key")
    ErrValueInvalid        = errors.New("invalid configuration value")
    ErrRevisionMismatch    = errors.New("revision mismatch")
    ErrScopeInvalid        = errors.New("scope not allowed for this key")
    ErrPermissionDenied    = errors.New("permission denied")
    ErrReloadFailed        = errors.New("configuration reload failed")
    ErrKeyNotMutable       = errors.New("key is not mutable at runtime")
    ErrSnapshotBuildFailed = errors.New("snapshot build failed")
    ErrBundleBuildFailed   = errors.New("runtime bundle build failed")
    ErrBundleSwapFailed    = errors.New("runtime bundle swap failed")
    ErrReconcileFailed     = errors.New("bundle reconciliation failed")
    ErrNoCurrentBundle     = errors.New("no current runtime bundle")
    ErrSupervisorStopped   = errors.New("supervisor has been stopped")
    ErrRegistryRequired    = errors.New("registry is required")
)
```

Per-enum parse errors: `ErrInvalidKind`, `ErrInvalidBackendKind`, `ErrInvalidScope`,
`ErrInvalidApplyBehavior`, `ErrInvalidValueType`, `ErrInvalidReconcilerPhase`.

### 2.3 Ports Layer (`commons/systemplane/ports/`)

Seven interfaces defining all external dependencies:

```go
// Persistence — read/write config entries
type Store interface {
    Get(ctx context.Context, target domain.Target) (ReadResult, error)
    Put(ctx context.Context, target domain.Target, ops []WriteOp,
        expected domain.Revision, actor domain.Actor, source string) (domain.Revision, error)
}

type WriteOp struct { Key string; Value any; Reset bool }
type ReadResult struct { Entries []domain.Entry; Revision domain.Revision }

// Audit trail — change history
type HistoryStore interface {
    ListHistory(ctx context.Context, filter HistoryFilter) ([]HistoryEntry, error)
}

type HistoryEntry struct {
    Revision  domain.Revision
    Key       string
    Scope     string
    SubjectID string
    OldValue  any
    NewValue  any
    ActorID   string
    ChangedAt time.Time
}

type HistoryFilter struct { Kind, Scope, SubjectID, Key string; Limit, Offset int }

// Real-time change notifications
type ChangeFeed interface {
    Subscribe(ctx context.Context, handler func(ChangeSignal)) error  // blocks until ctx cancelled
}

type ChangeSignal struct {
    Target        domain.Target
    Revision      domain.Revision
    ApplyBehavior domain.ApplyBehavior
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

// Application-specific runtime dependency builder (FULL rebuild)
type BundleFactory interface {
    Build(ctx context.Context, snap domain.Snapshot) (domain.RuntimeBundle, error)
}

// INCREMENTAL rebuild — extends BundleFactory with component-granular rebuilds.
// When the Supervisor detects only a subset of components changed, it calls
// BuildIncremental instead of Build, reusing unchanged components.
type IncrementalBundleFactory interface {
    BundleFactory
    BuildIncremental(ctx context.Context, snap domain.Snapshot,
        previous domain.RuntimeBundle, prevSnap domain.Snapshot) (domain.RuntimeBundle, error)
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

### 2.4 Registry (`commons/systemplane/registry/`)

Thread-safe in-memory registry of key definitions:

```go
type Registry interface {
    Register(def domain.KeyDef) error
    MustRegister(def domain.KeyDef)         // panics — startup only
    Get(key string) (domain.KeyDef, bool)
    List(kind domain.Kind) []domain.KeyDef  // sorted by key name
    Validate(key string, value any) error   // nil value always valid (reset)
}

func New() Registry   // returns *inMemoryRegistry (RWMutex-protected)
```

**Validation** (`validation.go`):
- `validateValue(def, value)` → type check + custom validator
- **JSON coercion**: `float64` without fractional part accepted as `int`; `int`/`int64` widened to `float`
- `isObjectCompatible(value)` uses `reflect.Map`; `isArrayCompatible(value)` uses `reflect.Array`/`Slice`
- Nil values always pass validation (they mean "reset to default")

### 2.5 Service Layer (`commons/systemplane/service/`)

#### Manager

```go
type Manager interface {
    GetConfigs(ctx context.Context) (ResolvedSet, error)
    GetSettings(ctx context.Context, subject Subject) (ResolvedSet, error)
    PatchConfigs(ctx context.Context, req PatchRequest) (WriteResult, error)
    PatchSettings(ctx context.Context, subject Subject, req PatchRequest) (WriteResult, error)
    GetConfigSchema(ctx context.Context) ([]SchemaEntry, error)
    GetSettingSchema(ctx context.Context) ([]SchemaEntry, error)
    GetConfigHistory(ctx context.Context, filter HistoryFilter) ([]HistoryEntry, error)
    GetSettingHistory(ctx context.Context, filter HistoryFilter) ([]HistoryEntry, error)
    ApplyChangeSignal(ctx context.Context, signal ChangeSignal) error
    Resync(ctx context.Context) error
}
```

**`ManagerConfig`** (constructor dependencies):

```go
type ManagerConfig struct {
    Registry             registry.Registry
    Store                ports.Store
    History              ports.HistoryStore
    Supervisor           Supervisor
    Builder              *SnapshotBuilder

    // ConfigWriteValidator is called BEFORE persistence with a preview snapshot.
    // Return non-nil error to reject the write.
    ConfigWriteValidator func(ctx context.Context, snapshot domain.Snapshot) error

    // StateSync is called AFTER successful writes for live-read escalation.
    // Used to update ConfigManager atomically.
    StateSync            func(ctx context.Context, snapshot domain.Snapshot)
}
```

**Key behaviors:**
- **Read path**: Uses supervisor's cached snapshot when available; falls back to builder
- **Write path**: `validate ops → preview snapshot (if ConfigWriteValidator configured) → escalate → store.Put → apply escalation`
- **Escalation application**: Maps strongest `ApplyBehavior` to supervisor method:
  - `ApplyBootstrapOnly` → no-op
  - `ApplyLiveRead` → `PublishSnapshot` + `StateSync` callback
  - `ApplyWorkerReconcile` → `ReconcileCurrent`
  - `ApplyBundleRebuild` / `ApplyBundleRebuildAndReconcile` → full `Reload`
- **Redaction**: All read results and history entries are redacted per `RedactPolicy`/`Secret` flag
- **Masking**: `RedactMask` shows last 4 runes; `RedactFull` and `Secret=true` show `"****"`

**Supporting types:**
- `Subject { Scope, SubjectID }` — settings target
- `PatchRequest { Ops []WriteOp, ExpectedRevision, Actor, Source }` — write input
- `WriteResult { Revision }` — write output
- `ResolvedSet { Values map[string]EffectiveValue, Revision }` — read output
- `SchemaEntry { Key, Kind, AllowedScopes, ValueType, DefaultValue, MutableAtRuntime, ApplyBehavior, Secret, RedactPolicy, Description, Group }` — schema metadata

#### Supervisor

```go
type Supervisor interface {
    Current() domain.RuntimeBundle
    Snapshot() domain.Snapshot
    PublishSnapshot(ctx context.Context, snap domain.Snapshot, reason string) error
    ReconcileCurrent(ctx context.Context, snap domain.Snapshot, reason string) error
    Reload(ctx context.Context, reason string, extraTenantIDs ...string) error
    Stop(ctx context.Context) error
}
```

**`SupervisorConfig`**:

```go
type SupervisorConfig struct {
    Builder     *SnapshotBuilder
    Factory     ports.BundleFactory
    Reconcilers []ports.BundleReconciler

    // Observer is invoked AFTER each successful reload with structured info
    // about the build strategy (full vs incremental).
    Observer func(ReloadEvent)
}

type ReloadEvent struct {
    Strategy BuildStrategy              // "full" or "incremental"
    Reason   string                     // caller-supplied (e.g., "changefeed-signal")
    Snapshot domain.Snapshot
    Bundle   domain.RuntimeBundle
}
```

**Reload lifecycle** (the heart of the system):

1. **Build snapshot**: `builder.BuildFull(ctx, tenantIDs...)`
2. **Build bundle**: Try incremental first (if factory implements `IncrementalBundleFactory` and previous exists), fall back to full
3. **Reconcile BEFORE commit**: Run all reconcilers against candidate while previous is still active (prevents state corruption on failure)
4. **Atomic swap**: `snapshot.Store(&snap)` + `bundle.Store(&holder{candidate})`
5. **Resource adoption**: If candidate implements `resourceAdopter`, call `AdoptResourcesFrom(previous)` to nil-out transferred pointers
6. **Observer callback**: Notify host application (e.g., update bundleState, swap loggers)
7. **Close previous**: `previous.Close()` — only tears down REPLACED components since transferred pointers are nil

**Hidden interfaces** (implement on your bundle for advanced patterns):
- `resourceAdopter { AdoptResourcesFrom(previous RuntimeBundle) }` — called after commit to mark transferred resources
- `rollbackDiscarder { Discard(ctx) error }` — called on failed candidate instead of `Close()`

#### SnapshotBuilder

```go
type SnapshotBuilder struct { registry Registry; store Store }

func NewSnapshotBuilder(reg, store) (*SnapshotBuilder, error)

func (b) BuildConfigs(ctx) (map[string]EffectiveValue, Revision, error)
func (b) BuildGlobalSettings(ctx) (map[string]EffectiveValue, Revision, error)
func (b) BuildSettings(ctx, Subject) (map[string]EffectiveValue, Revision, error)
func (b) BuildFull(ctx, tenantIDs ...string) (Snapshot, error)
```

**Override cascade** (3-layer for tenant settings):
1. `initDefaults(defs)` → populate from KeyDef.DefaultValue
2. `applyOverrides(effective, entries, source)` → overlay store entries
3. **Tenant path**: `registry defaults → global override → per-tenant override`

#### Escalation

```go
func Escalate(reg Registry, ops []WriteOp) (ApplyBehavior, []string, error)
```

- Returns strongest `ApplyBehavior` across all ops (by `Strength()` 0–4)
- Rejects `ApplyBootstrapOnly` and `MutableAtRuntime=false` keys
- Rejects duplicate keys in batch
- Empty batch → `ApplyLiveRead`
- Returns the list of keys that drove the escalation

### 2.6 Bootstrap (`commons/systemplane/bootstrap/`)

| File | Purpose |
|------|---------|
| `config.go` | `BootstrapConfig`, `PostgresBootstrapConfig`, `MongoBootstrapConfig`, `SecretStoreConfig` |
| `backend.go` | `BackendFactory` registry, `RegisterBackendFactory()`, `NewBackendFromConfig()` → `BackendResources{Store, History, ChangeFeed, Closer}` |
| `builtin/backend.go` | `init()` registers Postgres + MongoDB factories |
| `env.go` | `LoadFromEnv()` reads `SYSTEMPLANE_*` env vars |
| `classifier.go` | `IsBootstrapOnly(def)` — `!MutableAtRuntime || ApplyBootstrapOnly`; `IsRuntimeManaged(def)` — inverse |
| `defaults.go` | Default table/collection names |
| `postgres_identifiers.go` | Regex validation for PG identifiers (`^[a-z_][a-z0-9_]{0,62}$`) |

**Environment variables** for standalone systemplane backend:

| Variable | Default | Description |
|----------|---------|-------------|
| `SYSTEMPLANE_BACKEND` | — | `postgres` or `mongodb` |
| `SYSTEMPLANE_POSTGRES_DSN` | — | PostgreSQL DSN (falls back to app's PG DSN) |
| `SYSTEMPLANE_POSTGRES_SCHEMA` | `system` | Schema name |
| `SYSTEMPLANE_POSTGRES_ENTRIES_TABLE` | `runtime_entries` | Entries table |
| `SYSTEMPLANE_POSTGRES_HISTORY_TABLE` | `runtime_history` | History table |
| `SYSTEMPLANE_POSTGRES_REVISION_TABLE` | `runtime_revisions` | Revisions table |
| `SYSTEMPLANE_POSTGRES_NOTIFY_CHANNEL` | `systemplane_changes` | PG LISTEN/NOTIFY channel |
| `SYSTEMPLANE_SECRET_MASTER_KEY` | — | AES-256-GCM master key (32 bytes, raw or base64) |
| `SYSTEMPLANE_MONGODB_URI` | — | MongoDB connection URI |
| `SYSTEMPLANE_MONGODB_DATABASE` | `systemplane` | MongoDB database |
| `SYSTEMPLANE_MONGODB_ENTRIES_COLLECTION` | `runtime_entries` | Entries collection |
| `SYSTEMPLANE_MONGODB_HISTORY_COLLECTION` | `runtime_history` | History collection |
| `SYSTEMPLANE_MONGODB_WATCH_MODE` | `change_stream` | `change_stream` or `poll` |
| `SYSTEMPLANE_MONGODB_POLL_INTERVAL_SEC` | `5` | Poll interval (poll mode only) |

### 2.7 Adapters

| Adapter | Location | Key Feature |
|---------|----------|-------------|
| PostgreSQL Store | `adapters/store/postgres/` | 3 tables + indexes, optimistic concurrency, `pg_notify`, integer-preserving JSON decode |
| MongoDB Store | `adapters/store/mongodb/` | Sentinel revision doc (`__revision_meta__`), multi-doc transactions, requires replica set |
| Secret Codec | `adapters/store/secretcodec/` | AES-256-GCM with random nonce, AAD = `"kind\|scope\|subject\|key"`, envelope format `{__systemplane_secret_v: 1, alg, nonce, ciphertext}` |
| PostgreSQL ChangeFeed | `adapters/changefeed/postgres/` | LISTEN/NOTIFY, exponential backoff reconnect, revision resync on reconnect |
| MongoDB ChangeFeed | `adapters/changefeed/mongodb/` | Change stream or poll mode, auto-reconnect, revision-jump escalation |
| DebouncedFeed | `adapters/changefeed/debounce.go` | Per-target trailing-edge debounce (default 100ms + 50ms jitter), escalates to strongest ApplyBehavior in window |
| SafeInvokeHandler | `adapters/changefeed/safe_handler.go` | Catches handler panics, returns `ErrHandlerPanic` |
| Fiber HTTP | `adapters/http/fiber/` | 9 endpoints, DTOs, `If-Match`/`ETag` concurrency, middleware, domain→HTTP error mapping |
| Store Contract Tests | `adapters/store/storetest/` | 15 backend-agnostic tests: CRUD, concurrency, reset, history, pagination |
| Feed Contract Tests | `adapters/changefeed/feedtest/` | 3 backend-agnostic tests: signal receipt, cancellation, multiple signals |

**PostgreSQL Store DDL** (created automatically):
- `runtime_entries`: PK (`kind`, `scope`, `subject`, `key`), JSONB `value`, BIGINT `revision`
- `runtime_history`: BIGSERIAL `id`, `old_value`/`new_value` JSONB, `actor_id`, `changed_at`
- `runtime_revisions`: PK (`kind`, `scope`, `subject`), `revision` counter, `apply_behavior`

**HTTP Error Mapping**:

| Domain Error | HTTP Status | Code |
|-------------|-------------|------|
| `ErrKeyUnknown` | 400 | `system_key_unknown` |
| `ErrValueInvalid` | 400 | `system_value_invalid` |
| `ErrKeyNotMutable` | 400 | `system_key_not_mutable` |
| `ErrScopeInvalid` | 400 | `system_scope_invalid` |
| `ErrRevisionMismatch` | 409 | `system_revision_mismatch` |
| `ErrPermissionDenied` | 403 | `system_permission_denied` |
| `ErrReloadFailed` | 500 | `system_reload_failed` |
| `ErrSupervisorStopped` | 503 | `system_unavailable` |

### 2.8 Test Utilities (`commons/systemplane/testutil/`)

| Fake | Implements | Key Features |
|------|-----------|--------------|
| `FakeStore` | `ports.Store` | In-memory, optimistic concurrency, `Seed()` for pre-population |
| `FakeHistoryStore` | `ports.HistoryStore` | In-memory, newest-first, `Append()`/`AppendForKind()` |
| `FakeBundle` / `FakeBundleFactory` | `RuntimeBundle` / `BundleFactory` | Tracks Close state, `SetError()`, `CallCount()` |
| `FakeReconciler` | `BundleReconciler` | Configurable phase, records all `ReconcileCall`s, `SetError()` |
| `FakeIncrementalBundleFactory` | `IncrementalBundleFactory` | Embeds `FakeBundleFactory` + `IncrementalBuildFunc`, `IncrementalCallCount()` |

---

## 3. The Three Configuration Authorities

| Phase | Authority | Scope | Mutability |
|-------|-----------|-------|------------|
| **Bootstrap** | Env vars → `defaultConfig()` + `loadConfigFromEnv()` | Server address, TLS, auth, telemetry | Immutable after startup |
| **Runtime** | Systemplane Store + Supervisor | Rate limits, workers, timeouts, DB pools, CORS | Hot-reloadable via API |
| **Legacy bridge** | `ConfigManager.Get()` | Backward-compat for existing code | Updated by StateSync callback + observer |

**Single source of truth**: `{service}KeyDefs()` is THE canonical source of all
default values. The `defaultConfig()` function derives its values from KeyDefs
via `defaultSnapshotFromKeyDefs()` → `configFromSnapshot()`. No manual sync
required between defaults, key definitions, or struct tags.

**Component-granular awareness**: Every key's `Component` field (e.g., `"postgres"`,
`"redis"`, `"rabbitmq"`, `"s3"`, `"http"`, `"logger"`, or `ComponentNone`) enables
the `IncrementalBundleFactory` to rebuild only the affected infrastructure component
when that key changes, instead of tearing down and rebuilding everything.

---

## 4. Apply Behavior Taxonomy

Every config key MUST be classified with exactly one `ApplyBehavior`:

| ApplyBehavior | Code Constant | Strength | Runtime Effect | Use When |
|---------------|---------------|----------|----------------|----------|
| **bootstrap-only** | `domain.ApplyBootstrapOnly` | 4 | Immutable after startup. Never changes. | Server listen address, TLS, auth enable, telemetry endpoints |
| **live-read** | `domain.ApplyLiveRead` | 0 | Read from snapshot on every request. Zero cost. | Rate limits, timeouts, cache TTLs — anything read per-request |
| **worker-reconcile** | `domain.ApplyWorkerReconcile` | 1 | Reconciler restarts affected workers | Worker intervals, scheduler periods |
| **bundle-rebuild** | `domain.ApplyBundleRebuild` | 2 | Full bundle swap: new PG/Redis/RMQ/S3 clients | Connection strings, pool sizes, credentials |
| **bundle-rebuild+worker-reconcile** | `domain.ApplyBundleRebuildAndReconcile` | 3 | Bundle swap AND worker restart | Worker enable/disable (needs new connections + restart) |

**Strength** determines escalation: when a PATCH contains multiple keys with different
behaviors, `Escalate()` picks the strongest (highest number). If any key is
`ApplyBootstrapOnly` or `MutableAtRuntime=false`, the entire write is rejected.

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
| **Component** | Infrastructure component: `"postgres"`, `"redis"`, `"rabbitmq"`, `"s3"`, `"http"`, `"logger"`, or `ComponentNone` (`"_none"`) for pure business-logic keys |

### Step 3: Derive defaultConfig from KeyDefs (Single Source of Truth)

The `{service}KeyDefs()` function IS the canonical source of all defaults. The
`defaultConfig()` function derives from it — no manual struct literal:

```go
// config_defaults.go — derived from KeyDefs, not manually maintained
func defaultConfig() *Config {
    return configFromSnapshot(defaultSnapshotFromKeyDefs({service}KeyDefs()))
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
// bootstrap_only_config.go (part of systemplane_factory.go in matcher)
type BootstrapOnlyConfig struct {
    EnvName                string
    ServerAddress          string
    TLSCertFile            string
    TLSKeyFile             string
    TLSTerminatedUpstream  bool
    TrustedProxies         string
    AuthEnabled            bool
    AuthHost               string
    AuthTokenSecret        string
    TelemetryEnabled       bool
    TelemetryServiceName   string
    // ... other immutable keys
}

func ExtractBootstrapOnlyConfig(cfg *Config) BootstrapOnlyConfig {
    return BootstrapOnlyConfig{
        EnvName:       cfg.App.EnvName,
        ServerAddress: cfg.Server.Address,
        // ...
    }
}
```

### Step 5: Register All Keys

Create key definition files. For large services (100+ keys), split into focused
sub-files by group (Matcher uses 9 sub-files):

```go
// systemplane_keys.go — orchestrator
func Register{Service}Keys(reg registry.Registry) error {
    for _, def := range {service}KeyDefs() {
        if err := reg.Register(def); err != nil {
            return fmt.Errorf("register key %q: %w", def.Key, err)
        }
    }
    return nil
}

func {service}KeyDefs() []domain.KeyDef {
    return concatKeyDefs(
        {service}KeyDefsAppServer(),
        {service}KeyDefsPostgres(),
        {service}KeyDefsMessaging(),
        {service}KeyDefsRuntimeHTTP(),
        {service}KeyDefsRuntimeServices(),
        {service}KeyDefsStorageExport(),
        {service}KeyDefsWorkers(),
        // ... more groups
    )
}

func concatKeyDefs(groups ...[]domain.KeyDef) []domain.KeyDef {
    n := 0
    for _, g := range groups { n += len(g) }
    result := make([]domain.KeyDef, 0, n)
    for _, g := range groups { result = append(result, g...) }
    return result
}
```

Each sub-file contains key definitions with the `Component` field:

```go
// systemplane_keys_postgres.go
func {service}KeyDefsPostgres() []domain.KeyDef {
    return []domain.KeyDef{
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
        // ...
    }
}
```

**Validator functions** (separate file `systemplane_keys_validation.go`):

```go
func validatePositiveInt(value any) error {
    n, ok := toInt(value)
    if !ok { return fmt.Errorf("expected integer, got %T", value) }
    if n <= 0 { return fmt.Errorf("must be positive, got %d", n) }
    return nil
}

func validateLogLevel(value any) error { /* check against allowed levels */ }
func validateSSLMode(value any) error  { /* check against PG SSL modes */ }
func validateAbsoluteHTTPURL(value any) error { /* URL parsing check */ }
```

### Step 6: Build the Bundle and BundleFactory

Define what runtime resources the service manages. **Critical**: implement
`IncrementalBundleFactory` for component-granular rebuilds.

```go
// systemplane_bundle.go
type {Service}Bundle struct {
    Infra  *InfraBundle
    HTTP   *HTTPPolicyBundle
    Logger *LoggerBundle

    // Ownership tracking — prevents double-free of shared resources
    ownsPostgres, ownsRedis, ownsRabbitMQ, ownsObjectStorage bool
}

type InfraBundle struct {
    Postgres      *libPostgres.Client
    Redis         *libRedis.Client
    RabbitMQ      *libRabbitmq.RabbitMQConnection
    ObjectStorage io.Closer
}

type HTTPPolicyBundle struct {
    BodyLimitBytes     int
    CORSAllowedOrigins string
    CORSAllowedMethods string
    CORSAllowedHeaders string
}

type LoggerBundle struct { Logger libLog.Logger; Level string }

func (b *{Service}Bundle) Close(ctx context.Context) error {
    // Close in REVERSE dependency order, only owned resources:
    // Logger → ObjectStorage → RabbitMQ → Redis → Postgres
    if b.ownsObjectStorage && b.Infra.ObjectStorage != nil { b.Infra.ObjectStorage.Close() }
    if b.ownsRabbitMQ && b.Infra.RabbitMQ != nil { b.Infra.RabbitMQ.Close() }
    if b.ownsRedis && b.Infra.Redis != nil { b.Infra.Redis.Close() }
    if b.ownsPostgres && b.Infra.Postgres != nil { b.Infra.Postgres.Close() }
    return nil
}

// AdoptResourcesFrom is called by the Supervisor after commit.
// Nil-out transferred pointers in previous so previous.Close()
// does NOT close adopted components.
func (b *{Service}Bundle) AdoptResourcesFrom(previous domain.RuntimeBundle) {
    prev, ok := previous.(*{Service}Bundle)
    if !ok || prev == nil { return }
    // Example: if we reused postgres from prev, nil it out in prev
    if !b.ownsPostgres { prev.Infra.Postgres = nil }
    if !b.ownsRedis { prev.Infra.Redis = nil }
    // ... etc
}
```

**BundleFactory with incremental builds**:

```go
// systemplane_factory.go
type {Service}BundleFactory struct {
    bootstrapCfg *BootstrapOnlyConfig
}

// Full rebuild — creates everything from scratch
func (f *{Service}BundleFactory) Build(ctx context.Context, snap domain.Snapshot) (domain.RuntimeBundle, error) {
    logger := f.buildLogger(snap)
    infra, err := f.buildAllInfra(ctx, snap, logger)
    if err != nil { return nil, err }
    http := f.buildHTTPPolicy(snap)
    return &{Service}Bundle{
        Infra: infra, HTTP: http, Logger: &LoggerBundle{Logger: logger},
        ownsPostgres: true, ownsRedis: true, ownsRabbitMQ: true, ownsObjectStorage: true,
    }, nil
}

// Incremental rebuild — only rebuild changed components
func (f *{Service}BundleFactory) BuildIncremental(ctx context.Context, snap domain.Snapshot,
    previous domain.RuntimeBundle, prevSnap domain.Snapshot) (domain.RuntimeBundle, error) {

    prev := previous.(*{Service}Bundle)
    changed := f.diffChangedComponents(snap, prevSnap) // uses keyComponentMap

    bundle := &{Service}Bundle{}
    if changed["postgres"] { bundle.Infra.Postgres = f.buildPostgres(ctx, snap); bundle.ownsPostgres = true }
    else { bundle.Infra.Postgres = prev.Infra.Postgres; bundle.ownsPostgres = false }
    // ... repeat for each component
    return bundle, nil
}
```

**The `keyComponentMap`**: Built once from `{service}KeyDefs()` at factory construction
time. Maps each key to its `Component` field. When diffing snapshots, the factory
checks which keys changed and collects the set of affected components.

### Step 7: Implement Reconcilers

Build reconcilers for side effects on config changes. Each reconciler declares
its `Phase()` — the supervisor sorts by phase before execution:

```go
// HTTP Policy — PhaseValidation (gate that can reject changes)
type HTTPPolicyReconciler struct{}
func (r *HTTPPolicyReconciler) Name() string { return "http-policy" }
func (r *HTTPPolicyReconciler) Phase() domain.ReconcilerPhase { return domain.PhaseValidation }
func (r *HTTPPolicyReconciler) Reconcile(ctx context.Context, _, candidate domain.RuntimeBundle, _ domain.Snapshot) error {
    bundle := candidate.(*{Service}Bundle)
    if bundle.HTTP.BodyLimitBytes < 0 { return fmt.Errorf("body limit must be non-negative") }
    if bundle.HTTP.CORSAllowedOrigins == "" { return fmt.Errorf("CORS origins required") }
    return nil
}

// Publisher — PhaseValidation (ensures RabbitMQ channels are connected)
type PublisherReconciler struct {
    // holds references to SwappablePublisher instances
}
func (r *PublisherReconciler) Name() string { return "publisher" }
func (r *PublisherReconciler) Phase() domain.ReconcilerPhase { return domain.PhaseValidation }
func (r *PublisherReconciler) Reconcile(ctx context.Context, prev, candidate domain.RuntimeBundle, _ domain.Snapshot) error {
    // When RabbitMQ connection changed, create staged publishers
    // on the candidate bundle. The observer callback swaps them in later.
    return nil
}

// Worker — PhaseSideEffect (external side effects, runs last)
type WorkerReconciler struct {
    workerManager *WorkerManager
}
func (r *WorkerReconciler) Name() string { return "worker" }
func (r *WorkerReconciler) Phase() domain.ReconcilerPhase { return domain.PhaseSideEffect }
func (r *WorkerReconciler) Reconcile(ctx context.Context, _, _ domain.RuntimeBundle, snap domain.Snapshot) error {
    cfg := snapshotToWorkerConfig(snap)
    return r.workerManager.ApplyConfig(cfg)
}
```

**Phase ordering is enforced by the type system** — you cannot register a
reconciler without declaring its phase. The supervisor stable-sorts by phase,
so reconcilers within the same phase retain their registration order.

### Step 8: Wire Identity and Authorization

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
}
func (a *{Service}Authorizer) Authorize(ctx context.Context, permission string) error {
    if !a.authEnabled { return nil }
    // Map permission to RBAC action, call auth.Authorize()
}
```

### Step 9: Build the Init Function

```go
// systemplane_init.go
func Init{Service}Systemplane(ctx context.Context, cfg *Config, configManager *ConfigManager,
    workerManager *WorkerManager, logger log.Logger,
    observer func(service.ReloadEvent)) (*SystemplaneComponents, error) {

    // 1. Extract bootstrap-only config
    bootstrapCfg := ExtractBootstrapOnlyConfig(cfg)

    // 2. Load backend config (default: reuse app's Postgres DSN)
    backendCfg := Load{Service}BackendConfig(cfg)

    // 3. Create registry + register all keys
    reg := registry.New()
    if err := Register{Service}Keys(reg); err != nil {
        return nil, err
    }

    // 4. Configure backend with registry metadata (secret keys + apply behaviors)
    configureBackendWithRegistry(backendCfg, reg)

    // 5. Create backend (Store + History + ChangeFeed)
    backend, err := builtin.NewBackendFromConfig(ctx, backendCfg)
    if err != nil { return nil, fmt.Errorf("systemplane backend: %w", err) }

    // 6. Create snapshot builder
    snapBuilder, err := service.NewSnapshotBuilder(reg, backend.Store)
    if err != nil { backend.Close(); return nil, err }

    // 7. Create bundle factory (supports incremental builds)
    bundleFactory := New{Service}BundleFactory(&bootstrapCfg)

    // 8. Seed store from current env-var config
    if err := seedStoreForInitialReload(ctx, configManager, reg, backend.Store); err != nil {
        backend.Close(); return nil, err
    }

    // 9. Build reconcilers (phase-sorted by supervisor)
    reconcilers := []ports.BundleReconciler{
        NewHTTPPolicyReconciler(),
        NewPublisherReconciler(/* swappable publishers */),
        NewWorkerReconciler(workerManager),
    }

    // 10. Create supervisor + initial reload
    supervisor, err := service.NewSupervisor(service.SupervisorConfig{
        Builder:     snapBuilder,
        Factory:     bundleFactory,
        Reconcilers: reconcilers,
        Observer:    observer,  // host app's reload callback
    })
    if err != nil { backend.Close(); return nil, err }

    if err := supervisor.Reload(ctx, "initial-bootstrap"); err != nil {
        backend.Close(); return nil, err
    }

    // 11. Create manager with callbacks
    baseCfg := configManager.Get()
    manager, err := service.NewManager(service.ManagerConfig{
        Registry:   reg,
        Store:      backend.Store,
        History:    backend.History,
        Supervisor: supervisor,
        Builder:    snapBuilder,
        // Pre-write validation: reject invalid configs before persistence
        ConfigWriteValidator: func(_ context.Context, snap domain.Snapshot) error {
            candidateCfg := snapshotToFullConfig(snap, baseCfg)
            return candidateCfg.Validate()
        },
        // Post-write sync for live-read keys
        StateSync: func(_ context.Context, snap domain.Snapshot) {
            newCfg := snapshotToFullConfig(snap, baseCfg)
            configManager.swapConfig(newCfg)
        },
    })
    if err != nil { backend.Close(); return nil, err }

    return &SystemplaneComponents{
        ChangeFeed: backend.ChangeFeed,
        Supervisor: supervisor,
        Manager:    manager,
        Backend:    backend.Closer,
    }, nil
}
```

### Step 10: Mount HTTP API, Start ChangeFeed, and Wire Active Bundle State

```go
// systemplane_mount.go
func MountSystemplaneAPI(app *fiber.App, manager service.Manager,
    authEnabled bool) error {

    authorizer := &{Service}Authorizer{authEnabled: authEnabled}
    identity := &{Service}IdentityResolver{}

    handler := fiberhttp.NewHandler(manager, authorizer, identity)
    handler.Mount(app.Group("/v1/system"))
    return nil
}

// In init.go — start debounced change feed subscriber
debouncedFeed := changefeed.NewDebouncedFeed(
    spComponents.ChangeFeed,
    changefeed.WithWindow(200 * time.Millisecond),
)

feedCtx, cancelFeed := context.WithCancel(ctx)
go func() {
    _ = debouncedFeed.Subscribe(feedCtx, func(signal ports.ChangeSignal) {
        _ = spComponents.Manager.ApplyChangeSignal(feedCtx, signal)
    })
}()

// Active bundle state — live-read accessor for infrastructure consumers
type activeMatcherBundleState struct {
    mu     sync.RWMutex
    bundle *{Service}Bundle
}
func (s *activeMatcherBundleState) Current() *{Service}Bundle { /* RLock + return */ }
func (s *activeMatcherBundleState) Update(b *{Service}Bundle) { /* Lock + store */ }

// Observer callback (passed to InitSystemplane):
runtimeReloadObserver := func(event service.ReloadEvent) {
    bundle := event.Bundle.(*{Service}Bundle)
    bundleState.Update(bundle)
    configManager.UpdateFromSystemplane(event.Snapshot)
    // Swap logger, publishers, etc.
}
```

**Shutdown sequence** (in `service.go`):

```go
func (s *Service) Stop(ctx context.Context) {
    s.configManager.Stop()              // 1. Prevent mutations
    s.cancelChangeFeed()                // 2. Stop change feed BEFORE supervisor
    s.spComponents.Supervisor.Stop(ctx) // 3. Stop supervisor + close bundle
    s.spComponents.Backend.Close()      // 4. Close store connection
    s.workerManager.Stop()              // 5. Stop workers
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
- Database connection strings (PG host, port, user, password) → `Component: "postgres"`
- Redis connection (host, master name, password) → `Component: "redis"`
- RabbitMQ connection (URI, host, port, user, password) → `Component: "rabbitmq"`
- Object storage (endpoint, bucket, credentials) → `Component: "s3"`
- These become `ApplyBundleRebuild` keys in systemplane

**Bootstrap-Only (env-var, immutable after startup)**:
- Server listen address and port → `Component: ComponentNone` (not mutable)
- TLS certificate paths
- Auth enable/disable
- Auth service address
- Telemetry endpoints (OTEL collector)
- These become `ApplyBootstrapOnly` keys

**Application Runtime (hot-reloadable)**:
- Rate limits → `Component: ComponentNone` + `ApplyLiveRead`
- Worker intervals and enable/disable flags → `Component: ComponentNone` + `ApplyWorkerReconcile`
- Timeouts (webhook, health check) → `Component: ComponentNone` + `ApplyLiveRead`
- Feature flags → `Component: ComponentNone` + `ApplyLiveRead`
- Cache TTLs → `Component: ComponentNone` + `ApplyLiveRead`

### 6.3 Identify Runtime Dependencies (Bundle Candidates)

List all infrastructure clients that the service creates at startup:

```bash
# Find client constructors
grep -rn 'libPostgres.New\|libRedis.New\|libRabbitmq.New\|storage.New' internal/ --include='*.go'

# Find connection pools
grep -rn 'sql.Open\|pgx\|redis.New\|amqp.Dial' internal/ --include='*.go'
```

Each of these becomes a field in the `InfraBundle` and a component name in
`keyComponentMap`.

### 6.4 Identify Background Workers

```bash
# Find worker patterns
grep -rn 'ticker\|time.NewTicker\|cron\|worker\|scheduler' internal/ --include='*.go' | grep -v _test.go
```

Each worker with configurable intervals becomes a `WorkerReconciler` candidate.

### 6.5 Generate the Key Inventory

Create a table with columns:
- Key name (dotted: `postgres.primary_host`)
- Current env var (`POSTGRES_HOST`)
- Default value
- Type (`string`, `int`, `bool`, `float`)
- Kind (`config` or `setting`)
- Scope (`global` or `tenant`)
- ApplyBehavior
- Component (`postgres`, `redis`, `rabbitmq`, `s3`, `http`, `logger`, `_none`)
- Secret (yes/no)
- MutableAtRuntime (yes/no)
- Group
- Validator (if any)

---

## 7. Implementing the Migration

### 7.1 Files to Create (per service)

Key files organized by function. Reference is Matcher's `internal/bootstrap/`:

**Core Systemplane Wiring:**

| File | Purpose | Matcher Reference |
|------|---------|-------------------|
| `systemplane_init.go` | Init function with 11-step boot + change feed start | `internal/bootstrap/systemplane_init.go` |
| `systemplane_mount.go` | HTTP route registration | `internal/bootstrap/systemplane_mount.go` |

**Key Definitions (split by group for large services):**

| File | Purpose | Matcher Reference |
|------|---------|-------------------|
| `systemplane_keys.go` | Orchestrator: `Register{Service}Keys()` + `{service}KeyDefs()` | `internal/bootstrap/systemplane_keys.go` |
| `systemplane_keys_app_server.go` | App env + server HTTP + TLS keys | `internal/bootstrap/systemplane_keys_app_server.go` |
| `systemplane_keys_tenancy.go` | Tenant identity + connectivity + resilience | `internal/bootstrap/systemplane_keys_tenancy.go` |
| `systemplane_keys_postgres.go` | Primary + replica + pooling + operations | `internal/bootstrap/systemplane_keys_postgres.go` |
| `systemplane_keys_messaging.go` | Redis core + runtime + RabbitMQ connection + health | `internal/bootstrap/systemplane_keys_messaging.go` |
| `systemplane_keys_runtime_http.go` | Auth + swagger + telemetry + rate limit | `internal/bootstrap/systemplane_keys_runtime_http.go` |
| `systemplane_keys_runtime_services.go` | Infrastructure + idempotency + fetcher | `internal/bootstrap/systemplane_keys_runtime_services.go` |
| `systemplane_keys_storage_export.go` | Deduplication + object storage + export worker | `internal/bootstrap/systemplane_keys_storage_export.go` |
| `systemplane_keys_workers.go` | Webhook + cleanup worker | `internal/bootstrap/systemplane_keys_workers.go` |
| `systemplane_keys_archival.go` | Scheduler + archival lifecycle + storage + runtime | `internal/bootstrap/systemplane_keys_archival.go` |
| `systemplane_keys_validation.go` | Validator functions used by KeyDef.Validator | `internal/bootstrap/systemplane_keys_validation.go` |
| `systemplane_keys_helpers.go` | `concatKeyDefs()` utility | `internal/bootstrap/systemplane_keys_helpers.go` |

**Bundle + Factory:**

| File | Purpose | Matcher Reference |
|------|---------|-------------------|
| `systemplane_bundle.go` | Bundle struct + Close + AdoptResourcesFrom (ownership tracking) | `internal/bootstrap/systemplane_bundle.go` |
| `systemplane_factory.go` | BundleFactory + IncrementalBundleFactory (full + incremental) | `internal/bootstrap/systemplane_factory.go` |
| `systemplane_factory_infra.go` | Per-component builders: buildPostgres, buildRedis, buildRabbitMQ, buildS3 | `internal/bootstrap/systemplane_factory_infra.go` |

**Reconcilers:**

| File | Purpose | Matcher Reference |
|------|---------|-------------------|
| `systemplane_reconciler_http.go` | HTTP policy validation (PhaseValidation) | `internal/bootstrap/systemplane_reconciler_http.go` |
| `systemplane_reconciler_publishers.go` | RabbitMQ publisher staging (PhaseValidation) | `internal/bootstrap/systemplane_reconciler_publishers.go` |
| `systemplane_reconciler_worker.go` | Worker restart (PhaseSideEffect) | `internal/bootstrap/systemplane_reconciler_worker.go` |

**Identity + Authorization:**

| File | Purpose | Matcher Reference |
|------|---------|-------------------|
| `systemplane_identity.go` | JWT → Actor bridge | `internal/bootstrap/systemplane_identity.go` |
| `systemplane_authorizer.go` | Permission mapping | `internal/bootstrap/systemplane_authorizer.go` |

**Config Manager Integration:**

| File | Purpose | Matcher Reference |
|------|---------|-------------------|
| `config_manager_systemplane.go` | Snapshot → Config hydration (`configFromSnapshot`, `snapshotToFullConfig`) | `internal/bootstrap/config_manager_systemplane.go` |
| `config_manager_seed.go` | Env → Store one-time seed (`SeedStore`, `buildSeedOps`) | `internal/bootstrap/config_manager_seed.go` |
| `config_manager_helpers.go` | Type-safe value comparison (`valuesEquivalent`) | `internal/bootstrap/config_manager_helpers.go` |
| `config_validation.go` | Production config guards (used by ConfigWriteValidator) | `internal/bootstrap/config_validation.go` |

**Runtime Integration:**

| File | Purpose | Matcher Reference |
|------|---------|-------------------|
| `active_bundle_state.go` | Thread-safe live-read accessor for current bundle | `internal/bootstrap/active_bundle_state.go` |
| `config/.config-map.example` | Bootstrap-only key reference (operators) | `config/.config-map.example` |

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
| `config_defaults.go` | Derive from `{service}KeyDefs()` via `configFromSnapshot(defaultSnapshotFromKeyDefs(...))` |
| `config_manager.go` | Add `UpdateFromSystemplane()`, `enterSeedMode()`, `atomic.Pointer[Config]` for lock-free reads |
| `init.go` | Wire systemplane init after workers, before HTTP mount; add reload observer callback |
| `service.go` | Add systemplane shutdown sequence (5-step) |
| `docker-compose.yml` | Remove `env_file`, inline defaults with `${VAR:-default}` |
| `Makefile` | Remove `set-env`, `clear-envs` targets |

### 7.4 The Snapshot→Config Hydration Function

This is the most labor-intensive part — mapping every snapshot key back to the
Config struct. Pattern from Matcher:

```go
// config_manager_systemplane.go

// configFromSnapshot builds a Config entirely from snapshot values.
// ALL fields come from the snapshot — no bootstrap overlay.
func configFromSnapshot(snap domain.Snapshot) *Config {
    cfg := &Config{}

    cfg.Postgres.PrimaryHost = snapString(snap, "postgres.primary_host", defaultPostgresHost)
    cfg.Postgres.PrimaryPort = snapInt(snap, "postgres.primary_port", defaultPostgresPort)
    cfg.Postgres.MaxOpenConns = snapInt(snap, "postgres.max_open_connections", defaultMaxOpenConns)
    cfg.Redis.Host = snapString(snap, "redis.host", defaultRedisHost)
    cfg.RateLimitMax = snapInt(snap, "rate_limit.max", defaultRateLimitMax)
    // ... every runtime key (~170 lines in Matcher)

    return cfg
}

// snapshotToFullConfig hydrates from snapshot, then overlays bootstrap-only
// fields from the previous config (they never change at runtime).
func snapshotToFullConfig(snap domain.Snapshot, oldCfg *Config) *Config {
    cfg := configFromSnapshot(snap)

    // Copy bootstrap-only fields that systemplane cannot change
    cfg.App.EnvName = oldCfg.App.EnvName
    cfg.Server.Address = oldCfg.Server.Address
    cfg.Auth = oldCfg.Auth           // entire auth section is bootstrap-only
    cfg.Telemetry = oldCfg.Telemetry // entire telemetry section
    cfg.Idempotency.HMACSecret = oldCfg.Idempotency.HMACSecret

    return cfg
}
```

**Helper functions** for type-safe extraction (with JSON coercion):

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

### 7.5 The Active Bundle State Pattern

Infrastructure consumers (health checks, dynamic providers) need the **current**
bundle on every request. Use a thread-safe accessor:

```go
// active_bundle_state.go
type activeBundleState struct {
    mu     sync.RWMutex
    bundle *{Service}Bundle
}

func (s *activeBundleState) Current() *{Service}Bundle {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.bundle
}

func (s *activeBundleState) Update(b *{Service}Bundle) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.bundle = b
}

// Used by infrastructure providers:
func currentPostgresClient() *libPostgres.Client {
    if bundle := bundleState.Current(); bundle != nil && bundle.Infra.Postgres != nil {
        return bundle.Infra.Postgres
    }
    return originalPostgresClient // fallback to init-time client
}
```

---

## 8. HTTP API Endpoints

After migration, the service exposes these endpoints:

| Method | Path | Description | Auth Permission |
|--------|------|-------------|-----------------|
| `GET` | `/v1/system/configs` | View all resolved config values | `system/configs:read` |
| `PATCH` | `/v1/system/configs` | Update config values (with `If-Match` for concurrency) | `system/configs:write` |
| `GET` | `/v1/system/configs/schema` | View all key definitions (types, defaults, mutability) | `system/configs/schema:read` |
| `GET` | `/v1/system/configs/history` | Audit trail of config changes | `system/configs/history:read` |
| `POST` | `/v1/system/configs/reload` | Force a full reload | `system/configs/reload:write` |
| `GET` | `/v1/system/settings` | View resolved settings (`?scope=global\|tenant`) | `system/settings:read` |
| `PATCH` | `/v1/system/settings` | Update settings | `system/settings:write` |
| `GET` | `/v1/system/settings/schema` | View setting key definitions | `system/settings/schema:read` |
| `GET` | `/v1/system/settings/history` | Settings audit trail | scope-dependent |

Settings routes have an extra `settingsScopeAuthorization` middleware that
elevates to `system/settings/global:{action}` when `?scope=global` is queried.

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

    configs := reg.List(domain.KindConfig)
    settings := reg.List(domain.KindSetting)
    assert.Greater(t, len(configs)+len(settings), 0)
}

func TestRegister{Service}Keys_DefaultsMatchConfig(t *testing.T) {
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

func TestBundleFactory_BuildIncremental_ReusesUnchangedComponents(t *testing.T) {
    snap1 := buildTestSnapshot(t)
    snap2 := modifySnapshot(snap1, "rate_limit.max", 500) // only changes _none component

    factory := New{Service}BundleFactory(testBootstrapConfig())
    prev, _ := factory.Build(ctx, snap1)

    candidate, _ := factory.BuildIncremental(ctx, snap2, prev, snap1)
    b := candidate.(*{Service}Bundle)

    // Postgres should be reused (not owned by candidate)
    assert.False(t, b.ownsPostgres)
}
```

### 9.3 Reconciler Tests

```go
func TestWorkerReconciler_AppliesConfig(t *testing.T) {
    wm := NewTestWorkerManager()
    reconciler := NewWorkerReconciler(wm)

    snap := buildModifiedSnapshot(t, "export_worker.enabled", true)
    err := reconciler.Reconcile(context.Background(), nil, nil, snap)
    require.NoError(t, err)

    assert.True(t, wm.LastAppliedConfig().ExportWorkerEnabled)
}
```

### 9.4 Contract Tests (Backend-Agnostic)

Run the `storetest` and `feedtest` contract suites against your backend:

```go
func TestPostgresStore_ContractSuite(t *testing.T) {
    store, history := setupPostgresStore(t) // testcontainers
    storetest.RunAll(t, /* factory that returns store+history */)
}

func TestPostgresChangeFeed_ContractSuite(t *testing.T) {
    feed := setupPostgresFeed(t) // testcontainers
    feedtest.RunAll(t, /* factory that returns store+feed */)
}
```

**Store contract tests (15):**
- GetEmptyTarget, PutSingleOp, PutBatch, OptimisticConcurrency, ResetOp
- NilValueOp, EmptyBatchIsNoOp, TypedNilValueOp, PutPreservesOtherKeys
- RevisionMonotonicallyIncreasing, ConcurrentPuts
- HistoryRecording, BatchHistoryConsistency, HistoryFiltering, HistoryPagination

**Feed contract tests (3):**
- SubscribeReceivesSignal, ContextCancellationStops, MultipleSignals

### 9.5 ConfigWriteValidator Tests

```go
func TestConfigWriteValidator_RejectsInvalidConfig(t *testing.T) {
    // Test that production guards prevent persisting invalid config
    snap := buildSnapshot(t)
    snap.Configs["rate_limit.enabled"] = ev(false) // can't disable in production

    err := validator(context.Background(), snap)
    assert.Error(t, err)
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
| Full restart for any change | Only `ApplyBootstrapOnly` keys need restart |

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

# Secret encryption (REQUIRED — rejected in production without it):
SYSTEMPLANE_SECRET_MASTER_KEY=<32-byte key, raw or base64>
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

The Matcher service registers **~130 keys** across 20 groups, split into 9 focused
sub-files. Here's the breakdown by group and ApplyBehavior:

| Group | Keys | BootstrapOnly | BundleRebuild | LiveRead | WorkerReconcile | Rebuild+Reconcile |
|-------|------|---------------|---------------|----------|-----------------|-------------------|
| `app` | 2 | 1 | 1 | - | - | - |
| `server` | 8 | 4 | 4 | - | - | - |
| `tenancy` | 11 | - | 11 | - | - | - |
| `postgres` | 19 | - | 18 | 1 | - | - |
| `redis` | 12 | - | 12 | - | - | - |
| `rabbitmq` | 8 | - | 8 | - | - | - |
| `auth` | 3 | 3 | - | - | - | - |
| `swagger` | 3 | - | 3 | - | - | - |
| `telemetry` | 7 | 7 | - | - | - | - |
| `rate_limit` | 7 | - | - | 7 | - | - |
| `infrastructure` | 2 | - | 1 | 1 | - | - |
| `idempotency` | 3 | 1 | 2 | - | - | - |
| `callback_rate_limit` | 1 | - | - | 1 | - | - |
| `fetcher` | 9 | - | 4 | - | 3 | 2 |
| `deduplication` | 1 | - | - | 1 | - | - |
| `object_storage` | 6 | - | 6 | - | - | - |
| `export_worker` | 4 | - | - | 1 | 1 | 2 |
| `webhook` | 1 | - | - | 1 | - | - |
| `cleanup_worker` | 4 | - | - | - | 2 | 2 |
| `scheduler` | 1 | - | - | - | 1 | - |
| `archival` | 12 | - | - | 1 | 3 | 8 |

**Secrets**: ~9 keys with `RedactFull` policy (passwords, tokens, certificates, access keys).

**Components referenced**: `postgres`, `redis`, `rabbitmq`, `s3`, `http`, `logger`, `_none`.

---

## Appendix B: Key Sub-File Organization (Reference)

| Sub-File | Groups Covered | Key Count |
|----------|---------------|-----------|
| `systemplane_keys_app_server.go` | app, server | ~11 |
| `systemplane_keys_tenancy.go` | tenancy | ~11 |
| `systemplane_keys_postgres.go` | postgres | ~19 |
| `systemplane_keys_messaging.go` | redis, rabbitmq | ~20 |
| `systemplane_keys_runtime_http.go` | auth, swagger, telemetry, rate_limit | ~17 |
| `systemplane_keys_runtime_services.go` | infrastructure, idempotency, callback_rate_limit, fetcher | ~14 |
| `systemplane_keys_storage_export.go` | deduplication, object_storage, export_worker | ~11 |
| `systemplane_keys_workers.go` | webhook, cleanup_worker | ~5 |
| `systemplane_keys_archival.go` | scheduler, archival | ~12 |

---

## Appendix C: Quick Reference Commands

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
