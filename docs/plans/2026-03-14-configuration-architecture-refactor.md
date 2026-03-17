# Configuration Architecture Refactor Implementation Plan

## Goal

Replace Matcher's current YAML/Viper-driven runtime configuration model with a reusable `pkg/systemplane` library that:

- separates **bootstrap/platform config** from **runtime configs/settings**
- supports **PostgreSQL and MongoDB** backends through adapters
- owns the standard HTTP transport for system configuration management
- can atomically rebuild and swap runtime dependency bundles without process restart
- keeps the **last known good** runtime bundle active if a reload fails

Matcher is not public yet, so this plan assumes a **clean naming cutover** instead of preserving legacy API names or compatibility shims.

---

## Current Matcher Reality We Must Design Around

Today's codebase has three different concepts that are easy to conflate:

1. **System runtime config (local, YAML/Viper-driven today)**
   - Routes live in `internal/bootstrap/config_api_routes.go`
   - Handlers live in `internal/bootstrap/config_api.go`
   - DTOs live in `internal/bootstrap/config_api_dto.go`
   - Current endpoints are singular: `/v1/system/config`, `/schema`, `/history`, `/reload`
   - Runtime authority today is the local `ConfigManager` + YAML/Viper/file watcher

2. **Business configuration (bounded-context data, keep separate)**
   - Routes live in `internal/configuration/adapters/http/routes.go`
   - Endpoints live under `/v1/config/...`
   - This includes contexts, sources, field maps, rules, schedules, fee schedules
   - This is domain configuration, not generic system config

3. **Remote tenant settings call (outbound integration, not a local API)**
   - Current outbound call lives in `internal/shared/infrastructure/tenant/adapters/remote_configuration.go`
   - URL is `/tenants/{tenant}/services/{service}/settings`
   - Matcher does **not** currently expose a local `/settings` endpoint
   - Despite the route name, this surface currently resolves **tenant infrastructure wiring** such as Postgres and Redis settings, not generic local runtime settings
   - Matcher consumes it only for multi-tenant infrastructure resolution through the tenant connection manager

This refactor must keep those concerns distinct.

---

## Terminology and Ownership

### `configs`

`configs` are **admin/system-level runtime configuration**.

Examples:
- feature switches with system-wide effect
- runtime limits
- worker intervals
- connector behavior that is safe to hot-apply

Properties:
- generally global/system scope
- more privileged mutation path
- often affect shared services or background workers

### `settings`

`settings` are **operator-safe tenant/runtime settings**.

Examples:
- tenant-visible operational toggles
- non-destructive runtime preferences
- tenant-scoped behavior that does not require platform re-bootstrap

Properties:
- may be tenant-scoped
- must never require app code to hardcode roles
- must use action/permission checks, not role names

### What does **not** move into `pkg/systemplane`

- reconciliation contexts
- sources
- field maps
- match rules
- fee schedules
- schedules
- any other bounded-context business configuration currently exposed under `/v1/config/...` and slated for route renaming away from that prefix

---

## Non-Negotiable Architectural Decisions

1. **The extraction target is `pkg/systemplane`, not `pkg/config`.**
   - `pkg/systemplane` must not import `internal/...`.
   - It must be reusable by products/plugins that are not Matcher.

2. **The core library is backend-agnostic.**
   - The core defines domain types, registry, validation, runtime snapshot/bundle logic, supervisor, and transport contracts.
   - PostgreSQL and MongoDB implementations live in adapters.

3. **PostgreSQL and MongoDB are both first-class supported backends.**
   - PostgreSQL is not the architecture.
   - It is only one adapter.
   - MongoDB-only products/plugins must be able to use the same library surface.

4. **The library owns standard HTTP endpoints.**
   - Apps should not duplicate route registration, DTOs, handlers, schema formatting, or history responses.
   - Matcher should mount a transport adapter from `pkg/systemplane`, not rebuild the API in `internal/bootstrap`.

5. **Use plural API names and fix naming now.**
   - New canonical endpoints are `/v1/system/configs` and `/v1/system/settings`.
   - Because Matcher is not public yet, we should not preserve the singular `/v1/system/config` name.

6. **No role hardcoding in app logic.**
   - Authorization is resource/action based.
   - The library should speak in permission/action names such as `system/configs:read`, `system/configs:write`, `system/settings/tenant:read`, `system/settings/tenant:write`, `system/settings/global:read`, `system/settings/global:write`, `system/configs/schema:read`, `system/settings/schema:read`, `system/configs/history:read`, `system/settings/history:read`, and `system/configs/reload:write`, while keeping the exact mapping pluggable per application.
   - Global setting overrides are admin-level operations and must require the global settings permissions, not the tenant settings permissions.

7. **The data model stays intentionally generic.**
   - The library is a typed-and-validated KV runtime system.
   - Apps register key metadata, validation, scope, secrecy/redaction, and apply behavior.
   - The library does not know business semantics for specific keys.

8. **Environment variables are bootstrap/platform only.**
   - Env vars remain valid only for startup concerns such as process identity, listener bind, TLS files, bootstrap auth/telemetry wiring, runtime-system backend selection, and the connection details required to reach the runtime system store/feed itself.
   - Business/system knobs move to configs/settings.
   - YAML/Viper ceases to be the long-term runtime authority.

9. **Runtime reload must never evict the last known good bundle.**
   - Rebuild candidate bundle off to the side.
   - Validate it.
   - Atomically swap only on success.
   - If rebuild fails, keep serving with the previous bundle.

10. **Runtime reload is not a readiness/liveness failure by itself.**
    - A reload event or change feed notification must not make the pod look dead.
    - Health should degrade only when the active serving bundle is no longer valid or the process cannot serve safely.

11. **Restart remains required for true bootstrap/platform changes.**
    - listener bind
    - TLS certificate path wiring
    - infra connection bootstrap
    - env/config-map/bootstrap-only values

12. **No backward-compatibility layer for old names or old internals.**
    - No long-lived dual route surface.
    - No wrapper preserving the old YAML-oriented `ConfigManager` contract.

---

## Target Architecture

### 1. Core model

The core library revolves around four concepts:

- **Registry**: app registers every supported key with metadata
- **Store**: generic persistence port for configs/settings
- **Supervisor**: rebuilds candidate runtime bundles and swaps them atomically
- **Transport adapter**: standard HTTP surface owned by the library

### 2. Registry model

Each registered key defines:

- `kind`: `config` or `setting`
- `key`: dotted key name
- `scope`: `global`, `tenant`, or future-safe variants if needed
- `value_type`: scalar / object / array metadata
- `default_value`
- `validator`
- `redaction_policy`
- `secret` flag
- `apply_behavior`
- `mutable_at_runtime`
- `documentation metadata` for schema/UI

`apply_behavior` should describe how the runtime handles changes, for example:

- `bundle-rebuild`
- `live-read`
- `worker-reconcile`
- `write-only-no-runtime-effect`
- `bootstrap-only`

That keeps the KV model generic while still allowing safe runtime application rules.

### 3. Persistence model

Use one generic logical record shape for both backends:

- `kind` (`config` or `setting`)
- `scope`
- `subject` (empty for global, tenant ID for tenant-scoped settings)
- `key`
- `value`
- `revision`
- `updated_at`
- `updated_by`
- `source`

Recommended implementation:

- **PostgreSQL adapter**: one table such as `system.runtime_entries`
- **MongoDB adapter**: one collection such as `runtime_entries`

Keep history/audit separate from the active KV store.

### 3.1 Effective value resolution

Inheritance always starts from **code defaults**.

Rules:

- defaults are defined in code through the registered key metadata
- persistence stores overrides only; it never becomes the source of defaults
- reset means deleting the relevant override record and recomputing the effective value from the remaining chain

Resolution order:

- `configs`: code default -> global override
- `settings`: code default -> optional global setting override -> tenant override

This means inherited values are always ultimately subject to the code-defined default, never to an env var default, YAML default, or database-stored default row.

Scope rule:

- tenant settings are the default operating mode of `/v1/system/settings`
- global setting overrides exist only as an explicit higher-scope layer above tenant overrides
- global setting overrides are admin-only and must never be writable through the tenant-scoped permission path

### 4. Change propagation model

The core depends on a generic `ChangeFeed` port.

- **PostgreSQL adapter**: LISTEN/NOTIFY or equivalent adapter-owned invalidation
- **MongoDB adapter**: change streams, with explicit polling fallback only if deployment constraints prevent reliable stream usage

The feed is an **invalidation signal**, not the source of truth.
On change, the supervisor reloads canonical state from the store and builds a candidate bundle.

### 5. Runtime supervisor / bundle swap model

Introduce a bootstrap umbrella/supervisor responsible for:

1. loading bootstrap config from env/platform sources
2. creating the initial runtime store/feed adapters
3. loading runtime configs/settings from the store
4. building a **runtime dependency bundle**
5. atomically swapping the active bundle when a candidate validates successfully

The active bundle should contain runtime-swappable dependencies only, for example:

- rebuilt database/cache/message/storage/webhook/auth clients whose effective configuration is runtime-managed
- rate limiters and other live-read runtime helpers
- worker runtime options and reconcile coordinators
- caches of validated settings/configs
- dynamically replaceable service facades

The bundle should **not** contain process-bootstrap concerns that require restart.

Failure rule:

- If a runtime reload fails, keep the previous active bundle.
- Emit logs/metrics/audit.
- Do not crash the process.
- Do not fail readiness/liveness solely because a reload happened.

### 6. HTTP transport owned by the library

Canonical routes:

- `GET /v1/system/configs`
- `PATCH /v1/system/configs`
- `GET /v1/system/configs/schema`
- `GET /v1/system/configs/history`
- `POST /v1/system/configs/reload` only if kept temporarily
- `GET /v1/system/settings`
- `PATCH /v1/system/settings`
- `GET /v1/system/settings/schema`
- `GET /v1/system/settings/history`

Route semantics:

- `configs` = admin/system runtime configuration
- `settings` = operator-safe runtime settings
- tenant identity comes from auth context, never from request body or ad-hoc role logic
- `/v1/system/settings` defaults to the current tenant scope resolved from auth context
- global settings operations require an explicit scope selection such as `scope=global` and must be authorized with admin-level global settings permissions

Standard behavior:

- `GET` returns effective values, defaults, source, revision, and redaction-aware fields
- `PATCH` accepts partial KV updates
- `null` means reset to default or inherited value
- writes use optimistic concurrency with `If-Match` on the current revision
- `settings` writes resolve tenant scope from auth context, never from body/path/query input
- `settings` reset semantics are scope-aware: tenant reset falls back to global override or code default; global reset falls back to code default
- schema is generated from registry metadata, not app-local duplicated DTO helpers
- history reflects runtime KV changes, not file mutations

Transport ownership model:

- `pkg/systemplane` owns handlers, DTOs, schema formatting, and route registration
- applications provide only framework hooks such as protected-route builders, actor extraction, tenant extraction, and manager instance wiring
- authorization remains action-based; the library does not hardcode role names such as admin or operator

### 7. Matcher-specific meaning

For Matcher specifically:

- `/v1/system/configs` replaces today's local `/v1/system/config` surface
- business configuration remains owned by the configuration bounded context, but its routes are renamed away from `/v1/config/...` to proper domain endpoints as part of this cutover
- outbound remote `/tenants/{tenant}/services/{service}/settings` stays a separate integration surface because it currently serves tenant infrastructure resolution, not the new local runtime settings plane
- local `/v1/system/settings` must not proxy, mirror, or silently reuse that external contract without an explicit future convergence plan
- admin-only global setting overrides in Matcher are part of the local system plane and are unrelated to the external tenant-manager settings contract

---

## Suggested `pkg/systemplane` Package Layout

```text
pkg/systemplane/
├── domain/
│   ├── entry.go              # generic config/setting record
│   ├── key.go                # key definition and metadata
│   ├── kind.go               # config vs setting
│   ├── scope.go              # global vs tenant
│   ├── target.go             # logical target set (kind + scope + subject)
│   ├── revision.go           # set-level revision/version primitives
│   ├── snapshot.go           # immutable effective runtime state
│   └── bundle.go             # runtime dependency bundle contract
├── registry/
│   ├── registry.go           # key registration and lookup
│   └── validation.go         # metadata-driven validation
├── ports/
│   ├── store.go              # read/write/list/reset contract
│   ├── history.go            # audit/history read/write contract
│   ├── changefeed.go         # invalidation feed contract
│   ├── identity.go           # actor/tenant resolution at service boundary
│   ├── authorizer.go         # permission authorization contract
│   ├── bundle_factory.go     # app-specific bundle rebuild contract
│   └── reconciler.go         # post-swap worker/module reconcile hooks
├── service/
│   ├── manager.go            # read/write orchestrator for configs/settings
│   ├── supervisor.go         # candidate rebuild + atomic swap
│   ├── snapshot_builder.go   # defaults + overrides materialization
│   └── activation.go         # swap + reconcile sequencing
├── adapters/
│   ├── store/
│   │   ├── postgres/
│   │   │   ├── store.go
│   │   │   └── store_test.go
│   │   └── mongodb/
│   │       ├── store.go
│   │       └── store_test.go
│   ├── changefeed/
│   │   ├── postgres/
│   │   │   ├── feed.go
│   │   │   └── feed_test.go
│   │   └── mongodb/
│   │       ├── feed.go
│   │       └── feed_test.go
│   └── http/
│       └── fiber/
│           ├── routes.go     # standard route registration
│           ├── handler.go    # shared configs/settings handlers
│           ├── dto.go        # request/response contracts
│           └── authz.go      # resource/action mapping, no role logic
└── bootstrap/
    ├── config.go             # bootstrap-only backend selector/env contract
    ├── backend.go            # postgres vs mongodb selection helpers
    └── classifier.go         # startup-only vs runtime-managed separation
```

Design rule: only `adapters/...` may know whether storage is PostgreSQL or MongoDB, or whether the HTTP framework is Fiber.

---

## Structural Interface Direction

The goal here is to freeze the **shape of the system plane**, not to prescribe full implementation details.

### Domain primitives

Recommended core types:

```go
type Kind string

const (
    KindConfig  Kind = "config"
    KindSetting Kind = "setting"
)

type Scope string

const (
    ScopeGlobal Scope = "global"
    ScopeTenant Scope = "tenant"
)

type ApplyBehavior string

const (
    ApplyLiveRead                       ApplyBehavior = "live-read"
    ApplyWorkerReconcile               ApplyBehavior = "worker-reconcile"
    ApplyBundleRebuild                 ApplyBehavior = "bundle-rebuild"
    ApplyBundleRebuildAndReconcile     ApplyBehavior = "bundle-rebuild+worker-reconcile"
    ApplyBootstrapOnly                 ApplyBehavior = "bootstrap-only"
)

type BackendKind string

const (
    BackendPostgres BackendKind = "postgres"
    BackendMongoDB  BackendKind = "mongodb"
)

type Target struct {
    Kind      Kind
    Scope     Scope
    SubjectID string
}

type Revision uint64
```

`Revision` is a **set-level revision token**, not a per-row field. It represents the current version of one logical target set:

- all global `configs`
- all global `settings`
- all tenant `settings` for one tenant subject

That is the token used by `If-Match` on `PATCH`.

### Registry direction

Applications own key registration. The library owns validation and lookup mechanics.

```go
type Registry interface {
    Register(def KeyDef) error
    MustRegister(def KeyDef)
    Get(key string) (KeyDef, bool)
    List(kind Kind) []KeyDef
    Validate(key string, value any) error
}
```

`KeyDef` should carry:

- key name
- kind (`config` or `setting`)
- allowed scopes
- code default value
- value type metadata
- validator
- secret/redaction metadata
- apply behavior
- runtime mutability flag
- documentation metadata for schema output

Rules:

- only keys that belong to the runtime system plane are registered here
- bootstrap-only keys are classified by bootstrap config helpers, not persisted by the runtime store
- deprecated env/YAML aliases are normalized before registration; the registry contains only canonical keys

### Persistence direction

The store layer remains generic and persists overrides only.

```go
type WriteOp struct {
    Key   string
    Value any
    Reset bool
}

type ReadResult struct {
    Entries   []Entry
    Revision  Revision
}

type Store interface {
    Get(ctx context.Context, target Target) (ReadResult, error)
    Put(ctx context.Context, target Target, ops []WriteOp, expected Revision, actor Actor, source string) (Revision, error)
}
```

Rules:

- defaults are not stored in the backend
- unknown keys are rejected before persistence
- `Reset=true` deletes the override row/document
- writes return the new target-set revision for optimistic concurrency
- `Put` is atomic for the full patch batch within one logical target
- `configs` always write against the single global configs target
- `settings` write against either the explicit global settings target or the current tenant settings target

### History and change-feed direction

```go
type HistoryStore interface {
    ListHistory(ctx context.Context, filter HistoryFilter) ([]HistoryEntry, error)
}

type ChangeSignal struct {
    Target   Target
    Revision Revision
}

type ChangeFeed interface {
    Subscribe(ctx context.Context, handler func(ChangeSignal)) error
}
```

`ChangeSignal` is advisory only. It tells the supervisor which logical target changed and what revision to resync from, but the store remains the only source of truth.

History write ownership:

- `Store.Put` appends runtime history records as part of the same backend write boundary as the active override mutation
- `HistoryStore` is the read surface for listing history

### Runtime bundle direction

The library owns the supervisor mechanics. Applications own bundle construction.

```go
type RuntimeBundle interface {
    Close(ctx context.Context) error
}

type BundleFactory interface {
    Build(ctx context.Context, snap Snapshot) (RuntimeBundle, error)
}

type BundleReconciler interface {
    Name() string
    Reconcile(ctx context.Context, previous RuntimeBundle, candidate RuntimeBundle, snap Snapshot) error
}

type Supervisor interface {
    Current() RuntimeBundle
    Snapshot() Snapshot
    PublishSnapshot(ctx context.Context, snap Snapshot, reason string) error
    ReconcileCurrent(ctx context.Context, snap Snapshot, reason string) error
    Reload(ctx context.Context, reason string) error
}
```

`RuntimeBundle` represents only runtime-swappable dependencies. It must not include bootstrap-only process wiring.

Supervisor sequencing must support all apply behaviors in the matrix:

1. build candidate snapshot from defaults + overrides
2. build candidate runtime bundle
3. validate candidate bundle
4. atomically publish candidate snapshot + candidate bundle
5. run `BundleReconciler` hooks for `worker-reconcile` and `bundle-rebuild+worker-reconcile` families
6. if reconciliation fails, roll back to the previous snapshot + bundle and keep serving with the last known good state
7. close the retired bundle only after successful handoff

That sequencing is mandatory for rows such as:

- `fetcher.enabled`
- `export_worker.enabled`
- `cleanup_worker.enabled`
- `archival.enabled`
- `archival.storage_*`

### Service direction

```go
type Actor struct {
    ID string
}

type Subject struct {
    Scope     Scope
    SubjectID string
}

type PatchRequest struct {
    Ops              []WriteOp
    ExpectedRevision Revision
    Actor            Actor
    Source           string
}

type Manager interface {
    GetConfigs(ctx context.Context) (ResolvedSet, error)
    GetSettings(ctx context.Context, subject Subject) (ResolvedSet, error)

    PatchConfigs(ctx context.Context, req PatchRequest) (WriteResult, error)
    PatchSettings(ctx context.Context, subject Subject, req PatchRequest) (WriteResult, error)

    GetConfigSchema(ctx context.Context) ([]SchemaEntry, error)
    GetSettingSchema(ctx context.Context) ([]SchemaEntry, error)

    GetConfigHistory(ctx context.Context, filter HistoryFilter) ([]HistoryEntry, error)
    GetSettingHistory(ctx context.Context, filter HistoryFilter) ([]HistoryEntry, error)

    Resync(ctx context.Context) error
}
```

The manager orchestrates:

- registry validation
- effective-value resolution
- optimistic concurrency
- override persistence
- apply-behavior escalation across a patch batch
- supervisor reload/reconcile triggering when apply behavior requires it

Escalation rule:

- `live-read` only: persist override and publish a new active snapshot; no bundle rebuild
- `worker-reconcile` only: persist override, publish a new active snapshot, and run reconciler against the current bundle
- any batch containing `bundle-rebuild`: build and swap a candidate bundle
- any batch containing `bundle-rebuild+worker-reconcile`: build and swap a candidate bundle, then run reconciler hooks before retiring the old bundle
- mixed batches always escalate to the strongest behavior present

### Transport integration direction

The library core must not import application-specific auth packages or any HTTP framework types.
Applications should provide actor/tenant resolution and authorization through framework-agnostic service hooks, while HTTP adapters may define framework-specific hook wrappers.

```go
type IdentityResolver interface {
    Actor(ctx context.Context) (Actor, error)
    TenantID(ctx context.Context) (string, error)
}

type Authorizer interface {
    Authorize(ctx context.Context, permission string) error
}
```

Adapter rule:

- `pkg/systemplane/adapters/http/fiber` may expose Fiber-specific hook types such as `RequirePermission`, `ResolveActor`, and `ResolveTenantID`
- those Fiber hooks adapt into `IdentityResolver` and `Authorizer`
- only adapter packages may import `fiber`

### Bootstrap configuration direction

`pkg/systemplane` itself still needs bootstrap-only configuration so the process can discover and connect to the runtime store/feed before any runtime config exists.

Recommended bootstrap types:

```go
type BootstrapConfig struct {
    Backend  BackendKind
    Postgres *PostgresBootstrapConfig
    MongoDB  *MongoBootstrapConfig
}

type PostgresBootstrapConfig struct {
    DSN           string
    Schema        string
    EntriesTable  string
    HistoryTable  string
    NotifyChannel string
}

type MongoBootstrapConfig struct {
    URI                string
    Database           string
    EntriesCollection  string
    HistoryCollection  string
    WatchMode          string
    PollInterval       time.Duration
}
```

Recommended bootstrap env contract:

- `SYSTEMPLANE_BACKEND=postgres|mongodb`
- `SYSTEMPLANE_POSTGRES_DSN`
- `SYSTEMPLANE_POSTGRES_SCHEMA`
- `SYSTEMPLANE_POSTGRES_ENTRIES_TABLE`
- `SYSTEMPLANE_POSTGRES_HISTORY_TABLE`
- `SYSTEMPLANE_POSTGRES_NOTIFY_CHANNEL`
- `SYSTEMPLANE_MONGODB_URI`
- `SYSTEMPLANE_MONGODB_DATABASE`
- `SYSTEMPLANE_MONGODB_ENTRIES_COLLECTION`
- `SYSTEMPLANE_MONGODB_HISTORY_COLLECTION`
- `SYSTEMPLANE_MONGODB_WATCH_MODE=change_stream|poll`
- `SYSTEMPLANE_MONGODB_POLL_INTERVAL_SEC`

Rules:

- these env vars configure only the runtime-plane store/feed bootstrap
- they are never mirrored into `/v1/system/configs`
- application runtime keys such as Matcher `postgres.*`, `redis.*`, `rabbitmq.*`, and `object_storage.*` remain part of the runtime plane and are distinct from the bootstrap configuration that powers `pkg/systemplane` itself

---

## HTTP Contract Direction

The goal is a stable system-plane contract shared by all applications using `pkg/systemplane`.

### Config endpoints

- `GET /v1/system/configs`
- `PATCH /v1/system/configs`
- `GET /v1/system/configs/schema`
- `GET /v1/system/configs/history`
- `POST /v1/system/configs/reload` only as a temporary break-glass endpoint

Permissions:

- `system/configs:read`
- `system/configs:write`
- `system/configs/schema:read`
- `system/configs/history:read`
- `system/configs/reload:write`

Behavior:

- always global scope
- `PATCH` uses `If-Match` with the current **global configs set revision**
- `null` resets override and falls back to code default
- responses include effective value, default, override, source, the current global configs set revision, and redaction-aware fields

### Settings endpoints

- `GET /v1/system/settings`
- `PATCH /v1/system/settings`
- `GET /v1/system/settings/schema`
- `GET /v1/system/settings/history`

Permissions:

- tenant scope: `system/settings/tenant:read`, `system/settings/tenant:write`
- global scope: `system/settings/global:read`, `system/settings/global:write`

Behavior:

- tenant scope is the default operating mode
- tenant identity is resolved from auth context only
- tenant writes must not accept tenant ID in body, path, or query
- global operations require explicit `scope=global`
- global setting overrides are admin-only through the global settings permissions
- `PATCH` uses `If-Match` with the current target-set revision for the selected scope (`global` or current tenant)
- tenant reset falls back to global override or code default
- global reset falls back to code default

### Schema contract

Schema responses should be generated only from registry metadata and include at minimum:

- key
- kind
- allowed scopes
- value type
- default value
- mutability flag
- apply behavior
- secret/redaction metadata
- description/grouping metadata

### History contract

History responses should represent runtime KV mutations, not file mutations, and include at minimum:

- revision
- key
- scope
- subject ID
- old value (redacted if needed)
- new value (redacted if needed)
- actor ID
- changed timestamp

### Error contract

The system plane should use a stable, small error vocabulary such as:

- `system_key_unknown`
- `system_value_invalid`
- `system_revision_mismatch`
- `system_scope_invalid`
- `system_permission_denied`
- `system_reload_failed`

### Transport contract rules

- all secret fields are redacted in reads and history output
- unknown keys fail fast before persistence
- transport must never infer schema outside the registry
- transport must never infer tenant identity from client-supplied fields for tenant-scoped settings

---

## Bootstrap vs Runtime Split

Bootstrap-only values remain in env/config-map space because they define how the process comes up and where the runtime system itself lives.

Bootstrap-only examples:

- service identity and environment name
- server bind/listener/TLS wiring
- auth bootstrap wiring
- telemetry exporter bootstrap wiring
- which system backend to use (`postgres` or `mongodb`)
- backend connection details and credentials for the runtime system store/feed itself

Runtime-managed values move into `configs` or `settings`.

Runtime-managed examples:

- worker intervals and page sizes
- runtime rate limits
- feature toggles that are safe to hot-apply
- connector behavior that can be rebuilt safely through the supervisor

Classification rule:

- if a value changes how the process boots, binds, or discovers its own control plane, it is bootstrap-only
- if a value can be applied by live read, worker reconcile, or candidate bundle rebuild, it is runtime-managed

---

## Initial Configuration Decision Matrix

This matrix is the target execution inventory for the current Matcher configuration surface.
The migration lands as **one cutover wave**. The question is not "what is swappable today?"; the question is "what must be refactored so every non-bootstrap key becomes runtime-swappable after cutover?"

| Key family | Plane kind | Scope | Target apply behavior | Final classification | Current reality / required refactor |
|---|---|---|---|---|---|
| `app.env_name` | bootstrap | global | bootstrap-only | keep in env | Process/platform identity; keep outside runtime plane |
| `app.log_level` | config | global | bundle-rebuild | move to `system/configs` | Logger is initialized from env at startup; add logger swap or runtime bundle ownership |
| `server.address`, `server.tls_cert_file`, `server.tls_key_file` | bootstrap | global | bootstrap-only | keep in env | Listener/process wiring; restart-bound by design |
| `server.tls_terminated_upstream`, `server.trusted_proxies` | bootstrap | global | bootstrap-only | keep in env | Trust boundary and security posture; restart-bound by design |
| `server.body_limit_bytes`, `server.cors_allowed_origins`, `server.cors_allowed_methods`, `server.cors_allowed_headers` | config | global | bundle-rebuild | move to `system/configs` | Fiber app config and CORS middleware are fixed at app creation; require HTTP-layer bundle rebuild/swap |
| `tenancy.default_tenant_id`, `tenancy.default_tenant_slug` | config | global | bundle-rebuild | move to `system/configs` | Default tenant values are pushed into auth bootstrap/global state; require tenant extractor and auth default rebuild |
| canonical `tenancy.multi_tenant_*` | config | global | bundle-rebuild | move to `system/configs` | Tenant infra resolver/connection manager is built once; require rebuild/swap. Drop deprecated `multi_tenant_infra_enabled` alias from the new canonical registry |
| `postgres.*` | config | global | bundle-rebuild | move to `system/configs`, except the connection details required by `pkg/systemplane` itself | Application Postgres clients, pools, and query-timeout behavior are startup-built; require swappable runtime DB bundle |
| `redis.*` | config | global | bundle-rebuild | move to `system/configs`, except the connection details required by `pkg/systemplane` itself | Redis topology/client is built once; require swappable runtime Redis bundle |
| `rabbitmq.*` | config | global | bundle-rebuild | move to `system/configs` | Messaging connection/client wiring is startup-built; require swappable runtime messaging bundle |
| `object_storage.*` | config | global | bundle-rebuild | move to `system/configs` | S3/MinIO clients are startup-built; require swappable runtime storage bundle |
| `auth.enabled`, `auth.host`, `auth.token_secret` | bootstrap | global | bootstrap-only | keep in env | Auth bootstrap and security boundary remain restart-bound |
| `swagger.*` | config | global | bundle-rebuild | move to `system/configs` | Swagger routes/spec overrides are mounted at startup; require route reconcile or HTTP-layer bundle swap |
| `telemetry.*` | bootstrap | global | bootstrap-only | keep in env | Exporter/bootstrap observability wiring remains restart-bound |
| `rate_limit.*` | config | global | live-read | move to `system/configs` | Already dynamic on the request path; preserve current live-read behavior |
| `infrastructure.health_check_timeout_sec` | config | global | live-read | move to `system/configs` | Already dynamic in readiness path; preserve current live-read behavior |
| `infrastructure.connect_timeout_sec` | config | global | bundle-rebuild | move to `system/configs` | Currently shared with bootstrap connection/telemetry startup budgets; split bootstrap timeout concerns from runtime dependency rebuild concerns |
| `idempotency.retry_window_sec`, `idempotency.success_ttl_hours` | config | global | bundle-rebuild | move to `system/configs` | Idempotency repository captures TTL policy at construction; require rebuild or dynamic policy source |
| `idempotency.hmac_secret` | config | global | bundle-rebuild | move to `system/configs` as secret | Same as above, plus secret handling/redaction/at-rest policy |
| `callback_rate_limit.per_minute` | config | global | live-read | move to `system/configs` | Current limiter stores limit at construction; replace with live-read wrapper or equivalent lightweight swap |
| `deduplication.ttl_sec` | config | global | bundle-rebuild | move to `system/configs` | Ingestion use case captures TTL at wiring time; require dynamic policy source or rebuilt use case/facade |
| `fetcher.enabled` | config | global | bundle-rebuild+worker-reconcile | move to `system/configs` | Discovery module is startup-gated today and file reload explicitly blocks this key; enabling/disabling requires module construction/teardown |
| `fetcher.url`, `fetcher.allow_private_ips`, `fetcher.health_timeout_sec`, `fetcher.request_timeout_sec`, `fetcher.schema_cache_ttl_sec`, `fetcher.extraction_poll_sec`, `fetcher.extraction_timeout_sec` | config | global | bundle-rebuild | move to `system/configs` | Fetcher client, poller, and schema-cache wiring are built once; require discovery-module rebuild/swap |
| `fetcher.discovery_interval_sec` | config | global | worker-reconcile | move to `system/configs` | Already runtime-updated today once the discovery module exists |
| `export_worker.enabled` | config | global | bundle-rebuild+worker-reconcile | move to `system/configs` | Runtime toggle is startup-frozen today and may require object-storage dependency creation/removal |
| `export_worker.poll_interval_sec`, `export_worker.page_size` | config | global | worker-reconcile | move to `system/configs` | Already runtime-applied through worker-manager reconciliation |
| `export_worker.presign_expiry_sec` | config | global | live-read | move to `system/configs` | Already exposed dynamically to export handlers |
| `cleanup_worker.enabled` | config | global | bundle-rebuild+worker-reconcile | move to `system/configs` | Runtime toggle is startup-frozen today and may require object-storage dependency creation/removal |
| `cleanup_worker.interval_sec`, `cleanup_worker.batch_size`, `cleanup_worker.grace_period_sec` | config | global | worker-reconcile | move to `system/configs` | Already runtime-applied through worker-manager reconciliation |
| `scheduler.interval_sec` | config | global | worker-reconcile | move to `system/configs` | Already runtime-applied through worker-manager reconciliation |
| `archival.enabled` | config | global | bundle-rebuild+worker-reconcile | move to `system/configs` | Runtime toggle is startup-frozen today and archival worker/storage/DB setup are bootstrap-built |
| `archival.interval_hours`, `archival.hot_retention_days`, `archival.warm_retention_months`, `archival.cold_retention_months`, `archival.batch_size`, `archival.partition_lookahead` | config | global | worker-reconcile | move to `system/configs` | Runtime worker settings; keep on worker reconciliation path |
| `archival.storage_bucket`, `archival.storage_prefix`, `archival.storage_class` | config | global | bundle-rebuild+worker-reconcile | move to `system/configs` | Archive storage client and related runtime wiring are startup-built today; split this from pure worker knobs |
| `archival.presign_expiry_sec` | config | global | live-read | move to `system/configs` | Archive handler already supports a runtime getter, but Matcher does not wire it yet |
| `webhook.timeout_sec` | config | global | bundle-rebuild | move to `system/configs` | HTTP connector captures timeout at construction; require connector rebuild or dynamic timeout source |
| local tenant/operator-safe `settings` in Matcher today | setting | tenant | n/a yet | initial local `system/settings` set is empty until concrete keys are approved | No concrete local settings surface found in current repo |
| outbound `/tenants/{tenant}/services/{service}/settings` | not local setting | tenant | external infra resolution | keep outside local `pkg/systemplane` plane | External tenant infra lookup, not local runtime settings |

Interpretation rules:

- `config` rows map to `/v1/system/configs`
- `setting` rows map to `/v1/system/settings`
- `bootstrap` rows stay outside the runtime system plane
- rows marked `bundle-rebuild` must rebuild a candidate bundle and swap atomically
- rows marked `bundle-rebuild+worker-reconcile` require both candidate dependency rebuild and runtime worker/module start-stop reconciliation
- rows marked `worker-reconcile` must flow through supervisor/worker-manager reconciliation without restarting the process
- rows marked `live-read` must be read from the active runtime snapshot on demand
- `postgres.*`, `redis.*`, `rabbitmq.*`, and `object_storage.*` in this table refer to **application runtime dependencies**, not to the runtime-plane's own storage/changefeed bootstrap configuration
- the runtime plane itself still needs explicit bootstrap env configuration for backend selection and connection details, and that bootstrap surface must support **both PostgreSQL and MongoDB adapters**

---

## Domain Route Rename Map

The current `/v1/config/...` prefix belongs to the configuration bounded context, not to the new system plane.
As part of this refactor, those routes should move to domain-native paths.

Canonical replacement map:

- `POST /v1/config/contexts` -> `POST /v1/contexts`
- `GET /v1/config/contexts` -> `GET /v1/contexts`
- `GET /v1/config/contexts/:contextId` -> `GET /v1/contexts/:contextId`
- `PATCH /v1/config/contexts/:contextId` -> `PATCH /v1/contexts/:contextId`
- `DELETE /v1/config/contexts/:contextId` -> `DELETE /v1/contexts/:contextId`
- `POST /v1/config/contexts/:contextId/clone` -> `POST /v1/contexts/:contextId/clone`

- `POST /v1/config/contexts/:contextId/sources` -> `POST /v1/contexts/:contextId/sources`
- `GET /v1/config/contexts/:contextId/sources` -> `GET /v1/contexts/:contextId/sources`
- `GET /v1/config/contexts/:contextId/sources/:sourceId` -> `GET /v1/contexts/:contextId/sources/:sourceId`
- `PATCH /v1/config/contexts/:contextId/sources/:sourceId` -> `PATCH /v1/contexts/:contextId/sources/:sourceId`
- `DELETE /v1/config/contexts/:contextId/sources/:sourceId` -> `DELETE /v1/contexts/:contextId/sources/:sourceId`

- `POST /v1/config/contexts/:contextId/sources/:sourceId/field-maps` -> `POST /v1/contexts/:contextId/sources/:sourceId/field-maps`
- `GET /v1/config/contexts/:contextId/sources/:sourceId/field-maps` -> `GET /v1/contexts/:contextId/sources/:sourceId/field-maps`
- `PATCH /v1/config/field-maps/:fieldMapId` -> `PATCH /v1/field-maps/:fieldMapId`
- `DELETE /v1/config/field-maps/:fieldMapId` -> `DELETE /v1/field-maps/:fieldMapId`

- `POST /v1/config/contexts/:contextId/rules` -> `POST /v1/contexts/:contextId/rules`
- `GET /v1/config/contexts/:contextId/rules` -> `GET /v1/contexts/:contextId/rules`
- `GET /v1/config/contexts/:contextId/rules/:ruleId` -> `GET /v1/contexts/:contextId/rules/:ruleId`
- `PATCH /v1/config/contexts/:contextId/rules/:ruleId` -> `PATCH /v1/contexts/:contextId/rules/:ruleId`
- `DELETE /v1/config/contexts/:contextId/rules/:ruleId` -> `DELETE /v1/contexts/:contextId/rules/:ruleId`
- `POST /v1/config/contexts/:contextId/rules/reorder` -> `POST /v1/contexts/:contextId/rules/reorder`

- `POST /v1/config/fee-schedules` -> `POST /v1/fee-schedules`
- `GET /v1/config/fee-schedules` -> `GET /v1/fee-schedules`
- `GET /v1/config/fee-schedules/:scheduleId` -> `GET /v1/fee-schedules/:scheduleId`
- `PATCH /v1/config/fee-schedules/:scheduleId` -> `PATCH /v1/fee-schedules/:scheduleId`
- `DELETE /v1/config/fee-schedules/:scheduleId` -> `DELETE /v1/fee-schedules/:scheduleId`
- `POST /v1/config/fee-schedules/:scheduleId/simulate` -> `POST /v1/fee-schedules/:scheduleId/simulate`

- `POST /v1/config/contexts/:contextId/schedules` -> `POST /v1/contexts/:contextId/schedules`
- `GET /v1/config/contexts/:contextId/schedules` -> `GET /v1/contexts/:contextId/schedules`
- `GET /v1/config/contexts/:contextId/schedules/:scheduleId` -> `GET /v1/contexts/:contextId/schedules/:scheduleId`
- `PATCH /v1/config/contexts/:contextId/schedules/:scheduleId` -> `PATCH /v1/contexts/:contextId/schedules/:scheduleId`
- `DELETE /v1/config/contexts/:contextId/schedules/:scheduleId` -> `DELETE /v1/contexts/:contextId/schedules/:scheduleId`

Route-cutover rule:

- `/v1/system/configs` and `/v1/system/settings` are reserved for the system plane only
- `/v1/config/...` is removed entirely after the domain route rename
- there is no generic `/v1/settings` route family in Matcher

---

## Phased Implementation Plan

These phases are **engineering sequencing**, not separate public API waves. Matcher still cuts over in **one go**. Phases 6 through 8 ship in the same cutover branch and are merge-gate completion work, not follow-up releases.

### Phase 0 — Freeze vocabulary, scope, and cutover rules

Decisions to lock before code changes:

- `pkg/systemplane` is the package name
- `/v1/system/configs` and `/v1/system/settings` are the canonical endpoints
- `/v1/system/config` is retired, not aliased long-term
- current business endpoints under `/v1/config/...` are renamed away from that prefix during the same cutover
- outbound tenant-manager settings calls are not silently repurposed as the new local settings API
- `/v1/system/settings` defaults to current-tenant scope; global settings require explicit scope selection plus admin-level global settings permissions
- env vars are bootstrap/platform only after cutover
- `pkg/systemplane` bootstrap backend selection is env-only and supports both PostgreSQL and MongoDB
- target set revision semantics are the basis for `If-Match`
- `bundle-rebuild+worker-reconcile` is a first-class apply behavior, not an implementation footnote

Deliverables:

- updated plan document
- approved terminology table
- approved runtime-vs-bootstrap classification list
- approved structural interfaces for registry, store, manager, supervisor, and transport hooks
- approved HTTP contract for configs/settings, including scope and permission rules
- approved bootstrap env contract for `pkg/systemplane`

### Phase 1 — Build the backend-agnostic core in `pkg/systemplane`

Implement:

- core domain types
- `Target`, `Revision`, and `ApplyBehavior` primitives
- key registry
- metadata-driven validation
- immutable snapshot model
- manager/service contracts
- supervisor contract and atomic swap mechanics
- bundle factory interface for applications
- reconcile hook contracts for worker/module start-stop behavior
- framework-agnostic identity/authorization contracts

Rules:

- no `internal/...` imports
- no PostgreSQL-specific types in core
- no MongoDB-specific types in core
- no Fiber types in core contracts

Acceptance:

- contract tests pass against in-memory fakes
- core can build effective state from defaults + overrides
- supervisor can reject invalid candidate bundles and keep the current bundle active
- supervisor supports `live-read`, `worker-reconcile`, `bundle-rebuild`, and `bundle-rebuild+worker-reconcile`

### Phase 2 — Add bootstrap config and both backend adapters

Implement bootstrap-only `pkg/systemplane` config loading from env and wire both backend families.

Implement both adapters in scope, not as “later maybe” work.

Bootstrap responsibilities:

- parse and validate `SYSTEMPLANE_BACKEND`
- parse and validate PostgreSQL bootstrap env config
- parse and validate MongoDB bootstrap env config
- construct the correct store/history adapter family before loading any runtime config

PostgreSQL adapter responsibilities:

- read/write/reset entries
- optimistic concurrency by revision
- list effective configs/settings by scope
- advisory invalidation via PostgreSQL feed bootstrap config

MongoDB adapter responsibilities:

- equivalent read/write/reset behavior
- equivalent revision/concurrency semantics
- equivalent query model for global and tenant scopes
- change-stream assumptions documented explicitly, including polling fallback behavior when needed

Acceptance:

- shared contract test suite passes for both adapters
- no behavior drift between PostgreSQL and MongoDB semantics
- bootstrap can select either backend from env without changing application runtime key registration
- `Store.Put` persists active overrides and history consistently for both backend families

### Phase 3 — Add change feed adapters

Implement:

- PostgreSQL invalidation feed
- MongoDB change stream feed
- debounce/jitter/resync behavior in supervisor

Rules:

- feed event is advisory only
- canonical reload always comes from the store
- missed events trigger full resync, not partial guesswork

Acceptance:

- two instances converge after writes
- feed interruption does not crash the app
- reconnect + resync restores convergence

### Phase 4 — Introduce Matcher runtime seams and supervisor ownership

Matcher-specific work:

- replace direct `ConfigManager`/YAML/Viper runtime authority with store-backed runtime authority
- integrate supervisor into bootstrap so runtime-managed reads come from the active snapshot and runtime-managed dependencies come from the active bundle
- create runtime seams for every non-bootstrap family in the matrix:
  - HTTP runtime policy seam for `server.body_limit_bytes`, `server.cors_allowed_*`, and `swagger.*`
  - logger seam for `app.log_level`
  - tenant/auth-default seam for `tenancy.default_tenant_*`
  - tenant infra resolver seam for canonical `tenancy.multi_tenant_*`
  - swappable runtime infra bundle for application `postgres.*`, `redis.*`, `rabbitmq.*`, and `object_storage.*`
  - runtime facades or rebuild seams for `idempotency.*`, `callback_rate_limit.*`, `deduplication.ttl_sec`, and `webhook.timeout_sec`
  - discovery module seam for `fetcher.*`
  - worker/module reconcile seams for export, cleanup, scheduler, and archival families
- preserve restart-only behavior for platform/bootstrap concerns

Important Matcher rule:

- readiness/liveness must remain stable during runtime reloads unless serving safety is actually compromised

Acceptance:

- every runtime-managed key family has exactly one owning seam: active snapshot, active bundle, or reconcile hook
- runtime reload builds candidate bundle and swaps atomically
- failed reload keeps the last known good bundle active
- no process restart needed for runtime-managed changes

### Phase 5 — Replace local system config API with library-owned transport

Replace:

- `internal/bootstrap/config_api_routes.go`
- `internal/bootstrap/config_api.go`
- `internal/bootstrap/config_api_dto.go`

with mounted transport from `pkg/systemplane/adapters/http/fiber`.

Implement:

- configs endpoints
- settings endpoints
- schema output from registry metadata
- history output from history port
- action-based authorization wiring
- explicit tenant-vs-global settings scope handling in the shared transport
- request/response DTOs aligned with the shared HTTP contract

Acceptance:

- apps do not duplicate DTO/handler/route code
- auth is permission/action based, not role based
- singular `/v1/system/config` is removed
- global setting overrides are admin-only in both transport rules and permission mapping

### Phase 6 — Execute the one-go Matcher cutover

Move the **full non-bootstrap key set** into `system/configs` in one cutover, performing the required refactors for rows that are not runtime-safe today.

For Matcher:

- migrate all runtime-managed config families from the matrix into the registry and store-backed runtime plane
- rename current `/v1/config/...` domain configuration routes to proper domain endpoints
- introduce local `/v1/system/settings` only for keys explicitly classified as operator-safe runtime settings
- do not conflate new local settings with the existing outbound tenant-manager settings client
- update Swagger/OpenAPI, tests, client calls, and internal references to the new domain route map in the same phase

Concrete implementation checkpoints for every `bundle-rebuild+worker-reconcile` row:

| Matrix row | Concrete checkpoint |
|---|---|
| `fetcher.enabled` | Discovery becomes a supervisor-managed module bundle owning fetcher client, extraction poller, schema-cache wiring, routes facade, and discovery worker. Done when toggling the flag can add/remove the full discovery module without restart and a failed enable leaves the previous state active. |
| `export_worker.enabled` | Export worker lifecycle is decoupled from bootstrap and reconciled after bundle swap. Done when enabling/disabling export worker does not require restart, can create/release its runtime dependencies safely, and rolls back cleanly on failure. |
| `cleanup_worker.enabled` | Cleanup worker lifecycle is decoupled from bootstrap and reconciled independently of export worker. Done when toggling cleanup worker does not require restart and does not force unrelated worker churn unless a shared dependency actually changed. |
| `archival.enabled` | Archival becomes a supervisor-managed module owning archive handler facade, archival DB handle, archival storage client, and archival worker. Done when runtime patch can enable/disable archival end-to-end without restart and failed activation keeps the last known good archival state active. |
| `archival.storage_bucket`, `archival.storage_prefix`, `archival.storage_class` | Archival storage is rebuilt from the candidate bundle and reconciled into both archive downloads and archival worker execution. Done when a runtime patch moves archive reads/writes to the new storage target atomically and rollback preserves the prior storage target on failure. |

Acceptance:

- each migrated key has registry metadata, validation, scope, redaction policy, and apply behavior
- all non-bootstrap Matcher keys are now owned by the runtime plane, not by YAML/Viper/env runtime authority
- tenant-scoped settings resolve tenant identity from auth context only
- no bounded-context route remains under `/v1/config/...`
- global setting override behavior is explicit and requires admin-level global settings permissions
- the cutover works with `pkg/systemplane` bootstrap configured for either PostgreSQL or MongoDB

### Phase 7 — In the same cutover branch, remove YAML/Viper and shrink env usage to bootstrap-only

Remove:

- Viper runtime authority
- YAML runtime authority
- file watcher runtime authority
- env vars for business/system knobs that now live in configs/settings

Keep env vars only for:

- bootstrap/platform identity
- bind/listener settings
- runtime system backend selection
- runtime system store/feed connection details and credentials
- other restart-required process wiring

Acceptance:

- no business/system runtime knob is still sourced from YAML/Viper/env by default
- restart is only needed for real bootstrap/platform changes
- application runtime `postgres.*`, `redis.*`, `rabbitmq.*`, and `object_storage.*` are sourced from the runtime plane, not confused with `pkg/systemplane` bootstrap env

### Phase 8 — In the same cutover branch, finalize naming cleanup and hardening

Finalize:

- docs and Swagger/OpenAPI use `/configs` and `/settings`
- tests stop referencing `/v1/system/config`
- tests and docs stop referencing `/v1/config/...` for domain configuration routes
- audit/history language reflects configs/settings, not YAML reload semantics
- legacy config manager internals are deleted

Acceptance:

- one runtime authority
- one canonical API naming scheme
- one supervisor-controlled bundle swap mechanism
- one explicit bootstrap env contract for system-plane backend selection and connection bootstrap

---

## Schema / History / Reload Endpoint Policy

### Schema

Keep it.

Reason:
- the KV model is generic by design
- operators and UIs need registry-derived metadata, constraints, scope, redaction flags, and mutability information

### History

Keep it.

Reason:
- configs/settings are operational control planes
- auditability matters even before Matcher is public
- history should describe runtime changes, not file mutations

### Reload

Do **not** make manual reload a permanent primary mechanism.

Recommended position:

- keep a manual reload endpoint only as a **temporary troubleshooting/break-glass tool** during the migration period
- once PostgreSQL/MongoDB change feeds are stable, remove the public endpoint from the normal API contract
- if forced resync remains valuable, keep it as an internal admin/ops hook or CLI, not a core product endpoint

---

## Final Recommendation

The correct target is not “Postgres-backed config.”
The correct target is a **library-owned runtime system plane** in `pkg/systemplane` with:

- backend-agnostic core
- PostgreSQL and MongoDB adapters
- library-owned HTTP transport
- clear `configs` vs `settings` semantics
- action-based authorization
- bootstrap-only env usage
- supervisor-driven atomic bundle swaps
- last-known-good safety on reload failure

That is the architecture Matcher should implement now.
