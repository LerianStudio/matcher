# Phase 0 — Approved Contracts

**Status**: REVIEW GATE — these contracts are frozen before Phase 1 implementation begins.

No Go code may be written for `pkg/systemplane` until every section in this document has been reviewed and accepted. Any change to a frozen contract after Phase 1 begins requires an explicit amendment with justification, not a silent drift.

This document extracts and locks the five contract surfaces defined in the [Configuration Architecture Refactor Implementation Plan](2026-03-14-configuration-architecture-refactor.md). It is the single source of truth for what Phase 1 must implement and what downstream phases must conform to.

---

## 1. Terminology Table

These definitions are canonical. Code, comments, docs, API responses, and error messages must use these terms consistently. Synonyms and legacy names (e.g., "config" singular for the API surface, "parameter", "property", "preference") are not permitted in new code.

| Term | Definition | Example |
|------|-----------|---------|
| **config** | An admin/system-level runtime configuration value. Always global scope. Mutation requires elevated permissions. Affects shared services, background workers, or infrastructure wiring. Exposed under `/v1/system/configs`. | `app.log_level`, `rate_limit.requests_per_second`, `fetcher.enabled` |
| **setting** | An operator-safe runtime setting. May be tenant-scoped or global-scoped. Tenant scope is the default operating mode. Does not require platform re-bootstrap. Exposed under `/v1/system/settings`. | A future tenant-visible operational toggle or non-destructive runtime preference. Initial Matcher settings set is empty until concrete keys are approved. |
| **bootstrap** | A value that defines how the process comes up, binds, discovers its own control plane, or establishes security boundaries. Lives in env vars or config maps. Never persisted in the runtime store. Requires process restart to change. | `app.env_name`, `server.address`, `auth.enabled`, `SYSTEMPLANE_BACKEND` |
| **key** | The dotted string identifier for one registered config or setting. Unique within the registry. Canonical form only; deprecated aliases are normalized before registration. | `fetcher.discovery_interval_sec`, `archival.storage_bucket` |
| **entry** | One persisted override record in the runtime store. Contains key, value, scope, subject, revision metadata, and provenance. Entries represent overrides only; code defaults are never stored as entries. | A row in `system.runtime_entries` (PostgreSQL) or a document in `runtime_entries` (MongoDB). |
| **target** | A logical partition of entries defined by the combination of kind, scope, and subject. Revision tokens are scoped per target. | Global configs target (`kind=config, scope=global, subject=""`), tenant settings target for tenant X (`kind=setting, scope=tenant, subject="<tenant-id>"`). |
| **revision** | A set-level version token for one logical target. Monotonically increasing within a target. Used for optimistic concurrency via `If-Match` on PATCH requests. Not a per-row field. | Revision 17 of the global configs target. |
| **snapshot** | An immutable materialized view of effective values for a given target, produced by merging code defaults with stored overrides in resolution order. The active snapshot is the source of truth for live-read keys. | The current effective state of all global configs after applying overrides on top of code defaults. |
| **bundle** | A runtime dependency bundle containing rebuilt, swappable dependencies (database clients, cache clients, message clients, worker coordinators, etc.) constructed from a snapshot. The supervisor owns the active bundle and swaps it atomically. | A bundle holding rebuilt Postgres pool, Redis client, and RabbitMQ connection after a `postgres.*` config change. |
| **apply behavior** | A classification that describes how the runtime handles changes to a key. Determines the supervisor's response when a key is patched: no action (bootstrap-only), direct snapshot read (live-read), worker notification (worker-reconcile), full dependency rebuild (bundle-rebuild), or both (bundle-rebuild+worker-reconcile). | `rate_limit.*` keys use `live-read`; `fetcher.enabled` uses `bundle-rebuild+worker-reconcile`. |

---

## 2. Runtime-vs-Bootstrap Classification

This matrix is frozen. It is the target execution inventory for Matcher's configuration surface. The migration lands as one cutover wave. Interpretation rules follow the table.

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

### Interpretation rules

- `config` rows map to `/v1/system/configs`.
- `setting` rows map to `/v1/system/settings`.
- `bootstrap` rows stay outside the runtime system plane entirely.
- Rows marked `bundle-rebuild` must rebuild a candidate bundle and swap atomically.
- Rows marked `bundle-rebuild+worker-reconcile` require both candidate dependency rebuild and runtime worker/module start-stop reconciliation.
- Rows marked `worker-reconcile` must flow through supervisor/worker-manager reconciliation without restarting the process.
- Rows marked `live-read` must be read from the active runtime snapshot on demand.
- `postgres.*`, `redis.*`, `rabbitmq.*`, and `object_storage.*` in this table refer to **application runtime dependencies**, not to the runtime-plane's own storage/changefeed bootstrap configuration.
- The runtime plane itself still needs explicit bootstrap env configuration for backend selection and connection details, and that bootstrap surface must support both PostgreSQL and MongoDB adapters.
- Mixed batches always escalate to the strongest apply behavior present in the batch.

---

## 3. Structural Interface Signatures

These are the Go types and interfaces that Phase 1 must implement. The architecture plan defines some types explicitly and references others through interface signatures. This section codifies both categories into a complete, implementable contract.

### Domain enums

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
    ApplyLiveRead                   ApplyBehavior = "live-read"
    ApplyWorkerReconcile            ApplyBehavior = "worker-reconcile"
    ApplyBundleRebuild              ApplyBehavior = "bundle-rebuild"
    ApplyBundleRebuildAndReconcile  ApplyBehavior = "bundle-rebuild+worker-reconcile"
    ApplyBootstrapOnly              ApplyBehavior = "bootstrap-only"
)

type BackendKind string

const (
    BackendPostgres BackendKind = "postgres"
    BackendMongoDB  BackendKind = "mongodb"
)
```

### Domain primitives

```go
// Revision is a set-level revision token, not a per-row field.
// It represents the current version of one logical target set:
//   - all global configs
//   - all global settings
//   - all tenant settings for one tenant subject
// This is the token used by If-Match on PATCH.
type Revision uint64

// Actor identifies who performed a mutation.
type Actor struct {
    ID string
}

// Target identifies a logical partition of entries by kind, scope, and subject.
// SubjectID is empty for global targets and holds the tenant ID for tenant-scoped targets.
type Target struct {
    Kind      Kind
    Scope     Scope
    SubjectID string
}
```

### Domain records

```go
// Entry is one persisted override record in the runtime store.
// Entries represent overrides only; code defaults are never stored as entries.
type Entry struct {
    Kind      Kind
    Scope     Scope
    Subject   string
    Key       string
    Value     any
    Revision  Revision
    UpdatedAt time.Time
    UpdatedBy string
    Source    string
}

// ValueType describes the expected shape of a key's value for schema output
// and validation routing.
type ValueType string

const (
    ValueTypeString  ValueType = "string"
    ValueTypeBool    ValueType = "bool"
    ValueTypeInt     ValueType = "int"
    ValueTypeFloat   ValueType = "float"
    ValueTypeObject  ValueType = "object"
    ValueTypeArray   ValueType = "array"
)

// RedactPolicy controls how secret/sensitive values are presented in reads and history.
type RedactPolicy string

const (
    RedactNone  RedactPolicy = "none"
    RedactFull  RedactPolicy = "full"
    RedactMask  RedactPolicy = "mask"
)

// ValidatorFunc is a function that validates a candidate value for a key.
// It returns nil if the value is valid, or an error describing the violation.
type ValidatorFunc func(value any) error

// KeyDef carries the full metadata for one registered key.
type KeyDef struct {
    Key              string
    Kind             Kind
    AllowedScopes    []Scope
    DefaultValue     any
    ValueType        ValueType
    Validator        ValidatorFunc
    Secret           bool
    RedactPolicy     RedactPolicy
    ApplyBehavior    ApplyBehavior
    MutableAtRuntime bool
    Description      string
    Group            string
}
```

### Domain composites

```go
// Snapshot is an immutable materialized view of effective values for a set of targets.
// Produced by merging code defaults with stored overrides in resolution order.
type Snapshot struct {
    Configs         map[string]EffectiveValue
    GlobalSettings  map[string]EffectiveValue
    TenantSettings  map[string]map[string]EffectiveValue // tenantID -> key -> value
    Revision        Revision
    BuiltAt         time.Time
}

// EffectiveValue represents the resolved value for one key after applying the
// inheritance chain: code default -> global override -> tenant override.
type EffectiveValue struct {
    Key          string
    Value        any
    Default      any
    Override     any
    Source       string       // "default", "global-override", "tenant-override"
    Revision     Revision
    Redacted     bool
}

// RuntimeBundle represents only runtime-swappable dependencies.
// It must not include bootstrap-only process wiring.
// Applications implement this interface with their own concrete bundle type.
type RuntimeBundle interface {
    Close(ctx context.Context) error
}
```

### Registry

```go
// Registry holds all registered key definitions. Applications own key registration;
// the library owns validation and lookup mechanics.
type Registry interface {
    // Register adds a key definition. Returns an error if the key is already registered
    // or the definition is invalid.
    Register(def KeyDef) error

    // MustRegister adds a key definition and panics on error.
    // Intended for use in init() or application startup where failure is fatal.
    MustRegister(def KeyDef)

    // Get returns the key definition for the given key name.
    // The bool is false if the key is not registered.
    Get(key string) (KeyDef, bool)

    // List returns all registered key definitions of the given kind.
    List(kind Kind) []KeyDef

    // Validate checks a candidate value against the registered validator for the given key.
    // Returns an error if the key is not registered or the value is invalid.
    Validate(key string, value any) error
}
```

### Ports

```go
// WriteOp represents one key mutation within an atomic patch batch.
// When Reset is true, the override for this key is deleted and the effective
// value falls back to the next level in the inheritance chain.
type WriteOp struct {
    Key   string
    Value any
    Reset bool
}

// ReadResult is the response from reading all entries for a target.
type ReadResult struct {
    Entries  []Entry
    Revision Revision
}

// Store is the generic persistence port for configs/settings.
// It persists overrides only; code defaults are never stored.
type Store interface {
    // Get returns all stored overrides for the given target and the current
    // set-level revision for that target.
    Get(ctx context.Context, target Target) (ReadResult, error)

    // Put atomically writes a batch of operations against a single target.
    // expected is the caller's last-known revision for optimistic concurrency.
    // Returns the new revision on success.
    // Unknown keys must be rejected before persistence.
    // Reset=true deletes the override row/document for that key.
    Put(ctx context.Context, target Target, ops []WriteOp, expected Revision, actor Actor, source string) (Revision, error)
}

// HistoryEntry represents one recorded mutation in the history store.
type HistoryEntry struct {
    Revision  Revision
    Key       string
    Scope     Scope
    SubjectID string
    OldValue  any
    NewValue  any
    ActorID   string
    ChangedAt time.Time
}

// HistoryFilter constrains history queries.
type HistoryFilter struct {
    Kind      Kind
    Scope     Scope
    SubjectID string
    Key       string
    Limit     int
    Offset    int
}

// HistoryStore is the read surface for listing runtime KV mutation history.
// Write ownership belongs to Store.Put, which appends history records as part
// of the same backend write boundary as the active override mutation.
type HistoryStore interface {
    ListHistory(ctx context.Context, filter HistoryFilter) ([]HistoryEntry, error)
}

// ChangeSignal is an advisory invalidation notification.
// It tells the supervisor which logical target changed and what revision to
// resync from. The store remains the only source of truth.
type ChangeSignal struct {
    Target   Target
    Revision Revision
}

// ChangeFeed is the invalidation feed contract.
// The feed is advisory only. On change, the supervisor reloads canonical state
// from the store and builds a candidate bundle.
type ChangeFeed interface {
    Subscribe(ctx context.Context, handler func(ChangeSignal)) error
}

// IdentityResolver extracts actor and tenant identity from the service context.
// The core library must not import application-specific auth packages.
type IdentityResolver interface {
    Actor(ctx context.Context) (Actor, error)
    TenantID(ctx context.Context) (string, error)
}

// Authorizer checks whether the current context has a given permission.
// Authorization is resource/action based; the library never hardcodes role names.
type Authorizer interface {
    Authorize(ctx context.Context, permission string) error
}

// BundleFactory is the application-provided contract for constructing a
// RuntimeBundle from a snapshot. The library owns supervisor mechanics;
// applications own bundle construction.
type BundleFactory interface {
    Build(ctx context.Context, snap Snapshot) (RuntimeBundle, error)
}

// BundleReconciler is a post-swap hook for worker/module start-stop behavior.
// Each reconciler is named for observability and receives both the previous
// and candidate bundles along with the new snapshot.
type BundleReconciler interface {
    Name() string
    Reconcile(ctx context.Context, previous RuntimeBundle, candidate RuntimeBundle, snap Snapshot) error
}
```

### Service

```go
// Subject identifies the scope and optional tenant for a settings operation.
type Subject struct {
    Scope     Scope
    SubjectID string
}

// PatchRequest carries a batch of write operations with optimistic concurrency.
type PatchRequest struct {
    Ops              []WriteOp
    ExpectedRevision Revision
    Actor            Actor
    Source           string
}

// WriteResult is the response from a successful patch operation.
type WriteResult struct {
    Revision Revision
}

// ResolvedSet is the response from reading effective configs or settings.
// Contains the fully resolved effective values and the current target revision.
type ResolvedSet struct {
    Values   map[string]EffectiveValue
    Revision Revision
}

// SchemaEntry is one key's metadata as presented in schema endpoint responses.
// Generated from registry metadata only; never inferred outside the registry.
type SchemaEntry struct {
    Key              string
    Kind             Kind
    AllowedScopes    []Scope
    ValueType        ValueType
    DefaultValue     any
    MutableAtRuntime bool
    ApplyBehavior    ApplyBehavior
    Secret           bool
    RedactPolicy     RedactPolicy
    Description      string
    Group            string
}

// Manager orchestrates read/write operations for configs and settings.
// It handles registry validation, effective-value resolution, optimistic concurrency,
// override persistence, apply-behavior escalation, and supervisor reload/reconcile
// triggering.
type Manager interface {
    // GetConfigs returns the fully resolved effective values for all global configs.
    GetConfigs(ctx context.Context) (ResolvedSet, error)

    // GetSettings returns the fully resolved effective values for the given subject.
    // Tenant scope: code default -> optional global setting override -> tenant override.
    // Global scope: code default -> global override.
    GetSettings(ctx context.Context, subject Subject) (ResolvedSet, error)

    // PatchConfigs atomically writes a batch of config overrides.
    // Always targets the global configs set. Escalates apply behavior across the batch.
    PatchConfigs(ctx context.Context, req PatchRequest) (WriteResult, error)

    // PatchSettings atomically writes a batch of setting overrides for the given subject.
    // Tenant writes resolve tenant identity from auth context only.
    // Global writes require explicit scope selection and admin-level permissions.
    PatchSettings(ctx context.Context, subject Subject, req PatchRequest) (WriteResult, error)

    // GetConfigSchema returns registry-derived metadata for all config keys.
    GetConfigSchema(ctx context.Context) ([]SchemaEntry, error)

    // GetSettingSchema returns registry-derived metadata for all setting keys.
    GetSettingSchema(ctx context.Context) ([]SchemaEntry, error)

    // GetConfigHistory returns mutation history for config keys.
    GetConfigHistory(ctx context.Context, filter HistoryFilter) ([]HistoryEntry, error)

    // GetSettingHistory returns mutation history for setting keys.
    GetSettingHistory(ctx context.Context, filter HistoryFilter) ([]HistoryEntry, error)

    // Resync forces a full reload from the store and rebuilds the active snapshot.
    // Used as a break-glass recovery mechanism.
    Resync(ctx context.Context) error
}

// Supervisor owns candidate bundle rebuild and atomic swap.
// It supports all apply behaviors in the classification matrix.
type Supervisor interface {
    // Current returns the active runtime bundle.
    Current() RuntimeBundle

    // Snapshot returns the active runtime snapshot.
    Snapshot() Snapshot

    // PublishSnapshot atomically publishes a new active snapshot.
    // Used for live-read changes that do not require a bundle rebuild.
    PublishSnapshot(ctx context.Context, snap Snapshot, reason string) error

    // ReconcileCurrent runs reconciler hooks against the current bundle with a new snapshot.
    // Used for worker-reconcile changes that do not require a full bundle rebuild.
    ReconcileCurrent(ctx context.Context, snap Snapshot, reason string) error

    // Reload triggers a full cycle: load from store, build candidate snapshot,
    // build candidate bundle, validate, swap atomically, run reconcilers.
    // If any step fails, the previous snapshot and bundle remain active.
    Reload(ctx context.Context, reason string) error
}

// SnapshotBuilder materializes effective values from code defaults and stored overrides.
// Resolution order:
//   - configs: code default -> global override
//   - settings: code default -> optional global setting override -> tenant override
type SnapshotBuilder interface {
    // Build constructs an immutable snapshot from the registry and stored overrides.
    Build(ctx context.Context, registry Registry, overrides []ReadResult) (Snapshot, error)
}
```

---

## 4. HTTP Contract

### Endpoints

#### Config endpoints

| Method | Path | Permission | Description |
|--------|------|-----------|-------------|
| `GET` | `/v1/system/configs` | `system/configs:read` | Returns effective values for all global configs. Response includes effective value, default, override, source, the current global configs set revision, and redaction-aware fields. |
| `PATCH` | `/v1/system/configs` | `system/configs:write` | Accepts partial KV updates. Uses `If-Match` header with the current global configs set revision for optimistic concurrency. `null` value resets override and falls back to code default. |
| `GET` | `/v1/system/configs/schema` | `system/configs/schema:read` | Returns registry-derived metadata for all config keys. Generated from registry, not from duplicated DTO helpers. |
| `GET` | `/v1/system/configs/history` | `system/configs/history:read` | Returns runtime KV mutation history for configs. Not file mutation history. |
| `POST` | `/v1/system/configs/reload` | `system/configs/reload:write` | Temporary break-glass endpoint. Forces a full resync from the store and candidate bundle rebuild. Removed once change feeds are stable. |

#### Settings endpoints

| Method | Path | Permission (tenant scope) | Permission (global scope) | Description |
|--------|------|--------------------------|--------------------------|-------------|
| `GET` | `/v1/system/settings` | `system/settings/tenant:read` | `system/settings/global:read` | Returns effective values. Tenant scope is the default. Global scope requires explicit `scope=global` query parameter. |
| `PATCH` | `/v1/system/settings` | `system/settings/tenant:write` | `system/settings/global:write` | Accepts partial KV updates. Uses `If-Match` with the current target-set revision. Tenant identity from auth context only, never from body/path/query. |
| `GET` | `/v1/system/settings/schema` | `system/settings/schema:read` | `system/settings/schema:read` | Returns registry-derived metadata for all setting keys. |
| `GET` | `/v1/system/settings/history` | `system/settings/history:read` | `system/settings/history:read` | Returns runtime KV mutation history for settings. |

### Scope rules

- **Tenant scope is the default operating mode** for `/v1/system/settings`.
- Tenant identity is resolved from auth context only. Tenant writes must not accept tenant ID in body, path, or query.
- **Global operations require explicit `scope=global`** query parameter and must be authorized with admin-level global settings permissions (`system/settings/global:*`), not tenant settings permissions.
- Global setting overrides are a higher-scope layer above tenant overrides. They are admin-only.
- **Configs are always global scope.** There is no tenant-scoped config concept.

### Reset semantics

- `PATCH` with `null` value means reset (delete the override).
- **Config reset**: falls back to code default.
- **Tenant setting reset**: falls back to global setting override, or code default if no global override exists.
- **Global setting reset**: falls back to code default.

### Response shape — GET (configs and settings)

```json
{
  "revision": 17,
  "values": {
    "rate_limit.requests_per_second": {
      "key": "rate_limit.requests_per_second",
      "value": 1000,
      "default": 500,
      "override": 1000,
      "source": "global-override",
      "revision": 17,
      "redacted": false
    },
    "idempotency.hmac_secret": {
      "key": "idempotency.hmac_secret",
      "value": "****",
      "default": "****",
      "override": "****",
      "source": "global-override",
      "revision": 17,
      "redacted": true
    }
  }
}
```

### Request shape — PATCH

```json
{
  "values": {
    "rate_limit.requests_per_second": 2000,
    "callback_rate_limit.per_minute": null
  }
}
```

Header: `If-Match: 17`

### Response shape — PATCH

```json
{
  "revision": 18
}
```

### Response shape — Schema

```json
{
  "entries": [
    {
      "key": "rate_limit.requests_per_second",
      "kind": "config",
      "allowed_scopes": ["global"],
      "value_type": "int",
      "default_value": 500,
      "mutable_at_runtime": true,
      "apply_behavior": "live-read",
      "secret": false,
      "redact_policy": "none",
      "description": "Maximum requests per second for the global rate limiter",
      "group": "rate_limit"
    }
  ]
}
```

### Response shape — History

```json
{
  "entries": [
    {
      "revision": 18,
      "key": "rate_limit.requests_per_second",
      "scope": "global",
      "subject_id": "",
      "old_value": 1000,
      "new_value": 2000,
      "actor_id": "user-abc-123",
      "changed_at": "2026-03-14T10:30:00Z"
    }
  ]
}
```

Secret fields are redacted in both GET responses and history output according to their `RedactPolicy`.

### Error vocabulary

The system plane uses a stable, small error vocabulary. All error responses use a consistent shape.

| Error code | HTTP status | Meaning |
|-----------|-------------|---------|
| `system_key_unknown` | 400 | One or more keys in the request are not registered in the registry. |
| `system_value_invalid` | 400 | One or more values failed registry-driven validation. |
| `system_revision_mismatch` | 409 | The `If-Match` revision does not match the current target-set revision. |
| `system_scope_invalid` | 400 | The requested scope is not valid for the target kind or the requested keys. |
| `system_permission_denied` | 403 | The caller lacks the required permission for the requested operation. |
| `system_reload_failed` | 500 | A resync/reload operation failed. The previous bundle remains active. |

### Error response shape

```json
{
  "code": "system_value_invalid",
  "message": "validation failed for key 'rate_limit.requests_per_second': value must be a positive integer",
  "details": {
    "key": "rate_limit.requests_per_second",
    "violation": "value must be a positive integer"
  }
}
```

### Transport contract rules

- All secret fields are redacted in reads and history output.
- Unknown keys fail fast before persistence.
- Transport must never infer schema outside the registry.
- Transport must never infer tenant identity from client-supplied fields for tenant-scoped settings.

---

## 5. Bootstrap Env Contract

These environment variables configure only the `pkg/systemplane` runtime-plane store/feed bootstrap. They are read once at process startup. They are never mirrored into `/v1/system/configs`. Application runtime keys such as `postgres.*`, `redis.*`, `rabbitmq.*`, and `object_storage.*` are part of the runtime plane and are distinct from these bootstrap variables.

### Backend selection

| Variable | Required | Values | Description |
|----------|----------|--------|-------------|
| `SYSTEMPLANE_BACKEND` | Yes | `postgres` or `mongodb` | Selects the backend adapter family for the runtime store, history store, and change feed. |

### PostgreSQL backend variables

Required when `SYSTEMPLANE_BACKEND=postgres`.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SYSTEMPLANE_POSTGRES_DSN` | Yes | (none) | Connection string for the PostgreSQL instance hosting the runtime store. |
| `SYSTEMPLANE_POSTGRES_SCHEMA` | No | `system` | PostgreSQL schema for runtime tables. |
| `SYSTEMPLANE_POSTGRES_ENTRIES_TABLE` | No | `runtime_entries` | Table name for active override entries. |
| `SYSTEMPLANE_POSTGRES_HISTORY_TABLE` | No | `runtime_history` | Table name for mutation history records. |
| `SYSTEMPLANE_POSTGRES_REVISION_TABLE` | No | `runtime_revisions` | Table name for per-target revision state used for optimistic concurrency and feed resync. |
| `SYSTEMPLANE_POSTGRES_NOTIFY_CHANNEL` | No | `systemplane_changes` | PostgreSQL LISTEN/NOTIFY channel name for invalidation signals. |

### MongoDB backend variables

Required when `SYSTEMPLANE_BACKEND=mongodb`.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SYSTEMPLANE_MONGODB_URI` | Yes | (none) | Connection URI for the MongoDB instance hosting the runtime store. |
| `SYSTEMPLANE_MONGODB_DATABASE` | No | `systemplane` | Database name for runtime collections. |
| `SYSTEMPLANE_MONGODB_ENTRIES_COLLECTION` | No | `runtime_entries` | Collection name for active override entries. |
| `SYSTEMPLANE_MONGODB_HISTORY_COLLECTION` | No | `runtime_history` | Collection name for mutation history records. |
| `SYSTEMPLANE_MONGODB_WATCH_MODE` | No | `change_stream` | Change propagation mode: `change_stream` or `poll`. Use `poll` only when deployment constraints prevent reliable change stream usage. |
| `SYSTEMPLANE_MONGODB_POLL_INTERVAL_SEC` | No | `5` | Polling interval in seconds when `SYSTEMPLANE_MONGODB_WATCH_MODE=poll`. Ignored in `change_stream` mode. |

### Bootstrap config structs

These structs map directly to the env vars above and are the only configuration surface for `pkg/systemplane` itself:

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
    RevisionTable string
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

### Rules

- These env vars configure only the runtime-plane store/feed bootstrap.
- They are never mirrored into `/v1/system/configs` or `/v1/system/settings`.
- Application runtime keys (`postgres.*`, `redis.*`, `rabbitmq.*`, `object_storage.*`) remain part of the runtime plane and must not be confused with the `SYSTEMPLANE_*` bootstrap configuration.
- Both PostgreSQL and MongoDB are first-class backends. The library must function identically regardless of which backend is selected.
- When `SYSTEMPLANE_BACKEND` is not set, the bootstrap must fail with a clear error, not silently default to either backend.
