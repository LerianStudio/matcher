# Matcher — Simplify Sweep Report

## Simplify Summary

- **Codebase:** `github.com/LerianStudio/matcher` (transaction reconciliation engine)
- **Scope:** Whole codebase (`/Users/fredamaral/repos/lerianstudio/matcher`)
- **Generated:** 2026-04-21
- **Method:** 4 parallel explorer agents (interface hunter, translation-layer hunter, architecture topology mapper, cascade detector)
- **Total candidates identified:** 38 distinct findings (after cross-explorer deduplication of 5 triangulated cases)
- **Kill list:** 22 items | **Review list:** 12 items | **Keep list:** 14 items
- **Estimated collapse:** ~5,000–6,000 LOC across ~350 files (net ~4% of internal/) with zero public-API impact. Second-wave (repository-interface collapse) adds another ~3,000 LOC.

---

## Hard Constraint

**Declared constraint:** Public HTTP routes + database schema.

**Load-bearing surface:**

| Surface | Location | Notes |
|---|---|---|
| 114 protected HTTP routes | `internal/{context}/adapters/http/routes.go` (7 files) + `internal/bootstrap/routes.go` | Swagger-documented at `docs/swagger/swagger.json`. Consumed by Next.js console (internal same-repo) and any external client. |
| Database schema | `components/db/migrations/` (32 migrations) | Append-only; rollback pairs required |
| OpenAPI spec | `docs/swagger/swagger.json` | Generated from handler annotations |
| HTTP JSON tag conventions | DTOs under `internal/{context}/adapters/http/dto/` | **Drift detected** (camelCase vs snake_case); see REVIEW item R-06 |
| CLI entry | `cmd/matcher/main.go` | Single entry binary |

**Touch policy:** Nothing in the load-bearing list is modified. Internal changes that preserve this surface are permitted.

---

## Kill List

HIGH-confidence, no public-API impact, ready for `ring:dev-cycle`. Each row is one task unit; cascade groupings (Kxx) indicate items that must collapse together.

| # | Name | file:line | Smell | Rebuttal | Blast radius | Action |
|---|---|---|---|---|---|---|
| K-01 | `SwappableLogger` | `internal/bootstrap/swappable_logger.go:19` | One-consumer facade with unused swap affordance | `state.swap()` has one call site (the constructor itself). No exported `Swap` method. CLAUDE.md explicitly documents: "Runtime log-level swapping is not implemented — changing `LOG_LEVEL` requires a process restart." The code ships a mechanism the docs tell operators doesn't work. | 255 LOC (128 prod + 127 test) | **DELETE** file + test; replace `logger = NewSwappableLogger(logger)` at `init.go:438` with the underlying logger |
| K-02 | `ingestion/ports.Dispatcher` | `internal/ingestion/ports/dispatcher.go:7` | Dead speculative interface | **Zero production implementers.** Only a throwaway `mockDispatcher` in tests. | 1 port file + 1 test | **DELETE** port + test |
| K-03 | `matching/ports.FXSource` | `internal/matching/ports/fx_source.go:12` | Dead speculative interface | **Zero production implementers, zero production callers.** File comment says "Only implement/wire it if matching must fetch rates at runtime." YAGNI confession committed to the repo. | 1 port file | **DELETE** port |
| K-04 | `internal/governance/ports/` | `internal/governance/ports/doc.go` | Empty scaffolding | Directory contains only `doc.go` — reserved real estate for seams that have never materialized. | 1 directory | **DELETE** directory; inline governance repositories directly into query/command UseCases |
| K-05 | `internal/matching/domain/doc.go` | `internal/matching/domain/doc.go` | Dead file | 2-line file with no referents | 1 file | **DELETE** |
| K-06 | **Cascade K-06: shared ports single-impl cluster** | — | Single-implementation ports | **15 shared/ports interfaces, each with exactly 1 production impl, 0 alternate impls, 0 swap tests with divergent behavior.** File comments cite "break cross-context coupling" — depguard-rule-shaped justification, not behavior-shaped. Fix the rules with carve-outs (20 lines); don't pay thousands of lines for unnecessary indirection. | ~40 files, ~1,200 LOC | **DELETE** ports + cross-adapters; expose concrete types |
| K-06a | ↳ `MatchTrigger` | `internal/shared/ports/match_trigger.go:15` | 1 impl wraps one `RunMatch` goroutine invocation | `cross.MatchTriggerAdapter` is 40 lines. | 3 callers | Inline or reduce to function type |
| K-06b | ↳ `ContextProvider` | `internal/shared/ports/match_trigger.go:24` | 1 impl, 3-line body (`AutoMatchOnUpload && IsActive`) | Port exists only because ingestion can't import configuration/domain | 2 callers | Inline; depguard carve-out for read-only config access |
| K-06c | ↳ `ContextAccessProvider` | `internal/shared/ports/context_access.go:20` | 1 impl, 2-field projection | — | 2 HTTP verifiers | Inline as helper in `shared/adapters/http/` |
| K-06d | ↳ `TenantLister` | `internal/shared/ports/tenant_lister.go:7` | 1 impl (existing partition/tenant manager) | No switch; 4 workers receive the same concrete type | 4 workers | Pass concrete tenant manager directly |
| K-06e | ↳ `M2MProvider` | `internal/shared/ports/m2m.go:19` | 1 impl | AWS Secrets vs env-var are internal details of one adapter | 3 callers | Export concrete provider |
| K-06f | ↳ `FetcherBridgeIntake` | `internal/shared/ports/fetcher_bridge.go:78` | 1 impl, 1-method port, 1-method adapter calling ingestion use case | Bootstrap + discovery bridge only | 2 callers | Collapse with depguard carve-out for bridge path |
| K-06g | ↳ `ExtractionLifecycleLinkWriter` | `internal/shared/ports/fetcher_bridge.go:112` | 1 impl wraps 1 repo call + state-machine method | — | 2 callers | Move atomic UPDATE as method on extraction repo |
| K-06h | ↳ `BridgeOrchestrator` | `internal/shared/ports/bridge_orchestrator.go:90` | 1 impl with 5+ collaborators; sole consumer is bridge worker | Same context owns both sides | 2 callers | Worker depends on concrete orchestrator |
| K-06i | ↳ `BridgeSourceResolver` | `internal/shared/ports/bridge_orchestrator.go:105` | 1 impl, JOIN indirection to dodge imports | — | 1 caller | Keep SQL as repo method; orchestrator calls directly |
| K-06j | ↳ `ArtifactRetrievalGateway` + `ArtifactTrustVerifier` + `ArtifactCustodyStore` + `CustodyKeyBuilder` | `internal/shared/ports/artifact_custody.go` | 4 interfaces for 4 impls, 4 consumers, 1 backend | No "second artifact source" exists or is planned | ~1,500 LOC across fetcher bridge cluster | Collapse into a cohesive custody package with concrete types |
| K-06k | ↳ `IngestionEventPublisher` | `internal/shared/ports/ingestion_events.go:12` | 1 impl (RabbitMQ) | Comment cites "break outbox→ingestion coupling"; only one bus | ~5 callers | Inline or drop |
| K-06l | ↳ `IdempotencyRepository` | `internal/shared/ports/idempotency.go:11` | 1 impl (Redis) | No second store | 2 callers | Consumers take concrete Redis type |
| K-07 | **Cascade K-07: matching ports single-impl cluster** | — | Single-implementation ports | 7 matching ports wrap `configRepositories.*Repository` one-to-one. Each adapter delegates single calls with identical field sets. `MatchingConfigurationProvider` is a 380-line facade producing 4 wrappers that each implement a different port with 1 consumer (matching UseCase). **Triangulated by Cluster 1 + Cluster 2 + Cluster 3.** | ~15 files | **DELETE** ports + cross-adapters |
| K-07a | ↳ `matching/ports.ContextProvider` | `internal/matching/ports/context_provider.go:27` | 1 impl | 6-field copy | 1 caller | — |
| K-07b | ↳ `matching/ports.SourceProvider` | `internal/matching/ports/source_provider.go:37` | 1 impl | Pagination glue belongs in repo | 1 caller | — |
| K-07c | ↳ `matching/ports.MatchRuleProvider` | `internal/matching/ports/match_rule_provider.go:18` | 1 impl | Mirror of SourceProvider | 1 caller | — |
| K-07d | ↳ `matching/ports.FeeRuleProvider` | `internal/matching/ports/fee_rule_provider.go:13` | 1 impl | 9-line delegation | 1 caller | — |
| K-07e | ↳ `matching/ports.ExceptionCreator` | `internal/matching/ports/exception_creator.go:27` | Postgres impl lives inside matching itself | — | 1 caller | UseCase takes concrete `*Repository` |
| K-07f | ↳ `matching/ports.TransactionRepository` | `internal/matching/ports/transaction_repository.go:24` | Wraps ingestion tx repo with zero transformation | **Triangulated** (Cluster 1 + Cluster 2 + Cluster 4) | 1 caller | Promote repo to shared or allow matching → ingestion read |
| K-07g | ↳ `MatchingConfigurationProvider` facade | `internal/shared/adapters/cross/matching_adapters.go:26` | 380-line aggregate facade | Collapses naturally once K-07a–d die | — | **DELETE** with the cluster |
| K-08 | `cross.BaseTransactionRepository` interface | `internal/shared/adapters/cross/transaction_repository_adapter.go:29` | Interface declared to "enable mocking" | **Triangulated.** No production alternative. Existing integration tests (55 added 2026-02-06 per memory) cover the behavior. | ~400 LOC (incl. sqlmock tests of pure glue) | **DELETE**; use concrete `*ingestionTxRepo.Repository` |
| K-09 | `cross.ExceptionTransactionRepository` interface | `internal/shared/adapters/cross/exception_matching_gateway.go:38` | Same pattern as K-08 | 1 prod impl + 1 mock | ~6 LOC interface + adapter-glue tests | **DELETE** |
| K-10 | `cross.ExceptionContextLookup` + `TransactionFinder` + `JobFinder` + `SourceContextFinder` | `internal/shared/adapters/cross/exception_context_lookup.go:16` | 4 interfaces declared in same file as their sole concrete consumer | Single-impl, single-consumer cluster declared in-file; no substitution exercised | ~55 LOC | **DELETE** interfaces; keep `TransactionContextLookup` struct + `mapSourceLookupError` helper |
| K-11 | Redundant bootstrap provider constructors | `internal/bootstrap/init.go:3003-3021` | Duplicate construction | `initMatchingModule` creates `configProvider` with 4 repos, then calls `NewSourceProviderAdapter(repos.configSource)` and `NewFeeRuleProviderAdapter(repos.configFeeRule)` which each construct ANOTHER `MatchingConfigurationProvider` with 3 nils. | 5-line fix + can delete 2 convenience constructors | Replace with `configProvider.SourceProvider()` / `configProvider.FeeRuleProvider()` |
| K-12 | `FieldMap` + `ReconciliationSource` cross-kernel struct duplication | `internal/shared/domain/field_map.go` ↔ `internal/configuration/domain/entities/field_map.go` | Translation-free adapter | Structs are byte-identical; 2 `toShared(...)`/`fromShared(...)` copy functions exist purely to translate between identical shapes. | ~30 LOC + 2 copy functions | **DELETE** duplicates via type alias to `shared/domain/` |
| K-13 | `configuration/ports.FeeScheduleRepository` duplication | `internal/configuration/ports/fee_schedule.go:14` | Identical interface redeclared | **Byte-identical** to `matching/domain/repositories.FeeScheduleRepository`. Two definitions exist because the two packages cannot import each other — exact proof that "clean architecture" produced duplication, not decoupling. | 2 files → 1 | Collapse via type alias to single definition in `internal/shared/domain/fee/` |
| K-14 | `MatchRuleProviderAdapter` translation-free wrapper | `internal/shared/adapters/cross/matching_adapters.go` | Translation-free adapter | Wraps paginated call with zero transformation | 40 LOC | Fold into a pagination helper or delete with K-07c |
| K-15 | Standalone source/fee-rule constructors (redundant wiring style) | `internal/shared/adapters/cross/matching_adapters.go:135,149` | Redundant factory | 2 constructors always produce the same provider shape as `MatchingConfigurationProvider.*Provider()` accessors | 2 constructors | **DELETE** once wiring style is unified |
| K-16 | `TrustedContentInput` / `IngestFromTrustedStreamInput` duplication | `internal/shared/ports/fetcher_bridge.go` + `internal/ingestion/services/command/trusted_stream_commands.go` | Internal DTO ↔ identical input struct | Identical field sets with identical types; adapter body is 27 lines of 1:1 field copy | 27 LOC adapter body → 1 line | Type alias |
| K-17 | `pkg/errors.go` ten typed-error wrapper structs | `pkg/errors.go` | Speculative pattern-matching surface | **Only `APIError` interface and `NewError()` factory are used externally.** Zero call sites do `errors.As(&NotFoundError{}, err)` to distinguish categories. Wrappers survive via a nil-check idiom in tests (`var typedNil *matchererrors.NotFoundError`) — which validates type existence, not behavior. | ~100 LOC + 3 test file updates | **DELETE** 10 wrappers + constructors; keep `BaseError`, `APIError` interface, `Definition`, `NewError()` |
| K-18 | Span-only Query UseCases (governance, configuration, exception, ingestion, reporting) | `internal/governance/services/query/actor_mapping_queries.go` + parallels | One-consumer facade | 15-20 read paths (T1/T2/T3/T9/T10 and siblings) where the UseCase body is a span wrapper + direct repo call with error wrap. Nothing else. Tracing already happens inside `pgcommon.WithTenantReadQuery`. | ~1,500 LOC across 5 contexts | **COLLAPSE** into direct handler→repo calls; handler owns the outer span |
| K-19 | Type-alias shim files in `{context}/domain/value_objects/` | `internal/exception/domain/value_objects/exception_severity.go` + peers | Backwards-compat shim | 45-line files re-exporting `shared/domain/exception/severity.go`. "Backward compat" in a pre-public app with no external consumers = dead wrapper. | ~45 LOC per file | **DELETE**; migrate callers to import `shared/domain/*` directly |
| K-20 | `BaseTransactionRepository` + `TransactionRepositoryAdapter` non-Tx pass-throughs | `internal/shared/adapters/cross/transaction_repository_adapter.go:80-172,323-342` | Pass-through shim | 5 non-`WithTx` methods are 22-line pass-throughs. The `*WithTx` variants do real work (span + `WithTenantTxOrExistingProvider`). | ~5 methods | Drop the non-WithTx methods or use struct-embedding of base repo |
| K-21 | `configuration/domain/value_objects/rule_type.go` + `context_type.go` Parse wrappers | Config domain aliases | Config seam over constant | Aliases `shared.ParseRuleType` with `fmt.Errorf("parsing rule type: %w", err)` — one extra error-wrap layer, no added context | 2 files × ~30 LOC | **DELETE** Parse wrappers; keep type aliases (bounded-context vocabulary is legitimate) |
| K-22 | `pkg/chanutil` + `pkg/storageopt` single-use helpers | `pkg/chanutil/` (20 LOC), `pkg/storageopt/` (22 LOC) | One-consumer facade | Each under 25 LOC, each used by ≤6 callers. Evaluate case-by-case against inline cost. | ~42 LOC | **REVIEW** for inline vs keep — marginal kill |

---

## Review List

MEDIUM confidence, requires caller coordination, touches cascade chains, or crosses a public-API boundary.

| # | Name | file:line | Smell | Why uncertain | Public-API impact | Recommended next step |
|---|---|---|---|---|---|---|
| R-01 | **Repository-over-single-SQL-backend collapse** | 23 interfaces across all contexts (`internal/*/domain/repositories/`) | Repository over a single SQL backend | **Every one of 23 repo interfaces has exactly one postgres implementation. No mongo, no in-memory, no recorded intent to add a second store.** But collapsing means ~250 import sites and ~23 mock files to update. Interfaces leak SQL shape (`*sql.Tx` aliases) — they aren't genuinely abstract anyway. | none | Dedicated refactor pass. Keep `WithTx` methods (real ergonomic need). Keep sqlmock tests (they test SQL, not interface contracts). ~3,000 LOC savings. |
| R-02 | Postgres UUID-as-string round-trip | ~15 adapter model files (`internal/*/adapters/postgres/{aggregate}/{name}.go`) | Translation tax without semantic divergence | Every read does `uuid.Parse(...)`, every write does `.String()`. Postgres driver scans `uuid.UUID` natively into `UUID` columns. But the refactor touches 15 files + tests and is mechanical, not conceptual. | none | Dedicated batch pass: `string` → `uuid.UUID`, `sql.NullString` → `uuid.NullUUID` in model fields; delete `uuid.Parse`/`.String()` in `NewPostgreSQLModel`/`ToEntity` |
| R-03 | Fetcher-bridge 4-port cascade | `internal/shared/ports/fetcher_bridge.go` + `bridge_orchestrator.go` + `bridge_source_resolver.go` + 4 adapters | Ports-and-adapters cluster for a single cross-context pipeline | ~900 LOC of ports + adapters to serve one bridge worker. Real terminal (worker loop), but speculative middle rings. **This was an explicit architectural decision** during T-001/T-002 (per MEMORY.md fetcher bridge notes). Collapsing requires depguard relaxation for this bridge path. | indirect | Fred decision: does the `cross-context-*` depguard rule have a carve-out for bridge paths? If yes, ~900 LOC collapses. |
| R-04 | `AutoMatchContextProviderAdapter` + `ContextProvider` port | `internal/shared/adapters/cross/auto_match_adapters.go:51-73` | Cross-context shim over 3-line body | Depguard-rule-shaped. Collapse requires allowing ingestion → configuration read. | none | Tied to R-03 decision |
| R-05 | `InfrastructureProvider` | `internal/shared/ports/infrastructure.go:174` | Single-impl port | Deeply embedded in every postgres adapter (63 files). IS the tenant-isolation enforcement point. Collapsing means every pg adapter takes `*sql.DB + *redis.Client + tenant resolver` — more painful than the seam. | indirect | **Keep** for now; revisit after multi-tenant maturity. Documented as earned-but-saturated. |
| R-06 | Public-API JSON tag drift (snake_case vs camelCase) | Governance `ArchiveMetadataResponse`, `ActorMappingResponse`; Exception `BulkResolveRequest`/`BulkFailure` | Convention inconsistency | Most DTOs are `camelCase`; governance + exception bulk are `snake_case`. Drift is at-boundary — silent rename would break clients. | **at-boundary** | Freeze current tags OR migrate to `camelCase` with dual-tag + deprecation. Do not silently rename. |
| R-07 | `ObjectStorageClient` port | `internal/shared/ports/object_storage.go:40` | Single backend (S3/SeaweedFS) behind port | Two "impls" at runtime but both front the same backend; the second is a hot-swap lifecycle wrapper, not a behavioral alternative. | none | Lean DELETE; requires replacing bootstrap's dynamic wrapper with atomic-pointer swap |
| R-08 | `ExtractionJobPoller` + `SchemaCache` | `internal/discovery/ports/extraction_poller.go:10` + `schema_cache.go:12` | Multiple wrappers (dynamic/noop/provider-backed) | Not behavioral swap — lifecycle wrappers for hot-reload. Deletion needs atomic-pointer swap replacement. | none | Same pattern as R-07 |
| R-09 | `AuditLogRepository` | `internal/shared/ports/audit.go:20` | Single-impl port | Cross-context append-only store used by matching, exception, governance. "Audit log is owned by governance" is a semantic boundary, not depguard theatre. Slack-worthy. | none | Probably kill in favor of concrete governance-owned type exported upward |
| R-10 | Exception's 5 UseCase structs | `internal/exception/services/command/` | Multi-struct without shared state | 5 separate UseCase structs (exception / dispute / dispatch / comment / callback), 5 constructors, 5 dep sets. Could be one grouped UseCase. Saves ~200 LOC of wiring. | none | Refactor pass; no functional change |
| R-11 | `internal/bootstrap/init.go` monolith | `internal/bootstrap/init.go` (3,564 lines, 96 funcs, 14 `initXxxModule`) | Pending decomposition | Seam missing. Subfiles (`init_discovery.go`, `init_fetcher_bridge.go`, `init_startup_probe.go`, `init_status_archival.go`) already prove the pattern works by topic. Split by module next. | none | Split into `init_{context}.go` — target no single file > 500 LOC |
| R-12 | Per-aggregate PG sub-packages | 26 sub-packages under `internal/{context}/adapters/postgres/{aggregate}/` | Directory convention without payoff | Each sub-package has exactly 3 identically-shaped files (`{name}.go`, `{name}.postgresql.go`, `errors.go`). Merging into `postgres/{context}/` removes 26 subdirs + 26 `errors.go` boilerplates. ~400 LOC saved. | none | Contentious — the subdirs aid navigation in IDEs. Flatten only where the aggregate has no unique complexity. |

---

## Keep List

Earned abstractions. Stop questioning these until evidence changes.

| Name | file:line | Smell category resembled | Evidence that justifies it |
|---|---|---|---|
| `Parser` / `StreamingParser` / `ParserRegistry` | `internal/ingestion/ports/parser.go:48,71,87` | Strategy pattern | **Three divergent prod impls** (CSV, JSON, XML), each parsing a different format with different tests asserting different behavior. Genuine strategy. |
| `Lock` / `RefreshableLock` value types | `internal/matching/ports/lock_manager.go:16,24` | Value type returned from manager | Callers handle locks polymorphically; returning `Lock` as a value type is the cleanest API shape |
| `WorkerLifecycle` / `WorkerFactory` | `internal/bootstrap/worker_manager.go:33` | Factory pattern | **5+ different concrete workers** (export, cleanup, archival, scheduler, discovery) share a `Start/Stop` contract. Factory parameterized by config. Real polymorphism. |
| HTTP response DTO converters | `internal/*/adapters/http/dto/converters.go` | Translation layer | `time.Time → RFC3339`, `decimal.Decimal.String()`, `uuid.UUID.String()`, nullable pointer formatting, enum `.String()` — legitimate serialization work at the public boundary |
| HTTP request DTO `ToDomainInput` methods | `internal/*/adapters/http/dto/requests.go` | Translation layer | String → value_object enum parsing, JSON size/depth validation, deprecated field rejection (e.g., `CreateContextRequest.UnmarshalJSON` rejects `rateId`). Real input validation. |
| `governance/dto/AuditLogToResponse` + `extractTruncationMarkers` | `internal/governance/adapters/http/dto/converters.go` | Non-trivial serialization | Inspects embedded JSON envelope for truncation markers, handles malformed payloads. Real work. |
| `configuration/dto/fee_schedule.feeStructureToMap` | `internal/configuration/adapters/http/dto/fee_schedule.go` | Polymorphic serialization | Interface type-switch (`FlatFee`/`PercentageFee`/`TieredFee`) to polymorphic JSON. Real work. |
| `ConfirmablePublisher` | `internal/shared/adapters/rabbitmq/confirmable_publisher.go` | Pub with bells | Publisher confirms, channel recovery, exponential backoff. This is the library lib-commons wishes it had. |
| `ExtractionLifecycleLinkWriterAdapter.LinkExtractionToIngestion` (the method, not the port) | `internal/shared/adapters/cross/fetcher_bridge_adapters.go` | Concrete atomic op | State-machine validation + atomic SQL write + concurrency guard. Real work. Keep the struct; kill the port (K-06g). |
| `ExceptionMatchingGateway` (concrete struct, not the port) | `internal/shared/adapters/cross/exception_matching_gateway.go` | Cross-aggregate coordinator | Context resolution via transaction lookup, cross-aggregate coordination. Keep struct; kill port (K-06). |
| Postgres models handling JSONB | e.g., `match_run.Stats`, `field_map.Mapping` | Translation layer with semantic transform | `json.Marshal`/`Unmarshal` is real work — DB column is `JSONB`, domain field is structured Go type |
| `pkg/constant/errors.go` | `pkg/constant/errors.go` | Error-code catalog | Product error codes (MTC-XXXX) used across codebase. Constant table, not abstraction. |
| `pkg/errors.go` (the `APIError` interface + `NewError()` + `BaseError`, NOT the typed wrappers K-17) | `pkg/errors.go` | Error contract root | `APIError` interface + factory genuinely used by handlers; typed wrappers (K-17) are the speculative part |
| `dynamic*` bootstrap wrappers (`dynamic_fetcher_client`, `dynamic_infrastructure_provider`, `dynamic_lock_manager`, etc.) | `internal/bootstrap/dynamic_*.go` | Lifecycle wrapper | Real runtime-reconfig concern distinct from underlying client. Hot-swap via atomic pointer. |

---

## Cascade Chains

Chains where killing one abstraction cascades through the codebase.

| Chain ID | Leaf | Ring depth | Terminal type | Collapse blast radius |
|---|---|---|---|---|
| **C1 (K-01)** | `SwappableLogger` state swap | 1 | **SPECULATIVE** — `swap()` has one call site (constructor itself); no external Swap method exists; CLAUDE.md documents feature as unimplemented | 255 LOC (file + test) |
| **C2 (K-10)** | `ExceptionContextLookup` + 3 finder interfaces | 2 | REAL (exception UseCase) but interface layer exercises no substitution | ~55 LOC |
| **C3 (K-08)** | `BaseTransactionRepository` interface | 2 | REAL (matching UseCase) but interface exists only to mock production repo in adapter-glue tests | ~400 LOC |
| **C4 (K-09)** | `ExceptionTransactionRepository` interface | 2 | Same pattern as C3 | ~6 LOC |
| **C5 (K-11)** | `NewSourceProviderAdapter`/`NewFeeRuleProviderAdapter` bootstrap convenience constructors | 2 | REAL (bootstrap, tests) but creates redundant `MatchingConfigurationProvider` instances per boot | ~25 LOC + 2 redundant allocations |
| **C6 (R-03)** | Fetcher-bridge 4-port chain (`FetcherBridgeIntake` + `ExtractionLifecycleLinkWriter` + `BridgeSourceResolver` + `BridgeOrchestrator`) | 2→3 | REAL (bridge_worker) but each port has exactly 1 impl + 1 consumer; depguard-enforced | ~900 LOC |
| **C7 (K-21)** | `configuration/domain/value_objects/rule_type.go` + `context_type.go` Parse wrappers | 1 | REAL (45 consumers) but wrappers add only an extra error-wrap layer | 2 × ~30 LOC + removal of wrap layer |
| **C8 (R-04)** | `shared/ports.ContextProvider` auto-match port | 2 | REAL (ingestion auto-match) but 3-line body (`AutoMatchOnUpload && IsActive`) wrapped in port | ~200 LOC combined with adapter + test |

---

## Next Steps

### Wave 1 — pure wins (zero risk, ~300 LOC)

Feed these straight into `ring:dev-cycle` in any order. No architectural discussion required.

1. **K-01 `SwappableLogger`** — delete file + test, swap call site. 255 LOC, zero risk.
2. **K-02 + K-03 dead ports** — delete `ingestion/ports.Dispatcher` + `matching/ports.FXSource`. ~40 LOC, zero risk.
3. **K-04 empty governance/ports** — delete directory. 1 file.
4. **K-05 empty matching/domain/doc.go** — delete file. 2 lines.
5. **K-11 redundant bootstrap constructors** — 5-line fix in `initMatchingModule`.

### Wave 2 — single-impl port consolidation (~1,500–2,000 LOC)

One task per cascade group; each cascade is a self-contained collapse.

6. **K-06 shared ports cluster** (15 interfaces + adapters) — update depguard carve-outs in `.golangci.yml` first (~20 lines), then delete ports and rewire consumers to concrete types.
7. **K-07 matching ports cluster** (7 interfaces + `MatchingConfigurationProvider`) — collapse via direct config-repo imports.
8. **K-08 + K-09 + K-10 adapter-mocking interfaces** — delete; convert unit tests that tested pure glue to integration coverage (already exists per memory).
9. **K-12 + K-13 + K-16 duplicated struct/interface declarations** — type-alias to single definitions in `shared/domain/`.
10. **K-17 `pkg/errors.go` typed wrappers** — delete 10 structs + 10 constructors.
11. **K-18 span-only Query UseCases** — collapse 15-20 read paths to direct handler→repo calls; move tracing into `pgcommon.WithTenantReadQuery` (already has it).
12. **K-19 type-alias shim files** — migrate callers, delete shims.
13. **K-20 + K-21 method-level pass-throughs** — surgical.

### Wave 3 — larger refactors, require Fred's judgment

14. **R-01 repository-interface collapse** — 23 interfaces → 23 concrete structs. ~3,000 LOC saved. Mechanical but touches ~250 import sites.
15. **R-02 Postgres UUID-as-string** — ~15 adapter files + tests.
16. **R-03 + R-04 depguard carve-out decision** — does the bridge path get a cross-context exception? Unlocks ~1,100 LOC if yes.
17. **R-06 JSON tag drift** — public-API versioning decision.
18. **R-07 + R-08 dynamic wrapper replacements** — atomic-pointer swap pattern.
19. **R-10 + R-11 structural** — merge exception UseCases, split bootstrap `init.go`.

### Guidance

- **Re-run `ring:dev-simplify` after Wave 2.** Killing the ports cluster will expose new cascade chains that were invisible while wrapped in abstractions.
- **Reassess the Hard Constraint surface before each release.** The 114 protected routes will grow; add new surface to the "do not touch" list as it ships.
- **Update `.golangci.yml` depguard rules strategically.** The rules earned their keep (protecting bounded-context integrity), but the current version enforces via infinite single-impl ports. Add explicit carve-outs: `ingestion → configuration/domain` (read-only context checks), `matching → ingestion/domain` (transaction reads), bridge-specific paths. The carve-out list is the load-bearing contract, not the thousands of lines of port wrapping.

---

## Cross-Explorer Triangulation

Findings that surfaced independently in multiple explorers — evidence compounds rather than duplicates:

| Finding | Cluster 1 (interfaces) | Cluster 2 (translation) | Cluster 3 (topology) | Cluster 4 (cascade) |
|---|:-:|:-:|:-:|:-:|
| `TransactionRepositoryAdapter` (K-07f + K-08 + K-20) | ✓ | ✓ | — | ✓ |
| `MatchRuleProvider`/`SourceProvider`/`FeeRuleProvider` (K-07, K-14, K-15) | ✓ | ✓ | ✓ | — |
| `ExceptionContextLookup` + finders (K-10) | ✓ | — | — | ✓ |
| `FeeScheduleRepository` duplication (K-13) | ✓ | ✓ | — | — |
| Single-impl port epidemic (K-06, K-07) | ✓ (62 interfaces) | — | ✓ (60+ interfaces, 16/20 shared) | — |

The triangulation is the skill's primary contribution. Items that surfaced in isolation (e.g., `SwappableLogger` — Cluster 4 only) gained confidence from their own internal evidence. Items that triangulated (the port epidemic, the `TransactionRepositoryAdapter`) are maximum-confidence regardless of individual explorer depth.

---

**Status:** COMPLETE. Report ready for Wave 1 execution via `ring:dev-cycle`.
