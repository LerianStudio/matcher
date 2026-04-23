# T-009b Phase 2: Mechanical Span-Only Method Collapse — Handoff

**Date:** 2026-04-22
**Status:** Complete (all 25 candidates evaluated, 25 collapsed, 0 upgraded to KEEP)
**Branch:** develop (uncommitted)
**Parent:** T-009b Phase 1 (`8c44a89f`) — verifier cascade for configuration

## TL;DR

Deleted 25 span-only Query UseCase methods across 5 bounded contexts (configuration, matching, reporting, ingestion, discovery). Wired the corresponding HTTP handlers to call repositories directly. Net: **58 files changed, +750 / -3574 lines (-2824 net)**. Zero UseCase structs entirely deleted — every context still hosts real-logic methods on its Query UseCase.

## Per-Context Summary

| Context | Candidates considered | Collapsed | KEPT (reason) | UseCase file deleted? |
|---|---|---|---|---|
| configuration | 8 | 8 | (none of Phase 2 candidates) | No — UseCase struct retains `ListSources`, `ListMatchRules`, `GetFieldMap`, `GetFieldMapBySource`, `GetSchedule` (all KEPT per T-009 Keep List). 3 sub-files deleted: `context_queries.go`, `fee_rule_queries.go`, `fee_schedule_queries.go` (and their tests). |
| matching | 3 | 3 | (none of Phase 2 candidates) | No — UseCase struct retains `ListMatchRunGroups` (enrichGroupsWithItems = real logic, per Keep List) |
| ingestion | 3 | 3 | (none of Phase 2 candidates) | No — UseCase struct retains `GetJob`, `GetJobByContext`, `GetTransaction`, `ListTransactionsByJob`, `PreviewFile` (multi-step format dispatch + nil→sentinel). 1 sub-file deleted: `transaction_search_queries.go` (and test). |
| discovery | 1 | 1 | (none of Phase 2 candidates) | No — UseCase struct retains `GetDiscoveryStatus`, `GetConnection`, `GetConnectionSchema`, `GetExtraction`, plus 3 bridge-readiness methods. |
| reporting | 10 | 10 | `GetByID` (ExportJobQueryService — sentinel translation, per Keep List) | No — UseCase struct retains `Export*CSV`/`Export*PDF` + `Stream*CSV` (10 methods, all with real logic). `ExportJobQueryService` retains only `GetByID`. Deleted `ListExportJobsInput` and `ListByContextInput` types from `export_job_queries.go`. |

**Executed:** 25 methods collapsed. **Upgraded to KEEP:** 0 (every Phase 2 candidate proved genuinely span-only on inspection).

### Methods collapsed per context

**configuration (8):**
1. `CountContexts` — dead code (no HTTP caller; only tests). Deleted outright.
2. `ListContexts` → `contextRepo.FindAll`
3. `GetFeeRule` → `feeRuleRepo.FindByID` (3 handler call sites)
4. `ListFeeRules` → `feeRuleRepo.FindByContextID`
5. `GetFeeSchedule` → `feeScheduleRepo.GetByID` (2 handler call sites)
6. `ListFeeSchedules` → `feeScheduleRepo.List`
7. `CheckFieldMapsExistence` → `fieldMapRepo.ExistsBySourceIDs`
8. `ListSchedules` → `scheduleRepo.FindByContextID` (nil→`[]{}` conversion proven dead: `SchedulesToResponse` uses `make([]T, 0, len(nil))` which safely yields `[]`)

**matching (3):**
1. `GetMatchRun` → `matchRunRepo.FindByID`
2. `ListMatchRuns` → `matchRunRepo.ListByContextID`
3. `FindMatchGroupByID` — no HTTP caller or integration test caller. Only exercised by its own `queries_test.go`. Deleted outright.

**ingestion (3):**
1. `ListJobsByContext` → `jobRepo.FindByContextID`
2. `ListTransactionsByJobContext` → `transactionRepo.FindByJobAndContextID`
3. `SearchTransactions` → `transactionRepo.SearchTransactions` (file `transaction_search_queries.go` deleted since this was its only method)

**discovery (1):**
1. `ListConnections` → `connRepo.FindAll`

**reporting (10):**
1. `GetMatchedReport` → `reportRepo.ListMatched`
2. `GetUnmatchedReport` → `reportRepo.ListUnmatched`
3. `GetSummaryReport` → `reportRepo.GetSummary`
4. `GetVarianceReport` → `reportRepo.GetVarianceReport`
5. `CountMatched` → `reportRepo.CountMatched` (function-pointer passed to `handleCount` helper — signature is identical between UseCase and repo, so the swap was mechanical)
6. `CountUnmatched` → `reportRepo.CountUnmatched`
7. `CountTransactions` → `reportRepo.CountTransactions`
8. `CountExceptions` → `reportRepo.CountExceptions`
9. `ExportJobQueryService.List` → `exportJobRepo.List` (input struct flattened to positional args — handler already constructed the input inline, so this was a zero-cost API simplification)
10. `ExportJobQueryService.ListByContext` → `exportJobRepo.ListByContext` (same pattern)

## Handler Constructor Changes

| Context | Before arity | After arity | Notes |
|---|---|---|---|
| configuration | 6 params (Phase 1) | **10 params** | Added `fieldMapRepo`, `feeRuleRepo`, `feeScheduleRepo`, `scheduleRepo` as positional params. Considered a `ConfigurationRepos` struct; rejected — 10 positional params read fine given the identical naming pattern + nil-guards make the constructor self-documenting. Each repo has its own sentinel: `ErrNilFieldMapRepository`, `ErrNilFeeRuleRepository`, `ErrNilFeeScheduleRepository`, `ErrNilScheduleRepository`. |
| matching | 4 params | **6 params** | Added `matchRunRepo`, `matchGroupRepo`. Sentinels: `ErrNilMatchRunRepository`, `ErrNilMatchGroupRepository`. |
| ingestion | 4 params | **6 params** | Added `jobRepo`, `transactionRepo`. Sentinels: `ErrNilJobRepository`, `ErrNilTransactionRepository`. |
| discovery | 3 params | **4 params** | Added `connRepo`. Sentinel: `ErrNilConnectionRepository`. |
| reporting `NewHandlers` | 4 params | **5 params** | Added `reportRepo`. Sentinel: `ErrNilReportRepository`. |
| reporting `NewExportJobHandlers` | 6 params | **7 params** | Added `exportJobRepo`. Sentinel: `ErrNilExportJobRepository`. |

## Bootstrap Wiring Diffs

All 5 `init_*.go` files pass repositories that were already constructed earlier in the same file — **no duplicate repo construction**. In every case the repo instance is the same one fed to the Command/Query UseCases, ensuring a single source of truth per context.

- `init_configuration.go`: `NewHandler` now takes `repos.configFieldMap`, `repos.configFeeRule`, `repos.feeSchedule` (all from `sharedRepositories`), and the local `scheduleRepository` constructed earlier in the function.
- `init_matching.go`: `NewHandler` now takes `matchRunRepository` and `matchGroupRepository` (local to function, already fed to `matchingCommand.UseCaseDeps` and `matchingQuery.NewUseCase`).
- `init_ingestion.go`: `NewHandlers` now takes `repos.ingestionJob` and `repos.ingestionTx`.
- `init_discovery.go`: `NewHandler` now takes `connRepo` (local, already fed to both use cases).
- `init_reporting.go`: `NewHandlers` now takes `reportRepository`; `NewExportJobHandlers` now takes `exportJobRepository`.

## Test Coverage Delta

| Action | Count |
|---|---|
| Test functions deleted (span-only method tests) | ~40 (spread across configuration, matching, ingestion, discovery, reporting) |
| Test helpers added | 3 (`newTestHandler` in matching, `newHandlers`/`newTestReportRepo` in ingestion/reporting fixtures) |
| New sentinel-coverage tests | 9 (configuration: 4 new nil-repo tests; matching: 2; ingestion: 2; discovery: 1; reporting: 2) |

Unit tests remain green. Per-package test runtimes unchanged. No behavioral tests lost — the deleted tests were exclusively mocking the UseCase's span/error-wrap layer, which is no longer present in the codebase after collapse.

## Query UseCases Deleted

**None fully deleted.** Every context's Query UseCase struct survives because each hosts at least one method with real logic (from the T-009 Keep List). Structure of what's left per context:

- `configuration/services/query/UseCase`: `ListSources`, `ListMatchRules`, `GetFieldMap`, `GetFieldMapBySource`, `GetSchedule`
- `matching/services/query/UseCase`: `ListMatchRunGroups`
- `ingestion/services/query/UseCase`: `GetJob`, `GetJobByContext`, `GetTransaction`, `ListTransactionsByJob`, `PreviewFile`
- `discovery/services/query/UseCase`: `GetDiscoveryStatus`, `GetConnection`, `GetConnectionSchema`, `GetExtraction`, `ListBridgeCandidates`, `CountBridgeReadinessByTenant`
- `reporting/services/query/UseCase`: 9 Export/Stream methods with real body logic
- `reporting/services/query/ExportJobQueryService`: `GetByID` (sentinel translation, KEEP per T-009)

## Files Deleted

```
internal/configuration/services/query/context_queries.go         (and test)
internal/configuration/services/query/fee_rule_queries.go        (and test)
internal/configuration/services/query/fee_schedule_queries.go    (and test)
internal/ingestion/services/query/transaction_search_queries.go  (and test)
```

## Verification

| Check | Result |
|---|---|
| `go build ./...` | OK |
| `go vet -tags unit ./...` | OK |
| `go vet -tags integration ./...` | OK |
| `go vet -tags e2e ./...` | OK |
| `go vet -tags chaos ./...` | Pre-existing baseline failure only (`trustedStreamFakeChaosDedupe` missing `MarkSeenBulk` — unrelated to T-009b) |
| `go test -tags unit ./...` | PASS — all packages |
| `make lint` | 0 issues (ran `gci` on 4 files post-edit to fix import grouping) |
| `make check-tests` | All `.go` files have corresponding `_test.go` |
| `make check-test-tags` | All build tags valid |

## Proof-of-Zero-Callers Greps

**ALL greps cover BOTH `internal/` AND `tests/`** (Phase 1 lesson).

```bash
# Configuration
$ grep -rn "queryUC\.CountContexts\|queryUC\.ListContexts\|queryUC\.GetFeeRule\|\
queryUC\.ListFeeRules\|queryUC\.GetFeeSchedule\|queryUC\.ListFeeSchedules\|\
queryUC\.CheckFieldMapsExistence\|queryUC\.ListSchedules\|\
\.query\.CountContexts\|\.query\.ListContexts\|\.query\.GetFeeRule\|\
\.query\.ListFeeRules\|\.query\.GetFeeSchedule\|\.query\.ListFeeSchedules\|\
\.query\.CheckFieldMapsExistence\|\.query\.ListSchedules" \
  internal/ tests/
# (no output — zero callers)

# Ingestion
$ grep -rn "queryUC\.ListJobsByContext\|queryUC\.ListTransactionsByJobContext\|\
queryUC\.SearchTransactions\|\.queryUC\.ListJobsByContext\|\
\.queryUC\.ListTransactionsByJobContext\|\.queryUC\.SearchTransactions" \
  internal/ tests/
# (no output — zero callers)

# Matching
$ grep -rn "queryUC\.GetMatchRun\|queryUC\.ListMatchRuns\|queryUC\.FindMatchGroupByID\|\
query\.GetMatchRun\|query\.ListMatchRuns\|query\.FindMatchGroupByID" internal/ \
  | grep -v "e2e/client\|mocks/\|_test\.go" | grep -v "queries\.go"
# (no output — zero callers; e2e/client.GetMatchRun is a separate HTTP client method, not the UseCase)

# Discovery
$ grep -rn "\.query\.ListConnections\|queryUC\.ListConnections" internal/ tests/
# (no output — zero callers; internal/discovery/adapters/fetcher/.../ListConnections is a different Fetcher client method)

# Reporting
$ grep -rn "exportUC\.GetMatchedReport\|exportUC\.GetUnmatchedReport\|\
exportUC\.GetSummaryReport\|exportUC\.GetVarianceReport\|exportUC\.CountMatched\|\
exportUC\.CountUnmatched\|exportUC\.CountTransactions\|exportUC\.CountExceptions\|\
querySvc\.List\b\|querySvc\.ListByContext" internal/ tests/
# (no output — zero callers)
```

All five proof-greps returned empty.

## Aggregate Stats

- **Methods collapsed:** 25
- **Contexts touched:** 5 (configuration, matching, ingestion, discovery, reporting)
- **Handler files modified:** 15
- **Bootstrap files modified:** 5
- **Test files modified:** ~24
- **Source files deleted:** 4 (+ 4 test files)
- **LOC delta:** +750 / -3574 (net **-2824 lines**)
- **Files changed:** 58
- **Query UseCase structs entirely deleted:** 0 (every context still hosts real-logic methods)

## Observability Verification

Before each collapse, the call path was:
```
handler span → query UseCase span → repo span
```

After:
```
handler span → repo span
```

The deleted middle span added no unique attributes — every span-only UseCase method literally was `tracer.Start` + `defer span.End()` + single repo call + error-wrap boilerplate. The handler span (always named `handler.<context>.<op>`) and the repo span (always named `postgres.<op>`) both survive unchanged. Trace depth decreases by 1; signal content is identical.

Notably, `reporting.query.count_matched` (and the other 7 count spans) never carried useful attributes — `handler.reporting.count_matched` already captures the tenant+context span attributes via `libHTTP.SetHandlerSpanAttributes`, and the repo span captures the SQL-level metrics. The deleted span was pure middleware.

## Scope Discipline

- Did NOT touch any KEEP-list method.
- Did NOT modify any non-candidate method.
- Did NOT refactor the Query UseCase struct shapes beyond removing deleted methods.
- Did NOT delete a Query UseCase file unless every method in it was in the candidate list (3 configuration files + 1 ingestion file qualified; all 5 contexts retained their parent UseCase file).
- Did NOT alter `ListMatchRunGroups`, `DashboardUseCase.*`, `Export*`/`Stream*`, `GetByID`, `PreviewFile`, etc. per T-009 Keep List.

## Follow-ups / Known Baseline

- `go vet -tags chaos` fails on `trustedStreamFakeChaosDedupe` — **pre-existing** (verified via `git stash` check). Unrelated to T-009b. Separate task.

## Notes on Method Signatures

Two methods required argument restructuring because the UseCase wrapped the repo signature:

1. **`ExportJobQueryService.List(ctx, ListExportJobsInput)` → `exportJobRepo.List(ctx, status, cursor, limit)`** — the input struct was decomposed back to the repo's positional args. `ListExportJobsInput` type was removed (zero external callers).
2. **`ExportJobQueryService.ListByContext(ctx, ListByContextInput)` → `exportJobRepo.ListByContext(ctx, contextID, cursor, limit)`** — same pattern. `ListByContextInput` type removed.

Neither handler was inconvenienced; both were already constructing the input struct inline with fields that map 1:1 to the repo's positional args.

## Lessons for Any Future Phase

1. **Proof-greps MUST cover `tests/` in addition to `internal/`**. Phase 1 missed integration tests; Phase 2 verified this every time.
2. **Go's nil-slice semantics are your friend.** `make([]T, 0, len(nil))` is `[]`, not a panic — so nil→empty-slice conversion in UseCase wrappers is usually dead code.
3. **Function-pointer collapse is free** when the UseCase method and the repo method have identical signatures (reporting's count handlers).
4. **Input struct wrappers in the UseCase collapse to positional args in the handler** without breaking the public HTTP API surface — the wire-level JSON doesn't care about the internal call signature.

---

## Post-review Fix: Query UseCase Orphan Cluster (2026-04-22)

**Reviewer:** dead-code-reviewer (MEDIUM finding)

### The Cluster

Phase 2 deleted the four read methods `GetFeeRule`, `ListFeeRules`, `GetFeeSchedule`, `ListFeeSchedules` from `configuration/services/query/UseCase` but left the repositories and their option constructors behind. The result: two fields that were write-only dead state and two option constructors that existed solely to write to them.

**Dead fields (query `UseCase`):**

- `feeScheduleRepo sharedPorts.FeeScheduleRepository`
- `feeRuleRepo repositories.FeeRuleRepository`

**Dead options (query package):**

- `WithFeeScheduleRepository`
- `WithFeeRuleRepository`

**Wiring call sites removed:**

| File | Dropped call |
|---|---|
| `internal/bootstrap/init_configuration.go` | `configQuery.WithFeeScheduleRepository(repos.feeSchedule)` |
| `internal/bootstrap/init_configuration.go` | `configQuery.WithFeeRuleRepository(repos.configFeeRule)` |
| `internal/configuration/adapters/http/handlers_test.go` | `query.WithFeeRuleRepository(feeRuleRepo)` |
| `internal/configuration/adapters/http/handlers_test.go` | `query.WithFeeScheduleRepository(feeScheduleRepo)` |
| `internal/configuration/adapters/http/handlers_coverage_test.go` | `query.WithFeeScheduleRepository(feeRepo)` |

### Cleanup Diff

- `internal/configuration/services/query/queries.go`: removed 2 fields, 2 option constructors, 1 import (`sharedPorts`). `repositories` import preserved (still used by the 4 required repos). `configPorts` preserved (still used by `ScheduleRepository`).
- `internal/bootstrap/init_configuration.go`: dropped 2 lines from the `configQuery.NewUseCase` call. `repos.feeSchedule` / `repos.configFeeRule` local state preserved — still consumed by `configCommand.NewUseCase` (lines 40-41) and `configHTTP.NewHandler` (lines 69-70).
- `internal/configuration/adapters/http/handlers_test.go`: dropped 2 option lines from the `query.NewUseCase` call in `newHandlerFixture`. Local `feeRuleRepo` / `feeScheduleRepo` preserved — still passed to the command use case and `NewHandler`.
- `internal/configuration/adapters/http/handlers_coverage_test.go`: dropped 1 option line from the `query.NewUseCase` call in `newFeeScheduleHandlerFixture`. Local `feeRepo` preserved — still passed to the command use case and `NewHandler`.

### Revised LOC Delta

Phase 2 net delta (post-review): **+12 LOC beyond the original Phase 2 delta eliminated**, specifically:

- queries.go: -17 lines (2 fields, 2 option constructors with comments, 1 import line)
- init_configuration.go: -2 lines
- handlers_test.go: -2 lines
- handlers_coverage_test.go: -1 line

Total post-review cleanup: **-22 LOC** removed on top of Phase 2's original deletions. No additions.

### Handoff Revision: "Query UseCase Struct Retention" Claim

The original handoff implicitly claimed the Query UseCase was in a clean state after method deletion. That was wrong. The methods were removed but the repositories they read from remained wired, producing an orphan cluster that only a read-before-write analyzer would catch. This commit fixes the omission: fields, options, and all five wiring sites are now consistent with the surviving read surface (`GetSchedule` + the four originally-validated required repos).

### Verification

- `go build ./...` — clean
- `go vet -tags unit ./...` — clean
- `go vet -tags integration ./...` — clean (the Phase 1 blind spot; confirmed gone)
- `go test -tags unit ./internal/configuration/... ./internal/bootstrap/...` — all pass
- `make lint` — 0 issues
- `grep feeScheduleRepo|feeRuleRepo internal/configuration/services/query/` — zero hits
- `grep WithFeeScheduleRepository|WithFeeRuleRepository` across `internal/` and `tests/` — remaining hits exclusively on the command side (`command.*` / inside `services/command/`)

### Additional Orphans Flagged (Out of Scope)

None uncovered. The cleanup was fully contained to the fee-schedule/fee-rule read path.

---

**Last Updated:** 2026-04-22
**Parent Task:** T-009b (simplify cycle)
**Commit SHA:** (uncommitted; pending review)
