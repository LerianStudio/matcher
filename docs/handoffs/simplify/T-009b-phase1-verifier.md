# T-009b Phase 1: Verifier Cascade Redesign — Handoff

**Date:** 2026-04-22
**Status:** Complete (Phase 1 only — mechanical 18 non-verifier methods deferred to Phase 2)
**Branch:** develop
**Parent:** `docs/handoffs/simplify/T-009.md`

## TL;DR

Rewired the configuration `TenantOwnershipVerifier` to take `repositories.ContextRepository` directly instead of the query `UseCase`, then cut the three span-only wrapper methods (`GetContext`, `GetSource`, `GetMatchRule`) that only existed to feed the verifier + three GET handlers. The three existing handler call sites now call the repository directly. Net LOC: **−238** (401 deleted, 163 inserted).

Surprise finding: three of the four verifiers in the original scope (`matching`, `ingestion`, `reporting`) **already** took a narrow port (`ContextAccessProvider` / `ContextProvider`), not a query UseCase. Only `configuration` was still coupled. The T-009 handoff's "4 verifier cascade" was actually a "1 verifier cascade" — scope reduced.

## Scope surprise (documented)

The T-009 handoff said the verifier cascade spanned 4 contexts. Actual state:

| Context       | Pre-change                             | Status           |
| ------------- | -------------------------------------- | ---------------- |
| configuration | `*query.UseCase`                       | **Changed**      |
| matching      | `ports.ContextProvider` (already port) | No change needed |
| ingestion     | `sharedPorts.ContextAccessProvider`    | No change needed |
| reporting     | `sharedPorts.ContextAccessProvider`    | No change needed |

The three already-decoupled verifiers predate T-009 and live in unrelated work — no cascade to chase.

## Option chosen: A (full domain repo)

Switched the verifier to take `repositories.ContextRepository` directly rather than inventing a single-method `ContextOwnershipReader` interface.

**Reasoning:**

- Depguard's `http-handlers-boundary` rule denies postgres adapters but explicitly **permits** `domain/repositories` imports from HTTP adapters. No architectural barrier.
- Matching/ingestion/reporting verifiers already follow this pattern (port, but the repo is a pre-existing named interface — same shape).
- A fresh `ContextOwnershipReader` interface would be ISP-for-ISP's-sake. The repository interface isn't so wide it obscures what the verifier uses — it's seven methods on one aggregate. Narrow-interface wins when blast radius is real; here there isn't any.
- Tests already mocked at `FindByID` level via `mocks.MockContextRepository`, so switching to repo-direct was a **simplification**, not a complication — the verifier_test.go dropped the plumbing-only `createVerifierWithMocks` helper and its `createMockRepositories` shim that only existed to construct a throwaway UseCase.

## Signature changes

### Verifier

```go
// Before
func NewTenantOwnershipVerifier(queryUseCase *query.UseCase) sharedhttp.TenantOwnershipVerifier

// After
func NewTenantOwnershipVerifier(contextRepo repositories.ContextRepository) sharedhttp.TenantOwnershipVerifier
```

### Handler constructor

```go
// Before
func NewHandler(commandUseCase *command.UseCase, queryUseCase *query.UseCase, production bool) (*Handler, error)

// After
func NewHandler(
    commandUseCase *command.UseCase,
    queryUseCase *query.UseCase,
    contextRepo repositories.ContextRepository,
    sourceRepo repositories.SourceRepository,
    matchRuleRepo repositories.MatchRuleRepository,
    production bool,
) (*Handler, error)
```

Three new sentinels added: `ErrNilContextRepository`, `ErrNilSourceRepository`, `ErrNilMatchRuleRepository`.

### Handler struct

Added three fields (`contextRepo`, `sourceRepo`, `matchRuleRepo`). Kept `query` — it still serves 15+ other methods (ListContexts, CountContexts, ListSources, ListMatchRules, GetFieldMap\*, GetSchedule, GetFeeRule\*, GetFeeSchedule\*, CheckFieldMapsExistence, etc.).

## Security audit note preserved verbatim

The `NewTenantOwnershipVerifier` godoc retains the exact Taura Security audit block identifying configuration as the single verifier that does NOT enforce `ErrContextNotActive`, with the PAUSED-recovery-path rationale and the state-machine diagram. The body logic is unchanged: no active-status check in the configuration verifier. Matching/ingestion/reporting verifiers were untouched and still enforce active-status.

Regression tests preserved:

- `TestPausedContextRemainsAccessibleForConfiguration` (all 4 statuses)
- `TestPausedContextCanBeReactivatedViaDomainStateMachine`
- `TestPausedContextCanBeReadViaConfigurationVerifier`
- `TestPausedContextCanBeDeletedViaConfigurationVerifier`
- `TestConfigurationVerifierNeverReturnsErrContextNotActive`

## UseCase methods deleted

Three methods removed from `internal/configuration/services/query/`:

| Method                                               | Caller count before | Caller count after |
| ---------------------------------------------------- | ------------------- | ------------------ |
| `UseCase.GetContext(ctx, contextID)`                 | 2 (verifier + handler) | 0                  |
| `UseCase.GetSource(ctx, contextID, sourceID)`        | 3 (handler + ensureSourceAccess helper) | 0 |
| `UseCase.GetMatchRule(ctx, contextID, ruleID)`       | 1 (handler)            | 0                  |

Proof of zero remaining callers (original grep scoped only `internal/` — see "Post-review fix" below for the corrected grep that also covers `tests/`):

```text
$ grep -rn 'queryUC\.GetContext\|queryUseCase\.GetContext' internal/ tests/
(no output)

$ grep -rn 'queryUC\.GetSource\|queryUseCase\.GetSource' internal/ tests/
(no output)

$ grep -rn 'queryUC\.GetMatchRule\|queryUseCase\.GetMatchRule' internal/ tests/
(no output)
```

## Handler call sites migrated

| File                         | Change                                                          |
| ---------------------------- | --------------------------------------------------------------- |
| `handlers_context.go:210`    | `handler.query.GetContext` → `handler.contextRepo.FindByID`     |
| `handlers_source.go:228`     | `handler.query.GetSource` → `handler.sourceRepo.FindByID`       |
| `handlers.go:125` (ensureSourceAccess) | `handler.query.GetSource` → `handler.sourceRepo.FindByID` |
| `handlers_match_rule.go:220` | `handler.query.GetMatchRule` → `handler.matchRuleRepo.FindByID` |

The `sql.ErrNoRows` → 404 translation remains in the handler (where it belonged anyway — the deleted UseCase methods just span-wrapped `FindByID` without adding error mapping).

## Test coverage delta

| Metric                      | Before T-009b P1 | After T-009b P1 |
| --------------------------- | ---------------- | --------------- |
| Overall unit coverage       | 79.0%            | **79.1%**       |
| Unit tests                  | 15,551           | 15,512          |
| Test files touched          | —                | 5               |

Test count dropped by 39: removed tests targeted the three deleted UseCase methods (`TestGetContext_*`, `TestGetSource_*`, `TestGetMatchRule_*`) and surgical excisions from `queries_test.go`. The verifier tests themselves retained identical assertions — they were already mocking at the `FindByID` level, so the switch was mechanical.

Coverage rose slightly because the span-only wrappers (which had 100% line coverage but measured redundant instrumentation) disappeared, and the verifier's own code path became the sole site hitting `FindByID` for ownership checks, inflating that path's observed coverage density.

## Files touched (17 total)

**Source (11):**

- `internal/bootstrap/init_configuration.go` — pass 3 repos to `NewHandler`
- `internal/configuration/adapters/http/handlers.go` — struct fields, constructor, `ensureSourceAccess`
- `internal/configuration/adapters/http/handlers_context.go` — `GetContext` call migrated
- `internal/configuration/adapters/http/handlers_source.go` — `GetSource` call migrated
- `internal/configuration/adapters/http/handlers_match_rule.go` — `GetMatchRule` call migrated
- `internal/configuration/adapters/http/verifier.go` — repo-based constructor + audit note
- `internal/configuration/services/query/context_queries.go` — `GetContext` deleted
- `internal/configuration/services/query/source_queries.go` — `GetSource` deleted
- `internal/configuration/services/query/match_rule_queries.go` — `GetMatchRule` deleted

**Tests (6):**

- `internal/configuration/adapters/http/handlers_test.go` — `NewHandler` fixture
- `internal/configuration/adapters/http/handlers_auth_test.go` — `NewHandler` fixture
- `internal/configuration/adapters/http/handlers_coverage_test.go` — `NewHandler` fixtures + 3 new nil-repo sentinel tests
- `internal/configuration/adapters/http/verifier_test.go` — fully rewritten (dropped `createVerifierWithMocks` plumbing)
- `internal/configuration/services/query/context_queries_test.go` — dropped `TestGetContext_*`
- `internal/configuration/services/query/source_queries_test.go` — dropped `TestGetSource_*`
- `internal/configuration/services/query/match_rule_queries_test.go` — dropped `TestGetMatchRule_*`
- `internal/configuration/services/query/queries_test.go` — surgical excisions from `TestContextQueries`, `TestContextQueryErrorsBubble` (removed entirely), `TestSourceQueries`, `TestMatchRuleQueries`

## Verification

| Check                                                | Result                                  |
| ---------------------------------------------------- | --------------------------------------- |
| `go build ./...`                                     | Clean                                   |
| `go vet -tags unit ./internal/...`                   | Clean                                   |
| `go vet -tags integration ./internal/...`            | Clean                                   |
| `go vet -tags e2e ./...`                             | Clean                                   |
| `make test` (unit)                                   | PASS — 15,512 tests, 4 skipped, 79.1% coverage |
| `make lint` (golangci-lint)                          | 0 issues                                |
| `make lint-custom`                                   | Only pre-existing `DeleteByExceptionAndID` warning (unrelated) |
| `make check-tests`                                   | All `.go` have corresponding `_test.go` |
| `make check-test-tags`                               | All required build tags present         |

## LOC delta

```text
17 files changed, 163 insertions(+), 401 deletions(-)
```

**Net: −238 lines.**

## Follow-up candidates discovered

1. **T-009b Phase 2 — mechanical 18 non-verifier methods** (explicit next step per original scope). Now unblocked: handlers already have the three repos plumbed, so additional repo fields (FeeRuleRepository, FeeScheduleRepository, ScheduleRepository, FieldMapRepository) can follow the same pattern for the remaining span-only wrappers — but only the ones not in the T-009 KEEP list (nil-sentinel translation, conditional dispatch, sql.ErrNoRows filter).

2. **`DeleteByExceptionAndID` missing WithTx variant** (pre-existing, unrelated). Custom linter has been warning since before T-009; still there.

3. **Verifier-test DRY helper opportunity in matching/ingestion/reporting**: those three contexts each have their own verifier tests with similar mock-setup. Not in scope, flagged only.

4. **`errDBError` remains in `queries_test.go`** — still used by `schedule_queries_test.go`, kept. No cleanup needed.

## Out of scope for Phase 1 (explicitly)

- The 18 mechanical non-verifier methods flagged in the T-009 handoff's "Why Deferred" section 2 ("Handler doesn't currently inject the repo"). Scoped for T-009b Phase 2.
- Exception and discovery verifier files (different patterns, T-009 note flagged them as out-of-scope).
- Fee schedule verifier logic in `internal/shared/domain/fee/verifier.go` (unrelated — fee verification, not ownership).

## Decision log

- **Option A over Option B:** see "Option chosen" above.
- **Keep `query` field on Handler:** removing it would touch 15+ unrelated handler methods. Out of Phase 1 scope.
- **Keep `*query.UseCase` as `NewHandler` parameter:** still needed for 15+ methods; no reason to accept a nil or remove it when it's heavily used.
- **Add sentinels eagerly:** the three new `ErrNil*Repository` sentinels mirror the existing `ErrNilCommandUseCase`/`ErrNilQueryUseCase` convention. Constructor nil-checks are required (we care when wiring is wrong at bootstrap).

## Post-review fix

Reviewers' consequences-reviewer flagged a HIGH issue: the original zero-caller proof-grep scoped only `internal/`, which missed 9 call sites in `tests/integration/configuration/http_crud_test.go`. `go vet -tags integration ./...` failed with 9 undefined-method errors — this blocked `make ci`.

**Fix:** Migrated the 9 test call sites to use repositories directly, mirroring how production HTTP handlers operate post-T-009b.

### Test file migration

`tests/integration/configuration/http_crud_test.go` (1 file, +34 / −22, net **+12 lines**):

1. Changed `buildUseCases(t, h)` return signature from `(cmd, query)` to `(cmd, query, testRepos)`, where the new `testRepos` struct bundles the three aggregate repositories (`ctx`, `source`, `matchRule`). The repo instances were already being constructed inside `buildUseCases` — the function simply wasn't returning them.
2. Updated the 10 callers of `buildUseCases` across 9 test functions (9 migrated calls + 1 pagination test that kept using only the query UseCase). The four patterns in use:
   - `cmdUC, queryUC, repos := …` — full lifecycle tests (Context, Source, MatchRule)
   - `cmdUC, queryUC, _ := …` — FieldMap lifecycle + ListContextsPagination (both still use query methods not touched by T-009b)
   - `cmdUC, _, repos := …` — ReorderPriorities (reads rules by ID post-reorder via repo)
   - `cmdUC, _, _ := …` — three validation tests that only exercise commands
3. Swapped the 9 affected `queryUC.Get*` call sites to the equivalent repository `FindByID` calls:

| Line (orig) | Before                                           | After                                           |
| ----------- | ------------------------------------------------ | ----------------------------------------------- |
| 95          | `queryUC.GetContext(ctx, created.ID)`            | `repos.ctx.FindByID(ctx, created.ID)`           |
| 131         | `queryUC.GetContext(ctx, created.ID)`            | `repos.ctx.FindByID(ctx, created.ID)`           |
| 160         | `queryUC.GetSource(ctx, parent.ID, created.ID)`  | `repos.source.FindByID(ctx, parent.ID, created.ID)` |
| 196         | `queryUC.GetSource(ctx, parent.ID, created.ID)`  | `repos.source.FindByID(ctx, parent.ID, created.ID)` |
| 283         | `queryUC.GetMatchRule(ctx, parent.ID, created.ID)` | `repos.matchRule.FindByID(ctx, parent.ID, created.ID)` |
| 317         | `queryUC.GetMatchRule(ctx, parent.ID, created.ID)` | `repos.matchRule.FindByID(ctx, parent.ID, created.ID)` |
| 430         | `queryUC.GetMatchRule(ctx, parent.ID, rule3.ID)` | `repos.matchRule.FindByID(ctx, parent.ID, rule3.ID)` |
| 434         | `queryUC.GetMatchRule(ctx, parent.ID, rule1.ID)` | `repos.matchRule.FindByID(ctx, parent.ID, rule1.ID)` |
| 438         | `queryUC.GetMatchRule(ctx, parent.ID, rule2.ID)` | `repos.matchRule.FindByID(ctx, parent.ID, rule2.ID)` |

### Return-type invariance confirmed

The repository `FindByID` methods return identical types to the deleted UseCase methods:

- `contextRepo.FindByID(ctx, id) (*entities.ReconciliationContext, error)`
- `sourceRepo.FindByID(ctx, contextID, id) (*entities.ReconciliationSource, error)`
- `matchRuleRepo.FindByID(ctx, contextID, id) (*entities.MatchRule, error)`

All tests only dereference `.ID`, `.Name`, `.Priority`, `.Mapping` — stable domain-entity fields. Error assertions use `require.NoError(t, err)` on the happy path and `require.Error(t, err)` after delete (no sentinel-string check that would have broken on the UseCase→repo layer change).

### Verification post-fix

| Check                                         | Result                          |
| --------------------------------------------- | ------------------------------- |
| `go build ./...`                              | Clean                           |
| `go vet -tags unit ./...`                     | Clean                           |
| `go vet -tags integration ./...`              | **Clean (was failing)**         |
| `go vet -tags e2e ./...`                      | Clean                           |
| `go vet -tags chaos ./...`                    | Pre-existing baseline (unrelated to T-009b — `DedupeService.MarkSeenBulk` missing on test fake) |
| `go test -tags=unit ./... -count=1`           | PASS — no failures              |
| Zero-caller grep across `internal/ tests/`    | No matches                      |

Diff stat for the fix: `1 file changed, 34 insertions(+), 22 deletions(-)`.

### Why the original grep missed this

The original scan:

```text
grep -rn "GetContext(ctx|GetSource(ctx|GetMatchRule(ctx" internal/
```

Only looked at `internal/`. Integration test files live under `tests/integration/`. The corrected grep (shown at the top of this handoff) covers both roots and would have surfaced the 9 sites before the delete landed.

### Not addressed (explicit)

- Code-reviewer L1 (handler doc-comment asymmetry): cosmetic only, skipped.
- Code-reviewer L2 (sentinel-block consistency): reviewer called this out as a positive; no action.
