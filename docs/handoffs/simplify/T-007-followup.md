# T-007 Follow-up — Promote SourceType + Collapse ReconciliationSource

**Date**: 2026-04-22
**Status**: Complete — changes in working tree, awaiting review
**Parent**: T-007 (commit `2ab0a37d`) deferred this promotion as a 33-file blast radius

## Summary

T-007 consolidated 3 of 4 declared duplicates but deferred unifying
`ReconciliationSource` because the typed `Type` field required promoting
`value_objects.SourceType` to the shared kernel. This follow-up executes that
promotion using Matcher's documented type-alias pattern, collapsing the
remaining duplicate and deleting the copy-function bridge.

## What moved

| From                                                                       | To                                                        |
|----------------------------------------------------------------------------|-----------------------------------------------------------|
| `internal/configuration/domain/value_objects/source_type.go` (canonical)   | `internal/shared/domain/source_type.go` (canonical)       |
| `internal/configuration/domain/entities/reconciliation_source.go` (struct + methods + DTOs + sentinels) | `internal/shared/domain/reconciliation_source.go` (canonical) |
| `internal/configuration/domain/entities/reconciliation_source_test.go`     | `internal/shared/domain/reconciliation_source_test.go`    |

The former `ReconciliationSource` struct declared in
`internal/shared/domain/field_map.go` (with untyped `Type string` and a
divergence NOTE) has been deleted in favour of the new typed canonical file.

## Pattern used: type alias (CLAUDE.md "type-alias pattern")

The old file paths now contain thin alias shims that re-export the canonical
types from the shared kernel. Call sites importing
`configuration/domain/value_objects` or `configuration/domain/entities`
continue to compile without modification — Go type aliases preserve identity,
so `entities.ReconciliationSource` and `shared.ReconciliationSource` are
literally the same type.

**Three re-export mechanisms in play:**
1. `type X = Y` — struct and enum type aliases (preserves method sets)
2. Constants re-declared via `const X = sharedPkg.X` — identity preserved at compile time
3. `var X = sharedPkg.X` — used for function values (`ParseSourceType`, `NewReconciliationSource`) and sentinel errors. Go disallows function aliases, but `var` holding a func value gives call sites the same API with zero runtime cost.

### Why `var` for functions?

The brief asked for the cleanest pattern. A wrapper function
(`func ParseSourceType(s string) (SourceType, error) { return shared.ParseSourceType(s) }`)
is an extra stack frame, an extra function signature to keep in sync, and an
extra line of code per exported symbol. `var ParseSourceType = shared.ParseSourceType`
is one line, no stack frame, signature impossible to drift. Same reasoning for
`var NewReconciliationSource = shared.NewReconciliationSource`.

## What collapsed

| Removed                                    | Where                                                         |
|--------------------------------------------|---------------------------------------------------------------|
| Duplicate `ReconciliationSource` struct    | Was declared twice (configuration + shared, with `Type` divergence) — now one canonical declaration |
| `toSharedReconciliationSource()` function  | `internal/shared/adapters/cross/configuration_adapters.go` — no longer needed because `entities.ReconciliationSource` *is* `shared.ReconciliationSource` |
| Divergence NOTE comment                    | `internal/shared/domain/field_map.go` — the rationale no longer applies |
| `configEntities` import in cross adapter   | Unused after copy function deletion                           |
| 2 stale wrapcheck signature rules in `.golangci.yml` | `internal/shared/domain` is already in `ignore-package-globs`, so the explicit rules were redundant |

## Files touched

| Bucket              | Count | Notes                                                         |
|---------------------|------:|---------------------------------------------------------------|
| Modified (Go)       | 7     | Includes the aliased shim files and cross-adapter cleanup     |
| Modified (config)   | 1     | `.golangci.yml` wrapcheck rule cleanup                        |
| Created (Go)        | 4     | `source_type.go`, `source_type_test.go`, `reconciliation_source.go`, `reconciliation_source_test.go` — all under `internal/shared/domain/` |
| Deleted             | 0     | Configuration files remain as alias shims (pattern-consistent with exception/Severity) |

LOC delta from `git diff --stat` plus the 4 new files: **~227 net lines
added**, but this is misleading. The migration *moves* ~200 lines from
configuration to shared and *deletes* the duplicate struct + copy-function
shim. The genuine new code is ~50 lines of alias-verification tests.

### File inventory

**Modified:**
- `.golangci.yml` — stale wrapcheck signatures replaced with comments
- `internal/configuration/domain/entities/reconciliation_source.go` — now ~40-line alias shim
- `internal/configuration/domain/entities/reconciliation_source_test.go` — now 57-line alias-verification test
- `internal/configuration/domain/value_objects/source_type.go` — now ~30-line alias shim
- `internal/shared/adapters/cross/configuration_adapters.go` — `toSharedReconciliationSource` deleted
- `internal/shared/adapters/cross/configuration_adapters_test.go` — test assertion dropped unnecessary string cast
- `internal/shared/domain/field_map.go` — old `ReconciliationSource` struct + NOTE + unused import removed
- `internal/shared/domain/field_map_test.go` — `Type "csv"` literals changed to `shared.SourceType("csv")`

**Created:**
- `internal/shared/domain/source_type.go` — canonical enum (promoted from configuration)
- `internal/shared/domain/source_type_test.go` — enum unit tests
- `internal/shared/domain/reconciliation_source.go` — canonical struct + constructor + Update + ConfigJSON + DTOs + sentinels
- `internal/shared/domain/reconciliation_source_test.go` — full behaviour tests (migrated from configuration test, adapted to `shared_test` external package)

## Verification results

| Step | Command | Result |
|------|---------|--------|
| Build clean | `go build ./...` | ✅ no output |
| Vet clean (no tags) | `go vet ./...` | ✅ no output |
| Vet clean (unit) | `go vet -tags unit ./...` | ✅ no output |
| Vet clean (integration) | `go vet -tags integration ./...` | ✅ no output |
| Vet clean (e2e) | `go vet -tags e2e ./...` | ✅ no output |
| Unit tests | `go test -tags unit -count=1 ./internal/...` | ✅ 0 FAIL |
| Full lint | `make lint` | ✅ 0 issues |
| Custom lint | `make lint-custom` | ✅ pre-existing repositorytx warning unrelated to this change (`comment.postgresql.go`) |
| Test coverage check | `make check-tests` | ✅ every .go has a _test.go |
| Swagger regeneration | `make generate-docs` | ✅ SourceType enum rendered with all 5 values in 5+ endpoint schemas |
| Generated artifacts check | `make check-generated-artifacts` | ✅ up to date |
| No copy-function leaks | `grep -rn toSharedReconciliationSource internal/ tests/` | ✅ 0 hits outside old handoff docs |

### Swagger enum verification

`docs/swagger/swagger.json` still contains the canonical enum in all affected
schemas:
```
"enum": ["LEDGER", "BANK", "GATEWAY", "CUSTOM", "FETCHER"]
```

Swag resolved the enum correctly through the alias — the Swagger annotations
(`@Description`, `@Enum`) on the canonical definition at
`internal/shared/domain/source_type.go` are picked up without issue. No
fallback annotations were needed on the alias shim.

## Depguard rule impact

No new depguard rules needed. The move is *toward* shared, which is the
designated bridge — no existing rule fires.

- **cross-context rules** (configuration → matching etc.): unaffected — configuration still owns its entities file; it just aliases into shared.
- **shared-adapters-boundary**: allows `internal/shared/domain` imports, which is exactly what now carries `ReconciliationSource`. The previous rule about "shared adapters cannot import configuration entities" still holds — and in the cross adapter we actually *removed* the configEntities import, tightening compliance.
- **domain-purity / entity-purity**: `internal/shared/domain/reconciliation_source.go` only imports `context`, `encoding/json`, `errors`, `strings`, `time`, `google/uuid`, `lib-commons/commons/assert`, and the local `constants` + `fee` siblings. No adapter or application imports. Clean.

## Wrapcheck rule cleanup

Two wrapcheck `extra-rules` entries explicitly listed the old
`entities.ReconciliationSource` function/method signatures. These became
stale because the canonical functions are now in `internal/shared/domain`.
Rather than relist signatures for the new location, the rules were replaced
with comments — `internal/shared/domain` is already in `ignore-package-globs`
(line 994 of `.golangci.yml`), so the new location is covered uniformly with
`FieldMap`, `AuditLog`, etc.

## Follow-up candidates discovered

While scanning for `type \w+ string` declarations, a few other consolidation
opportunities surfaced:

| Type                  | Current locations                                                                  | Severity |
|-----------------------|------------------------------------------------------------------------------------|----------|
| `ExtractionStatus`    | Declared **twice** — `shared/domain/transaction.go:56` and `discovery/domain/value_objects/extraction_status.go:15` | HIGH — genuine duplicate with divergent values |
| `ContextStatus`       | `configuration/domain/value_objects/context_status.go` — used cross-context via `cross/matching_adapters_test.go` | MEDIUM — similar shape to SourceType pre-migration |
| `RoutingTarget`       | `exception/domain/services/routing.go:25` | LOW — localized, unlikely to need shared |
| `DisputeState`, `DisputeCategory` | `exception/domain/dispute/` | LOW — confined to exception |

**Recommended next ticket**: collapse `ExtractionStatus` duplicate using the
same type-alias pattern (the two decls may have different constants — check
before unifying).

## Notes for the reviewer

- The type-alias mechanism makes this change *compile-time-only*. Runtime
  behaviour is identical — same method sets, same string values, same error
  instances (verified by `assert.Same` in the alias-verification test).
- The ~44 files that reference `entities.ReconciliationSource` or
  `value_objects.SourceType` needed **zero changes** to their import paths or
  type names. The only edits to call sites were (a) the cross-adapter test
  dropping a now-unnecessary `string(src.Type)` cast, and (b) pre-existing
  `field_map_test.go` stub tests updating bare `"csv"` literals to
  `shared.SourceType("csv")` now that the shared struct is typed.
- `maxSourceNameLength` remains unexported. The migrated test uses a local
  `reconciliationSourceMaxNameLength = 50` constant as a test contract, so
  the shared domain doesn't need to export an implementation detail.

## Cannot-be-rolled-back without rewrite

The former divergence (`shared.ReconciliationSource{Type: string}` vs
`entities.ReconciliationSource{Type: value_objects.SourceType}`) existed only
because T-007 declined to pay the migration cost. With the migration
complete, any rollback would either reintroduce the divergence NOTE and copy
function, or revert the whole canonical-location move. Neither should be
needed.

## Post-review fixes (2026-04-22)

Applied after the 10-reviewer pass on this follow-up. Three items closed,
all scoped to already-touched files.

1. **MEDIUM — duplicate test file at the alias location.**
   `internal/configuration/domain/value_objects/source_type_test.go` was a
   195-line full copy of the canonical shared-kernel tests, exercising the
   same types through the alias. Deleted and replaced with a ~40-line
   alias-identity test (`TestSourceTypeAlias`) that mirrors the Severity-
   and ReconciliationSource-alias pattern: compile-time assignment across
   the alias boundary, constant equality, `errors.Is` on the sentinel, and
   `reflect.ValueOf(...).Pointer()` equality on the `var`-exported
   `ParseSourceType`. Single `TestSourceType_Valid` definition now lives at
   `internal/shared/domain/source_type_test.go`.
2. **LOW — explanatory noise in `configuration_adapters.go`.**
   Removed the 3-line comment explaining why the now-deleted
   `toSharedReconciliationSource` function is not needed. The `return
   source, nil` with a typed return signature speaks for itself.
3. **LOW — untyped string literals in `field_map_test.go`.**
   Typed four sites (`Type: "csv"`, `Type: "csv"`, `Type: "api"`,
   `Type: "rest"`) to `shared.SourceType(...)` for uniformity with the rest
   of the file post-migration. These strings are test-only format hints
   and are not enum constants, so the cast form is the correct typed
   expression.

### Verification

- `go build ./...` — clean
- `go vet -tags unit ./...` — clean
- `go test -tags unit` on `value_objects`, `shared/domain`, `shared/adapters/cross` — all pass
- `make lint` — 0 issues
- `grep -rn 'TestSourceType_Valid' internal/` — exactly one hit, at
  `internal/shared/domain/source_type_test.go:39`

### LOC delta

| File                                                                              | Before | After | Delta |
|-----------------------------------------------------------------------------------|-------:|------:|------:|
| `internal/configuration/domain/value_objects/source_type_test.go`                 |    194 |    49 |  -145 |
| `internal/shared/adapters/cross/configuration_adapters.go` (comment only)         |      — |     — |    -3 |
| `internal/shared/domain/field_map_test.go` (4 literals typed, no line count shift) |      — |     — |    ±0 |
| **Net**                                                                           |        |       | **-148** |
