# determinism linter

Flags `time.Now()` and `uuid.New()` calls inside `*_test.go` files whose
function body also constructs an entity via a `New{Entity}` constructor.
Tests that bake real wall-clock timestamps or random UUIDs into entities
become flaky whenever assertions touch the resulting value.

## Suggested replacements

- **Time:** `internal/testutil.WithFixedTime(t, fixed, &nowFunc, fn)` or a
  plain `time.Date(...)` literal stored in a test constant.
- **UUIDs:** a deterministic pool (`uuid.MustParse("...")` constants) or a
  per-test seeded generator. See `internal/testutil/` for the current set
  of shared helpers.

## Status

Advisory. Runs under `make lint-custom-strict` only; the default
`make lint-custom` target does not enable it. This mirrors the
`goroutineleak` rollout: the linter graduates to strict mode once the
existing violations in the codebase are cleaned up.

## Scope + heuristics

- Only `*_test.go` files are inspected.
- A function is scanned only if its body contains at least one call shaped
  like `New{CapitalName}(...)` — the matcher entity-constructor convention.
- `time.Now().Add(...)` is suppressed (deadline/TTL idiom, not entity
  construction).
- `testutil/` packages, the `tools/` tree, `mocks/` directories, and any
  `testdata/` tree are skipped.

The linter is intentionally heuristic — it errs toward false negatives
rather than chasing every selector expression. A type-aware implementation
would require full package loading, which is not worth the cost for an
advisory nudge.
