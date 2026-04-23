// Package chanutil provides utilities for Go channel operations.
//
// # Rationale
//
// This package earns its directory on the following grounds:
//
//   - Cross-cutting purpose: close-only channel lifecycle detection is a
//     concurrency primitive, not a domain concept. It does not belong inside
//     any single bounded context.
//   - Caller distribution: as of 2026-04-21 (T-012 audit), ClosedSignalChannel
//     is used by 6 worker implementations across 5 unrelated bounded contexts:
//     configuration (scheduler_worker), reporting (cleanup_worker,
//     export_worker), governance (archival_worker), and discovery
//     (bridge_worker, custody_retention_worker). Inlining would duplicate the
//     nil-vs-closed-vs-open discrimination at every worker's
//     prepareRunState().
//   - Non-trivial contract: the helper exists to reject a specific misuse
//     (sending values on a close-only channel) that is not obvious from the
//     3-line body. The contract is worth naming.
//
// # When to reopen this decision
//
// If any of the following change, re-audit this package:
//
//   - Caller count drops below 3 unrelated bounded contexts (e.g. workers
//     consolidate or migrate to a framework-provided lifecycle primitive).
//   - The standard library or lib-commons provides an equivalent helper —
//     prefer the upstream version and delete this package.
//   - The contract changes shape (e.g. needs to observe a value channel),
//     at which point a different abstraction replaces this one.
package chanutil
