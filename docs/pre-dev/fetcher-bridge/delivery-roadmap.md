# Delivery Roadmap: Fetcher Bridge

## Executive Summary

| Metric | Value |
|--------|-------|
| **Start Date** | 2026-04-15 |
| **MVP End Date** | 2026-04-27 |
| **End Date** | 2026-05-07 |
| **Total Duration** | 17 working days (3.5 weeks) |
| **Critical Path** | T-001 → T-002 → T-003 → T-004 → T-005 → T-006 |
| **Parallel Streams** | 2 identified by dependency graph, 0 exploitable with current team size |
| **Team Composition** | 1 developer |
| **Development Mode** | AI Agent via `ring:dev-cycle` |
| **Human Validation Multiplier** | 1.2x |
| **Multiplier Source** | Custom |
| **Capacity Utilization** | 90% |
| **Formula** | `ai_estimate × multiplier ÷ 0.90 ÷ 8 ÷ team_size` |
| **Delivery Cadence** | Continuous |
| **Contingency Buffer** | 15% (2 working days) |
| **Confidence Level** | High |

## Planning Notes

- The exact effort conversion from the Gate 3 estimates produces **11.8 developer-days** of work.
- For scheduling, each task was rounded up to whole working days to avoid unrealistic sub-day handoffs and context fragmentation.
- With a single developer, the theoretical dependency parallelism between T-001 and T-002 does not reduce the practical delivery timeline. The roadmap therefore uses a resource-constrained serial path.

## Delivery Milestones

### Milestone 1: Intake And Trust Foundations (2026-04-15 to 2026-04-22)

**Deliverable:** Matcher can accept trusted internal streams and verify externally retrieved Fetcher artifacts into tenant-scoped custody.

| Task | Type | Effort | Start | End | Dependencies | Assignee | Status |
|------|------|--------|-------|-----|--------------|----------|--------|
| T-001 | Foundation | 2.3d formula / 3d scheduled | 2026-04-15 | 2026-04-17 | - | Backend | ready |
| T-002 | Foundation | 2.2d formula / 3d scheduled | 2026-04-20 | 2026-04-22 | - | Backend | ready |

**Completion Criteria:**
- T-001 and T-002 are working together as a trusted bridge foundation.
- No downstream reconciliation input is accepted before trust verification succeeds.
- The feature is ready for end-to-end bridge orchestration work.

### Milestone 2: MVP End-To-End Bridge (2026-04-23 to 2026-04-27)

**Deliverable:** A completed Fetcher extraction can automatically become usable reconciliation input with persisted extraction-to-ingestion linkage.

| Task | Type | Effort | Start | End | Dependencies | Assignee | Status |
|------|------|--------|-------|-----|--------------|----------|--------|
| T-003 | Feature | 2.3d formula / 3d scheduled | 2026-04-23 | 2026-04-27 | T-001, T-002 | Backend | blocked |

**Completion Criteria:**
- FETCHER sources are usable end-to-end for the happy path.
- Default-tenant handling is preserved.
- Restart-safe bridge linkage behavior is demonstrated.

### Milestone 3: Truthful Operational Status (2026-04-28 to 2026-05-01)

**Deliverable:** Existing Matcher operational endpoints can distinguish ready, pending, failed, and stale bridge outcomes with actionable visibility.

| Task | Type | Effort | Start | End | Dependencies | Assignee | Status |
|------|------|--------|-------|-----|--------------|----------|--------|
| T-004 | Integration | 1.7d formula / 2d scheduled | 2026-04-28 | 2026-04-29 | T-003 | Backend | blocked |
| T-005 | Integration | 2.0d formula / 2d scheduled | 2026-04-30 | 2026-05-01 | T-003, T-004 | Backend | blocked |

**Completion Criteria:**
- Operational surfaces report truthful readiness state.
- Failure categories are useful enough for support and escalation.
- Stale backlog stops hiding inside ambiguous completion semantics.

### Milestone 4: Rollout Controls And Buffer (2026-05-04 to 2026-05-07)

**Deliverable:** The bridge is safe to introduce in hybrid environments with explicit rollout and retention controls, plus contingency time for unresolved policy edges.

| Task | Type | Effort | Start | End | Dependencies | Assignee | Status |
|------|------|--------|-------|-----|--------------|----------|--------|
| T-006 | Polish | 1.3d formula / 2d scheduled | 2026-05-04 | 2026-05-05 | T-003, T-005 | Backend | blocked |
| Buffer | Contingency | 2d | 2026-05-06 | 2026-05-07 | T-006 | Backend | ready |

**Completion Criteria:**
- Rollout posture is explicit rather than implicit.
- Retention policy behavior is implemented and validated.
- Delivery still holds if policy-sensitive details require final adjustment.

## Critical Path Analysis

### Dependency-Correct Path

The dependency graph creates two initial foundations (`T-001`, `T-002`) that merge into the bridge worker (`T-003`), then flow through visibility, failure hardening, and rollout controls.

### Resource-Constrained Path

Because the assigned team size is **1 developer**, there is no exploitable parallel stream in practice. The real delivery path is therefore fully serialized:

`T-001 → T-002 → T-003 → T-004 → T-005 → T-006`

| Task | Formula Days | Scheduled Days | Slack | On Critical Path? |
|------|--------------|----------------|-------|-------------------|
| T-001 | 2.3d | 3d | 0 | Yes |
| T-002 | 2.2d | 3d | 0 | Yes |
| T-003 | 2.3d | 3d | 0 | Yes |
| T-004 | 1.7d | 2d | 0 | Yes |
| T-005 | 2.0d | 2d | 0 | Yes |
| T-006 | 1.3d | 2d | 0 | Yes |

**Minimum formula duration:** 11.8 developer-days  
**Planned scheduled duration:** 15 working days  
**Committed duration with buffer:** 17 working days

## Resource Allocation

| Role | Count | Utilization | Assigned Tasks |
|------|-------|-------------|----------------|
| Backend Engineer | 1 | 90% AI capacity with 1.2x human validation overhead | T-001 through T-006 |

### Bottlenecks

- All tasks are backend tasks.
- No parallel execution is realistically available with the current team size.
- Any slip on T-001, T-002, or T-003 directly moves the whole roadmap.

### Recommendation

If timeline compression becomes necessary, the only meaningful lever is **adding at least one more developer** so T-001 and T-002 can proceed in parallel and later operational work can overlap with rollout preparation.

## Risk Milestones

| Milestone | Date | Risk Level | Impact | Mitigation |
|-----------|------|------------|--------|------------|
| Trust boundary design freeze (T-002) | 2026-04-22 | High | Unresolved key-distribution or custody decisions can expand scope or stall implementation | Freeze D4/D9 decisions before T-002 execution starts |
| Bridge worker claim semantics (T-003) | 2026-04-27 | High | Duplicate downstream readiness outcomes would undermine correctness and trust | Finalize claim/idempotency behavior before orchestration coding; test restart safety early |
| Failure taxonomy hardening (T-005) | 2026-05-01 | Medium | Ambiguous retry vs terminal failure behavior can corrupt operational visibility | Resolve D5 before task execution and keep taxonomy explicit |
| Rollout/backfill policy finalization (T-006) | 2026-05-05 | Medium | Retention and backfill ambiguity can delay production activation | Prefer forward-only conservative rollout if policy remains unresolved |

## Timeline Visualization

```text
2026-04-15 to 2026-04-17  [T-001 Trusted Fetcher Stream Intake]
2026-04-20 to 2026-04-22  [T-002 Verified Artifact Retrieval And Custody]
2026-04-23 to 2026-04-27  [T-003 Automatic Completed-Extraction Bridging]
2026-04-28 to 2026-04-29  [T-004 Truthful Operational Readiness Projection]
2026-04-30 to 2026-05-01  [T-005 Retry-Safe Failure And Staleness Control]
2026-05-04 to 2026-05-05  [T-006 Controlled Rollout And Retention Operations]
2026-05-06 to 2026-05-07  [Contingency Buffer]
```

## Assumptions

1. One developer owns the feature end-to-end during the roadmap window.
2. Scope remains fixed to the six Gate 3 tasks.
3. No new external dependency is introduced for v1.
4. Existing Matcher infrastructure and local environments remain available.
5. Design decisions around key distribution, retention, and backfill are resolved in time for the relevant tasks.
6. AI-agent-hour estimates remain directionally accurate within normal variance.

## Constraints

1. Single-developer execution removes practical parallelization.
2. This roadmap assumes continuous delivery, so there are no sprint or cycle spillovers to manage.
3. Hybrid deployment support means rollout controls must remain infrastructure-agnostic.
4. Existing operational visibility surfaces are reused; no new UI may absorb status complexity.

## Confidence Score

**Score:** 88 / 100

Why:
- Dependency clarity is high.
- Team capacity is explicit.
- Critical path is straightforward.
- Remaining uncertainty is mostly policy-driven, not architectural.

## Gate 4 Validation

- All tasks are scheduled with concrete dates.
- Critical path is identified and validated.
- Team capacity uses the required 90% AI-agent baseline.
- Continuous delivery cadence is respected.
- Contingency buffer is included.
- High-risk milestones are flagged with mitigations.
