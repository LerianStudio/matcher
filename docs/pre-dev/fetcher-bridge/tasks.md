# Tasks: Fetcher Bridge

## Summary

| Task | Title | Type | Hours | Confidence | Blocks | Status |
|------|-------|------|-------|------------|--------|--------|
| T-001 | Trusted Fetcher Stream Intake | Foundation | 14.0 | Medium | T-003 | ✅ Done |
| T-002 | Verified Artifact Retrieval And Custody | Foundation | 13.0 | Medium | T-003 | ⏸️ Pending |
| T-003 | Automatic Completed-Extraction Bridging | Feature | 14.0 | Medium | T-004, T-005, T-006 | ⏸️ Pending |
| T-004 | Truthful Operational Readiness Projection | Integration | 10.0 | High | T-005 | ⏸️ Pending |
| T-005 | Retry-Safe Failure And Staleness Control | Integration | 12.0 | Medium | T-006 | ⏸️ Pending |
| T-006 | Controlled Rollout And Retention Operations | Polish | 8.0 | Low |  | ⏸️ Pending |
| | **TOTAL** | | **71.0h** | | | |

## Business Deliverables

| Task | Deliverable (business view) |
|------|-----------------------------|
| T-001 | **Trusted internal ingestion becomes possible**. Matcher can accept verified Fetcher-produced data without pretending it came from a user-upload flow. |
| T-002 | **External extraction output becomes trustworthy custody data**. Matcher can prove that retrieved Fetcher data is authentic before it is allowed to influence reconciliation. |
| T-003 | **FETCHER sources actually work end-to-end**. A completed extraction can now turn into usable reconciliation input without manual fallback. |
| T-004 | **Operations can finally trust the status they see**. Existing Matcher dashboards and operational views can distinguish pending, ready, failed, and stale bridge outcomes. |
| T-005 | **Production failures become operable instead of mysterious**. Teams can tell whether they should wait, retry, or escalate, and stale backlog stops hiding in “complete” states. |
| T-006 | **The bridge can be introduced safely in real environments**. Teams get controlled rollout behavior, explicit retention handling, and a path for backfill or forward-only activation. |

## T-001: Trusted Fetcher Stream Intake

**Type:** Foundation

**Deliverable:** Matcher can accept a trusted internal stream from the Fetcher bridge and create a downstream ingestion outcome without relying on multipart HTTP upload.

**Scope**

Includes:
- a cross-context intake boundary from the bridge into ingestion
- support for trusted internal stream-based intake
- support for Fetcher-shaped content transformation into existing intake expectations

Excludes:
- external artifact retrieval and trust verification
- worker orchestration and claiming logic
- dashboard visibility work

**Success Criteria**

Functional:
- trusted Fetcher-produced content can create a downstream intake outcome through an internal application boundary
- the intake path reuses existing ingestion business behavior rather than inventing a separate processing pipeline

Technical:
- bounded-context isolation is preserved through shared ports and cross adapters
- no self-HTTP loop is required for internal bridge handoff

Operational:
- downstream intake output is linkable back to the originating extraction lifecycle

Quality:
- regression coverage exists for stream-based intake and Fetcher-shaped content handling

**User Value:** Operators no longer depend on an upload-only path for Fetcher-backed reconciliation data.

**Technical Value:** Establishes the internal bridge-to-ingestion handoff required by all later tasks.

**Technical Components**

From TRD:
- Reconciliation Input Intake Port
- Lifecycle Link Writer

From architecture constraints:
- shared cross-context adapter pattern
- existing ingestion command core

**Dependencies**

Blocks:
- T-003

Requires:
- none

Optional:
- none

**Effort Estimate**

- Baseline: AI Agent via `ring:dev-cycle`
- AI Estimate: 14.0 AI-agent-hours
- Estimation Method: `ring:backend-engineer-golang`
- Confidence: Medium

Breakdown:
- cross-context intake contract: 3.0h
- ingestion use-case extension around existing stream core: 4.0h
- Fetcher-shape transformation behavior: 4.0h
- bootstrap and regression coverage: 3.0h

**Risks**

Risk 1:
Impact: High
Probability: Medium
Mitigation: keep the intake boundary thin and reuse existing downstream business logic
Fallback: narrow scope to the minimum Fetcher-specific intake contract for v1

Risk 2:
Impact: Medium
Probability: Medium
Mitigation: treat nested Fetcher content shape as an explicit intake concern, not an implicit parser assumption
Fallback: introduce a Fetcher-specific transform stage before generic downstream normalization

**Testing Strategy**

- Unit: internal intake boundary behavior, Fetcher-shape transformation, downstream linkage handling
- Integration: trusted stream intake creates expected downstream outcome and preserves existing side effects
- Regression: existing upload-based ingestion path remains unaffected

**Definition of Done**

- internal trusted stream intake path is available
- downstream intake outcome is produced and correlatable
- cross-context boundary remains architecture-compliant
- unit and integration tests pass
- telemetry and error semantics are defined for the new intake path

## T-002: Verified Artifact Retrieval And Custody

**Type:** Foundation

**Deliverable:** Matcher can retrieve a completed Fetcher artifact, verify it as trustworthy, and write a Matcher-owned tenant-scoped custody copy governed by policy.

**Scope**

Includes:
- artifact retrieval from external custody
- trust verification before downstream use
- custody-copy persistence under Matcher ownership

Excludes:
- bridge worker scheduling and candidate claiming
- downstream intake execution
- operational status projection

**Success Criteria**

Functional:
- completed extraction metadata can resolve to retrievable artifact content
- retrieved content must either become trusted custody content or an explicit terminal trust failure

Technical:
- retrieval, verification, and custody are separable stages with distinct outcomes
- tenant-scoped custody rules are preserved in both default and non-default tenant modes

Operational:
- custody reference can be correlated to the originating extraction
- terminal trust failures are distinguishable from transient retrieval failures

Quality:
- deterministic coverage exists for good content, integrity failure, and retrieval failure paths

**User Value:** Operators can trust that only verified Fetcher data is allowed to influence reconciliation.

**Technical Value:** Establishes the trust boundary the whole feature depends on.

**Technical Components**

From TRD:
- Artifact Retrieval Gateway
- Artifact Trust Verifier
- Artifact Custody Store

**Dependencies**

Blocks:
- T-003

Requires:
- none

Optional:
- none

**Effort Estimate**

- Baseline: AI Agent via `ring:dev-cycle`
- AI Estimate: 13.0 AI-agent-hours
- Estimation Method: `ring:backend-engineer-golang`
- Confidence: Medium

Breakdown:
- retrieval/config bootstrap generalization: 4.0h
- verification service behavior: 4.0h
- custody-write behavior: 2.0h
- tests around trust and storage: 3.0h

**Risks**

Risk 1:
Impact: High
Probability: High
Mitigation: lock design decisions on key distribution and custody form before implementation starts
Fallback: constrain v1 to the simplest approved trust boundary consistent with policy

Risk 2:
Impact: Medium
Probability: Medium
Mitigation: validate that current completion metadata can represent real retrieval targets without brittle assumptions
Fallback: add a normalization step between extraction metadata and retrieval descriptor construction

**Testing Strategy**

- Unit: trust verification result classes, custody-path behavior, tenant scoping
- Integration: external retrieval plus custody persistence in a dev-compatible object-storage environment
- Negative-path: integrity failure and decrypt/authentication failure stay terminal

**Definition of Done**

- retrieval and verification stages are operationally distinct
- trusted content produces custody reference
- untrusted content is rejected explicitly
- tenant-safe custody behavior is covered
- automated tests pass for success and failure paths

## T-003: Automatic Completed-Extraction Bridging

**Type:** Feature

**Deliverable:** A background bridge automatically turns eligible completed extractions into downstream ingestion outcomes and writes the extraction-to-ingestion linkage.

**Scope**

Includes:
- eligible-work discovery
- safe claim and orchestration semantics
- happy-path retrieval, verification, custody, intake, and linkage write-back

Excludes:
- dashboard-ready projection details beyond minimal core status updates
- full retry taxonomy and stale handling
- rollout policy and backfill controls

**Success Criteria**

Functional:
- eligible completed extractions with no downstream linkage are automatically processed
- successful bridge execution creates and persists downstream linkage

Technical:
- duplicate worker execution does not create duplicate downstream readiness outcomes
- worker lifecycle follows existing background processing standards

Operational:
- default tenant is included in bridge work discovery
- bridge work can recover safely from restart without replaying success incorrectly

Quality:
- happy-path integration coverage exists from completed extraction to populated downstream linkage

**User Value:** This is the first point at which FETCHER sources become truly usable end-to-end.

**Technical Value:** Converts the bridge from isolated primitives into working product behavior.

**Technical Components**

From TRD:
- Extraction Lifecycle Source
- Readiness Orchestrator
- Lifecycle Link Writer

**Dependencies**

Blocks:
- T-004
- T-005
- T-006

Requires:
- T-001
- T-002

Optional:
- none

**Effort Estimate**

- Baseline: AI Agent via `ring:dev-cycle`
- AI Estimate: 14.0 AI-agent-hours
- Estimation Method: `ring:backend-engineer-golang`
- Confidence: Medium

Breakdown:
- candidate-scan and claim semantics: 4.0h
- worker lifecycle and tenant fanout: 5.0h
- orchestration and linkage updates: 3.0h
- restart/idempotency coverage: 2.0h

**Risks**

Risk 1:
Impact: High
Probability: Medium
Mitigation: make claim semantics explicit before coding the worker loop
Fallback: serialize bridge execution more conservatively in v1 if concurrency risk remains unresolved

Risk 2:
Impact: Medium
Probability: Medium
Mitigation: isolate orchestration responsibilities from projection and policy concerns
Fallback: ship only happy-path bridge automation first, with advanced policy deferred to later tasks

**Testing Strategy**

- Unit: candidate selection, claim/no-claim behavior, tenant fanout semantics
- Integration: completed extraction to downstream linkage flow under worker execution
- Reliability: restart-safe and duplicate-claim prevention tests

**Definition of Done**

- automatic bridge worker runs through happy path
- downstream linkage is persisted
- restart-safe/idempotent execution is demonstrated
- default tenant behavior is verified
- observability hooks exist for core lifecycle stages

## T-004: Truthful Operational Readiness Projection

**Type:** Integration

**Deliverable:** Existing Matcher operational dashboard endpoints can distinguish pending, ready, failed, and stale Fetcher bridge outcomes with correlatable lifecycle identifiers.

**Scope**

Includes:
- read-model and query support for bridge readiness states
- dashboard-facing aggregate and drill-down data needs
- status semantics aligned to PRD and UX criteria

Excludes:
- net-new screens or net-new dashboard UI
- advanced retry policy implementation
- retention and rollout controls

**Success Criteria**

Functional:
- existing operational views can expose backlog, ready counts, and failure counts for Fetcher bridge processing
- correlatable identifiers support drill-down from aggregate view to lifecycle record

Technical:
- dashboard queries read from a projection or query-optimized surface, not from ad hoc orchestration scans

Operational:
- operators can tell whether a completed extraction is pending, ready, failed, or stale without engineering-only interpretation

Quality:
- projection queries and endpoint behavior are regression-covered

**User Value:** Operators can finally trust the status they see for Fetcher-backed processing.

**Technical Value:** Separates product-facing status truth from write-path orchestration internals.

**Technical Components**

From TRD:
- Operational Readiness Projection

**Dependencies**

Blocks:
- T-005

Requires:
- T-003

Optional:
- none

**Effort Estimate**

- Baseline: AI Agent via `ring:dev-cycle`
- AI Estimate: 10.0 AI-agent-hours
- Estimation Method: `ring:backend-engineer-golang`
- Confidence: High

Breakdown:
- read-model and repository work: 4.0h
- query/use-case/endpoint wiring: 3.0h
- DTO/cache/regression coverage: 3.0h

**Risks**

Risk 1:
Impact: Medium
Probability: Low
Mitigation: keep status semantics fixed in one projection contract
Fallback: restrict v1 visibility to the smallest operator-useful metric set

Risk 2:
Impact: Medium
Probability: Medium
Mitigation: do not expose low-level orchestration records directly to dashboard consumers
Fallback: create an explicit translation layer between lifecycle state and dashboard status

**Testing Strategy**

- Unit: readiness-state mapping and aggregate calculations
- Integration: dashboard-facing query paths over realistic bridge outcomes
- Tenant visibility: tenant-safe aggregation and filtering checks

**Definition of Done**

- dashboard endpoints can distinguish bridge readiness outcomes
- backlog and aging information is available through existing product surfaces
- lifecycle drill-down identifiers are exposed consistently
- automated tests pass for aggregate and detail behaviors

## T-005: Retry-Safe Failure And Staleness Control

**Type:** Integration

**Deliverable:** The bridge classifies failures meaningfully, retries only safe transient failures, marks permanent failures explicitly, and surfaces stale backlog conditions.

**Scope**

Includes:
- failure taxonomy across retrieval, trust, and downstream intake stages
- bounded retry policy for transient failures
- stale backlog classification and surfacing

Excludes:
- broader rollout/backfill policy
- new UI or manual operations screens

**Success Criteria**

Functional:
- transient failures are retryable and permanent trust failures are terminal
- stale bridge candidates become visible through defined status semantics

Technical:
- failure taxonomy is persisted in a form that supports projection and support diagnosis
- retry logic does not duplicate successful downstream readiness outcomes

Operational:
- operators and support can distinguish whether the system should wait, retry, or escalate

Quality:
- failure classification and retry logic are covered with deterministic tests

**User Value:** Production failures stop being mysterious and start being actionable.

**Technical Value:** Makes the bridge operable in real-world conditions rather than only in happy-path demos.

**Technical Components**

From TRD:
- Reliability and Failure Semantics
- Operational Readiness Projection

**Dependencies**

Blocks:
- T-006

Requires:
- T-003
- T-004

Optional:
- none

**Effort Estimate**

- Baseline: AI Agent via `ring:dev-cycle`
- AI Estimate: 12.0 AI-agent-hours
- Estimation Method: `ring:backend-engineer-golang`
- Confidence: Medium

Breakdown:
- failure model and lifecycle persistence: 4.0h
- bounded retry and replay safety: 4.0h
- stale logic and visibility coverage: 4.0h

**Risks**

Risk 1:
Impact: High
Probability: High
Mitigation: lock explicit failure-policy decisions before detailed implementation begins
Fallback: ship a narrower initial taxonomy and expand once policy is settled

Risk 2:
Impact: Medium
Probability: Medium
Mitigation: ensure projection semantics are derived from source-of-truth lifecycle data, not inferred loosely
Fallback: keep stale classification server-side only until semantics are stable

**Testing Strategy**

- Unit: failure classification and retryability rules
- Integration: retry path, terminal trust failure path, downstream-failure path, stale classification path
- Reliability: duplicate-prevention and restart behavior under failure

**Definition of Done**

- failure taxonomy exists and is operationally visible
- retry behavior is bounded and safe
- stale backlog is surfaced explicitly
- test suite covers transient, terminal, and stale cases

## T-006: Controlled Rollout And Retention Operations

**Type:** Polish

**Deliverable:** The bridge can be safely enabled, retained, and rolled out with explicit policy controls for custody retention and forward-only or backfill operation.

**Scope**

Includes:
- rollout control switches and operational safeguards
- custody retention enforcement behavior
- forward-only or backfill-capable activation model

Excludes:
- any broad new migration framework
- net-new operational UI

**Success Criteria**

Functional:
- the bridge can be enabled or withheld intentionally
- custody retention behavior follows explicit policy
- rollout mode is unambiguous for new versus historical completed extractions

Technical:
- policy controls do not break existing Fetcher extraction behavior when disabled

Operational:
- teams can activate the bridge in hybrid environments without needing one-off code changes

Quality:
- policy and rollout behaviors are regression-covered

**User Value:** Teams can introduce the bridge safely instead of flipping it on blindly in production.

**Technical Value:** Reduces deployment risk and prevents hidden retention or backfill surprises.

**Technical Components**

From TRD:
- Deployment Topology
- Data Lifecycle Principles
- Quality Attributes

**Dependencies**

Blocks:
- none

Requires:
- T-003
- T-005

Optional:
- T-004

**Effort Estimate**

- Baseline: AI Agent via `ring:dev-cycle`
- AI Estimate: 8.0 AI-agent-hours
- Estimation Method: `ring:backend-engineer-golang`
- Confidence: Low

Breakdown:
- rollout control behavior: 2.0h
- retention enforcement: 3.0h
- backfill/forward-only handling and tests: 3.0h

**Risks**

Risk 1:
Impact: High
Probability: High
Mitigation: finalize retention and backfill decisions before implementation planning is frozen
Fallback: default to forward-only rollout with conservative retention if policy remains unresolved

Risk 2:
Impact: Medium
Probability: Medium
Mitigation: keep rollout controls explicit and reversible
Fallback: scope v1 to feature-disabled-by-default with manual enablement only

**Testing Strategy**

- Unit: policy-switch behavior and retention rules
- Integration: disabled-mode passthrough, enabled-mode behavior, forward-only/backfill behavior
- Operational: configuration regression checks across hybrid deployment assumptions

**Definition of Done**

- rollout behavior is explicit and testable
- retention policy is enforced, not implied
- forward-only or backfill mode is unambiguous
- disabling the bridge preserves pre-existing behavior

## Delivery Sequencing

### Critical Path

1. T-001 Trusted Fetcher Stream Intake
2. T-002 Verified Artifact Retrieval And Custody
3. T-003 Automatic Completed-Extraction Bridging
4. T-004 Truthful Operational Readiness Projection
5. T-005 Retry-Safe Failure And Staleness Control
6. T-006 Controlled Rollout And Retention Operations

### Parallelization Opportunities

- T-001 and T-002 can start in parallel because one establishes downstream intake and the other establishes trusted artifact handling.
- T-004 can start once T-003 stabilizes the lifecycle contract, even if T-005 policy hardening is still underway.
- T-006 should wait until core bridge semantics are stable because rollout and retention choices are policy-sensitive.

## Gate 3 Validation

- All TRD components are covered by tasks.
- Every task delivers working software.
- No task exceeds 16 AI-agent-hours.
- Dependencies are explicit and non-circular.
- Testing strategy is defined per task.
- Risks and mitigations are documented.
