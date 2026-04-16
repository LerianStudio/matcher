# UX Criteria: Fetcher Bridge

## Problem Validation Status

**Status:** Validated

`fetcher-bridge` solves an operator-trust problem, not a visual-design problem. Today, a Fetcher-backed source can appear successfully extracted without becoming usable reconciliation data, which creates a misleading completion signal for reconciliation operations and a support burden for platform teams.

This feature is backend-only. The relevant user experience is therefore the operational experience of truth, clarity, and traceability across existing Matcher workflows and views.

## Refined Personas

### Reconciliation Operations Analyst

**Primary need:** Trust that a completed Fetcher-backed run actually results in usable reconciliation input.

**Needs from this feature:**
- Truthful status progression
- Clear indication when intervention is required
- Confidence that supported Fetcher sources do not require manual fallback in normal operation

### Platform / Integration Engineer

**Primary need:** Diagnose where a Fetcher-backed run stopped and whether it is safe to retry or escalate.

**Needs from this feature:**
- Distinct lifecycle stages
- Clear failure categorization
- Correlatable run identifiers across extraction and downstream readiness

### Risk / Audit Stakeholder

**Primary need:** Trust the lineage from external extraction to usable reconciliation data.

**Needs from this feature:**
- Verifiable readiness outcome
- Explicit rejection of invalid or unsafe data
- Reviewable operational trail for key lifecycle events

## UX Acceptance Criteria

### Functional Experience Criteria

1. A Fetcher extraction that is shown as ready for downstream use must truly be usable reconciliation input.
2. A Fetcher extraction that does not become usable must surface an explicit failure outcome rather than remaining misleadingly complete.
3. Operators must be able to tell whether a Fetcher-backed run is pending, ready, failed, or stale using existing Matcher operational surfaces.
4. Support teams must be able to correlate extraction completion with downstream readiness using stable business identifiers.
5. Invalid or unsafe extracted data must be rejected decisively rather than partially accepted.

### Usability Criteria

1. Lifecycle states must use plain-language labels that match operator expectations.
2. Failure outcomes must be distinguishable by category, not collapsed into a single vague error state.
3. An operator should be able to answer, without engineering-only investigation:
   - Did the extraction complete?
   - Did it become usable?
   - If not, what broad class of failure happened?
   - Does this require waiting, retry, or escalation?
4. Existing operational views must not require users to infer readiness from indirect signals.

### Trust and Safety Criteria

1. The system must never present untrusted extracted data as ready for reconciliation.
2. Integrity-related failures must be surfaced as explicit stop conditions.
3. The bridge must preserve tenant-safe behavior and prevent cross-tenant confusion in operational reporting.

## Operational Visibility Requirements

This feature does not add new UI. It must, however, support existing Matcher operational visibility and dashboard data needs.

### Required Operational Questions the Product Must Answer

1. How many Fetcher extractions have completed?
2. How many completed Fetcher extractions became usable reconciliation input?
3. How many are pending post-extraction processing?
4. How many failed, and by which broad failure category?
5. Which completed extractions have been waiting too long to become usable?

### Required Outcome Semantics

- **Pending:** extraction completed, downstream readiness still in progress
- **Ready:** extracted data is usable for reconciliation
- **Failed:** extracted data did not become usable and requires attention or retry policy handling
- **Stale:** completed extraction has exceeded the expected time window without becoming ready

### Dashboard Data Expectations

Existing Matcher operational views should be able to represent:
- Volume of completed versus ready Fetcher-backed runs
- Failure distribution over time
- Backlog aging for completed-but-not-ready runs
- Tenant-appropriate operational trend visibility

## Accessibility and Usability Considerations

There is no new interface in v1, so no wireframes or screen-level accessibility requirements apply. The relevant accessibility and usability expectations are:

1. Any status surfaced in existing product views must use language that is understandable without engineering-specific interpretation.
2. Existing views should not rely on color alone to distinguish readiness or failure if this data is visualized later.
3. Labels and exported status values should be consistent across product surfaces, audit review, and support workflows.
4. Operational information should be structured so that it can be consumed clearly in dashboards, tables, logs, and audit exports.

## Risks and Gaps in the PRD

1. The PRD intentionally leaves the acceptable time window for post-extraction readiness open; this should be fixed before delivery planning.
2. The PRD does not yet specify which failure categories must be visible to business operators versus platform-only consumers.
3. Historical backfill versus forward-only rollout remains unresolved and may affect success measurement.
4. The PRD assumes existing Matcher operational views are the right destination for visibility, but does not yet define the minimum required visibility contract those views must satisfy.

## Verdict

Gate 1 UX criteria are sufficient for a backend-only feature, provided the team treats operational truthfulness and visibility as product requirements rather than implementation details.
