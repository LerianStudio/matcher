# PRD: Fetcher Bridge

## Executive Summary

`fetcher-bridge` closes the gap between successful Fetcher extractions and usable reconciliation data inside Matcher. Today, a source can appear successfully extracted without becoming available for downstream reconciliation, which creates false confidence for operators and leaves a supported source type operationally incomplete. This feature makes Fetcher-backed sources trustworthy and usable end-to-end by ensuring completed extraction results become verified, traceable, and match-ready within Matcher.

## Problem Statement

Matcher currently allows teams to configure and run Fetcher-backed sources, but a completed Fetcher extraction does not become usable reconciliation data inside Matcher. The result is an operational trust gap: operators see extraction success without receiving matchable output, forcing fallback workflows and undermining confidence in the product's end-to-end reconciliation promise.

### Impact

- Fetcher-backed sources are not fully usable in practice even though they appear supported.
- Reconciliation operators cannot rely on extraction completion as a truthful signal that data is ready for downstream use.
- Platform teams inherit avoidable support overhead when they must investigate or work around stalled post-extraction flow.
- Audit and risk stakeholders lack a complete lineage from extraction completion to reconciliation readiness.

### Current Workarounds

- Operators fall back to manual or alternate ingestion paths when Fetcher-completed data does not progress.
- Support and platform teams correlate multiple internal states manually to determine whether a Fetcher extraction actually became usable.

## Research Basis

- Existing product gap documented in `docs/pre-dev/fetcher-bridge/research.md`.
- Current state evidence that Fetcher completion does not flow into ingestion or matching: `FETCHER_MATCHER.md:16-22`, `FETCHER_MATCHER.md:48-58`.
- Existing operator-visible extraction status and downstream linkage expectations: `internal/discovery/adapters/http/dto/responses.go:74-133`.

## Primary Users and Personas

### Reconciliation Operations Analyst

**Goal:** Run reconciliations without manual intervention or hidden post-extraction failure states.

**Frustrations:**
- A completed extraction is not a reliable signal that data is actually ready.
- Troubleshooting requires escalation to engineering or platform teams.

### Platform / Integration Engineer

**Goal:** Operate Fetcher-backed reconciliation flows reliably across tenants and sources.

**Frustrations:**
- The current flow stops after extraction success, leaving an incomplete automation chain.
- Failures after extraction are difficult to classify and support consistently.

### Risk / Audit Stakeholder

**Goal:** Trust that reconciled data has a clear, verifiable lineage from extraction through readiness.

**Frustrations:**
- Current completion semantics overstate what the system has actually delivered.
- There is no clear business-level confirmation that extracted data became usable reconciliation input.

## User Stories

1. As a reconciliation operator, I want a completed Fetcher extraction to become usable reconciliation data so that I can trust Fetcher-backed sources without manual fallback.
2. As a reconciliation operator, I want failed post-extraction processing to be clearly categorized so that I know when data is not safe or ready to use.
3. As a platform engineer, I want end-to-end traceability from extraction completion to reconciliation readiness so that I can diagnose issues quickly and support tenants reliably.
4. As a risk or audit stakeholder, I want a trustworthy record that extracted data became verified and ready for reconciliation so that operational claims can be validated.

## Functional Requirements

### FR-001 Truthful Completion

Fetcher-backed sources must progress from extraction completion to usable reconciliation input inside Matcher.

**Acceptance Criteria**
- A completed Fetcher extraction results in a corresponding reconciliation-ready outcome or an explicit failure outcome.
- Operators are not left with a misleading "complete but unusable" state.

### FR-002 Verified Data Trust

Matcher must only treat Fetcher extraction output as usable once its authenticity and integrity have been verified.

**Acceptance Criteria**
- Unverified or corrupted extraction output is never treated as usable reconciliation input.
- Integrity-related failures are surfaced as explicit failure outcomes rather than silent retries or ambiguous statuses.

### FR-003 End-to-End Traceability

Operators and support teams must be able to follow the lifecycle from Fetcher extraction to reconciliation readiness.

**Acceptance Criteria**
- Each Fetcher-backed run can be traced from extraction completion to downstream readiness outcome.
- Support teams can determine whether a failure happened before readiness, during readiness, or after readiness was established.

### FR-004 Operational Visibility

The feature must support operational decision-making through existing Matcher visibility surfaces.

**Acceptance Criteria**
- Existing operational views can distinguish backlog, success, and failure states for Fetcher-backed processing.
- Operators can identify stalled or failed post-extraction work without engineering-only investigation.

### FR-005 Safe Multi-Tenant Operation

The feature must preserve tenant isolation and prevent one tenant's extraction results from affecting another tenant's reconciliation flow.

**Acceptance Criteria**
- Processing outcomes are isolated to the originating tenant's business context.
- Operational visibility and traceability remain tenant-appropriate.

## Business Requirements

### Access and Control

- Only authenticated and appropriately authorized users may view or act on Fetcher-related operational status.
- The system must support accountability for who triggered, observed, or investigated significant operational actions related to this flow.

### Auditability

- The business must be able to demonstrate that Fetcher-backed data became trusted, usable reconciliation input or was explicitly rejected.
- Significant state changes in the bridge lifecycle must be traceable for operational review.

## Dashboard Requirements

**Consumer:** Reconciliation operations, platform support, and business or operations managers using existing Matcher dashboard capabilities.

**Decisions Supported:**
- Whether Fetcher-backed processing is healthy
- Whether completed extractions are accumulating without becoming usable
- Whether failures are rising or concentrated in certain sources or tenants
- Whether manual intervention or escalation is required

**Key Metrics:**
- Total Fetcher extractions completed
- Total Fetcher extractions that became usable reconciliation input
- Distribution by status: pending, ready, failed, stale
- Failure trend over time
- Aging of completed extractions that have not yet become usable

**Refresh Expectation:** Near real-time operational visibility.

**Note:** This feature does not introduce a net-new dashboard user interface in v1. It must support existing Matcher operational visibility patterns.

## Success Metrics

- At least 95% of supported completed Fetcher extractions become usable reconciliation input without manual fallback within the agreed operational window.
- 100% of post-extraction failures surface an explicit, operator-recognizable failure outcome rather than an ambiguous completed state.
- Manual fallback for supported Fetcher-backed sources is reduced to near zero in normal operation after rollout.
- Support teams can determine the lifecycle outcome of a Fetcher-backed run from existing product visibility and traceability surfaces without deep engineering-only investigation.

## In Scope

- Turning completed Fetcher extractions into usable reconciliation input inside Matcher
- Making Fetcher-backed source support operationally truthful end-to-end
- Providing clear lifecycle visibility for success, pending, and failure outcomes
- Supporting existing operational dashboard and reporting needs for this flow
- Preserving tenant-safe processing and business traceability

## Out of Scope

- Net-new user interfaces or net-new dashboard screens for v1
- Manual re-drive or manual reprocessing screens in v1
- New external dependencies introduced solely for this feature in v1
- Broad redesign of reconciliation rule authoring or source modeling beyond what is required to make Fetcher-backed sources usable
- Expanding this v1 into a generic ingestion redesign for all source types

## Assumptions

- Fetcher-backed sources remain a supported Matcher capability and should be operationally complete, not advisory.
- Existing Matcher operational visibility surfaces are the right first place to expose this feature's outcomes.
- This feature is primarily backend and operator-facing, even though it has dashboard visibility implications.

## Open Questions

- What is the exact business commitment for how quickly a completed extraction should become usable reconciliation input?
- Should historical completed extractions be brought into the new flow on rollout, or is the initial release forward-only?
- Which failure categories must be visible to operators versus reserved for platform-only diagnostics?
