---
feature: fetcher-bridge
gate: 2
trd_standard_gate: 3
date: 2026-04-15
deployment:
  model: hybrid
tech_stack:
  primary: go-backend
  standards_loaded:
    - go-standards-index
    - go-bootstrap-observability
    - go-security
    - go-architecture
    - go-multi-tenant
    - devops
    - sre
project_technologies:
  - category: application-architecture
    prd_requirement: truthful end-to-end Fetcher source processing
    choice: modular-monolith extension with explicit cross-context bridge
    rationale: preserves existing bounded-context boundaries while introducing a controlled handoff from extraction completion to reconciliation readiness
  - category: processing-pattern
    prd_requirement: post-extraction work must be reliable, visible, and retry-safe
    choice: asynchronous background orchestration with durable lifecycle checkpoints
    rationale: avoids misleading inline success semantics and supports controlled retries and backlog handling
  - category: trust-boundary
    prd_requirement: only verified extraction output becomes usable reconciliation input
    choice: staged verification pipeline before downstream intake
    rationale: separates transport success from data trust and prevents unsafe data from entering reconciliation
  - category: storage-capability
    prd_requirement: matcher must retain its own tenant-scoped copy for operational custody
    choice: blob-storage custody layer with policy-driven retention
    rationale: decouples reconciliation readiness from upstream storage lifecycle and supports hybrid deployment portability
  - category: visibility-pattern
    prd_requirement: existing matcher dashboards must expose backlog and readiness outcomes
    choice: derived operational projection for dashboard endpoints
    rationale: keeps dashboard reads query-optimized and independent from write-path orchestration details
---

# TRD: Fetcher Bridge

## Architecture Summary

`fetcher-bridge` is a bounded integration pipeline that converts externally completed extraction results into trusted reconciliation input inside Matcher. The solution extends the existing modular monolith by introducing a dedicated bridge flow across discovery, ingestion, reporting, and governance responsibilities while preserving explicit context ownership.

The architecture separates four concerns that are currently collapsed into a misleading "extraction complete" outcome: completion detection, artifact trust verification, downstream intake, and operational readiness projection. This separation is the core mechanism that makes completion truthful.

## Architecture Style

### Primary Style

- Modular monolith extension
- Hexagonal context boundaries
- Asynchronous background orchestration
- CQRS-aligned operational projection for dashboard consumption

### Why this style fits

- The feature extends an existing bounded-context system rather than introducing a net-new product surface.
- The business problem spans multiple contexts, but still lives inside a single deployable service boundary.
- The feature requires reliable post-completion work that should not be coupled to synchronous request/response semantics.
- Existing operational visibility needs are best served by derived read models rather than by overloading orchestration records for dashboard reads.

## Component Design

### Component Map

| Component | Responsibility | Owning Context |
|-----------|----------------|----------------|
| Extraction Lifecycle Source | Expose completed extractions that have not yet become readiness outcomes | Discovery |
| Readiness Orchestrator | Coordinate the bridge lifecycle from completed extraction to readiness or failure | Discovery |
| Artifact Retrieval Gateway | Retrieve external extraction artifacts using tenant-scoped external credentials and location metadata | Shared integration boundary |
| Artifact Trust Verifier | Validate that retrieved content is authentic, intact, and safe to continue processing | Shared trust boundary |
| Artifact Custody Store | Persist Matcher-owned tenant-scoped custody copy according to retention policy | Shared storage boundary |
| Reconciliation Input Intake Port | Accept trusted extraction content into the existing normalization and reconciliation-input pipeline | Ingestion |
| Lifecycle Link Writer | Persist linkage between extraction lifecycle and downstream intake outcome | Discovery |
| Operational Readiness Projection | Expose dashboard-ready backlog, readiness, and failure views | Reporting |
| Audit Evidence Recorder | Preserve traceable business evidence for important lifecycle transitions when required | Governance |

### Component Responsibilities

#### 1. Extraction Lifecycle Source

This component identifies work that is eligible for bridging.

It must:
- surface completed extractions that do not yet have downstream readiness linkage
- preserve tenant-scoped work discovery
- support deterministic replay without duplicating successful outcomes

It must not:
- perform trust verification
- own downstream normalization behavior
- provide dashboard-ready aggregates directly

#### 2. Readiness Orchestrator

This is the central control component for the bridge.

It must:
- claim eligible work safely
- coordinate retrieval, verification, custody, and intake stages
- record durable stage transitions and outcome classifications
- apply retry policy for transient operational failures
- stop decisively on trust failures

It must not:
- embed domain parsing logic
- bypass tenant context controls
- expose raw internal lifecycle mechanics as user-facing semantics

#### 3. Artifact Trust Verifier

This component transforms "artifact exists" into "artifact is trustworthy." It is the architectural boundary that enforces FR-002.

It must:
- validate authenticity and integrity before downstream intake
- distinguish permanent trust failures from transient transport failures
- produce clear verification outcomes for lifecycle recording

It must not:
- accept ambiguous or partially verified artifacts as ready
- silently downgrade failures into warnings

#### 4. Reconciliation Input Intake Port

This component is the bridge handoff into existing downstream business processing.

It must:
- accept trusted artifact content as a programmatic intake operation
- return a stable downstream intake outcome or failure classification
- preserve existing ingestion ownership of normalization and downstream publication

It must not:
- re-own extraction lifecycle state
- accept untrusted upstream content

#### 5. Operational Readiness Projection

This component provides query-optimized read models for existing dashboard endpoints and operational reporting.

It must:
- distinguish completed, pending, ready, failed, and stale outcomes
- support aggregate counts and backlog aging views
- support drill-down from high-level status to correlatable lifecycle identifiers

It must not:
- require operators to infer readiness from low-level implementation detail
- become the source of truth for orchestration state

## Component Boundaries and Interfaces

### Required Ports

| Port | Purpose | Input | Output |
|------|---------|-------|--------|
| `CompletedExtractionReader` | Discover eligible completed extractions | tenant-scoped lifecycle query | list of bridge candidates |
| `BridgeWorkClaimer` | Prevent duplicate processing of the same candidate | bridge candidate identity | claim outcome |
| `ExternalArtifactLocator` | Resolve retrieval target from extraction metadata | completed extraction | retrieval descriptor |
| `ArtifactRetriever` | Fetch extraction content from external custody | retrieval descriptor | artifact stream + metadata |
| `ArtifactVerifier` | Produce trust decision for retrieved content | artifact stream + metadata | trusted content or terminal failure |
| `CustodyWriter` | Write Matcher-owned custody copy | trusted content + retention policy | custody reference |
| `ReconciliationInputIntake` | Hand trusted content into downstream intake | trusted content + source context | intake outcome |
| `LifecycleOutcomeRecorder` | Persist bridge stage and final outcome | bridge stage event | updated lifecycle record |
| `OperationalProjectionUpdater` | Refresh query-ready status views | bridge outcome event | projection update result |
| `AuditEvidenceWriter` | Persist immutable evidence where required | significant lifecycle event | audit acknowledgment |

### Interface Principles

- Discovery owns candidate selection and linkage metadata.
- Ingestion owns normalization and downstream reconciliation-input creation.
- Reporting owns aggregated operational views.
- Governance owns immutable evidence, not control flow.
- Shared integration ports carry only the minimum cross-context contract needed for the handoff.

## PRD Feature Mapping

| PRD Requirement | Architectural Realization |
|-----------------|--------------------------|
| FR-001 Truthful Completion | Readiness Orchestrator + Lifecycle Outcome Recorder + Operational Readiness Projection |
| FR-002 Verified Data Trust | Artifact Retrieval Gateway + Artifact Trust Verifier + guarded intake handoff |
| FR-003 End-to-End Traceability | Lifecycle Link Writer + correlatable identifiers + audit evidence support |
| FR-004 Operational Visibility | Operational Readiness Projection feeding existing dashboard endpoints |
| FR-005 Safe Multi-Tenant Operation | Tenant-context-based work discovery, custody, intake, and projection isolation |

## Data Architecture

### Ownership Model

| Data Concept | Owner | Notes |
|-------------|-------|-------|
| Extraction lifecycle record | Discovery | Source of truth for extraction completion and downstream linkage |
| Trusted artifact custody reference | Shared storage boundary with discovery linkage | References Matcher-owned custody location and policy outcome |
| Reconciliation input job or equivalent downstream intake outcome | Ingestion | Source of truth for normalization and downstream readiness handoff |
| Operational readiness projection | Reporting | Derived read model optimized for dashboard endpoints |
| Immutable lifecycle evidence | Governance | Only where business audit requirements demand append-only record |

### Conceptual Flow

1. Extraction lifecycle transitions into externally completed state.
2. Readiness orchestrator discovers eligible completed work lacking downstream readiness linkage.
3. External artifact descriptor is resolved from completion metadata.
4. Artifact content is retrieved.
5. Trust verification is performed.
6. Trusted content is written to Matcher custody storage according to retention policy.
7. Trusted content is handed to the downstream reconciliation-input intake port.
8. Downstream linkage is written back to the extraction lifecycle record.
9. Operational readiness projection is updated for dashboard endpoints.
10. Audit evidence is recorded for significant lifecycle transitions if required.

### Data Lifecycle Principles

- Raw external completion metadata does not imply readiness.
- Trusted content and readiness outcome are separate lifecycle concepts.
- Derived projections can be rebuilt from lifecycle truth and downstream linkage.
- Retention policy for custody copies must be explicit and policy-driven, not incidental.

## Integration Patterns

### Trigger Pattern

Primary pattern: asynchronous background orchestration.

Rationale:
- decouples extraction polling from downstream readiness work
- supports replay-safe backlog handling
- avoids long synchronous completion paths
- aligns with existing worker-oriented operational patterns

### External System Contract Pattern

The external system provides completion metadata and an externally retrievable artifact reference rather than a business-ready reconciliation input. The bridge therefore treats upstream completion as a precursor event, not a terminal success state.

### Downstream Handoff Pattern

Bridge-to-intake communication should be a direct internal application boundary, not a self-HTTP loop. This keeps the handoff inside the modular monolith while preserving context boundaries through shared ports.

### Projection Pattern

Operational dashboards should read from a derived projection, not from raw orchestration records. This supports efficient aggregation and avoids leaking low-level bridge internals into user-facing semantics.

## Security Architecture

### Authentication and Authorization

- Token-based authentication with role-based access control governs user access to operational status and dashboard endpoints.
- Machine-to-machine authentication governs access to external artifact retrieval and other service-level bridge interactions.
- Access control applies to visibility and operational actions; tenant context applies to data isolation.

### Tenant Isolation

- Tenant identity must be derived from validated execution context, never from request payloads or ad hoc parameters.
- Work discovery, artifact custody, downstream intake, and projection updates must all remain scoped to the originating tenant.
- Default-tenant handling must be explicit so eligible work is not silently skipped.

### Trust Boundary Controls

- Retrieval success alone is insufficient to continue processing.
- Trust verification is the mandatory gate before custody storage and downstream intake.
- Permanent trust failures must terminate the readiness flow and surface explicit lifecycle outcomes.

### Secret and Credential Handling

- External access credentials and verification material must be sourced from centralized secret management and never logged.
- Runtime controls may tune operational behavior, but secret material must remain outside operator-visible telemetry.

## Reliability and Failure Semantics

### Failure Classes

| Failure Class | Architectural Treatment |
|---------------|-------------------------|
| Transient retrieval or connectivity failure | retryable within bounded policy |
| Permanent trust verification failure | terminal failure outcome |
| Downstream intake failure after trust verification | explicit downstream failure outcome with controlled replay semantics |
| Projection update failure | non-authoritative side-effect failure; must not erase core lifecycle truth |

### Idempotency Model

- Every eligible extraction candidate must have a stable bridge identity.
- Reprocessing the same bridge identity must not create duplicate downstream readiness outcomes.
- Custody writes and downstream handoffs must be replay-safe under worker restart conditions.

### Staleness Model

- Completed extractions that remain unresolved beyond the agreed operational window must become visible as stale.
- Staleness is an operational status derived from elapsed time, not a substitute for root-cause failure classification.

## Operational Visibility Architecture

### Required Read Models

Existing Matcher dashboard endpoints should be able to answer:
- how many completed extractions exist
- how many became ready
- how many remain pending
- how many failed
- how many are stale

### Read Pattern

- aggregate views for operational health
- filtered backlog views for triage
- correlatable detail views keyed by lifecycle identifiers

### Pagination Strategy

For backlog and triage views, cursor-based pagination is preferred because the dataset can grow continuously and operators primarily move forward through recent problem cases rather than jump to arbitrary page numbers.

## Deployment Topology

### Deployment Model: Hybrid

The architecture must work across both managed-cloud and customer-managed environments.

Implications:
- storage, messaging, and secret-management dependencies are treated as capabilities rather than vendor-specific services
- retention, connectivity, and runtime controls must be externally configurable
- bridge behavior must not assume a single infrastructure hosting model
- operational visibility must remain available regardless of deployment substrate

## Quality Attributes

### Performance

- The bridge must support streaming-oriented artifact processing rather than architecture that assumes full in-memory buffering.
- Readiness processing must complete within a configurable operational window defined after planning.

### Scalability

- The orchestrator must support per-tenant backlog processing without coupling all tenants into a single global bottleneck.
- Projection reads must remain efficient as completed extraction volume grows.

### Reliability

- Worker restart must not lose bridge progress or create duplicate readiness outcomes.
- Permanent trust failures must be explicit and observable.

### Observability

- Every major bridge stage must emit correlatable operational telemetry.
- Operators need a clear separation between pending work, terminal failure, and ready outcomes.

## ADRs

### ADR-001: Use asynchronous background orchestration rather than inline completion handoff

- Context: extraction completion today is visible before readiness exists, and inline handoff would keep that coupling fragile.
- Options: inline callback handoff, background orchestration, explicit manual materialization.
- Decision: background orchestration.
- Rationale: best supports reliability, backlog handling, replay safety, and truthful lifecycle semantics.
- Consequences: requires candidate discovery, durable work claiming, and explicit lifecycle checkpoints.

### ADR-002: Treat trust verification as a mandatory architectural stage before downstream intake

- Context: external completion metadata does not guarantee readiness-safe content.
- Options: direct intake after retrieval, verification before intake, best-effort verification after intake.
- Decision: verification before intake.
- Rationale: preserves product trust and prevents unsafe data from entering reconciliation.
- Consequences: introduces explicit terminal trust-failure outcomes and a dedicated trust boundary.

### ADR-003: Serve dashboards from derived operational projections rather than orchestration records

- Context: existing operational views need fast, comprehensible status visibility.
- Options: query raw lifecycle records, derive read-optimized projection, expose only engineering traces.
- Decision: derive read-optimized projection.
- Rationale: supports efficient aggregation and product-facing status semantics.
- Consequences: projection freshness becomes an operational concern, but source-of-truth ownership remains clear.

## Gate 2 Validation

- All PRD requirements are mapped to architecture components.
- Component boundaries are explicit and remain aligned to existing bounded contexts.
- Interfaces are technology-agnostic.
- Deployment model is recorded as `Hybrid`.
- No vendor-specific infrastructure products are named in the architecture.
