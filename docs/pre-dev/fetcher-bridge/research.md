---
feature: fetcher-bridge
gate: 0
date: 2026-04-15
research_mode: integration
agents_dispatched: 4
topology:
  scope: backend-only
  structure: single-repo
auth_requirement: project-integrated-lib-auth
license_requirement: none
has_ui: false
---

# Research: Fetcher Bridge

## Executive Summary

`fetcher-bridge` is an integration feature, not a simple modification. Matcher already submits Fetcher extraction jobs and polls them to completion, but completed results never flow into ingestion or matching, so `SourceTypeFetcher` is operationally incomplete.

The research result is clear: the safest shape is a tenant-aware background bridge that picks up completed extractions, downloads and verifies the artifact, stores a Matcher-owned copy, and hands the stream into the existing ingestion command flow through a shared cross-context boundary. The main code gap is not parsing logic, but orchestration: repository query surface, worker lifecycle, storage/crypto integration, and truthful downstream status semantics.

## Research Mode

Mode selected: `integration`

Why:
- The feature connects Matcher to an external system contract owned by Fetcher.
- The critical design surface spans object storage, crypto verification, multi-tenancy, worker orchestration, and cross-context ingestion handoff.
- Existing repo patterns matter, but external best practices also materially affect the design because integrity and key-handling are part of the problem.

## Codebase Research

### Existing patterns to follow

- Discovery completion is already callback-shaped but unused today. `ExtractionPoller.PollUntilComplete` supports `onComplete` and `onFailed`, yet the current caller passes `nil` callbacks, so no downstream bridge runs after `COMPLETE` is persisted.
  Refs: `internal/discovery/services/worker/extraction_poller.go:98-103`, `internal/discovery/services/worker/extraction_poller.go:347-351`, `internal/discovery/services/command/extraction_commands.go:127-131`

- The repository already models the downstream linkage hook. `ExtractionRequest.IngestionJobID` exists and is persisted, so the bridge should populate an existing concept rather than invent a new relationship field.
  Refs: `internal/discovery/domain/entities/extraction_request.go:33-49`, `internal/discovery/adapters/postgres/extraction/extraction.go:17-24`, `FETCHER_MATCHER.md:164-166`

- Worker lifecycle patterns are already established. Existing workers use `Start`/`Stop`, `SafeGoWithContextAndComponent`, stop/done channels, tenant fanout, Redis `SetNX` locking, and bounded retry or backoff behavior.
  Refs: `internal/discovery/services/worker/discovery_worker.go:136-167`, `internal/discovery/services/worker/discovery_worker.go:460-519`, `internal/reporting/services/worker/export_worker.go:159-245`, `internal/reporting/services/worker/export_worker.go:1101-1123`, `internal/configuration/services/worker/scheduler_worker.go:171-208`

- Cross-context integration must reuse shared ports and adapters. Ingestion already consumes configuration and matching capabilities through `internal/shared/ports` and `internal/shared/adapters/cross`, not direct context imports.
  Refs: `internal/shared/adapters/cross/configuration_adapters.go:19-69`, `internal/shared/adapters/cross/auto_match_adapters.go:75-129`, `internal/bootstrap/init.go:2864-2923`

- Object storage is already stream-friendly and tenant-scoped. The shared storage port supports `Upload`, `Download`, `Delete`, and related operations, and the S3 adapter is already used against S3-compatible backends.
  Refs: `internal/shared/ports/object_storage.go:24-47`, `internal/reporting/adapters/storage/s3_client.go:170-301`, `internal/reporting/services/worker/export_worker.go:1174-1183`

- Governance provides the nearest existing integrity-verification pattern. Its archival worker already downloads stored data and verifies integrity before use, which is directly relevant to the bridge's trust model.
  Refs: `internal/governance/services/worker/archival_worker.go:845-902`

### Important codebase constraints

- There is currently no repository scan method for `COMPLETE AND IngestionJobID IS NULL`, which means a dedicated bridge worker would require repository expansion before it can poll for pending work.
  Ref: `internal/discovery/domain/repositories/extraction_repository.go:20-37`

- `resultPath` validation currently assumes a slash-prefixed path shape, which may become too restrictive depending on how Fetcher bucket keys are represented or normalized.
  Refs: `internal/discovery/adapters/fetcher/client_validation.go:75-95`, `internal/discovery/domain/entities/extraction_request.go:177-196`

- Object-storage bootstrap is currently reporting-driven, so a bridge worker likely needs bootstrap changes to initialize storage independently of reporting features.
  Ref: `internal/bootstrap/init.go:2756-2794`

- Audit integration is not yet standardized in discovery. If the bridge must produce append-only audit evidence for download, verification, and ingestion linkage, that path will need to be designed explicitly.
  Refs: `internal/configuration/adapters/audit/outbox_publisher.go:22-86`, `internal/exception/services/command/dispatch_commands.go:193-229`, `internal/matching/services/command/commands.go:46-89`

### Conflicting or surprising findings

- `SourceTypeFetcher` already exists in configuration, but matching's own source-type enum does not include `FETCHER`. The current cross adapter string-casts config source types into matching metadata and explicitly relies on matching treating them opaquely.
  Refs: `internal/configuration/domain/value_objects/source_type.go:18-35`, `internal/matching/ports/source_provider.go:13-26`, `internal/shared/adapters/cross/matching_adapters.go:269-276`

- The planning seed document assumes ingestion is HTTP-only, but the repo already contains a stream-based ingestion command core that takes an `io.Reader`. The real gap is cross-context access and orchestration, not the absence of programmatic ingestion logic.
  Refs: `internal/ingestion/services/command/commands.go:256-317`, `internal/ingestion/adapters/http/handlers.go:327-387`, `FETCHER_MATCHER.md:106-113`

- No existing Fetcher-to-ingestion bridge, RabbitMQ completion consumer, or object-storage consumer was found.

### Prior solutions search

- No `docs/solutions/` directory or equivalent prior-solution knowledge base was found in this repo.

## Best Practices Research

### Secure integration guidance

- Treat the bridge as an idempotent background process with explicit durable phases rather than as a best-effort callback. Durable status improves replay safety, incident recovery, and operator confidence.
  Sources:
  - https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-apis/
  - https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/

- Retry only transport and object-store failures. HMAC mismatches and AES-GCM authentication failures should be treated as permanent security failures, not transient errors.
  Sources:
  - https://pkg.go.dev/crypto/hmac
  - https://pkg.go.dev/crypto/cipher
  - https://csrc.nist.gov/pubs/sp/800/38/d/final

- Minimize plaintext retention. If Matcher must persist a decrypted copy, it should be tenant-scoped, auditable, and short-lived unless there is an explicit operational need for longer retention.
  Sources:
  - https://cheatsheetseries.owasp.org/cheatsheets/Cryptographic_Storage_Cheat_Sheet.html
  - https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html

- Tenant isolation must be enforced across the whole bridge: job pickup, object keying, crypto context, downstream ingestion context, logs, metrics, and retries.
  Sources:
  - https://docs.aws.amazon.com/wellarchitected/latest/saas-lens/tenant-isolation.html
  - https://cheatsheetseries.owasp.org/cheatsheets/Multi_Tenant_Security_Cheat_Sheet.html

### Key management guidance

- Strong external guidance favors envelope encryption with centralized key management rather than sharing long-lived raw symmetric keys across services. That said, the existing Fetcher contract already appears to depend on a shared master or derived-key model, so this is an architectural tension to resolve rather than an immediate implementation assumption.
  Sources:
  - https://docs.aws.amazon.com/kms/latest/developerguide/data-keys.html
  - https://developer.hashicorp.com/vault/docs/secrets/transit

### Open-source analogs

- HashiCorp Vault is useful for centralized cryptography and rotation patterns.
- Restic is useful for S3-compatible storage reliability and integrity-checking patterns.
- Grafana Loki is useful for multi-tenant object-storage and retention design patterns.

Refs:
- https://github.com/hashicorp/vault
- https://github.com/restic/restic
- https://github.com/grafana/loki

## Framework Documentation

### Detected stack and versions

- Go: `1.26.1`
- `github.com/LerianStudio/lib-auth/v2`: `v2.6.0`
- `github.com/LerianStudio/lib-commons/v4`: `v4.6.0-beta.7`
- `github.com/aws/aws-sdk-go-v2`: `v1.41.5`
- `github.com/aws/aws-sdk-go-v2/service/s3`: `v1.98.0`
- `github.com/aws/aws-sdk-go-v2/service/secretsmanager`: `v1.41.5`

Manifest ref: `go.mod:1-44`

### Relevant framework guidance

- Go's `io.Pipe` is synchronous and well-suited to streaming transforms across goroutines when errors are propagated with `CloseWithError`.
  Source: https://pkg.go.dev/io

- HMAC verification must use `hmac.Equal` to avoid timing leaks.
  Source: https://pkg.go.dev/crypto/hmac

- AES-GCM is the correct stdlib AEAD primitive for new work; failures from `Open` must be treated as authentication failure and the plaintext discarded.
  Sources:
  - https://pkg.go.dev/crypto/aes
  - https://pkg.go.dev/crypto/cipher

- AWS SDK for Go v2 supports S3-compatible endpoints using `BaseEndpoint` and `UsePathStyle`, which aligns with Matcher's existing SeaweedFS-compatible adapter.
  Source: AWS SDK for Go v2 developer guide

- Secrets should come from Secrets Manager and mutable operational knobs should remain in systemplane or bootstrap config according to whether they are hot-reload safe.
  Sources:
  - https://docs.aws.amazon.com/secretsmanager/latest/userguide/best-practices.html
  - official `lib-commons` docs for `commons/systemplane` and `commons/secretsmanager`

### Practical implication for this feature

- The repo already has the right foundational primitives for streaming storage and secret retrieval. The bridge does not need new external dependencies to be planned.

## Product and UX Research

### Problem validation

- Reconciliation operators and integration engineers currently experience a trust gap: a Fetcher extraction can become `COMPLETE`, yet the result never becomes ingestible or matchable data.
  Refs: `FETCHER_MATCHER.md:16-22`, `FETCHER_MATCHER.md:48-58`

- The API already exposes extraction status and optional ingestion linkage, which means users can reasonably expect truthful downstream completion semantics.
  Ref: `internal/discovery/adapters/http/dto/responses.go:74-133`

### Primary actors

- Reconciliation Operations Analyst: wants automated, reliable reconciliation inputs.
- Platform / Integration Engineer: wants retry-safe processing, tenant-safe operations, and rollout control.
- Risk / Audit stakeholder: wants verifiable lineage from extraction through ingestion.
- On-call engineer: wants failure categories that distinguish download, decrypt, integrity, and ingestion failures.

### Backend-only operator experience requirements

- Operators must be able to distinguish `extracted` from `ingested and match-ready`.
- Integrity failures must be explicit hard stops.
- The system should expose a correlatable chain: `extractionRequestId -> fetcherJobId -> stored artifact -> ingestionJobId`.
- Retry behavior must be bounded and safe to re-drive.
- Tenant-scoped backlog and failure visibility are required for safe rollout and support.

## Synthesis

### Key patterns to follow

- Use a dedicated worker-shaped bridge or equivalent durable handoff model, not an untracked inline side effect.
- Reuse the shared object-storage abstraction and shared cross-context adapter pattern already present in the repo.
- Reuse the existing ingestion command core that already accepts `io.Reader` input.
- Keep tenant derivation context-driven and include default-tenant handling in any background sweep.

### Constraints identified

- No direct cross-context imports are allowed; any discovery-to-ingestion bridge must go through `internal/shared/` abstractions.
- The discovery repository surface is currently too narrow for a standalone bridge worker and will need extension.
- Matching's `SourceTypeFetcher` semantics are not actually modeled downstream yet.
- Audit expectations for the bridge are not yet defined even though the business problem is trust-sensitive.

### What this means for Gate 1 and Gate 2

- The PRD should frame the user problem as a truthfulness and trust gap, not merely a transport enhancement.
- The TRD should treat the bridge as a bounded background integration pipeline with explicit security and status semantics.
- The plan should not assume a brand-new ingestion engine; it should focus on shared-port exposure, worker orchestration, result verification, and downstream linkage.

### Open questions to carry forward

- Should download be triggered inline from the poller or by a dedicated worker that scans completed unlinked extractions?
- What is the retention policy for Matcher's stored copy?
- How are HMAC and AES keys distributed to Matcher without widening the trust boundary more than necessary?
- What terminal and retry semantics should exist for download, HMAC mismatch, decryption failure, and ingestion failure?
- How should `SourceTypeFetcher` map into match-rule semantics once the data is ingested?
- Is rollout forward-only, or must existing completed extractions be backfilled?
