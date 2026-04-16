# Fetcher -> Matcher Data Ingestion Bridge

**Status:** Planning in progress. Audit complete; research complete; awaiting design decisions before implementation plan is authored.

**Last updated:** 2026-04-15
**Owner:** Fred Amaral (fred@lerian.studio)

---

## 1. Background

### 1.1 Why this document exists

During an API contract audit between Matcher and Fetcher (see `docs/audits/` for full audit artifacts when they're written), an architectural gap surfaced:

- `SourceTypeFetcher` is a valid source type declared in Matcher's configuration domain at `internal/configuration/domain/value_objects/source_type.go:29`.
- The discovery context can submit extraction jobs to Fetcher and poll them to completion.
- **No code in Matcher consumes the extraction results.** `ExtractionRequest.IngestionJobID` exists as a nullable field but is never populated. The `onComplete` callback on `PollUntilComplete` is nil at every call site. No worker reads Fetcher's object-storage bucket; no RabbitMQ consumer listens for Fetcher completion events; no cross-context bridge exists.

Result: a user can configure a reconciliation source with `type=FETCHER`, Matcher will happily run the extraction, and the data will go nowhere. Reconciliation only runs against file uploads today.

This document captures the design and planning for **closing that gap** — downloading extraction output from Fetcher's SeaweedFS-backed bucket, verifying its integrity, writing it to Matcher's own SeaweedFS instance, and handing it into the ingestion pipeline so reconciliation can match against it.

### 1.2 What this document is not

- Not an implementation plan with file paths and code examples. That deliverable comes after design questions are resolved and will be produced via `ring-pm-team:pre-dev-full` (or the lighter `pre-dev-feature` if scoped down).
- Not a user-facing spec. The audience is the engineering team plus future AI agents picking up the work.
- Not a final decision record. Several sections are explicitly marked PENDING.

---

## 2. Current State

### 2.1 What exists today

| Capability | Status | Location |
|-----------|--------|----------|
| Submit extraction jobs to Fetcher (`POST /v1/fetcher`) | Working | `internal/discovery/adapters/fetcher/client.go` (SubmitExtractionJob) |
| Poll extraction status (`GET /v1/fetcher/{id}`) | Working | `internal/discovery/adapters/fetcher/client.go` (GetExtractionJobStatus) |
| Poll loop with state machine transitions | Working | `internal/discovery/services/worker/extraction_poller.go` |
| Persist `resultPath`, `resultHmac`, `requestHash` on COMPLETE | Working | `internal/discovery/domain/entities/extraction_request.go` |
| `ExtractionRequest.IngestionJobID` field | Declared, never populated | `extraction_request.go:38` ("Nullable: linked to downstream ingestion when available") |
| `SourceTypeFetcher` enum value | Declared, no consumer in matching | `internal/configuration/domain/value_objects/source_type.go:29` |
| OAuth2 M2M auth to Fetcher | Working | `internal/discovery/adapters/fetcher/client_auth.go`, `internal/discovery/adapters/m2m/token_exchanger.go` |
| Schema + connection metadata sync | Working | `internal/discovery/services/command/discovery_commands.go` |
| Echo-field divergence logging (mappedFields / filters) | Working (recently added) | `internal/discovery/services/command/extraction_commands.go` |

### 2.2 What's missing

| Capability | Why it matters |
|-----------|----------------|
| Download from Fetcher's S3 | Fetcher holds the encrypted bytes; nobody in Matcher reads them |
| AES-GCM decryption | Fetcher encrypts at rest; plaintext must be recovered before parsing |
| HMAC-SHA256 verification | Integrity contract Fetcher ships; missing verification means trust-on-read |
| Storage in Matcher's SeaweedFS | Matcher should own a copy of the data that drives its reconciliations |
| Programmatic ingestion entry point | Ingestion today is HTTP-multipart only; no internal API to feed it a stream |
| Extraction -> ingestion link | `IngestionJobID` field hook exists but nothing writes to it |
| Source-type consumer in matching | `SourceTypeFetcher` is an orphan enum — configured, never matches |

---

## 3. Research Findings

Seven topics were investigated. Citations use `path:line` format against the two repos: Matcher at `/Users/fredamaral/repos/lerianstudio/matcher` and Fetcher at `/Users/fredamaral/repos/lerianstudio/fetcher`.

### R1 — Fetcher's extraction output format

- **Format:** Single JSON file per job. Not sharded, not multi-part.
- **Shape:** `map[string]map[string][]map[string]any` — datasource -> table -> rows. Each row is a flat `map[string]any` of column->value. Reference: `fetcher/components/worker/.../extract_data.go:126`.
- **Encoding:** UTF-8, pretty-printed via `json.MarshalIndent` with 2-space indent. `extract_data.go:192`.
- **Compression:** None at the JSON layer. Compression happens (if at all) below via SeaweedFS.
- **Encryption:** AES-GCM applied to the plaintext JSON before storage. The encrypted bytes are what lives in the bucket. `extract_data.go:214-219`.
- **Size metrics:** `sizeBytes` and `rowCount` are computed at completion and returned in `JobResultData`. `extract_data.go:199-200`.
- **Size limits:** No explicit documented ceiling in Fetcher. Bounded only by storage and memory.

### R2 — Fetcher's S3/object-storage conventions

- **Object key pattern:** `{tenant-prefix}/external-data/{job-id}.json`. Resolved via `tms3.GetS3KeyStorageContext`. `extract_data.go:195, 220-223`.
- **API conveyed key:** `JobResponse.ResultPath` contains the full tenant-prefixed S3 key, not a presigned URL or an HTTP path. `fetcher/pkg/model/job.go:322`.
- **Bucket:** Not exposed in worker config; set at Manager deployment time.
- **Access model:** Consumers are expected to hit the bucket directly with shared credentials. There is **no HTTP download endpoint** on Fetcher's Manager API for the result file. `resultPath` plus the bucket name plus shared creds == download URL.
- **Multi-tenancy:** Per-tenant prefix is enforced in the object key.
- **Lifecycle:** No retention policy in Fetcher's worker. Bucket-level lifecycle policies are the only cleanup mechanism unless consumers delete.

### R3 — Fetcher's HMAC + encryption contract

- **Algorithm:** HMAC-SHA256. Reference: `fetcher/scripts/crypto/derive-key/verification-guide.md:42`.
- **Key derivation:** HKDF-SHA256 with salt=nil, info (context) = `"fetcher-external-hmac-v1"`. Output length 32 bytes. Reference: `fetcher/scripts/crypto/derive-key/key_deriver.go:25, 33, 106-116`.
- **Master key source:** Single shared `APP_ENC_KEY` (base64-encoded, >= 32 bytes). All three derived keys (credentials, internal HMAC, external HMAC) come from the same master, differentiated by HKDF context string. `key_deriver.go:64-100`.
- **Signed payload format:** `<unix-timestamp>.<plaintext-body>` — JWT-style pre-signature concatenation. Reference: `verification-guide.md:44`.
- **What Matcher would verify:** Compute HMAC of `<timestamp>.<decrypted-json>`; compare (constant-time) against `resultHmac` from the API response.
- **Conveyance:** HMAC is in the API response only (`JobResponse.ResultHmac`), not in a sidecar file or object metadata. `job.go:323`.
- **Rotation:** No documented protocol. Changing `APP_ENC_KEY` breaks verification for all prior documents.
- **AES-GCM key:** Presumably derived from the same master with a distinct context string; the research agent did not fully map this but the pattern is clear from `key_deriver.go`. CONFIRM: which HKDF context string is the AES key derived from? (See D4 / D9 below.)

### R4 — Matcher's object-storage adapter

- **Port interface:** `internal/shared/ports/object_storage.go:24-47` — `ObjectStorageClient` with `Upload`, `UploadWithOptions`, `Download`, `Delete`, `GeneratePresignedURL`, `Exists`.
- **Implementation:** `internal/reporting/adapters/storage/s3_client.go:51-418`. S3-compatible; verified working against AWS S3, MinIO, and SeaweedFS.
- **Streaming:** Yes. Upload/Download accept `io.Reader` / `io.ReadCloser`. No buffering required for large files.
- **Config env vars:** `OBJECT_STORAGE_ENDPOINT`, `OBJECT_STORAGE_REGION`, `OBJECT_STORAGE_BUCKET`, `OBJECT_STORAGE_ACCESS_KEY_ID`, `OBJECT_STORAGE_SECRET_ACCESS_KEY`, `OBJECT_STORAGE_USE_PATH_STYLE`, `OBJECT_STORAGE_ALLOW_INSECURE_ENDPOINT`. `s3_client.go:27-37`.
- **Defaults:** `Endpoint=http://localhost:8333` (SeaweedFS local), `Region=us-east-1`, `Bucket=matcher-exports`, `UsePathStyle=true`.
- **Tenant isolation:** Keys are scoped via `libS3.GetObjectStorageKey(tenantID, originalKey)`. Reference: `reporting/.../export_worker.go:1182`.
- **Existing usage:** Reporting exports (match / unmatched / variance reports). Governance archives. New Fetcher path will share the same bucket unless separated via config (recommended — see D2).

### R5 — Matcher's ingestion entry points

- **Supported formats:** CSV, JSON, XML (including ISO 20022 `camt.053`).
- **Parser registry:** `internal/ingestion/adapters/parsers/registry.go:35-48` — `GetParser(format)` returns a parser that consumes `io.Reader`.
- **HTTP-only today:** `internal/ingestion/adapters/http/handlers.go:34-104` is the only entry path. Multipart upload, 100MB cap.
- **No programmatic ingestion API:** No use case or worker today that ingests a stream obtained outside the HTTP handler. **This is the gap Fetcher-bridge planning must design around.**
- **Pipeline stages:** HTTP upload -> parser selection -> streaming parse into normalized transactions -> Redis-based dedup -> outbox/RabbitMQ event publish -> optional auto-match trigger.

### R6 — Extraction entity state machine

- **States:** `PENDING` -> `SUBMITTED` -> `EXTRACTING` -> `COMPLETE` | `FAILED` | `CANCELLED`. Reference: `internal/discovery/domain/value_objects/extraction_status.go:12-30`. Terminal states at lines 48-50.
- **Transition methods** (on `ExtractionRequest` in `extraction_request.go`):
  - `MarkSubmitted(fetcherJobID)` — records Fetcher job ID. Line 96-116.
  - `MarkExtracting()` — idempotent. Line 120-144.
  - `MarkComplete(resultPath)` — validates `resultPath` format. Line 148-175.
  - `MarkFailed(errMsg)` — any non-terminal -> FAILED. Line 201-220.
  - `MarkCancelled()` — any non-terminal -> CANCELLED. Line 224-239.
- **Persisted fields:** `FetcherJobID`, `ResultPath`, `ErrorMessage`, `IngestionJobID`, timestamps.
- **No outbox/events on state change:** Transitions are synchronous method calls. A consumer that wants to react to COMPLETE must poll the entity; no fan-out mechanism exists. **This is relevant to D1 below — the download-trigger model.**

### R7 — Matcher's worker patterns

Two templates to follow:

- **`internal/configuration/services/worker/scheduler_worker.go`:** Redis SetNX distributed locking (lock key prefix `matcher:scheduler:schedule:`, TTL = `max(2*interval, 5s)`); lifecycle via `Start()` launching `SafeGoWithContextAndComponent` goroutine and `Stop()` signaling via `stopCh` + `doneCh`; interval-based ticker (default 1 minute); observability via `otel.Tracer("configuration.scheduler_worker")`.
- **`internal/reporting/services/worker/export_worker.go`:** Similar lifecycle; configurable interval (default 5s); tenant enumeration via `sharedPorts.TenantLister.ListTenants()` with single-tenant fallback; exponential backoff on retry (1s initial, 5m max, 2.0 multiplier).

**Tenant enumeration gotcha:** The default tenant uses the `public` schema and won't appear in `pg_namespace` UUID-schema enumeration. `TenantLister.ListTenants()` must include the default tenant manually. This is noted in the project memory under "Key Architecture Insights."

---

## 4. Key Surprises

Three findings from the research reshape the design:

### Surprise 1 — Fetcher stores encrypted at rest

Fetcher does not store the plaintext JSON. It:

1. Runs the extraction to produce plaintext JSON.
2. Computes HMAC-SHA256 over `<unix-timestamp>.<plaintext-body>`.
3. AES-GCM encrypts the plaintext.
4. Writes the encrypted bytes to S3.
5. Returns `resultPath` (S3 key) + `resultHmac` (hex of plaintext HMAC) via API.

**Implication:** Matcher cannot treat `resultPath` as "download and parse." It must:

- Download encrypted bytes.
- Decrypt via AES-GCM with a key derived from the shared master.
- Compute HMAC over `<timestamp>.<plaintext>` and compare to `resultHmac` (constant-time).
- Only then feed plaintext into the parser.

This adds a crypto dependency Matcher doesn't have today in the Fetcher path. Relevant design question: **D4** (key distribution) and **D9** (encryption handling).

### Surprise 2 — Single JSON file per job, nested shape

The output is not CSV, not Parquet, not multi-file. It's **one JSON document** with nested shape `datasource -> table -> rows[]`. Matcher's existing JSON parser in ingestion expects a flat list of transactions per source. A **shape transform** is needed: either flatten at the adapter boundary before handing to the parser, or add a nested-JSON parser variant to the registry.

### Surprise 3 — `ExtractionRequest.IngestionJobID` is a hook waiting for us

The field exists. The comment reads "Nullable: linked to downstream ingestion when available." Whoever wrote this left a hook for the feature to plug into. We're not designing a new field; we're populating an existing one. This simplifies the state-machine extension — the entity already admits an ingestion linkage.

---

## 5. Proposed Architecture (Sketch)

This is a sketch, not a committed plan. The write-plan phase will detail file paths, interfaces, and code. Read this as "shape of the solution space."

```
[Fetcher]                              [Matcher]
    |                                       |
    | POST /v1/fetcher                      |
    |<--------------------------------------|
    | 202 Accepted {jobId}                  |
    |-------------------------------------->|
    |                                       | state: SUBMITTED
    |                                       |
    | GET /v1/fetcher/{jobId} (polled)      |
    |<--------------------------------------|
    | {status: COMPLETE, resultPath, ...}   |
    |-------------------------------------->|
    |                                       | state: COMPLETE
    |                                       |   |
    |                                       |   v
    |                                       | ExtractionDownloadWorker (new)
    |                                       |   |
    | GET s3://fetcher-bucket/{tenant}/     |   |
    |     external-data/{jobId}.json        |<--|
    |<--------------------------------------|
    | encrypted bytes                       |   |
    |-------------------------------------->|   v
    |                                       | AES-GCM decrypt
    |                                       |   |
    |                                       |   v
    |                                       | HMAC-SHA256 verify
    |                                       |   |
    |                                       |   v
    |                                       | Write plaintext to Matcher SeaweedFS
    |                                       | s3://matcher-bucket/{tenant}/
    |                                       |     fetcher-extractions/{jobId}.json
    |                                       |   |
    |                                       |   v
    |                                       | Call ingestion.IngestFromStream
    |                                       | (new programmatic entry point)
    |                                       |   |
    |                                       |   v
    |                                       | ExtractionRequest.IngestionJobID = <id>
    |                                       | state: COMPLETE (with IngestionJobID set)
    |                                       |   |
    |                                       |   v
    |                                       | Normal ingestion pipeline:
    |                                       |   parse -> dedup -> outbox -> match
```

### 5.1 Components (to detail in write-plan)

- **`ExtractionDownloadWorker`** (new): Polls for `ExtractionRequest` rows in state `COMPLETE` with `IngestionJobID IS NULL`. Acquires Redis lock per extraction ID. Downloads, decrypts, verifies, re-stores, triggers ingestion.
- **Fetcher storage client** (new): Either extend the existing `FetcherClient` port, or introduce a new `FetcherStorageClient` port specifically for S3 access with Fetcher's bucket configuration.
- **Crypto adapter** (new): HKDF key derivation + HMAC verification + AES-GCM decryption. Isolated in a small package to keep the blast radius of key material tight.
- **Ingestion programmatic API** (new): A use case like `IngestFromStream(ctx, sourceID, stream io.Reader, format string, metadata)` invoked by the download worker. Reuses the existing parser registry.
- **Nested-JSON shape transform** (new): Flattens `datasource -> table -> rows` into the flat-row shape the existing parser expects. Likely a new parser variant rather than modifying the generic JSON parser.
- **State-machine extension** (in existing `ExtractionRequest`): Either add a new state (`INGESTED`) or rely on `IngestionJobID` population as the terminal flag for "ready for matching." Choice depends on whether partial-ingestion states are needed (see D5).
- **Source-type consumer in matching** (in matching context): Teach the matching pipeline that `SourceTypeFetcher` sources pull their transactions from the linked ingestion job.

---

## 6. Resolved Decisions

These items are already decided and should not be relitigated unless new evidence changes the calculus.

| Decision | Rationale |
|----------|-----------|
| Matcher downloads to its own SeaweedFS (not read-through) | User directive; decouples Matcher's availability from Fetcher's bucket retention |
| HMAC verification is mandatory | Integrity is a non-negotiable contract when trusting external data into reconciliation |
| Reuse existing object-storage adapter (`ObjectStorageClient`) | Already supports SeaweedFS, streaming, tenant-scoped keys |
| Reuse existing parser registry for JSON | Avoid parallel parser stacks; add nested-shape handling as a variant |
| New worker follows `scheduler_worker.go` / `export_worker.go` pattern | Redis SetNX locking, TenantLister, `SafeGoWithContextAndComponent`, span-based observability |
| Tenant enumeration must include default tenant | Known project gotcha; code that forgets this silently skips default-tenant data |

---

## 7. Design Questions (PENDING USER INPUT)

Research has pre-answered some framing. Items in **bold** need user judgment. Italics are research-informed defaults awaiting confirmation.

### D1 — Download trigger model [BOLD]

**Question:** What triggers the download?

- (a) Inline: the existing extraction poller, on detecting COMPLETE, immediately enqueues/invokes the download.
- (b) Separate worker: a new background worker periodically scans for `COMPLETE AND IngestionJobID IS NULL` extractions.
- (c) Explicit: a user/API action materializes a completed extraction on demand.

Research note: the current state machine has no events/outbox on transitions, so (a) requires adding an in-memory callback (already supported via the nil `onComplete` hook) or an async dispatch. (b) is the cleanest decoupling and matches existing worker patterns. (c) is the most controlled but adds user ceremony.

**Awaiting:** choice.

### D2 — Retention policy [BOLD]

**Question:** After successful ingestion, what happens to the plaintext copy in Matcher's SeaweedFS?

- (a) Delete immediately after successful ingestion (minimizes sensitive-data footprint).
- (b) TTL-based expiry (e.g., 7 / 30 / 90 days) for debugging and re-ingestion.
- (c) Keep indefinitely; admins prune manually.

Research note: Fetcher has no documented retention; Matcher is effectively the long-term store if Fetcher's bucket has aggressive lifecycle policies.

**Awaiting:** choice, plus TTL value if (b).

### D3 — Ingestion programmatic entry point [CONFIRM]

*Research-informed framing:* Ingestion today is HTTP-multipart only. The Fetcher bridge requires a new programmatic use case in the ingestion context (e.g., `IngestFromStream`) that takes a reader plus source metadata and feeds the existing parser pipeline. The new worker invokes this use case directly (cross-context call via shared port, not HTTP loopback).

**Awaiting:** confirmation of this direction, OR an alternative (e.g., reverse direction via HTTP loopback, or extraction-dedicated parser not sharing the ingestion pipeline).

### D4 — HMAC + AES key distribution [BOLD]

**Question:** How does Matcher obtain the keys?

- (a) Matcher gets the raw master `APP_ENC_KEY` via env var / systemplane / secrets manager, derives both HMAC and AES keys in-process via HKDF with the documented context strings.
- (b) Fetcher exposes a key-material endpoint (authenticated) that returns the derived external HMAC + AES keys.
- (c) Operators distribute the pre-derived keys via secrets manager only (no master-key exposure to Matcher).

Options differ in who holds the master secret. (a) is simplest, matches what `make derive-key` in Fetcher does, but puts master key bytes inside Matcher's trust boundary. (b) needs a new Fetcher API and PKI. (c) splits the operational responsibility between ops and two services.

**Awaiting:** choice, plus config-key naming (e.g., `FETCHER_EXT_HMAC_KEY` vs `APP_ENC_KEY`).

### D5 — Failure semantics [BOLD]

**Question:** How does each failure mode behave?

Granular sub-questions:

- **Download fails (transient):** retry N times with exponential backoff, then what? Mark `ExtractionRequest` FAILED? Keep COMPLETE but signal a separate "download-failed" state? Park for manual retry?
- **HMAC mismatch:** hard fail (FAILED terminal)? DLQ? Alert-only-continue-anyway (never)?
- **Decryption fails:** same handling as HMAC mismatch, or different?
- **Ingestion fails after successful download + decrypt:** roll back? Mark ingestion job FAILED and leave extraction COMPLETE? Flag for re-ingestion without re-download?

Research note: `ExtractionRequest` state machine has only one failure terminal (`FAILED`) and no sub-cause field beyond `ErrorMessage`. Adding a new state or a structured failure category may be warranted.

**Awaiting:** per-mode decisions.

### D6 — `SourceTypeFetcher` semantics in match rules [CONFIRM]

*Research-informed framing:* The entity `ExtractionRequest` has `IngestionJobID` as the linkage hook. The natural semantic: a source with `type=FETCHER` has its transactions produced by the linked ingestion job, which was produced by the linked Fetcher extraction. Match rules reference fields by name, and since Fetcher's output is `datasource -> table -> rows`, field names in rules should be qualified (e.g., `{datasource}.{table}.{column}`) or the transform layer should flatten into a per-table source.

**Awaiting:** confirmation, OR a different mapping strategy (e.g., one source per `<datasource, table>` pair, vs one source for the whole extraction).

### D7 — Cut-over and backfill [BOLD]

**Question:** Are there existing `COMPLETE` extractions in production that need to be processed by the new pipeline once it ships, or is this feature strictly forward-only?

**Awaiting:** choice, plus backfill strategy if applicable (one-shot migration? rolling worker?).

### D8 — Size bounds and streaming [CONFIRM]

*Research-informed framing:* Fetcher has no documented size limit. Matcher's existing ingestion HTTP handler caps multipart at 100MB. The download worker should stream end-to-end (download, decrypt, parse) rather than buffering. The AES-GCM block construction in Fetcher may or may not allow streaming decryption without full buffering; this needs a small spike during write-plan.

**Awaiting:** maximum acceptable extraction size (hard fail above?), and whether streaming vs buffered is a hard requirement.

### D9 — Encryption handling strategy [BOLD]

**Question:** What does Matcher store in its own SeaweedFS?

- (a) Decrypt in-process only; store plaintext in Matcher's bucket. Faster subsequent access, duplicates sensitive data across two services' storage.
- (b) Store encrypted bytes verbatim; decrypt every time on read. Matches Fetcher's posture, requires Matcher to hold decryption key for every read.
- (c) Re-encrypt with a Matcher-owned key; store under Matcher's encryption scheme. Isolates key material from Fetcher's key lifecycle; adds key-management burden.

**Awaiting:** choice. My weak prior is (a) if retention is short (D2 = delete-after-ingest) or (c) if retention is long (D2 = keep indefinitely).

---

## 8. Planning Workflow Preference [BOLD]

**Question:** Which planning skill should produce the implementation plan once design questions are answered?

- `ring-pm-team:pre-dev-feature` — Lightweight 5-gate pre-dev for <2-day features. Probably too small given the cross-context + crypto surface.
- `ring-pm-team:pre-dev-full` — 10-gate full workflow for >=2-day features. Produces PRD, TRD, API design, data model, dependency map, task breakdown, subtasks, delivery plan. My recommendation given the scope.
- `ring:write-plan` — One-shot implementation plan; no gated validation. Faster but skips PRD/TRD rigor.

**Awaiting:** choice.

---

## 9. Open Items Not Yet Discussed

These are things the research surfaced but we haven't yet framed as questions. Flag if you want them folded into D1-D9:

- **Credentials for Fetcher's bucket:** Matcher needs access to Fetcher's SeaweedFS for download. IAM / shared access keys / presigned URLs? Related to D4.
- **Observability:** Metrics on download duration, HMAC-mismatch rate, decryption failures. Alerting thresholds. Dashboards.
- **Idempotency on download:** If the worker crashes mid-download, what state transitions are safe to replay?
- **Concurrent download limits:** Bound on concurrent in-flight downloads per tenant to prevent thundering-herd on Fetcher's bucket.
- **Audit trail:** Does the `governance` context need to record the download + verification + ingestion event chain? Audit logs are append-only and hash-chained per project memory.
- **Multi-tenant isolation across the bridge:** Extraction job under tenant A; worker must not download into tenant B's bucket path. Existing tenant-scoping covers this but should be explicit in the plan.
- **Rollout strategy:** Feature flag? Canary on one tenant first? What does "feature enabled" mean operationally?

---

## 10. Next Steps

1. User answers D1-D9 (and D-planning in section 8).
2. Open items in section 9 are either folded in or explicitly deferred.
3. I dispatch the chosen planning skill with this document + research artifacts as input.
4. Planning skill produces the implementation plan (PRD, TRD, tasks, code-level detail).
5. Implementation executes via `ring-dev-team:dev-cycle` with gated validation.
6. Audit fixes from this session (SSL shape, typed filters, echo fields) are committed atomically on `develop` once user approves — separate from this planning work.

---

## Appendix A — Audit Fix Summary (for context)

This planning work is a follow-up to a contract audit between Matcher and Fetcher. The audit surfaced 12 findings; 3 were real and fixed, 9 were false positives (mostly because commit `6cffce2` — the OAuth2 rewrite — had already preempted the issues).

**Real findings fixed in this session:**

| ID | Severity | Fix |
|----|----------|-----|
| CRITICAL-1 | CRITICAL | SSL field shape: `bool` -> `*fetcherSSLResponse` + `SSLEnabled` / `SSLMode` on domain type |
| MEDIUM-2 | MEDIUM | Typed `fetcherFilterCondition` struct on both wire sides + converters |
| MEDIUM-3 | MEDIUM | Parse JobResponse echo fields (mappedFields, filters) + divergence logging |

**False positives closed:**

| ID | Why |
|----|-----|
| HIGH-1 (pagination) | Already implemented in `6cffce2` with pagination loop + 4 tests |
| HIGH-2 (HMAC verification) | Architectural: Matcher doesn't consume extraction data today; nothing to verify |
| HIGH-3 (200/202) | Already implemented in `6cffce2` with status-code surfacing + dedup logging |
| MEDIUM-1 (error body parsing) | Already implemented in `6cffce2` via `client_errors.go` |
| MEDIUM-4 (status trim) | Already trimmed in `6cffce2` to the 4 statuses Fetcher actually emits |
| LOW-1 (latencyMs widening) | Safe int -> int64 widening; no issue |
| LOW-2 (time as string) | Intentional adapter-boundary parsing via `parseOptionalRFC3339` |
| LOW-3 (lexical drift) | Doc comment already present near X-Product-Name header |
| LOW-4 (ApplicationName case) | Constant is already lowercase (`"matcher"`) |

All fixes currently uncommitted on `develop`.

---

*End of document.*
