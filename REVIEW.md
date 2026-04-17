# Code Review Report — Fetcher Bridge Feature (T-001..T-006)

**Review Date:** 2026-04-17
**Branch:** `develop`
**Base:** `74a6a6fe` (chore(release): 1.3.0-beta.15)
**Head:** `a1fa8987` (chore(mocks): regenerate ingestion job repository mock)
**Commits Reviewed:** 9
**Files Changed:** 194
**Lines:** +35,631 / -2,422
**Reviewers:** 65 specialized agents across 8 thematic slices (8 reviewer angles: code, business-logic, security, test, nil-safety, consequences, dead-code, performance)
**Orchestration:** Ring codereview skill, 2 waves + addenda, ~10 hours wall-clock
**Final Verdict:** **NEEDS_FIXES** — 14 blocking items (3 CRITICAL, 6 HIGH, 5 MEDIUM), ~3.5 hours of mechanical work, 4 coordination decisions

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Reviewer Verdict Matrix](#2-reviewer-verdict-matrix)
3. [Cross-Cutting Concerns](#3-cross-cutting-concerns)
4. [Critical Findings](#4-critical-findings)
5. [High-Severity Findings](#5-high-severity-findings)
6. [Medium-Severity Findings](#6-medium-severity-findings)
7. [Low-Severity Findings](#7-low-severity-findings)
8. [Prioritized Remediation Plan](#8-prioritized-remediation-plan)
9. [What Was Done Well](#9-what-was-done-well)
10. [Review Methodology & Insights](#10-review-methodology--insights)
11. [Handoff Artifacts](#11-handoff-artifacts)

---

## 1. Executive Summary

The Fetcher Bridge feature delivers a cohesive 6-task vertical (T-001 trusted-stream intake → T-002 verified retrieval + custody → T-003 bridging worker → T-004 readiness projection → T-005 retry/staleness → T-006 retention sweep) plus infrastructure wiring. The architecture is **fundamentally sound**:

- **Atomic state-machine** with `LinkExtractionToIngestion` (T-001 Gate 8 precondition satisfied — triangulated across 5 reviewers)
- **Two-state-machine discipline** cleanly separates upstream `Status` (discovery) from bridge state (`BridgeLastError`, `BridgeAttempts`)
- **Defense-in-depth crypto** with HKDF-locked context strings, constant-time `hmac.Equal`, SSRF guards on artifact transport, bootstrap hard-fail on missing `APP_ENC_KEY`
- **Passive backoff** via `updated_at ASC` DB reordering — eliminates dual-clock bug class
- **Three-layer orphan prevention**: SQL `status='COMPLETED'` filter + atomic INSERT stamp (Polish Fix 4) + canonical UUID lowercasing (Polish Fix 7)
- **Convergence marker design** for retention sweep: provably bounded O(orphans-per-cycle)

The review identifies **14 blocking items** before merge to `main`:

- **3 CRITICAL** — ~25 minutes total fix (format-verb typo, 429 misclassification, uncapped replay read)
- **6 HIGH** — mostly mechanical (file splits, replica routing, partition invariant tightening, reconciler hydration gap)
- **5 MEDIUM** — include 2 architectural decisions and 3 small code fixes

**Plus 4 coordination items** requiring team alignment:
- **C3**: HMAC contract with Fetcher team (blocks real-Fetcher integration)
- **C7**: Custody write-once posture decision
- **C15**: Worker-liveness signal / "truthful readiness" product decision
- **C22**: Immutable-vs-idempotent failure message contract

Total estimated mechanical effort to ship: **~3.5 hours**.

---

## 2. Reviewer Verdict Matrix

| Slice | Files | Code | Biz | Sec | Test | Nil | Cons | Dead | Perf | Outcome |
|-------|:-----:|:----:|:---:|:---:|:----:|:---:|:----:|:----:|:----:|:--------|
| 1. fetcher-transport-client | 29 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | **PASS** |
| 2. t001-trusted-stream-intake | 29 | ✅ | ✅ | ✅ | ⚠️ | ✅ | ✅ | ✅ | ✅ | **PASS (caveats)** |
| 3. t002-verified-retrieval-custody | 15 | ✅ | ⚠️ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | **NEEDS_DISCUSSION** |
| 4. t003-bridge-worker-state-machine | 40 | ❌ | ⚠️ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | **NEEDS_FIXES** |
| 5. t004-readiness-projection | 17 | ✅ | ⚠️ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | **NEEDS_FIXES** |
| 6. t005-retry-staleness-failure | 11 | ✅ | ⚠️ | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ | **NEEDS_DISCUSSION** |
| 7. t006-retention-custody-sweep | 4 | ❌ | ⚠️ | ✅ | ⚠️ | ✅ | ✅ | ✅ | ✅ | **NEEDS_FIXES** |
| 8. infrastructure-wiring-migrations | 36 | ⚠️ | ⚠️ | ✅ | ✅ | ✅ | ⚠️ | ✅ | ✅ | **NEEDS_DISCUSSION** |

**Aggregate:** 47 ✅ · 12 ⚠️ · 4 ❌ · 2 absorbed

**Legend:** ✅ PASS · ⚠️ PASS with caveats / NEEDS_DISCUSSION · ❌ FAIL

---

## 3. Cross-Cutting Concerns

Issues that span multiple slices or were identified by multiple reviewer angles. These are structurally validated (convergence = confidence in the finding's reality).

| ID | Finding | Slices Affected | Triangulation | Sev |
|----|---------|:---------------:|:-------------:|:---:|
| C1 | `%worker` format-verb typo at 6 sites | t003, t005, t006 | **10 angles** | 🔴 CRIT |
| C2 | HTTP 429 misclassified as terminal | t005 | 1 (perf) | 🔴 CRIT |
| C3 | HMAC contract divergence (P6 precondition) | t002 | 1 (biz) | 🔴 CRIT |
| C4 | Readiness queries use primary DB | t004 | 1 (perf) | 🟠 HIGH |
| C5 | P1 streaming plaintext gap | t002 | 3 (code/biz/perf) | 🟠 HIGH |
| C6 | `recoverDigest` uncapped download | t002 | 1 (perf) | 🟠 HIGH |
| C7 | Custody write-once TOCTOU | t002 | 4 (code/biz/sec/test) | 🟠 HIGH |
| C8 | 4 files > 500 lines (Ring BLOCKING) | t003 | 1 (code) | 🟠 HIGH |
| C14 | Ready/Failed partition overlap | t004 | 1 (biz) | 🟠 HIGH |
| C15 | No worker-liveness signal | t004 | 1 (biz) | 🟠 HIGH |
| C17 | `BridgeRetryMaxAttempts` missing from reconciler | infra | **3** (code/dead/cons) | 🟠 HIGH |
| C21 | `executeMarkBridgeFailed` missing NULL guard | t005 | 1 (biz) | 🟠 HIGH |
| C30 | `initFetcherBridgeWorker` asymmetric soft-disable | infra | 1 (biz) | 🟠 HIGH |
| C9 | Redundant `FindByID` in LinkExtractionToIngestion | t003 | 1 (perf) | 🟡 MED |
| C10 | Eligibility partial index predicate drift | t003, t005 | 3 (perf/biz/cons) | 🟡 MED |
| C11 | Drilldown hydrates unused JSONB | t004 | 1 (perf) | 🟡 MED |
| C12 | Missing fuzz for security parsers | t001 | 1 (test) | 🟡 MED |
| C13 | `sweepCycle()` + default-tenant untested | t006 | 1 (test) | 🟡 MED |
| C19 | HTTP DTO omits bridge/custody fields | t003/t005 | 2 (cons) | 🟡 MED |
| C20 | CustodyRetentionWorker.Start(nil) misleading sentinel | t006 | 1 (code) | 🟡 MED |
| C22 | Same-class MarkBridgeFailed message refresh | t005 | 1 (biz) | 🟡 MED |
| C23 | `EnsureBridgeOperational` missing `ArtifactCustody` | infra | 1 (nil) | 🟡 MED |
| C25 | Config accessors in `config_env.go` lack nil guards | infra | 1 (nil) | 🟡 MED |
| C26 | Extraction repo constructed twice in bootstrap | t003 | 1 (cons) | 🟡 MED |
| C27 | No E2E journey for `/bridge/summary` + `/bridge/candidates` | t003 | 1 (cons) | 🟡 MED |
| C31 | Migrations 000026/000027 use plain `CREATE INDEX` (no CONCURRENTLY) | infra | 1 (biz) | 🟡 MED |
| C18 | `FetcherBridgeAdapters.ArtifactRetrieval/.ArtifactVerifier` unread | infra | 1 (dead) | 🟢 LOW |
| C24 | FETCHER_MATCHER.md stale vs shipped code | infra | 2 (code/cons) | 🟢 LOW |
| C28 | Test stubs inconsistent: `*sql.Tx` vs `sharedPorts.Tx` | t003 | 1 (cons) | 🟢 LOW |
| C29 | `custody_retention_worker.go` imports `shared/adapters/custody` directly | t003 | 1 (cons) | 🟢 LOW |

---

## 4. Critical Findings

Severity calibration: **Security vulnerability, data loss/corruption risk, or contract-violating behavior that silently corrupts production state**. Must fix before any merge.

### C1 — `%worker` Format-Verb Typo Corrupts Lock-Acquire Error Messages

**Severity:** 🔴 CRITICAL
**Triangulation:** 10 reviewer angles across 3 slices (t003, t005, t006)
**Reviewers flagging:** code, business-logic, security, nil-safety, consequences, dead-code, performance (multiple slices)

**Location:**
- `internal/discovery/services/worker/bridge_worker.go:613, 624, 631`
- `internal/discovery/services/worker/custody_retention_worker.go:537, 548, 555`

**Problem:**

Six `fmt.Errorf` calls use the literal `%worker` verb:

```go
return false, "", fmt.Errorf("get redis connection: %worker", err)
return false, "", fmt.Errorf("get redis client: %worker", err)
return false, "", fmt.Errorf("redis setnx for ...: %worker", err)
```

Go's `fmt` package parses `%w` as the error-wrap verb and treats the trailing `orker` as literal text. Error chain wrapping via `errors.Is` still works for non-nil errors, but:

1. **Log output is corrupted.** A real Redis timeout produces `"get redis connection: connection refused: dial tcp 127.0.0.1:6379: connect: timeoutorker"`. Operators grepping for clean error messages will see the trailing `orker` on every occurrence.
2. **`errors.Is` breaks on nil errors.** When `err == nil`, `%worker` renders as `%!w(error)(<nil>)orker` — the error chain is broken at the nil sentinel boundary.
3. **Distributed tracing attributes** inherit the garbled string via `libOpentelemetry.HandleSpanError`, polluting observability dashboards.

**Root cause:** Likely a find/replace refactor that renamed a receiver variable `w` to `worker` without respecting `%w` format-verb boundaries. The same pattern exists in both `bridge_worker.go` (introduced in T-003) and `custody_retention_worker.go` (T-006 copied it from the bridge worker pattern).

**Impact:** Operational observability across every Fetcher-bridge failure path. Not a correctness bug, but actively hostile to incident response.

**Remediation:**
```go
return false, "", fmt.Errorf("get redis connection: %w", err)
return false, "", fmt.Errorf("get redis client: %w", err)
return false, "", fmt.Errorf("redis setnx for bridge lock: %w", err)
```

Also add a `forbidigo` rule to `.golangci.yml` that flags `%worker`, `%wo`, `%wor`, `%worke` to prevent recurrence.

**Effort:** ~5 minutes (6 line edits + 1 linter rule).

---

### C2 — HTTP 429 From Fetcher Misclassified as Terminal → Mass Extraction Loss

**Severity:** 🔴 CRITICAL
**Triangulation:** 1 angle (performance reviewer on t005 slice)
**Reviewer lens:** Performance reviewer explicitly considering upstream rate-limit interaction

**Location:**
- `internal/discovery/adapters/fetcher/artifact_retrieval.go:247-257` (classification)
- `internal/discovery/services/worker/bridge_retry_classifier.go:120-124` (consumption)

**Problem:**

`classifyArtifactResponse` explicitly transient-cases only HTTP 408 and 425:

```go
case resp.StatusCode == http.StatusRequestTimeout,
    resp.StatusCode == http.StatusTooEarly:
    return nil, fmt.Errorf("%w: fetcher returned status %d",
        sharedPorts.ErrArtifactRetrievalFailed, resp.StatusCode)
```

All other 4xx codes fall through to a generic `>= http.StatusBadRequest` branch that wraps with `sharedPorts.ErrIntegrityVerificationFailed`. The bridge retry classifier then maps `ErrIntegrityVerificationFailed` to `RetryTerminal`, calling `persistTerminalFailure` and writing `bridge_last_error = 'integrity_failed'`.

**Net effect:** A short Fetcher rate-limit window that returns HTTP 429 (Too Many Requests) to multiple concurrent bridge attempts permanently kills every in-flight extraction with class `integrity_failed`, which is **semantically wrong** (nothing was tampered with — it was upstream rate limiting) **and unrecoverable** (terminal class excludes the row from `FindEligibleForBridge` forever).

**Impact quantification:**
- Interval 30s × BatchSize 50 × N tenants = up to **50N extractions terminally failed per 30-second Fetcher 429 window**
- No automatic recovery — requires manual DB intervention per extraction
- Operator sees `bridge_last_error='integrity_failed'` which misdirects their triage ("HMAC tampered? Key mismatch?") when the actual cause was Fetcher throttling

**Remediation:** Add HTTP 429 to the transient case:

```go
case resp.StatusCode == http.StatusRequestTimeout,
    resp.StatusCode == http.StatusTooEarly,
    resp.StatusCode == http.StatusTooManyRequests: // 429 — rate limited
    return nil, fmt.Errorf("%w: fetcher returned status %d",
        sharedPorts.ErrArtifactRetrievalFailed, resp.StatusCode)
```

Update 2 existing tests that currently assert terminal-on-429:
- `internal/discovery/adapters/fetcher/artifact_retrieval_test.go:346`
- `internal/discovery/adapters/fetcher/client_response_classification_test.go:99`

**Bonus hardening:** Respect `Retry-After` header by adding `N × retry_after_seconds` to `updated_at` on 429, so the extraction parks past the rate-limit window before the next eligibility tick picks it up.

**Effort:** ~15 minutes.

---

### C3 — HMAC Contract Divergence Between Matcher and Fetcher (P6 Precondition)

**Severity:** 🔴 CRITICAL (blocks real-Fetcher integration)
**Triangulation:** 1 angle (business-logic reviewer on t002 slice)
**Reviewer lens:** Business-logic mental execution against the documented Fetcher contract

**Location:**
- Matcher: `internal/discovery/adapters/fetcher/artifact_verifier.go:258` (HMAC computed over ciphertext bytes)
- Matcher: `internal/discovery/adapters/fetcher/artifact_retrieval.go:30-32` (HMAC header transported)
- Fetcher contract per `FETCHER_MATCHER.md:90, 146`: HMAC computed over `<unix-timestamp>.<plaintext>`

**Problem:**

The verifier in `artifact_verifier.go` calls:

```go
if err := verifyHMAC(v.hmacKey, ciphertextBytes, expectedHMAC); err != nil {
    return nil, fmt.Errorf("%w: hmac mismatch", sharedPorts.ErrIntegrityVerificationFailed)
}
```

This signs the **encrypted bytes**. But `FETCHER_MATCHER.md` §R3 and §D4, plus Fetcher's internal `key_deriver.go`, document that Fetcher signs `<unix-timestamp>.<plaintext>` — the **unencrypted content with a timestamp prefix for freshness binding**.

**Net effect:** Every real-Fetcher verification will **silently terminal-fail** with `ErrIntegrityVerificationFailed`. This is currently masked because all tests use locally-generated artifacts with locally-computed HMACs (the tests agree with themselves).

This gap is explicitly tracked as **P6** in `/Users/fredamaral/.claude/projects/-Users-fredamaral-repos-lerianstudio-matcher/memory/project_fetcher_bridge_t002_preconditions.md` — the memory note is known, but the resolution is deferred.

**Related critical concern:** No freshness/replay-across-time check exists. Fetcher's timestamp exists specifically to bind the HMAC to a moment; Matcher does not consume a timestamp at all. Even if the HMAC contract is fixed to match Fetcher's, without freshness validation, an old retained ciphertext+HMAC pair re-presented later will verify forever.

**Remediation options (from P6 preconditions doc):**

- **Option A**: Both sides drop to HMAC-over-ciphertext. Simplest, but loses freshness guarantee. Requires Fetcher team to change `key_deriver.go`.
- **Option B**: Matcher pivots to HMAC(timestamp.plaintext). Requires adding timestamp header to artifact response, freshness window enforcement in verifier, and coordination on the canonical format.
- **Option C**: Double-signed artifacts (both HMAC formats transmitted). Most resilient during transition, but increases contract complexity.

**Owner:** Matcher + Fetcher teams (requires cross-team decision meeting).

**Effort:** ~2 hours meeting + 1 day implementation (depends on option chosen).

**Blocking:** Any real-Fetcher integration. Can ship T-001..T-006 against the mock Fetcher, but cannot enable `FETCHER_ENABLED=true` against production Fetcher until resolved.

---

## 5. High-Severity Findings

Severity calibration: **Business logic error, missing critical validation, nil/null safety violation, or performance issue that degrades production behavior at scale**. Must fix before Gate 5 validation.

### C4 — Readiness Queries Use Primary DB Instead of Replica

**Severity:** 🟠 HIGH
**Triangulation:** 1 angle (performance reviewer on t004 slice, explicit FAIL verdict)

**Location:**
- `internal/discovery/adapters/postgres/extraction/extraction_readiness_queries.go:63` (`CountBridgeReadiness`)
- `internal/discovery/adapters/postgres/extraction/extraction_readiness_queries.go:151` (`ListBridgeCandidates`)

**Problem:**

Both readiness queries use `pgcommon.WithTenantTxProvider` which routes to the primary database. The reporting context has an established pattern — `pgcommon.WithTenantReadQuery` — that routes to replicas (with primary fallback), used by `internal/reporting/adapters/postgres/dashboard/dashboard.postgresql.go:65` for the equivalent dashboard aggregation workload.

**Net effect:**
- Dashboard polling (every 5-30 seconds per operator session per tenant) competes with write traffic on the primary DB.
- Per-tenant × per-session × per-second-refresh × N concurrent dashboard users = measurable load on primary.
- Connection pool on primary is sized for writes, not read amplification.
- As tenant count grows (currently small, targeting hundreds), this scales linearly.

**Remediation:**

Switch both queries to replica-first pattern:

```go
// Before:
err := pgcommon.WithTenantTxProvider(ctx, r.provider, func(tx sharedPorts.Tx) error {
    return r.countBridgeReadiness(ctx, tx, threshold, counts)
})

// After:
err := pgcommon.WithTenantReadQuery(ctx, r.provider, func(qx pgcommon.QueryExecutor) error {
    return r.countBridgeReadinessQuery(ctx, qx, threshold, counts)
})
```

The `QueryExecutor` interface already supports `QueryRowContext` / `QueryContext`. Falls back to primary when no replica is configured. Estimated ~20 lines changed across two methods.

**Effort:** ~30 minutes.

---

### C5 — P1 Streaming Plaintext Gap: 256 MiB Buffered → 512 MiB Peak RSS Per Concurrent Artifact

**Severity:** 🟠 HIGH
**Triangulation:** 3 reviewer angles (code, business-logic, performance on t002 slice)

**Location:**
- `internal/discovery/adapters/fetcher/artifact_verifier.go:240-272` (`VerifyAndDecrypt`)
- `internal/discovery/adapters/fetcher/artifact_verifier.go:284-301` (`readBoundedCiphertext`)

**Problem:**

The verifier materializes the full ciphertext into a `[]byte` before HMAC verification and AES-GCM decryption:

```go
ciphertextBytes, err := readBoundedCiphertext(ciphertext, maxCiphertextBytes)
// ciphertextBytes is now up to 256 MiB in memory
if err := verifyHMAC(v.hmacKey, ciphertextBytes, expectedHMAC); err != nil { ... }
plaintext, err := decryptAESGCM(v.aesKey, iv, ciphertextBytes)
// plaintext is now ALSO up to ~256 MiB in memory
```

Post-decrypt, the plaintext lives in a second `[]byte` wrapped by `bytes.Reader`. Peak resident memory per concurrent artifact ≈ **2 × 256 MiB = 512 MiB**.

With `MaxIdleConnsPerHost=10` on the artifact HTTP client, up to 10 concurrent downloads can run — worst-case transient heap **~5 GiB**.

Additionally, `io.ReadAll` on the `LimitReader` allocates in doubling steps (16KB → 32KB → 64KB → ... → 256 MiB), reaching **~512 MiB backing store during the final grow** before trimming.

**Impact:**
- **OOMKill risk in pods < 2 GiB memory.** This is the default for many K8s deployments.
- Go's default `GOGC=100` triggers GC on heap doubling — a pod at 1 GiB memory limit will OOMKill before GC can reclaim the transient doubling.
- Under bursty T-003 bridge worker load (multiple tenants with pending extractions), the cumulative heap pressure becomes operational.

**This is explicitly documented as precondition P1** in `memory/project_fetcher_bridge_t002_preconditions.md` — known gap, deferred to future work.

**Remediation options:**

1. **Stream HMAC via `io.TeeReader`** while buffering blocks for AES-GCM (AES-GCM requires full ciphertext because the auth tag is at the tail). Reduces peak from 2× to 1×.
2. **Chunked authenticated encryption** (STREAM construction) — requires Fetcher contract change.
3. **Spill to tenant-scoped temp file** backed by `io.Reader` — caps RSS at the cost of disk I/O. Safest interim.
4. **Enforce boot-time hard-fail** if pod memory < `2 GiB` when `FETCHER_ENABLED=true`, set `GOMEMLIMIT` to 85% of limit. Interim operational mitigation.

**Owner:** Fetcher-bridge team.

**Effort:** 1-3 days for streaming implementation; ~30 minutes for the GOMEMLIMIT + hard-fail interim.

**Status:** On backlog per T-002 preconditions memo. Until shipped, operators must enforce minimum pod memory via Helm values.

---

### C6 — `recoverDigest` Uses Uncapped `io.Copy` — Bypasses 256 MiB Cap on Replay

**Severity:** 🟠 HIGH
**Triangulation:** 1 angle (performance reviewer on t002 slice)

**Location:** `internal/shared/adapters/custody/artifact_custody_store.go:417-436`

**Problem:**

On the custody replay path (`Exists == true`), `recoverDigest` re-hashes the persisted bytes to preserve the `source_metadata.custody_sha256` audit contract:

```go
func (store *ArtifactCustodyStore) recoverDigest(ctx context.Context, key string) (int64, string, error) {
    reader, err := store.storage.Download(ctx, key)
    if err != nil {
        return 0, "", fmt.Errorf("download for replay recovery: %w", err)
    }
    defer reader.Close()

    hasher := sha256.New()
    counter := &counterWriter{}
    if _, err := io.Copy(io.MultiWriter(hasher, counter), reader); err != nil {
        return 0, "", ...
    }
    return counter.n, hex.EncodeToString(hasher.Sum(nil)), nil
}
```

`io.Copy` has no size bound. The verifier's ingest path enforces `maxCiphertextBytes = 256 MiB` via `io.LimitReader`, but this replay path does not mirror that cap.

**Impact:**

An adversary that achieves custody key poisoning (or even an unrelated bug that writes an oversized blob to a known key) can cause `recoverDigest` to pull an arbitrarily large blob into hashing. Hashing streams efficiently, but `io.Copy` itself doesn't bound read time or bytes — a hostile storage backend could feed gigabytes and stall the retention worker.

More realistically: if the object-storage backend ever mis-reports `Exists == true` for a stale/corrupted key, the worker reads unbounded bytes.

**Remediation:** Wrap Download with `io.LimitReader` at the same cap:

```go
const maxReplayBytes = 256 * 1024 * 1024 // Match verifier cap

func (store *ArtifactCustodyStore) recoverDigest(ctx context.Context, key string) (int64, string, error) {
    reader, err := store.storage.Download(ctx, key)
    if err != nil {
        return 0, "", fmt.Errorf("download for replay recovery: %w", err)
    }
    defer reader.Close()

    limited := io.LimitReader(reader, maxReplayBytes+1)
    hasher := sha256.New()
    counter := &counterWriter{}
    if _, err := io.Copy(io.MultiWriter(hasher, counter), limited); err != nil {
        return 0, "", ...
    }
    if counter.n > maxReplayBytes {
        return 0, "", fmt.Errorf("replay recovery exceeded %d byte cap", maxReplayBytes)
    }
    return counter.n, hex.EncodeToString(hasher.Sum(nil)), nil
}
```

Promote `maxReplayBytes` / `maxCiphertextBytes` to a shared constant in `internal/shared/ports/` so the ingest + replay caps stay in lockstep.

**Effort:** ~5 minutes.

---

### C7 — Custody Write-Once TOCTOU Race: `Exists→Upload` Is Not Atomic

**Severity:** 🟠 HIGH
**Triangulation:** 4 reviewer angles (code, business-logic, security, test on t002 slice)

**Location:** `internal/shared/adapters/custody/artifact_custody_store.go:186-222`

**Problem:**

The custody `Store` method checks `Exists` then performs `Upload`:

```go
existed, err := store.storage.Exists(ctx, key)
if err != nil {
    return nil, fmt.Errorf("...: %w", ...)
}

if existed {
    // Replay path — recover digest
    size, sha256Hex, err := store.recoverDigest(ctx, key)
    ...
    return &ref, nil
}

// New write path — upload with TeeReader for SHA-256 + counter
...
_, err = store.storage.Upload(ctx, key, teed, 0, store.contentType)
```

This is a classic check-then-act TOCTOU window. Two concurrent `Store()` calls with the same `(tenantID, extractionID)` can both observe `existed == false` and both proceed to `Upload`. S3 `PutObject` semantics (without `If-None-Match`) are **last-write-wins**, not write-once.

The comment at line 181-185 claims write-once semantics, but the implementation only guarantees idempotency-after-serialization.

**Impact:**

- Under concurrent bridge worker retries (exactly the scenario the comment claims to handle), two replicas can race for the same extraction. The bridge worker's distributed Redis lock gates this in practice, but the custody store's contract is not race-safe on its own.
- If the distributed lock ever fails (TTL expiry, network partition during lock release), two workers can concurrently bridge the same extraction, both calling `Store`, both racing on the same custody key.
- The second writer's bytes may differ from the first's (different crypto nonce derivation, different Fetcher response timing) — last-write-wins silently overwrites.

**Remediation options:**

- **Option 1 (recommended):** Use S3's conditional PUT with `If-None-Match: *` header. AWS S3 now supports this; SeaweedFS may not. Requires extending `ObjectStorageClient` port with a conditional-upload method.
- **Option 2:** Document the caller-side locking precondition explicitly in the `Store` doc comment, and downgrade the contract from "write-once" to "idempotent-on-first-success-when-caller-holds-lock." The bridge worker's Redis SetNX already provides this in the happy path.
- **Option 3:** Serialize writes with a per-`(tenantID, extractionID)` mutex at the store level (in-process only; doesn't protect cross-replica).

**Owner:** Fetcher-bridge team. Decision required.

**Effort:** ~30 minutes for Option 2 (documentation + doc comment rewrite); ~2-4 hours for Option 1 (port extension + conditional upload wiring + tests).

---

### C8 — Four Files Exceed 500-Line Ring BLOCKING Threshold

**Severity:** 🟠 HIGH
**Triangulation:** 1 angle (code reviewer on t003 slice, explicit FAIL verdict)

**Location:**
- `internal/discovery/services/worker/bridge_worker.go` — **680 lines**
- `internal/discovery/services/worker/bridge_worker_integration_test.go` — **1013 lines**
- `internal/discovery/services/worker/bridge_worker_test.go` — **675 lines**
- `internal/discovery/services/command/bridge_extraction_commands_test.go` — **630 lines**

**Problem:**

Ring file-size enforcement policy (`dev-team/skills/shared-patterns/file-size-enforcement.md`) treats > 500 lines as CRITICAL-severity for non-auto-generated Go files. The prior polish on `bridge_worker.go` (Polish Fix 2 removed the exponential-backoff helpers) brought it from 750+ to 680 lines, but it's still above the cap.

**Cohesion analysis:** `bridge_worker.go` mixes four responsibilities:
1. Worker lifecycle (Start/Stop/Done/UpdateRuntimeConfig)
2. Polling loop (pollCycle, processTenant, bridgeOne)
3. T-005 retry logic (persistTerminalFailure, handleTransientFailure)
4. Redis distributed locking (acquireLock, releaseLock)

**Remediation — suggested split** (already identified by code reviewer):

```
bridge_worker.go        (struct + Start/Stop/Done/UpdateRuntimeConfig + prepareRunState + run)
bridge_worker_poll.go   (pollCycle + processTenant + bridgeOne)
bridge_worker_retry.go  (persistTerminalFailure + handleTransientFailure + terminalFailureMessage + logBridgeError)
bridge_worker_lock.go   (acquireLock + releaseLock)
```

All files stay in `package worker`. Test files mirror the source split:
- `bridge_worker_test.go` (lifecycle tests)
- `bridge_worker_poll_test.go`
- `bridge_worker_retry_test.go`

Integration file splits by scenario:
- `bridge_worker_integration_test.go` → keeps happy path
- `bridge_worker_integration_default_tenant_test.go`
- `bridge_worker_integration_distributed_lock_test.go`

**Effort:** ~60 minutes for both source + test splits. No logic changes.

---

### C14 — Ready/Failed Readiness Partition Overlap (Root Cause: C21)

**Severity:** 🟠 HIGH
**Triangulation:** 1 angle (business-logic reviewer on t004 slice, NEEDS_DISCUSSION verdict)

**Location:** `internal/discovery/adapters/postgres/extraction/extraction_readiness_queries.go:72-79` and `:207-224`

**Problem:**

The `CountBridgeReadiness` FILTER aggregates:

```sql
COUNT(*) FILTER (WHERE status = 'COMPLETE' AND ingestion_job_id IS NOT NULL) AS ready_count,
...
COUNT(*) FILTER (WHERE status IN ('FAILED', 'CANCELLED') OR bridge_last_error IS NOT NULL) AS failed_count,
```

A row with `status = 'COMPLETE' AND ingestion_job_id IS NOT NULL AND bridge_last_error IS NOT NULL` would be counted in **both** buckets. The partition invariant claimed by `BridgeReadinessCounts.Total()` (that counts sum to total row count) silently breaks.

**Is this state reachable?** Today, no — because `FindEligibleForBridge` filters `bridge_last_error IS NULL`, so a terminally-failed row never flows through `LinkExtractionToIngestion`. But this invariant is **non-local**: it depends on worker-adapter-filter semantics holding elsewhere. Any future:

- Manual retry admin endpoint that clears `bridge_last_error` to re-queue a row
- Direct DB fix by operations to link a previously-failed-but-output-present extraction
- Code path change that bypasses `FindEligibleForBridge`

...silently breaks the partition without any test or constraint catching it.

**Root cause relationship:** This finding is the **symptom**. The write-layer race that actually produces such a row is **C21** (`executeMarkBridgeFailed` missing `AND ingestion_job_id IS NULL` guard).

**Remediation — two-layer fix:**

1. **Primary fix (C21):** SQL-layer guard on `executeMarkBridgeFailed`.
2. **Defense-in-depth (C14):** Add `AND bridge_last_error IS NULL` to the `ready_count` FILTER:

```sql
COUNT(*) FILTER (WHERE status = 'COMPLETE'
                   AND ingestion_job_id IS NOT NULL
                   AND bridge_last_error IS NULL) AS ready_count,
```

Also mirror in the `ListBridgeCandidates` "ready" state drilldown at line 207.

3. **Alternative:** Add a DB `CHECK` constraint: `CHECK (NOT (ingestion_job_id IS NOT NULL AND bridge_last_error IS NOT NULL))`. More invasive but catches any future caller.

4. **Alternative:** Domain-level guard in `ExtractionRequest.LinkToIngestion` — reject if `BridgeLastError != ""`.

**Effort:** ~15 minutes for the SQL FILTER change + integration test.

---

### C15 — No Worker-Liveness Signal; "Truthful Readiness" Claim Unfulfilled

**Severity:** 🟠 HIGH
**Triangulation:** 1 angle (business-logic reviewer on t004 slice)

**Location:**
- `internal/discovery/services/query/bridge_readiness_queries.go:46-50` (`BridgeReadinessSummary` struct)
- `internal/discovery/adapters/http/dto/bridge_readiness_dto.go:22-31` (`BridgeReadinessSummaryResponse`)

**Problem:**

The T-004 commit message explicitly brands the feature as "**Truthful** operational readiness projection" (commit `f0f32ca`). The 5-way partition (ready/pending/stale/failed/in_flight) accurately reflects row-level state, but the dashboard **cannot distinguish**:

- "Worker is running + draining backlog" (healthy)
- "Worker is stopped + backlog silently accumulating" (broken)

Today the operator can only infer worker death by watching the `pending` bucket grow until rows flip to `stale` — a process that takes `staleThreshold` (default 1 hour) to materialize.

For a feature promising **truthful** readiness, "operator must wait an hour to notice the worker is dead" is a real product-claim violation.

**Evidence of in-memory state:** `bridge_worker.go:138` has `running atomic.Bool`. `custody_retention_worker.go` has similar. This state exists but is not surfaced anywhere.

**Remediation options:**

**Option A (minimal):** Add a boolean `workerRunning` to the summary DTO. Aggregates across replicas via Redis health-check key. Requires coordinating lease signals.

**Option B (richer):** Add `lastTickAt timestamp` to summary. Bridge worker writes its last successful cycle timestamp to a shared cache/DB on every tick. Dashboard computes `staleness = now() - lastTickAt` and exposes a separate `workerStaleness` field. Operators see "worker last ticked 3 hours ago" vs "worker is healthy."

**Option C (scope adjustment):** Rename the feature to "Extraction backlog dashboard" or similar — don't claim "truthful" if the dashboard can't detect the worker being dead.

**Owner:** Product + engineering (requires scope decision).

**Effort:** ~1 hour for Option A, ~4 hours for Option B including cache wiring, 15 minutes for Option C (doc + commit message update).

---

### C17 — `BridgeRetryMaxAttempts` Missing From Reconciler → Runtime Hot-Reload Silently No-Ops

**Severity:** 🟠 HIGH
**Triangulation:** **3 reviewer angles** (code-infra, dead-code-infra, consequences-infra)

**Location:**
- `internal/bootstrap/systemplane_reconciler_worker.go:85-110` (`snapshotToWorkerConfig` — the defect)
- `internal/bootstrap/systemplane_keys_runtime_services.go:397-409` (definition promises `MutableAtRuntime: true`)
- `internal/bootstrap/worker_manager_runtime.go:579-583` (comparator reads `RetryMaxAttempts`)
- `internal/bootstrap/config.go:497-503` (accessor falls back to default on zero)

**Problem:**

The `BridgeRetryMaxAttempts` systemplane key is registered with `ApplyBehavior: ApplyWorkerReconcile` and `MutableAtRuntime: true`, promising operators they can PUT a new value and the worker reconciles live. But `snapshotToWorkerConfig()` — invoked by `newRuntimeReloadObserver` on every systemplane reload — does NOT hydrate this field from the snapshot:

```go
// Current (buggy):
return &Config{
    Fetcher: FetcherConfig{
        BridgeIntervalSec: snapInt(snap, "fetcher.bridge_interval_sec", defaultBridgeInterval),
        BridgeBatchSize:   snapInt(snap, "fetcher.bridge_batch_size", defaultBridgeBatchSize),
        // ... missing:
        // BridgeRetryMaxAttempts: snapInt(snap, "fetcher.bridge_retry_max_attempts", defaultBridgeRetryMaxAttempts),
        CustodyRetentionSweepIntervalSec: ...,
        CustodyRetentionGracePeriodSec:   ...,
    },
}
```

**Cascade:**
1. Operator PUTs `fetcher.bridge_retry_max_attempts = 10` via systemplane API
2. Audit log records success
3. `newRuntimeReloadObserver` fires
4. `snapshotToWorkerConfig` produces a Config with `RetryMaxAttempts = 0` (zero-value)
5. `workerConfigChanged` compares comparable config: `0 == 0` → no change detected
6. Bridge worker never restarts
7. `applyFetcherBridgeRuntimeConfig` resolves back to the default via `<=0` fallback

**Operator experience:** PUT succeeds. Audit log succeeds. Worker behavior **silently** does not change. The contract the operator relied on (`MutableAtRuntime: true`) is a lie.

**Note on sibling key:** `BridgeStaleThresholdSec` is **also missing** from `snapshotToWorkerConfig()`, but it's NOT affected by this bug because the HTTP handler reads it via `configGetter()` (ConfigManager path), and `ConfigManager.UpdateFromSystemplane` at `config_manager_systemplane.go:167` DOES plumb it. The LiveRead flow works. Only the WorkerReconcile path is broken, and only `BridgeRetryMaxAttempts` is WorkerReconcile.

**Remediation:**

```go
// Add to snapshotToWorkerConfig:
BridgeRetryMaxAttempts: snapInt(snap, "fetcher.bridge_retry_max_attempts", defaultBridgeRetryMaxAttempts),
```

Add a regression test in `systemplane_reconciler_worker_test.go` asserting the value round-trips. Extend `TestWorkerReconciler_FetcherConfig_AppliedToCfg` to cover `BridgeRetryMaxAttempts`. Add an end-to-end reconcile test in `worker_manager_runtime_test.go` that asserts a max-attempts change through a full `ApplyConfig` cycle produces `workerConfigChanged == true` and triggers a bridge worker restart.

**Effort:** ~10 minutes (1 line of code + 2 tests).

**Why HIGH not MEDIUM:** The operator-facing contract is broken. Operators relying on runtime tuning will debug for hours before realizing the PUT did nothing. Documented and audited, but behaviorally dead.

---

### C21 — `executeMarkBridgeFailed` Missing `AND ingestion_job_id IS NULL` Guard (Root Cause of C14)

**Severity:** 🟠 HIGH
**Triangulation:** 1 angle (business-logic reviewer on t005 slice)

**Location:** `internal/discovery/adapters/postgres/extraction/extraction_bridge_failure.go:253-267`

**Problem:**

`executeMarkBridgeFailed` performs an unconditional `UPDATE` keyed only by `id`:

```go
_, err := tx.ExecContext(ctx, `
    UPDATE extraction_requests
    SET bridge_attempts = $1,
        bridge_last_error = $2,
        bridge_last_error_message = $3,
        bridge_failed_at = $4,
        updated_at = $5
    WHERE id = $6
`, extraction.BridgeAttempts, extraction.BridgeLastError, ...)
```

Contrast with `executeIncrementBridgeAttempts` (extraction_bridge_failure.go:209-217) which correctly includes a race-safe guard (Polish Fix 3):

```go
_, err := tx.ExecContext(ctx, `
    UPDATE extraction_requests
    SET bridge_attempts = $1, updated_at = $2
    WHERE id = $3 AND ingestion_job_id IS NULL
`, ...)
```

**Concurrency scenario (lock-TTL-expiry race):**

1. Replica A's bridge orchestrator succeeds: `LinkIfUnlinked` writes `ingestion_job_id = J1`
2. Replica A's lock TTL expires (default 60s, slow Fetcher/S3 interaction)
3. Replica B acquires the lock, picks up the same extraction from its older snapshot
4. Replica B's orchestrator's Fetcher call fails (artifact not found)
5. Replica B's `persistTerminalFailure` calls `MarkBridgeFailed` → `executeMarkBridgeFailed`
6. The UPDATE succeeds because the WHERE clause has no `ingestion_job_id IS NULL` guard
7. **Row now has `ingestion_job_id = J1` AND `bridge_last_error = 'artifact_not_found'`**

The invariant "a row is EITHER linked OR terminally-failed, never both" breaks. Downstream: **C14** (readiness partition overlap) manifests.

**Remediation:**

Mirror the narrow-UPDATE pattern from `executeIncrementBridgeAttempts`:

```go
result, err := tx.ExecContext(ctx, `
    UPDATE extraction_requests
    SET bridge_attempts = $1,
        bridge_last_error = $2,
        bridge_last_error_message = $3,
        bridge_failed_at = $4,
        updated_at = $5
    WHERE id = $6 AND ingestion_job_id IS NULL
`, ...)

rowsAffected, err := result.RowsAffected()
if err != nil {
    return fmt.Errorf("rows affected check: %w", err)
}
if rowsAffected == 0 {
    // Either not found, or concurrent link won
    // Probe to distinguish
    var exists bool
    err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM extraction_requests WHERE id = $1)`, id).Scan(&exists)
    if err != nil { return ... }
    if !exists { return repositories.ErrExtractionNotFound }
    return sharedPorts.ErrExtractionAlreadyLinked
}
```

The worker's `persistTerminalFailure` should treat `ErrExtractionAlreadyLinked` as "concurrent link won — treat as benign, log at info":

```go
if err := worker.extractionRepo.MarkBridgeFailed(ctx, extraction); err != nil {
    if errors.Is(err, sharedPorts.ErrExtractionAlreadyLinked) {
        logger.Log(ctx, libLog.LevelInfo, "bridge: terminal-failure write skipped — concurrent link won")
        return nil
    }
    return err
}
```

**Regression test:** sqlmock test setting up the race condition (seed `ingestion_job_id = <uuid>` then attempt `executeMarkBridgeFailed`) and assert:
- `rowsAffected == 0`
- Returned error matches `sharedPorts.ErrExtractionAlreadyLinked`
- Extraction row is NOT mutated

**Effort:** ~15 minutes (SQL change + sentinel handling + regression test).

---

### C30 — `initFetcherBridgeWorker` Silent Soft-Disable on Missing Upstream Dependencies

**Severity:** 🟠 HIGH
**Triangulation:** 1 angle (business-logic reviewer on infra-wiring slice)

**Location:** `internal/bootstrap/init_fetcher_bridge.go:436-461` (both `initFetcherBridgeWorker` and `initCustodyRetentionWorker`)

**Problem:**

When `FETCHER_ENABLED=true`, the initialization helpers silently return `(nil, nil)` with only a warn log if key upstream dependencies are nil:

```go
func initFetcherBridgeWorker(...) (*discoveryWorker.BridgeWorker, error) {
    if cfg == nil || !cfg.Fetcher.Enabled {
        return nil, nil
    }

    if bundle == nil {
        logger.Log(ctx, libLog.LevelWarn, "fetcher bridge worker not wired: bundle is nil")
        return nil, nil // SILENT SOFT-DISABLE
    }

    if err := EnsureBridgeOperational(bundle); err != nil {
        return nil, err // HARD FAIL on missing crypto
    }

    if provider == nil {
        logger.Log(ctx, libLog.LevelWarn, "fetcher bridge worker not wired: provider is nil")
        return nil, nil // SILENT SOFT-DISABLE
    }

    if extractionRepo == nil {
        logger.Log(ctx, libLog.LevelWarn, "fetcher bridge worker not wired: extraction repo is nil")
        return nil, nil // SILENT SOFT-DISABLE
    }
    ...
}
```

**Asymmetry:** `EnsureBridgeOperational` correctly hard-fails on missing APP_ENC_KEY / ObjectStorage (P4 hardening). But nil `bundle` / `provider` / `extractionRepo` — which represent upstream module initialization failures — are soft-disabled with only a warn log.

**Net effect:** If an upstream module (ingestion UseCase or extraction repository) fails to initialize but the failure isn't propagated, the bridge silently doesn't run even though Fetcher is enabled. Bootstrap completes; bridge never ticks; operator has no signal beyond a warn line that may scroll past.

**Why this defeats the P4 intent:** The `EnsureBridgeOperational` hardening exists specifically because "silently degraded" is the wrong default for a Fetcher-enabled deployment. The same reasoning applies to upstream deps.

**Remediation options:**

**Option 1 (recommended):** Hard-fail on missing upstream deps when `FETCHER_ENABLED=true`:

```go
if bundle == nil {
    return nil, fmt.Errorf("%w: bridge adapter bundle is nil", ErrFetcherBridgeNotOperational)
}
if provider == nil {
    return nil, fmt.Errorf("%w: infrastructure provider is nil", ErrFetcherBridgeNotOperational)
}
if extractionRepo == nil {
    return nil, fmt.Errorf("%w: extraction repo is nil", ErrFetcherBridgeNotOperational)
}
```

**Option 2:** Document the invariant — "bundle/provider/extractionRepo must be non-nil when `FETCHER_ENABLED=true` and upstream modules succeeded." Add a bootstrap-level assertion that tests this explicitly.

**Combined with C23:** Extending `EnsureBridgeOperational` to also validate the `ArtifactCustody` field (currently omitted) closes the full gate.

**Effort:** ~10 minutes for Option 1 (replace 3 warn-and-return with error returns) + corresponding test updates.

---

## 6. Medium-Severity Findings

Severity calibration: **Code quality issue affecting maintainability, missing error handling, or contract drift that doesn't currently break production but creates fragility.**

### C9 — Redundant `FindByID` in `LinkExtractionToIngestion`

**Severity:** 🟡 MEDIUM
**Location:** `internal/shared/adapters/cross/fetcher_bridge_adapters.go:172-206`

The `LinkExtractionToIngestion` adapter method loads the full extraction row via `FindByID` to run state-machine validation (`extraction.LinkToIngestion`), then performs the atomic `LinkIfUnlinked` UPDATE. On the happy path this is **2 DB round-trips per bridge outcome** instead of 1.

Meanwhile, the orchestrator at `internal/discovery/services/command/bridge_extraction_commands.go:184` has already loaded the extraction in `loadEligibleExtraction`. The same row is fetched twice per bridge pass.

**Impact:** Doubles the `SELECT ... WHERE id = $1` cost per processed extraction at batch size 50, across N tenants, per tick. Index-backed so per-query cost is low, but semantically wasteful and amplifies with scale.

**Remediation:** Extend the `ExtractionLifecycleLinkWriter` port to accept the pre-loaded `*entities.ExtractionRequest` (or a minimal `ExtractionLinkContext{ID, Status}` struct). The orchestrator passes the entity it already has.

**Effort:** ~30 minutes (port change + 2 call sites + tests).

---

### C10 — `idx_extraction_requests_eligible_for_bridge` Partial Index Predicate Drift

**Severity:** 🟡 MEDIUM
**Triangulation:** 3 angles (perf × 2 + biz + cons)
**Location:** `migrations/000024_fetcher_bridge_indexes.up.sql:21-23` vs `internal/discovery/adapters/postgres/extraction/extraction.postgresql.go:604-613`

Migration 000024 defines the partial index as:
```sql
CREATE INDEX CONCURRENTLY idx_extraction_requests_eligible_for_bridge
ON extraction_requests (updated_at ASC)
WHERE status = 'COMPLETE' AND ingestion_job_id IS NULL;
```

But T-005 added `AND bridge_last_error IS NULL` to the query's WHERE clause as part of the livelock prevention (P2). PostgreSQL will use the partial index, then filter terminal rows from the heap — so correctness is intact, but the index carries progressively more dead rows as terminal failures accumulate.

**Impact:** At steady state with a healthy bridge, negligible. During an incident that terminally-fails many extractions (e.g., Fetcher outage classified wrong — see C2), the index size grows linearly with terminal population until archival or manual cleanup. Planner still uses it but does more heap fetches.

**Remediation:** Follow-up migration 000028 drops and recreates the index with tightened predicate:
```sql
DROP INDEX CONCURRENTLY IF EXISTS idx_extraction_requests_eligible_for_bridge;
CREATE INDEX CONCURRENTLY idx_extraction_requests_eligible_for_bridge
  ON extraction_requests (updated_at ASC)
  WHERE status = 'COMPLETE'
    AND ingestion_job_id IS NULL
    AND bridge_last_error IS NULL;
```

**Effort:** ~15 minutes (new migration pair + verification against existing index).

---

### C11 — Drilldown Hydrates Unused JSONB Columns (~100 Wasted Unmarshals/Page)

**Severity:** 🟡 MEDIUM
**Location:** `internal/discovery/adapters/postgres/extraction/extraction_readiness_queries.go:198` → `extraction.go:62-73` (`ExtractionModel.ToDomain`)

`ListBridgeCandidates` selects the full 18-column `allColumns` set via `scanExtraction`, which triggers `json.Unmarshal` for `tables` and `filters` JSONB columns. But the `BridgeCandidateResponse` DTO only exposes 9 fields — `tables` and `filters` are never projected to the HTTP response.

**Impact:** 100 unmarshal calls per 50-row page (Tables + Filters per row × 2) × N pages × N tenants × dashboard polling frequency. Each `json.Unmarshal` into a slice allocates repeatedly, adding GC pressure on a hot dashboard path.

**Remediation:** Add a narrow projection + scan path (e.g., `scanBridgeCandidateRow`) that selects only the columns the drilldown actually renders:
- `id, connection_id, ingestion_job_id, fetcher_job_id, status, created_at, updated_at, bridge_attempts, bridge_last_error, bridge_failed_at`

Leave full entity hydration for the `ExtractionRequestResponse` detail endpoint.

**Effort:** ~30 minutes (new scan helper + test).

---

### C12 — Missing Fuzz for `normalizeExtractionStatus` and `validateFetcherResultPath`

**Severity:** 🟡 MEDIUM
**Location:**
- `internal/discovery/adapters/fetcher/client_validation.go:53-88` (`normalizeExtractionStatus`)
- `internal/discovery/adapters/fetcher/client_validation.go:90-111` (`validateFetcherResultPath`)

Both functions are security-adjacent parsers that receive external input from the Fetcher service. `normalizeExtractionStatus` gates the COMPLETE → ResultPath validation transition. `validateFetcherResultPath` is path-traversal defense (rejects schemes, query strings, fragments, relative paths, traversal segments).

Neither has a Go fuzz test. Unit tests cover ASCII cases; missing coverage:
- URL-encoded traversal (`/data/%2e%2e/etc/passwd`)
- Windows-style separators (`\\..\\`)
- NUL bytes (`/data/file\x00.json`)
- Unicode path normalization bypasses (`/data/\u2024\u2024/file`)
- Mixed separators
- Pathological case variations in status normalization

**Remediation:** Add `FuzzNormalizeExtractionStatus` + `FuzzValidateFetcherResultPath` with seed corpora covering the above attack classes. Assertions: either error return OR no `..` / `//` / null bytes / scheme / fragment in output.

**Effort:** ~1 hour (two fuzz targets + seed corpus).

---

### C13 — `sweepCycle()` + Default-Tenant Inclusion Untested

**Severity:** 🟡 MEDIUM
**Location:** `internal/discovery/services/worker/custody_retention_worker.go:334-378` + `custody_retention_worker_test.go`

The core orchestration function of the retention worker — lock acquire, tenant iteration, default-tenant inclusion, aggregation across tenants — has **zero unit tests**. The existing `stubInfraProvider`'s `GetRedisConnection` returns `(nil, nil)` which panics `acquireLock`, so `sweepCycle` is structurally untestable with the current stubs.

Default-tenant inclusion is load-bearing per `MEMORY.md` (the "Default tenant not dispatched" regression in 2026-02-06). No test asserts retention worker iterates over a list containing both default + UUID tenants.

**Remediation:**

1. Upgrade `stubInfraProvider` to return a configurable Redis stub (lock-acquired, lock-not-acquired, error)
2. Add 5 sweepCycle tests:
   - Lock not acquired → early return, no deletes
   - Lock error → warn log, no deletes
   - Tenant list error → warn log, no deletes
   - Multi-tenant aggregation (includes default + UUID tenants, asserts per-tenant counts sum)
   - Empty tenant-list string skipped

**Effort:** ~2 hours (stub upgrade + 5 tests).

---

### C19 — HTTP DTO Omits Bridge/Custody Fields; Contradicts Drilldown Promise

**Severity:** 🟡 MEDIUM
**Triangulation:** 2 angles (consequences × 2 on t003 + t005)
**Location:**
- `internal/discovery/adapters/http/dto/responses.go:75-133` (`ExtractionRequestResponse`, `ExtractionRequestFromEntity`)
- `internal/discovery/adapters/http/dto/bridge_readiness_dto.go:34-36` (drilldown promise)

`BridgeCandidateResponse` docstring claims: "consumers wanting full extraction state can call the existing `GET /v1/discovery/extractions/{extractionId}` endpoint." But `ExtractionRequestResponse` (and its entity-to-DTO mapper `ExtractionRequestFromEntity`) does NOT surface the 5 new bridge/custody fields: `BridgeAttempts`, `BridgeLastError`, `BridgeLastErrorMessage`, `BridgeFailedAt`, `CustodyDeletedAt`.

When an operator drills from the dashboard's "failed" bucket into an individual extraction, the response does NOT show the actual failure class, message, attempt count, or failure timestamp. They're told "this extraction failed" but cannot see WHY through the referenced endpoint.

**Remediation:** Add the 5 fields to `ExtractionRequestResponse` with `omitempty`:

```go
type ExtractionRequestResponse struct {
    // ... existing fields ...
    BridgeAttempts         int        `json:"bridgeAttempts,omitempty"`
    BridgeLastError        string     `json:"bridgeLastError,omitempty"`
    BridgeLastErrorMessage string     `json:"bridgeLastErrorMessage,omitempty"`
    BridgeFailedAt         *time.Time `json:"bridgeFailedAt,omitempty"`
    CustodyDeletedAt       *time.Time `json:"custodyDeletedAt,omitempty"`
}
```

Update `ExtractionRequestFromEntity` to populate them. Regenerate Swagger.

Also add `BridgeLastError` to `BridgeCandidateResponse` for the failed-bucket drilldown.

**Effort:** ~45 minutes (DTO + mapper + Swagger regen + JSON-shape test).

---

### C20 — `CustodyRetentionWorker.Start(nil)` Returns Misleading Sentinel

**Severity:** 🟡 MEDIUM
**Location:** `internal/discovery/services/worker/custody_retention_worker.go:213-216`

The nil-receiver guard on `Start()` returns `ErrNilCustodyRetentionExtractionRepo` — a sentinel whose semantic is "worker was constructed without an extraction repository." But the actual failure mode is "worker itself is nil." The caller's `errors.Is` check will match both conditions with the same identity, making nil-worker bugs look like dependency-injection bugs.

**Remediation:** Introduce a dedicated sentinel:

```go
// At package level:
var ErrCustodyRetentionWorkerNil = errors.New("custody retention worker is nil")

// In Start:
func (worker *CustodyRetentionWorker) Start(ctx context.Context) error {
    if worker == nil {
        return ErrCustodyRetentionWorkerNil
    }
    ...
}
```

Update `TestCustodyRetentionWorker_NilReceiverGuards` assertion.

**Effort:** ~5 minutes.

---

### C22 — Same-Class `MarkBridgeFailed` Refreshes Message; Immutable-vs-Idempotent Contract Ambiguity

**Severity:** 🟡 MEDIUM
**Location:** `internal/discovery/domain/entities/extraction_request_bridge_failure.go:92-104`

The inferred requirement states "failure records immutable once written." The implementation rejects class mismatch via `ErrBridgeFailureClassRequired` (test `TestMarkBridgeFailed_DifferentClass_Rejected` pins this), BUT allows same-class re-call to **refresh the message and timestamp**:

```go
// Existing behavior:
if er.BridgeLastError != "" {
    if er.BridgeLastError != class {
        return fmt.Errorf("%w: existing class %s", ErrBridgeFailureClassRequired, er.BridgeLastError)
    }
    // Same class: refresh message + timestamp
}
er.BridgeLastError = class
er.BridgeLastErrorMessage = message
er.BridgeFailedAt = time.Now().UTC()
```

**Scenario where this matters:** The original terminal failure's reason is captured (e.g., `"integrity_failed: HMAC mismatch at extraction 2026-04-16T10:23:01Z with tenant X"`). A subsequent transient failure that escalates via max-attempts writes `"escalated to terminal after 5 attempts: custody_store_failed"` — same class, different message. **The original reason is lost.**

**Contract question:** Is this a feature (readiness drilldown shows the most recent signal) or a bug (audit trail should preserve the first reason)?

**Remediation options:**

**Option A:** Document that "idempotent-refresh" IS the contract (current behavior). Update the docstring + inferred requirement. Add a fuzz test pinning the same-class-message-freshness invariant.

**Option B:** Add a first-writer-wins guard — skip the message/timestamp refresh when `BridgeLastErrorMessage != ""`:

```go
if er.BridgeLastError == "" {
    er.BridgeLastError = class
    er.BridgeLastErrorMessage = message
    er.BridgeFailedAt = time.Now().UTC()
}
// else: idempotent no-op
```

**Owner:** Fetcher-bridge team. Decision required.

**Effort:** ~10 minutes either way, plus decision meeting.

---

### C23 — `EnsureBridgeOperational` Missing `ArtifactCustody` Field Check

**Severity:** 🟡 MEDIUM
**Location:** `internal/bootstrap/init_fetcher_bridge.go:389-410`

The operational gate validates `Intake`, `LinkWrite`, and `VerifiedArtifactOrchestrator` but NOT `ArtifactCustody`, even though `NewBridgeExtractionOrchestrator` requires a non-nil custody argument and `initFetcherBridgeWorker:487` reads `bundle.ArtifactCustody`.

Today the invariant holds (those fields are written atomically with `VerifiedArtifactOrchestrator` at `init_fetcher_bridge.go:268-271`), but an audit-safety perspective calls for an explicit gate that covers every field the orchestrator constructor consumes.

**Remediation:**

```go
if bundle.ArtifactCustody == nil {
    return fmt.Errorf("%w: artifact custody store is not wired", ErrFetcherBridgeNotOperational)
}
```

Add between the `VerifiedArtifactOrchestrator` check and the existing `return nil`.

**Effort:** ~5 minutes.

---

### C25 — Legacy Config Accessors Lack Nil-Receiver Guards

**Severity:** 🟡 MEDIUM
**Location:** `internal/bootstrap/config_env.go:333, 343, 353, 363, 373, 383`

Inconsistent nil-receiver guards across sibling `*Config` methods:
- Newer `FetcherBridgeInterval`, `FetcherBridgeBatchSize`, `FetcherBridgeStaleThreshold`, `FetcherBridgeRetryMaxAttempts`, `FetcherCustodyRetentionSweepInterval`, `FetcherCustodyRetentionGracePeriod`, `FetcherMaxExtractionBytes` all guard `cfg == nil`
- Older `FetcherHealthTimeout`, `FetcherRequestTimeout`, `FetcherDiscoveryInterval`, `FetcherSchemaCacheTTL`, `FetcherExtractionPollInterval`, `FetcherExtractionTimeout` do NOT guard `cfg == nil` — panic on nil receiver

Today's call chains always supply a non-nil `cfg`, but the inconsistency is a latent fragility.

**Remediation:** Add `cfg == nil` guard returning the default:

```go
func (cfg *Config) FetcherHealthTimeout() time.Duration {
    if cfg == nil || cfg.Fetcher.HealthTimeoutSec <= 0 {
        return defaultFetcherHealthTimeout
    }
    return time.Duration(cfg.Fetcher.HealthTimeoutSec) * time.Second
}
```

Apply to 6 accessors.

**Effort:** ~10 minutes.

---

### C26 — Extraction Repository Constructed Twice in Bootstrap

**Severity:** 🟡 MEDIUM
**Location:** `internal/bootstrap/init.go:2491` and `internal/bootstrap/init_discovery.go:185`

`discoveryExtractionRepo.NewRepository(provider)` is constructed twice — once for the bridge wiring, once for the discovery HTTP module. Two separate repo instances share the same provider, so there's no correctness hazard today. But a future stateful change on the repo (connection pool, caches, metrics) would silently diverge between the HTTP path and the bridge/custody workers.

**Remediation:** Construct `extractionRepo` once at the top of `initModulesAndMessaging` and pass the same instance into `initOptionalDiscoveryWorker`.

**Effort:** ~10 minutes.

---

### C27 — No E2E Journey for `/bridge/summary` + `/bridge/candidates`

**Severity:** 🟡 MEDIUM
**Location:** `tests/e2e/journeys/` (missing)

No end-to-end or integration test exercises `GET /v1/discovery/extractions/bridge/summary` or `GET .../bridge/candidates`. The mock Fetcher server (`tests/e2e/mock/fetcher_server.go`) wasn't updated to simulate a completed extraction whose bridge state is observable end-to-end via these endpoints.

Regression coverage depends solely on handler-level unit tests + integration SQL tests. If the HTTP routing, DTO serialization, or SQL+DTO end-to-end flow drifts, it won't surface until production.

**Remediation:** Add at least one journey E2E test that:
1. Starts an extraction
2. Forces it to complete with bridge failure via the mock
3. Reads the summary + candidate endpoints
4. Asserts bucket counts / drilldown rows

**Effort:** ~2 hours (mock extension + journey test).

---

### C31 — Migrations 000026/000027 Use Plain `CREATE INDEX` (No `CONCURRENTLY`)

**Severity:** 🟡 MEDIUM
**Location:** `migrations/000026_bridge_failure_semantics.up.sql:55-64`, `migrations/000027_custody_deletion_marker.up.sql:36-39`

Migrations 000024 and 000025 use `CREATE INDEX CONCURRENTLY IF NOT EXISTS`. Migrations 000026 and 000027 use plain `CREATE INDEX IF NOT EXISTS`. Not a correctness issue, but on large `extraction_requests` or `ingestion_jobs` tables the non-concurrent form takes exclusive write locks during migration apply — blocking bridge + ingestion traffic for the duration of the index build.

With `MultiStatementEnabled: true` in the golang-migrate driver, statements run on a raw connection (no wrapping tx), so `CONCURRENTLY` IS compatible.

**Remediation:** Switch to `CREATE INDEX CONCURRENTLY IF NOT EXISTS` for consistency. If 000026/000027 have already been deployed, add a follow-up migration that drops and recreates with CONCURRENTLY (carefully, to avoid double-indexing).

**Effort:** ~10 minutes if unreleased; ~30 minutes if already deployed to any environment (requires drop+recreate migration).

---

## 7. Low-Severity Findings

Severity calibration: **Code style, minor cleanup opportunities, or quality-of-life improvements.** Track as TODOs; address opportunistically.

### C18 — `FetcherBridgeAdapters.ArtifactRetrieval`/`.ArtifactVerifier` Populated But Unread

**Severity:** 🟢 LOW
**Location:** `internal/bootstrap/init_fetcher_bridge.go:88, 268, 89, 269`

The `FetcherBridgeAdapters` bundle struct declares `ArtifactRetrieval` and `ArtifactVerifier` fields, populates them at construction time, but no production code reads them. `VerifiedArtifactRetrievalOrchestrator` already holds both internally.

**Remediation:** Either drop the two fields (simplest) or add a doc comment explaining they're retained for future diagnostic observability.

**Effort:** ~5 minutes.

---

### C24 — FETCHER_MATCHER.md Claims "Planning in Progress" but T-001..T-006 Shipped

**Severity:** 🟢 LOW
**Triangulation:** 2 angles (code-infra + consequences-infra)
**Location:** `FETCHER_MATCHER.md:3-5, 38-48, 231-335`

The document declares "Planning in progress", lists design decisions D2/D4/D5/D9 as PENDING, and claims `IngestionJobID is declared, never populated`. But:
- All T-001 through T-006 are shipped (9 commits under review)
- D2/D4/D9 are locked in `MEMORY.md`
- `IngestionJobID` IS populated at `bridge_extraction_commands.go:308`

An operator or AI agent reading this will believe the feature is still in design.

**Remediation options:**
- Update §2 "Current State" to mark T-001..T-006 shipped
- Migrate PENDING decisions to "Resolved Decisions" table
- OR delete if `docs/pre-dev/fetcher-bridge/{prd,trd}.md` supersede it

**Effort:** ~15 minutes for an update pass; ~5 minutes to delete and redirect to pre-dev docs.

---

### C28 — Test Stubs Inconsistent: `*sql.Tx` vs `sharedPorts.Tx`

**Severity:** 🟢 LOW
**Location:** `internal/discovery/adapters/http/handlers_test.go:229, 237, 278`

In `handlers_test.go`, the mock for `MarkBridgeFailedWithTx` and `IncrementBridgeAttemptsWithTx` declares parameters as `*sql.Tx`, while `MarkCustodyDeletedWithTx` uses the canonical `sharedPorts.Tx`. Since `sharedPorts.Tx = *sql.Tx` (type alias), the code compiles and satisfies the interface, but the split convention confuses readers grepping for the port type.

**Remediation:** Replace `*sql.Tx` with `sharedPorts.Tx` in the two outliers.

**Effort:** ~2 minutes.

---

### C29 — `custody_retention_worker.go` Imports `shared/adapters/custody` Directly

**Severity:** 🟢 LOW
**Location:** `internal/discovery/services/worker/custody_retention_worker.go:28, 441`

`custody_retention_worker.go` imports `internal/shared/adapters/custody.BuildObjectKey` directly. The `worker-no-adapters` depguard rule denies postgres/redis/rabbitmq imports but not `shared/adapters/custody`, so this passes the linter. Nevertheless it's a worker-to-adapter-helper coupling that violates the spirit of hex boundaries.

**Remediation options:**
- Expose `BuildObjectKey` behind a `sharedPorts.CustodyKeyBuilder` interface
- Extend `worker-no-adapters` depguard rule to deny `internal/shared/adapters/custody`

**Effort:** ~30 minutes for port extension; ~5 minutes for lint rule tightening.

---

### Slice-Local LOW Findings (Abbreviated)

The following LOW-severity findings were surfaced by individual reviewers and are documented in the full transcripts at `/private/tmp/claude-501/.../tasks/`. Summarized here:

**Slice 1 (fetcher-transport-client):**
- `%v` error wrapping at `client_transport.go:163` violates forbidigo (uses `//nolint:errorlint` suppression inconsistently)
- `validateFetcherResultPath` sentinel `ErrFetcherResultPathTraversal` is misleading for unclean-but-not-traversal paths
- Duplicate compile-time interface check in `client.go:32` AND `client_test.go:17`
- `SetM2MProvider(nil)` asymmetry vs `WithM2MProvider` functional option
- `IsHealthy` bypasses auth+retry+breaker without doc comment
- Pagination overflow message phrasing
- Dead `_ = r` in `client_connections_pagination_test.go:269`
- Three duplicated normalize-extraction-status tests across two files
- `normalizeExtractionStatus` has no-op cases for statusSubmitted/Extracting that should be removed
- 401 retry on POST undocumented (works in practice; comment needed)
- HMAC IV not in HMAC input (GCM authenticates IV internally; purist critique only)
- Minor URL message clarity in pagination overflow

**Slice 2 (t001):**
- `span` parameter in `runTrustedStreamPipeline` could be renamed `parentSpan`
- `capReader.Read` peeked byte intentionally discarded — comment recommended
- `canonicalExtractionIDFromMetadata` silent fail-open on malformed UUID should log WARN
- `DefaultMaxExtractionBytes=2 GiB` too generous for small pods; consider 256 MiB default
- `findExistingTrustedStreamJob` TransactionCount uses prior job's persisted rows (not current content) — document on short-circuit
- Handler state parsing case-insensitive; not end-to-end tested
- SourceMetadata `custody_key`/`custody_sha256`/`fetcher_job_id` plumbed into ingestion but silently ignored — latent; future logging could leak
- `ErrBridgeFailureClassRequired` docstring mixed-style
- `ParseBridgeErrorClass` return style slightly non-idiomatic
- Default branch in `BridgeRetryPolicy.String()` returns `unknownRetentionBucket` — intentional but cosmetic drift

**Slice 3 (t002):**
- HKDF salt=nil warning in docstring should emphasize "MUST BE NIL — Fetcher contract lock"
- `isNilArtifactHTTPClient` uses reflect — cheap at construction time; comment misleading about "per-request"
- `Store` method is 87 lines; consider extracting `handleReplay` helper
- `counterWriter` could be a package-private helper with explicit Write-never-errors contract
- `VerifyAndDecrypt` uses `_, span` discarding span-enriched ctx (downstream helpers don't take ctx)
- Error messages don't leak key bytes (positive observation, documented as "well done")
- `ArtifactCustodyReference.StoredAt` on replay returns current wall clock — document or zero-value

**Slice 4 (t003):**
- Duplicate tests `TestLogMappedFieldsDivergence_*` across `extraction_helper_functions_test.go` + `extraction_support_test.go`
- `extraction_helper_functions_test.go` naming doesn't match Ring test convention (no matching source)
- `sortedKeys`/`sortedTableKeys` could use `slices.Sorted(maps.Keys(m))` (Go 1.23+)
- `decodeRows` could pre-allocate `rows := make([]json.RawMessage, 0, 1024)`
- `extraction_link_fuzz_test.go` seed routing could rotate mock outcome branches more deterministically
- Port-level files `bridge_orchestrator_test.go` + `fetcher_bridge_test.go` contain only self-comparability canaries
- `TestFetcherBridgeAdaptersPairingCanary` is empty stub
- `stubExtractionRepo` variants in 3 test files duplicate interface implementation
- `TestBridgeWorker_Stop_ConcurrentStopsReturnExactlyOneNil` uses `time.Sleep(50ms)` — flake risk on loaded CI
- `%v` vs `%w` wrap at `handlers_bridge_readiness.go` (error message)

**Slice 5 (t004):**
- Handler cursor validation order (uuid.Parse before CreatedAt.IsZero)
- Use-case method ordering (staleThreshold < 0 check before uc == nil)
- Docstring drift: "four-way" vs "five-way" in `extraction_readiness_queries.go:33`
- Test name `TestGetBridgeReadinessSummary_ReturnsAllFourBuckets` claims "four" but asserts 5
- DTO doc comment references "four-bucket partition" inconsistently with five-way prose elsewhere
- Dead branches in `ListBridgeCandidates` handler for query sentinels (handler pre-validates)
- 341-line `handlers_bridge_readiness.go` slightly over Ring 300-line soft cap
- `resolveStaleThreshold` sub-unit clamping silent
- Handler staleness closure race theoretical (writes post-bootstrap)

**Slice 6 (t005):**
- `persistTerminalFailure` logs class twice under `class` + `bridge.class` keys (copy-paste)
- Message prefix inconsistency: `terminalFailureMessage` vs `escalated to terminal after N attempts` format
- `terminalFailureMessage(nil)` → `"unknown failure"` masks wiring bug
- `BridgeLastErrorMessage` not sanitized for control bytes (CR/LF/NUL)
- `BridgeRetryBackoff` has no max-elapsed-time semantics (documentation gap)
- `EscalateAfterMaxAttempts` only special-cases `ErrBridgeSourceUnresolvable` (class-level info loss for other sentinels)
- `bridge_retry_backoff.go:53` constant placement shared with classifier unusual
- Misleading nil-receiver sentinel on `Start()` for custody retention
- `Done()` method lacks nil-receiver guard (sibling pattern consistency)

**Slice 7 (t006):**
- `CustodyRetentionDefaultBatchSize()` is a getter for a package-private const (could be an exported const)
- `CustodyRetentionWorker.Start(nil)` misleading sentinel (upgraded to C20)
- `Done()` lacks nil-receiver guard (shared with C20)
- `sweepCycle` uses `context.Background()` in Stop() log calls (lose tracing ctx)
- Test stub `observedTenants` field written but never asserted
- `contains()` helper duplicates `slices.Contains` from stdlib
- Stub thread-safety inconsistency (`stubRetentionExtractionRepo.FindBridgeRetentionCandidates` takes lock twice)
- `TestMarkCustodyDeleted_CoercesToUTC` tautological assertion on final line
- `TestCustodyRetentionWorker_MarkerFailureIsNonFatal` doesn't verify mark was attempted
- `retentionBucket` priority when both `IngestionJobID` and `BridgeLastError` set untested
- Redis lock acquire error wrapping loses context on repeated calls

**Slice 8 (infra):**
- Chaos test Redis-unavailable gap documented as t.Skip stub (honest but CI shows PASS)
- Magic number `expectedTotalKeys = 142` — structural cross-check could complement
- `TestListBridgeCandidates_FullPage_EmitsNextCursor` uses `_ int` for limit arg (not validated)
- Cursor CreatedAt assertion missing (only ID checked)
- Handler `staleness` field write unprotected (called once at bootstrap; safe today)
- 4 locations hold "1 hour" default (guardrail comments present; shared constant would prevent drift)
- `classifyResponse` passes 1xx as success (theoretical; Go transport hides 1xx)
- `newArtifactHTTPClient` dereferences cfg without nil guard (caller guards)
- `describeBridgeWiring` dereferences bundle without nil guard (caller guards)

---

## 8. Prioritized Remediation Plan

### Tier 1 — Quick Wins (≤45 minutes, ship as single polish commit)

| Order | ID | Action | Effort |
|:-----:|:--:|--------|:------:|
| 1 | C1 | `%worker` → `%w` × 6 sites + forbidigo rule | 5min |
| 2 | C2 | Add `http.StatusTooManyRequests` to transient case + flip 2 tests | 15min |
| 3 | C6 | Wrap `recoverDigest` Download with `io.LimitReader` + shared constant | 5min |
| 4 | C17 | Add `BridgeRetryMaxAttempts` to `snapshotToWorkerConfig()` + regression test | 10min |
| 5 | C20 | Introduce `ErrCustodyRetentionWorkerNil` sentinel + test | 5min |
| 6 | C21 | Add `AND ingestion_job_id IS NULL` to `executeMarkBridgeFailed` + sentinel handling + test | 15min |
| 7 | C23 | Add `bundle.ArtifactCustody == nil` check to `EnsureBridgeOperational` | 5min |
| 8 | C30 | Hard-fail on missing bundle/provider/extractionRepo when `FETCHER_ENABLED=true` | 10min |

**Closes:** 3 CRITICAL + 5 HIGH. Net: **8 of 14 blockers resolved in under an hour.**

### Tier 2 — Same-Day Mechanical (~2.5 hours)

| Order | ID | Action | Effort |
|:-----:|:--:|--------|:------:|
| 9 | C4 | Switch readiness queries to `WithTenantReadQuery` (replica-first) | 30min |
| 10 | C8 | Split `bridge_worker.go` + 3 test files per Ring file-size rule | 60min |
| 11 | C14 | Add `AND bridge_last_error IS NULL` to `ready_count` FILTER (defense-in-depth after C21) | 15min |
| 12 | C25 | Nil-receiver guards on 6 `Fetcher*` accessors in `config_env.go` | 10min |
| 13 | C26 | Consolidate `discoveryExtractionRepo.NewRepository(provider)` into single instance | 10min |
| 14 | C31 | Switch migrations 000026/000027 to `CREATE INDEX CONCURRENTLY` (OR follow-up migration if deployed) | 10-30min |

**Closes:** 1 additional HIGH + 5 MEDIUM. Net: **14 of 14 mechanical blockers resolved in ~3.5 hours total.**

### Tier 3 — Coordination Required (schedule team syncs)

| ID | Decision Required | Owner | Timeline |
|:--:|-------------------|:-----:|:--------:|
| C3 | HMAC contract resolution (A/B/C from P6 memo) — blocks real-Fetcher integration | Matcher + Fetcher teams | Cross-team meeting this week |
| C7 | Custody write-once posture: S3 conditional upload vs document lock reliance | Fetcher-bridge team | Decision ≤1 day, impl 30min-4hr depending on option |
| C15 | Worker-liveness signal: deliver OR rename "truthful" claim | Product + engineering | Product decision ≤1 day |
| C22 | Immutable vs idempotent `MarkBridgeFailed` reason — document OR guard | Fetcher-bridge team | ≤30 min decision + 10 min code |

### Tier 4 — Deferrable (next slice or backlog)

- C5 — P1 streaming plaintext (already on T-002 preconditions backlog)
- C9 — Redundant `FindByID` in link writer
- C10 — Eligibility partial index predicate drift
- C11 — Drilldown JSONB hydration waste
- C12 — Missing security-parser fuzz
- C13 — `sweepCycle` + default-tenant untested
- C18 — Unread adapter fields
- C19 — HTTP DTO bridge/custody field omission
- C24 — FETCHER_MATCHER.md staleness
- C27 — E2E journey for bridge endpoints
- C28 — Test stub type consistency
- C29 — Worker-to-adapter import drift

All are quality-of-life polish; none affect correctness at current deployment scale.

---

## 9. What Was Done Well

The review identified significant architectural strengths worth preserving and emulating:

**Invariants and correctness:**
- T-001 Gate 8 convergent precondition fully satisfied — atomic `LinkIfUnlinked` + state-machine `LinkToIngestion` domain method + orphan-prevention short-circuit, triangulated clean across 5 reviewers
- Two-state-machine discipline (upstream discovery `Status` vs bridge `BridgeLastError`) documented inline on the entity with clear separation
- Three-layer orphan prevention: SQL `status='COMPLETED'` filter + atomic INSERT stamp (Polish Fix 4) + canonical UUID lowercase (Polish Fix 7)
- Convergence marker design (migration 000027 + `MarkCustodyDeleted`): provably bounded O(orphans-per-cycle), documented in both code and SQL

**Crypto and security:**
- HKDF context strings locked to Fetcher contract with explicit "DO NOT ROTATE" docstrings at `artifact_verifier.go:30` and `shared/ports/fetcher.go:81, 98`
- `hmac.Equal` constant-time compare with explicit comment warning against `bytes.Equal`
- `BuildObjectKey` rejects control bytes + path separators in tenant IDs (prevents NUL-smuggling + SigV2 header injection)
- SSRF defense inherited by artifact downloads via `BuildArtifactTransport` (DNS-resolve-then-dial + private-IP blocklist + redirect rejection)
- AES-GCM `gcm.NonceSize()` validated against IV length before `gcm.Open`
- Explicit size cap enforced at two layers (Content-Length pre-check + `LimitReader` at read time)
- AES-GCM open failures deliberately return bare sentinel without embedding underlying cause (prevents learning which crypto stage caught corruption)
- `EnsureBridgeOperational` hard-fail on `FETCHER_ENABLED ∧ missing APP_ENC_KEY` (P4 precondition met)

**Concurrency and race-safety:**
- Passive backoff via `updated_at ASC` reordering eliminates dual-clock bug class entirely (Polish Fix 2)
- Atomic narrow UPDATE for `IncrementBridgeAttempts` (Polish Fix 3) with `WHERE ingestion_job_id IS NULL` guard
- Distributed Redis lock (SetNX) + atomic SQL UPDATE for defense-in-depth
- Lua-script atomic lock release with UUID token comparison (prevents releasing another owner's lock)
- `Stop()` uses `CompareAndSwap` to eliminate TOCTOU window
- `MarkCustodyDeleted` intentionally doesn't bump `updated_at` (prevents re-entering retention sweep)

**Testing discipline:**
- Exemplary TDD coverage on crypto/state-machine surfaces: fuzz + property + integration tests with independent re-implementation (HKDF re-derived via `golang.org/x/crypto/hkdf` directly in property tests to avoid rubber-stamping)
- 200-iteration × 3-flip-sites tamper detection property tests (600 effective bit-flip cases)
- Hamming-distance assertion in HKDF context-separation property test (rejects weak PRFs)
- Cross-verification in `KeyMaterial_Separation` property (master-A-signed artifacts cannot be verified with master-B)
- Every state transition tested (valid AND invalid) for entity state machine
- Totality test for retry classifier (enumerates every sentinel, asserts each produces recognized policy)
- Exact SQL shape pinned via `regexp.QuoteMeta` in sqlmock tests
- Boundary testing: exact-limit, +1 byte, -1 byte for size caps
- Integration test with real Postgres + Redis + MinIO + httptest Fetcher impersonator

**Mock/test infrastructure:**
- Mock regeneration discipline: all 8 `JobRepository` + 10 `ExtractionRepository` implementers updated in lockstep
- Compile-time interface checks (`var _ = (*Impl)(nil)`) throughout cross adapters
- Pipeline reuse in T-001: `runTrustedStreamPipeline` shares helpers with `StartIngestion`, regression test guards no drift
- Build tags consistent: `unit`, `integration`, `e2e`, `chaos` tags correctly applied

**Config and ops:**
- Bounded validators prevent operator footguns (e.g., `min/maxFetcherMaxExtractionBytes [1 MiB, 16 GiB]`, `min/maxBridgeIntervalSec [5, 3600]`)
- Explicit separation of bootstrap-only vs runtime config with documented rationale
- `ApplyBootstrapOnly` for HKDF-derived-key cache (APP_ENC_KEY) with preservation across snapshot reloads
- TestConfigMapExample_SyncWithNonHotReloadEnvVars guard enforces the "hot-reloadable keys stay OUT of .env" rule

**Migration and schema:**
- Index predicates match query predicates exactly for the two highest-traffic paths (custody retention sweep, source resolver)
- Semicolon-strip fix (commit 047da45) with excellent root-cause analysis in commit message — unblocked 584 failing integration tests
- Migration 000021 down guard hard-fails on incompatible data (prevents silent provenance corruption)
- All 27 migrations paired, reversible, sequentially ordered
- Migration 000026/000027 DOWN scripts drop columns only after dropping dependent indexes (correct order)

**Documentation hygiene:**
- Comments explain "why" not just "what" throughout Polish Fix 1-7 comments
- Docstrings cite past gate reviews (e.g., "Polish Fix 3 — fixes T-005 livelock flagged by gate 8")
- Entity docstrings document the two-state-machine invariant explicitly
- Migration header comments explain index intent + predicate rationale

**Cross-context isolation:**
- Discovery never imports ingestion directly — bridge orchestrator consumes `sharedPorts.FetcherBridgeIntake` + `sharedPorts.ExtractionLifecycleLinkWriter` via shared kernel
- Type-alias pattern for shared kernel migrations maintains backward compatibility
- Depguard rules enforce architectural boundaries in CI

---

## 10. Review Methodology & Insights

### Approach

The review used the **Ring codereview skill** protocol:

1. **Pre-analysis pipeline** (Mithril) — static analysis, AST extraction, call-graph analysis, data-flow analysis producing 5 reviewer-specific context files
2. **Review slicing** — an adaptive cohesion-based slicer (`ring:review-slicer`) evaluated the 194-file PR and decomposed it into 8 thematic slices aligned to the T-001..T-006 task boundaries plus fetcher-transport foundation + cross-cutting infrastructure
3. **Parallel reviewer dispatch** — 8 specialized reviewer agents × 8 slices = **64 reviewer passes** (later growing to 65 as a straggler arrived post-consolidation)
4. **Cross-slice merge + deduplication** — findings aggregated, exact-match deduplicated, fuzzy-matched for cross-cutting concerns, tagged by triangulation count
5. **Consolidation report** (this document)

**Wave structure:**
- Wave 1 (dispatched together): 4 slices × 8 reviewers = 32 agents covering fetcher-transport, t001-intake, t002-custody, t003-state-machine
- Wave 2 (dispatched together): 4 slices × 8 reviewers = 32 agents covering t004-readiness, t005-retry, t006-retention, infra-wiring
- Stragglers arrived over ~4 hours post-Wave-2 dispatch

**Review total:** ~10 hours wall-clock, ~450 minutes aggregate reviewer-minutes, 65 independent threat-model analyses.

### Key Insights from Orchestrating 65 Reviewers

**1. Convergence count is a poor severity proxy.**

The `%worker` typo (C1) was flagged by 10 reviewers and took 5 minutes to fix. The HMAC contract divergence (C3) was flagged by 1 reviewer (business-logic, via mental execution against the Fetcher contract) and blocks every real-Fetcher integration. The partition overlap (C14) and its root cause (C21) were each flagged by 1 reviewer on different slices; together they break a foundational correctness invariant.

The lesson: **convergence signals reality but not importance.** A single-angle finding from the right lens (business-logic mental execution, security adversarial thinking, consequences operator-visibility analysis) can be more load-bearing than a 10-angle consensus on a format-string nit. The job of consolidation is not to count votes — it's to **understand which lens catches which defect class** and weight findings accordingly.

**2. Cross-slice merge surfaces invisible structural defects.**

C14 (partition symptom, t004-biz) and C21 (write-layer race cause, t005-biz) were found by different business-logic reviewers on different slices. Neither could see the whole picture. The merge step IS the review. Without it, you have 8 disconnected opinions per slice and zero detection of structural bugs that cross slice boundaries.

**3. Consequences reviewers triangulate operator-visible contracts.**

C17 started as a 1-angle dead-code finding ("field missing from snapshot") → promoted to 2-angle when code-infra also flagged it ("SQL hydration gap") → promoted to HIGH when consequences-infra described the operator-visible lie ("PUT succeeds, audit log succeeds, worker never restarts"). Same underlying symbol, three escalating severities depending on which angle was looking.

The final severity comes from asking "**what does a real operator experience?**" — that's consequences-reviewer work, and no other reviewer type does it systematically.

**4. "All agents completed" is a distributed-consensus problem.**

Over the course of the review, I declared "64/64 complete" three times prematurely — first when actually 57 were done, then 60, then 63, then 64 (and the 65th straggler arrived after that). Background agent completions arrive asynchronously via notifications that look identical to each other; there's no authoritative "all done" signal.

**For future review orchestrations:** treat "done" as a sliding tolerance. After ~90% of expected agents report, the marginal value of additional findings drops sharply. After ~95%, deliver the consolidation and let stragglers arrive as addenda. Never claim completeness with certainty — always phrase it as "current state, final unless updated." The review process is always a moving window.

**5. Parallel orchestration isn't just about speed.**

The 65-agent dispatch took 10 hours wall-clock vs an estimated 30-40 hours for a single deep reviewer to achieve comparable coverage. But more importantly, it produced correlations like C14↔C21 (separate slices, separate reviewers, combined structural finding) that **no serial review could find.** When reviewers work in isolation on their own scope, they can't detect cross-scope interactions. The parallel dispatch forces those interactions to be discovered at the merge step.

**6. File-size enforcement is a leading indicator of architectural debt.**

The 4 files exceeding 500 lines (C8) all emerged from cohesive functional choices earlier in the slice's evolution (bridge_worker.go initially ~750, integration test initially ~1200). Each file's growth was locally justifiable, but the aggregate breaks a Ring standard with real operational consequences (slower code review, harder refactor safety, obscured responsibilities). **Ring's file-size-enforcement rule correctly turns this into a blocker** because it's one of the few mechanical proxies that catches "this file grew too cohesive."

---

## 11. Handoff Artifacts

### Output Files

- **This report:** `REVIEW.md` (project root) — represents Ring `codereview` Step 3-S-Merge + Step 8 completion
- **Mithril pre-analysis:** `docs/codereview/`
  - `context-code-reviewer.md`
  - `context-business-logic-reviewer.md`
  - `context-security-reviewer.md`
  - `context-test-reviewer.md`
  - `context-nil-safety-reviewer.md`
  - `go-ast.json`, `mixed-ast.json`, `go-calls.json`, `go-flow.json`
  - `security-summary.md`
  - `scope.json`
  - `static-analysis.json`
- **65 individual reviewer transcripts:** `/private/tmp/claude-501/-Users-fredamaral-repos-lerianstudio-matcher/f819bd6b-e4ae-4114-a4e3-71a8f12cd92f/tasks/`

### Review Totals

| Metric | Count |
|--------|------:|
| Files changed | 194 |
| Lines added | 35,631 |
| Lines deleted | 2,422 |
| Commits reviewed | 9 |
| Reviewer agents dispatched | 65 |
| Reviewer angles | 8 (code, business-logic, security, test, nil-safety, consequences, dead-code, performance) |
| Thematic slices | 8 |
| Dispatch waves | 2 + addenda |
| Wall-clock duration | ~10 hours |
| Total findings | ~74 |
| CRITICAL blockers | 3 |
| HIGH blockers | 6 |
| MEDIUM blockers | 5 |
| LOW findings (follow-up) | ~50 |
| Cross-cutting concerns (C1-C31) | 31 |
| Blocking items | 14 |
| Coordination decisions required | 4 |

### Estimated Remediation Effort

- **Tier 1 (mechanical, quick wins):** ~70 minutes total, closes 8 of 14 blockers
- **Tier 2 (mechanical, same-day):** ~2.5 hours, closes remaining 6 blockers
- **Tier 3 (coordination):** 4 decisions + ~1 day implementation
- **Total mechanical work:** ~3.5 hours

### Recommended Next Steps

1. **Immediately (before any merge):** Address Tier 1 items as a single polish commit. Takes under an hour and resolves 3 CRITICAL + 5 HIGH blockers.
2. **Within 24 hours:** Schedule Tier 3 decision meetings (C3 HMAC with Fetcher team, C7 custody posture, C15 product liveness, C22 message contract). These gate subsequent implementation.
3. **Within same sprint:** Address Tier 2 mechanical fixes. These are safe refactors that don't require coordination.
4. **Backlog:** Track Tier 4 findings (deferrable) as follow-up polish items. None block production deployment at current scale.

### Version Info

- Review skill version: Ring codereview 1.23.0
- Go version: 1.26.0
- Project: matcher (Transaction reconciliation engine for Lerian Studio)
- License: Elastic License 2.0
- Review conducted: 2026-04-17

---

**End of Review Report**

Produced by Ring codereview skill orchestration.
For questions or follow-up, consult individual reviewer transcripts or contact the Fetcher-bridge feature team.
