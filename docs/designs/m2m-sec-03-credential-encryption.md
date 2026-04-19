# M2M-SEC-03: Encrypted M2M Credential L2 Cache

**Status:** Design — not yet implemented
**Owner:** Platform Security
**Target branch:** `feat/lib-commons-v5`
**Integration point:** `internal/shared/adapters/m2m/provider.go:288` (`TODO(SECURITY: M2M-SEC-03)` in `storeInRedis`)
**Related prior art:** [`project_fetcher_bridge_decisions.md`](../../../../.claude/projects/-Users-fredamaral-repos-lerianstudio-matcher/memory/project_fetcher_bridge_decisions.md) (D4 — HKDF from env master key, versioned context strings)

---

## 1. Threat Model

### In scope — attacks this defeats

| Attack | Today (plaintext L2) | With AES-256-GCM + HKDF |
|---|---|---|
| **Redis snapshot / RDB dump leak** (backup stolen, disk image exfil) | `clientSecret` recovered verbatim from JSON | Attacker gets ciphertext; needs `SYSTEMPLANE_SECRET_MASTER_KEY` to recover plaintext |
| **Shared-Redis multi-tenant leak** (other services on same Valkey cluster reading our keys) | Plaintext visible via `KEYS` + `GET` | Ciphertext only; other services lack our HKDF context |
| **L2 misconfiguration** (Redis auth disabled in dev bled into stage, or `requirepass` missing) | Any network peer reads creds | Ciphertext only |
| **Replication lag observer** (read replica or eavesdropper on the sentinel replication stream) | Plaintext on the wire (if TLS absent) | Ciphertext on the wire |
| **In-memory scrape of `redis-server` process** | `clientSecret` in Redis value arena | Ciphertext in value arena; key material lives only in the Go process |

### Out of scope — attacks this does NOT defeat

- **In-memory compromise of the Matcher Go process after decrypt** — L1 cache (`sync.Map` at [`provider.go:80`](../../internal/shared/adapters/m2m/provider.go#L80)) holds plaintext `*ports.M2MCredentials`. A core dump, `/proc/pid/mem` read, or debugger attach leaks creds. Mitigation is OS-level hardening, not crypto.
- **Compromised master key** — If `SYSTEMPLANE_SECRET_MASTER_KEY` leaks (env var exfil, systemplane DB dump, CI secret leak), the HKDF output is derivable and all ciphertexts are readable. Master-key rotation (see §3) is the only mitigation.
- **Application-layer exfiltration** — A handler that logs `creds.ClientSecret` (violating the `json:"-"` tags at [`ports/m2m.go:9-10`](../../internal/shared/ports/m2m.go#L9)) bypasses this layer entirely. Static analysis + code review own this.
- **Authoritative store compromise (AWS Secrets Manager)** — Out of scope; this encrypts the *cache*, not the source of truth.
- **Timing side channels against AES-GCM** — Go stdlib uses constant-time AES on amd64/arm64 with AES-NI; acceptable residual risk.

---

## 2. Cryptographic Design

### 2.1 Algorithm: AES-256-GCM

**Choice:** `crypto/cipher.NewGCM(cipher.Block)` over a 32-byte AES key.

**Justification:**
- **AEAD** — authenticates ciphertext, so tampered bytes fail closed (which maps cleanly to "treat as cache miss and refetch"). No manual MAC construction, no encrypt-then-MAC pitfalls.
- **Standard library only** — `crypto/aes`, `crypto/cipher`, `crypto/rand`, `golang.org/x/crypto/hkdf`. No third-party crypto dependencies to audit or pin.
- **Constant-time on target platforms** — AES-NI on amd64 and ARMv8 crypto extensions; Go runtime auto-selects.
- **Consistent with existing Matcher crypto surface** — the fetcher-bridge work (D4, memory note above) also chose AES-256-GCM + HKDF, so operators learn one pattern.

Alternatives rejected:
- **ChaCha20-Poly1305** — equally safe but diverges from the fetcher-bridge precedent. No performance win at ~256-byte payloads.
- **Fernet / NaCl secretbox** — external dependency; Fernet mandates AES-128-CBC + HMAC (not AEAD-native).

### 2.2 Key Derivation: HKDF-SHA256 from `SYSTEMPLANE_SECRET_MASTER_KEY`

Match the fetcher-bridge precedent exactly: raw master key in env → local HKDF → purpose-specific subkey.

**Input keying material (IKM):** base64-decoded bytes of `SYSTEMPLANE_SECRET_MASTER_KEY` (already 32 raw bytes after decode — see the well-known dev key at [`systemplane_init.go:31`](../../internal/bootstrap/systemplane_init.go#L31), which is 44 base64 chars = 32 bytes).

**Salt:** empty (`nil`).
- Rationale: the master key is already high-entropy (32 random bytes from a CSPRNG); HKDF's salt exists to condition *low-entropy* inputs. `rfc5869` §3.1 allows empty salt and the construction remains secure when IKM is uniformly random.
- Not per-entry random: we want deterministic key derivation so any pod can decrypt any ciphertext without storing the salt alongside. Per-entry salt would buy nothing — the per-entry nonce already ensures unique ciphertexts.

**Info / context string:** `"matcher.m2m.cache.v1"` (ASCII, 20 bytes)
- `matcher.` — namespace, prevents collision with any other HKDF consumer in the codebase (fetcher-bridge uses `fetcher-external-*-v1`).
- `m2m.cache.` — purpose, domain-separates from any future systemplane-derived subkey.
- `.v1` — version suffix. **Critical for rotation** (§3). To bump algorithm, key size, or envelope layout, change the info string (`.v2`) — this derives a fully independent key from the same master.

**Derivation timing: once at construction, cached on the struct.**
- Per-operation derivation would cost ~5µs of HMAC-SHA256 per Set/Get — negligible, but hygienically pointless: the derived subkey has the same security boundary as the struct itself. If the struct is alive, the key is recoverable from memory either way.
- Chosen tradeoff: **derive once in `NewM2MCredentialProvider`, hold as a `[]byte` field on `*credentialCipher`**. On shutdown there is no explicit zeroing (Go's GC + no `mlock` means best-effort only); documented as accepted risk.

**Pseudocode (not an implementation — shape only):**
```
ikm, err := base64.StdEncoding.DecodeString(os.Getenv("SYSTEMPLANE_SECRET_MASTER_KEY"))
if err != nil {
    return nil, fmt.Errorf("decode master key: %w", err)
}
if len(ikm) != 32 {
    return nil, errCipherInvalidKey // IKM MUST be exactly 32 bytes after decode
}

reader := hkdf.New(sha256.New, ikm, nil /* salt */, []byte("matcher.m2m.cache.v1"))
key32  := make([]byte, 32)
if _, err := io.ReadFull(reader, key32); err != nil {
    return nil, fmt.Errorf("derive subkey: %w", err)
}
```

> **MUST:** Cipher construction MUST validate `len(ikm) == 32` after base64 decode; anything else returns `errCipherInvalidKey`. This prevents silent key truncation or padding that would weaken the derived subkey.

### 2.3 Nonce: 12-byte random per Set

- GCM standard nonce size (`aes.GCM.NonceSize() == 12`).
- Source: `crypto/rand.Read`.
- Collision math: 2⁴⁸ nonces before birthday collision becomes non-negligible. At a worst-case 10 Set/sec (tenant count × cache churn), that's 8.9M years. Non-issue.

> **MUST NOT:** Implementation MUST NOT fall back to a zero nonce on `rand.Read` failure. It MUST return an error and MUST NOT emit a cache entry or ciphertext envelope. Silently drop the L2 write (same "best effort" posture as the existing `_ = rds.Set(...)` at [`provider.go:295`](../../internal/shared/adapters/m2m/provider.go#L295)) and increment the `m2m_cache_encrypt_failures_total{reason="rand_read"}` counter.

Nonce reuse under AES-GCM catastrophically breaks confidentiality and authenticity (two ciphertexts under the same key+nonce leak plaintext XOR and allow forgery). Degrading to "plaintext in L2" by skipping encryption is preferable to writing a ciphertext that is cryptographically compromised — but the design chooses neither. It chooses to refuse to emit a cache entry at all. See §6.1 for the corresponding test requirement.

### 2.4 Ciphertext Envelope

Byte layout (fixed prefix + variable tail):

```
offset  size   field
------  ----   -----
 0       1     version byte (0x01 = v1)
 1      12     nonce (12 random bytes from crypto/rand)
13       N     ciphertext || auth_tag
               (N = plaintext_len + 16; GCM tag is 16 bytes, appended by Seal)
```

Total overhead: **29 bytes** (1 version + 12 nonce + 16 tag) over plaintext length.

**Encoding for Redis:** base64-standard (`base64.StdEncoding`) of the full envelope, stored as a Redis string value.
- Rationale: keeps the value human-inspectable in `redis-cli` for ops triage (prefix `AQ...` → v1). Redis binary-safe strings would work too; base64 chosen for debuggability at a 33% size cost on ~256-byte payloads (acceptable).

**Why a version byte instead of relying on the info string alone:**
- Info-string versioning (`.v1` → `.v2`) derives a different *key* — required for true rotation.
- The envelope version byte tells the **decrypt** path which key/layout to attempt first, so we can coexist with in-flight v1 entries during a v2 rollout.
- Without the version byte, every decrypt would have to try v2 then fall back to v1 — wasteful and racy.

### 2.5 Plaintext: canonical JSON of `redisCredentials`

The plaintext input to `Seal` is the existing `redisCredentials` JSON ([`provider.go:62-65`](../../internal/shared/adapters/m2m/provider.go#L62)):
```json
{"clientId":"...","clientSecret":"..."}
```
No change to the serialization model — encryption wraps the existing bytes. This keeps the L1 path, the `InvalidateCredentials` flow, and the struct-level `json:"-"` tags entirely unaffected.

---

## 3. Key Rotation Strategy

Two independent rotation axes, both supported by the envelope version byte.

### 3.1 Algorithm / layout rotation (v1 → v2)

1. Ship code that knows how to **decrypt** both v1 and v2 envelopes (version byte dispatch).
2. Flip a build-time or systemplane-driven constant `currentCipherVersion = 2` so new Sets use v2.
3. v1 entries expire naturally via the existing L2 TTL (5 min default, [`provider.go:30`](../../internal/shared/adapters/m2m/provider.go#L30)) — no migration worker needed.
4. After one TTL window, remove v1 decrypt code.

### 3.2 Master key rotation

1. Provision `SYSTEMPLANE_SECRET_MASTER_KEY_NEXT` alongside the current key.
2. Bump info string to `"matcher.m2m.cache.v2"` — derives a fresh subkey from the new master.
3. New Sets encrypt under v2 key; v1 entries fail to decrypt under v2 key and are treated as cache miss (§3.3 below).
4. After one TTL window, remove `SYSTEMPLANE_SECRET_MASTER_KEY_OLD`.

No dual-write / dual-read complexity needed — the TTL is the migration window.

### 3.3 Decrypt failure semantics: **fail-open-as-miss**

On any decrypt error (bad version byte, GCM auth-tag failure, truncated envelope, wrong key, base64 decode failure):
- Treat the Redis entry as a **cache miss** (return `(nil, false)` from `getFromRedis`).
- The outer `GetCredentials` flow falls through to AWS Secrets Manager — the authoritative source. See [`provider.go:168-181`](../../internal/shared/adapters/m2m/provider.go#L168).
- **Log at WARN**, not ERROR, with `version_byte`, `tenant_org_id`, and error category (`auth_tag`, `layout`, `base64`). Auth-tag failures could indicate tampering *or* routine rotation; ops correlates with deploy timeline.
- Increment `m2m_cache_decrypt_failures_total{reason="auth_tag|layout|base64"}` counter.
- **Do NOT** delete the Redis key on decrypt failure — let TTL handle it. Deleting risks a cascading failure if a bug causes all decrypts to fail.

This is a deliberate choice: the cache is an optimization. A decrypt error MUST NOT block credential retrieval.

### 3.4 Rotation axes are INDEPENDENT — MUST NOT combine

The two rotation mechanisms above (§3.1 envelope version byte, §3.2 info-string version) are **deliberately orthogonal** and address different threats:

| Axis | What it changes | Why you rotate it | Migration window |
|---|---|---|---|
| **§3.1 Envelope version byte** (`0x01` → `0x02`) | Algorithm, key size, nonce layout, tag placement | Algorithmic break (hypothetical AES-GCM weakness), larger tag, post-quantum migration | One L2 TTL (5 min) |
| **§3.2 Info-string version** (`.v1` → `.v2`) | HKDF-derived subkey (same master, different domain separator) — OR a fresh master entirely | Master-key rotation, HKDF context hygiene after a scare | One L2 TTL (5 min) |

> **MUST NOT:** Operators MUST NOT rotate both axes simultaneously. Doing so makes it impossible to distinguish a layout-decode failure from an auth-tag failure during the migration window, because every v1 entry will fail for both reasons at once. The `m2m_cache_decrypt_failures_total` counter labels (`reason="layout|auth_tag|base64"`) become uninformative, and rollback is ambiguous (which axis to revert first?).
>
> **MUST (ordering):** When both axes need to change, rotate them **sequentially**, separated by at least `2 × credCacheTTL` (default 10 min) to ensure all in-flight entries under the previous configuration have drained via natural TTL expiry before the next change begins. Typical order: algorithm first (§3.1) — because it's the newer, less-tested code path — then master key (§3.2). This lets the envelope-version dispatch validate the new layout under a known-good key before the key itself moves.

The independence also means **either axis can be rotated alone**: changing the envelope layout does not force a master-key rotation, and rotating the master does not force an algorithm change. This is deliberate — coupling them would turn every crypto update into a key-management event and vice versa.

---

## 4. API Shape

### 4.1 Location: sibling file `internal/shared/adapters/m2m/cipher.go`

**Not** a free-standing `pkg/crypto/` package. Justification:
- The cipher is inseparable from the m2m cache envelope format — no other caller benefits from exposing it.
- Placing it in `pkg/` would invite reuse with different info strings, defeating the purpose-binding that HKDF provides.
- Matcher convention is to keep narrow helpers co-located with their sole consumer until a second consumer materializes (see `pkg/chanutil`, `pkg/storageopt` — both earned promotion through actual reuse).
- If fetcher-bridge and m2m both gain mature, stable cipher helpers, a later promotion to `pkg/crypto/aeadenvelope/` is a mechanical refactor.

### 4.2 Proposed shape (package-private type, constructor, two methods)

```go
// cipher.go (sketch — not implementation)

// credentialCipher wraps AES-256-GCM with a versioned envelope format,
// using a key derived once from SYSTEMPLANE_SECRET_MASTER_KEY via HKDF-SHA256.
type credentialCipher struct {
    aead    cipher.AEAD   // constructed from the HKDF-derived 32-byte key
    version byte          // current envelope version for new Seals
}

// newCredentialCipher derives the subkey from the master and constructs the AEAD.
// Returns a sentinel error if masterKey is empty or cannot be base64-decoded.
func newCredentialCipher(masterKey string) (*credentialCipher, error)

// Encrypt wraps plaintext in a v1 envelope: [version(1) | nonce(12) | ciphertext+tag].
// Returns the envelope bytes (caller base64-encodes before Redis Set).
func (c *credentialCipher) Encrypt(plaintext []byte) ([]byte, error)

// Decrypt validates the envelope version, extracts the nonce, and verifies+decrypts.
// On any failure (bad version, GCM auth failure, truncation), returns a typed error
// that the caller maps to "cache miss".
func (c *credentialCipher) Decrypt(envelope []byte) ([]byte, error)
```

**Typed errors** (package-private, for construction validation and the decrypt counter-label taxonomy in §3.3):
```go
var (
    errCipherInvalidKey       = errors.New("m2m cipher: master key must decode to exactly 32 bytes")
    errCipherVersionUnknown   = errors.New("m2m cipher: unknown envelope version")
    errCipherEnvelopeTooShort = errors.New("m2m cipher: envelope truncated")
    errCipherAuthFailed       = errors.New("m2m cipher: GCM auth tag verification failed")
)
```

### 4.3 Wiring changes in `M2MCredentialProvider`

- Add `cipher *credentialCipher` field to the struct at [`provider.go:73`](../../internal/shared/adapters/m2m/provider.go#L73).
- Extend `NewM2MCredentialProvider` signature with the master key (read from env at bootstrap, passed in — keeps the adapter free of `os.Getenv` calls, consistent with existing DI style).
- `storeInRedis` ([`provider.go:259-296`](../../internal/shared/adapters/m2m/provider.go#L259)): JSON-marshal → `cipher.Encrypt` → base64 → `rds.Set`.
- `getFromRedis` ([`provider.go:225-255`](../../internal/shared/adapters/m2m/provider.go#L225)): `rds.Get` → base64 decode → `cipher.Decrypt` → JSON-unmarshal. Any error on this path returns `(nil, false)`.
- `InvalidateCredentials` ([`provider.go:185-213`](../../internal/shared/adapters/m2m/provider.go#L185)): unchanged — deletes opaque key.

### 4.4 Bootstrap integration

A new validation helper next to `ValidateSystemplaneSecrets` at [`systemplane_init.go:107`](../../internal/bootstrap/systemplane_init.go#L107):

```go
// ValidateM2MCacheKey ensures the master key is present and usable for HKDF
// before M2MCredentialProvider tries to derive. Called from the same startup
// validation gate as ValidateSystemplaneSecrets.
func ValidateM2MCacheKey(envName string) error  // same contract as systemplane check
```

The master key check is already enforced at startup (lines 108-112 of `systemplane_init.go`) — m2m can piggyback on that validation without introducing a new env var. **Reusing the existing key** is the whole point of HKDF domain separation.

---

## 5. Performance

### 5.1 AES-GCM overhead on a ~256-byte payload

Target payload: `{"clientId":"<uuid-ish 36 chars>","clientSecret":"<opaque 128-256 chars>"}` → ~200-300 bytes.

Expected numbers (stdlib AES-NI, 2023-era x86 / M-series ARM):
- `Seal` on 256 bytes: **~500 ns** (≈500 MB/s steady-state; fixed ~400 ns AEAD setup dominates at this size).
- `Open` on 256 bytes: **~500 ns**.
- HKDF derivation: **one-time at startup**, ~5 µs. Not on the hot path.
- base64 encode/decode round trip: **~300 ns**.

Total added latency per L2 Set: **~1 µs**. Per L2 Get (decrypt + base64): **~1 µs**.

Contrast: Redis round-trip p50 on a co-located Valkey is 200-500 µs. Crypto overhead is **<0.5%** of the Redis call — invisible in the flame graph.

### 5.2 L1 still carries the hot path

The 30s L1 in-memory cache at [`provider.go:27`](../../internal/shared/adapters/m2m/provider.go#L27) holds plaintext `*ports.M2MCredentials`. For a tenant with steady traffic:
- First request: L1 miss → L2 miss → AWS → store-encrypted-in-L2 → store-plain-in-L1.
- Next 30s: L1 hit, zero crypto, zero Redis.
- After 30s: L1 expires → L2 hit → **one decrypt**, then L1 re-populated.
- Every 5 min (default L2 TTL): L2 expires → AWS call.

So for a tenant making 1000 req/s, we do **~33 decrypts per minute** (2/sec), not 1000. Negligible CPU.

### 5.3 Fail-open vs fail-closed on decrypt errors

**Chose fail-open-as-miss** (§3.3). Rationale:
- Cache is an optimization, not a source of truth. The authoritative path (AWS Secrets Manager) remains.
- A broken-decrypt storm (e.g. master-key rotation bug) degrades to elevated AWS-SM call volume, not an outage.
- Fail-closed would turn a crypto bug into a service outage — unacceptable blast radius for a caching layer.

Guardrail: if `m2m_cache_decrypt_failures_total{reason="auth_tag"}` exceeds a PromQL threshold (e.g. >10/min sustained), page the on-call. That indicates either tampering or a rotation error that needs human attention.

---

## 6. Test Plan

### 6.1 Unit (`cipher_test.go`, build tag `//go:build unit`)

- **Round-trip** — `Encrypt(plaintext)` → `Decrypt(out)` returns original bytes for empty, small (1 byte), typical (256 B), and large (64 KB) inputs.
- **Wrong-key detection** — build two cipher instances from different master keys; envelope from A must fail `Decrypt` on B with `errCipherAuthFailed`.
- **Tamper detection** — flip a single bit in each of (version byte, nonce, ciphertext, tag); each must error without panic and produce the correct typed error.
- **Version dispatch** — envelope with `version=0xFF` returns `errCipherVersionUnknown`; with `version=0x00` (reserved) same.
- **Truncation** — envelopes shorter than `1 + 12 + 16 = 29` bytes return `errCipherEnvelopeTooShort`; no panic on any `len` from 0 to 28.
- **Nonce uniqueness** — 10,000 `Encrypt` calls on the same plaintext produce 10,000 distinct envelopes (sanity check on `crypto/rand`).
- **`rand.Read` failure is fail-closed** — inject a failing `io.Reader` in place of `crypto/rand.Reader` (via a test-only constructor hook); assert `Encrypt` returns an error, emits **no** envelope (return value is zero-length or nil), the caller skips the Redis `Set` (no entry is written), and the `m2m_cache_encrypt_failures_total{reason="rand_read"}` counter is incremented by exactly 1.
- **Deterministic derivation** — two `newCredentialCipher(sameKey)` instances produce interoperable envelopes (encrypted by one, decrypted by the other).
- **Info-string domain separation** — hand-construct a cipher with info `"matcher.m2m.cache.v2"` (via test-only constructor); its envelope must **fail** to decrypt on a `v1` instance, even with the same master.

### 6.2 Integration (`provider_redis_test.go`, build tag `//go:build integration`)

- **Real Valkey via testcontainers** — spin up valkey image, construct `M2MCredentialProvider` with a real `*libRedis.Client`, exercise `GetCredentials` → `InvalidateCredentials` → `GetCredentials` and assert:
  - Redis `GET` on the raw key returns **base64-encoded ciphertext** (not plaintext JSON); assert first decoded byte == `0x01`.
  - After `InvalidateCredentials`, `EXISTS` returns 0.
- **TTL expiry** — set `credCacheTTL = 2s`, wait 3s, assert L2 repopulated from source on next `GetCredentials`.
- **Concurrent access** — 100 goroutines calling `GetCredentials` for the same tenant; assert exactly one AWS-SM call via mock `SecretsClient` counter (L1 dedup holds).
- **Cross-pod simulation** — two `M2MCredentialProvider` instances (simulating pods) sharing a Redis; provider A Set → provider B Get returns equal credentials.

### 6.3 Negative / failure-mode

- **Bootstrap rejects empty master key** — `newCredentialCipher("")` returns typed error; `NewM2MCredentialProvider` fails closed if bootstrap hadn't already validated.
- **Bootstrap rejects malformed base64** — non-base64 input returns typed error.
- **Redis returns legacy plaintext JSON** — simulate by writing raw JSON to Redis, then `GetCredentials`: expect decrypt failure → cache-miss fallthrough → AWS call → re-stored as ciphertext. Counter `m2m_cache_decrypt_failures_total{reason="layout"}` incremented by 1. (This doubles as the rollout compatibility test — see §7.)
- **Chaos test** — with Toxiproxy injecting corruption on the Redis response path, assert no panics, all requests eventually serve from AWS.

### 6.4 Coverage gate

Target: **≥90%** on `cipher.go` (small, pure — high coverage is cheap). The provider-integration paths inherit the existing 70% threshold.

---

## 7. Rollout

### 7.1 No feature flag

The cache is self-healing: encrypted-by-new-code Sets interleaved with plaintext-left-by-old-code Gets both resolve correctly because a decrypt failure is indistinguishable from a cache miss (§3.3). Within one `credCacheTTL` window (default 5 min) after deploy, all live entries are ciphertext.

A feature flag would add a code path where plaintext is re-written for N minutes, which is exactly the vulnerability we're closing. No.

### 7.2 Backwards compatibility during rollout

Timeline for a rolling deploy with old and new pods coexisting:

| Phase | Old pods Set | New pods Set | Old pods Get | New pods Get |
|---|---|---|---|---|
| T=0, deploy begins | plaintext JSON | ciphertext | plaintext → OK; ciphertext → JSON-unmarshal fails → cache miss → AWS call | plaintext → decrypt fails (not a valid v1 envelope) → cache miss → AWS call; ciphertext → decrypt OK |
| T=5 min (one TTL) | — | ciphertext | — | ciphertext only — all plaintext expired |
| T = rollout complete | — | ciphertext | — | ciphertext |

Old-pod `Get` on ciphertext: the JSON unmarshal at [`provider.go:242`](../../internal/shared/adapters/m2m/provider.go#L242) will fail on binary envelope bytes → returns `(nil, false)` → falls through to AWS. **Safe.**

Peak AWS-SM call rate during the rollout: bounded by tenant count × (1 / L1 TTL) = tenant count / 30s. For 100 tenants, that's ~200 calls/min for 5 minutes. Well within AWS-SM quotas.

### 7.3 Deployment gate

**MUST land before:**
- Any multi-region deploy (cross-region Redis replication widens exposure).
- Any deploy with a Redis cluster shared across services (e.g. co-tenancy with non-Matcher Lerian services).
- Any deploy that enables cross-AZ Redis replication without TLS on the replication channel.

**MAY defer for:**
- Single-region, dedicated-Valkey, internal-VPC-only deploys — current TTL + network isolation is adequate stopgap (the existing TODO comment at [`provider.go:290-294`](../../internal/shared/adapters/m2m/provider.go#L290) documents this mitigation).

Ownership: Platform Security sign-off required to close the TODO; reviewers include a threat-model re-read against the deploy target.

---

## 8. Open Questions

1. **Do we care about master-key zeroization in memory?** Go doesn't provide `mlock` or reliable zero-on-GC. Standard posture in Matcher today (see fetcher-bridge decisions) is to accept the residual risk. Confirm this is still acceptable for M2M credential subkey material.

2. **Single cipher instance per provider, or per-tenant subkey derivation?** Current design: one cipher for the whole service, tenant isolation comes from the Redis key prefix (`valkey.GetKeyContext` at [`provider.go:129`](../../internal/shared/adapters/m2m/provider.go#L129)). Alternative: derive a per-tenant subkey via HKDF info `"matcher.m2m.cache.v1.<tenantOrgID>"`. This would mean a compromised subkey only exposes one tenant. Cost: `GetCredentials` does one extra HMAC per call (~5 µs) and a per-tenant cipher cache. **Recommendation: defer to v2** unless Platform Security insists on it now — the key-prefix isolation plus the shared master-key compromise already being "game over" makes per-tenant subkeys a defense-in-depth luxury at v1.

3. **Metrics namespace.** Proposed `m2m_cache_decrypt_failures_total{reason=...}` — confirm this aligns with current matcher Prometheus conventions (snake_case, `_total` suffix for counters).

4. **Should `InvalidateCredentials` also increment a metric?** Useful for spotting 401-storms from the downstream Fetcher. Orthogonal to this design but natural to include in the same patch.

5. **Alertmanager page threshold for `reason="auth_tag"`.** Suggested >10/min sustained for 5 min. Needs ops review — the failure rate during a master-key rotation may legitimately spike briefly.

---

## Appendix A — Files touched by the eventual implementation

| File | Change |
|---|---|
| `internal/shared/adapters/m2m/cipher.go` | **new** — `credentialCipher` type, `Encrypt`/`Decrypt` |
| `internal/shared/adapters/m2m/cipher_test.go` | **new** — unit tests (§6.1) |
| `internal/shared/adapters/m2m/provider.go` | modify `NewM2MCredentialProvider` signature; wire cipher into `storeInRedis` / `getFromRedis`; remove TODO at line 288 |
| `internal/shared/adapters/m2m/provider_test.go` | update constructor call sites |
| `internal/shared/adapters/m2m/provider_redis_test.go` | **new** — integration tests (§6.2) |
| `internal/bootstrap/systemplane_init.go` | add `ValidateM2MCacheKey` (or extend existing validator) |
| `internal/bootstrap/` (wherever `M2MCredentialProvider` is eventually constructed — currently not wired) | pass master key from env into constructor |

No migration, no schema change, no config-map change (reuses `SYSTEMPLANE_SECRET_MASTER_KEY`).
