// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package custody contains the shared-kernel adapter that persists
// verified Fetcher artifacts under Matcher-owned, tenant-scoped storage.
//
// Keeping the implementation in internal/shared/adapters is deliberate:
// the custody store is written to by discovery (T-002) and read from by
// ingestion (T-003) once the bridge worker lands. Putting it in either
// context would violate the cross-context isolation rule enforced by
// depguard. The shared kernel is the designated bridge.
package custody

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// KeyPrefix is the top-level prefix inside the tenant namespace where
// verified artifacts live. Exported so operators and tests can assert on
// the convention without reaching into private constants.
const KeyPrefix = "fetcher-artifacts"

// URIScheme is prepended to the custody URI so the returned reference
// clearly identifies the custody store. The "custody" scheme is
// internal-only: no HTTP tooling will try to follow it, and it keeps
// custody URIs visually distinct from raw S3 URIs in logs.
const URIScheme = "custody"

// plaintextContentType is the MIME type recorded with every custody
// write. Fetcher emits flat JSON per its worker contract; custody copies
// preserve the format (D9: no re-encryption).
const plaintextContentType = "application/json"

// Sentinel errors for custody-store construction.
var (
	// ErrNilObjectStorage indicates the custody store was constructed
	// without an underlying object storage client. The custody store has
	// no fallback — storage is not optional.
	ErrNilObjectStorage = errors.New("custody store requires an object storage client")

	// ErrCustodyRefRequired indicates Delete was called with a zero-valued
	// reference. The custody URI or key is the only handle Delete has; we
	// refuse to guess.
	ErrCustodyRefRequired = errors.New("custody reference is required for delete")

	// ErrReplayRecoveryCapExceeded indicates the persisted custody object
	// exceeded the ingest-side byte cap (sharedPorts.MaxArtifactBytes)
	// during replay recovery. Only reachable if Exists misreports or a
	// non-ingest writer bypasses the verifier — treat as integrity failure.
	ErrReplayRecoveryCapExceeded = errors.New("replay recovery exceeded artifact byte cap")
)

// ArtifactCustodyStore implements sharedPorts.ArtifactCustodyStore on top
// of an S3-compatible object storage client. The plaintext flows
// through two transforms before upload:
//
//  1. SHA-256 is computed streaming via a TeeReader so we can fingerprint
//     the persisted bytes without buffering them.
//  2. A byte counter tracks how many bytes actually reach the uploader so
//     the returned reference carries a reliable size.
//
// Both transforms are synchronous with the upload; if upload is
// streaming, we end up with a streaming SHA and size as a side effect.
// When the underlying ObjectStorageClient buffers the body, nothing
// changes — we still get accurate size + digest.
type ArtifactCustodyStore struct {
	storage sharedPorts.ObjectStorageClient
	now     func() time.Time
}

// Compile-time interface checks.
var (
	_ sharedPorts.ArtifactCustodyStore = (*ArtifactCustodyStore)(nil)
	_ sharedPorts.CustodyKeyBuilder    = (*ArtifactCustodyStore)(nil)
)

// BuildObjectKey delegates to the package-level BuildObjectKey so the
// store value satisfies the sharedPorts.CustodyKeyBuilder port. Workers
// can then consume the key builder via the port interface instead of
// importing this adapter package — see the worker-no-adapters depguard
// rule for the enforcement.
func (store *ArtifactCustodyStore) BuildObjectKey(
	tenantID string,
	extractionID uuid.UUID,
) (string, error) {
	return BuildObjectKey(tenantID, extractionID)
}

// Option is a functional option for customising the custody store at
// construction time. Currently only NowFunc is overridable; kept as an
// option so tests can freeze time without exposing internal state.
type Option func(*ArtifactCustodyStore)

// WithNowFunc overrides the wall-clock source. Only useful in tests.
func WithNowFunc(fn func() time.Time) Option {
	return func(store *ArtifactCustodyStore) {
		if fn != nil {
			store.now = fn
		}
	}
}

// NewArtifactCustodyStore wires the custody store around the given
// object storage client. The storage client must be non-nil — there is no
// meaningful default because the bucket, credentials, and TLS posture all
// live on the storage client.
func NewArtifactCustodyStore(
	storage sharedPorts.ObjectStorageClient,
	opts ...Option,
) (*ArtifactCustodyStore, error) {
	if storage == nil {
		return nil, ErrNilObjectStorage
	}

	store := &ArtifactCustodyStore{
		storage: storage,
		now:     func() time.Time { return time.Now().UTC() },
	}

	for _, opt := range opts {
		if opt != nil {
			opt(store)
		}
	}

	return store, nil
}

// Store writes the plaintext to tenant-scoped custody. The key layout is:
//
//	{tenantID}/fetcher-artifacts/{extractionID}.json
//
// where tenantID is the tenant namespace prefix. This layout matches the
// conventions used by reporting exports and governance archives (see
// libS3.GetObjectStorageKey) so operators have a single mental model for
// where tenant data lives in object storage.
//
// Write-once semantics (C7 hardening): the upload runs through
// UploadIfAbsent, which issues a conditional PUT (If-None-Match: *) to
// the underlying object store. If the key already exists, the storage
// layer returns sharedPorts.ErrObjectAlreadyExists and Store drops into
// the replay path — recovering digest + size from the persisted bytes
// instead of re-uploading. This closes the TOCTOU window that a separate
// Exists + Upload pair left open: two concurrent writers can no longer
// both observe "absent" and both PUT, because the server-side condition
// is a single atomic check. The bridge worker's Redis distributed lock
// stays in place as defense-in-depth (and because not every S3-compatible
// backend honours the condition header reliably).
//
// Replay-recovery cost: on the ErrObjectAlreadyExists branch, Store
// performs one extra Download() to recompute SHA-256 and Size from the
// persisted bytes. This costs one round-trip on replays (rare in the
// happy path) but preserves the audit contract — source_metadata never
// carries empty custody_sha256, even after a partial-success retry.
//
// On failure other than ErrObjectAlreadyExists, the underlying storage
// error is wrapped with ErrCustodyStoreFailed; callers retry on the
// wrapped sentinel, not the underlying S3 error.
func (store *ArtifactCustodyStore) Store(
	ctx context.Context,
	input sharedPorts.ArtifactCustodyWriteInput,
) (*sharedPorts.ArtifactCustodyReference, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "custody.artifact_custody_store.store")
	defer span.End()

	if store == nil || store.storage == nil {
		err := fmt.Errorf("%w: store not initialised", sharedPorts.ErrCustodyStoreFailed)
		libOpentelemetry.HandleSpanError(span, "nil custody store", err)

		return nil, err
	}

	if err := validateWriteInput(input); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "invalid custody write input", err)

		return nil, err
	}

	key, err := BuildObjectKey(input.TenantID, input.ExtractionID)
	if err != nil {
		wrapped := fmt.Errorf("%w: build object key: %w", sharedPorts.ErrCustodyStoreFailed, err)
		libOpentelemetry.HandleSpanError(span, "build custody object key failed", wrapped)

		return nil, wrapped
	}

	hasher := sha256.New()
	counter := &counterWriter{}
	teed := io.TeeReader(io.TeeReader(input.Content, hasher), counter)

	// Conditional PUT: the storage layer issues If-None-Match: * and maps
	// a 412 Precondition Failed response onto ErrObjectAlreadyExists. This
	// atomically closes the check-then-act window a separate Exists +
	// Upload pair left open. Concurrent writers can no longer both succeed
	// against the same key; at most one wins, the other replays.
	storedKey, err := store.storage.UploadIfAbsent(ctx, key, teed, plaintextContentType)
	if err != nil {
		if errors.Is(err, sharedPorts.ErrObjectAlreadyExists) {
			// Replay path: the object is already in custody from a prior
			// successful write. Re-hash the persisted bytes so the returned
			// reference carries the authoritative digest + size — the
			// input reader we just streamed through the TeeReader describes
			// THIS caller's bytes, which may differ from what's persisted
			// (different crypto nonce, different Fetcher response buffer).
			// The audit contract says source_metadata must describe the
			// persisted bytes, so we ignore the teed hash and re-read.
			size, sha, recoverErr := store.recoverDigest(ctx, key)
			if recoverErr != nil {
				wrapped := fmt.Errorf("%w: replay recovery: %w", sharedPorts.ErrCustodyStoreFailed, recoverErr)
				libOpentelemetry.HandleSpanError(span, "custody replay recovery failed", wrapped)

				return nil, wrapped
			}

			// StoredAt on the replay path reflects the recovery time (now),
			// NOT the original upload time. Object storage backends do not
			// expose the original creation timestamp through our Download
			// port, so recording "now" is the best we can do without a
			// second metadata round-trip. Consumers that must distinguish
			// first-write from replay should not rely on StoredAt alone;
			// the caller-side dedup hash already provides that signal.
			return &sharedPorts.ArtifactCustodyReference{
				URI:      URIScheme + "://" + key,
				Key:      key,
				Size:     size,
				SHA256:   sha,
				StoredAt: store.now(),
			}, nil
		}

		wrapped := fmt.Errorf("%w: upload: %w", sharedPorts.ErrCustodyStoreFailed, err)
		libOpentelemetry.HandleSpanError(span, "custody upload failed", wrapped)

		return nil, wrapped
	}

	return &sharedPorts.ArtifactCustodyReference{
		URI:      URIScheme + "://" + storedKey,
		Key:      storedKey,
		Size:     counter.n,
		SHA256:   hex.EncodeToString(hasher.Sum(nil)),
		StoredAt: store.now(),
	}, nil
}

// Open streams the custody plaintext back. Used by the bridge worker to feed
// previously-persisted custody copies into the ingestion pipeline without
// re-downloading from Fetcher. Returns ErrCustodyStoreFailed wrapped on
// failure.
func (store *ArtifactCustodyStore) Open(
	ctx context.Context,
	ref sharedPorts.ArtifactCustodyReference,
) (io.ReadCloser, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "custody.artifact_custody_store.open")
	defer span.End()

	if store == nil || store.storage == nil {
		err := fmt.Errorf("%w: store not initialised", sharedPorts.ErrCustodyStoreFailed)
		libOpentelemetry.HandleSpanError(span, "nil custody store", err)

		return nil, err
	}

	key := strings.TrimSpace(ref.Key)
	if key == "" {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "missing custody key", ErrCustodyRefRequired)

		return nil, ErrCustodyRefRequired
	}

	reader, err := store.storage.Download(ctx, key)
	if err != nil {
		wrapped := fmt.Errorf("%w: download: %w", sharedPorts.ErrCustodyStoreFailed, err)
		libOpentelemetry.HandleSpanError(span, "custody download failed", wrapped)

		return nil, wrapped
	}

	return reader, nil
}

// Delete removes a custody copy. Invoked by the bridge worker after the
// downstream ingestion job has succeeded (D2: delete-after-ingest). Any
// failure is wrapped with ErrCustodyStoreFailed so callers can treat
// retention cleanup as retryable.
func (store *ArtifactCustodyStore) Delete(
	ctx context.Context,
	ref sharedPorts.ArtifactCustodyReference,
) error {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "custody.artifact_custody_store.delete")
	defer span.End()

	if store == nil || store.storage == nil {
		err := fmt.Errorf("%w: store not initialised", sharedPorts.ErrCustodyStoreFailed)
		libOpentelemetry.HandleSpanError(span, "nil custody store", err)

		return err
	}

	key := strings.TrimSpace(ref.Key)
	if key == "" {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "missing custody key", ErrCustodyRefRequired)

		return ErrCustodyRefRequired
	}

	if err := store.storage.Delete(ctx, key); err != nil {
		wrapped := fmt.Errorf("%w: delete: %w", sharedPorts.ErrCustodyStoreFailed, err)
		libOpentelemetry.HandleSpanError(span, "custody delete failed", wrapped)

		return wrapped
	}

	return nil
}

// BuildObjectKey constructs the tenant-scoped object key for a given
// extraction. Exported so tests (and future operators / migration tools)
// can assert on the layout without coupling to the full upload call.
//
// Layout:
//
//	{tenantID}/fetcher-artifacts/{extractionID}.json
//
// The tenant id must not contain '/' or any ASCII control character
// (bytes < 0x20 or 0x7F). Either would let adversarial input alter the
// object key in ways downstream consumers do not expect:
//
//   - '/' introduces ambiguous prefixes and enables path traversal into
//     neighbouring tenant namespaces.
//   - NUL bytes terminate C-string-based filesystem and S3 client paths
//     silently, so "tenant\x00evil" can masquerade as "tenant" to any
//     code that forgets to be NUL-aware.
//   - Other control bytes (CR/LF/TAB) smuggle header-injection attacks
//     into object-storage backends that pipe keys through HTTP request
//     lines (notably older SigV2 signers and some proxies).
//
// The custody store refuses to build the key rather than sanitise it, so
// the failure is loud instead of silent.
func BuildObjectKey(tenantID string, extractionID uuid.UUID) (string, error) {
	trimmed := strings.TrimSpace(tenantID)
	if trimmed == "" {
		return "", sharedPorts.ErrArtifactTenantIDRequired
	}

	if strings.Contains(trimmed, "/") {
		return "", fmt.Errorf(
			"%w: tenant id must not contain '/'",
			sharedPorts.ErrArtifactTenantIDRequired,
		)
	}

	if containsControlByte(trimmed) {
		return "", fmt.Errorf(
			"%w: tenant id must not contain control characters",
			sharedPorts.ErrArtifactTenantIDRequired,
		)
	}

	if extractionID == uuid.Nil {
		return "", sharedPorts.ErrArtifactExtractionIDRequired
	}

	return trimmed + "/" + KeyPrefix + "/" + extractionID.String() + ".json", nil
}

// containsControlByte returns true if s contains any ASCII control byte
// (< 0x20 or == 0x7F). Runs over the raw bytes, not runes, because
// object-storage keys are byte strings: a multi-byte UTF-8 rune can
// legitimately include continuation bytes in the 0x80-0xBF range, which
// are fine, but the 0x00-0x1F and 0x7F ranges are never valid code-unit
// bytes in UTF-8 and so flag safely at the byte level.
func containsControlByte(s string) bool {
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b < 0x20 || b == 0x7F {
			return true
		}
	}

	return false
}

func validateWriteInput(input sharedPorts.ArtifactCustodyWriteInput) error {
	if input.ExtractionID == uuid.Nil {
		return sharedPorts.ErrArtifactExtractionIDRequired
	}

	if strings.TrimSpace(input.TenantID) == "" {
		return sharedPorts.ErrArtifactTenantIDRequired
	}

	if input.Content == nil {
		return sharedPorts.ErrArtifactCiphertextRequired
	}

	return nil
}

// counterWriter counts bytes flowing through a writer. Cheaper than
// wrapping io.Copy because the upload path consumes the reader
// internally. We drop bytes into the void once we have counted them.
//
// Contract: Write always returns (len(p), nil) — it never errors and never
// short-writes. This is intentional: counterWriter is paired with a
// sha256.Hash inside an io.MultiWriter (see Store and recoverDigest) so the
// upload pipeline can compute the content length alongside the streaming
// hash without a second pass over the bytes. Callers that need an errant-
// write signal must use a different writer.
type counterWriter struct {
	n int64
}

func (c *counterWriter) Write(p []byte) (int, error) {
	c.n += int64(len(p))

	return len(p), nil
}

// recoverDigest re-hashes the bytes at the given key so a replay path
// (where Store sees the object already exists) can return a fully
// populated reference instead of one with empty Size/SHA256. The Download
// reader is closed eagerly and any close failure is folded into the
// returned error so the caller never has to worry about leaked handles.
//
// Defense-in-depth: the read is capped at sharedPorts.MaxArtifactBytes to
// mirror the ingest verifier. Today only capped objects reach this path
// (the ingest verifier would have refused anything larger before persistence),
// but a storage backend that misreports Exists — or a future caller that
// writes directly — must not be able to make replay materialise more bytes
// than the ingest side permits.
func (store *ArtifactCustodyStore) recoverDigest(ctx context.Context, key string) (int64, string, error) {
	reader, err := store.storage.Download(ctx, key)
	if err != nil {
		return 0, "", fmt.Errorf("download for replay recovery: %w", err)
	}

	hasher := sha256.New()
	counter := &counterWriter{}
	limited := io.LimitReader(reader, sharedPorts.MaxArtifactBytes+1)

	if _, copyErr := io.Copy(io.MultiWriter(hasher, counter), limited); copyErr != nil {
		_ = reader.Close()

		return 0, "", fmt.Errorf("hash persisted custody bytes: %w", copyErr)
	}

	if counter.n > sharedPorts.MaxArtifactBytes {
		_ = reader.Close()

		return 0, "", fmt.Errorf("%w: %d bytes", ErrReplayRecoveryCapExceeded, sharedPorts.MaxArtifactBytes)
	}

	if closeErr := reader.Close(); closeErr != nil {
		return 0, "", fmt.Errorf("close custody reader after replay recovery: %w", closeErr)
	}

	return counter.n, hex.EncodeToString(hasher.Sum(nil)), nil
}
