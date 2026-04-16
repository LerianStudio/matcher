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

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

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

// Compile-time interface check.
var _ sharedPorts.ArtifactCustodyStore = (*ArtifactCustodyStore)(nil)

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
// Write-once semantics (T-003 P3 hardening): an existence check runs before
// upload. If the key already exists, Store returns a reference that points
// at the existing object without re-uploading. This preserves T-003's
// idempotency guarantee — replaying the bridge worker against the same
// extraction cannot produce a different custody copy with a different
// SHA-256, because the second attempt never overwrites the first.
//
// Replay-recovery cost (T-003 polish Fix 7): on the existed==true branch,
// Store performs one extra Download() to recompute SHA-256 and Size from
// the persisted bytes. This costs one round-trip on replays (rare in the
// happy path) but preserves the audit contract — source_metadata never
// carries empty custody_sha256, even after a partial-success retry. The
// alternative (returning empty SHA/Size on replay) propagated empty hash
// strings into the ingestion job's source_metadata, breaking downstream
// audit tooling that relies on the digest as a content-identity key.
//
// On failure, the underlying storage error is wrapped with
// ErrCustodyStoreFailed; callers retry on the wrapped sentinel, not the
// underlying S3 error.
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

	// Write-once guard: if the key already exists, treat it as a replay and
	// return a reference to the existing object without re-uploading. This
	// keeps custody SHA-256 stable across bridge replays — a critical
	// invariant for T-003's "no duplicate downstream readiness outcomes"
	// guarantee.
	existed, existErr := store.storage.Exists(ctx, key)
	if existErr != nil {
		// Treat Exists failure as transient and surface it wrapped. We do
		// NOT proceed to Upload blindly — doing so would overwrite on
		// partial-success retries, which is exactly the bug P3 fixes.
		wrapped := fmt.Errorf("%w: exists probe: %w", sharedPorts.ErrCustodyStoreFailed, existErr)
		libOpentelemetry.HandleSpanError(span, "custody exists probe failed", wrapped)

		return nil, wrapped
	}

	if existed {
		// Replay-recovery: re-hash the persisted bytes so source_metadata
		// downstream of the bridge orchestrator never carries empty digest
		// fields. Cost is one extra round-trip per replay (rare in the
		// happy path); benefit is a stable audit contract.
		size, sha, recoverErr := store.recoverDigest(ctx, key)
		if recoverErr != nil {
			wrapped := fmt.Errorf("%w: replay recovery: %w", sharedPorts.ErrCustodyStoreFailed, recoverErr)
			libOpentelemetry.HandleSpanError(span, "custody replay recovery failed", wrapped)

			return nil, wrapped
		}

		return &sharedPorts.ArtifactCustodyReference{
			URI:      URIScheme + "://" + key,
			Key:      key,
			Size:     size,
			SHA256:   sha,
			StoredAt: store.now(),
		}, nil
	}

	hasher := sha256.New()
	counter := &counterWriter{}
	teed := io.TeeReader(io.TeeReader(input.Content, hasher), counter)

	storedKey, err := store.storage.Upload(ctx, key, teed, plaintextContentType)
	if err != nil {
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
func (store *ArtifactCustodyStore) recoverDigest(ctx context.Context, key string) (int64, string, error) {
	reader, err := store.storage.Download(ctx, key)
	if err != nil {
		return 0, "", fmt.Errorf("download for replay recovery: %w", err)
	}

	hasher := sha256.New()
	counter := &counterWriter{}

	if _, copyErr := io.Copy(io.MultiWriter(hasher, counter), reader); copyErr != nil {
		_ = reader.Close()

		return 0, "", fmt.Errorf("hash persisted custody bytes: %w", copyErr)
	}

	if closeErr := reader.Close(); closeErr != nil {
		return 0, "", fmt.Errorf("close custody reader after replay recovery: %w", closeErr)
	}

	return counter.n, hex.EncodeToString(hasher.Sum(nil)), nil
}
