// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package ports

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/google/uuid"
)

// Sentinel errors for the verified-artifact pipeline (T-002).
//
// The verifier separates two failure classes so callers can drive retry
// policy correctly:
//
//   - ErrIntegrityVerificationFailed is terminal. HMAC mismatches and
//     AES-GCM auth-tag failures both collapse to this sentinel because the
//     caller cannot meaningfully distinguish between the two without
//     leaking information about which key derivation boundary was crossed.
//   - ErrArtifactRetrievalFailed is transient. Network timeouts,
//     TCP resets, connection refusals, and Fetcher 5xx responses all map
//     here so the bridge worker can retry with exponential backoff
//     (implemented under T-005).
//
// The two must not be conflated: retrying a terminal verification failure
// wastes budget and masks tampering as flakiness.
var (
	// ErrIntegrityVerificationFailed indicates the retrieved artifact failed
	// HMAC validation or AES-GCM authentication. This is a terminal error:
	// the retrieved bytes are not trustworthy and MUST NOT be persisted to
	// the custody store. Callers MUST NOT retry on this sentinel because
	// the ciphertext itself is corrupted or forged.
	ErrIntegrityVerificationFailed = errors.New(
		"artifact integrity verification failed",
	)

	// ErrArtifactRetrievalFailed indicates a transient failure while pulling
	// the artifact from Fetcher's custody. Network errors, TCP resets, 5xx
	// responses from Fetcher, and partial-read failures all collapse here so
	// the bridge worker can drive retry with exponential backoff.
	ErrArtifactRetrievalFailed = errors.New("artifact retrieval failed")

	// ErrCustodyStoreFailed indicates the custody store rejected the write
	// for a reason other than bad input (e.g. upstream S3 error, tenant path
	// construction rejected). Callers SHOULD treat this as transient — the
	// verified plaintext is still in-memory at the call site and re-uploading
	// is safe.
	ErrCustodyStoreFailed = errors.New("custody store write failed")

	// ErrNilArtifactRetrievalGateway indicates a required
	// ArtifactRetrievalGateway dependency was nil at construction time.
	ErrNilArtifactRetrievalGateway = errors.New(
		"artifact retrieval gateway is required",
	)

	// ErrNilArtifactTrustVerifier indicates a required ArtifactTrustVerifier
	// dependency was nil at construction time.
	ErrNilArtifactTrustVerifier = errors.New(
		"artifact trust verifier is required",
	)

	// ErrNilArtifactCustodyStore indicates a required ArtifactCustodyStore
	// dependency was nil at construction time.
	ErrNilArtifactCustodyStore = errors.New("artifact custody store is required")

	// ErrArtifactDescriptorRequired indicates a retrieval call was made with
	// a zero-valued descriptor. Distinct from ErrNilArtifactRetrievalGateway
	// so callers can tell a missing input from an unwired dependency.
	ErrArtifactDescriptorRequired = errors.New(
		"artifact retrieval descriptor is required",
	)

	// ErrArtifactExtractionIDRequired indicates a custody write was attempted
	// with a zero extraction UUID. Without it the custody reference cannot
	// be correlated back to the originating extraction.
	ErrArtifactExtractionIDRequired = errors.New(
		"artifact extraction id is required",
	)

	// ErrArtifactTenantIDRequired indicates a custody write was attempted
	// with an empty tenant id. Tenant-scoped custody paths depend on the id
	// being present.
	ErrArtifactTenantIDRequired = errors.New(
		"artifact tenant id is required",
	)

	// ErrArtifactHMACRequired indicates the verifier was called without a
	// non-empty HMAC digest. The Fetcher contract requires this header;
	// absence is a terminal verification failure.
	ErrArtifactHMACRequired = errors.New("artifact hmac digest is required")

	// ErrArtifactCiphertextRequired indicates the verifier was called without
	// ciphertext bytes. Distinct from ErrIntegrityVerificationFailed because
	// this is a caller-side input mistake, not a forged payload.
	ErrArtifactCiphertextRequired = errors.New("artifact ciphertext is required")

	// ErrArtifactIVRequired indicates the verifier was called without a
	// non-empty IV hex string. The Fetcher contract always ships the IV in
	// the X-Fetcher-Artifact-Iv header; absence is a terminal input
	// validation failure the verifier wraps with
	// ErrIntegrityVerificationFailed so bridge callers stop retrying.
	ErrArtifactIVRequired = errors.New("artifact iv is required")
)

// ArtifactRetrievalDescriptor addresses a completed Fetcher artifact for
// download. The bridge worker resolves an ExtractionRequest into one of
// these before handing it to the retrieval gateway.
//
// ExtractionID correlates the retrieval back to the extraction lifecycle
// for observability and tenant scoping. It is carried separately from the
// URL because the URL is what we talk to Fetcher over; the ExtractionID is
// what Matcher uses internally.
type ArtifactRetrievalDescriptor struct {
	// ExtractionID is the Matcher-owned id of the originating extraction
	// lifecycle. Used for custody path construction and tenant scoping.
	ExtractionID uuid.UUID
	// TenantID is the tenant that owns the extraction. Custody writes land
	// under this tenant's prefix.
	TenantID string
	// URL is the fully-qualified fetcher endpoint that serves the artifact
	// ciphertext. Constructed by the caller, not this layer. Typically a
	// concatenation of Fetcher base URL + tenant-scoped result path.
	URL string
}

// ArtifactRetrievalResult carries the raw ciphertext plus the integrity
// metadata required to verify it. Fields named Content* refer to the wire
// body; fields named HMAC* and IV* carry verification metadata.
//
// Content is the raw ciphertext ReadCloser. Callers MUST close it exactly
// once (typically through a defer after handoff to the verifier). The
// verifier consumes the bytes to validate HMAC + authenticated encryption
// before returning plaintext.
type ArtifactRetrievalResult struct {
	// Content is the ciphertext body. Must be closed by the caller.
	Content io.ReadCloser
	// ContentLength mirrors the HTTP Content-Length header. Zero when the
	// server did not advertise a length.
	ContentLength int64
	// ContentType mirrors the HTTP Content-Type header. Empty when absent.
	ContentType string
	// HMAC is the hex-encoded HMAC-SHA256 digest computed by Fetcher over
	// the ciphertext (per the fetcher-external-hmac-v1 contract). HMAC is
	// over ciphertext — not plaintext — so integrity can be checked before
	// we ever call gcm.Open. This value is the authority the verifier
	// compares against in constant time (hmac.Equal).
	HMAC string
	// IV is the initialisation vector required by AES-GCM. Transported
	// separately from the ciphertext per the Fetcher contract. Empty when
	// the server did not ship an IV header (e.g. unauthenticated payload
	// format — which the verifier rejects).
	IV string
}

// ArtifactCustodyReference identifies a custody copy after it has been
// successfully persisted. Returned by ArtifactCustodyStore.Store and
// consumed by downstream code that needs to read the plaintext back (e.g.
// the bridge worker handing content into ingestion, or a retention sweep
// deleting the copy after ingestion succeeds).
type ArtifactCustodyReference struct {
	// URI is the fully-qualified custody location. Opaque to callers; the
	// custody store recognises it in Delete().
	URI string
	// Key is the tenant-scoped object key, useful for logging and
	// observability where a full URI is noisy.
	Key string
	// Size is the byte size of the persisted plaintext. Zero when unknown
	// (e.g. streaming upload where the underlying S3 client does not
	// surface the write length).
	Size int64
	// SHA256 is the hex-encoded SHA-256 digest of the persisted plaintext,
	// allowing later readers to re-validate the custody copy without
	// re-downloading from Fetcher. Empty when the custody writer could not
	// compute it streaming.
	SHA256 string
	// StoredAt is the UTC timestamp when the custody copy was written.
	StoredAt time.Time
}

// ArtifactRetrievalGateway is the outbound port that reaches across the
// Fetcher boundary to pull ciphertext + metadata. Implementations must:
//   - honour ctx deadlines and cancellation
//   - separate transient failures (wrap ErrArtifactRetrievalFailed) from
//     permanent failures (e.g. 404 → ErrFetcherResourceNotFound)
//   - never decrypt in-place: retrieval is a pure IO stage, decryption is
//     the verifier's sole responsibility
type ArtifactRetrievalGateway interface {
	Retrieve(
		ctx context.Context,
		descriptor ArtifactRetrievalDescriptor,
	) (*ArtifactRetrievalResult, error)
}

// ArtifactTrustVerifier is the outbound port that validates and decrypts
// retrieved artifacts. Implementations must:
//   - derive keys deterministically from a master secret via HKDF-SHA256
//     using the contract-locked context strings (fetcher-external-hmac-v1
//     and fetcher-external-aes-v1)
//   - compare HMAC digests in constant time (hmac.Equal)
//   - return ErrIntegrityVerificationFailed for BOTH HMAC mismatches and
//     AES-GCM auth-tag failures (caller cannot meaningfully distinguish)
//   - never log or embed key material in errors
type ArtifactTrustVerifier interface {
	VerifyAndDecrypt(
		ctx context.Context,
		ciphertext io.Reader,
		hmacHex string,
		ivHex string,
	) (io.Reader, error)
}

// ArtifactCustodyWriteInput carries the arguments for a custody write.
// Separated from the Reader so future callers can add fields (e.g.
// ContentLength hint, tenancy override) without churning the signature.
type ArtifactCustodyWriteInput struct {
	// ExtractionID is the Matcher-owned extraction id. Embedded in the
	// object key so operators can correlate a custody copy back to the
	// originating extraction.
	ExtractionID uuid.UUID
	// TenantID is the tenant prefix to store under. Custody writes MUST
	// land inside the tenant's namespace — cross-tenant writes are a
	// security incident.
	TenantID string
	// Content is the plaintext reader. Custody stores plaintext per D9 of
	// the fetcher-bridge design; re-encryption is explicitly out of scope.
	Content io.Reader
}

// ArtifactCustodyStore is the outbound port that persists verified
// plaintext in Matcher-owned custody. Implementations must:
//   - prefix keys with the tenant id (multi-tenant isolation)
//   - return a reference that can later be passed to Delete for D2's
//     delete-after-ingest retention policy
//   - surface transient infrastructure errors by wrapping
//     ErrCustodyStoreFailed so callers can retry
type ArtifactCustodyStore interface {
	Store(
		ctx context.Context,
		input ArtifactCustodyWriteInput,
	) (*ArtifactCustodyReference, error)

	Delete(ctx context.Context, ref ArtifactCustodyReference) error
}
