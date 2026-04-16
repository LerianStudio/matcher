// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// The verified-artifact orchestrator stitches retrieval, verification, and
// custody writing into a single use-case method. Its correctness is
// defined by four invariants:
//
//  1. happy path: every stage produces its expected output and the final
//     custody reference carries the extraction + tenant + plaintext.
//  2. HMAC mismatches are terminal: no custody write is attempted.
//  3. AES-GCM auth-tag failures are terminal in the same way.
//  4. Retrieval network / IO errors are transient and surface to the
//     caller without corrupting custody state.
//
// The mock stages below are deliberately tiny and sit next to the
// cornerstone test so the four contract invariants remain readable in one
// place. Per CLAUDE.md, manual mocks are preferred for ports with five or
// fewer methods; ArtifactRetrievalGateway, ArtifactTrustVerifier, and
// ArtifactCustodyStore all qualify.

// fakeRetrievalGateway is a manual mock for ArtifactRetrievalGateway.
type fakeRetrievalGateway struct {
	result    *sharedPorts.ArtifactRetrievalResult
	err       error
	callCount int
	lastDesc  sharedPorts.ArtifactRetrievalDescriptor
}

func (gw *fakeRetrievalGateway) Retrieve(
	_ context.Context,
	desc sharedPorts.ArtifactRetrievalDescriptor,
) (*sharedPorts.ArtifactRetrievalResult, error) {
	gw.callCount++
	gw.lastDesc = desc

	if gw.err != nil {
		return nil, gw.err
	}

	return gw.result, nil
}

// fakeTrustVerifier is a manual mock for ArtifactTrustVerifier. It
// records the ciphertext bytes it was asked to verify so assertions can
// prove the orchestrator wired retrieval output into verifier input.
type fakeTrustVerifier struct {
	plaintext    []byte
	err          error
	callCount    int
	sawHMAC      string
	sawIV        string
	sawCipherLen int
}

func (v *fakeTrustVerifier) VerifyAndDecrypt(
	_ context.Context,
	ciphertext io.Reader,
	hmacHex string,
	ivHex string,
) (io.Reader, error) {
	v.callCount++
	v.sawHMAC = hmacHex
	v.sawIV = ivHex

	if ciphertext != nil {
		buf, _ := io.ReadAll(ciphertext)
		v.sawCipherLen = len(buf)
	}

	if v.err != nil {
		return nil, v.err
	}

	return bytes.NewReader(v.plaintext), nil
}

// fakeCustodyStore is a manual mock for ArtifactCustodyStore. It
// intentionally implements Store AND Delete so the interface compile
// check in production code continues to hold.
type fakeCustodyStore struct {
	ref           *sharedPorts.ArtifactCustodyReference
	err           error
	storeCalls    int
	deleteCalls   int
	lastInput     sharedPorts.ArtifactCustodyWriteInput
	lastPlaintext []byte
	deletedRef    sharedPorts.ArtifactCustodyReference
}

func (s *fakeCustodyStore) Store(
	_ context.Context,
	input sharedPorts.ArtifactCustodyWriteInput,
) (*sharedPorts.ArtifactCustodyReference, error) {
	s.storeCalls++
	s.lastInput = input

	if input.Content != nil {
		buf, _ := io.ReadAll(input.Content)
		s.lastPlaintext = buf
	}

	if s.err != nil {
		return nil, s.err
	}

	return s.ref, nil
}

func (s *fakeCustodyStore) Delete(
	_ context.Context,
	ref sharedPorts.ArtifactCustodyReference,
) error {
	s.deleteCalls++
	s.deletedRef = ref

	return nil
}

func newTestDescriptor(t *testing.T) (uuid.UUID, string, sharedPorts.ArtifactRetrievalDescriptor) {
	t.Helper()

	extractionID := uuid.New()
	tenantID := "tenant-" + extractionID.String()[:8]

	return extractionID, tenantID, sharedPorts.ArtifactRetrievalDescriptor{
		ExtractionID: extractionID,
		TenantID:     tenantID,
		URL:          "https://fetcher.example.test/v1/artifacts/abc",
	}
}

// TestRetrieveAndCustodyVerifiedArtifact_HappyPath asserts that the
// orchestrator invokes each stage in order and forwards plaintext into
// the custody store with the originating extraction + tenant scoping.
// This is the cornerstone of the RED → GREEN cycle.
func TestRetrieveAndCustodyVerifiedArtifact_HappyPath(t *testing.T) {
	t.Parallel()

	extractionID, tenantID, descriptor := newTestDescriptor(t)
	ciphertext := []byte("ciphertext-bytes-opaque")
	plaintext := []byte(`{"rows":[{"id":1}]}`)

	gateway := &fakeRetrievalGateway{
		result: &sharedPorts.ArtifactRetrievalResult{
			Content:       io.NopCloser(bytes.NewReader(ciphertext)),
			ContentLength: int64(len(ciphertext)),
			ContentType:   "application/octet-stream",
			HMAC:          "deadbeef",
			IV:            "feedface",
		},
	}
	verifier := &fakeTrustVerifier{plaintext: plaintext}
	store := &fakeCustodyStore{
		ref: &sharedPorts.ArtifactCustodyReference{
			URI:    "s3://matcher-artifacts/" + tenantID + "/fetcher-artifacts/" + extractionID.String() + ".json",
			Key:    tenantID + "/fetcher-artifacts/" + extractionID.String() + ".json",
			Size:   int64(len(plaintext)),
			SHA256: "abc123",
		},
	}

	orchestrator, err := NewVerifiedArtifactRetrievalOrchestrator(gateway, verifier, store)
	require.NoError(t, err)

	out, err := orchestrator.RetrieveAndCustodyVerifiedArtifact(
		context.Background(),
		VerifiedArtifactRetrievalInput{Descriptor: descriptor},
	)
	require.NoError(t, err)
	require.NotNil(t, out)
	require.NotNil(t, out.Custody)

	assert.Equal(t, 1, gateway.callCount, "retrieval invoked exactly once")
	assert.Equal(t, descriptor, gateway.lastDesc, "descriptor propagated to gateway")
	assert.Equal(t, 1, verifier.callCount, "verifier invoked exactly once")
	assert.Equal(t, "deadbeef", verifier.sawHMAC, "verifier saw HMAC from retrieval")
	assert.Equal(t, "feedface", verifier.sawIV, "verifier saw IV from retrieval")
	assert.Equal(t, len(ciphertext), verifier.sawCipherLen, "verifier saw ciphertext bytes")
	assert.Equal(t, 1, store.storeCalls, "custody store invoked exactly once")
	assert.Equal(t, extractionID, store.lastInput.ExtractionID, "extraction id wired to custody")
	assert.Equal(t, tenantID, store.lastInput.TenantID, "tenant id wired to custody")
	assert.Equal(t, plaintext, store.lastPlaintext, "plaintext handed to custody")
	assert.Equal(t, store.ref.URI, out.Custody.URI, "orchestrator returns custody URI")
	assert.Equal(t, 0, store.deleteCalls, "no delete in happy path")
}

// TestRetrieveAndCustodyVerifiedArtifact_HMACMismatch_Terminal asserts a
// verification failure is terminal: the custody store is NEVER touched,
// and the caller receives ErrIntegrityVerificationFailed.
func TestRetrieveAndCustodyVerifiedArtifact_HMACMismatch_Terminal(t *testing.T) {
	t.Parallel()

	_, _, descriptor := newTestDescriptor(t)

	gateway := &fakeRetrievalGateway{
		result: &sharedPorts.ArtifactRetrievalResult{
			Content: io.NopCloser(strings.NewReader("bytes")),
			HMAC:    "badhmac",
			IV:      "iv",
		},
	}
	verifier := &fakeTrustVerifier{err: sharedPorts.ErrIntegrityVerificationFailed}
	store := &fakeCustodyStore{}

	orchestrator, err := NewVerifiedArtifactRetrievalOrchestrator(gateway, verifier, store)
	require.NoError(t, err)

	out, err := orchestrator.RetrieveAndCustodyVerifiedArtifact(
		context.Background(),
		VerifiedArtifactRetrievalInput{Descriptor: descriptor},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
	assert.Nil(t, out)
	assert.Equal(t, 0, store.storeCalls, "no custody write on integrity failure")
}

// TestRetrieveAndCustodyVerifiedArtifact_AuthTagFailure_Terminal asserts
// that AES-GCM auth-tag failures reach the caller under the same terminal
// sentinel as HMAC mismatches. Collapsing the two classes into one
// sentinel is the contract defined by the verifier port.
func TestRetrieveAndCustodyVerifiedArtifact_AuthTagFailure_Terminal(t *testing.T) {
	t.Parallel()

	_, _, descriptor := newTestDescriptor(t)

	gateway := &fakeRetrievalGateway{
		result: &sharedPorts.ArtifactRetrievalResult{
			Content: io.NopCloser(strings.NewReader("bytes")),
			HMAC:    "abc",
			IV:      "iv",
		},
	}
	// Simulate AES-GCM auth-tag failure collapsed into the integrity sentinel.
	verifier := &fakeTrustVerifier{err: sharedPorts.ErrIntegrityVerificationFailed}
	store := &fakeCustodyStore{}

	orchestrator, err := NewVerifiedArtifactRetrievalOrchestrator(gateway, verifier, store)
	require.NoError(t, err)

	_, err = orchestrator.RetrieveAndCustodyVerifiedArtifact(
		context.Background(),
		VerifiedArtifactRetrievalInput{Descriptor: descriptor},
	)
	require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
	assert.Equal(t, 0, store.storeCalls, "no custody write on auth-tag failure")
}

// TestRetrieveAndCustodyVerifiedArtifact_RetrievalFailure_Transient
// asserts that retrieval network / IO failures surface to the caller
// under ErrArtifactRetrievalFailed without any downstream side-effect.
// The bridge worker (T-005) owns retry policy; the orchestrator's only
// job is to preserve the transient/terminal distinction.
func TestRetrieveAndCustodyVerifiedArtifact_RetrievalFailure_Transient(t *testing.T) {
	t.Parallel()

	_, _, descriptor := newTestDescriptor(t)

	gateway := &fakeRetrievalGateway{err: sharedPorts.ErrArtifactRetrievalFailed}
	verifier := &fakeTrustVerifier{}
	store := &fakeCustodyStore{}

	orchestrator, err := NewVerifiedArtifactRetrievalOrchestrator(gateway, verifier, store)
	require.NoError(t, err)

	_, err = orchestrator.RetrieveAndCustodyVerifiedArtifact(
		context.Background(),
		VerifiedArtifactRetrievalInput{Descriptor: descriptor},
	)
	require.ErrorIs(t, err, sharedPorts.ErrArtifactRetrievalFailed)
	assert.Equal(t, 0, verifier.callCount, "verifier not invoked on retrieval failure")
	assert.Equal(t, 0, store.storeCalls, "no custody write on retrieval failure")
}

// TestRetrieveAndCustodyVerifiedArtifact_CustodyFailure_WrapsSentinel
// asserts that a downstream custody error surfaces under
// ErrCustodyStoreFailed. The orchestrator MUST NOT suppress nor
// reinterpret storage failures — they are the caller's signal to retry.
func TestRetrieveAndCustodyVerifiedArtifact_CustodyFailure_WrapsSentinel(t *testing.T) {
	t.Parallel()

	_, _, descriptor := newTestDescriptor(t)

	gateway := &fakeRetrievalGateway{
		result: &sharedPorts.ArtifactRetrievalResult{
			Content: io.NopCloser(strings.NewReader("bytes")),
			HMAC:    "abc",
			IV:      "iv",
		},
	}
	verifier := &fakeTrustVerifier{plaintext: []byte("plain")}
	store := &fakeCustodyStore{err: errors.New("s3 put rejected")}

	orchestrator, err := NewVerifiedArtifactRetrievalOrchestrator(gateway, verifier, store)
	require.NoError(t, err)

	_, err = orchestrator.RetrieveAndCustodyVerifiedArtifact(
		context.Background(),
		VerifiedArtifactRetrievalInput{Descriptor: descriptor},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, sharedPorts.ErrCustodyStoreFailed)
}

// TestNewVerifiedArtifactRetrievalOrchestrator_NilDependencies asserts
// the constructor rejects every nil dependency with a distinct sentinel
// so bootstrap failures are locatable without a debugger.
func TestNewVerifiedArtifactRetrievalOrchestrator_NilDependencies(t *testing.T) {
	t.Parallel()

	gateway := &fakeRetrievalGateway{}
	verifier := &fakeTrustVerifier{}
	store := &fakeCustodyStore{}

	t.Run("nil gateway", func(t *testing.T) {
		t.Parallel()

		_, err := NewVerifiedArtifactRetrievalOrchestrator(nil, verifier, store)
		require.ErrorIs(t, err, sharedPorts.ErrNilArtifactRetrievalGateway)
	})

	t.Run("nil verifier", func(t *testing.T) {
		t.Parallel()

		_, err := NewVerifiedArtifactRetrievalOrchestrator(gateway, nil, store)
		require.ErrorIs(t, err, sharedPorts.ErrNilArtifactTrustVerifier)
	})

	t.Run("nil custody store", func(t *testing.T) {
		t.Parallel()

		_, err := NewVerifiedArtifactRetrievalOrchestrator(gateway, verifier, nil)
		require.ErrorIs(t, err, sharedPorts.ErrNilArtifactCustodyStore)
	})
}

// TestWrapRetrievalError_Taxonomy exercises the wrapRetrievalError helper
// across all three branches: nil passthrough, already-wrapped preservation
// (ErrFetcherResourceNotFound must remain inspectable), and unwrapped
// fallback. Without this test the helper's 40% of lines stayed dark even
// though its decision logic underpins the terminal / transient split for
// every retrieval failure.
func TestWrapRetrievalError_Taxonomy(t *testing.T) {
	t.Parallel()

	t.Run("nil stays nil", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, wrapRetrievalError(nil))
	})

	t.Run("already retrieval-wrapped preserved", func(t *testing.T) {
		t.Parallel()

		original := sharedPorts.ErrArtifactRetrievalFailed
		got := wrapRetrievalError(original)
		require.ErrorIs(t, got, sharedPorts.ErrArtifactRetrievalFailed)
		assert.Equal(t, original, got, "identity preserved so callers keep their wrap chain")
	})

	t.Run("not-found passthrough", func(t *testing.T) {
		t.Parallel()

		got := wrapRetrievalError(sharedPorts.ErrFetcherResourceNotFound)
		require.ErrorIs(t, got, sharedPorts.ErrFetcherResourceNotFound)
		assert.Equal(t, sharedPorts.ErrFetcherResourceNotFound, got, "404 passthrough, no rewrap")
	})

	t.Run("unwrapped error gets retrieval wrap", func(t *testing.T) {
		t.Parallel()

		raw := errors.New("bare transport")
		got := wrapRetrievalError(raw)
		require.ErrorIs(t, got, sharedPorts.ErrArtifactRetrievalFailed)
		require.ErrorIs(t, got, raw, "underlying preserved under wrap")
	})
}

// TestWrapCustodyError_Taxonomy exercises the wrapCustodyError helper with
// the same three-branch contract as wrapRetrievalError.
func TestWrapCustodyError_Taxonomy(t *testing.T) {
	t.Parallel()

	t.Run("nil stays nil", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, wrapCustodyError(nil))
	})

	t.Run("already custody-wrapped preserved", func(t *testing.T) {
		t.Parallel()

		original := sharedPorts.ErrCustodyStoreFailed
		got := wrapCustodyError(original)
		require.ErrorIs(t, got, sharedPorts.ErrCustodyStoreFailed)
		assert.Equal(t, original, got, "identity preserved")
	})

	t.Run("unwrapped error gets custody wrap", func(t *testing.T) {
		t.Parallel()

		raw := errors.New("bare backend")
		got := wrapCustodyError(raw)
		require.ErrorIs(t, got, sharedPorts.ErrCustodyStoreFailed)
		require.ErrorIs(t, got, raw, "underlying preserved under wrap")
	})
}

// TestRetrieveAndCustodyVerifiedArtifact_NilGatewayResult_Transient asserts
// that a gateway that returns (nil, nil) is treated as a transient retrieval
// failure. Without this guard the orchestrator would dereference nil below,
// turning a contract violation into a panic. AC-O2 terminal-vs-transient
// distinction relies on the error carrying ErrArtifactRetrievalFailed.
func TestRetrieveAndCustodyVerifiedArtifact_NilGatewayResult_Transient(t *testing.T) {
	t.Parallel()

	_, _, descriptor := newTestDescriptor(t)

	// Gateway returns nil result AND nil error — a broken implementation
	// shape the orchestrator must still refuse to proceed on.
	gateway := &fakeRetrievalGateway{result: nil}
	verifier := &fakeTrustVerifier{}
	store := &fakeCustodyStore{}

	orchestrator, err := NewVerifiedArtifactRetrievalOrchestrator(gateway, verifier, store)
	require.NoError(t, err)

	_, err = orchestrator.RetrieveAndCustodyVerifiedArtifact(
		context.Background(),
		VerifiedArtifactRetrievalInput{Descriptor: descriptor},
	)
	require.ErrorIs(t, err, sharedPorts.ErrArtifactRetrievalFailed)
	assert.Equal(t, 0, verifier.callCount, "verifier never called when retrieval body is nil")
	assert.Equal(t, 0, store.storeCalls, "no custody write on nil body")
}

// TestRetrieveAndCustodyVerifiedArtifact_NilGatewayContent_Transient
// asserts the secondary branch: gateway returns a non-nil result envelope
// with a nil Content reader. Same terminal signal but different code path.
func TestRetrieveAndCustodyVerifiedArtifact_NilGatewayContent_Transient(t *testing.T) {
	t.Parallel()

	_, _, descriptor := newTestDescriptor(t)

	gateway := &fakeRetrievalGateway{
		result: &sharedPorts.ArtifactRetrievalResult{
			Content: nil,
			HMAC:    "abc",
			IV:      "iv",
		},
	}
	verifier := &fakeTrustVerifier{}
	store := &fakeCustodyStore{}

	orchestrator, err := NewVerifiedArtifactRetrievalOrchestrator(gateway, verifier, store)
	require.NoError(t, err)

	_, err = orchestrator.RetrieveAndCustodyVerifiedArtifact(
		context.Background(),
		VerifiedArtifactRetrievalInput{Descriptor: descriptor},
	)
	require.ErrorIs(t, err, sharedPorts.ErrArtifactRetrievalFailed)
	assert.Equal(t, 0, store.storeCalls, "no custody write on nil content body")
}

// TestRetrieveAndCustodyVerifiedArtifact_NonIntegrityVerifierError_Wrapped
// asserts that a verifier error that is NOT the integrity sentinel (e.g. a
// surprise HKDF init failure) surfaces to the caller as a wrapped
// verify-artifact error rather than leaking the raw implementation detail.
// The outer error chain must still be inspectable via the original cause.
func TestRetrieveAndCustodyVerifiedArtifact_NonIntegrityVerifierError_Wrapped(t *testing.T) {
	t.Parallel()

	_, _, descriptor := newTestDescriptor(t)

	surpriseErr := errors.New("hkdf init exploded unexpectedly")

	gateway := &fakeRetrievalGateway{
		result: &sharedPorts.ArtifactRetrievalResult{
			Content: io.NopCloser(strings.NewReader("bytes")),
			HMAC:    "abc",
			IV:      "iv",
		},
	}
	verifier := &fakeTrustVerifier{err: surpriseErr}
	store := &fakeCustodyStore{}

	orchestrator, err := NewVerifiedArtifactRetrievalOrchestrator(gateway, verifier, store)
	require.NoError(t, err)

	_, err = orchestrator.RetrieveAndCustodyVerifiedArtifact(
		context.Background(),
		VerifiedArtifactRetrievalInput{Descriptor: descriptor},
	)
	require.Error(t, err)
	// Not the integrity sentinel — it is a wrapped "verify artifact" error.
	require.NotErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed,
		"non-integrity error must not collapse to integrity sentinel")
	require.ErrorIs(t, err, surpriseErr, "underlying cause preserved for diagnosis")
	assert.Equal(t, 0, store.storeCalls, "no custody write on non-integrity verifier error")
}

// failingPlaintextReader always returns an error before yielding any bytes.
// Used to exercise the io.ReadAll failure branch in retrieveAndVerify that
// materialises plaintext into a bounded buffer.
type failingPlaintextReader struct {
	err error
}

func (r *failingPlaintextReader) Read(_ []byte) (int, error) {
	return 0, r.err
}

// plaintextVerifier is a drop-in verifier that hands back an arbitrary
// io.Reader (rather than a bytes-backed one) so tests can inject readers
// that fail mid-stream.
type plaintextVerifier struct {
	reader io.Reader
}

func (v *plaintextVerifier) VerifyAndDecrypt(
	_ context.Context,
	_ io.Reader,
	_ string,
	_ string,
) (io.Reader, error) {
	return v.reader, nil
}

// TestRetrieveAndCustodyVerifiedArtifact_PlaintextReadFailure_Transient
// asserts the materialisation branch: if reading the verifier's plaintext
// Reader fails, we surface ErrArtifactRetrievalFailed so the bridge worker
// treats the failure as transient rather than corrupt. AC-O2 relies on this
// distinction: a failed read is "network blip", not "tampered payload".
func TestRetrieveAndCustodyVerifiedArtifact_PlaintextReadFailure_Transient(t *testing.T) {
	t.Parallel()

	_, _, descriptor := newTestDescriptor(t)

	gateway := &fakeRetrievalGateway{
		result: &sharedPorts.ArtifactRetrievalResult{
			Content: io.NopCloser(strings.NewReader("bytes")),
			HMAC:    "abc",
			IV:      "iv",
		},
	}
	readErr := errors.New("plaintext stream snapped")
	verifier := &plaintextVerifier{reader: &failingPlaintextReader{err: readErr}}
	store := &fakeCustodyStore{}

	orchestrator, err := NewVerifiedArtifactRetrievalOrchestrator(gateway, verifier, store)
	require.NoError(t, err)

	_, err = orchestrator.RetrieveAndCustodyVerifiedArtifact(
		context.Background(),
		VerifiedArtifactRetrievalInput{Descriptor: descriptor},
	)
	require.ErrorIs(t, err, sharedPorts.ErrArtifactRetrievalFailed)
	require.ErrorIs(t, err, readErr, "root cause preserved for diagnosis")
	assert.Equal(t, 0, store.storeCalls, "no custody write when plaintext read fails")
}

// TestRetrieveAndCustodyVerifiedArtifact_NilCustodyReference_Wrapped
// asserts the defensive branch where a broken custody store returns
// (nil, nil). The orchestrator must surface that as a custody failure so
// the bridge worker can retry rather than crash on a nil dereference
// downstream.
func TestRetrieveAndCustodyVerifiedArtifact_NilCustodyReference_Wrapped(t *testing.T) {
	t.Parallel()

	_, _, descriptor := newTestDescriptor(t)

	gateway := &fakeRetrievalGateway{
		result: &sharedPorts.ArtifactRetrievalResult{
			Content: io.NopCloser(strings.NewReader("bytes")),
			HMAC:    "abc",
			IV:      "iv",
		},
	}
	verifier := &fakeTrustVerifier{plaintext: []byte("plain")}
	// Broken store: returns success code but no reference.
	store := &fakeCustodyStore{ref: nil}

	orchestrator, err := NewVerifiedArtifactRetrievalOrchestrator(gateway, verifier, store)
	require.NoError(t, err)

	out, err := orchestrator.RetrieveAndCustodyVerifiedArtifact(
		context.Background(),
		VerifiedArtifactRetrievalInput{Descriptor: descriptor},
	)
	require.ErrorIs(t, err, sharedPorts.ErrCustodyStoreFailed)
	assert.Nil(t, out, "no output envelope when custody ref is nil")
}

// TestRetrieveAndCustodyVerifiedArtifact_ValidatesInput asserts the
// orchestrator refuses to hand a zero-valued descriptor to the gateway —
// we want a clear input-side sentinel, not an obscure downstream error.
func TestRetrieveAndCustodyVerifiedArtifact_ValidatesInput(t *testing.T) {
	t.Parallel()

	gateway := &fakeRetrievalGateway{}
	verifier := &fakeTrustVerifier{}
	store := &fakeCustodyStore{}

	orchestrator, err := NewVerifiedArtifactRetrievalOrchestrator(gateway, verifier, store)
	require.NoError(t, err)

	t.Run("missing extraction id", func(t *testing.T) {
		t.Parallel()

		_, err := orchestrator.RetrieveAndCustodyVerifiedArtifact(
			context.Background(),
			VerifiedArtifactRetrievalInput{
				Descriptor: sharedPorts.ArtifactRetrievalDescriptor{
					TenantID: "tenant",
					URL:      "https://fetcher.example.test/x",
				},
			},
		)
		require.ErrorIs(t, err, sharedPorts.ErrArtifactExtractionIDRequired)
	})

	t.Run("missing tenant id", func(t *testing.T) {
		t.Parallel()

		_, err := orchestrator.RetrieveAndCustodyVerifiedArtifact(
			context.Background(),
			VerifiedArtifactRetrievalInput{
				Descriptor: sharedPorts.ArtifactRetrievalDescriptor{
					ExtractionID: uuid.New(),
					URL:          "https://fetcher.example.test/x",
				},
			},
		)
		require.ErrorIs(t, err, sharedPorts.ErrArtifactTenantIDRequired)
	})

	t.Run("missing url", func(t *testing.T) {
		t.Parallel()

		_, err := orchestrator.RetrieveAndCustodyVerifiedArtifact(
			context.Background(),
			VerifiedArtifactRetrievalInput{
				Descriptor: sharedPorts.ArtifactRetrievalDescriptor{
					ExtractionID: uuid.New(),
					TenantID:     "tenant",
				},
			},
		)
		require.ErrorIs(t, err, sharedPorts.ErrArtifactDescriptorRequired)
	})
}
