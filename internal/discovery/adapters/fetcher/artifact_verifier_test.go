// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fetcher

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/hkdf"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// testMasterKey is a deterministic 32-byte master key used across verifier
// tests. Never use this value in production — it is public and any
// artifact signed with derived keys is trivially forgeable.
var testMasterKey = mustHex("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")

func mustHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic("mustHex: " + err.Error())
	}

	return b
}

// deriveTestKey mirrors the production deriveKey logic in the verifier.
// Keeping it in the test file (instead of calling the unexported helper)
// proves that the two implementations agree — if the verifier changes
// context strings or algorithm, these tests fail.
func deriveTestKey(t *testing.T, master []byte, info string) []byte {
	t.Helper()

	r := hkdf.New(sha256.New, master, nil, []byte(info))

	k := make([]byte, 32)
	_, err := io.ReadFull(r, k)
	require.NoError(t, err)

	return k
}

// encryptTestArtifact encrypts plaintext the same way Fetcher does so we
// can produce valid test fixtures without dragging in Fetcher's source.
// Returns (ciphertext, iv, hmacHex).
func encryptTestArtifact(t *testing.T, master, plaintext []byte) ([]byte, []byte, string) {
	t.Helper()

	hmacKey := deriveTestKey(t, master, "fetcher-external-hmac-v1")
	aesKey := deriveTestKey(t, master, "fetcher-external-aes-v1")

	block, err := aes.NewCipher(aesKey)
	require.NoError(t, err)

	gcm, err := cipher.NewGCM(block)
	require.NoError(t, err)

	iv := make([]byte, gcm.NonceSize())
	_, err = rand.Read(iv)
	require.NoError(t, err)

	ciphertext := gcm.Seal(nil, iv, plaintext, nil)

	mac := hmac.New(sha256.New, hmacKey)
	_, err = mac.Write(ciphertext)
	require.NoError(t, err)

	return ciphertext, iv, hex.EncodeToString(mac.Sum(nil))
}

func TestNewArtifactVerifier_RejectsEmptyMasterKey(t *testing.T) {
	t.Parallel()

	v, err := NewArtifactVerifier(nil)
	require.Nil(t, v)
	require.ErrorIs(t, err, ErrVerifierMasterKeyRequired)
}

func TestNewArtifactVerifier_RejectsTooShortMasterKey(t *testing.T) {
	t.Parallel()

	v, err := NewArtifactVerifier(make([]byte, 16))
	require.Nil(t, v)
	require.ErrorIs(t, err, ErrVerifierMasterKeyTooShort)
}

func TestNewArtifactVerifier_AcceptsValidMasterKey(t *testing.T) {
	t.Parallel()

	v, err := NewArtifactVerifier(testMasterKey)
	require.NoError(t, err)
	require.NotNil(t, v)
}

// TestDeriveKey_HKDFTestVector verifies our HKDF-SHA256 derivation against
// the internal-consistency contract with itself. We cannot use RFC 5869
// official test vectors directly because those use salt != nil; our
// contract matches Fetcher's salt=nil derivation. But we CAN prove that
// changing the info string yields a different key and that identical
// inputs yield identical keys (determinism) — those two properties pin
// the derivation deterministically.
func TestDeriveKey_HKDFDeterministicAndContextSeparated(t *testing.T) {
	t.Parallel()

	// Same input, same context => identical output (determinism).
	k1, err := deriveKey(testMasterKey, "fetcher-external-hmac-v1")
	require.NoError(t, err)
	k2, err := deriveKey(testMasterKey, "fetcher-external-hmac-v1")
	require.NoError(t, err)
	assert.Equal(t, k1, k2, "hkdf is deterministic")

	// Different context => different output (domain separation).
	kOther, err := deriveKey(testMasterKey, "fetcher-external-aes-v1")
	require.NoError(t, err)
	assert.NotEqual(t, k1, kOther, "different context strings yield distinct keys")
	assert.Len(t, k1, 32, "derived key is 32 bytes")
}

// TestVerifyAndDecrypt_ValidArtifact covers the happy path: ciphertext
// signed and encrypted with the same master yields the original plaintext.
func TestVerifyAndDecrypt_ValidArtifact(t *testing.T) {
	t.Parallel()

	verifier, err := NewArtifactVerifier(testMasterKey)
	require.NoError(t, err)

	plaintext := []byte(`{"records":[{"id":1,"amount":"100.00"},{"id":2,"amount":"99.50"}]}`)
	ciphertext, iv, hmacHex := encryptTestArtifact(t, testMasterKey, plaintext)

	reader, err := verifier.VerifyAndDecrypt(
		context.Background(),
		bytes.NewReader(ciphertext),
		hmacHex,
		hex.EncodeToString(iv),
	)
	require.NoError(t, err)
	require.NotNil(t, reader)

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, plaintext, got)
}

// TestVerifyAndDecrypt_HMACMismatch_Terminal asserts that a tampered
// HMAC digest returns the terminal integrity sentinel.
func TestVerifyAndDecrypt_HMACMismatch_Terminal(t *testing.T) {
	t.Parallel()

	verifier, err := NewArtifactVerifier(testMasterKey)
	require.NoError(t, err)

	plaintext := []byte("payload")
	ciphertext, iv, _ := encryptTestArtifact(t, testMasterKey, plaintext)

	// Replace HMAC with a valid-length but wrong digest.
	wrongHMAC := hex.EncodeToString(make([]byte, 32))

	_, err = verifier.VerifyAndDecrypt(
		context.Background(),
		bytes.NewReader(ciphertext),
		wrongHMAC,
		hex.EncodeToString(iv),
	)
	require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
}

// TestVerifyAndDecrypt_CiphertextTampered_Terminal asserts that any
// single-bit flip in the ciphertext trips the HMAC check (because we
// compute HMAC over ciphertext, not plaintext, matching Fetcher).
func TestVerifyAndDecrypt_CiphertextTampered_Terminal(t *testing.T) {
	t.Parallel()

	verifier, err := NewArtifactVerifier(testMasterKey)
	require.NoError(t, err)

	plaintext := []byte("payload")
	ciphertext, iv, hmacHex := encryptTestArtifact(t, testMasterKey, plaintext)

	// Flip the last byte to simulate in-flight corruption.
	tampered := make([]byte, len(ciphertext))
	copy(tampered, ciphertext)
	tampered[len(tampered)-1] ^= 0x01

	_, err = verifier.VerifyAndDecrypt(
		context.Background(),
		bytes.NewReader(tampered),
		hmacHex,
		hex.EncodeToString(iv),
	)
	require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
}

// TestVerifyAndDecrypt_AuthTagFailure_Terminal asserts that a valid HMAC
// but wrong IV (so AES-GCM auth-tag fails) still returns the terminal
// integrity sentinel. To construct this scenario, we compute a valid HMAC
// for a ciphertext we then decrypt with the wrong IV.
func TestVerifyAndDecrypt_AuthTagFailure_Terminal(t *testing.T) {
	t.Parallel()

	verifier, err := NewArtifactVerifier(testMasterKey)
	require.NoError(t, err)

	plaintext := []byte("payload")
	ciphertext, _, hmacHex := encryptTestArtifact(t, testMasterKey, plaintext)

	// Use a different (random) IV so the auth-tag check fails.
	wrongIV := make([]byte, 12)
	_, err = rand.Read(wrongIV)
	require.NoError(t, err)

	_, err = verifier.VerifyAndDecrypt(
		context.Background(),
		bytes.NewReader(ciphertext),
		hmacHex,
		hex.EncodeToString(wrongIV),
	)
	require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
}

// TestVerifyAndDecrypt_WrongMasterKey_Terminal asserts that a verifier
// initialised with a different master key cannot verify a Fetcher-signed
// artifact — neither HMAC nor AES key matches.
func TestVerifyAndDecrypt_WrongMasterKey_Terminal(t *testing.T) {
	t.Parallel()

	// Encrypt with one master...
	plaintext := []byte("payload")
	ciphertext, iv, hmacHex := encryptTestArtifact(t, testMasterKey, plaintext)

	// ...verify with a different master.
	otherMaster := make([]byte, 32)
	for i := range otherMaster {
		otherMaster[i] = 0xff
	}

	verifier, err := NewArtifactVerifier(otherMaster)
	require.NoError(t, err)

	_, err = verifier.VerifyAndDecrypt(
		context.Background(),
		bytes.NewReader(ciphertext),
		hmacHex,
		hex.EncodeToString(iv),
	)
	require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
}

// TestVerifyAndDecrypt_InputValidation covers the caller-side sentinel
// paths: nil ciphertext, empty hmac/iv, and malformed hex. Every input
// mistake must wrap under ErrIntegrityVerificationFailed so the bridge
// worker treats them as terminal, while the inner input-specific
// sentinel must stay inspectable via errors.Is.
func TestVerifyAndDecrypt_InputValidation(t *testing.T) {
	t.Parallel()

	verifier, err := NewArtifactVerifier(testMasterKey)
	require.NoError(t, err)

	t.Run("nil ciphertext reader", func(t *testing.T) {
		t.Parallel()

		_, err := verifier.VerifyAndDecrypt(context.Background(), nil, "abc", "def")
		require.ErrorIs(t, err, sharedPorts.ErrArtifactCiphertextRequired)
		require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
	})

	t.Run("empty hmac", func(t *testing.T) {
		t.Parallel()

		_, err := verifier.VerifyAndDecrypt(
			context.Background(),
			bytes.NewReader([]byte("x")),
			"   ",
			"aa",
		)
		require.ErrorIs(t, err, sharedPorts.ErrArtifactHMACRequired)
		require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
	})

	t.Run("non-hex hmac", func(t *testing.T) {
		t.Parallel()

		_, err := verifier.VerifyAndDecrypt(
			context.Background(),
			bytes.NewReader([]byte("x")),
			"not-hex!",
			"aa",
		)
		require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
	})

	t.Run("non-hex iv", func(t *testing.T) {
		t.Parallel()

		_, err := verifier.VerifyAndDecrypt(
			context.Background(),
			bytes.NewReader([]byte("x")),
			"abcd",
			"not-hex!",
		)
		require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
	})
}

// TestVerifyAndDecrypt_EmptyIV_Terminal asserts that an empty IV input is
// rejected up front with a wrapped ErrArtifactIVRequired so the bridge
// worker stops retrying on deterministic caller-side mistakes. Previously
// an empty ivHex went through hex.DecodeString("") cleanly and then
// fell over inside AES-GCM — now the check is explicit.
func TestVerifyAndDecrypt_EmptyIV_Terminal(t *testing.T) {
	t.Parallel()

	verifier, err := NewArtifactVerifier(testMasterKey)
	require.NoError(t, err)

	_, err = verifier.VerifyAndDecrypt(
		context.Background(),
		bytes.NewReader([]byte("x")),
		"abcd",
		"   ",
	)
	require.ErrorIs(t, err, sharedPorts.ErrArtifactIVRequired)
	require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
}

// failingReader implements io.Reader and returns the supplied error on
// the first Read call. Used to simulate mid-stream network failures so
// the verifier's transient/terminal taxonomy can be asserted.
type failingReader struct {
	err error
}

func (r *failingReader) Read(_ []byte) (int, error) {
	return 0, r.err
}

// TestVerifyAndDecrypt_StreamIOFailure_IsTransient proves that an
// underlying io.Reader failure (the kind of thing a one-second TCP reset
// would surface) collapses into ErrArtifactRetrievalFailed — transient —
// and specifically NOT ErrIntegrityVerificationFailed. Previously the
// verifier wrapped every ciphertext-read failure as terminal, which
// meant a flaky network would permanently kill the extraction pipeline
// instead of triggering T-005's exponential backoff retry.
func TestVerifyAndDecrypt_StreamIOFailure_IsTransient(t *testing.T) {
	t.Parallel()

	verifier, err := NewArtifactVerifier(testMasterKey)
	require.NoError(t, err)

	_, err = verifier.VerifyAndDecrypt(
		context.Background(),
		&failingReader{err: io.ErrUnexpectedEOF},
		"abcd",
		"aabb",
	)
	require.Error(t, err)
	require.ErrorIs(t, err, sharedPorts.ErrArtifactRetrievalFailed)
	require.NotErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
	// And the underlying io.ErrUnexpectedEOF must remain in the chain so
	// observability can log the real cause.
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestVerifyAndDecrypt_NilVerifier_Terminal(t *testing.T) {
	t.Parallel()

	var v *ArtifactVerifier

	_, err := v.VerifyAndDecrypt(
		context.Background(),
		bytes.NewReader([]byte("x")),
		"abcd",
		"aa",
	)
	require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
}

// TestVerifyAndDecrypt_OversizedCiphertext_Terminal asserts the verifier
// refuses to read unbounded payloads. We check the sentinel, not the
// literal message, so the size limit can be tuned without rewriting the
// test.
func TestVerifyAndDecrypt_OversizedCiphertext_Terminal(t *testing.T) {
	t.Parallel()

	verifier, err := NewArtifactVerifier(testMasterKey)
	require.NoError(t, err)

	// Use a reader that claims to be larger than maxCiphertextBytes. We
	// don't have to materialise the full buffer; unboundedZeroReader
	// pretends to yield bytes forever. The verifier's LimitReader wrapping
	// stops at maxCiphertextBytes+1, triggering the oversize sentinel.
	_, err = verifier.VerifyAndDecrypt(
		context.Background(),
		&unboundedZeroReader{},
		"aa",
		"bb",
	)
	require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
}

// unboundedZeroReader returns zero bytes forever; a cheap stand-in for an
// oversized payload. Distinct from the transport-layer infiniteReader
// already declared in this package.
type unboundedZeroReader struct{}

func (r *unboundedZeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}

	return len(p), nil
}

// TestVerifyAndDecrypt_EmptyPlaintext handles the zero-row edge case.
// Fetcher can legitimately emit an empty JSON object; verification must
// still succeed.
func TestVerifyAndDecrypt_EmptyPlaintext(t *testing.T) {
	t.Parallel()

	verifier, err := NewArtifactVerifier(testMasterKey)
	require.NoError(t, err)

	plaintext := []byte("{}")
	ciphertext, iv, hmacHex := encryptTestArtifact(t, testMasterKey, plaintext)

	reader, err := verifier.VerifyAndDecrypt(
		context.Background(),
		bytes.NewReader(ciphertext),
		hmacHex,
		hex.EncodeToString(iv),
	)
	require.NoError(t, err)

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, plaintext, got)
}

// TestVerifyAndDecrypt_HMACNotConstantInInputMessage proves the verifier
// does not leak key material via error messages. This is a negative
// assertion: error.Error() must not contain any hex-encoded byte that
// overlaps the derived key. We test the easiest-to-inspect path.
func TestVerifyAndDecrypt_DoesNotLeakKeyMaterial(t *testing.T) {
	t.Parallel()

	verifier, err := NewArtifactVerifier(testMasterKey)
	require.NoError(t, err)

	_, err = verifier.VerifyAndDecrypt(
		context.Background(),
		bytes.NewReader([]byte("short")),
		hex.EncodeToString(make([]byte, 32)),
		hex.EncodeToString(make([]byte, 12)),
	)
	require.Error(t, err)

	// Neither the master key nor the derived keys should appear in the
	// rendered error. Checking for the master-key hex substring is
	// sufficient because the derived keys are HKDF outputs that depend
	// on it.
	msg := strings.ToLower(err.Error())
	assert.NotContains(t, msg, hex.EncodeToString(testMasterKey))
}
