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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/hkdf"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// This file holds Gate 5 property-based tests for the ArtifactVerifier.
// Gate 3 unit tests pin a handful of hand-picked fixtures; these tests
// exercise the same contracts over hundreds of generated inputs so we can
// catch classes of bugs (off-by-one bit flips, context collisions, state
// carry-over between calls) that example-based tests miss by construction.
//
// Anti-cheat note (from the task brief): the generators below invoke
// golang.org/x/crypto/hkdf directly rather than calling the unexported
// deriveKey helper. Copying the production code into the harness would
// rubber-stamp any bug that lives in deriveKey. We keep the context
// strings ("fetcher-external-hmac-v1", "fetcher-external-aes-v1") exactly
// because those are the contract with Fetcher — not implementation detail.

// propertyMaxCount is the default iteration budget for quick.Check in this
// file. 100 is the Ring standard; a few bit-flip properties bump to 200
// because their state space is larger (bits-in-triple × inputs).
const propertyMaxCount = 100

// seededConfig builds a quick.Config with a fixed seed so failures are
// reproducible across CI runs. Seeds are chosen per property to avoid
// accidentally rediscovering the same counterexample across tests.
func seededConfig(maxCount int, seed int64) *quick.Config {
	return &quick.Config{
		MaxCount: maxCount,
		Rand:     rand.New(rand.NewSource(seed)),
	}
}

// hkdfDeriveIndependent re-implements the production HKDF derivation via
// the stdlib-adjacent golang.org/x/crypto/hkdf package. The shape matches
// the production wrapper in artifact_verifier.go (salt=nil, 32-byte
// output, SHA-256), but the implementation path is distinct so a subtle
// bug in the production wrapper (e.g. wrong hash, non-nil salt, wrong
// length) would show up as a property failure rather than a rubber stamp.
func hkdfDeriveIndependent(master []byte, info string) ([]byte, error) {
	reader := hkdf.New(sha256.New, master, nil, []byte(info))

	key := make([]byte, 32)
	if _, err := io.ReadFull(reader, key); err != nil {
		return nil, fmt.Errorf("independent hkdf read: %w", err)
	}

	return key, nil
}

// encryptForProperty mirrors the Fetcher signing side end-to-end. It is
// intentionally re-derived here instead of calling encryptTestArtifact
// from artifact_verifier_test.go so the property harness survives future
// refactors of that helper. Returns (ciphertext, hmac-hex, iv-hex).
func encryptForProperty(t *testing.T, master, plaintext, iv []byte) ([]byte, string, string) {
	t.Helper()

	hmacKey, err := hkdfDeriveIndependent(master, hkdfContextHMAC)
	require.NoError(t, err)

	aesKey, err := hkdfDeriveIndependent(master, hkdfContextAES)
	require.NoError(t, err)

	block, err := aes.NewCipher(aesKey)
	require.NoError(t, err)

	gcm, err := cipher.NewGCM(block)
	require.NoError(t, err)

	ciphertext := gcm.Seal(nil, iv, plaintext, nil)

	mac := hmac.New(sha256.New, hmacKey)
	_, err = mac.Write(ciphertext)
	require.NoError(t, err)

	return ciphertext, hex.EncodeToString(mac.Sum(nil)), hex.EncodeToString(iv)
}

// generatePropertyMaster produces a deterministic-but-varied 32-byte
// master key from a random source. We avoid crypto/rand here: quick.Check
// wants determinism seeded from its own Rand for reproducibility.
func generatePropertyMaster(rng *rand.Rand) []byte {
	out := make([]byte, 32)

	_, _ = rng.Read(out) // rand.Rand.Read never returns an error.

	return out
}

// generatePropertyIV produces a 12-byte GCM nonce from a seeded Rand.
// Deterministic by seed — only safe in tests.
func generatePropertyIV(rng *rand.Rand) []byte {
	iv := make([]byte, 12)

	_, _ = rng.Read(iv)

	return iv
}

// TestProperty_HKDF_Deterministic (Invariant 1): for any master key and
// context string, deriveKey produces the same 32-byte output on every
// call. No hidden state, no clock coupling, no per-call entropy.
//
// This is the linchpin of the Fetcher contract: if Matcher's derivation
// is non-deterministic, every verification becomes a coin toss and
// plaintext silently disappears behind intermittent auth-tag failures.
func TestProperty_HKDF_Deterministic(t *testing.T) {
	t.Parallel()

	cfg := seededConfig(propertyMaxCount, 101)

	// The two production contexts plus the same strings padded so the
	// property does not collapse to a single case when the generator
	// shrinks to empty.
	contexts := []string{
		hkdfContextHMAC,
		hkdfContextAES,
		"fetcher-external-hmac-v1",
		"fetcher-external-aes-v1",
	}

	property := func(seedPick uint16, ctxIdx uint8) bool {
		masterRNG := rand.New(rand.NewSource(int64(seedPick) + 1))
		master := generatePropertyMaster(masterRNG)
		info := contexts[int(ctxIdx)%len(contexts)]

		first, err := deriveKey(master, info)
		if err != nil {
			return false
		}

		second, err := deriveKey(master, info)
		if err != nil {
			return false
		}

		third, err := deriveKey(master, info)
		if err != nil {
			return false
		}

		return bytes.Equal(first, second) && bytes.Equal(second, third) && len(first) == 32
	}

	require.NoError(t, quick.Check(property, cfg))
}

// TestProperty_HKDF_ContextSeparation (Invariant 2): different info
// strings produce distinct derived keys. This is how the HMAC and AES
// keys stay independent even though they share the master.
//
// The property checks two things: (a) inequality, and (b) substantial
// bit-level divergence. A weak implementation that produced similar but
// not identical outputs would pass strict inequality alone; the Hamming
// distance guard rejects that bug class.
func TestProperty_HKDF_ContextSeparation(t *testing.T) {
	t.Parallel()

	cfg := seededConfig(propertyMaxCount, 202)

	property := func(masterSeed uint16, ctxA, ctxB string) bool {
		// Skip empty / identical contexts: invariant is "distinct in ⇒
		// distinct out", trivially true when inputs are equal.
		if ctxA == ctxB || ctxA == "" || ctxB == "" {
			return true
		}

		masterRNG := rand.New(rand.NewSource(int64(masterSeed) + 1))
		master := generatePropertyMaster(masterRNG)

		keyA, err := deriveKey(master, ctxA)
		if err != nil {
			return false
		}

		keyB, err := deriveKey(master, ctxB)
		if err != nil {
			return false
		}

		if bytes.Equal(keyA, keyB) {
			return false
		}

		// Require >= 32 bits of Hamming distance. HKDF-SHA256 outputs
		// behave as pseudo-random given distinct info; the expected
		// distance for 32-byte = 256-bit outputs is ~128, so 32 is a very
		// loose floor chosen to reject weak implementations, not to pin a
		// statistical bound.
		return hammingDistance(keyA, keyB) >= 32
	}

	require.NoError(t, quick.Check(property, cfg))
}

// TestProperty_HKDF_ContextSeparation_ProductionContexts pins the
// specific contract: the two production context strings MUST derive
// different keys for any master key. This is a narrower version of
// Invariant 2 focused on the exact contract.
func TestProperty_HKDF_ContextSeparation_ProductionContexts(t *testing.T) {
	t.Parallel()

	cfg := seededConfig(propertyMaxCount, 203)

	property := func(masterSeed uint32) bool {
		masterRNG := rand.New(rand.NewSource(int64(masterSeed) + 1))
		master := generatePropertyMaster(masterRNG)

		hmacKey, err := deriveKey(master, hkdfContextHMAC)
		if err != nil {
			return false
		}

		aesKey, err := deriveKey(master, hkdfContextAES)
		if err != nil {
			return false
		}

		return !bytes.Equal(hmacKey, aesKey) && hammingDistance(hmacKey, aesKey) >= 32
	}

	require.NoError(t, quick.Check(property, cfg))
}

// TestProperty_VerifyAndDecrypt_Roundtrip (Invariant 3): for any valid
// master key and plaintext, if we produce ciphertext+HMAC+IV via the
// independent HKDF path and hand them to the verifier, we get the exact
// plaintext back.
//
// Why this matters: the verifier and our helper must agree on every
// crypto parameter (context strings, algorithms, nonce size, HMAC shape).
// If ANY parameter drifts, this property fails and tells us exactly
// which artifact shape stopped verifying.
func TestProperty_VerifyAndDecrypt_Roundtrip(t *testing.T) {
	t.Parallel()

	cfg := seededConfig(propertyMaxCount, 304)

	property := func(masterSeed uint16, plaintextLen uint8, ivSeed uint32) bool {
		masterRNG := rand.New(rand.NewSource(int64(masterSeed) + 1))
		master := generatePropertyMaster(masterRNG)

		// Bound plaintext to 255 bytes to keep the test fast. The
		// invariant is size-independent; bigger payloads do not exercise
		// a different code path in AES-GCM.
		plaintext := make([]byte, int(plaintextLen))
		_, _ = masterRNG.Read(plaintext)

		ivRNG := rand.New(rand.NewSource(int64(ivSeed) + 7))
		iv := generatePropertyIV(ivRNG)

		ciphertext, hmacHex, ivHex := encryptForProperty(t, master, plaintext, iv)

		verifier, err := NewArtifactVerifier(master)
		if err != nil {
			return false
		}

		reader, err := verifier.VerifyAndDecrypt(
			context.Background(),
			bytes.NewReader(ciphertext),
			hmacHex,
			ivHex,
		)
		if err != nil {
			return false
		}

		got, err := io.ReadAll(reader)
		if err != nil {
			return false
		}

		return bytes.Equal(plaintext, got)
	}

	require.NoError(t, quick.Check(property, cfg))
}

// TestProperty_VerifyAndDecrypt_TamperDetection (Invariant 4): for any
// valid triple (ciphertext, hmac, iv), flipping any single bit in any
// component causes VerifyAndDecrypt to return
// ErrIntegrityVerificationFailed.
//
// We iterate three scopes per generated payload — ciphertext byte, HMAC
// byte, IV byte — and flip one bit at a randomly chosen index inside the
// scope. Doing three distinct scope flips per input exercises HMAC-
// verification (ciphertext bit flip), HMAC-decode (hmac bit flip), and
// AES-GCM auth-tag (iv bit flip) in one property.
func TestProperty_VerifyAndDecrypt_TamperDetection(t *testing.T) {
	t.Parallel()

	// Bumped to 200 because each generated input is tested against three
	// different bit-flip sites; the effective test count is 600.
	cfg := seededConfig(200, 405)

	property := func(masterSeed uint16, plaintextLen uint8, ivSeed, flipSeed uint32) bool {
		masterRNG := rand.New(rand.NewSource(int64(masterSeed) + 1))
		master := generatePropertyMaster(masterRNG)

		plaintext := make([]byte, int(plaintextLen))
		_, _ = masterRNG.Read(plaintext)

		ivRNG := rand.New(rand.NewSource(int64(ivSeed) + 7))
		iv := generatePropertyIV(ivRNG)

		ciphertext, hmacHex, ivHex := encryptForProperty(t, master, plaintext, iv)

		verifier, err := NewArtifactVerifier(master)
		if err != nil {
			return false
		}

		flipRNG := rand.New(rand.NewSource(int64(flipSeed) + 13))

		// (a) Flip one bit in the ciphertext.
		if len(ciphertext) > 0 {
			tamperedCT := flipOneBit(ciphertext, flipRNG)

			_, err := verifier.VerifyAndDecrypt(
				context.Background(),
				bytes.NewReader(tamperedCT),
				hmacHex,
				ivHex,
			)
			if !errors.Is(err, sharedPorts.ErrIntegrityVerificationFailed) {
				return false
			}
		}

		// (b) Flip one bit in the HMAC hex (decode, flip in binary,
		// re-encode). Flipping a hex char directly would produce an
		// unrelated mutation; binary-level flip is what the adversary
		// model describes.
		hmacBytes, decodeErr := hex.DecodeString(hmacHex)
		if decodeErr != nil {
			return false
		}

		tamperedHMAC := flipOneBit(hmacBytes, flipRNG)

		_, err = verifier.VerifyAndDecrypt(
			context.Background(),
			bytes.NewReader(ciphertext),
			hex.EncodeToString(tamperedHMAC),
			ivHex,
		)
		if !errors.Is(err, sharedPorts.ErrIntegrityVerificationFailed) {
			return false
		}

		// (c) Flip one bit in the IV. AES-GCM treats IV as part of the
		// authentication input; any flip should fault the auth tag.
		ivBytes, decodeErr := hex.DecodeString(ivHex)
		if decodeErr != nil {
			return false
		}

		tamperedIV := flipOneBit(ivBytes, flipRNG)

		_, err = verifier.VerifyAndDecrypt(
			context.Background(),
			bytes.NewReader(ciphertext),
			hmacHex,
			hex.EncodeToString(tamperedIV),
		)

		return errors.Is(err, sharedPorts.ErrIntegrityVerificationFailed)
	}

	require.NoError(t, quick.Check(property, cfg))
}

// TestProperty_KeyMaterial_Separation (Invariant 5): for two distinct
// master keys with the same plaintext and IV, the produced ciphertexts
// differ AND the HMACs differ. This is a direct consequence of Invariant
// 2 (context separation) plus AES-GCM's key dependence.
//
// The property intentionally fixes plaintext and IV across the two
// encryptions — if ciphertext or HMAC collided under these constraints,
// the master key would be derivable from the difference and the
// one-wayness assumption of HKDF-SHA256 would be broken.
func TestProperty_KeyMaterial_Separation(t *testing.T) {
	t.Parallel()

	cfg := seededConfig(propertyMaxCount, 506)

	property := func(seedA, seedB uint16, plaintextLen uint8, ivSeed uint32) bool {
		if seedA == seedB {
			return true // identical seeds ⇒ identical masters ⇒ trivially skip.
		}

		rngA := rand.New(rand.NewSource(int64(seedA) + 1))
		masterA := generatePropertyMaster(rngA)

		rngB := rand.New(rand.NewSource(int64(seedB) + 1))
		masterB := generatePropertyMaster(rngB)

		if bytes.Equal(masterA, masterB) {
			return true // collision in the 32-byte seed expansion; skip.
		}

		// Shared plaintext + IV pins the only free variable to the key
		// material itself.
		plaintext := make([]byte, int(plaintextLen))
		plaintextRNG := rand.New(rand.NewSource(int64(ivSeed) + 7))
		_, _ = plaintextRNG.Read(plaintext)

		ivRNG := rand.New(rand.NewSource(int64(ivSeed) + 17))
		iv := generatePropertyIV(ivRNG)

		ctA, hmacA, _ := encryptForProperty(t, masterA, plaintext, iv)
		ctB, hmacB, _ := encryptForProperty(t, masterB, plaintext, iv)

		if bytes.Equal(ctA, ctB) {
			return false
		}

		if hmacA == hmacB {
			return false
		}

		// Cross-verification must fail: a verifier built from masterA
		// must reject an artifact signed with masterB, and vice versa.
		verA, err := NewArtifactVerifier(masterA)
		if err != nil {
			return false
		}

		_, err = verA.VerifyAndDecrypt(
			context.Background(),
			bytes.NewReader(ctB),
			hmacB,
			hex.EncodeToString(iv),
		)
		if !errors.Is(err, sharedPorts.ErrIntegrityVerificationFailed) {
			return false
		}

		verB, err := NewArtifactVerifier(masterB)
		if err != nil {
			return false
		}

		_, err = verB.VerifyAndDecrypt(
			context.Background(),
			bytes.NewReader(ctA),
			hmacA,
			hex.EncodeToString(iv),
		)

		return errors.Is(err, sharedPorts.ErrIntegrityVerificationFailed)
	}

	require.NoError(t, quick.Check(property, cfg))
}

// hammingDistance counts the total number of differing bits across two
// equal-length byte slices. Used to assert "substantial" divergence
// between derived keys in the context-separation property. Returns 0 if
// lengths differ — callers always compare two 32-byte HKDF outputs so
// the length check is defensive.
func hammingDistance(left, right []byte) int {
	if len(left) != len(right) {
		return 0
	}

	dist := 0

	for i := range left {
		xor := left[i] ^ right[i]
		for xor != 0 {
			dist += int(xor & 1)
			xor >>= 1
		}
	}

	return dist
}

// flipOneBit returns a copy of src with a single bit flipped at a random
// offset. The flip position is derived from the provided Rand so the
// mutation is reproducible when a property fails. Returns src unchanged
// when src is empty (caller guards against that case).
func flipOneBit(src []byte, rng *rand.Rand) []byte {
	out := make([]byte, len(src))
	copy(out, src)

	if len(out) == 0 {
		return out
	}

	byteIdx := rng.Intn(len(out))
	bitIdx := rng.Intn(8)
	out[byteIdx] ^= 1 << bitIdx

	return out
}
