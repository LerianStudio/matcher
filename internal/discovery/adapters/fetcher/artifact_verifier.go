// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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
	"strconv"
	"strings"

	"golang.org/x/crypto/hkdf"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// HKDF context strings — these are contract-locked by the Fetcher bridge
// design (D4 in FETCHER_MATCHER.md). Changing either string would break
// verification for every artifact Fetcher has ever signed. Both services
// derive identical keys from a shared APP_ENC_KEY only when these
// strings are exact byte-for-byte matches.
const (
	// hkdfContextHMAC is the HKDF info string for the HMAC-SHA256 key.
	// Must match Fetcher's key_deriver.go HKDF info exactly.
	hkdfContextHMAC = "fetcher-external-hmac-v1"

	// hkdfContextAES is the HKDF info string for the AES-256-GCM key.
	// Must match Fetcher's key_deriver.go HKDF info exactly.
	hkdfContextAES = "fetcher-external-aes-v1"

	// derivedKeyLen is the length of each derived key in bytes. 32 bytes
	// is required for both HMAC-SHA256 (input must be at least 32 bytes)
	// and AES-256-GCM (key must be exactly 32 bytes).
	derivedKeyLen = 32

	// minMasterKeyLen is the minimum raw master key length in bytes.
	// HKDF extract accepts any length input, but accepting a short master
	// key would silently weaken the derived keys below their nominal
	// security level. 32 bytes matches the Fetcher contract.
	minMasterKeyLen = 32

	// maxCiphertextBytes caps how much ciphertext the verifier will read
	// into memory. Fetcher has no documented upper bound, but unbounded
	// reads invite memory-exhaustion DoS. 256 MiB is well above realistic
	// extraction output sizes (Fetcher's output is flat JSON, not
	// multi-shard binary).
	maxCiphertextBytes = 256 * 1024 * 1024
)

// Sentinel errors for verifier construction. Runtime verification errors
// collapse to sharedPorts.ErrIntegrityVerificationFailed so callers see
// one terminal signal regardless of which crypto stage failed.
var (
	// ErrVerifierMasterKeyRequired indicates the verifier was constructed
	// without a master key. Empty APP_ENC_KEY at bootstrap time means
	// Matcher cannot verify any Fetcher artifact; we fail fast.
	ErrVerifierMasterKeyRequired = errors.New(
		"artifact verifier master key is required",
	)

	// ErrVerifierMasterKeyTooShort indicates the master key is shorter
	// than the contract minimum. HKDF would accept it but the derived
	// keys would inherit the weakness.
	ErrVerifierMasterKeyTooShort = errors.New(
		"artifact verifier master key is too short",
	)

	// errCiphertextExceedsLimit is an unexported sentinel used when the
	// retrieved ciphertext is larger than maxCiphertextBytes. Callers
	// never see this directly; it is wrapped into
	// sharedPorts.ErrIntegrityVerificationFailed so the outward signal
	// stays single-terminal (size-cap is considered an attack, not a
	// flaky network).
	errCiphertextExceedsLimit = errors.New("ciphertext exceeds byte limit")

	// errCiphertextReadFailed marks an underlying io.Reader failure (e.g.
	// io.ErrUnexpectedEOF, TCP reset mid-stream) distinct from the
	// size-cap violation. Verify-level callers collapse this into
	// sharedPorts.ErrArtifactRetrievalFailed so the bridge worker treats
	// it as a transient retry candidate — a one-second network blip must
	// not permanently fail the extraction pipeline.
	errCiphertextReadFailed = errors.New("ciphertext stream read failed")
)

// ArtifactVerifier implements sharedPorts.ArtifactTrustVerifier. It holds
// the derived HMAC and AES keys in memory; they live for the lifetime of
// the verifier and are never exposed via any getter, logger, or error
// message. The master key is not retained — once HKDF-extracted into the
// derived keys, the raw bytes can be discarded by the caller.
type ArtifactVerifier struct {
	hmacKey []byte
	aesKey  []byte
}

// Compile-time interface check.
var _ sharedPorts.ArtifactTrustVerifier = (*ArtifactVerifier)(nil)

// NewArtifactVerifier derives per-context keys from the shared master key
// and returns a ready-to-use verifier. The masterKey must be at least 32
// bytes; shorter keys silently weaken derived-key security so we reject
// them up front.
//
// Key derivation uses HKDF-SHA256 with salt=nil and the two
// contract-locked info strings. This matches Fetcher's key_deriver.go
// byte-for-byte; changing either string breaks every previously-signed
// artifact.
func NewArtifactVerifier(masterKey []byte) (*ArtifactVerifier, error) {
	if len(masterKey) == 0 {
		return nil, ErrVerifierMasterKeyRequired
	}

	if len(masterKey) < minMasterKeyLen {
		return nil, fmt.Errorf(
			"%w: want >= %d bytes, got %d",
			ErrVerifierMasterKeyTooShort,
			minMasterKeyLen,
			len(masterKey),
		)
	}

	hmacKey, err := deriveKey(masterKey, hkdfContextHMAC)
	if err != nil {
		return nil, fmt.Errorf("derive hmac key: %w", err)
	}

	aesKey, err := deriveKey(masterKey, hkdfContextAES)
	if err != nil {
		return nil, fmt.Errorf("derive aes key: %w", err)
	}

	return &ArtifactVerifier{hmacKey: hmacKey, aesKey: aesKey}, nil
}

// deriveKey runs HKDF-Extract-then-Expand with SHA-256 over the master
// key using the given info context string. Salt is intentionally nil;
// the info context string provides the domain separation.
func deriveKey(masterKey []byte, contextInfo string) ([]byte, error) {
	reader := hkdf.New(sha256.New, masterKey, nil, []byte(contextInfo))

	key := make([]byte, derivedKeyLen)
	if _, err := io.ReadFull(reader, key); err != nil {
		return nil, fmt.Errorf("hkdf read: %w", err)
	}

	return key, nil
}

// VerifyAndDecrypt validates the ciphertext against the supplied HMAC
// digest, then AES-GCM decrypts it and returns a reader over the
// plaintext. Failure modes fall into two retry classes:
//
// Terminal (ErrIntegrityVerificationFailed — never retry):
//   - nil ciphertext reader / empty hmacHex / empty ivHex: caller-side
//     mistakes wrapped so the inner ErrArtifact{Ciphertext,HMAC,IV}Required
//     sentinel remains inspectable via errors.Is.
//   - HMAC mismatch (constant-time compare, hmac.Equal).
//   - AES-GCM auth-tag failure (gcm.Open returned error) — caller cannot
//     meaningfully tell which crypto stage caught the corruption.
//   - Oversized ciphertext (> maxCiphertextBytes) — oversize is an attack
//     class, not flakiness, so we refuse to retry.
//   - Non-hex HMAC digest or IV — the decode can only fail on malformed
//     input, which retry cannot fix.
//
// Transient (ErrArtifactRetrievalFailed — safe to retry):
//   - Underlying io.Reader failure during ciphertext ingestion (TCP reset,
//     io.ErrUnexpectedEOF, connection close mid-stream). A one-second
//     network blip must not permanently fail the extraction; the bridge
//     worker retries the whole retrieve→verify→custody pipeline.
func (verifier *ArtifactVerifier) VerifyAndDecrypt(
	ctx context.Context,
	ciphertext io.Reader,
	hmacHex string,
	ivHex string,
) (io.Reader, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // tracer only

	_, span := tracer.Start(ctx, "fetcher.artifact_verifier.verify_and_decrypt")
	defer span.End()

	if verifier == nil {
		err := fmt.Errorf("%w: verifier not initialised", sharedPorts.ErrIntegrityVerificationFailed)
		libOpentelemetry.HandleSpanError(span, "nil verifier", err)

		return nil, err
	}

	if ciphertext == nil {
		// Wrapped with ErrIntegrityVerificationFailed so the bridge worker
		// treats missing input as terminal (retrying an absent reader is
		// futile). The inner sentinel remains inspectable via errors.Is so
		// tests and observability can distinguish cause.
		err := fmt.Errorf("%w: %w", sharedPorts.ErrIntegrityVerificationFailed, sharedPorts.ErrArtifactCiphertextRequired)
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "ciphertext required", err)

		return nil, err
	}

	if strings.TrimSpace(hmacHex) == "" {
		err := fmt.Errorf("%w: %w", sharedPorts.ErrIntegrityVerificationFailed, sharedPorts.ErrArtifactHMACRequired)
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "hmac required", err)

		return nil, err
	}

	if strings.TrimSpace(ivHex) == "" {
		err := fmt.Errorf("%w: %w", sharedPorts.ErrIntegrityVerificationFailed, sharedPorts.ErrArtifactIVRequired)
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "iv required", err)

		return nil, err
	}

	expectedMAC, err := hex.DecodeString(strings.TrimSpace(hmacHex))
	if err != nil {
		wrapped := fmt.Errorf("%w: hmac digest is not hex", sharedPorts.ErrIntegrityVerificationFailed)
		libOpentelemetry.HandleSpanError(span, "hmac hex decode failed", wrapped)

		return nil, wrapped
	}

	iv, err := hex.DecodeString(strings.TrimSpace(ivHex))
	if err != nil {
		wrapped := fmt.Errorf("%w: iv is not hex", sharedPorts.ErrIntegrityVerificationFailed)
		libOpentelemetry.HandleSpanError(span, "iv hex decode failed", wrapped)

		return nil, wrapped
	}

	ciphertextBytes, err := readBoundedCiphertext(ciphertext)
	if err != nil {
		// Stream IO failures are transient — a one-second reset must not
		// permanently fail the extraction. Size-cap violations are
		// terminal: retrying a bomb is pointless and risky.
		if errors.Is(err, errCiphertextReadFailed) {
			wrapped := fmt.Errorf("%w: %w", sharedPorts.ErrArtifactRetrievalFailed, err)
			libOpentelemetry.HandleSpanError(span, "ciphertext stream read failed", wrapped)

			return nil, wrapped
		}

		wrapped := fmt.Errorf("%w: %w", sharedPorts.ErrIntegrityVerificationFailed, err)
		libOpentelemetry.HandleSpanError(span, "ciphertext read failed", wrapped)

		return nil, wrapped
	}

	if err := verifyHMAC(verifier.hmacKey, ciphertextBytes, expectedMAC); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "hmac verification failed", err)

		return nil, err
	}

	plaintext, err := decryptAESGCM(verifier.aesKey, iv, ciphertextBytes)
	if err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "aes-gcm decryption failed", err)

		return nil, err
	}

	return bytes.NewReader(plaintext), nil
}

// readBoundedCiphertext reads at most maxCiphertextBytes+1 into memory;
// anything larger returns an error without materialising the whole buffer.
//
// Two distinct failure modes with different retry semantics:
//   - Underlying io.Reader failure (network reset, ErrUnexpectedEOF) is
//     wrapped in errCiphertextReadFailed so VerifyAndDecrypt can report it
//     as transient (sharedPorts.ErrArtifactRetrievalFailed).
//   - Size-cap breach is wrapped in errCiphertextExceedsLimit which stays
//     terminal (sharedPorts.ErrIntegrityVerificationFailed) — oversized
//     payloads are an attack class, not flakiness.
func readBoundedCiphertext(r io.Reader) ([]byte, error) {
	limited := io.LimitReader(r, maxCiphertextBytes+1)

	buf, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errCiphertextReadFailed, err)
	}

	if int64(len(buf)) > maxCiphertextBytes {
		return nil, fmt.Errorf(
			"%w: %s bytes",
			errCiphertextExceedsLimit,
			strconv.FormatInt(maxCiphertextBytes, 10),
		)
	}

	return buf, nil
}

// verifyHMAC computes HMAC-SHA256 over ciphertext and compares it to
// the expected digest in constant time.
func verifyHMAC(key, ciphertext, expected []byte) error {
	mac := hmac.New(sha256.New, key)
	// mac.Write on hmac.New always returns (n, nil); error-check is a
	// belt-and-braces since the interface allows errors.
	if _, err := mac.Write(ciphertext); err != nil {
		return fmt.Errorf("%w: hmac write failed", sharedPorts.ErrIntegrityVerificationFailed)
	}

	computed := mac.Sum(nil)

	// hmac.Equal is constant-time regardless of input length; this is the
	// contract that blocks timing side-channels. Never use bytes.Equal
	// here — it short-circuits on mismatch and leaks the first-diverging
	// byte position in timing.
	if !hmac.Equal(computed, expected) {
		return sharedPorts.ErrIntegrityVerificationFailed
	}

	return nil
}

// decryptAESGCM validates authenticated ciphertext with AES-256-GCM. An
// auth-tag failure from gcm.Open is the same terminal signal as an HMAC
// mismatch from the caller's perspective, so both return the same
// sentinel.
func decryptAESGCM(key, iv, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		// aes.NewCipher only fails on wrong key size; our derived key is
		// always 32 bytes so this branch is defensive only.
		return nil, fmt.Errorf("%w: aes key init failed", sharedPorts.ErrIntegrityVerificationFailed)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: gcm init failed", sharedPorts.ErrIntegrityVerificationFailed)
	}

	if len(iv) != gcm.NonceSize() {
		// Wrong IV size is an input-side corruption, same terminal signal.
		return nil, fmt.Errorf(
			"%w: iv size %d does not match gcm nonce size %d",
			sharedPorts.ErrIntegrityVerificationFailed,
			len(iv),
			gcm.NonceSize(),
		)
	}

	plaintext, err := gcm.Open(nil, iv, ciphertext, nil)
	if err != nil {
		// AES-GCM open failure == auth-tag mismatch == terminal.
		// Deliberately do not embed the underlying error: its message can
		// reveal whether the failure was auth-tag vs malformed ciphertext,
		// and we want a single undifferentiated signal.
		return nil, sharedPorts.ErrIntegrityVerificationFailed
	}

	return plaintext, nil
}
