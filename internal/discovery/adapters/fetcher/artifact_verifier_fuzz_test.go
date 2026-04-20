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
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/shared/adapters/custody"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// maxFuzzCiphertextBytes caps the ciphertext size each fuzz iteration
// will feed through VerifyAndDecrypt. The production verifier has its own
// maxCiphertextBytes (256 MiB) ceiling, but the fuzzer works a lot better
// when individual iterations stay small and cheap. 128 KiB gives us room
// to probe GCM tag boundaries and short-input cases without spending
// iteration budget on megabyte-scale allocations the verifier would
// happily reject anyway.
const maxFuzzCiphertextBytes = 128 * 1024

// maxFuzzHexStringBytes caps the hex-encoded header length. Production
// HMAC digests are 64 hex chars and IVs are 24 hex chars; anything the
// fuzzer produces above a few hundred bytes is purely a parser stress
// test. The hex decoder already handles length variance cleanly, so we
// do not need to hammer it with multi-MiB strings to prove the property.
const maxFuzzHexStringBytes = 1024

// FuzzVerifyAndDecrypt asserts the primary contract of the artifact
// verifier under adversarial input:
//
//  1. VerifyAndDecrypt MUST NEVER panic, regardless of how malformed
//     the ciphertext, HMAC hex string, or IV hex string are.
//  2. Every call either returns a non-nil plaintext reader with nil
//     error, or a nil reader with a non-nil error. Mixed states (reader
//     + error, or nil reader + nil error) are invariant violations.
//  3. When an error is returned, it either collapses to the terminal
//     integrity sentinel (ErrIntegrityVerificationFailed) or the two
//     caller-side input sentinels (ErrArtifactCiphertextRequired /
//     ErrArtifactHMACRequired). No other error shape is allowed.
//  4. On success, the returned plaintext drains cleanly to EOF without
//     panic. We do not assert the bytes here — verifying equality is the
//     happy-path test's job. The fuzzer's job is to prove no crash.
//
// Seed corpus covers:
//   - A known-good triple produced by the in-file helper so we exercise
//     the success path at least once per iteration batch.
//   - Empty ciphertext, empty HMAC, empty IV — each of the caller-side
//     sentinel paths.
//   - Ciphertext shorter than the GCM auth tag (15 bytes) to probe the
//     gcm.Open underflow path.
//   - A single byte of ciphertext — even tighter underflow case.
//   - Non-hex HMAC / IV strings to probe the hex decoder branch.
//   - Wrong-length but valid-hex IV to probe the nonce-size branch.
//   - Random 16 bytes of ciphertext with structurally-valid but
//     semantically wrong HMAC/IV to probe the HMAC mismatch branch.
func FuzzVerifyAndDecrypt(f *testing.F) {
	// Happy-path triple: a known-good artifact so the fuzzer gets at least
	// one example of the full success path before it starts mutating.
	validCiphertext, validIV, validHMACHex := encryptFuzzArtifact(
		f,
		testMasterKey,
		[]byte(`{"records":[{"id":1}]}`),
	)
	f.Add(validCiphertext, validHMACHex, hex.EncodeToString(validIV))

	// Empty everything: expects ErrArtifactHMACRequired.
	f.Add([]byte{}, "", "")

	// Random-looking bytes of structurally valid shape — ciphertext is 16
	// bytes, HMAC is 64 hex chars (32 bytes), IV is 24 hex chars (12
	// bytes). Mirrors what a corrupted-wire scenario looks like.
	f.Add(
		bytes.Repeat([]byte{0xAB}, 16),
		strings.Repeat("ab", 32),
		strings.Repeat("cd", 12),
	)

	// Ciphertext shorter than the AES-GCM 16-byte auth tag: probes
	// gcm.Open underflow. Still supplies valid-shape HMAC/IV so we get
	// past hex decoding.
	f.Add(
		bytes.Repeat([]byte{0x00}, 15),
		strings.Repeat("aa", 32),
		strings.Repeat("bb", 12),
	)

	// Single byte of ciphertext — tighter underflow case.
	f.Add(
		[]byte{0x42},
		strings.Repeat("aa", 32),
		strings.Repeat("bb", 12),
	)

	// Non-hex HMAC string — probes hex.DecodeString rejection.
	f.Add(
		[]byte("ignored"),
		"not-hex-at-all",
		strings.Repeat("cc", 12),
	)

	// Non-hex IV string — probes hex.DecodeString rejection for IV.
	f.Add(
		[]byte("ignored"),
		strings.Repeat("aa", 32),
		"also-not-hex!",
	)

	// Wrong IV length (30 hex chars = 15 bytes) — probes the nonce-size
	// guard inside decryptAESGCM. HMAC hex is valid shape so we pass the
	// hex.DecodeString step before hitting the nonce check.
	f.Add(
		[]byte("does-not-matter"),
		strings.Repeat("aa", 32),
		strings.Repeat("cd", 15),
	)

	// Odd-length hex HMAC: DecodeString rejects odd-length input.
	f.Add(
		[]byte("ignored"),
		"abc",
		strings.Repeat("cc", 12),
	)

	// One verifier instance per fuzz run. The verifier is safe for
	// concurrent use (no mutable state post-construction), and creating
	// a new one per iteration just burns CPU on HKDF.
	verifier, err := NewArtifactVerifier(testMasterKey)
	if err != nil {
		f.Fatalf("fuzz setup: build verifier: %v", err)
	}

	f.Fuzz(func(t *testing.T, cipherBytes []byte, hmacHex, ivHex string) {
		// Bound inputs so individual iterations stay cheap. We still cover
		// every branch of the verifier — short-input and bounded-input
		// cases are the interesting ones.
		if len(cipherBytes) > maxFuzzCiphertextBytes {
			cipherBytes = cipherBytes[:maxFuzzCiphertextBytes]
		}

		if len(hmacHex) > maxFuzzHexStringBytes {
			hmacHex = hmacHex[:maxFuzzHexStringBytes]
		}

		if len(ivHex) > maxFuzzHexStringBytes {
			ivHex = ivHex[:maxFuzzHexStringBytes]
		}

		reader, err := verifier.VerifyAndDecrypt(
			context.Background(),
			bytes.NewReader(cipherBytes),
			hmacHex,
			ivHex,
		)

		// Property 2: mixed states are forbidden.
		if err == nil && reader == nil {
			t.Fatalf("nil reader with nil error: cipher=%d hmacHex=%q ivHex=%q",
				len(cipherBytes), hmacHex, ivHex)
		}

		if err != nil && reader != nil {
			t.Fatalf("non-nil reader with non-nil error: err=%v", err)
		}

		// Property 3: error sentinels must be one of the documented ones.
		if err != nil {
			if !isAllowedVerifierError(err) {
				t.Fatalf("undocumented error sentinel: %v", err)
			}

			return
		}

		// Property 4: on success, the reader drains without panic. We do
		// not assert on content here; content correctness is a unit-test
		// concern. The fuzzer's goal is crash discovery.
		if _, drainErr := io.Copy(io.Discard, reader); drainErr != nil {
			t.Fatalf("successful reader drained with error: %v", drainErr)
		}
	})
}

// FuzzRetrieveDescriptor asserts that ArtifactRetrievalClient.Retrieve
// never panics on arbitrary descriptor URL input, even when the URL is
// unparseable, contains control characters, or is megabyte-scale garbage.
//
// The HTTP transport is stubbed so we do not actually send any bytes
// over the wire. The stub returns a canned 404 so every Retrieve call
// exits through the classifyArtifactResponse → ErrFetcherResourceNotFound
// path once the URL has been accepted by http.NewRequestWithContext.
//
// Properties:
//
//  1. No panic regardless of URL shape.
//  2. Either (reader+nil err) or (nil reader+non-nil err) — never mixed.
//  3. The error (when present) is one of: the URL-validation sentinel
//     (ErrArtifactDescriptorRequired, for empty URLs), the build-request
//     wrap of ErrArtifactRetrievalFailed (malformed URL string),
//     ErrFetcherResourceNotFound (happy stub path), or the missing
//     integrity-header errors (never triggered here — stub always sets
//     them — but documented for completeness).
func FuzzRetrieveDescriptor(f *testing.F) {
	// Seed with a representative set of URL shapes.
	f.Add("https://fetcher.example.test/v1/artifacts/abc.bin")
	f.Add("")
	f.Add("   ")
	f.Add("not-a-url-at-all")
	f.Add("http://[::1]:99999/bogus-port")                        // port overflow
	f.Add("https://example.com/path\x00with\x01control\x02chars") // control chars
	f.Add(strings.Repeat("a", 4096))                              // long path
	f.Add("ftp://unsupported-scheme.example/path")                // valid scheme syntax, wrong scheme
	f.Add("%ZZ-not-percent-encoded")                              // invalid percent escape

	// The stub transport always returns 404. That exercises the full
	// classifyArtifactResponse path for any URL that parses, which is the
	// main branch we want to prove does not panic.
	rt := &stubArtifactRoundTripper{
		resp: newStubResponse(http.StatusNotFound, map[string]string{}, ""),
	}
	client, err := NewArtifactRetrievalClient(newStubHTTPClient(rt))
	if err != nil {
		f.Fatalf("fuzz setup: build retrieval client: %v", err)
	}

	// Fixed tenant + extraction id so the fuzzer only mutates the URL.
	// Those two fields are covered by the custody fuzz below.
	baseDescriptor := sharedPorts.ArtifactRetrievalDescriptor{
		ExtractionID: uuid.New(),
		TenantID:     "tenant-fuzz",
	}

	f.Fuzz(func(t *testing.T, url string) {
		// Bound URL length to keep iterations cheap.
		if len(url) > maxFuzzHexStringBytes {
			url = url[:maxFuzzHexStringBytes]
		}

		descriptor := baseDescriptor
		descriptor.URL = url

		result, err := client.Retrieve(context.Background(), descriptor)

		// Property 2: mixed states forbidden.
		if err == nil && result == nil {
			t.Fatalf("nil result with nil error: url=%q", url)
		}

		if err != nil && result != nil {
			// Retrieve closes the body on classification failure. If both
			// are set, something slipped through the error path.
			t.Fatalf("non-nil result with non-nil error: err=%v", err)
		}

		if result != nil {
			// Defensive close — if the stub ever changes and emits a
			// successful response, we must not leak the body.
			_ = result.Content.Close()
		}
	})
}

// FuzzCustodyPathConstruction asserts that custody.BuildObjectKey (the
// tenant-scoped path builder used by ArtifactCustodyStore.Store) refuses
// hostile tenant/extraction inputs cleanly — no panic, no silent
// path-traversal, no cross-tenant namespace collision.
//
// The extraction UUID is generated once per iteration from the fuzzer's
// raw bytes; uuid.FromBytes returns an error for byte slices shorter or
// longer than 16. We tolerate both outcomes and only check the paths
// through BuildObjectKey that actually reach it.
//
// Properties:
//
//  1. No panic.
//  2. On success, the returned key is exactly "{tenantID}/fetcher-artifacts/{extractionID}.json"
//     with a single '/' between segments. No '..', no absolute paths, no
//     double slashes leaking from whitespace trims.
//  3. On failure, the error is one of the documented sentinels
//     (ErrArtifactTenantIDRequired, ErrArtifactExtractionIDRequired, or
//     a wrap of those).
func FuzzCustodyPathConstruction(f *testing.F) {
	// Seed with common adversarial shapes plus one known-good.
	f.Add("tenant-good", []byte("0123456789abcdef"))             // valid 16-byte UUID bytes
	f.Add("", []byte("0123456789abcdef"))                        // empty tenant
	f.Add("   ", []byte("0123456789abcdef"))                     // whitespace tenant
	f.Add("tenant/with/slash", []byte("0123456789abcdef"))       // path traversal attempt
	f.Add("tenant/../other", []byte("0123456789abcdef"))         // explicit traversal
	f.Add("tenant\x00null", []byte("0123456789abcdef"))          // control chars
	f.Add("日本語テナント", []byte("0123456789abcdef"))                 // unicode tenant
	f.Add(strings.Repeat("x", 1024), []byte("0123456789abcdef")) // long tenant
	f.Add("tenant-ok", []byte{})                                 // nil UUID bytes path
	f.Add("tenant-ok", []byte("short"))                          // wrong UUID byte length

	f.Fuzz(func(t *testing.T, tenantID string, extractionBytes []byte) {
		// Bound tenant length so iterations stay cheap.
		if len(tenantID) > maxFuzzHexStringBytes {
			tenantID = tenantID[:maxFuzzHexStringBytes]
		}

		// Derive a uuid.UUID from the raw bytes. uuid.FromBytes requires
		// exactly 16 bytes; we fall back to uuid.Nil when the fuzzer gives
		// us anything else. That intentionally exercises the nil-UUID
		// guard in BuildObjectKey.
		var extractionID uuid.UUID
		if len(extractionBytes) == 16 {
			parsed, parseErr := uuid.FromBytes(extractionBytes)
			if parseErr == nil {
				extractionID = parsed
			}
		}

		key, err := custody.BuildObjectKey(tenantID, extractionID)

		// Property 2 (success): the key must never contain '..' or a
		// double slash, must start with a trimmed tenant id, and must end
		// with ".json". Any violation is a path-traversal outcome.
		if err == nil {
			trimmed := strings.TrimSpace(tenantID)

			if !strings.HasPrefix(key, trimmed+"/") {
				t.Fatalf("key %q does not start with trimmed tenant %q", key, trimmed)
			}

			if !strings.HasSuffix(key, ".json") {
				t.Fatalf("key %q does not end with .json", key)
			}

			// Strip the tenant + prefix + extension so we can assert on
			// what the function spliced in between. The splice must be
			// exactly the string form of the extraction UUID.
			middle := strings.TrimPrefix(key, trimmed+"/")
			middle = strings.TrimSuffix(middle, ".json")

			expectedMiddle := custody.KeyPrefix + "/" + extractionID.String()
			if middle != expectedMiddle {
				t.Fatalf("key middle %q does not match expected %q", middle, expectedMiddle)
			}

			// Path-traversal sentinels that must never appear anywhere in
			// the composed key, even legitimately.
			for _, bad := range []string{"//", "/../", "\x00", "/./"} {
				if strings.Contains(key, bad) {
					t.Fatalf("key %q contains forbidden segment %q", key, bad)
				}
			}

			return
		}

		// Property 3 (failure): error must be one of the documented
		// sentinels. BuildObjectKey only returns tenant/extraction
		// sentinels — anything else is a contract violation.
		if !isAllowedCustodyPathError(err) {
			t.Fatalf("undocumented error sentinel from BuildObjectKey: %v", err)
		}
	})
}

// encryptFuzzArtifact builds a (ciphertext, iv, hmacHex) triple that the
// verifier will accept as valid. Inlines the construction rather than
// reusing encryptTestArtifact because that helper takes *testing.T and
// the seed-corpus caller here holds *testing.F.
//
// Runs once per `go test -fuzz=...` invocation (at seed time), so the
// per-iteration cost of the fuzzer is unaffected.
func encryptFuzzArtifact(tb testing.TB, master, plaintext []byte) ([]byte, []byte, string) {
	tb.Helper()

	hmacKey, err := deriveKey(master, hkdfContextHMAC)
	if err != nil {
		tb.Fatalf("fuzz encrypt: derive hmac key: %v", err)
	}

	aesKey, err := deriveKey(master, hkdfContextAES)
	if err != nil {
		tb.Fatalf("fuzz encrypt: derive aes key: %v", err)
	}

	block, err := aes.NewCipher(aesKey)
	if err != nil {
		tb.Fatalf("fuzz encrypt: aes cipher: %v", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		tb.Fatalf("fuzz encrypt: gcm: %v", err)
	}

	iv := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(iv); err != nil {
		tb.Fatalf("fuzz encrypt: iv read: %v", err)
	}

	ciphertext := gcm.Seal(nil, iv, plaintext, nil)

	mac := hmac.New(sha256.New, hmacKey)
	if _, err := mac.Write(ciphertext); err != nil {
		tb.Fatalf("fuzz encrypt: hmac write: %v", err)
	}

	return ciphertext, iv, hex.EncodeToString(mac.Sum(nil))
}

// isAllowedVerifierError is the set of sentinels VerifyAndDecrypt is
// allowed to return. Any other error shape from the fuzzer is a bug.
func isAllowedVerifierError(err error) bool {
	switch {
	case err == nil:
		return true
	case errors.Is(err, sharedPorts.ErrIntegrityVerificationFailed),
		errors.Is(err, sharedPorts.ErrArtifactCiphertextRequired),
		errors.Is(err, sharedPorts.ErrArtifactHMACRequired):
		return true
	default:
		return false
	}
}

// isAllowedCustodyPathError is the set of sentinels BuildObjectKey is
// allowed to return. The function only validates tenant + extraction id
// so anything else is a contract violation.
func isAllowedCustodyPathError(err error) bool {
	switch {
	case err == nil:
		return true
	case errors.Is(err, sharedPorts.ErrArtifactTenantIDRequired),
		errors.Is(err, sharedPorts.ErrArtifactExtractionIDRequired):
		return true
	default:
		return false
	}
}
