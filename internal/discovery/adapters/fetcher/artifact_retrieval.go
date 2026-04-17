// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package fetcher

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"

	"go.opentelemetry.io/otel/attribute"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Custom HTTP headers used by Fetcher to convey integrity metadata with
// the artifact body. Header names are case-insensitive at the HTTP layer,
// but we document the canonical capitalisation Fetcher sends.
const (
	// headerArtifactHMAC carries the hex-encoded HMAC-SHA256 digest that
	// Fetcher computed over the ciphertext body using the
	// fetcher-external-hmac-v1 derived key.
	headerArtifactHMAC = "X-Fetcher-Artifact-Hmac"

	// headerArtifactIV carries the hex-encoded initialisation vector
	// required by AES-256-GCM. Transported out-of-band so the verifier
	// does not have to parse a framing envelope over the ciphertext.
	headerArtifactIV = "X-Fetcher-Artifact-Iv"

	// maxArtifactBodyBytes caps the server-advertised Content-Length we
	// accept before we refuse to even start reading. Aligned with the
	// verifier's maxCiphertextBytes. A Content-Length larger than this
	// is rejected pre-read so we do not waste bandwidth on a payload we
	// would refuse anyway.
	maxArtifactBodyBytes = 256 * 1024 * 1024
)

// Sentinel errors specific to artifact retrieval. These augment the shared
// ErrArtifactRetrievalFailed, ErrIntegrityVerificationFailed, and
// ErrFetcherResourceNotFound sentinels with distinguishable causes for
// observability. classifyArtifactResponse wraps them under the appropriate
// terminal/transient sentinel so callers drive retry policy via a single
// errors.Is check while tests can still assert on the specific reason.
var (
	// ErrArtifactMissingHMACHeader indicates Fetcher returned 200 OK but
	// did not set the HMAC header. The contract says every completed
	// artifact carries one; absence is a terminal retrieval failure
	// (retrying would not produce a header that does not exist). Wrapped
	// by classifyArtifactResponse under
	// sharedPorts.ErrIntegrityVerificationFailed so terminal-ness is in
	// the error chain.
	ErrArtifactMissingHMACHeader = errors.New(
		"fetcher artifact response missing hmac header",
	)

	// ErrArtifactMissingIVHeader indicates Fetcher returned 200 OK but
	// did not set the IV header. Same terminal failure class as missing
	// HMAC and wrapped the same way.
	ErrArtifactMissingIVHeader = errors.New(
		"fetcher artifact response missing iv header",
	)

	// ErrArtifactBodyTooLarge indicates Fetcher advertised a
	// Content-Length larger than we are willing to accept. Terminal
	// because retrying would just download the same oversized bomb.
	// Wrapped under sharedPorts.ErrIntegrityVerificationFailed (the
	// bridge's single terminal signal) by classifyArtifactResponse.
	ErrArtifactBodyTooLarge = errors.New(
		"fetcher artifact body exceeds configured limit",
	)
)

// ArtifactHTTPClient is a tiny interface abstraction over *http.Client so
// the gateway can be unit-tested with a canned RoundTripper without
// instantiating the full fetcher HTTPClient. The production wiring uses
// the same underlying http.Client that HTTPFetcherClient already owns.
type ArtifactHTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// ArtifactRetrievalClient implements sharedPorts.ArtifactRetrievalGateway
// by issuing an HTTP GET against the Fetcher artifact URL carried in the
// descriptor. It deliberately does not reuse the extraction-level retry
// loop from client_transport.go because:
//
//  1. Artifact downloads may stream large bodies; the extraction-level
//     loop reads the full body into memory before classifying.
//  2. Retry policy is owned by T-005's bridge worker, not this gateway.
//
// The gateway fails fast on transport errors by wrapping them with
// ErrArtifactRetrievalFailed. Callers distinguish 404 as a separate
// terminal signal (ErrFetcherResourceNotFound).
type ArtifactRetrievalClient struct {
	httpClient ArtifactHTTPClient
}

// Compile-time interface check.
var _ sharedPorts.ArtifactRetrievalGateway = (*ArtifactRetrievalClient)(nil)

// NewArtifactRetrievalClient constructs the retrieval gateway around an
// existing HTTP client. The client is responsible for TLS, proxies,
// timeouts, and circuit breaking (per the wider fetcher package's
// HTTPClientConfig). Nil clients are rejected.
func NewArtifactRetrievalClient(httpClient ArtifactHTTPClient) (*ArtifactRetrievalClient, error) {
	if isNilArtifactHTTPClient(httpClient) {
		return nil, ErrFetcherClientNil
	}

	return &ArtifactRetrievalClient{httpClient: httpClient}, nil
}

// Retrieve issues the HTTP GET and captures body + headers into an
// ArtifactRetrievalResult. The Content ReadCloser MUST be closed by the
// caller — typically the verifier, which reads it fully and then closes
// via its defer.
func (client *ArtifactRetrievalClient) Retrieve(
	ctx context.Context,
	descriptor sharedPorts.ArtifactRetrievalDescriptor,
) (*sharedPorts.ArtifactRetrievalResult, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "fetcher.artifact_retrieval.retrieve")
	defer span.End()

	span.SetAttributes(
		attribute.String("fetcher.artifact.extraction_id", descriptor.ExtractionID.String()),
		attribute.String("fetcher.artifact.tenant_id", descriptor.TenantID),
	)

	if err := validateDescriptor(descriptor); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "invalid artifact descriptor", err)

		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, descriptor.URL, http.NoBody)
	if err != nil {
		wrapped := fmt.Errorf("%w: build request: %w", sharedPorts.ErrArtifactRetrievalFailed, err)
		libOpentelemetry.HandleSpanError(span, "build artifact retrieval request", wrapped)

		return nil, wrapped
	}

	// Fetcher's artifact endpoint does not itself require JSON; requesting
	// octet-stream is honest signalling to any intermediaries and disables
	// accidental JSON-body rewriting by proxies.
	req.Header.Set("Accept", "application/octet-stream, */*")

	// The request URL is built from the validated ArtifactRetrievalDescriptor.URL,
	// not from untrusted external input: the bridge worker constructs it from the
	// Fetcher base URL plus a tenant-scoped result path already validated by
	// validateFetcherResultPath on the extraction side. The response body is
	// handed off to the caller via ArtifactRetrievalResult.Content — closing
	// it is the caller's responsibility.
	resp, err := client.httpClient.Do(req)
	if err != nil {
		wrapped := fmt.Errorf("%w: transport error: %w", sharedPorts.ErrArtifactRetrievalFailed, err)
		libOpentelemetry.HandleSpanError(span, "artifact retrieval transport failure", wrapped)

		return nil, wrapped
	}

	result, err := classifyArtifactResponse(resp)
	if err != nil {
		// On classification failure we must close the body ourselves since
		// we are not handing it off to the caller.
		_ = resp.Body.Close()

		if errors.Is(err, sharedPorts.ErrFetcherResourceNotFound) {
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "fetcher artifact not found", err)
		} else {
			libOpentelemetry.HandleSpanError(span, "artifact response classification failed", err)
		}

		return nil, err
	}

	span.SetAttributes(
		attribute.Int64("fetcher.artifact.content_length", result.ContentLength),
		attribute.String("fetcher.artifact.content_type", result.ContentType),
	)

	return result, nil
}

// classifyArtifactResponse maps HTTP response codes to the shared
// retrieval sentinels and extracts the integrity-metadata headers.
//
// Success path: 2xx with non-empty HMAC + IV headers becomes an
// ArtifactRetrievalResult whose body is handed off to the caller.
//
// Failure paths collapse to two retry classes:
//
// Terminal (never retry):
//   - ErrFetcherResourceNotFound for 404 (extraction gone from Fetcher).
//   - ErrIntegrityVerificationFailed wrapping the inner status/cause for
//     4xx other than 404/408/425 — 401/403/413 and friends signal an
//     expired token, revoked IAM grant, or payload rejection; retrying
//     with the same inputs is futile and risks lockout cascades.
//   - ErrIntegrityVerificationFailed wrapping the missing-header sentinel
//     when HMAC or IV headers are absent — the Fetcher contract guarantees
//     them, so absence is a contract violation no retry can heal.
//   - ErrIntegrityVerificationFailed wrapping ErrArtifactBodyTooLarge when
//     Content-Length exceeds the cap — oversize is an attack class.
//
// Transient (safe to retry):
//   - ErrArtifactRetrievalFailed wrapping status for 5xx (upstream
//     crashed or is overloaded).
//   - ErrArtifactRetrievalFailed wrapping status for 408 Request Timeout,
//     425 Too Early, and 429 Too Many Requests — the client may succeed
//     on a second attempt. 429 is upstream rate-limiting, not a
//     tamper/integrity event; marking it terminal would let a brief
//     Fetcher rate-limit window permanently kill every in-flight
//     extraction. When present, the Retry-After header value is echoed
//     into the error message so operators can see the advised delay in
//     logs — the caller's retry scheduler, not this function, decides
//     when to actually re-attempt.
//   - ErrArtifactRetrievalFailed for 1xx/3xx (unexpected; might be a
//     proxy hiccup retry can resolve).
func classifyArtifactResponse(resp *http.Response) (*sharedPorts.ArtifactRetrievalResult, error) {
	if resp == nil {
		return nil, fmt.Errorf("%w: nil response", sharedPorts.ErrArtifactRetrievalFailed)
	}

	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, sharedPorts.ErrFetcherResourceNotFound
	case resp.StatusCode >= http.StatusInternalServerError:
		return nil, fmt.Errorf(
			"%w: fetcher returned status %d",
			sharedPorts.ErrArtifactRetrievalFailed,
			resp.StatusCode,
		)
	case resp.StatusCode == http.StatusTooManyRequests:
		// 429 is upstream rate-limiting: the same request will succeed once
		// the window elapses, so it must be transient. Echo Retry-After
		// into the error message when Fetcher provides it; the retry
		// scheduler (DB updated_at ASC reorder + worker loop) handles
		// actual parking — this function just surfaces the hint.
		retryAfter := strings.TrimSpace(resp.Header.Get("Retry-After"))
		if retryAfter != "" {
			return nil, fmt.Errorf(
				"%w: fetcher returned status %d, retry-after=%s",
				sharedPorts.ErrArtifactRetrievalFailed,
				resp.StatusCode,
				retryAfter,
			)
		}

		return nil, fmt.Errorf(
			"%w: fetcher returned status %d",
			sharedPorts.ErrArtifactRetrievalFailed,
			resp.StatusCode,
		)
	case resp.StatusCode == http.StatusRequestTimeout,
		resp.StatusCode == http.StatusTooEarly:
		// 408 and 425 are transient 4xx responses: the request can succeed
		// on a second attempt without operator intervention. Every other
		// 4xx (except 429, handled above) signals auth expiry, quota
		// breach, or a payload the server will keep rejecting.
		return nil, fmt.Errorf(
			"%w: fetcher returned status %d",
			sharedPorts.ErrArtifactRetrievalFailed,
			resp.StatusCode,
		)
	case resp.StatusCode >= http.StatusBadRequest:
		// Terminal: 401/403/413 and similar non-retryable 4xx. Wrapped
		// under ErrIntegrityVerificationFailed so the bridge worker
		// short-circuits retries on deterministic contract violations.
		// The name is imperfect (auth isn't "integrity"), but it is the
		// single terminal signal downstream code already honours.
		return nil, fmt.Errorf(
			"%w: fetcher returned status %d",
			sharedPorts.ErrIntegrityVerificationFailed,
			resp.StatusCode,
		)
	case resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices:
		// 1xx or 3xx on a GET that does not follow redirects is a contract
		// violation by Fetcher; treat as transient so the caller retries.
		return nil, fmt.Errorf(
			"%w: unexpected status %d",
			sharedPorts.ErrArtifactRetrievalFailed,
			resp.StatusCode,
		)
	}

	hmacValue := strings.TrimSpace(resp.Header.Get(headerArtifactHMAC))
	if hmacValue == "" {
		// Missing HMAC is terminal: Fetcher's contract guarantees the
		// header. Wrapping with ErrIntegrityVerificationFailed makes the
		// terminal-ness part of the error chain so downstream code can
		// match via errors.Is and skip retry.
		return nil, fmt.Errorf("%w: %w", sharedPorts.ErrIntegrityVerificationFailed, ErrArtifactMissingHMACHeader)
	}

	ivValue := strings.TrimSpace(resp.Header.Get(headerArtifactIV))
	if ivValue == "" {
		return nil, fmt.Errorf("%w: %w", sharedPorts.ErrIntegrityVerificationFailed, ErrArtifactMissingIVHeader)
	}

	if resp.ContentLength > maxArtifactBodyBytes {
		// Oversize is terminal: retrying would just download the same
		// bomb. Wrap under ErrIntegrityVerificationFailed so the bridge
		// worker treats this like any other non-retryable payload issue.
		return nil, fmt.Errorf(
			"%w: %w (content-length=%d, limit=%d)",
			sharedPorts.ErrIntegrityVerificationFailed,
			ErrArtifactBodyTooLarge,
			resp.ContentLength,
			int64(maxArtifactBodyBytes),
		)
	}

	body := resp.Body
	if body == nil {
		// Defensive: ensure downstream code can always io.Copy without a
		// nil-panic. Empty body with valid headers is an empty ciphertext,
		// which the verifier handles (and rejects on HMAC mismatch).
		body = io.NopCloser(bytes.NewReader(nil))
	}

	return &sharedPorts.ArtifactRetrievalResult{
		Content:       body,
		ContentLength: resp.ContentLength,
		ContentType:   resp.Header.Get("Content-Type"),
		HMAC:          hmacValue,
		IV:            ivValue,
	}, nil
}

func validateDescriptor(descriptor sharedPorts.ArtifactRetrievalDescriptor) error {
	if strings.TrimSpace(descriptor.URL) == "" {
		return sharedPorts.ErrArtifactDescriptorRequired
	}

	return nil
}

// isNilArtifactHTTPClient returns true both for nil interface values and
// typed-nil pointers assigned to the interface — e.g. a
// `(*http.Client)(nil)` accidentally stored behind an ArtifactHTTPClient
// interface. This is the same belt-and-braces nil check used elsewhere in
// the fetcher package (see client.go). Called once at construction time
// (NewArtifactRetriever), so the reflect overhead is negligible and the
// downstream HTTP round-trip dwarfs it by many orders of magnitude anyway.
func isNilArtifactHTTPClient(c ArtifactHTTPClient) bool {
	if c == nil {
		return true
	}

	v := reflect.ValueOf(c)
	// Only pointer-backed interfaces can hide a typed-nil; other kinds
	// (e.g. a struct value satisfying the interface) are by definition
	// non-nil once the interface check above passes.
	return v.Kind() == reflect.Pointer && v.IsNil()
}
