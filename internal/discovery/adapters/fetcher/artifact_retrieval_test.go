// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fetcher

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// stubArtifactRoundTripper returns a canned response without an actual
// network call. Using RoundTripper keeps the test honest: we exercise
// the real http.Client code path the production gateway uses.
type stubArtifactRoundTripper struct {
	resp *http.Response
	err  error
	seen *http.Request
}

func (s *stubArtifactRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	s.seen = req

	if s.err != nil {
		return nil, s.err
	}

	return s.resp, nil
}

func newStubResponse(status int, headers map[string]string, body string) *http.Response {
	h := http.Header{}

	for k, v := range headers {
		h.Set(k, v)
	}

	return &http.Response{
		StatusCode:    status,
		Header:        h,
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

func newStubHTTPClient(rt *stubArtifactRoundTripper) *http.Client {
	return &http.Client{Transport: rt}
}

func newTestArtifactDescriptor(t *testing.T) sharedPorts.ArtifactRetrievalDescriptor {
	t.Helper()

	return sharedPorts.ArtifactRetrievalDescriptor{
		ExtractionID: uuid.New(),
		TenantID:     "tenant-test",
		URL:          "https://fetcher.example.test/v1/artifacts/extraction-42.bin",
	}
}

func TestNewArtifactRetrievalClient_RejectsNilClient(t *testing.T) {
	t.Parallel()

	c, err := NewArtifactRetrievalClient(nil)
	require.Nil(t, c)
	require.ErrorIs(t, err, ErrFetcherClientNil)
}

func TestNewArtifactRetrievalClient_AcceptsClient(t *testing.T) {
	t.Parallel()

	c, err := NewArtifactRetrievalClient(&http.Client{})
	require.NoError(t, err)
	require.NotNil(t, c)
}

func TestRetrieve_HappyPath_ReturnsBodyAndMetadata(t *testing.T) {
	t.Parallel()

	body := "opaque-ciphertext-bytes"
	rt := &stubArtifactRoundTripper{
		resp: newStubResponse(http.StatusOK, map[string]string{
			"X-Fetcher-Artifact-Hmac": "deadbeef",
			"X-Fetcher-Artifact-Iv":   "feedface",
			"Content-Type":            "application/octet-stream",
		}, body),
	}

	client, err := NewArtifactRetrievalClient(newStubHTTPClient(rt))
	require.NoError(t, err)

	result, err := client.Retrieve(context.Background(), newTestArtifactDescriptor(t))
	require.NoError(t, err)
	require.NotNil(t, result)

	defer func() {
		_ = result.Content.Close()
	}()

	got, err := io.ReadAll(result.Content)
	require.NoError(t, err)
	assert.Equal(t, body, string(got))
	assert.Equal(t, int64(len(body)), result.ContentLength)
	assert.Equal(t, "application/octet-stream", result.ContentType)
	assert.Equal(t, "deadbeef", result.HMAC)
	assert.Equal(t, "feedface", result.IV)
	assert.NotNil(t, rt.seen, "request was issued")
	assert.Equal(t, http.MethodGet, rt.seen.Method)
}

func TestRetrieve_404_ReturnsResourceNotFound(t *testing.T) {
	t.Parallel()

	rt := &stubArtifactRoundTripper{
		resp: newStubResponse(http.StatusNotFound, nil, ""),
	}

	client, err := NewArtifactRetrievalClient(newStubHTTPClient(rt))
	require.NoError(t, err)

	_, err = client.Retrieve(context.Background(), newTestArtifactDescriptor(t))
	require.ErrorIs(t, err, sharedPorts.ErrFetcherResourceNotFound)
}

func TestRetrieve_500_IsTransient(t *testing.T) {
	t.Parallel()

	rt := &stubArtifactRoundTripper{
		resp: newStubResponse(http.StatusBadGateway, nil, "upstream down"),
	}

	client, err := NewArtifactRetrievalClient(newStubHTTPClient(rt))
	require.NoError(t, err)

	_, err = client.Retrieve(context.Background(), newTestArtifactDescriptor(t))
	require.ErrorIs(t, err, sharedPorts.ErrArtifactRetrievalFailed)
}

func TestRetrieve_TransportError_IsTransient(t *testing.T) {
	t.Parallel()

	rt := &stubArtifactRoundTripper{err: errors.New("dial tcp: connection refused")}

	client, err := NewArtifactRetrievalClient(newStubHTTPClient(rt))
	require.NoError(t, err)

	_, err = client.Retrieve(context.Background(), newTestArtifactDescriptor(t))
	require.ErrorIs(t, err, sharedPorts.ErrArtifactRetrievalFailed)
}

func TestRetrieve_MissingHMACHeader_IsTerminal(t *testing.T) {
	t.Parallel()

	rt := &stubArtifactRoundTripper{
		resp: newStubResponse(http.StatusOK, map[string]string{
			"X-Fetcher-Artifact-Iv": "feedface",
		}, "body"),
	}

	client, err := NewArtifactRetrievalClient(newStubHTTPClient(rt))
	require.NoError(t, err)

	_, err = client.Retrieve(context.Background(), newTestArtifactDescriptor(t))
	// Missing HMAC is terminal: the contract promises the header exists.
	// Both the specific cause and the terminal sentinel must be in chain.
	require.ErrorIs(t, err, ErrArtifactMissingHMACHeader)
	require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
}

func TestRetrieve_MissingIVHeader_IsTerminal(t *testing.T) {
	t.Parallel()

	rt := &stubArtifactRoundTripper{
		resp: newStubResponse(http.StatusOK, map[string]string{
			"X-Fetcher-Artifact-Hmac": "abc",
		}, "body"),
	}

	client, err := NewArtifactRetrievalClient(newStubHTTPClient(rt))
	require.NoError(t, err)

	_, err = client.Retrieve(context.Background(), newTestArtifactDescriptor(t))
	require.ErrorIs(t, err, ErrArtifactMissingIVHeader)
	require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
}

func TestRetrieve_EmptyURL_IsInputError(t *testing.T) {
	t.Parallel()

	client, err := NewArtifactRetrievalClient(&http.Client{})
	require.NoError(t, err)

	_, err = client.Retrieve(context.Background(), sharedPorts.ArtifactRetrievalDescriptor{
		ExtractionID: uuid.New(),
		TenantID:     "t",
	})
	require.ErrorIs(t, err, sharedPorts.ErrArtifactDescriptorRequired)
}

func TestRetrieve_ContentLengthExceedsLimit_IsTerminal(t *testing.T) {
	t.Parallel()

	// Craft a response whose Content-Length exceeds the body cap. The
	// body reader itself contains a short sentinel — we never read it
	// because classification happens before body consumption.
	oversized := &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"X-Fetcher-Artifact-Hmac": []string{"abc"},
			"X-Fetcher-Artifact-Iv":   []string{"def"},
		},
		Body:          io.NopCloser(strings.NewReader("unused")),
		ContentLength: int64(maxArtifactBodyBytes) + 1,
	}

	rt := &stubArtifactRoundTripper{resp: oversized}
	client, err := NewArtifactRetrievalClient(newStubHTTPClient(rt))
	require.NoError(t, err)

	_, err = client.Retrieve(context.Background(), newTestArtifactDescriptor(t))
	// Oversize collapses to the terminal sentinel so bridge workers stop
	// retrying a bomb, while the inner ErrArtifactBodyTooLarge remains
	// inspectable for observability.
	require.ErrorIs(t, err, ErrArtifactBodyTooLarge)
	require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
}

func TestRetrieve_Redirect_IsTransient(t *testing.T) {
	t.Parallel()

	rt := &stubArtifactRoundTripper{
		// 302 Found: Fetcher should not redirect artifact downloads.
		// Our http.Client is configured by the wider package not to follow
		// redirects, so we see the raw redirect status here.
		resp: newStubResponse(http.StatusFound, map[string]string{
			"Location": "https://elsewhere.test",
		}, ""),
	}

	client, err := NewArtifactRetrievalClient(newStubHTTPClient(rt))
	require.NoError(t, err)

	_, err = client.Retrieve(context.Background(), newTestArtifactDescriptor(t))
	require.ErrorIs(t, err, sharedPorts.ErrArtifactRetrievalFailed)
}

func TestClassifyArtifactResponse_NilResponse(t *testing.T) {
	t.Parallel()

	_, err := classifyArtifactResponse(nil)
	require.ErrorIs(t, err, sharedPorts.ErrArtifactRetrievalFailed)
}

func TestClassifyArtifactResponse_4xxOther_IsTerminal(t *testing.T) {
	t.Parallel()

	resp := newStubResponse(http.StatusForbidden, nil, "forbidden")
	_, err := classifyArtifactResponse(resp)
	// 403 is terminal: auth/IAM failures are deterministic contract
	// violations, retrying with the same token just risks lockout.
	require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
	require.NotErrorIs(t, err, sharedPorts.ErrArtifactRetrievalFailed)
}

// TestClassifyArtifactResponse_MissingHMAC_IsTerminal asserts that the
// missing-HMAC contract violation travels under
// ErrIntegrityVerificationFailed so the bridge worker stops retrying on
// a header Fetcher will never produce. The specific
// ErrArtifactMissingHMACHeader cause remains inspectable.
func TestClassifyArtifactResponse_MissingHMAC_IsTerminal(t *testing.T) {
	t.Parallel()

	resp := newStubResponse(http.StatusOK, map[string]string{
		"X-Fetcher-Artifact-Iv": "feedface",
	}, "body")

	_, err := classifyArtifactResponse(resp)
	require.ErrorIs(t, err, ErrArtifactMissingHMACHeader)
	require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
	require.NotErrorIs(t, err, sharedPorts.ErrArtifactRetrievalFailed)
}

// TestClassifyArtifactResponse_MissingIV_IsTerminal mirrors the HMAC
// case for the IV header.
func TestClassifyArtifactResponse_MissingIV_IsTerminal(t *testing.T) {
	t.Parallel()

	resp := newStubResponse(http.StatusOK, map[string]string{
		"X-Fetcher-Artifact-Hmac": "abc",
	}, "body")

	_, err := classifyArtifactResponse(resp)
	require.ErrorIs(t, err, ErrArtifactMissingIVHeader)
	require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
	require.NotErrorIs(t, err, sharedPorts.ErrArtifactRetrievalFailed)
}

// TestClassifyArtifactResponse_OversizedBody_IsTerminal ensures that a
// large advertised Content-Length fails terminally (retrying downloads
// the same bomb).
func TestClassifyArtifactResponse_OversizedBody_IsTerminal(t *testing.T) {
	t.Parallel()

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"X-Fetcher-Artifact-Hmac": []string{"abc"},
			"X-Fetcher-Artifact-Iv":   []string{"def"},
		},
		Body:          io.NopCloser(strings.NewReader("unused")),
		ContentLength: int64(maxArtifactBodyBytes) + 1,
	}

	_, err := classifyArtifactResponse(resp)
	require.ErrorIs(t, err, ErrArtifactBodyTooLarge)
	require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed)
}

// TestClassifyArtifactResponse_4xxTerminalVsTransient documents the
// retry policy split: 401/403/413 are terminal (auth expired, IAM
// revoked, payload rejected — retry cannot fix any of these), while 408
// Request Timeout and 425 Too Early are the rare 4xx responses where a
// retry could legitimately succeed.
func TestClassifyArtifactResponse_4xxTerminalVsTransient(t *testing.T) {
	t.Parallel()

	terminalCases := []struct {
		name   string
		status int
	}{
		{"401 unauthorized", http.StatusUnauthorized},
		{"403 forbidden", http.StatusForbidden},
		{"413 payload too large", http.StatusRequestEntityTooLarge},
		{"400 bad request", http.StatusBadRequest},
		{"429 too many requests", http.StatusTooManyRequests},
	}
	for _, tc := range terminalCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp := newStubResponse(tc.status, nil, "")
			_, err := classifyArtifactResponse(resp)
			require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed,
				"%s must be terminal to prevent retry loops", tc.name)
			require.NotErrorIs(t, err, sharedPorts.ErrArtifactRetrievalFailed,
				"%s must not collide with the transient sentinel", tc.name)
		})
	}

	transientCases := []struct {
		name   string
		status int
	}{
		{"408 request timeout", http.StatusRequestTimeout},
		{"425 too early", http.StatusTooEarly},
	}
	for _, tc := range transientCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp := newStubResponse(tc.status, nil, "")
			_, err := classifyArtifactResponse(resp)
			require.ErrorIs(t, err, sharedPorts.ErrArtifactRetrievalFailed,
				"%s must be transient so the bridge worker retries", tc.name)
			require.NotErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed,
				"%s must not masquerade as a terminal failure", tc.name)
		})
	}
}

// TestIsNilArtifactHTTPClient_TypedNilRejected proves the typed-nil
// defence-in-depth claim: a `(*http.Client)(nil)` stored behind the
// ArtifactHTTPClient interface is not nil to `==`, but reflect catches it.
// Regression guard for the documentation/implementation mismatch flagged
// by four Gate 8 reviewers.
func TestIsNilArtifactHTTPClient_TypedNilRejected(t *testing.T) {
	t.Parallel()

	// Bare nil interface.
	assert.True(t, isNilArtifactHTTPClient(nil), "bare nil interface must be rejected")

	// Typed nil: a nil *http.Client stored behind the interface. This is
	// the scenario the old doc promised to catch but the code didn't.
	var typedNil *http.Client
	assert.True(t, isNilArtifactHTTPClient(typedNil),
		"typed-nil (*http.Client)(nil) must be rejected")

	// Real client must pass.
	assert.False(t, isNilArtifactHTTPClient(&http.Client{}),
		"a valid *http.Client must NOT be reported as nil")
}

// TestNewArtifactRetrievalClient_RejectsTypedNil end-to-ends the
// constructor with a typed-nil pointer to guarantee callers that
// accidentally pass a zero-valued pointer get a clean error instead of
// a later Do-time panic.
func TestNewArtifactRetrievalClient_RejectsTypedNil(t *testing.T) {
	t.Parallel()

	var typedNil *http.Client
	c, err := NewArtifactRetrievalClient(typedNil)
	require.Nil(t, c)
	require.ErrorIs(t, err, ErrFetcherClientNil)
}

func TestClassifyArtifactResponse_NilBody_IsEmptyCiphertext(t *testing.T) {
	t.Parallel()

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"X-Fetcher-Artifact-Hmac": []string{"abc"},
			"X-Fetcher-Artifact-Iv":   []string{"def"},
		},
		Body:          nil,
		ContentLength: 0,
	}

	result, err := classifyArtifactResponse(resp)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Content, "always produce a non-nil reader")

	got, err := io.ReadAll(result.Content)
	require.NoError(t, err)
	assert.Empty(t, got)
}
