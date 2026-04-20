// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package m2m provides an OAuth2 client_credentials token exchanger for
// converting M2M credentials (client_id + client_secret) into Bearer tokens.
//
// The TokenExchanger calls the auth service's OAuth2 endpoint and caches
// tokens in-memory (sync.Map) keyed by client_id. A configurable cache
// buffer (default 30s) ensures tokens are refreshed before they expire.
package m2m

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// defaultCacheBuffer is subtracted from expires_in to avoid using near-expired tokens.
const defaultCacheBuffer = 30 * time.Second

// defaultHTTPTimeout is the timeout for token exchange HTTP requests.
const defaultHTTPTimeout = 10 * time.Second

// maxTokenResponseSize limits response body reads from the auth service
// to prevent memory exhaustion from malformed or malicious responses.
// Token responses are typically <4KB; 64KB is a generous upper bound.
const maxTokenResponseSize = 64 * 1024

// maxErrorBodyLen limits how many bytes of a non-OK response body are
// included in error messages, preventing auth service internals from leaking.
const maxErrorBodyLen = 200

// tokenEndpointPath is the OAuth2 token endpoint on the auth service.
const tokenEndpointPath = "/v1/login/oauth/access_token" // #nosec G101 -- OAuth2 endpoint URL path, not a credential //nolint:gosec

// Sentinel errors for token exchange operations.
var (
	ErrTokenExchangeNilClient        = errors.New("HTTP client is nil")
	ErrTokenExchangeAuthURLRequired  = errors.New("auth service URL is required")
	ErrTokenExchangeInvalidAuthURL   = errors.New("auth URL must use https:// scheme (use WithInsecureHTTP for local dev)")
	ErrTokenExchangeFailed           = errors.New("token exchange failed")
	ErrTokenExchangeEmptyToken       = errors.New("auth service returned empty access token")
	ErrTokenExchangeUnexpectedResult = errors.New("unexpected singleflight result type")
)

// cachedToken holds a Bearer token with its computed expiration time.
type cachedToken struct {
	accessToken string
	expiresAt   time.Time
}

// tokenResponse represents the OAuth2 token endpoint JSON response.
type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int64  `json:"expires_in"`
}

// TokenExchangerOption configures optional TokenExchanger parameters.
type TokenExchangerOption func(*TokenExchanger)

// WithHTTPClient overrides the default HTTP client used for token exchange.
func WithHTTPClient(client *http.Client) TokenExchangerOption {
	return func(te *TokenExchanger) {
		if client != nil {
			te.httpClient = client
		}
	}
}

// WithCacheBuffer overrides the default cache buffer duration.
// The buffer is subtracted from expires_in to refresh tokens before they expire.
func WithCacheBuffer(buffer time.Duration) TokenExchangerOption {
	return func(te *TokenExchanger) {
		if buffer >= 0 {
			te.cacheBuffer = buffer
		}
	}
}

// WithInsecureHTTP permits http:// URLs for the auth service. This is intended
// exclusively for local development and testing — production deployments MUST
// use https://. When this option is not set, NewTokenExchanger rejects non-HTTPS URLs.
func WithInsecureHTTP() TokenExchangerOption {
	return func(te *TokenExchanger) {
		te.allowInsecureHTTP = true
	}
}

// WithNowFunc overrides the clock used for token expiry calculations.
// Primarily useful for deterministic testing without time.Sleep.
// Defaults to time.Now().UTC() when not set.
func WithNowFunc(fn func() time.Time) TokenExchangerOption {
	return func(te *TokenExchanger) {
		if fn != nil {
			te.nowFunc = fn
		}
	}
}

// TokenExchanger exchanges M2M credentials for OAuth2 Bearer tokens
// using the client_credentials grant type. Tokens are cached in-memory
// keyed by client_id to avoid redundant round-trips to the auth service.
type TokenExchanger struct {
	authURL           string
	httpClient        *http.Client
	cacheBuffer       time.Duration
	allowInsecureHTTP bool             // permits http:// URLs (local dev only)
	nowFunc           func() time.Time // clock abstraction for testability
	tokenCache        sync.Map         // clientID -> *cachedToken

	// tenantClientMap is a reverse index: tenantOrgID -> clientID, used during 401
	// recovery to identify which cached token to invalidate without re-fetching
	// credentials. The map grows monotonically — it has no TTL or eviction
	// mechanism. At typical SaaS scale (thousands of tenants), size is negligible
	// (~200 bytes per entry). If tenant count exceeds ~100k, consider adding
	// LRU eviction or bounded size.
	tenantClientMap sync.Map

	flight singleflight.Group // coalesces concurrent exchanges per clientID
}

// NewTokenExchanger creates a token exchanger targeting the given auth service URL.
// Returns an error if authURL is empty or does not use https:// (unless
// WithInsecureHTTP is provided for local development). Use functional options
// to override the HTTP client, cache buffer, or clock.
func NewTokenExchanger(authURL string, opts ...TokenExchangerOption) (*TokenExchanger, error) {
	if authURL == "" {
		return nil, ErrTokenExchangeAuthURLRequired
	}

	te := &TokenExchanger{
		authURL: strings.TrimRight(authURL, "/"),
		httpClient: &http.Client{
			Timeout: defaultHTTPTimeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		cacheBuffer: defaultCacheBuffer,
		nowFunc:     func() time.Time { return time.Now().UTC() },
	}

	for _, opt := range opts {
		opt(te)
	}

	if te.httpClient == nil {
		return nil, ErrTokenExchangeNilClient
	}

	// Validate URL scheme after options are applied (WithInsecureHTTP may have been set).
	parsed, parseErr := url.Parse(te.authURL)
	if parseErr != nil {
		return nil, fmt.Errorf("%w: %w", ErrTokenExchangeInvalidAuthURL, parseErr)
	}

	if parsed.Scheme == "http" && !te.allowInsecureHTTP {
		return nil, ErrTokenExchangeInvalidAuthURL
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("%w: got %q", ErrTokenExchangeInvalidAuthURL, parsed.Scheme)
	}

	return te, nil
}

// GetToken returns a valid Bearer token for the given credentials.
// On cache hit with a non-expired token, the cached value is returned
// without contacting the auth service. On miss or expiry, a new token
// is exchanged via the client_credentials grant.
func (te *TokenExchanger) GetToken(ctx context.Context, clientID, clientSecret string) (string, error) {
	// Check cache first
	if cached, ok := te.tokenCache.Load(clientID); ok {
		ct, valid := cached.(*cachedToken)
		if valid && te.nowFunc().Before(ct.expiresAt) {
			return ct.accessToken, nil
		}
	}

	// Cache miss or expired — coalesce concurrent exchanges for the same clientID
	// via singleflight so only one goroutine hits the auth service.
	val, err, _ := te.flight.Do(clientID, func() (any, error) {
		// Double-check cache inside singleflight: another goroutine in the
		// same flight group may have already populated it just before us.
		if cached, ok := te.tokenCache.Load(clientID); ok {
			ct, valid := cached.(*cachedToken)
			if valid && te.nowFunc().Before(ct.expiresAt) {
				return ct.accessToken, nil
			}
		}

		token, expiresIn, exErr := te.exchangeCredentials(ctx, clientID, clientSecret)
		if exErr != nil {
			return "", exErr
		}

		// Cache with buffer subtracted from expiry
		effectiveTTL := time.Duration(expiresIn)*time.Second - te.cacheBuffer
		if effectiveTTL < 0 {
			effectiveTTL = 0
		}

		te.tokenCache.Store(clientID, &cachedToken{
			accessToken: token,
			expiresAt:   te.nowFunc().Add(effectiveTTL),
		})

		return token, nil
	})
	if err != nil {
		return "", fmt.Errorf("singleflight token exchange: %w", err)
	}

	token, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("%w: got %T", ErrTokenExchangeUnexpectedResult, val)
	}

	return token, nil
}

// InvalidateToken removes the cached token for the given client_id,
// forcing a fresh exchange on the next GetToken call.
func (te *TokenExchanger) InvalidateToken(clientID string) {
	te.tokenCache.Delete(clientID)
}

// InvalidateTokenByTenant evicts the Bearer token for a specific tenant from cache.
// If the tenant->clientID mapping is unknown (e.g., first-request for a new tenant
// that failed auth before RegisterTenantClient), clears the entire token cache as
// a safe fallback — over-invalidation is preferable to retrying with a stale token.
//
// Operational note: In deployments with high tenant counts (>10k), a burst of
// fallback invalidations could cause a brief thundering-herd of token re-exchanges.
// singleflight deduplicates per-clientID, limiting the actual auth-service load.
// If this becomes a concern, consider LRU eviction on tenantClientMap to reduce
// the number of tenants that hit the fallback path.
func (te *TokenExchanger) InvalidateTokenByTenant(tenantOrgID string) {
	if clientID, ok := te.tenantClientMap.Load(tenantOrgID); ok {
		if cid, valid := clientID.(string); valid && cid != "" {
			te.tokenCache.Delete(cid)

			return
		}
	}

	// Fallback: no mapping found — clear entire token cache.
	// This is safe because tokens are cheap to re-acquire and this only
	// happens on 401 (credential rotation scenario).
	te.tokenCache.Range(func(key, _ any) bool {
		te.tokenCache.Delete(key)

		return true
	})
}

// RegisterTenantClient records the mapping from tenantOrgID to clientID so that
// InvalidateTokenByTenant can efficiently invalidate the correct token without
// calling GetCredentials. Called by the fetcher client after a successful
// credential retrieval + token exchange sequence.
func (te *TokenExchanger) RegisterTenantClient(tenantOrgID, clientID string) {
	if tenantOrgID != "" && clientID != "" {
		te.tenantClientMap.Store(tenantOrgID, clientID)
	}
}

// Close clears the token cache and tenant->clientID mapping. Intended for
// graceful shutdown to narrow the memory-exposure window for secrets.
// Safe to call multiple times.
func (te *TokenExchanger) Close() {
	te.tokenCache.Range(func(key, _ any) bool {
		te.tokenCache.Delete(key)
		return true
	})
	te.tenantClientMap.Range(func(key, _ any) bool {
		te.tenantClientMap.Delete(key)
		return true
	})
}

// exchangeCredentials performs the actual HTTP POST to the auth service's
// OAuth2 token endpoint using the client_credentials grant type.
func (te *TokenExchanger) exchangeCredentials(ctx context.Context, clientID, clientSecret string) (string, int64, error) {
	endpoint := te.authURL + tokenEndpointPath

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", clientID)
	form.Set("client_secret", clientSecret)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, fmt.Errorf("building token exchange request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := te.httpClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("executing token exchange request: %w", err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTokenResponseSize))
	if err != nil {
		return "", 0, fmt.Errorf("reading token exchange response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		snippet := truncateBody(body, maxErrorBodyLen)

		return "", 0, fmt.Errorf("%w: auth service returned HTTP %d: %s", ErrTokenExchangeFailed, resp.StatusCode, snippet)
	}

	var tokenResp tokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", 0, fmt.Errorf("decoding token exchange response: %w", err)
	}

	if tokenResp.AccessToken == "" {
		return "", 0, ErrTokenExchangeEmptyToken
	}

	return tokenResp.AccessToken, tokenResp.ExpiresIn, nil
}

// truncateBody returns at most maxLen bytes of body as a string, appending
// an ellipsis when truncation occurs. This prevents auth service internals
// from leaking into error messages or logs.
func truncateBody(body []byte, maxLen int) string {
	if len(body) <= maxLen {
		return string(body)
	}

	return string(body[:maxLen]) + "...(truncated)"
}
