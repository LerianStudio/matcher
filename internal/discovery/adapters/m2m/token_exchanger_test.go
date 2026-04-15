// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package m2m_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/adapters/m2m"
)

func TestNewTokenExchanger_ValidConfig(t *testing.T) {
	t.Parallel()

	te, err := m2m.NewTokenExchanger("https://auth.local")
	require.NoError(t, err)
	assert.NotNil(t, te)
}

func TestNewTokenExchanger_HTTPRejectedWithoutInsecureOption(t *testing.T) {
	t.Parallel()

	te, err := m2m.NewTokenExchanger("http://auth.local")
	assert.Nil(t, te)
	require.Error(t, err)
	assert.ErrorIs(t, err, m2m.ErrTokenExchangeInvalidAuthURL)
}

func TestNewTokenExchanger_HTTPAllowedWithInsecureOption(t *testing.T) {
	t.Parallel()

	te, err := m2m.NewTokenExchanger("http://auth.local", m2m.WithInsecureHTTP())
	require.NoError(t, err)
	assert.NotNil(t, te)
}

func TestNewTokenExchanger_InvalidScheme(t *testing.T) {
	t.Parallel()

	te, err := m2m.NewTokenExchanger("ftp://auth.local")
	assert.Nil(t, te)
	require.Error(t, err)
	assert.ErrorIs(t, err, m2m.ErrTokenExchangeInvalidAuthURL)
}

func TestNewTokenExchanger_EmptyAuthURL(t *testing.T) {
	t.Parallel()

	te, err := m2m.NewTokenExchanger("")
	assert.Nil(t, te)
	require.Error(t, err)
	assert.ErrorIs(t, err, m2m.ErrTokenExchangeAuthURLRequired)
}

func TestNewTokenExchanger_NilHTTPClient(t *testing.T) {
	t.Parallel()

	te, err := m2m.NewTokenExchanger("https://auth.local", m2m.WithHTTPClient(nil))
	// WithHTTPClient(nil) is a no-op — the default client remains, so this succeeds.
	require.NoError(t, err)
	assert.NotNil(t, te)
}

func TestNewTokenExchanger_WithCustomOptions(t *testing.T) {
	t.Parallel()

	customClient := &http.Client{Timeout: 5 * time.Second}

	te, err := m2m.NewTokenExchanger(
		"https://auth.local",
		m2m.WithHTTPClient(customClient),
		m2m.WithCacheBuffer(60*time.Second),
	)
	require.NoError(t, err)
	assert.NotNil(t, te)
}

func TestGetToken_Success(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/v1/login/oauth/access_token", r.URL.Path)
		assert.Equal(t, "application/x-www-form-urlencoded", r.Header.Get("Content-Type"))

		err := r.ParseForm()
		require.NoError(t, err)
		assert.Equal(t, "client_credentials", r.FormValue("grant_type"))
		assert.Equal(t, "my-client", r.FormValue("client_id"))
		assert.Equal(t, "my-secret", r.FormValue("client_secret"))

		w.Header().Set("Content-Type", "application/json")

		resp := map[string]interface{}{
			"access_token": "test-bearer-token-abc123",
			"token_type":   "Bearer",
			"expires_in":   3600,
		}

		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer server.Close()

	te, err := m2m.NewTokenExchanger(server.URL, m2m.WithHTTPClient(server.Client()), m2m.WithInsecureHTTP())
	require.NoError(t, err)

	ctx := context.Background()

	token, getErr := te.GetToken(ctx, "my-client", "my-secret")
	require.NoError(t, getErr)
	assert.Equal(t, "test-bearer-token-abc123", token)
}

func TestGetToken_CacheHit(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)
		w.Header().Set("Content-Type", "application/json")

		resp := map[string]interface{}{
			"access_token": "cached-token",
			"token_type":   "Bearer",
			"expires_in":   3600,
		}

		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer server.Close()

	te, err := m2m.NewTokenExchanger(server.URL, m2m.WithHTTPClient(server.Client()), m2m.WithInsecureHTTP())
	require.NoError(t, err)

	ctx := context.Background()

	// First call: cache miss -> HTTP request
	token1, err1 := te.GetToken(ctx, "client-1", "secret-1")
	require.NoError(t, err1)
	assert.Equal(t, "cached-token", token1)

	// Second call: cache hit -> no HTTP request
	token2, err2 := te.GetToken(ctx, "client-1", "secret-1")
	require.NoError(t, err2)
	assert.Equal(t, "cached-token", token2)

	// Only one HTTP call should have been made
	assert.Equal(t, int64(1), callCount.Load())
}

func TestGetToken_CacheExpired(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)
		w.Header().Set("Content-Type", "application/json")

		resp := map[string]interface{}{
			"access_token": fmt.Sprintf("token-v%d", callCount.Load()),
			"token_type":   "Bearer",
			"expires_in":   60, // 60 seconds expires_in
		}

		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer server.Close()

	// Use a controllable clock instead of time.Sleep for deterministic testing.
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clockFn := func() time.Time { return now }

	te, err := m2m.NewTokenExchanger(
		server.URL,
		m2m.WithHTTPClient(server.Client()),
		m2m.WithInsecureHTTP(),
		m2m.WithCacheBuffer(0),
		m2m.WithNowFunc(clockFn),
	)
	require.NoError(t, err)

	ctx := context.Background()

	// First call: cache miss -> HTTP request
	token1, err1 := te.GetToken(ctx, "client-1", "secret-1")
	require.NoError(t, err1)
	assert.Equal(t, "token-v1", token1)
	assert.Equal(t, int64(1), callCount.Load())

	// Advance clock past the 60s expiry to simulate token expiration
	now = now.Add(2 * time.Minute)

	// Second call: expired -> new HTTP request
	token2, err2 := te.GetToken(ctx, "client-1", "secret-1")
	require.NoError(t, err2)
	assert.Equal(t, "token-v2", token2)
	assert.Equal(t, int64(2), callCount.Load())
}

func TestGetToken_ServerError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal server error"}`))
	}))
	defer server.Close()

	te, err := m2m.NewTokenExchanger(server.URL, m2m.WithHTTPClient(server.Client()), m2m.WithInsecureHTTP())
	require.NoError(t, err)

	ctx := context.Background()

	token, getErr := te.GetToken(ctx, "client-1", "secret-1")
	assert.Empty(t, token)
	require.Error(t, getErr)
	assert.ErrorIs(t, getErr, m2m.ErrTokenExchangeFailed)
}

func TestGetToken_InvalidJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{not valid json}`))
	}))
	defer server.Close()

	te, err := m2m.NewTokenExchanger(server.URL, m2m.WithHTTPClient(server.Client()), m2m.WithInsecureHTTP())
	require.NoError(t, err)

	ctx := context.Background()

	token, getErr := te.GetToken(ctx, "client-1", "secret-1")
	assert.Empty(t, token)
	require.Error(t, getErr)
	assert.Contains(t, getErr.Error(), "decoding token exchange response")
}

func TestGetToken_EmptyAccessToken(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		resp := map[string]interface{}{
			"access_token": "",
			"token_type":   "Bearer",
			"expires_in":   3600,
		}

		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer server.Close()

	te, err := m2m.NewTokenExchanger(server.URL, m2m.WithHTTPClient(server.Client()), m2m.WithInsecureHTTP())
	require.NoError(t, err)

	ctx := context.Background()

	token, getErr := te.GetToken(ctx, "client-1", "secret-1")
	assert.Empty(t, token)
	require.Error(t, getErr)
	assert.ErrorIs(t, getErr, m2m.ErrTokenExchangeEmptyToken)
}

func TestInvalidateToken(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)
		w.Header().Set("Content-Type", "application/json")

		resp := map[string]interface{}{
			"access_token": "token-v" + fmt.Sprintf("%d", callCount.Load()),
			"token_type":   "Bearer",
			"expires_in":   3600,
		}

		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer server.Close()

	te, err := m2m.NewTokenExchanger(server.URL, m2m.WithHTTPClient(server.Client()), m2m.WithInsecureHTTP())
	require.NoError(t, err)

	ctx := context.Background()

	// First call: populates cache
	_, err1 := te.GetToken(ctx, "client-1", "secret-1")
	require.NoError(t, err1)
	assert.Equal(t, int64(1), callCount.Load())

	// Invalidate
	te.InvalidateToken("client-1")

	// Next call must re-exchange
	_, err2 := te.GetToken(ctx, "client-1", "secret-1")
	require.NoError(t, err2)
	assert.Equal(t, int64(2), callCount.Load())
}

func TestGetToken_CacheBuffer(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)
		w.Header().Set("Content-Type", "application/json")

		resp := map[string]interface{}{
			"access_token": fmt.Sprintf("token-v%d", callCount.Load()),
			"token_type":   "Bearer",
			"expires_in":   30, // 30s — exactly the default cache buffer
		}

		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer server.Close()

	// With default 30s buffer, a 30s expires_in yields 0s effective TTL
	// -> token is immediately "expired" on the next call
	te, err := m2m.NewTokenExchanger(server.URL, m2m.WithHTTPClient(server.Client()), m2m.WithInsecureHTTP())
	require.NoError(t, err)

	ctx := context.Background()

	// First call
	token1, err1 := te.GetToken(ctx, "client-1", "secret-1")
	require.NoError(t, err1)
	assert.Equal(t, "token-v1", token1)
	assert.Equal(t, int64(1), callCount.Load())

	// Second call: effective TTL was 0, so token is already expired -> new fetch
	token2, err2 := te.GetToken(ctx, "client-1", "secret-1")
	require.NoError(t, err2)
	assert.Equal(t, "token-v2", token2)
	assert.Equal(t, int64(2), callCount.Load())
}

func TestGetToken_TrailingSlashInAuthURL(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify no double slash in path
		assert.Equal(t, "/v1/login/oauth/access_token", r.URL.Path)

		w.Header().Set("Content-Type", "application/json")

		resp := map[string]interface{}{
			"access_token": "slash-token",
			"token_type":   "Bearer",
			"expires_in":   3600,
		}

		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer server.Close()

	// Add trailing slash — constructor should trim it
	te, err := m2m.NewTokenExchanger(server.URL+"/", m2m.WithHTTPClient(server.Client()), m2m.WithInsecureHTTP())
	require.NoError(t, err)

	ctx := context.Background()

	token, getErr := te.GetToken(ctx, "client-1", "secret-1")
	require.NoError(t, getErr)
	assert.Equal(t, "slash-token", token)
}

func TestGetToken_DifferentClients(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)

		err := r.ParseForm()
		require.NoError(t, err)

		w.Header().Set("Content-Type", "application/json")

		resp := map[string]interface{}{
			"access_token": "token-for-" + r.FormValue("client_id"),
			"token_type":   "Bearer",
			"expires_in":   3600,
		}

		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer server.Close()

	te, err := m2m.NewTokenExchanger(server.URL, m2m.WithHTTPClient(server.Client()), m2m.WithInsecureHTTP())
	require.NoError(t, err)

	ctx := context.Background()

	token1, err1 := te.GetToken(ctx, "client-A", "secret-A")
	require.NoError(t, err1)
	assert.Equal(t, "token-for-client-A", token1)

	token2, err2 := te.GetToken(ctx, "client-B", "secret-B")
	require.NoError(t, err2)
	assert.Equal(t, "token-for-client-B", token2)

	// Two different clients = two HTTP calls
	assert.Equal(t, int64(2), callCount.Load())
}

func TestGetToken_ContextCancellation(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Wait for client cancellation or fallback timeout — avoids sleeping
		// unnecessarily when the client context is already cancelled.
		select {
		case <-r.Context().Done():
			return
		case <-time.After(500 * time.Millisecond):
			// fall through to write response
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	te, err := m2m.NewTokenExchanger(server.URL, m2m.WithHTTPClient(server.Client()), m2m.WithInsecureHTTP())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	token, getErr := te.GetToken(ctx, "client-1", "secret-1")
	assert.Empty(t, token)
	require.Error(t, getErr)
}

func TestInvalidateTokenByTenant_WithMapping(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)
		w.Header().Set("Content-Type", "application/json")

		resp := map[string]interface{}{
			"access_token": fmt.Sprintf("token-v%d", callCount.Load()),
			"token_type":   "Bearer",
			"expires_in":   3600,
		}

		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	te, err := m2m.NewTokenExchanger(server.URL, m2m.WithHTTPClient(server.Client()), m2m.WithInsecureHTTP())
	require.NoError(t, err)

	ctx := context.Background()

	// Populate cache for client-1
	_, err = te.GetToken(ctx, "client-1", "secret-1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), callCount.Load())

	// Register tenant->client mapping
	te.RegisterTenantClient("tenant-abc", "client-1")

	// Invalidate by tenant — should remove the correct token
	te.InvalidateTokenByTenant("tenant-abc")

	// Next call must re-exchange (cache was invalidated)
	_, err = te.GetToken(ctx, "client-1", "secret-1")
	require.NoError(t, err)
	assert.Equal(t, int64(2), callCount.Load())
}

func TestInvalidateTokenByTenant_WithoutMapping_ClearsAll(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)

		_ = r.ParseForm()
		w.Header().Set("Content-Type", "application/json")

		resp := map[string]interface{}{
			"access_token": "token-for-" + r.FormValue("client_id"),
			"token_type":   "Bearer",
			"expires_in":   3600,
		}

		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	te, err := m2m.NewTokenExchanger(server.URL, m2m.WithHTTPClient(server.Client()), m2m.WithInsecureHTTP())
	require.NoError(t, err)

	ctx := context.Background()

	// Populate cache for two different clients
	_, err = te.GetToken(ctx, "client-A", "secret-A")
	require.NoError(t, err)

	_, err = te.GetToken(ctx, "client-B", "secret-B")
	require.NoError(t, err)
	assert.Equal(t, int64(2), callCount.Load())

	// Invalidate by unknown tenant — no mapping exists, so all tokens are cleared
	te.InvalidateTokenByTenant("unknown-tenant")

	// Both clients must re-exchange
	_, err = te.GetToken(ctx, "client-A", "secret-A")
	require.NoError(t, err)

	_, err = te.GetToken(ctx, "client-B", "secret-B")
	require.NoError(t, err)
	assert.Equal(t, int64(4), callCount.Load(), "both tokens should have been invalidated")
}

func TestInvalidateTokenByTenant_DoesNotAffectOtherTenants(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)

		_ = r.ParseForm()
		w.Header().Set("Content-Type", "application/json")

		resp := map[string]interface{}{
			"access_token": "token-for-" + r.FormValue("client_id"),
			"token_type":   "Bearer",
			"expires_in":   3600,
		}

		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	te, err := m2m.NewTokenExchanger(server.URL, m2m.WithHTTPClient(server.Client()), m2m.WithInsecureHTTP())
	require.NoError(t, err)

	ctx := context.Background()

	// Populate cache for two different clients with tenant mappings
	_, err = te.GetToken(ctx, "client-A", "secret-A")
	require.NoError(t, err)
	te.RegisterTenantClient("tenant-1", "client-A")

	_, err = te.GetToken(ctx, "client-B", "secret-B")
	require.NoError(t, err)
	te.RegisterTenantClient("tenant-2", "client-B")

	assert.Equal(t, int64(2), callCount.Load())

	// Invalidate only tenant-1's token
	te.InvalidateTokenByTenant("tenant-1")

	// tenant-1's client must re-exchange
	_, err = te.GetToken(ctx, "client-A", "secret-A")
	require.NoError(t, err)
	assert.Equal(t, int64(3), callCount.Load())

	// tenant-2's client should still be cached (no re-exchange)
	_, err = te.GetToken(ctx, "client-B", "secret-B")
	require.NoError(t, err)
	assert.Equal(t, int64(3), callCount.Load(), "tenant-2 token should still be cached")
}

func TestRegisterTenantClient_EmptyInputs(t *testing.T) {
	t.Parallel()

	te, err := m2m.NewTokenExchanger("https://auth.local")
	require.NoError(t, err)

	// Empty tenant or client should be a no-op (not crash)
	te.RegisterTenantClient("", "client-1")
	te.RegisterTenantClient("tenant-1", "")
	te.RegisterTenantClient("", "")

	// InvalidateTokenByTenant with empty tenant should not crash
	te.InvalidateTokenByTenant("")
}

func TestGetToken_Concurrent(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)

		// Simulate auth latency so concurrent goroutines overlap.
		time.Sleep(50 * time.Millisecond)

		w.Header().Set("Content-Type", "application/json")

		resp := map[string]interface{}{
			"access_token": "concurrent-token",
			"token_type":   "Bearer",
			"expires_in":   3600,
		}

		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer server.Close()

	te, err := m2m.NewTokenExchanger(server.URL, m2m.WithHTTPClient(server.Client()), m2m.WithInsecureHTTP())
	require.NoError(t, err)

	const goroutines = 20

	ctx := context.Background()
	errs := make(chan error, goroutines)
	tokens := make(chan string, goroutines)

	// Launch N goroutines all requesting the same clientID concurrently.
	for range goroutines {
		go func() {
			tok, gErr := te.GetToken(ctx, "same-client", "same-secret")
			errs <- gErr
			tokens <- tok
		}()
	}

	// Collect results
	for range goroutines {
		require.NoError(t, <-errs)
		assert.Equal(t, "concurrent-token", <-tokens)
	}

	// With singleflight, all concurrent callers share a single in-flight
	// exchange. Exactly 1 HTTP call should have been made.
	assert.Equal(t, int64(1), callCount.Load(),
		"singleflight should coalesce concurrent exchanges into exactly 1 HTTP call")
}

func TestClose_ClearsCachesAndIsIdempotent(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)
		w.Header().Set("Content-Type", "application/json")

		resp := map[string]interface{}{
			"access_token": fmt.Sprintf("token-v%d", callCount.Load()),
			"token_type":   "Bearer",
			"expires_in":   3600,
		}

		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	te, err := m2m.NewTokenExchanger(server.URL, m2m.WithHTTPClient(server.Client()), m2m.WithInsecureHTTP())
	require.NoError(t, err)

	ctx := context.Background()

	// Populate cache + tenant mapping
	_, err = te.GetToken(ctx, "client-1", "secret-1")
	require.NoError(t, err)
	te.RegisterTenantClient("tenant-abc", "client-1")
	assert.Equal(t, int64(1), callCount.Load())

	// Close clears caches
	te.Close()

	// Next call must re-exchange (cache was cleared)
	_, err = te.GetToken(ctx, "client-1", "secret-1")
	require.NoError(t, err)
	assert.Equal(t, int64(2), callCount.Load())

	// Close is safe to call multiple times
	te.Close()
	te.Close()
}
