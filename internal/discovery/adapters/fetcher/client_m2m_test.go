// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fetcher

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	m2m "github.com/LerianStudio/matcher/internal/discovery/adapters/m2m"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// newTestTokenExchanger creates a TokenExchanger pointing at a test auth server.
// Uses WithInsecureHTTP since test servers use http:// URLs.
func newTestTokenExchanger(t *testing.T, authURL string) (*m2m.TokenExchanger, error) {
	t.Helper()

	return m2m.NewTokenExchanger(authURL, m2m.WithInsecureHTTP())
}

// tenantContext creates a context with a tenant ID that auth.GetTenantID can retrieve.
func tenantContext(tenantID string) context.Context {
	//nolint:staticcheck // context.WithValue with auth.TenantIDKey is the canonical pattern for test contexts
	return context.WithValue(context.Background(), auth.TenantIDKey, tenantID)
}

// mockM2MProvider is a test double for the M2MProvider interface.
type mockM2MProvider struct {
	creds           *sharedPorts.M2MCredentials
	err             error
	getCalls        atomic.Int64
	invalidateCalls atomic.Int64
	lastTenant      atomic.Value
}

func (m *mockM2MProvider) GetCredentials(_ context.Context, tenantOrgID string) (*sharedPorts.M2MCredentials, error) {
	m.getCalls.Add(1)
	m.lastTenant.Store(tenantOrgID)

	if m.err != nil {
		return nil, m.err
	}

	return m.creds, nil
}

func (m *mockM2MProvider) InvalidateCredentials(_ context.Context, _ string) error {
	m.invalidateCalls.Add(1)

	return nil
}

func TestHTTPFetcherClient_WithM2MProvider_InjectsBasicAuth(t *testing.T) {
	t.Parallel()

	var capturedAuth string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[]}`))
	}))
	defer server.Close()

	m2m := &mockM2MProvider{
		creds: &sharedPorts.M2MCredentials{
			ClientID:     "my-client-id",
			ClientSecret: "my-client-secret",
		},
	}

	client := newTestClient(t, server.URL)
	client.SetM2MProvider(m2m)

	// Set tenant ID in context (mimics auth middleware)
	ctx := tenantContext("tenant-org-123")

	_, err := client.ListConnections(ctx, "org-1")
	require.NoError(t, err)

	expectedAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("my-client-id:my-client-secret"))
	assert.Equal(t, expectedAuth, capturedAuth, "Should inject exact Basic auth header")
	assert.Equal(t, int64(1), m2m.getCalls.Load(), "GetCredentials should be called once")
}

func TestHTTPFetcherClient_WithoutM2MProvider_NoAuth(t *testing.T) {
	t.Parallel()

	var capturedAuth string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[]}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)

	// No M2M provider — single-tenant mode
	ctx := tenantContext("tenant-org-123")

	_, _ = client.ListConnections(ctx, "org-1")

	assert.Empty(t, capturedAuth, "Authorization header should NOT be set when m2mProvider is nil")
}

func TestHTTPFetcherClient_M2M_NoTenantInContext_UsesDefaultTenant(t *testing.T) {
	t.Parallel()

	// When no tenant is explicitly set in context, auth.GetTenantID returns the
	// configured default tenant (single-tenant mode). The M2M provider will be
	// called with the default tenant ID — this is the correct behavior because
	// in multi-tenant mode, requests always have an explicit tenant from JWT.

	m2m := &mockM2MProvider{
		creds: &sharedPorts.M2MCredentials{
			ClientID:     "default-id",
			ClientSecret: "default-secret",
		},
	}

	var capturedAuth string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[]}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	client.SetM2MProvider(m2m)

	// Empty context — auth.GetTenantID returns default tenant ID
	_, _ = client.ListConnections(context.Background(), "org-1")

	assert.NotEmpty(t, capturedAuth, "Should inject auth with default tenant credentials")
	assert.Equal(t, int64(1), m2m.getCalls.Load(), "GetCredentials should be called with default tenant")

	// Verify the provider received the default tenant ID (not empty string)
	expectedTenant := auth.GetTenantID(context.Background())
	assert.Equal(t, expectedTenant, m2m.lastTenant.Load(), "M2M provider should receive the default tenant ID")
}

func TestHTTPFetcherClient_M2M_CredentialError_PropagatesError(t *testing.T) {
	t.Parallel()

	m2m := &mockM2MProvider{
		err: errors.New("secrets manager unavailable"),
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[]}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	client.SetM2MProvider(m2m)

	ctx := tenantContext("tenant-org-123")

	_, err := client.ListConnections(ctx, "org-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "M2M credentials")
}

func TestHTTPFetcherClient_M2M_401Response_InvalidatesCredentials(t *testing.T) {
	t.Parallel()

	m2m := &mockM2MProvider{
		creds: &sharedPorts.M2MCredentials{
			ClientID:     "stale-id",
			ClientSecret: "stale-secret",
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	client.SetM2MProvider(m2m)

	ctx := tenantContext("tenant-org-123")

	_, err := client.ListConnections(ctx, "org-1")
	require.Error(t, err) // 401 produces a non-nil error from classifyResponse

	// With retry-on-401, the client retries once: 1st 401 → invalidate + retry → 2nd 401 → invalidate + return error
	assert.Equal(t, int64(2), m2m.invalidateCalls.Load(), "InvalidateCredentials should be called twice (once per 401)")
}

func TestWithM2MProvider_Option(t *testing.T) {
	t.Parallel()

	m2m := &mockM2MProvider{
		creds: &sharedPorts.M2MCredentials{
			ClientID:     "id",
			ClientSecret: "secret",
		},
	}

	opt := WithM2MProvider(m2m)

	client := &HTTPFetcherClient{}
	opt(client)

	assert.NotNil(t, client.m2mProvider, "M2MProvider should be set")
}

func TestWithM2MProvider_NilProvider(t *testing.T) {
	t.Parallel()

	opt := WithM2MProvider(nil)

	client := &HTTPFetcherClient{}
	opt(client)

	assert.Nil(t, client.m2mProvider, "M2MProvider should remain nil for nil input")
}

func TestHTTPFetcherClient_M2M_NilCredentials_ReturnsError(t *testing.T) {
	t.Parallel()

	// A provider that returns (nil, nil) — interface permits it even though
	// current implementations never do.
	nilCredsProvider := &mockM2MProvider{
		creds: nil,
		err:   nil,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[]}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	client.SetM2MProvider(nilCredsProvider)

	ctx := tenantContext("tenant-org-nil")

	_, err := client.ListConnections(ctx, "org-1")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherNilCredentials)
}

func TestHTTPFetcherClient_M2M_401WithTokenExchanger_InvalidatesByTenant(t *testing.T) {
	t.Parallel()

	// This test verifies the canonical OAuth2 single-retry on 401:
	// 1st 401 → invalidate caches, retry with fresh credentials (via injectAuth again)
	// 2nd 401 → return error (credentials genuinely invalid)
	//
	// GetCredentials is called twice: once for the original request, once for the retry.
	// InvalidateCredentials is called twice: once per 401 response.
	// The key verification is that invalidation uses InvalidateTokenByTenant
	// (via the reverse mapping), NOT GetCredentials for clientID lookup.

	m2mProv := &mockM2MProvider{
		creds: &sharedPorts.M2MCredentials{
			ClientID:     "test-client-id",
			ClientSecret: "test-client-secret",
		},
	}

	// Token exchanger auth server that returns a valid token
	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"test-token","token_type":"Bearer","expires_in":3600}`))
	}))
	defer authServer.Close()

	// Fetcher server that always returns 401
	fetcherServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
	}))
	defer fetcherServer.Close()

	client := newTestClient(t, fetcherServer.URL)
	client.SetM2MProvider(m2mProv)

	te, teErr := newTestTokenExchanger(t, authServer.URL)
	require.NoError(t, teErr)

	client.SetTokenExchanger(te)

	ctx := tenantContext("tenant-401-test")

	// Call: 1st 401 → invalidate + retry → 2nd 401 → return error
	_, err := client.ListConnections(ctx, "org-1")
	require.Error(t, err) // final 401 → error from classifyResponse

	// GetCredentials called twice: once per injectAuth (original + retry)
	assert.Equal(t, int64(2), m2mProv.getCalls.Load(),
		"GetCredentials should be called twice (original injectAuth + retry injectAuth)")

	// InvalidateCredentials called twice: once per 401 response
	assert.Equal(t, int64(2), m2mProv.invalidateCalls.Load(),
		"InvalidateCredentials should be called twice (once per 401 response)")
}

func TestHTTPFetcherClient_RetriesOnceOn401WithFreshToken(t *testing.T) {
	t.Parallel()

	// Simulate credential rotation: first request uses stale token → 401,
	// after cache invalidation the retry acquires a fresh token → 200.
	// Verifies that a single request succeeds transparently from the caller's perspective.

	var attempts atomic.Int32

	fetcherServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)

		authHeader := r.Header.Get("Authorization")

		if attempt == 1 {
			// First attempt: stale token → 401
			assert.Equal(t, "Bearer stale-token", authHeader, "First attempt should use stale token")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))

			return
		}

		// Second attempt: fresh token → 200
		assert.Equal(t, "Bearer fresh-token", authHeader, "Second attempt should use fresh token")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[{"id":"conn-1","type":"POSTGRESQL"}]}`))
	}))
	defer fetcherServer.Close()

	// Auth server returns different tokens on each call: stale then fresh
	var tokenCalls atomic.Int32

	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		call := tokenCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")

		if call == 1 {
			_, _ = w.Write([]byte(`{"access_token":"stale-token","token_type":"Bearer","expires_in":3600}`))

			return
		}

		_, _ = w.Write([]byte(`{"access_token":"fresh-token","token_type":"Bearer","expires_in":3600}`))
	}))
	defer authServer.Close()

	m2mProv := &mockM2MProvider{
		creds: &sharedPorts.M2MCredentials{
			ClientID:     "client-id",
			ClientSecret: "client-secret",
		},
	}

	client := newTestClient(t, fetcherServer.URL)
	client.SetM2MProvider(m2mProv)

	te, teErr := newTestTokenExchanger(t, authServer.URL)
	require.NoError(t, teErr)

	client.SetTokenExchanger(te)

	ctx := tenantContext("tenant-rotation-test")

	// The call should succeed transparently after the 401 retry
	conns, err := client.ListConnections(ctx, "org-1")
	require.NoError(t, err)
	require.Len(t, conns, 1)
	assert.Equal(t, "conn-1", conns[0].ID)

	// Exactly 2 attempts: 1st with stale token, 2nd with fresh token
	assert.Equal(t, int32(2), attempts.Load(), "Should have made exactly 2 HTTP attempts")

	// Invalidation happened once (on the first 401)
	assert.Equal(t, int64(1), m2mProv.invalidateCalls.Load(),
		"InvalidateCredentials should be called once (on the first 401)")
}
