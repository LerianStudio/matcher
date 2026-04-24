// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fetcher

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	discoveryPorts "github.com/LerianStudio/matcher/internal/discovery/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// mockTokenExchanger is a test double for the discoveryPorts.TokenExchanger interface.
type mockTokenExchanger struct {
	token                   string
	tokenErr                error
	getTokenCalls           atomic.Int64
	invalidateTokenCalls    atomic.Int64
	invalidateByTenantCalls atomic.Int64
	registerCalls           atomic.Int64
	lastClientID            atomic.Value
	lastTenantForRegister   atomic.Value
	lastTenantForInvalidate atomic.Value
}

var _ discoveryPorts.TokenExchanger = (*mockTokenExchanger)(nil)

func (m *mockTokenExchanger) GetToken(_ context.Context, clientID, _ string) (string, error) {
	m.getTokenCalls.Add(1)
	m.lastClientID.Store(clientID)

	if m.tokenErr != nil {
		return "", m.tokenErr
	}

	return m.token, nil
}

func (m *mockTokenExchanger) InvalidateToken(_ string) {
	m.invalidateTokenCalls.Add(1)
}

func (m *mockTokenExchanger) InvalidateTokenByTenant(tenantOrgID string) {
	m.invalidateByTenantCalls.Add(1)
	m.lastTenantForInvalidate.Store(tenantOrgID)
}

func (m *mockTokenExchanger) RegisterTenantClient(tenantOrgID, clientID string) {
	m.registerCalls.Add(1)
	m.lastTenantForRegister.Store(tenantOrgID)
	m.lastClientID.Store(clientID)
}

// --- injectAuth tests ---

func TestInjectAuth_NilM2MProvider_NoOp(t *testing.T) {
	t.Parallel()

	client := &HTTPFetcherClient{
		m2mProvider: nil, // single-tenant mode
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://example.com", http.NoBody)
	require.NoError(t, err)

	err = client.injectAuth(context.Background(), req)

	assert.NoError(t, err)
	assert.Empty(t, req.Header.Get("Authorization"))
}

func TestInjectAuth_EmptyTenantInContext_FallsBackToDefault(t *testing.T) {
	t.Parallel()

	// When TenantIDKey is set to "" in context, auth.GetTenantID falls through
	// to getDefaultTenantID() which returns the configured default tenant.
	// Therefore injectAuth will still call GetCredentials with that default ID.
	m2m := &mockM2MProvider{
		creds: &sharedPorts.M2MCredentials{ClientID: "id", ClientSecret: "secret"},
	}

	client := &HTTPFetcherClient{
		m2mProvider: m2m,
	}

	//nolint:staticcheck // context.WithValue with auth.TenantIDKey is the canonical pattern for test contexts
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", http.NoBody)
	require.NoError(t, err)

	err = client.injectAuth(ctx, req)

	// auth.GetTenantID returns default tenant — credentials are injected.
	assert.NoError(t, err)

	defaultTenant := auth.GetTenantID(ctx)
	if defaultTenant != "" {
		// When a default tenant is configured, GetCredentials is called.
		assert.Equal(t, int64(1), m2m.getCalls.Load(), "GetCredentials should be called with the default tenant")
		assert.NotEmpty(t, req.Header.Get("Authorization"), "Auth should be injected for default tenant")
	} else {
		// Only when no default is configured does the no-op path trigger.
		assert.Equal(t, int64(0), m2m.getCalls.Load())
		assert.Empty(t, req.Header.Get("Authorization"))
	}
}

func TestInjectAuth_GetCredentialsError_Propagates(t *testing.T) {
	t.Parallel()

	m2m := &mockM2MProvider{
		err: errors.New("vault sealed"),
	}

	client := &HTTPFetcherClient{
		m2mProvider: m2m,
	}

	ctx := tenantContext("tenant-123")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", http.NoBody)
	require.NoError(t, err)

	err = client.injectAuth(ctx, req)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "M2M credentials")
	assert.Contains(t, err.Error(), "tenant-123")
}

func TestInjectAuth_NilCredentials_ReturnsError(t *testing.T) {
	t.Parallel()

	m2m := &mockM2MProvider{
		creds: nil,
		err:   nil,
	}

	client := &HTTPFetcherClient{
		m2mProvider: m2m,
	}

	ctx := tenantContext("tenant-nil-creds")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", http.NoBody)
	require.NoError(t, err)

	err = client.injectAuth(ctx, req)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherNilCredentials)
	assert.Contains(t, err.Error(), "tenant-nil-creds")
}

func TestInjectAuth_WithTokenExchanger_SetsBearerToken(t *testing.T) {
	t.Parallel()

	m2m := &mockM2MProvider{
		creds: &sharedPorts.M2MCredentials{ClientID: "client-1", ClientSecret: "secret-1"},
	}
	te := &mockTokenExchanger{token: "bearer-token-xyz"}

	client := &HTTPFetcherClient{
		m2mProvider:    m2m,
		tokenExchanger: te,
	}

	ctx := tenantContext("tenant-bearer")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", http.NoBody)
	require.NoError(t, err)

	err = client.injectAuth(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "Bearer bearer-token-xyz", req.Header.Get("Authorization"))
	assert.Equal(t, int64(1), te.getTokenCalls.Load())
	assert.Equal(t, int64(1), te.registerCalls.Load(), "RegisterTenantClient should be called")
}

func TestInjectAuth_WithTokenExchanger_GetTokenError_Propagates(t *testing.T) {
	t.Parallel()

	m2m := &mockM2MProvider{
		creds: &sharedPorts.M2MCredentials{ClientID: "client-1", ClientSecret: "secret-1"},
	}
	te := &mockTokenExchanger{tokenErr: errors.New("auth server down")}

	client := &HTTPFetcherClient{
		m2mProvider:    m2m,
		tokenExchanger: te,
	}

	ctx := tenantContext("tenant-token-err")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", http.NoBody)
	require.NoError(t, err)

	err = client.injectAuth(ctx, req)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "exchanging credentials for bearer token")
}

func TestInjectAuth_WithoutTokenExchanger_SetsBasicAuth(t *testing.T) {
	t.Parallel()

	m2m := &mockM2MProvider{
		creds: &sharedPorts.M2MCredentials{ClientID: "basic-id", ClientSecret: "basic-secret"},
	}

	client := &HTTPFetcherClient{
		m2mProvider:    m2m,
		tokenExchanger: nil, // no token exchanger — fallback to BasicAuth
	}

	ctx := tenantContext("tenant-basic")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", http.NoBody)
	require.NoError(t, err)

	err = client.injectAuth(ctx, req)

	require.NoError(t, err)

	username, password, ok := req.BasicAuth()
	require.True(t, ok, "BasicAuth should be set")
	assert.Equal(t, "basic-id", username)
	assert.Equal(t, "basic-secret", password)
}

func TestInjectAuth_TokenExchanger_RegistersTenantClient(t *testing.T) {
	t.Parallel()

	m2m := &mockM2MProvider{
		creds: &sharedPorts.M2MCredentials{ClientID: "reg-client", ClientSecret: "reg-secret"},
	}
	te := &mockTokenExchanger{token: "some-token"}

	client := &HTTPFetcherClient{
		m2mProvider:    m2m,
		tokenExchanger: te,
	}

	ctx := tenantContext("tenant-register")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", http.NoBody)
	require.NoError(t, err)

	err = client.injectAuth(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, int64(1), te.registerCalls.Load())
	assert.Equal(t, "tenant-register", te.lastTenantForRegister.Load())
	assert.Equal(t, "reg-client", te.lastClientID.Load())
}

// --- invalidateM2MOnUnauthorized tests ---

func TestInvalidateM2MOnUnauthorized_Non401_NoOp(t *testing.T) {
	t.Parallel()

	m2m := &mockM2MProvider{
		creds: &sharedPorts.M2MCredentials{ClientID: "id", ClientSecret: "secret"},
	}

	client := &HTTPFetcherClient{
		m2mProvider: m2m,
	}

	ctx := tenantContext("tenant-200")

	client.invalidateM2MOnUnauthorized(ctx, http.StatusOK)

	assert.Equal(t, int64(0), m2m.invalidateCalls.Load(), "Should not invalidate on 200")
}

func TestInvalidateM2MOnUnauthorized_401_EmptyTenantContext_FallsBackToDefault(t *testing.T) {
	t.Parallel()

	// When TenantIDKey is set to "" in context, auth.GetTenantID falls through
	// to the configured default tenant. The no-op path only triggers when
	// GetTenantID truly returns "".
	m2m := &mockM2MProvider{
		creds: &sharedPorts.M2MCredentials{ClientID: "id", ClientSecret: "secret"},
	}

	client := &HTTPFetcherClient{
		m2mProvider: m2m,
	}

	//nolint:staticcheck // context.WithValue with auth.TenantIDKey is the canonical pattern for test contexts
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "")

	defaultTenant := auth.GetTenantID(ctx)

	client.invalidateM2MOnUnauthorized(ctx, http.StatusUnauthorized)

	if defaultTenant != "" {
		// Default tenant is configured — invalidation proceeds.
		assert.Equal(t, int64(1), m2m.invalidateCalls.Load(), "Should invalidate when default tenant is resolved")
	} else {
		// No default tenant — no-op path.
		assert.Equal(t, int64(0), m2m.invalidateCalls.Load(), "Should not invalidate without tenant")
	}
}

func TestInvalidateM2MOnUnauthorized_401_WithM2MProvider_Invalidates(t *testing.T) {
	t.Parallel()

	m2m := &mockM2MProvider{
		creds: &sharedPorts.M2MCredentials{ClientID: "id", ClientSecret: "secret"},
	}

	client := &HTTPFetcherClient{
		m2mProvider: m2m,
	}

	ctx := tenantContext("tenant-401")

	client.invalidateM2MOnUnauthorized(ctx, http.StatusUnauthorized)

	assert.Equal(t, int64(1), m2m.invalidateCalls.Load(), "Should invalidate M2M credentials on 401")
}

func TestInvalidateM2MOnUnauthorized_401_WithTokenExchanger_InvalidatesByTenant(t *testing.T) {
	t.Parallel()

	te := &mockTokenExchanger{}
	m2m := &mockM2MProvider{
		creds: &sharedPorts.M2MCredentials{ClientID: "id", ClientSecret: "secret"},
	}

	client := &HTTPFetcherClient{
		m2mProvider:    m2m,
		tokenExchanger: te,
	}

	ctx := tenantContext("tenant-te-401")

	client.invalidateM2MOnUnauthorized(ctx, http.StatusUnauthorized)

	assert.Equal(t, int64(1), te.invalidateByTenantCalls.Load(), "Token exchanger should be called")
	assert.Equal(t, "tenant-te-401", te.lastTenantForInvalidate.Load())
	assert.Equal(t, int64(1), m2m.invalidateCalls.Load(), "M2M provider should also be invalidated")
}

func TestInvalidateM2MOnUnauthorized_401_NilM2MProvider_OnlyTokenExchangerInvalidated(t *testing.T) {
	t.Parallel()

	te := &mockTokenExchanger{}

	client := &HTTPFetcherClient{
		m2mProvider:    nil, // nil M2M provider
		tokenExchanger: te,
	}

	ctx := tenantContext("tenant-no-m2m")

	client.invalidateM2MOnUnauthorized(ctx, http.StatusUnauthorized)

	// tokenExchanger.InvalidateTokenByTenant is still called — the nil check
	// for m2mProvider is after the tokenExchanger check.
	assert.Equal(t, int64(1), te.invalidateByTenantCalls.Load())
}

func TestInvalidateM2MOnUnauthorized_OtherErrorStatuses_NoOp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
	}{
		{"403 Forbidden", http.StatusForbidden},
		{"500 Internal Server Error", http.StatusInternalServerError},
		{"502 Bad Gateway", http.StatusBadGateway},
		{"400 Bad Request", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m2m := &mockM2MProvider{
				creds: &sharedPorts.M2MCredentials{ClientID: "id", ClientSecret: "secret"},
			}

			client := &HTTPFetcherClient{
				m2mProvider: m2m,
			}

			ctx := tenantContext("tenant-other")

			client.invalidateM2MOnUnauthorized(ctx, tt.statusCode)

			assert.Equal(t, int64(0), m2m.invalidateCalls.Load(), "Should NOT invalidate on status %d", tt.statusCode)
		})
	}
}

// --- Integration-level test: injectAuth + invalidateM2MOnUnauthorized via doRequestWithHeaders ---

func TestInjectAuth_BearerTokenAppearsInActualRequest(t *testing.T) {
	t.Parallel()

	var capturedAuthHeader string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuthHeader = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[]}`))
	}))
	defer srv.Close()

	m2m := &mockM2MProvider{
		creds: &sharedPorts.M2MCredentials{ClientID: "real-client", ClientSecret: "real-secret"},
	}
	te := &mockTokenExchanger{token: "real-bearer-token"}

	client := newTestClient(t, srv.URL)
	client.SetM2MProvider(m2m)
	client.SetTokenExchanger(te)

	ctx := tenantContext("tenant-e2e")

	_, err := client.ListConnections(ctx, "")
	require.NoError(t, err)

	assert.Equal(t, "Bearer real-bearer-token", capturedAuthHeader)
}
