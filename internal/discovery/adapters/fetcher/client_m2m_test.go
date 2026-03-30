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
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

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
		_, _ = w.Write([]byte(`{"connections":[]}`))
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
		_, _ = w.Write([]byte(`{"connections":[]}`))
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
		_, _ = w.Write([]byte(`{"connections":[]}`))
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
		_, _ = w.Write([]byte(`{"connections":[]}`))
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

	assert.Equal(t, int64(1), m2m.invalidateCalls.Load(), "InvalidateCredentials should be called on 401")
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
