//go:build unit

package adapters

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/LerianStudio/matcher/internal/shared/ports"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestNewRemoteConfigurationAdapter_RequiresCoreFields(t *testing.T) {
	t.Parallel()

	_, err := NewRemoteConfigurationAdapter(RemoteConfigurationConfig{})
	require.ErrorIs(t, err, errTenantManagerURLRequired)

	_, err = NewRemoteConfigurationAdapter(RemoteConfigurationConfig{BaseURL: "http://tenant-manager"})
	require.ErrorIs(t, err, errTenantManagerServiceEmpty)

	_, err = NewRemoteConfigurationAdapter(RemoteConfigurationConfig{BaseURL: "http://tenant-manager", ServiceName: "matcher"})
	require.ErrorIs(t, err, errTenantManagerAPIKeyEmpty)
}

func TestRemoteConfigurationAdapter_GetTenantConfig(t *testing.T) {
	t.Parallel()

	expected := &ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://tenant-a-primary",
		PostgresReplicaDSN: "postgres://tenant-a-replica",
		PostgresPrimaryDB:  "matcher_tenant_a",
		PostgresReplicaDB:  "matcher_tenant_a_replica",
		RedisAddresses:     []string{"redis-a:6379", "redis-b:6379"},
		RedisPassword:      "secret",
		RedisDB:            4,
		RedisProtocol:      3,
		RedisPoolSize:      15,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/tenants/tenant-a/services/matcher/settings", r.URL.Path)
		assert.Equal(t, "service-api-key", r.Header.Get("X-API-Key"))
		assert.NotEmpty(t, r.Header.Get("traceparent"))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"config": expected}))
	}))
	defer server.Close()

	originalPropagator := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTextMapPropagator(originalPropagator)
	})

	adapter, err := NewRemoteConfigurationAdapter(RemoteConfigurationConfig{
		BaseURL:        server.URL,
		ServiceName:    "matcher",
		ServiceAPIKey:  "service-api-key",
		RequestTimeout: 2 * time.Second,
	})
	require.NoError(t, err)

	traceCtx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		SpanID:     trace.SpanID{2, 2, 2, 2, 2, 2, 2, 2},
		TraceFlags: trace.FlagsSampled,
	}))

	resolved, err := adapter.GetTenantConfig(traceCtx, "tenant-a")
	require.NoError(t, err)
	require.NotNil(t, resolved)
	assert.Equal(t, expected.PostgresPrimaryDSN, resolved.PostgresPrimaryDSN)
	assert.Equal(t, expected.PostgresReplicaDSN, resolved.PostgresReplicaDSN)
	assert.Equal(t, expected.RedisAddresses, resolved.RedisAddresses)

	resolved.RedisAddresses[0] = "mutated"
	assert.Equal(t, "redis-a:6379", expected.RedisAddresses[0], "returned config must be a defensive copy")
}

func TestRemoteConfigurationAdapter_GetTenantConfig_SendsEnvironmentMetadata(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "staging", r.URL.Query().Get("environment"))
		assert.Equal(t, "staging", r.Header.Get("X-Tenant-Environment"))
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"config": ports.TenantConfig{PostgresPrimaryDSN: "postgres://tenant-a"}}))
	}))
	defer server.Close()

	adapter, err := NewRemoteConfigurationAdapter(RemoteConfigurationConfig{
		BaseURL:         server.URL,
		ServiceName:     "matcher",
		ServiceAPIKey:   "service-api-key",
		EnvironmentName: "staging",
	})
	require.NoError(t, err)

	_, err = adapter.GetTenantConfig(context.Background(), "tenant-a")
	require.NoError(t, err)
}

func TestRemoteConfigurationAdapter_GetTenantConfig_Non200IsSanitized(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"secret-response-body","token":"should-not-leak"}`))
	}))
	defer server.Close()

	adapter, err := NewRemoteConfigurationAdapter(RemoteConfigurationConfig{
		BaseURL:       server.URL,
		ServiceName:   "matcher",
		ServiceAPIKey: "service-api-key",
	})
	require.NoError(t, err)

	_, err = adapter.GetTenantConfig(context.Background(), "tenant-a")
	require.Error(t, err)
	require.ErrorContains(t, err, "tenant settings request failed")
	assert.NotContains(t, err.Error(), "secret-response-body")
	assert.NotContains(t, err.Error(), "should-not-leak")
}

func TestRemoteConfigurationAdapter_GetTenantConfig_TransportError(t *testing.T) {
	t.Parallel()

	transportErr := errors.New("dial failed")
	adapter, err := NewRemoteConfigurationAdapter(RemoteConfigurationConfig{
		BaseURL:       "http://tenant-manager",
		ServiceName:   "matcher",
		ServiceAPIKey: "service-api-key",
		HTTPClient: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return nil, transportErr
		})},
	})
	require.NoError(t, err)

	_, err = adapter.GetTenantConfig(context.Background(), "tenant-a")
	require.Error(t, err)
	require.ErrorContains(t, err, "call tenant settings endpoint")
	require.ErrorContains(t, err, transportErr.Error())
}

func TestRemoteConfigurationAdapter_GetTenantConfig_RequestTimeout(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()

	adapter, err := NewRemoteConfigurationAdapter(RemoteConfigurationConfig{
		BaseURL:        server.URL,
		ServiceName:    "matcher",
		ServiceAPIKey:  "service-api-key",
		RequestTimeout: 20 * time.Millisecond,
	})
	require.NoError(t, err)

	_, err = adapter.GetTenantConfig(context.Background(), "tenant-a")
	require.Error(t, err)
	require.ErrorContains(t, err, "call tenant settings endpoint")
}

func TestRemoteConfigurationAdapter_GetTenantConfig_MalformedJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"config":`))
	}))
	defer server.Close()

	adapter, err := NewRemoteConfigurationAdapter(RemoteConfigurationConfig{
		BaseURL:       server.URL,
		ServiceName:   "matcher",
		ServiceAPIKey: "service-api-key",
	})
	require.NoError(t, err)

	_, err = adapter.GetTenantConfig(context.Background(), "tenant-a")
	require.Error(t, err)
	require.ErrorContains(t, err, "decode tenant settings response")
}

func TestRemoteConfigurationAdapter_GetTenantConfig_MissingPayload(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"status": "ok"}))
	}))
	defer server.Close()

	adapter, err := NewRemoteConfigurationAdapter(RemoteConfigurationConfig{
		BaseURL:       server.URL,
		ServiceName:   "matcher",
		ServiceAPIKey: "service-api-key",
	})
	require.NoError(t, err)

	_, err = adapter.GetTenantConfig(context.Background(), "tenant-a")
	require.ErrorIs(t, err, errTenantConfigPayloadMissing)
}

func TestRemoteConfigurationAdapter_GetTenantConfig_NullPayloadIsRejected(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"config": nil}))
	}))
	defer server.Close()

	adapter, err := NewRemoteConfigurationAdapter(RemoteConfigurationConfig{
		BaseURL:       server.URL,
		ServiceName:   "matcher",
		ServiceAPIKey: "service-api-key",
	})
	require.NoError(t, err)

	_, err = adapter.GetTenantConfig(context.Background(), "tenant-a")
	require.ErrorIs(t, err, errTenantConfigPayloadMissing)
}

func TestNewRemoteConfigurationAdapter_ProductionTransportPolicy(t *testing.T) {
	t.Parallel()

	_, err := NewRemoteConfigurationAdapter(RemoteConfigurationConfig{
		BaseURL:            "http://api.example.com",
		ServiceName:        "matcher",
		ServiceAPIKey:      "service-api-key",
		RuntimeEnvironment: "production",
	})
	require.Error(t, err)

	_, err = NewRemoteConfigurationAdapter(RemoteConfigurationConfig{
		BaseURL:            "http://localhost:4003",
		ServiceName:        "matcher",
		ServiceAPIKey:      "service-api-key",
		RuntimeEnvironment: "production",
	})
	require.ErrorIs(t, err, errUnsafeTenantManagerTransport)

	_, err = NewRemoteConfigurationAdapter(RemoteConfigurationConfig{
		BaseURL:            "https://tenant-manager.internal",
		ServiceName:        "matcher",
		ServiceAPIKey:      "service-api-key",
		RuntimeEnvironment: "production",
	})
	require.NoError(t, err)
}

func TestRemoteConfigurationAdapter_GetTenantConfig_DoesNotFollowRedirects(t *testing.T) {
	t.Parallel()

	redirected := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected = true
		w.WriteHeader(http.StatusOK)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"config": ports.TenantConfig{PostgresPrimaryDSN: "postgres://redirected"}}))
	}))
	defer target.Close()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()

	adapter, err := NewRemoteConfigurationAdapter(RemoteConfigurationConfig{
		BaseURL:       server.URL,
		ServiceName:   "matcher",
		ServiceAPIKey: "service-api-key",
	})
	require.NoError(t, err)

	_, err = adapter.GetTenantConfig(context.Background(), "tenant-a")
	require.Error(t, err)
	require.ErrorContains(t, err, "tenant settings request failed")
	assert.False(t, redirected, "tenant settings client must not follow redirects")
}

func TestRemoteConfigurationAdapter_GetTenantConfig_RequiresTenantID(t *testing.T) {
	t.Parallel()

	adapter, err := NewRemoteConfigurationAdapter(RemoteConfigurationConfig{
		BaseURL:       "http://tenant-manager",
		ServiceName:   "matcher",
		ServiceAPIKey: "service-api-key",
	})
	require.NoError(t, err)

	_, err = adapter.GetTenantConfig(context.Background(), "   ")
	require.ErrorIs(t, err, errTenantIDRequired)
}
