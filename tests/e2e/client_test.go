//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	cfg := &E2EConfig{
		AppBaseURL:      "http://test-client:4018",
		DefaultTenantID: "client-tenant-123",
		RequestTimeout:  30 * time.Second,
	}

	client, err := NewClient(cfg)

	require.NoError(t, err)
	require.NotNil(t, client)
	assertSubclientsInitialized(t, client)
}

func assertSubclientsInitialized(t *testing.T, client *Client) {
	t.Helper()

	assert.NotNil(t, client.Configuration, "Configuration client should be initialized")
	assert.NotNil(t, client.Ingestion, "Ingestion client should be initialized")
	assert.NotNil(t, client.Matching, "Matching client should be initialized")
	assert.NotNil(t, client.Reporting, "Reporting client should be initialized")
	assert.NotNil(t, client.Governance, "Governance client should be initialized")
	assert.NotNil(t, client.Exception, "Exception client should be initialized")
}

func TestClient_SetTenantID(t *testing.T) {
	cfg := &E2EConfig{
		AppBaseURL:      "http://tenant-test:4018",
		DefaultTenantID: "initial-tenant",
		RequestTimeout:  10 * time.Second,
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)

	assert.Equal(t, "initial-tenant", client.TenantID())
	client.SetTenantID("new-tenant-id")
	assert.Equal(t, "new-tenant-id", client.TenantID())
}

func TestClient_WithDifferentConfigs(t *testing.T) {
	testCases := []struct {
		name    string
		cfg     *E2EConfig
		wantErr bool
	}{
		{
			name: "localhost config",
			cfg: &E2EConfig{
				AppBaseURL:      "http://localhost:4018",
				DefaultTenantID: "local-tenant",
				RequestTimeout:  10 * time.Second,
			},
		},
		{
			name: "remote config",
			cfg: &E2EConfig{
				AppBaseURL:      "https://api.example.com",
				DefaultTenantID: "remote-tenant-uuid",
				RequestTimeout:  60 * time.Second,
			},
		},
		{
			name: "minimal timeout",
			cfg: &E2EConfig{
				AppBaseURL:      "http://fast-api:3000",
				DefaultTenantID: "fast-tenant",
				RequestTimeout:  1 * time.Second,
			},
		},
		{
			name: "empty baseURL",
			cfg: &E2EConfig{
				AppBaseURL:      "",
				DefaultTenantID: "tenant",
				RequestTimeout:  5 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "zero timeout",
			cfg: &E2EConfig{
				AppBaseURL:      "http://timeout-test:4018",
				DefaultTenantID: "tenant",
				RequestTimeout:  0,
			},
			wantErr: true,
		},
		{
			name: "empty tenant ID",
			cfg: &E2EConfig{
				AppBaseURL:      "http://tenant-test:4018",
				DefaultTenantID: "",
				RequestTimeout:  5 * time.Second,
			},
			wantErr: true,
		},
		{
			name:    "nil config",
			cfg:     nil,
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client, err := NewClient(tc.cfg)
			if tc.wantErr {
				require.Error(t, err)
				assert.Nil(t, client)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, client)
			assertSubclientsInitialized(t, client)
		})
	}
}
