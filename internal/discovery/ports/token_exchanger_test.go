//go:build unit

package ports_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/discovery/ports"
)

// mockTokenExchanger is a test double for the TokenExchanger interface.
type mockTokenExchanger struct {
	getTokenFunc             func(ctx context.Context, clientID, clientSecret string) (string, error)
	invalidateTokenFunc      func(clientID string)
	invalidateTokenByTenFunc func(tenantOrgID string)
	registerTenantCliFn      func(tenantOrgID, clientID string)
}

func (m *mockTokenExchanger) GetToken(ctx context.Context, clientID, clientSecret string) (string, error) {
	if m.getTokenFunc != nil {
		return m.getTokenFunc(ctx, clientID, clientSecret)
	}

	return "", nil
}

func (m *mockTokenExchanger) InvalidateToken(clientID string) {
	if m.invalidateTokenFunc != nil {
		m.invalidateTokenFunc(clientID)
	}
}

func (m *mockTokenExchanger) InvalidateTokenByTenant(tenantOrgID string) {
	if m.invalidateTokenByTenFunc != nil {
		m.invalidateTokenByTenFunc(tenantOrgID)
	}
}

func (m *mockTokenExchanger) RegisterTenantClient(tenantOrgID, clientID string) {
	if m.registerTenantCliFn != nil {
		m.registerTenantCliFn(tenantOrgID, clientID)
	}
}

// Compile-time interface compliance check.
var _ ports.TokenExchanger = (*mockTokenExchanger)(nil)

func TestTokenExchanger_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	// Verify that the mock implements the interface correctly.
	var exchanger ports.TokenExchanger = &mockTokenExchanger{}
	assert.NotNil(t, exchanger)
}

func TestTokenExchanger_GetToken(t *testing.T) {
	t.Parallel()

	var capturedClientID string

	exchanger := &mockTokenExchanger{
		getTokenFunc: func(_ context.Context, clientID, _ string) (string, error) {
			capturedClientID = clientID

			return "bearer-token-abc", nil
		},
	}

	token, err := exchanger.GetToken(context.Background(), "client-123", "secret-456")

	assert.NoError(t, err)
	assert.Equal(t, "bearer-token-abc", token)
	assert.Equal(t, "client-123", capturedClientID)
}

func TestTokenExchanger_InvalidateToken(t *testing.T) {
	t.Parallel()

	var capturedClientID string

	exchanger := &mockTokenExchanger{
		invalidateTokenFunc: func(clientID string) {
			capturedClientID = clientID
		},
	}

	exchanger.InvalidateToken("client-123")

	assert.Equal(t, "client-123", capturedClientID)
}

func TestTokenExchanger_InvalidateTokenByTenant(t *testing.T) {
	t.Parallel()

	var capturedTenantOrgID string

	exchanger := &mockTokenExchanger{
		invalidateTokenByTenFunc: func(tenantOrgID string) {
			capturedTenantOrgID = tenantOrgID
		},
	}

	exchanger.InvalidateTokenByTenant("org-789")

	assert.Equal(t, "org-789", capturedTenantOrgID)
}

func TestTokenExchanger_RegisterTenantClient(t *testing.T) {
	t.Parallel()

	var capturedTenantOrgID, capturedClientID string

	exchanger := &mockTokenExchanger{
		registerTenantCliFn: func(tenantOrgID, clientID string) {
			capturedTenantOrgID = tenantOrgID
			capturedClientID = clientID
		},
	}

	exchanger.RegisterTenantClient("org-789", "client-123")

	assert.Equal(t, "org-789", capturedTenantOrgID)
	assert.Equal(t, "client-123", capturedClientID)
}
