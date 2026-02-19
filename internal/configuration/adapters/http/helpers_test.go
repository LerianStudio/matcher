//go:build unit

package http

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
)

func TestHelpersSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrMissingParameter", ErrMissingParameter},
		{"ErrTenantIDNotFound", ErrTenantIDNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			require.NotEmpty(t, tt.err.Error())
		})
	}
}

func TestTenantIDFromContext_DefaultWhenMissing(t *testing.T) {
	t.Parallel()

	// When no tenant ID is in context, auth.GetTenantID returns the default tenant ID
	tenantID, err := tenantIDFromContext(context.Background())
	require.NoError(t, err)
	require.Equal(t, auth.DefaultTenantID, tenantID.String())
}

func TestTenantIDFromContext_Valid(t *testing.T) {
	t.Parallel()

	validUUID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, validUUID)

	tenantID, err := tenantIDFromContext(ctx)
	require.NoError(t, err)
	require.Equal(t, validUUID, tenantID.String())
}

func TestTenantIDFromContext_InvalidUUID(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-uuid")

	_, err := tenantIDFromContext(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid tenant ID")
}
