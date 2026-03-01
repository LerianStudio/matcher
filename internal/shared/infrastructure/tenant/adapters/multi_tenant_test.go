//go:build unit

package adapters

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
)

// TestTenantContextPropagation_GetTenantID_ReturnsCorrectValue verifies that
// when a tenant ID is set in context via TenantIDKey, auth.GetTenantID returns it.
func TestTenantContextPropagation_GetTenantID_ReturnsCorrectValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tenantID   string
		wantResult string
	}{
		{
			name:       "valid_uuid_tenant_id",
			tenantID:   "550e8400-e29b-41d4-a716-446655440000",
			wantResult: "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:       "another_valid_tenant_id",
			tenantID:   "12345678-1234-1234-1234-123456789012",
			wantResult: "12345678-1234-1234-1234-123456789012",
		},
		{
			name:       "lowercase_tenant_id",
			tenantID:   "abcdef01-2345-6789-abcd-ef0123456789",
			wantResult: "abcdef01-2345-6789-abcd-ef0123456789",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.WithValue(context.Background(), auth.TenantIDKey, tt.tenantID)

			result := auth.GetTenantID(ctx)

			assert.Equal(t, tt.wantResult, result)
		})
	}
}

// TestTenantContextPropagation_GetTenantID_FallbackToDefault verifies that
// when no tenant ID is set in context, auth.GetTenantID returns the default.
func TestTenantContextPropagation_GetTenantID_FallbackToDefault(t *testing.T) {
	t.Parallel()

	defaultTenantID := auth.GetDefaultTenantID()

	tests := []struct {
		name string
		ctx  context.Context
	}{
		{
			name: "nil_context",
			ctx:  nil,
		},
		{
			name: "empty_context",
			ctx:  context.Background(),
		},
		{
			name: "context_with_nil_value",
			ctx:  context.WithValue(context.Background(), auth.TenantIDKey, nil),
		},
		{
			name: "context_with_empty_string",
			ctx:  context.WithValue(context.Background(), auth.TenantIDKey, ""),
		},
		{
			name: "context_with_wrong_type",
			ctx:  context.WithValue(context.Background(), auth.TenantIDKey, 12345),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := auth.GetTenantID(tt.ctx)

			assert.Equal(t, defaultTenantID, result)
		})
	}
}

// TestTenantContextPropagation_GetTenantSlug_ReturnsCorrectValue verifies that
// when a tenant slug is set in context, auth.GetTenantSlug returns it.
func TestTenantContextPropagation_GetTenantSlug_ReturnsCorrectValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tenantSlug string
		wantResult string
	}{
		{
			name:       "simple_slug",
			tenantSlug: "acme-corp",
			wantResult: "acme-corp",
		},
		{
			name:       "slug_with_numbers",
			tenantSlug: "tenant-123",
			wantResult: "tenant-123",
		},
		{
			name:       "single_word_slug",
			tenantSlug: "default",
			wantResult: "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.WithValue(context.Background(), auth.TenantSlugKey, tt.tenantSlug)

			result := auth.GetTenantSlug(ctx)

			assert.Equal(t, tt.wantResult, result)
		})
	}
}

// TestTenantContextPropagation_GetTenantSlug_FallbackToDefault verifies that
// when no tenant slug is set in context, auth.GetTenantSlug returns the default.
func TestTenantContextPropagation_GetTenantSlug_FallbackToDefault(t *testing.T) {
	t.Parallel()

	// Use the exported constant DefaultTenantSlug for comparison
	// since getDefaultTenantSlug() is not exported
	defaultTenantSlug := auth.DefaultTenantSlug

	tests := []struct {
		name string
		ctx  context.Context
	}{
		{
			name: "nil_context",
			ctx:  nil,
		},
		{
			name: "empty_context",
			ctx:  context.Background(),
		},
		{
			name: "context_with_nil_value",
			ctx:  context.WithValue(context.Background(), auth.TenantSlugKey, nil),
		},
		{
			name: "context_with_empty_string",
			ctx:  context.WithValue(context.Background(), auth.TenantSlugKey, ""),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := auth.GetTenantSlug(tt.ctx)

			assert.Equal(t, defaultTenantSlug, result)
		})
	}
}

// TestTenantContextPropagation_BothTenantIDAndSlug verifies that both
// tenant ID and tenant slug can be set and retrieved independently.
func TestTenantContextPropagation_BothTenantIDAndSlug(t *testing.T) {
	t.Parallel()

	tenantID := "550e8400-e29b-41d4-a716-446655440000"
	tenantSlug := "acme-corp"

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID)
	ctx = context.WithValue(ctx, auth.TenantSlugKey, tenantSlug)

	assert.Equal(t, tenantID, auth.GetTenantID(ctx))
	assert.Equal(t, tenantSlug, auth.GetTenantSlug(ctx))
}

// TestTenantContextPropagation_ContextInheritance verifies that tenant
// context values are inherited when creating child contexts.
func TestTenantContextPropagation_ContextInheritance(t *testing.T) {
	t.Parallel()

	tenantID := "550e8400-e29b-41d4-a716-446655440000"

	parentCtx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID)

	// Create child context with additional value
	childCtx := context.WithValue(parentCtx, "custom_key", "custom_value")

	// Tenant ID should be accessible from child context
	assert.Equal(t, tenantID, auth.GetTenantID(childCtx))
}

// TestDefaultTenantID_IsValidUUID verifies that the default tenant ID is a valid UUID.
func TestDefaultTenantID_IsValidUUID(t *testing.T) {
	t.Parallel()

	defaultTenantID := auth.GetDefaultTenantID()

	require.NotEmpty(t, defaultTenantID)
	// Check it has UUID format (8-4-4-4-12)
	assert.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`, defaultTenantID)
}

// TestDefaultTenantSlug_IsNotEmpty verifies that the default tenant slug is not empty.
func TestDefaultTenantSlug_IsNotEmpty(t *testing.T) {
	t.Parallel()

	// Use the exported constant DefaultTenantSlug
	defaultTenantSlug := auth.DefaultTenantSlug

	require.NotEmpty(t, defaultTenantSlug)
}
