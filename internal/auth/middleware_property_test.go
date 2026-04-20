//go:build unit

package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTenantIDInvariant_AlwaysReturnsValue(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		ctx  context.Context
	}{
		{"nil context", nil},
		{"empty context", context.Background()},
		{"nil value", context.WithValue(context.Background(), TenantIDKey, nil)},
		{"wrong type", context.WithValue(context.Background(), TenantIDKey, 12345)},
		{"empty string", context.WithValue(context.Background(), TenantIDKey, "")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := GetTenantID(tc.ctx)
			assert.NotEmpty(t, result, "GetTenantID must never return empty string")
		})
	}
}

func TestTenantSlugInvariant_AlwaysReturnsValue(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		ctx  context.Context
	}{
		{"nil context", nil},
		{"empty context", context.Background()},
		{"nil value", context.WithValue(context.Background(), TenantSlugKey, nil)},
		{"wrong type", context.WithValue(context.Background(), TenantSlugKey, 12345)},
		{"empty string", context.WithValue(context.Background(), TenantSlugKey, "")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := GetTenantSlug(tc.ctx)
			assert.NotEmpty(t, result, "GetTenantSlug must never return empty string")
			assert.Equal(t, DefaultTenantSlug, result)
		})
	}
}

func TestUserIDInvariant_HandlesEdgeCases(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		ctx      context.Context
		expected string
	}{
		{"nil context", nil, ""},
		{"empty context", context.Background(), ""},
		{"nil value", context.WithValue(context.Background(), UserIDKey, nil), ""},
		{"wrong type", context.WithValue(context.Background(), UserIDKey, 12345), ""},
		{"empty string", context.WithValue(context.Background(), UserIDKey, ""), ""},
		{"valid user id", context.WithValue(context.Background(), UserIDKey, "user-123"), "user-123"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := GetUserID(tc.ctx)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestTenantIDInvariant_JWTContextOnly(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		TenantIDKey,
		"550e8400-e29b-41d4-a716-446655440000",
	)
	ctx = context.WithValue(ctx, TenantSlugKey, "jwt-tenant")
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", GetTenantID(ctx))
	assert.Equal(t, "jwt-tenant", GetTenantSlug(ctx))

	headerCtx := context.WithValue(context.Background(), contextKey("X-Tenant-ID"), "header-tenant")
	headerCtx = context.WithValue(headerCtx, contextKey("X-Tenant-Slug"), "header-slug")
	assert.Equal(t, DefaultTenantID, GetTenantID(headerCtx), "header injection should be ignored")
	assert.Equal(
		t,
		DefaultTenantSlug,
		GetTenantSlug(headerCtx),
		"header injection should be ignored",
	)

	payloadCtx := context.WithValue(
		context.Background(),
		contextKey("body.tenantId"),
		"payload-tenant",
	)
	payloadCtx = context.WithValue(payloadCtx, contextKey("body.tenantSlug"), "payload-slug")
	assert.Equal(t, DefaultTenantID, GetTenantID(payloadCtx), "payload injection should be ignored")
	assert.Equal(
		t,
		DefaultTenantSlug,
		GetTenantSlug(payloadCtx),
		"payload injection should be ignored",
	)
}

func TestSingleTenantFallback_Property(t *testing.T) {
	t.Parallel()

	extractor, err := NewTenantExtractor(
		false,
		false,
		DefaultTenantID,
		DefaultTenantSlug,
		"test-secret",
		"development",
	)
	require.NoError(t, err)
	assert.False(t, extractor.authEnabled)

	app := fiber.New()
	app.Get("/tenant", extractor.ExtractTenant(), func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"tenantId":   GetTenantID(c.UserContext()),
			"tenantSlug": GetTenantSlug(c.UserContext()),
		})
	})

	t.Run("defaults when no tenant headers", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/tenant", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var payload map[string]string
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
		assert.Equal(t, DefaultTenantID, payload["tenantId"])
		assert.Equal(t, DefaultTenantSlug, payload["tenantSlug"])
	})

	t.Run("ignores tenant headers when auth disabled", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/tenant", http.NoBody)
		req.Header.Set("X-Tenant-ID", "header-tenant")
		req.Header.Set("X-Tenant-Slug", "header-slug")

		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var payload map[string]string
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
		assert.Equal(t, DefaultTenantID, payload["tenantId"])
		assert.Equal(t, DefaultTenantSlug, payload["tenantSlug"])
	})
}
