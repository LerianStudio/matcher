//go:build unit

package auth

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProtectedGroupWithNilExtractor(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	router := app.Group("/api")

	group := ProtectedGroup(router, nil, nil, "resource", "read")
	require.NotNil(t, group)

	group.Get("/test", func(c *fiber.Ctx) error {
		return c.SendString("success")
	})

	req := httptest.NewRequest(http.MethodGet, "/api/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "tenant extractor not initialized")
}

//nolint:paralleltest // This test modifies global state (default tenant settings)
func TestProtectedGroupWithValidExtractorButNilAuthClient(t *testing.T) {
	originalID := getDefaultTenantID()
	originalSlug := getDefaultTenantSlug()

	t.Cleanup(func() {
		if err := SetDefaultTenantID(originalID); err != nil {
			t.Logf("SetDefaultTenantID cleanup failed: %v", err)
		}
		if err := SetDefaultTenantSlug(originalSlug); err != nil {
			t.Logf("SetDefaultTenantSlug cleanup failed: %v", err)
		}
	})

	app := fiber.New()
	router := app.Group("/api")

	extractor, err := NewTenantExtractor(false, "00000000-0000-0000-0000-000000000001", "test-slug", "", "development")
	require.NoError(t, err)

	group := ProtectedGroup(router, nil, extractor, "resource", "read")
	require.NotNil(t, group)

	group.Get("/test", func(c *fiber.Ctx) error {
		tenantID := GetTenantID(c.UserContext())
		return c.SendString("tenant:" + tenantID)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "auth client not initialized")
}

func TestProtectedGroupWithMiddleware_NilExtractor(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	router := app.Group("/api")

	customMiddleware := func(c *fiber.Ctx) error {
		c.Set("X-Custom", "applied")
		return c.Next()
	}

	group := ProtectedGroupWithMiddleware(router, nil, nil, "resource", "read", customMiddleware)
	require.NotNil(t, group)

	group.Get("/test", func(c *fiber.Ctx) error {
		return c.SendString("success")
	})

	req := httptest.NewRequest(http.MethodGet, "/api/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "tenant extractor not initialized")
}

func TestProtectedGroupWithMiddleware_HandlerSliceConstruction(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	router := app.Group("/api")

	extractor, err := NewTenantExtractor(
		false,
		DefaultTenantID,
		DefaultTenantSlug,
		"",
		"development",
	)
	require.NoError(t, err)

	customMiddleware1 := func(c *fiber.Ctx) error {
		c.Set("X-Custom-1", "applied")
		return c.Next()
	}

	customMiddleware2 := func(c *fiber.Ctx) error {
		c.Set("X-Custom-2", "applied")
		return c.Next()
	}

	group := ProtectedGroupWithMiddleware(
		router,
		nil,
		extractor,
		"resource",
		"read",
		customMiddleware1,
		customMiddleware2,
	)
	require.NotNil(t, group)

	group.Get("/test", func(c *fiber.Ctx) error {
		return c.SendString("success")
	})

	req := httptest.NewRequest(http.MethodGet, "/api/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "auth client not initialized")
}

func TestProtectedGroupWithDifferentResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resource string
		action   string
	}{
		{"contexts read", "contexts", "read"},
		{"contexts write", "contexts", "write"},
		{"sources read", "sources", "read"},
		{"jobs create", "jobs", "create"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			app := fiber.New()
			router := app.Group("/api")
			extractor, err := NewTenantExtractor(
				false,
				DefaultTenantID,
				DefaultTenantSlug,
				"",
				"development",
			)
			require.NoError(t, err)

			group := ProtectedGroup(router, nil, extractor, tt.resource, tt.action)
			assert.NotNil(t, group)
		})
	}
}
