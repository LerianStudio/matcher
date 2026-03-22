//go:build unit

package auth

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	authMiddleware "github.com/LerianStudio/lib-auth/v2/auth/middleware"
	"github.com/LerianStudio/lib-commons/v4/commons/jwt"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProtectedGroupWithNilExtractor(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	router := app.Group("/api")

	_, err := ProtectedGroupWithActionsWithMiddleware(router, nil, nil, "resource", []string{"read"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNilTenantExtractor)
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

	group, err := ProtectedGroupWithActionsWithMiddleware(router, nil, extractor, "resource", []string{"read"})
	require.NoError(t, err)
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

	_, err := ProtectedGroupWithActionsWithMiddleware(router, nil, nil, "resource", []string{"read"}, customMiddleware)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNilTenantExtractor)
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

	group, err := ProtectedGroupWithActionsWithMiddleware(
		router,
		nil,
		extractor,
		"resource",
		[]string{"read"},
		customMiddleware1,
		customMiddleware2,
	)
	require.NoError(t, err)
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

			group, err := ProtectedGroupWithActionsWithMiddleware(router, nil, extractor, tt.resource, []string{tt.action})
			require.NoError(t, err)
			assert.NotNil(t, group)
		})
	}
}

func TestProtectedGroupWithMiddleware_AuthRunsBeforeTenantExtraction(t *testing.T) {
	t.Parallel()

	app := fiber.New(fiber.Config{ErrorHandler: func(c *fiber.Ctx, err error) error {
		tenantID := ""
		if ctx := c.UserContext(); ctx != nil {
			tenantID, _ = ctx.Value(TenantIDKey).(string)
		}

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"tenantId": tenantID,
			"error":    err.Error(),
		})
	}})
	router := app.Group("/api")

	extractor, err := NewTenantExtractor(false, DefaultTenantID, DefaultTenantSlug, "", "development")
	require.NoError(t, err)

	group, err := ProtectedGroupWithActionsWithMiddleware(router, nil, extractor, "resource", []string{"read"})
	require.NoError(t, err)

	group.Get("/test", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	assert.Contains(t, string(body), "auth client not initialized")
	assert.Contains(t, string(body), `"tenantId":""`)
}

func TestProtectedGroup_AuthEnabledInvalidTokenFailsBeforeLibAuth(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	router := app.Group("/api")

	extractor, err := NewTenantExtractor(true, DefaultTenantID, DefaultTenantSlug, "matcher-secret", "development")
	require.NoError(t, err)

	authClient := authMiddleware.NewAuthClient("http://authz.local", true, nil)

	group, err := ProtectedGroupWithActionsWithMiddleware(router, authClient, extractor, "resource", []string{"read"})
	require.NoError(t, err)

	group.Get("/test", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/test", http.NoBody)
	req.Header.Set("Authorization", "Bearer not-a-jwt")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
	assert.Contains(t, string(body), ErrInvalidToken.Error())
	assert.NotContains(t, string(body), "Internal Server Error")
	assert.NotContains(t, string(body), "Forbidden")
}

func TestProtectedGroupWithActionsWithMiddleware_NilExtractor(t *testing.T) {
	t.Parallel()

	_, err := ProtectedGroupWithActionsWithMiddleware(
		fiber.New().Group("/api"), nil, nil, "resource", []string{"read", "write"},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNilTenantExtractor)
}

func TestProtectedGroupWithActionsWithMiddleware_EmptyActions(t *testing.T) {
	t.Parallel()

	extractor, err := NewTenantExtractor(
		false, DefaultTenantID, DefaultTenantSlug, "", "development",
	)
	require.NoError(t, err)

	_, err = ProtectedGroupWithActionsWithMiddleware(
		fiber.New().Group("/api"), nil, extractor, "resource", []string{},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoActions)
}

func TestProtectedGroupWithActionsWithMiddleware_EmptyActionString(t *testing.T) {
	t.Parallel()

	extractor, err := NewTenantExtractor(
		false, DefaultTenantID, DefaultTenantSlug, "", "development",
	)
	require.NoError(t, err)

	_, err = ProtectedGroupWithActionsWithMiddleware(
		fiber.New().Group("/api"), nil, extractor, "resource", []string{"read", "  ", "write"},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEmptyAction)
}

func TestProtectedGroupWithActionsWithMiddleware_ValidInputCreatesGroup(t *testing.T) {
	t.Parallel()

	extractor, err := NewTenantExtractor(
		false, DefaultTenantID, DefaultTenantSlug, "", "development",
	)
	require.NoError(t, err)

	group, err := ProtectedGroupWithActionsWithMiddleware(
		fiber.New().Group("/api"), nil, extractor, "resource", []string{"read", "write"},
	)
	require.NoError(t, err)
	require.NotNil(t, group)
}

func TestProtectedGroupWithActionsWithMiddleware_MultiActionEnforcement(t *testing.T) {
	t.Parallel()

	// Track which action checks the auth server receives.
	var actionChecks []string

	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_, _ = w.Write([]byte("healthy"))
		case "/v1/authorize":
			// Decode the request body to capture the action being checked.
			var reqBody map[string]any
			if err := json.NewDecoder(r.Body).Decode(&reqBody); err == nil {
				if action, ok := reqBody["action"].(string); ok {
					actionChecks = append(actionChecks, action)
				}
			}
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"authorized": true}))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer authServer.Close()

	app := fiber.New()
	router := app.Group("/api")

	extractor, err := NewTenantExtractor(true, DefaultTenantID, DefaultTenantSlug, testTokenSecret, "development")
	require.NoError(t, err)

	authClient := authMiddleware.NewAuthClient(authServer.URL, true, nil)

	var seenTenantID string
	additionalMiddleware := func(c *fiber.Ctx) error {
		seenTenantID, _ = LookupTenantID(c.UserContext())
		return c.Next()
	}

	group, err := ProtectedGroupWithActionsWithMiddleware(
		router, authClient, extractor, "resource", []string{"read", "write"}, additionalMiddleware,
	)
	require.NoError(t, err)

	group.Get("/test", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	token := buildTestToken(t, jwt.MapClaims{
		"tenantId":   "550e8400-e29b-41d4-a716-446655440000",
		"tenantSlug": "tenant-a",
		"sub":        "user-456",
	})
	req := httptest.NewRequest(http.MethodGet, "/api/test", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", seenTenantID)

	// Both "read" and "write" authorization checks must have been invoked.
	require.Len(t, actionChecks, 2, "expected both read and write action checks")
	assert.Equal(t, "read", actionChecks[0])
	assert.Equal(t, "write", actionChecks[1])
}

func TestProtectedGroupWithMiddleware_AdditionalMiddlewareSeesTenantAndUserAfterAuth(t *testing.T) {
	t.Parallel()

	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_, _ = w.Write([]byte("healthy"))
		case "/v1/authorize":
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"authorized": true}))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer authServer.Close()

	app := fiber.New()
	router := app.Group("/api")

	extractor, err := NewTenantExtractor(true, DefaultTenantID, DefaultTenantSlug, testTokenSecret, "development")
	require.NoError(t, err)

	authClient := authMiddleware.NewAuthClient(authServer.URL, true, nil)

	var seenTenantID string
	var seenUserID string
	additionalMiddleware := func(c *fiber.Ctx) error {
		seenTenantID, _ = LookupTenantID(c.UserContext())
		seenUserID = GetUserID(c.UserContext())
		return c.Next()
	}

	group, err := ProtectedGroupWithActionsWithMiddleware(
		router, authClient, extractor, "resource", []string{"read"}, additionalMiddleware,
	)
	require.NoError(t, err)

	group.Get("/test", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	token := buildTestToken(t, jwt.MapClaims{
		"tenantId":   "550e8400-e29b-41d4-a716-446655440000",
		"tenantSlug": "tenant-a",
		"sub":        "user-123",
	})
	req := httptest.NewRequest(http.MethodGet, "/api/test", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", seenTenantID)
	assert.Equal(t, "user-123", seenUserID)
}
