// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package auth

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authMiddleware "github.com/LerianStudio/lib-auth/v3/auth/middleware"

	"github.com/LerianStudio/lib-commons/v5/commons/jwt"
)

const testTokenSecret = "secret"

// Test helper functions for token building.
func buildTestToken(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()

	if _, ok := claims["exp"]; !ok {
		claims["exp"] = time.Now().Add(time.Hour).Unix()
	}

	signed, err := jwt.Sign(claims, jwt.AlgHS256, []byte(testTokenSecret))
	require.NoError(t, err)

	return signed
}

func buildTestTokenWithoutExp(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()

	signed, err := jwt.Sign(claims, jwt.AlgHS256, []byte(testTokenSecret))
	require.NoError(t, err)

	return signed
}

func buildTestTokenInvalidSignature(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()

	if _, ok := claims["exp"]; !ok {
		claims["exp"] = time.Now().Add(time.Hour).Unix()
	}

	signed, err := jwt.Sign(claims, jwt.AlgHS256, []byte("wrong-secret"))
	require.NoError(t, err)

	return signed
}

func newTestFiberApp(extractor *TenantExtractor) *fiber.App {
	app := fiber.New(fiber.Config{ErrorHandler: func(fiberCtx *fiber.Ctx, err error) error {
		code := fiber.StatusInternalServerError

		var fe *fiber.Error
		if errors.As(err, &fe) {
			code = fe.Code
		}

		return fiberCtx.Status(code).JSON(fiber.Map{
			"error": err.Error(),
			"code":  code,
		})
	}})
	app.Get("/tenant", extractor.ExtractTenant(), func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"tenantId":   GetTenantID(c.UserContext()),
			"tenantSlug": GetTenantSlug(c.UserContext()),
			"userId":     GetUserID(c.UserContext()),
		})
	})

	return app
}

func TestGetTenantID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ctx      context.Context
		expected string
	}{
		{
			name: "returns tenant from context",
			ctx: context.WithValue(
				context.Background(),
				TenantIDKey,
				"550e8400-e29b-41d4-a716-446655440000",
			),
			expected: "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:     "returns default when not set",
			ctx:      context.Background(),
			expected: DefaultTenantID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := GetTenantID(tt.ctx)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetTenantSlug(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ctx      context.Context
		expected string
	}{
		{
			name:     "returns tenant slug from context",
			ctx:      context.WithValue(context.Background(), TenantSlugKey, "tenant-slug"),
			expected: "tenant-slug",
		},
		{
			name:     "returns default when not set",
			ctx:      context.Background(),
			expected: DefaultTenantSlug,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := GetTenantSlug(tt.ctx)
			assert.Equal(t, tt.expected, result)
		})
	}
}

//nolint:paralleltest // This test modifies global state (default tenant settings)
func TestDefaultTenantSetters(t *testing.T) {
	originalID := getDefaultTenantID()
	originalSlug := getDefaultTenantSlug()

	require.NoError(t, SetDefaultTenantID("11111111-1111-1111-1111-111111111111"))
	require.NoError(t, SetDefaultTenantSlug("custom"))
	t.Cleanup(func() {
		_ = SetDefaultTenantID(originalID)
		_ = SetDefaultTenantSlug(originalSlug)
	})

	assert.Equal(t, "11111111-1111-1111-1111-111111111111", getDefaultTenantID())
	assert.Equal(t, "custom", getDefaultTenantSlug())
}

//nolint:paralleltest // This test modifies global state (default tenant settings)
func TestSetDefaultTenantID_InvalidUUID(t *testing.T) {
	originalID := getDefaultTenantID()
	t.Cleanup(func() {
		_ = SetDefaultTenantID(originalID)
	})

	t.Run("rejects non-UUID string", func(t *testing.T) {
		err := SetDefaultTenantID("not-a-uuid")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidTenantID)
		assert.Equal(t, originalID, getDefaultTenantID(), "value must not change on error")
	})

	t.Run("rejects whitespace-only string", func(t *testing.T) {
		err := SetDefaultTenantID("   ")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidTenantID)
	})

	t.Run("accepts valid UUID", func(t *testing.T) {
		err := SetDefaultTenantID("550e8400-e29b-41d4-a716-446655440000")
		require.NoError(t, err)
		assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", getDefaultTenantID())
	})

	t.Run("empty string resets to default", func(t *testing.T) {
		err := SetDefaultTenantID("")
		require.NoError(t, err)
		assert.Equal(t, DefaultTenantID, getDefaultTenantID())
	})
}

//nolint:paralleltest // This test modifies global state (default tenant settings)
func TestSetDefaultTenantSlug_Validation(t *testing.T) {
	originalSlug := getDefaultTenantSlug()
	t.Cleanup(func() {
		_ = SetDefaultTenantSlug(originalSlug)
	})

	t.Run("rejects whitespace-only string", func(t *testing.T) {
		err := SetDefaultTenantSlug("   ")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidTenantSlug)
		assert.Equal(t, originalSlug, getDefaultTenantSlug(), "value must not change on error")
	})

	t.Run("rejects tab-only string", func(t *testing.T) {
		err := SetDefaultTenantSlug("\t\t")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidTenantSlug)
	})

	t.Run("accepts valid slug", func(t *testing.T) {
		err := SetDefaultTenantSlug("my-tenant")
		require.NoError(t, err)
		assert.Equal(t, "my-tenant", getDefaultTenantSlug())
	})

	t.Run("empty string resets to default", func(t *testing.T) {
		err := SetDefaultTenantSlug("")
		require.NoError(t, err)
		assert.Equal(t, DefaultTenantSlug, getDefaultTenantSlug())
	})
}

func TestGetUserID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ctx      context.Context
		expected string
	}{
		{
			name:     "returns user from context",
			ctx:      context.WithValue(context.Background(), UserIDKey, "user-456"),
			expected: "user-456",
		},
		{
			name:     "returns empty when not set",
			ctx:      context.Background(),
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := GetUserID(tt.ctx)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestLookupTenantID(t *testing.T) {
	t.Parallel()

	t.Run("returns explicit tenant from context", func(t *testing.T) {
		t.Parallel()

		ctx := context.WithValue(context.Background(), TenantIDKey, "tenant-a")
		tenantID, ok := LookupTenantID(ctx)
		require.True(t, ok)
		assert.Equal(t, "tenant-a", tenantID)
	})

	t.Run("does not fall back to default tenant", func(t *testing.T) {
		t.Parallel()

		tenantID, ok := LookupTenantID(context.Background())
		require.False(t, ok)
		assert.Empty(t, tenantID)
	})

	t.Run("nil context returns no tenant", func(t *testing.T) {
		t.Parallel()

		tenantID, ok := LookupTenantID(nil)
		require.False(t, ok)
		assert.Empty(t, tenantID)
	})

	t.Run("whitespace tenant is rejected", func(t *testing.T) {
		t.Parallel()

		ctx := context.WithValue(context.Background(), TenantIDKey, "   ")
		tenantID, ok := LookupTenantID(ctx)
		require.False(t, ok)
		assert.Empty(t, tenantID)
	})
}

func TestExtractClaimsFromToken(t *testing.T) {
	t.Parallel()

	validTenantID := "550e8400-e29b-41d4-a716-446655440000"

	t.Run("valid tokens", func(t *testing.T) {
		t.Parallel()
		testExtractClaimsValidTokens(t, validTenantID)
	})

	t.Run("invalid tokens", func(t *testing.T) {
		t.Parallel()
		testExtractClaimsInvalidTokens(t, validTenantID)
	})

	t.Run("missing or optional claims", func(t *testing.T) {
		t.Parallel()
		testExtractClaimsMissingClaims(t, validTenantID)
	})
}

func testExtractClaimsValidTokens(t *testing.T, validTenantID string) {
	t.Helper()
	validToken := buildTestToken(t, jwt.MapClaims{
		"tenantId":   validTenantID,
		"tenantSlug": "test-slug",
		"sub":        "test-user",
	})

	t.Run("extracts tenantId, tenantSlug, and sub from valid token", func(t *testing.T) {
		t.Parallel()

		tenantID, tenantSlug, userID, err := extractClaimsFromToken(
			validToken,
			DefaultTenantID,
			DefaultTenantSlug,
			[]byte(testTokenSecret),
			false,
		)
		require.NoError(t, err)
		assert.Equal(t, validTenantID, tenantID)
		assert.Equal(t, "test-slug", tenantSlug)
		assert.Equal(t, "test-user", userID)
	})

	snakeCaseToken := buildTestToken(t, jwt.MapClaims{
		"tenant_id":   validTenantID,
		"tenant_slug": "snake-slug",
		"sub":         "snake-user",
	})

	t.Run("extracts snake_case tenant claims", func(t *testing.T) {
		t.Parallel()

		tenantID, tenantSlug, userID, err := extractClaimsFromToken(
			snakeCaseToken,
			DefaultTenantID,
			DefaultTenantSlug,
			[]byte(testTokenSecret),
			false,
		)
		require.NoError(t, err)
		assert.Equal(t, validTenantID, tenantID)
		assert.Equal(t, "snake-slug", tenantSlug)
		assert.Equal(t, "snake-user", userID)
	})
}

func testExtractClaimsInvalidTokens(t *testing.T, validTenantID string) {
	t.Helper()

	t.Run("returns error for invalid token", func(t *testing.T) {
		t.Parallel()

		_, _, _, err := extractClaimsFromToken(
			"invalid-token",
			DefaultTenantID,
			DefaultTenantSlug,
			[]byte(testTokenSecret),
			false,
		)
		require.Error(t, err)
	})

	t.Run("returns error for invalid signature", func(t *testing.T) {
		t.Parallel()
		badToken := buildTestTokenInvalidSignature(t, jwt.MapClaims{"tenantId": validTenantID})
		_, _, _, err := extractClaimsFromToken(
			badToken,
			DefaultTenantID,
			DefaultTenantSlug,
			[]byte(testTokenSecret),
			false,
		)
		require.Error(t, err)
	})

	t.Run("returns error when exp missing", func(t *testing.T) {
		t.Parallel()
		missingExpToken := buildTestTokenWithoutExp(t, jwt.MapClaims{
			"tenantId": validTenantID,
			"sub":      "user-exp-missing",
		})
		_, _, _, err := extractClaimsFromToken(
			missingExpToken,
			DefaultTenantID,
			DefaultTenantSlug,
			[]byte(testTokenSecret),
			false,
		)
		require.Error(t, err)
	})

	t.Run("returns error for non-string tenantId", func(t *testing.T) {
		t.Parallel()
		invalidTypeToken := buildTestToken(t, jwt.MapClaims{"tenantId": 123})
		_, _, _, err := extractClaimsFromToken(
			invalidTypeToken,
			DefaultTenantID,
			DefaultTenantSlug,
			[]byte(testTokenSecret),
			false,
		)
		require.Error(t, err)
	})

	t.Run("returns error for invalid tenant uuid", func(t *testing.T) {
		t.Parallel()
		invalidUUIDToken := buildTestToken(t, jwt.MapClaims{"tenant_id": "not-a-uuid"})
		_, _, _, err := extractClaimsFromToken(
			invalidUUIDToken,
			DefaultTenantID,
			DefaultTenantSlug,
			[]byte(testTokenSecret),
			false,
		)
		require.Error(t, err)
	})
}

func testExtractClaimsMissingClaims(t *testing.T, validTenantID string) {
	t.Helper()

	t.Run("defaults tenantSlug when missing", func(t *testing.T) {
		t.Parallel()
		tenantOnlyToken := buildTestToken(t, jwt.MapClaims{
			"tenantId": validTenantID,
			"sub":      "test-user",
		})
		tenantID, tenantSlug, userID, err := extractClaimsFromToken(
			tenantOnlyToken,
			DefaultTenantID,
			DefaultTenantSlug,
			[]byte(testTokenSecret),
			false,
		)
		require.NoError(t, err)
		assert.Equal(t, validTenantID, tenantID)
		assert.Equal(t, DefaultTenantSlug, tenantSlug)
		assert.Equal(t, "test-user", userID)
	})

	t.Run("ignores tenantSlug without tenantId", func(t *testing.T) {
		t.Parallel()
		slugOnlyToken := buildTestToken(t, jwt.MapClaims{
			"tenantSlug": "slug-only",
			"sub":        "user-2",
		})
		tenantID, tenantSlug, userID, err := extractClaimsFromToken(
			slugOnlyToken,
			DefaultTenantID,
			DefaultTenantSlug,
			[]byte(testTokenSecret),
			false,
		)
		require.NoError(t, err)
		assert.Equal(t, DefaultTenantID, tenantID)
		assert.Equal(t, DefaultTenantSlug, tenantSlug)
		assert.Equal(t, "user-2", userID)
	})

	t.Run(
		"defaults tenant when no tenant claim and requireTenantClaims is false",
		func(t *testing.T) {
			t.Parallel()
			noTenantToken := buildTestToken(t, jwt.MapClaims{
				"sub": "user-1",
			})
			tenantID, tenantSlug, userID, err := extractClaimsFromToken(
				noTenantToken,
				DefaultTenantID,
				DefaultTenantSlug,
				[]byte(testTokenSecret),
				false,
			)
			require.NoError(t, err)
			assert.Equal(t, DefaultTenantID, tenantID)
			assert.Equal(t, DefaultTenantSlug, tenantSlug)
			assert.Equal(t, "user-1", userID)
		},
	)

	t.Run("returns error when no tenant claim and requireTenantClaims is true", func(t *testing.T) {
		t.Parallel()
		noTenantToken := buildTestToken(t, jwt.MapClaims{
			"sub": "user-1",
		})
		_, _, _, err := extractClaimsFromToken(
			noTenantToken,
			DefaultTenantID,
			DefaultTenantSlug,
			[]byte(testTokenSecret),
			true,
		)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrMissingTenantClaim)
	})
}

func TestTenantExtractor(t *testing.T) {
	t.Parallel()

	t.Run("creates extractor with auth disabled", func(t *testing.T) {
		t.Parallel()

		extractor, err := NewTenantExtractor(
			false,
			false,
			DefaultTenantID,
			DefaultTenantSlug,
			testTokenSecret,
			"development",
		)
		require.NoError(t, err)
		assert.NotNil(t, extractor)
		assert.False(t, extractor.authEnabled)
	})

	t.Run("creates extractor with auth enabled", func(t *testing.T) {
		t.Parallel()

		extractor, err := NewTenantExtractor(
			true,
			true,
			DefaultTenantID,
			DefaultTenantSlug,
			testTokenSecret,
			"development",
		)
		require.NoError(t, err)
		assert.NotNil(t, extractor)
		assert.True(t, extractor.authEnabled)
	})
}

func TestTenantExtractor_ExtractTenant(t *testing.T) {
	t.Parallel()

	t.Run("auth disabled uses default tenant", func(t *testing.T) {
		t.Parallel()
		testExtractTenantAuthDisabled(t)
	})

	t.Run("auth enabled scenarios", func(t *testing.T) {
		t.Parallel()
		testExtractTenantAuthEnabled(t)
	})
}

func testExtractTenantAuthDisabled(t *testing.T) {
	t.Helper()

	extractor, err := NewTenantExtractor(
		false,
		false,
		DefaultTenantID,
		DefaultTenantSlug,
		testTokenSecret,
		"development",
	)
	require.NoError(t, err)
	app := newTestFiberApp(extractor)

	req := httptest.NewRequest(http.MethodGet, "/tenant", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var payload map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	assert.Equal(t, DefaultTenantID, payload["tenantId"])
	assert.Equal(t, DefaultTenantSlug, payload["tenantSlug"])
	assert.Empty(t, payload["userId"])
}

func testExtractTenantAuthEnabled(t *testing.T) {
	t.Helper()

	t.Run("missing token returns unauthorized", func(t *testing.T) {
		t.Parallel()

		extractor, err := NewTenantExtractor(
			true,
			true,
			DefaultTenantID,
			DefaultTenantSlug,
			testTokenSecret,
			"development",
		)
		require.NoError(t, err)
		app := newTestFiberApp(extractor)

		req := httptest.NewRequest(http.MethodGet, "/tenant", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

		var payload map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
		assert.Equal(t, ErrMissingToken.Error(), payload["error"])
		assert.InEpsilon(t, float64(http.StatusUnauthorized), payload["code"], 0.001)
	})

	t.Run("invalid token returns unauthorized", func(t *testing.T) {
		t.Parallel()

		extractor, err := NewTenantExtractor(
			true,
			true,
			DefaultTenantID,
			DefaultTenantSlug,
			testTokenSecret,
			"development",
		)
		require.NoError(t, err)
		app := newTestFiberApp(extractor)

		req := httptest.NewRequest(http.MethodGet, "/tenant", http.NoBody)
		req.Header.Set("Authorization", "Bearer invalid-token")
		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

		var payload map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
		assert.Equal(t, ErrInvalidToken.Error(), payload["error"])
		assert.InEpsilon(t, float64(http.StatusUnauthorized), payload["code"], 0.001)
	})

	t.Run("valid token sets tenant", func(t *testing.T) {
		t.Parallel()

		extractor, err := NewTenantExtractor(
			true,
			true,
			DefaultTenantID,
			DefaultTenantSlug,
			testTokenSecret,
			"development",
		)
		require.NoError(t, err)
		app := newTestFiberApp(extractor)

		token := buildTestToken(t, jwt.MapClaims{
			"tenantId":   "550e8400-e29b-41d4-a716-446655440000",
			"tenantSlug": "tenant-slug",
			"sub":        "user-123",
		})

		req := httptest.NewRequest(http.MethodGet, "/tenant", http.NoBody)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var payload map[string]string
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
		assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", payload["tenantId"])
		assert.Equal(t, "tenant-slug", payload["tenantSlug"])
		assert.Equal(t, "user-123", payload["userId"])
	})
}

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()

	t.Run("escapes embedded quotes and wraps in quotes", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "\"tenant\"\"schema\"", QuoteIdentifier("tenant\"schema"))
	})

	t.Run("empty string", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "\"\"", QuoteIdentifier(""))
	})

	t.Run("no special characters", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "\"abc123DEF\"", QuoteIdentifier("abc123DEF"))
	})

	t.Run("multiple consecutive double quotes", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, `"a""""b""""""c"`, QuoteIdentifier(`a""b"""c`))
	})

	t.Run("very long identifier", func(t *testing.T) {
		t.Parallel()

		in := strings.Repeat("a", 10_000)
		out := QuoteIdentifier(in)
		assert.Equal(t, `"`+in+`"`, out)
	})
}

func TestApplyTenantSchema(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		TenantIDKey,
		"550e8400-e29b-41d4-a716-446655440000",
	)

	//nolint:paralleltest // This subtest manipulates package-level defaults and uses shared sqlmock resources.
	t.Run("applies search_path in transaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)

		mock.ExpectBegin()

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		mock.ExpectExec("SET LOCAL search_path TO \"550e8400-e29b-41d4-a716-446655440000\", public").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectRollback()
		mock.ExpectClose()

		err = ApplyTenantSchema(ctx, tx)
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())
		require.NoError(t, db.Close())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("rejects connection executor", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)

		conn, err := db.Conn(ctx)
		require.NoError(t, err)

		err = ApplyTenantSchema(ctx, conn)
		require.Error(t, err)
		require.NoError(t, conn.Close())
		mock.ExpectClose()
		require.NoError(t, db.Close())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("rejects unsupported executor", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)

		err = ApplyTenantSchema(ctx, db)
		require.Error(t, err)
		mock.ExpectClose()
		require.NoError(t, db.Close())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("rejects typed nil transaction", func(t *testing.T) {
		t.Parallel()

		var tx *sql.Tx

		err := ApplyTenantSchema(ctx, tx)
		require.Error(t, err)
	})

	t.Run("rejects typed nil connection", func(t *testing.T) {
		t.Parallel()

		var conn *sql.Conn

		err := ApplyTenantSchema(ctx, conn)
		require.Error(t, err)
	})
}

func TestProtectedGroup_NilExtractor(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	client := authMiddleware.NewAuthClient("", false, nil)

	_, err := ProtectedGroupWithActionsWithMiddleware(app, client, nil, "resource", []string{"read"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNilTenantExtractor)
}

func TestAuthorize_NilClient(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/secure", Authorize(nil, "resource", "read"), func(c *fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/secure", http.NoBody))
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestExtractClaimsFromToken_RejectsNoneAlgorithm(t *testing.T) {
	t.Parallel()

	header := "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0"
	payload := "eyJ0ZW5hbnRJZCI6IjU1MGU4NDAwLWUyOWItNDFkNC1hNzE2LTQ0NjY1NTQ0MDAwMCIsInN1YiI6ImF0dGFja2VyIiwiZXhwIjo5OTk5OTk5OTk5fQ"
	noneToken := header + "." + payload + "."

	_, _, _, err := extractClaimsFromToken(
		noneToken, DefaultTenantID, DefaultTenantSlug,
		[]byte(testTokenSecret), false,
	)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestExtractClaimsFromToken_FutureNBF_RejectsToken(t *testing.T) {
	t.Parallel()

	validTenantID := "550e8400-e29b-41d4-a716-446655440000"
	futureNBFToken := buildTestToken(t, jwt.MapClaims{
		"tenantId": validTenantID,
		"sub":      "user-nbf",
		"nbf":      time.Now().Add(1 * time.Hour).Unix(),
	})

	_, _, _, err := extractClaimsFromToken(
		futureNBFToken,
		DefaultTenantID,
		DefaultTenantSlug,
		[]byte(testTokenSecret),
		false,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestExtractClaimsFromToken_PastNBF_AcceptsToken(t *testing.T) {
	t.Parallel()

	validTenantID := "550e8400-e29b-41d4-a716-446655440000"
	pastNBFToken := buildTestToken(t, jwt.MapClaims{
		"tenantId": validTenantID,
		"sub":      "user-nbf-past",
		"nbf":      time.Now().Add(-1 * time.Hour).Unix(),
	})

	tenantID, _, userID, err := extractClaimsFromToken(
		pastNBFToken,
		DefaultTenantID,
		DefaultTenantSlug,
		[]byte(testTokenSecret),
		false,
	)
	require.NoError(t, err)
	assert.Equal(t, validTenantID, tenantID)
	assert.Equal(t, "user-nbf-past", userID)
}

func TestExtractTenant_ExpiredToken_ReturnsUnauthorized(t *testing.T) {
	t.Parallel()

	extractor, err := NewTenantExtractor(
		true, true, DefaultTenantID, DefaultTenantSlug, testTokenSecret, "development",
	)
	require.NoError(t, err)
	app := newTestFiberApp(extractor)

	expiredToken := buildTestToken(t, jwt.MapClaims{
		"tenantId": "550e8400-e29b-41d4-a716-446655440000",
		"sub":      "user-123",
		"exp":      time.Now().Add(-1 * time.Hour).Unix(),
	})

	req := httptest.NewRequest(http.MethodGet, "/tenant", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+expiredToken)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestExtractTenant_XUserIDHeader_RejectedInProduction(t *testing.T) {
	t.Parallel()

	extractor, err := NewTenantExtractor(
		false, false, DefaultTenantID, DefaultTenantSlug, testTokenSecret, "production",
	)
	require.NoError(t, err)

	app := fiber.New()
	app.Get("/user", extractor.ExtractTenant(), func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"userId": GetUserID(c.UserContext())})
	})

	req := httptest.NewRequest(http.MethodGet, "/user", http.NoBody)
	req.Header.Set("X-User-ID", "spoofed-user-id")
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var payload map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	assert.Empty(t, payload["userId"], "X-User-ID header must be ignored in production")
}

func TestExtractTenant_XUserIDHeader_AcceptedInDevelopment(t *testing.T) {
	t.Parallel()

	extractor, err := NewTenantExtractor(
		false, false, DefaultTenantID, DefaultTenantSlug, testTokenSecret, "development",
	)
	require.NoError(t, err)

	app := fiber.New()
	app.Get("/user", extractor.ExtractTenant(), func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"userId": GetUserID(c.UserContext())})
	})

	req := httptest.NewRequest(http.MethodGet, "/user", http.NoBody)
	req.Header.Set("X-User-ID", "dev-user-id")
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var payload map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	assert.Equal(t, "dev-user-id", payload["userId"], "X-User-ID header must be accepted in development")
}

// TestExtractTenant_XUserIDHeader_RejectedOutsideDevAndTest documents the
// security contract: only explicit "development" and "test" environments
// accept the X-User-ID dev header. Staging, UAT, QA, preview, empty, and
// anything else all reject — those environments may hold real data, so a
// plain-header impersonation vector is not acceptable.
func TestExtractTenant_XUserIDHeader_RejectedOutsideDevAndTest(t *testing.T) {
	t.Parallel()

	for _, envName := range []string{"staging", "uat", "qa", "preview", "sandbox", "Production", ""} {
		t.Run(envName, func(t *testing.T) {
			t.Parallel()

			extractor, err := NewTenantExtractor(
				false, false, DefaultTenantID, DefaultTenantSlug, testTokenSecret, envName,
			)
			require.NoError(t, err)

			app := fiber.New()
			app.Get("/user", extractor.ExtractTenant(), func(c *fiber.Ctx) error {
				return c.JSON(fiber.Map{"userId": GetUserID(c.UserContext())})
			})

			req := httptest.NewRequest(http.MethodGet, "/user", http.NoBody)
			req.Header.Set("X-User-ID", "spoofed-user-id")
			resp, err := app.Test(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)

			var payload map[string]string
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
			assert.Empty(t, payload["userId"],
				"env %q: X-User-ID header must be ignored outside dev/test", envName)
		})
	}
}

// TestExtractTenant_XUserIDHeader_AcceptedInTest asserts that the dev header
// is also accepted in the "test" environment (case-insensitive). This keeps
// in-memory test harnesses and local e2e runs working while still rejecting
// every other deployed environment.
func TestExtractTenant_XUserIDHeader_AcceptedInTest(t *testing.T) {
	t.Parallel()

	for _, envName := range []string{"test", "TEST", "Test", "Development", "DEVELOPMENT"} {
		t.Run(envName, func(t *testing.T) {
			t.Parallel()

			extractor, err := NewTenantExtractor(
				false, false, DefaultTenantID, DefaultTenantSlug, testTokenSecret, envName,
			)
			require.NoError(t, err)

			app := fiber.New()
			app.Get("/user", extractor.ExtractTenant(), func(c *fiber.Ctx) error {
				return c.JSON(fiber.Map{"userId": GetUserID(c.UserContext())})
			})

			req := httptest.NewRequest(http.MethodGet, "/user", http.NoBody)
			req.Header.Set("X-User-ID", "dev-user-id")
			resp, err := app.Test(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)

			var payload map[string]string
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
			assert.Equal(t, "dev-user-id", payload["userId"],
				"env %q: X-User-ID header must be accepted", envName)
		})
	}
}

//nolint:paralleltest // This test modifies global state (default tenant settings)
func TestDefaultTenantConcurrentAccess(t *testing.T) {
	originalID := getDefaultTenantID()
	t.Cleanup(func() {
		_ = SetDefaultTenantID(originalID)
	})

	const goroutines = 100
	done := make(chan bool, goroutines*2)

	for range goroutines {
		go func() {
			_ = SetDefaultTenantID("550e8400-e29b-41d4-a716-446655440000")
			done <- true
		}()
		go func() {
			_ = getDefaultTenantID()
			done <- true
		}()
	}

	for range goroutines * 2 {
		<-done
	}

	// Verify state remains valid after concurrent access
	finalID := getDefaultTenantID()
	assert.NotEmpty(t, finalID, "default tenant ID should remain valid after concurrent access")
}
