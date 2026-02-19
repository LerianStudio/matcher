//go:build unit

package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
	"github.com/LerianStudio/matcher/internal/auth"
	vo "github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

var errRedisConnection = errors.New("redis connection failed")

type stubIdempotencyRepo struct {
	acquireResult   bool
	acquireErr      error
	cachedResult    *IdempotencyResult
	getCachedErr    error
	markCompleteErr error
	markFailedErr   error
	reacquireResult bool
	reacquireErr    error

	acquireCalled      bool
	markCompleteCalled bool
	markFailedCalled   bool
	markFailedCalls    int
	getCachedCalled    bool
	reacquireCalled    bool
	lastKey            IdempotencyKey
	lastResponse       []byte
	lastStatus         int
}

func (repo *stubIdempotencyRepo) TryAcquire(_ context.Context, key IdempotencyKey) (bool, error) {
	repo.acquireCalled = true
	repo.lastKey = key

	return repo.acquireResult, repo.acquireErr
}

func (repo *stubIdempotencyRepo) MarkComplete(
	_ context.Context,
	key IdempotencyKey,
	response []byte,
	status int,
) error {
	repo.markCompleteCalled = true
	repo.lastKey = key
	repo.lastResponse = response
	repo.lastStatus = status

	return repo.markCompleteErr
}

func (repo *stubIdempotencyRepo) MarkFailed(_ context.Context, key IdempotencyKey) error {
	repo.markFailedCalled = true
	repo.markFailedCalls++
	repo.lastKey = key

	return repo.markFailedErr
}

func (repo *stubIdempotencyRepo) TryReacquireFromFailed(_ context.Context, key IdempotencyKey) (bool, error) {
	repo.reacquireCalled = true
	repo.lastKey = key

	return repo.reacquireResult, repo.reacquireErr
}

func decodeErrorResponse(t *testing.T, body io.Reader) libHTTP.ErrorResponse {
	t.Helper()

	var errResp libHTTP.ErrorResponse
	require.NoError(t, json.NewDecoder(body).Decode(&errResp))

	return errResp
}

func (repo *stubIdempotencyRepo) GetCachedResult(
	_ context.Context,
	key IdempotencyKey,
) (*IdempotencyResult, error) {
	repo.getCachedCalled = true
	repo.lastKey = key

	if repo.cachedResult != nil {
		return repo.cachedResult, repo.getCachedErr
	}

	return &IdempotencyResult{Status: IdempotencyStatusUnknown}, repo.getCachedErr
}

func TestIdempotencyMiddleware_SkipsGetRequests(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	repo := &stubIdempotencyRepo{}

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repo,
	}))

	app.Get("/test", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	req.Header.Set(HeaderXIdempotencyKey, "test-key")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.False(t, repo.acquireCalled, "should not check idempotency for GET")
}

func TestIdempotencyMiddleware_SkipsWithoutRepository(t *testing.T) {
	t.Parallel()

	app := fiber.New()

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{}))

	app.Post("/test", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader("body"))
	req.Header.Set(HeaderXIdempotencyKey, "test-key")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestIdempotencyMiddleware_SkipsPaths(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	repo := &stubIdempotencyRepo{}

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repo,
		SkipPaths:  []string{"/health", "/ready"},
	}))

	app.Post("/health", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	req := httptest.NewRequest(http.MethodPost, "/health", strings.NewReader("body"))
	req.Header.Set(HeaderXIdempotencyKey, "test-key")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.False(t, repo.acquireCalled, "should skip health endpoint")
}

func TestIdempotencyMiddleware_NewRequest_AcquiresLock(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	repo := &stubIdempotencyRepo{acquireResult: true}

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repo,
	}))

	app.Post("/test", func(c *fiber.Ctx) error {
		return c.Status(fiber.StatusCreated).JSON(fiber.Map{"id": "123"})
	})

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"data":"test"}`))
	req.Header.Set(HeaderXIdempotencyKey, "unique-key-123")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.True(t, repo.acquireCalled)
	// Key format: tenantID:method:path:userKey (no prefix in this test)
	expectedKey := IdempotencyKey(auth.DefaultTenantID + ":POST:/test:unique-key-123")
	assert.Equal(t, expectedKey, repo.lastKey)
	assert.True(t, repo.markCompleteCalled)
}

func TestIdempotencyMiddleware_DuplicateRequest_ReturnsCachedResponse(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	repo := &stubIdempotencyRepo{
		acquireResult: false,
		cachedResult: &IdempotencyResult{
			Status:     IdempotencyStatusComplete,
			Response:   []byte(`{"id":"cached-123"}`),
			HTTPStatus: http.StatusCreated,
		},
	}

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repo,
	}))

	app.Post("/test", func(_ *fiber.Ctx) error {
		t.Fatal("handler should not be called for cached response")
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"data":"test"}`))
	req.Header.Set(HeaderXIdempotencyKey, "unique-key-123")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.Equal(t, "true", resp.Header.Get(HeaderXIdempotencyReplayed))

	body, readErr := io.ReadAll(resp.Body)
	require.NoError(t, readErr)
	assert.JSONEq(t, `{"id":"cached-123"}`, string(body))
}

func TestIdempotencyMiddleware_PendingRequest_Returns409(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	repo := &stubIdempotencyRepo{
		acquireResult: false,
		cachedResult: &IdempotencyResult{
			Status: IdempotencyStatusPending,
		},
	}

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repo,
	}))

	app.Post("/test", func(_ *fiber.Ctx) error {
		t.Fatal("handler should not be called for pending request")
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"data":"test"}`))
	req.Header.Set(HeaderXIdempotencyKey, "unique-key-123")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	errResp := decodeErrorResponse(t, resp.Body)
	assert.Equal(t, http.StatusConflict, errResp.Code)
	assert.Equal(t, "request_in_progress", errResp.Title)
	assert.Equal(t, "A request with this idempotency key is currently being processed", errResp.Message)
}

func TestIdempotencyMiddleware_InvalidKeyFormat_Returns400Contract(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	repo := &stubIdempotencyRepo{}

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repo,
	}))

	app.Post("/test", func(_ *fiber.Ctx) error {
		t.Fatal("handler should not be called for invalid key")
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"data":"test"}`))
	req.Header.Set(HeaderXIdempotencyKey, "invalid key!")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	errResp := decodeErrorResponse(t, resp.Body)
	assert.Equal(t, http.StatusBadRequest, errResp.Code)
	assert.Equal(t, "invalid_idempotency_key", errResp.Title)
	assert.Equal(t, ErrInvalidIdempotencyKey.Error(), errResp.Message)
	assert.False(t, repo.acquireCalled, "repository should not be called when key format is invalid")
}

func TestIdempotencyMiddleware_MissingTenant_Returns500Contract(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	repo := &stubIdempotencyRepo{}

	app.Use(func(fiberCtx *fiber.Ctx) error {
		ctx := context.WithValue(context.Background(), auth.TenantIDKey, "   ")
		fiberCtx.SetUserContext(ctx)

		return fiberCtx.Next()
	})

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repo,
	}))

	app.Post("/test", func(_ *fiber.Ctx) error {
		t.Fatal("handler should not be called when tenant scoping is invalid")
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"data":"test"}`))
	req.Header.Set(HeaderXIdempotencyKey, "test-key")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	errResp := decodeErrorResponse(t, resp.Body)
	assert.Equal(t, http.StatusInternalServerError, errResp.Code)
	assert.Equal(t, "idempotency_configuration_error", errResp.Title)
	assert.Equal(t, "an unexpected error occurred", errResp.Message)
	assert.False(t, repo.acquireCalled, "repository should not be called when tenant is missing")
}

func TestIdempotencyMiddleware_FailedRequest_AllowsRetry(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	repo := &stubIdempotencyRepo{
		acquireResult:   false,
		reacquireResult: true,
		cachedResult: &IdempotencyResult{
			Status: IdempotencyStatusFailed,
		},
	}

	handlerCalled := false

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repo,
	}))

	app.Post("/test", func(c *fiber.Ctx) error {
		handlerCalled = true
		return c.SendString("retried")
	})

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"data":"test"}`))
	req.Header.Set(HeaderXIdempotencyKey, "unique-key-123")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, handlerCalled, "handler should be called for retry after failure")
	// Successful retry should mark complete and not mark failed again.
	assert.False(t, repo.markFailedCalled, "successful retry must not re-mark as failed")
	assert.Equal(t, 0, repo.markFailedCalls, "successful retry should not call MarkFailed")
	assert.True(t, repo.markCompleteCalled, "should cache the successful retry result")
}

func TestIdempotencyMiddleware_FailedRequest_RetryHandlerErrorMarksFailedTwice(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	repo := &stubIdempotencyRepo{
		acquireResult:   false,
		reacquireResult: true,
		cachedResult: &IdempotencyResult{
			Status: IdempotencyStatusFailed,
		},
	}

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repo,
	}))

	app.Post("/test", func(_ *fiber.Ctx) error {
		return fiber.NewError(fiber.StatusBadRequest, "validation failed")
	})

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"data":"test"}`))
	req.Header.Set(HeaderXIdempotencyKey, "retry-key")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.True(t, repo.markFailedCalled)
	assert.Equal(t, 1, repo.markFailedCalls, "failed retry should mark failed after handler error")
	assert.False(t, repo.markCompleteCalled, "failed retry must not mark complete")
}

func TestIdempotencyMiddleware_FailedRequest_ReacquireDeniedReturns409(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	repo := &stubIdempotencyRepo{
		acquireResult:   false,
		reacquireResult: false,
		cachedResult: &IdempotencyResult{
			Status: IdempotencyStatusFailed,
		},
	}

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repo,
	}))

	app.Post("/test", func(_ *fiber.Ctx) error {
		t.Fatal("handler should not be called when failed key retry was not reacquired")
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"data":"test"}`))
	req.Header.Set(HeaderXIdempotencyKey, "retry-key")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	errResp := decodeErrorResponse(t, resp.Body)
	assert.Equal(t, "request_in_progress", errResp.Title)
	assert.True(t, repo.reacquireCalled)
}

func TestIdempotencyMiddleware_WithAdapter_ReplaysCachedResponse(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	exceptionRepo := &stubExceptionRepo{
		acquireResult: false,
		cachedResult: &vo.IdempotencyResult{
			Status:     vo.IdempotencyStatusComplete,
			Response:   []byte(`{"id":"cached-adapter"}`),
			HTTPStatus: http.StatusAccepted,
		},
	}

	adapter := NewIdempotencyRepositoryAdapter(exceptionRepo)

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{Repository: adapter}))

	app.Post("/adapter", func(_ *fiber.Ctx) error {
		t.Fatal("handler should not be called for adapter replay")
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/adapter", strings.NewReader(`{"data":"test"}`))
	req.Header.Set(HeaderXIdempotencyKey, "adapter-key")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
	assert.Equal(t, "true", resp.Header.Get(HeaderXIdempotencyReplayed))
	body, readErr := io.ReadAll(resp.Body)
	require.NoError(t, readErr)
	assert.JSONEq(t, `{"id":"cached-adapter"}`, string(body))
}

func TestIdempotencyMiddleware_UsesAlternativeHeader(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	repo := &stubIdempotencyRepo{acquireResult: true}

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repo,
	}))

	app.Post("/test", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"data":"test"}`))
	req.Header.Set(HeaderIdempotencyKey, "alternative-key")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	// Key format: tenantID:method:path:userKey
	expectedKey := IdempotencyKey(auth.DefaultTenantID + ":POST:/test:alternative-key")
	assert.Equal(t, expectedKey, repo.lastKey)
}

func TestIdempotencyMiddleware_GeneratesHashForImplicitKey(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	repo := &stubIdempotencyRepo{acquireResult: true}

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repo,
	}))

	app.Post("/test", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"data":"test"}`))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, repo.acquireCalled)
	// Key format: tenantID:method:path:hash:xxxx
	assert.Contains(t, string(repo.lastKey), ":hash:", "should generate hash key")
	assert.True(
		t,
		strings.HasPrefix(string(repo.lastKey), auth.DefaultTenantID+":POST:/test:hash:"),
		"should have proper scoping prefix",
	)
}

func TestIdempotencyMiddleware_AppliesKeyPrefix(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	repo := &stubIdempotencyRepo{acquireResult: true}

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repo,
		KeyPrefix:  "matcher",
	}))

	app.Post("/test", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"data":"test"}`))
	req.Header.Set(HeaderXIdempotencyKey, "my-key")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	// Key format with prefix: prefix:tenantID:method:path:userKey
	expectedKey := IdempotencyKey("matcher:" + auth.DefaultTenantID + ":POST:/test:my-key")
	assert.Equal(t, expectedKey, repo.lastKey)
}

func TestIdempotencyMiddleware_MarksFailedOnError(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	repo := &stubIdempotencyRepo{acquireResult: true}

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repo,
	}))

	app.Post("/test", func(_ *fiber.Ctx) error {
		return fiber.NewError(fiber.StatusBadRequest, "validation failed")
	})

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"data":"test"}`))
	req.Header.Set(HeaderXIdempotencyKey, "error-key")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.True(t, repo.markFailedCalled, "should mark as failed on error")
	assert.False(t, repo.markCompleteCalled)
}

func TestIdempotencyMiddleware_AcquireError_Returns500(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	repo := &stubIdempotencyRepo{
		acquireResult: false,
		acquireErr:    errRedisConnection,
	}

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repo,
	}))

	app.Post("/test", func(_ *fiber.Ctx) error {
		t.Fatal("handler should not be called on acquire error")
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"data":"test"}`))
	req.Header.Set(HeaderXIdempotencyKey, "test-key")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	var errResp libHTTP.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	assert.Equal(t, http.StatusInternalServerError, errResp.Code)
	assert.Equal(t, "idempotency_error", errResp.Title)
	assert.Equal(t, "an unexpected error occurred", errResp.Message)
}

func TestIdempotencyMiddleware_SkipsWithoutKeyAndBody(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	repo := &stubIdempotencyRepo{}

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repo,
	}))

	app.Post("/test", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	req := httptest.NewRequest(http.MethodPost, "/test", http.NoBody)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.False(t, repo.acquireCalled, "should skip when no key and no body")
}

func TestExtractIdempotencyKey_PrefersXHeader(t *testing.T) {
	t.Parallel()

	app := fiber.New()

	var extractedKey string

	app.Post("/test", func(fiberCtx *fiber.Ctx) error {
		ctx := fiberCtx.UserContext()
		if ctx == nil {
			ctx = context.Background()
		}

		var err error

		extractedKey, err = extractIdempotencyKey(ctx, fiberCtx, "")
		require.NoError(t, err)

		return fiberCtx.SendString("ok")
	})

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader("body"))
	req.Header.Set(HeaderXIdempotencyKey, "x-header-key")
	req.Header.Set(HeaderIdempotencyKey, "standard-header-key")

	resp, err := app.Test(req)
	require.NoError(t, err)

	resp.Body.Close()

	// Key format: tenantID:method:path:userKey (X-Idempotency-Key takes precedence)
	expectedKey := auth.DefaultTenantID + ":POST:/test:x-header-key"
	assert.Equal(t, expectedKey, extractedKey)
}

func TestIdempotencyMiddleware_TenantIsolation(t *testing.T) {
	t.Parallel()

	repoA := &stubIdempotencyRepo{acquireResult: true}
	repoB := &stubIdempotencyRepo{acquireResult: true}

	// Middleware that sets tenant in context (simulating auth middleware)
	// In the real application, tenant extraction happens BEFORE idempotency middleware
	tenantMiddleware := func(tenantID string) fiber.Handler {
		return func(fiberCtx *fiber.Ctx) error {
			ctx := fiberCtx.UserContext()
			if ctx == nil {
				ctx = context.Background()
			}

			ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID)
			fiberCtx.SetUserContext(ctx)

			return fiberCtx.Next()
		}
	}

	// Create separate apps to simulate different tenants
	// In production, tenant middleware runs BEFORE idempotency middleware
	appA := fiber.New()
	appA.Use(tenantMiddleware("tenant-a-id")) // Tenant extraction first
	appA.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repoA,
		KeyPrefix:  "matcher",
	}))
	appA.Post("/test", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	appB := fiber.New()
	appB.Use(tenantMiddleware("tenant-b-id")) // Tenant extraction first
	appB.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repoB,
		KeyPrefix:  "matcher",
	}))
	appB.Post("/test", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	// Test tenant A
	reqA := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"data":"test"}`))
	reqA.Header.Set(HeaderXIdempotencyKey, "same-key")
	reqA.Header.Set("Content-Type", "application/json")

	respA, err := appA.Test(reqA)
	require.NoError(t, err)

	defer respA.Body.Close()

	keyA := repoA.lastKey

	// Test tenant B with same idempotency key and same path
	reqB := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"data":"test"}`))
	reqB.Header.Set(HeaderXIdempotencyKey, "same-key")
	reqB.Header.Set("Content-Type", "application/json")

	respB, err := appB.Test(reqB)
	require.NoError(t, err)

	defer respB.Body.Close()

	keyB := repoB.lastKey

	// Keys should be different because they include tenant ID
	assert.NotEqual(
		t,
		keyA,
		keyB,
		"same idempotency key should produce different keys for different tenants",
	)

	// Verify key format includes tenant ID
	assert.Contains(t, string(keyA), "tenant-a-id", "key A should contain tenant A ID")
	assert.Contains(t, string(keyB), "tenant-b-id", "key B should contain tenant B ID")

	// Verify the complete key format: prefix:tenantID:method:path:userKey
	assert.Equal(t, IdempotencyKey("matcher:tenant-a-id:POST:/test:same-key"), keyA)
	assert.Equal(t, IdempotencyKey("matcher:tenant-b-id:POST:/test:same-key"), keyB)
}

func TestIdempotencyMiddleware_MethodPathIsolation(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	repo := &stubIdempotencyRepo{acquireResult: true}

	app.Use(NewIdempotencyMiddleware(IdempotencyMiddlewareConfig{
		Repository: repo,
		KeyPrefix:  "matcher",
	}))

	app.Post("/resource", func(c *fiber.Ctx) error {
		return c.SendString("created")
	})

	app.Put("/resource", func(c *fiber.Ctx) error {
		return c.SendString("updated")
	})

	app.Patch("/other-resource", func(c *fiber.Ctx) error {
		return c.SendString("patched")
	})

	// Test POST /resource
	reqPost := httptest.NewRequest(
		http.MethodPost,
		"/resource",
		strings.NewReader(`{"data":"test"}`),
	)
	reqPost.Header.Set(HeaderXIdempotencyKey, "same-key")
	reqPost.Header.Set("Content-Type", "application/json")

	respPost, err := app.Test(reqPost)
	require.NoError(t, err)
	respPost.Body.Close()

	keyPost := repo.lastKey

	// Test PUT /resource with same idempotency key
	reqPut := httptest.NewRequest(http.MethodPut, "/resource", strings.NewReader(`{"data":"test"}`))
	reqPut.Header.Set(HeaderXIdempotencyKey, "same-key")
	reqPut.Header.Set("Content-Type", "application/json")

	respPut, err := app.Test(reqPut)
	require.NoError(t, err)
	respPut.Body.Close()

	keyPut := repo.lastKey

	// Test PATCH /other-resource with same idempotency key
	reqPatch := httptest.NewRequest(
		http.MethodPatch,
		"/other-resource",
		strings.NewReader(`{"data":"test"}`),
	)
	reqPatch.Header.Set(HeaderXIdempotencyKey, "same-key")
	reqPatch.Header.Set("Content-Type", "application/json")

	respPatch, err := app.Test(reqPatch)
	require.NoError(t, err)
	respPatch.Body.Close()

	keyPatch := repo.lastKey

	// All keys should be different due to method/path scoping
	assert.NotEqual(
		t,
		keyPost,
		keyPut,
		"same key on different methods should produce different idempotency keys",
	)
	assert.NotEqual(
		t,
		keyPost,
		keyPatch,
		"same key on different paths should produce different idempotency keys",
	)
	assert.NotEqual(
		t,
		keyPut,
		keyPatch,
		"different method+path combinations should produce different keys",
	)

	// Verify key format includes method and path
	assert.Contains(t, string(keyPost), ":POST:/resource:")
	assert.Contains(t, string(keyPut), ":PUT:/resource:")
	assert.Contains(t, string(keyPatch), ":PATCH:/other-resource:")
}

func TestExtractIdempotencyKey_MissingTenantID_ReturnsError(t *testing.T) {
	t.Parallel()

	app := fiber.New()

	var extractErr error

	app.Post("/test", func(fiberCtx *fiber.Ctx) error {
		// Inject a whitespace-only tenant ID directly into context so that
		// auth.GetTenantID returns "   " (passes != "" but fails TrimSpace).
		ctx := context.WithValue(context.Background(), auth.TenantIDKey, "   ")

		_, extractErr = extractIdempotencyKey(ctx, fiberCtx, "")

		return fiberCtx.SendString("ok")
	})

	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader("body"))
	req.Header.Set(HeaderXIdempotencyKey, "test-key")

	resp, err := app.Test(req)
	require.NoError(t, err)

	resp.Body.Close()

	require.ErrorIs(t, extractErr, ErrMissingTenantID,
		"should return ErrMissingTenantID when tenant ID is whitespace-only")
}

func TestValidateIdempotencyKeyFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		wantErr error
	}{
		{
			name:    "empty string",
			key:     "",
			wantErr: ErrEmptyIdempotencyKey,
		},
		{
			name:    "exactly 128 chars",
			key:     strings.Repeat("a", 128),
			wantErr: nil,
		},
		{
			name:    "129 chars exceeds max length",
			key:     strings.Repeat("a", 129),
			wantErr: ErrIdempotencyKeyTooLong,
		},
		{
			name:    "valid alphanumeric",
			key:     "abc123XYZ",
			wantErr: nil,
		},
		{
			name:    "valid UUID",
			key:     "550e8400-e29b-41d4-a716-446655440000",
			wantErr: nil,
		},
		{
			name:    "valid with hyphens and underscores",
			key:     "my-key_123",
			wantErr: nil,
		},
		{
			name:    "valid with colons",
			key:     "prefix:tenant:key",
			wantErr: nil,
		},
		{
			name:    "invalid with spaces",
			key:     "key with spaces",
			wantErr: ErrInvalidIdempotencyKey,
		},
		{
			name:    "invalid with special chars",
			key:     "key@#$%",
			wantErr: ErrInvalidIdempotencyKey,
		},
		{
			name:    "invalid unicode chars",
			key:     "key\u00e9\u00f1",
			wantErr: ErrInvalidIdempotencyKey,
		},
		{
			name:    "invalid null byte",
			key:     "key\x00value",
			wantErr: ErrInvalidIdempotencyKey,
		},
		{
			name:    "whitespace only",
			key:     "   ",
			wantErr: ErrInvalidIdempotencyKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateIdempotencyKeyFormat(tt.key)

			if tt.wantErr == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

// TestIdempotencyStatusConstants_MatchValueObjects verifies that the IdempotencyStatus
// constants defined in the shared HTTP package are equivalent to those defined in the
// exception domain value_objects package. Both packages define the same status strings
// independently; this test catches any future drift between the two sets of constants.
func TestIdempotencyStatusConstants_MatchValueObjects(t *testing.T) {
	t.Parallel()

	assert.Equal(t,
		string(IdempotencyStatusUnknown),
		string(vo.IdempotencyStatusUnknown),
		"IdempotencyStatusUnknown must match between shared/http and exception/domain/value_objects",
	)

	assert.Equal(t,
		string(IdempotencyStatusPending),
		string(vo.IdempotencyStatusPending),
		"IdempotencyStatusPending must match between shared/http and exception/domain/value_objects",
	)

	assert.Equal(t,
		string(IdempotencyStatusComplete),
		string(vo.IdempotencyStatusComplete),
		"IdempotencyStatusComplete must match between shared/http and exception/domain/value_objects",
	)

	assert.Equal(t,
		string(IdempotencyStatusFailed),
		string(vo.IdempotencyStatusFailed),
		"IdempotencyStatusFailed must match between shared/http and exception/domain/value_objects",
	)
}
