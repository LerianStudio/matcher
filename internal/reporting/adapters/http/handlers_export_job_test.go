//go:build unit

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	repomocks "github.com/LerianStudio/matcher/internal/reporting/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/reporting/services/command"
	"github.com/LerianStudio/matcher/internal/reporting/services/query"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	storageMocks "github.com/LerianStudio/matcher/internal/shared/objectstorage/mocks"
	"github.com/LerianStudio/matcher/pkg/constant"
)

// testTenantID is the tenant ID used in test middleware (setupExportJobTestApp).
var testTenantID = uuid.MustParse("11111111-1111-1111-1111-111111111111")

type storageClientMockConfig struct {
	uploadKey    string
	uploadErr    error
	downloadBody io.ReadCloser
	downloadErr  error
	deleteErr    error
	presignURL   string
	presignErr   error
	exists       bool
	existsErr    error
}

func newExportJobRepoMock(t *testing.T) *repomocks.MockExportJobRepository {
	t.Helper()

	ctrl := gomock.NewController(t)
	mock := repomocks.NewMockExportJobRepository(ctrl)

	return mock
}

func newStorageClientMock(
	t *testing.T,
	cfg storageClientMockConfig,
) *storageMocks.MockBackend {
	t.Helper()

	ctrl := gomock.NewController(t)
	mock := storageMocks.NewMockBackend(ctrl)

	mock.EXPECT().
		Upload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(cfg.uploadKey, cfg.uploadErr).
		AnyTimes()
	mock.EXPECT().
		Download(gomock.Any(), gomock.Any()).
		Return(cfg.downloadBody, cfg.downloadErr).
		AnyTimes()
	mock.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(cfg.deleteErr).AnyTimes()
	mock.EXPECT().
		GeneratePresignedURL(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(cfg.presignURL, cfg.presignErr).
		AnyTimes()
	mock.EXPECT().Exists(gomock.Any(), gomock.Any()).Return(cfg.exists, cfg.existsErr).AnyTimes()

	return mock
}

func setupExportJobTestApp(handler fiber.Handler, route string) *fiber.App {
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(
			c.UserContext(),
			auth.TenantIDKey,
			"11111111-1111-1111-1111-111111111111",
		)
		c.SetUserContext(ctx)

		return c.Next()
	})

	switch route {
	case "create":
		app.Post("/v1/contexts/:contextId/export-jobs", handler)
	case "get":
		app.Get("/v1/export-jobs/:jobId", handler)
	case "list":
		app.Get("/v1/export-jobs", handler)
	case "cancel":
		app.Post("/v1/export-jobs/:jobId/cancel", handler)
	case "download":
		app.Get("/v1/export-jobs/:jobId/download", handler)
	}

	return app
}

func TestNewExportJobHandlers(t *testing.T) {
	t.Parallel()

	repo := newExportJobRepoMock(t)
	storage := newStorageClientMock(t, storageClientMockConfig{})
	ctxProvider := &mockContextProvider{info: &ReconciliationContextInfo{ID: uuid.New()}}

	uc, err := command.NewExportJobUseCase(repo)
	require.NoError(t, err)

	querySvc, err := query.NewExportJobQueryService(repo)
	require.NoError(t, err)

	t.Run("creates handlers with valid dependencies", func(t *testing.T) {
		t.Parallel()

		handlers, err := NewExportJobHandlers(uc, querySvc, storage, ctxProvider, time.Hour, false)

		require.NoError(t, err)
		assert.NotNil(t, handlers)
	})

	t.Run("creates handlers with zero presign expiry uses default", func(t *testing.T) {
		t.Parallel()

		handlers, err := NewExportJobHandlers(uc, querySvc, storage, ctxProvider, 0, false)

		require.NoError(t, err)
		assert.NotNil(t, handlers)
	})

	t.Run("returns error with nil use case", func(t *testing.T) {
		t.Parallel()

		handlers, err := NewExportJobHandlers(nil, querySvc, storage, ctxProvider, time.Hour, false)

		require.Error(t, err)
		assert.Nil(t, handlers)
		require.ErrorIs(t, err, ErrNilExportJobUseCase)
	})

	t.Run("returns error with nil query service", func(t *testing.T) {
		t.Parallel()

		handlers, err := NewExportJobHandlers(uc, nil, storage, ctxProvider, time.Hour, false)

		require.Error(t, err)
		assert.Nil(t, handlers)
		require.ErrorIs(t, err, ErrNilExportJobQueryService)
	})

	t.Run("returns error with nil storage", func(t *testing.T) {
		t.Parallel()

		handlers, err := NewExportJobHandlers(uc, querySvc, nil, ctxProvider, time.Hour, false)

		require.Error(t, err)
		assert.Nil(t, handlers)
		require.ErrorIs(t, err, ErrNilStorageClientHandler)
	})

	t.Run("returns error with typed-nil storage", func(t *testing.T) {
		t.Parallel()

		var typedNilStorage *storageMocks.MockBackend

		handlers, err := NewExportJobHandlers(uc, querySvc, typedNilStorage, ctxProvider, time.Hour, false)

		require.Error(t, err)
		assert.Nil(t, handlers)
		require.ErrorIs(t, err, ErrNilStorageClientHandler)
	})

	t.Run("returns error with nil context provider", func(t *testing.T) {
		t.Parallel()

		handlers, err := NewExportJobHandlers(uc, querySvc, storage, nil, time.Hour, false)

		require.Error(t, err)
		assert.Nil(t, handlers)
		require.ErrorIs(t, err, ErrNilContextProvider)
	})
}

func setupExportJobTestAppWithContext(handler fiber.Handler, route string, _ uuid.UUID) *fiber.App {
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(
			c.UserContext(),
			auth.TenantIDKey,
			"11111111-1111-1111-1111-111111111111",
		)
		c.SetUserContext(ctx)

		return c.Next()
	})

	if route == "create" {
		app.Post("/v1/contexts/:contextId/export-jobs", handler)
	}

	return app
}

func setupCreateExportJobHandlers(
	t *testing.T,
	contextID uuid.UUID,
	repo *repomocks.MockExportJobRepository,
) *ExportJobHandlers {
	t.Helper()

	ctxProvider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	storage := newStorageClientMock(t, storageClientMockConfig{})

	uc, err := command.NewExportJobUseCase(repo)
	require.NoError(t, err)

	querySvc, err := query.NewExportJobQueryService(repo)
	require.NoError(t, err)

	handlers, err := NewExportJobHandlers(uc, querySvc, storage, ctxProvider, time.Hour, false)
	require.NoError(t, err)

	return handlers
}

func TestExportJobHandlers_CurrentRuntimeConfig_PreservesDefaultEnabledOnPartialOverride(t *testing.T) {
	t.Parallel()

	handlers := &ExportJobHandlers{enabled: false, presignExpiry: time.Hour}
	handlers.SetRuntimeConfigResolver(func(context.Context) ExportJobRuntimeConfig {
		return ExportJobRuntimeConfig{PresignExpiry: 2 * time.Hour}
	})

	runtimeCfg := handlers.currentRuntimeConfigForContext(context.Background())
	require.NotNil(t, runtimeCfg.Enabled)
	assert.False(t, *runtimeCfg.Enabled)
	assert.Equal(t, 2*time.Hour, runtimeCfg.PresignExpiry)
}

func makeCreateExportJobRequest(
	t *testing.T,
	app *fiber.App,
	contextID uuid.UUID,
	reqBody CreateExportJobRequest,
) *stdhttp.Response {
	t.Helper()

	body, err := json.Marshal(reqBody)
	require.NoError(t, err)

	req := httptest.NewRequest(
		stdhttp.MethodPost,
		"/v1/contexts/"+contextID.String()+"/export-jobs",
		bytes.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	return resp
}

func TestExportJobHandlers_CreateExportJob(t *testing.T) {
	t.Parallel()

	t.Run("creates export job successfully", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		repo := newExportJobRepoMock(t)
		repo.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).Times(1)

		handlers := setupCreateExportJobHandlers(t, contextID, repo)
		app := setupExportJobTestAppWithContext(handlers.CreateExportJob, "create", contextID)

		reqBody := CreateExportJobRequest{
			ReportType: "MATCHED",
			Format:     "CSV",
			DateFrom:   "2024-01-01",
			DateTo:     "2024-01-31",
		}

		resp := makeCreateExportJobRequest(t, app, contextID, reqBody)
		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusAccepted, resp.StatusCode)

		var response CreateExportJobResponse

		err := json.NewDecoder(resp.Body).Decode(&response)
		require.NoError(t, err)

		assert.NotEmpty(t, response.JobID)
		assert.Equal(t, string(entities.ExportJobStatusQueued), response.Status)
	})

	t.Run("returns service unavailable when export worker is disabled", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		repo := newExportJobRepoMock(t)

		handlers := setupCreateExportJobHandlers(t, contextID, repo)
		handlers.SetRuntimeEnabled(false)
		app := setupExportJobTestAppWithContext(handlers.CreateExportJob, "create", contextID)

		reqBody := CreateExportJobRequest{
			ReportType: "MATCHED",
			Format:     "CSV",
			DateFrom:   "2024-01-01",
			DateTo:     "2024-01-31",
		}

		resp := makeCreateExportJobRequest(t, app, contextID, reqBody)
		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusServiceUnavailable, resp.StatusCode)
	})

	t.Run("returns service unavailable when runtime config resolver disables create for request context", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		repo := newExportJobRepoMock(t)

		handlers := setupCreateExportJobHandlers(t, contextID, repo)
		resolverCalled := false
		handlers.SetRuntimeConfigResolver(func(ctx context.Context) ExportJobRuntimeConfig {
			resolverCalled = true
			enabled := true
			if tenantID, ok := auth.LookupTenantID(ctx); ok && tenantID == "11111111-1111-1111-1111-111111111111" {
				enabled = false
			}

			return ExportJobRuntimeConfig{Enabled: &enabled}
		})

		app := setupExportJobTestAppWithContext(handlers.CreateExportJob, "create", contextID)

		reqBody := CreateExportJobRequest{
			ReportType: "MATCHED",
			Format:     "CSV",
			DateFrom:   "2024-01-01",
			DateTo:     "2024-01-31",
		}

		resp := makeCreateExportJobRequest(t, app, contextID, reqBody)
		defer resp.Body.Close()

		assert.True(t, resolverCalled)
		assert.Equal(t, fiber.StatusServiceUnavailable, resp.StatusCode)
	})

	t.Run("validates tenant-scoped context before disabled worker response", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		repo := newExportJobRepoMock(t)

		handlers := setupCreateExportJobHandlers(t, contextID, repo)
		handlers.SetRuntimeEnabled(false)

		app := fiber.New()
		app.Use(func(c *fiber.Ctx) error {
			ctx := context.WithValue(
				c.UserContext(),
				auth.TenantIDKey,
				"11111111-1111-1111-1111-111111111111",
			)
			c.SetUserContext(ctx)

			return c.Next()
		})
		app.Post("/v1/contexts/:contextId/export-jobs", handlers.CreateExportJob)

		reqBody := CreateExportJobRequest{
			ReportType: "MATCHED",
			Format:     "CSV",
			DateFrom:   "2024-01-01",
			DateTo:     "2024-01-31",
		}

		body, err := json.Marshal(reqBody)
		require.NoError(t, err)

		req := httptest.NewRequest(
			stdhttp.MethodPost,
			"/v1/contexts/not-a-uuid/export-jobs",
			bytes.NewReader(body),
		)
		req.Header.Set("Content-Type", "application/json")

		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	})

	t.Run("accepts MATCHES alias and normalizes to MATCHED", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		repo := newExportJobRepoMock(t)
		repo.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, job *entities.ExportJob) error {
			assert.Equal(t, entities.ExportReportTypeMatched, job.ReportType)
			return nil
		}).Times(1)

		handlers := setupCreateExportJobHandlers(t, contextID, repo)
		app := setupExportJobTestAppWithContext(handlers.CreateExportJob, "create", contextID)

		reqBody := CreateExportJobRequest{
			ReportType: "MATCHES",
			Format:     "CSV",
			DateFrom:   "2024-01-01",
			DateTo:     "2024-01-31",
		}

		resp := makeCreateExportJobRequest(t, app, contextID, reqBody)
		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusAccepted, resp.StatusCode)

		var response CreateExportJobResponse

		err := json.NewDecoder(resp.Body).Decode(&response)
		require.NoError(t, err)

		assert.NotEmpty(t, response.JobID)
		assert.Equal(t, string(entities.ExportJobStatusQueued), response.Status)
	})

	t.Run("rejects EXCEPTIONS report type for async export", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		repo := newExportJobRepoMock(t)
		handlers := setupCreateExportJobHandlers(t, contextID, repo)
		app := setupExportJobTestAppWithContext(handlers.CreateExportJob, "create", contextID)

		reqBody := CreateExportJobRequest{
			ReportType: "EXCEPTIONS",
			Format:     "CSV",
			DateFrom:   "2024-01-01",
			DateTo:     "2024-01-31",
		}

		resp := makeCreateExportJobRequest(t, app, contextID, reqBody)
		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	})

	t.Run("rejects SUMMARY report type for async export", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		repo := newExportJobRepoMock(t)
		handlers := setupCreateExportJobHandlers(t, contextID, repo)
		app := setupExportJobTestAppWithContext(handlers.CreateExportJob, "create", contextID)

		reqBody := CreateExportJobRequest{
			ReportType: "SUMMARY",
			Format:     "CSV",
			DateFrom:   "2024-01-01",
			DateTo:     "2024-01-31",
		}

		resp := makeCreateExportJobRequest(t, app, contextID, reqBody)
		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	})

	t.Run("rejects PDF format", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		repo := newExportJobRepoMock(t)
		handlers := setupCreateExportJobHandlers(t, contextID, repo)
		app := setupExportJobTestAppWithContext(handlers.CreateExportJob, "create", contextID)

		reqBody := CreateExportJobRequest{
			ReportType: "MATCHED",
			Format:     "PDF",
			DateFrom:   "2024-01-01",
			DateTo:     "2024-01-31",
		}

		resp := makeCreateExportJobRequest(t, app, contextID, reqBody)
		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	})

	t.Run("rejects invalid date format", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		repo := newExportJobRepoMock(t)
		handlers := setupCreateExportJobHandlers(t, contextID, repo)
		app := setupExportJobTestAppWithContext(handlers.CreateExportJob, "create", contextID)

		reqBody := CreateExportJobRequest{
			ReportType: "MATCHED",
			Format:     "CSV",
			DateFrom:   "invalid",
			DateTo:     "2024-01-31",
		}

		resp := makeCreateExportJobRequest(t, app, contextID, reqBody)
		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	})

	t.Run("rejects dateFrom after dateTo", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		repo := newExportJobRepoMock(t)
		handlers := setupCreateExportJobHandlers(t, contextID, repo)
		app := setupExportJobTestAppWithContext(handlers.CreateExportJob, "create", contextID)

		reqBody := CreateExportJobRequest{
			ReportType: "MATCHED",
			Format:     "CSV",
			DateFrom:   "2024-02-15",
			DateTo:     "2024-01-01",
		}

		resp := makeCreateExportJobRequest(t, app, contextID, reqBody)
		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	})

	t.Run("rejects date range exceeding 365 days", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		repo := newExportJobRepoMock(t)
		handlers := setupCreateExportJobHandlers(t, contextID, repo)
		app := setupExportJobTestAppWithContext(handlers.CreateExportJob, "create", contextID)

		reqBody := CreateExportJobRequest{
			ReportType: "MATCHED",
			Format:     "CSV",
			DateFrom:   "2022-01-01",
			DateTo:     "2024-01-01",
		}

		resp := makeCreateExportJobRequest(t, app, contextID, reqBody)
		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	})

	t.Run("accepts date range within 365 days", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		repo := newExportJobRepoMock(t)
		repo.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).Times(1)

		handlers := setupCreateExportJobHandlers(t, contextID, repo)
		app := setupExportJobTestAppWithContext(handlers.CreateExportJob, "create", contextID)

		reqBody := CreateExportJobRequest{
			ReportType: "MATCHED",
			Format:     "CSV",
			DateFrom:   "2023-01-01",
			DateTo:     "2023-12-31",
		}

		resp := makeCreateExportJobRequest(t, app, contextID, reqBody)
		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusAccepted, resp.StatusCode)
	})
}

func TestExportJobHandlers_GetExportJob(t *testing.T) {
	t.Parallel()

	jobID := uuid.New()
	ctxProvider := &mockContextProvider{info: &ReconciliationContextInfo{ID: uuid.New()}}
	getHandler := func(h *ExportJobHandlers) fiber.Handler { return h.GetExportJob }

	t.Run("returns job successfully", func(t *testing.T) {
		t.Parallel()

		job := &entities.ExportJob{
			ID:         jobID,
			TenantID:   testTenantID,
			ContextID:  uuid.New(),
			ReportType: "MATCHED",
			Format:     "CSV",
			Status:     entities.ExportJobStatusQueued,
			CreatedAt:  time.Now().UTC(),
			ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
			UpdatedAt:  time.Now().UTC(),
		}

		repo := newExportJobRepoMock(t)
		repo.EXPECT().GetByID(gomock.Any(), jobID).Return(job, nil).Times(1)

		app := setupExportJobHandlersForRoute(t, repo, storageClientMockConfig{}, ctxProvider, getHandler, "get")

		req := httptest.NewRequest(
			stdhttp.MethodGet,
			"/v1/export-jobs/"+jobID.String(),
			stdhttp.NoBody,
		)

		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response ExportJobResponse

		err = json.NewDecoder(resp.Body).Decode(&response)
		require.NoError(t, err)

		assert.Equal(t, jobID.String(), response.ID)
		assert.Equal(t, "MATCHED", response.ReportType)
	})

	t.Run("returns 404 for not found", func(t *testing.T) {
		t.Parallel()

		repo := newExportJobRepoMock(t)
		repo.EXPECT().
			GetByID(gomock.Any(), gomock.Any()).
			Return(nil, command.ErrExportJobNotFound).
			Times(1)

		app := setupExportJobHandlersForRoute(t, repo, storageClientMockConfig{}, ctxProvider, getHandler, "get")

		req := httptest.NewRequest(
			stdhttp.MethodGet,
			"/v1/export-jobs/"+uuid.New().String(),
			stdhttp.NoBody,
		)

		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	})

	t.Run("returns 404 for job belonging to different tenant", func(t *testing.T) {
		t.Parallel()

		differentTenantID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
		job := &entities.ExportJob{
			ID:         jobID,
			TenantID:   differentTenantID,
			ContextID:  uuid.New(),
			ReportType: "MATCHED",
			Format:     "CSV",
			Status:     entities.ExportJobStatusQueued,
			CreatedAt:  time.Now().UTC(),
			ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
			UpdatedAt:  time.Now().UTC(),
		}

		repo := newExportJobRepoMock(t)
		repo.EXPECT().GetByID(gomock.Any(), jobID).Return(job, nil).Times(1)

		app := setupExportJobHandlersForRoute(t, repo, storageClientMockConfig{}, ctxProvider, getHandler, "get")

		req := httptest.NewRequest(
			stdhttp.MethodGet,
			"/v1/export-jobs/"+jobID.String(),
			stdhttp.NoBody,
		)

		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	})

	t.Run("returns 400 for invalid job ID", func(t *testing.T) {
		t.Parallel()

		repo := newExportJobRepoMock(t)

		app := setupExportJobHandlersForRoute(t, repo, storageClientMockConfig{}, ctxProvider, getHandler, "get")

		req := httptest.NewRequest(
			stdhttp.MethodGet,
			"/v1/export-jobs/invalid-uuid",
			stdhttp.NoBody,
		)

		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	})
}

func TestExportJobHandlers_ListExportJobs(t *testing.T) {
	t.Parallel()

	ctxProvider := &mockContextProvider{info: &ReconciliationContextInfo{ID: uuid.New()}}

	t.Run("returns list of jobs", func(t *testing.T) {
		t.Parallel()

		jobs := []*entities.ExportJob{
			{
				ID:         uuid.New(),
				TenantID:   uuid.New(),
				ContextID:  uuid.New(),
				ReportType: "MATCHED",
				Format:     "CSV",
				Status:     entities.ExportJobStatusQueued,
				CreatedAt:  time.Now().UTC(),
				ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
				UpdatedAt:  time.Now().UTC(),
			},
		}

		repo := newExportJobRepoMock(t)
		repo.EXPECT().
			List(gomock.Any(), gomock.Nil(), (*libHTTP.TimestampCursor)(nil), constants.DefaultPaginationLimit).
			Return(jobs, libHTTP.CursorPagination{}, nil).
			Times(1)

		storage := newStorageClientMock(t, storageClientMockConfig{})

		uc, err := command.NewExportJobUseCase(repo)
		require.NoError(t, err)

		querySvc, err := query.NewExportJobQueryService(repo)
		require.NoError(t, err)

		handlers, err := NewExportJobHandlers(uc, querySvc, storage, ctxProvider, time.Hour, false)
		require.NoError(t, err)

		app := setupExportJobTestApp(handlers.ListExportJobs, "list")

		req := httptest.NewRequest(stdhttp.MethodGet, "/v1/export-jobs", stdhttp.NoBody)

		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response ExportJobListResponse

		err = json.NewDecoder(resp.Body).Decode(&response)
		require.NoError(t, err)

		assert.Len(t, response.Items, 1)
		assert.Equal(t, constants.DefaultPaginationLimit, response.Limit)
		assert.False(t, response.HasMore)
		assert.Empty(t, response.NextCursor)
	})

	t.Run("filters by status query parameter", func(t *testing.T) {
		t.Parallel()

		jobs := []*entities.ExportJob{
			{
				ID:         uuid.New(),
				TenantID:   uuid.New(),
				ContextID:  uuid.New(),
				ReportType: "MATCHED",
				Format:     "CSV",
				Status:     entities.ExportJobStatusSucceeded,
				CreatedAt:  time.Now().UTC(),
				ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
				UpdatedAt:  time.Now().UTC(),
			},
		}

		statusFilter := string(entities.ExportJobStatusSucceeded)
		repo := newExportJobRepoMock(t)
		repo.EXPECT().
			List(gomock.Any(), gomock.Eq(&statusFilter), (*libHTTP.TimestampCursor)(nil), constants.DefaultPaginationLimit).
			Return(jobs, libHTTP.CursorPagination{}, nil).
			Times(1)

		storage := newStorageClientMock(t, storageClientMockConfig{})

		uc, err := command.NewExportJobUseCase(repo)
		require.NoError(t, err)

		querySvc, err := query.NewExportJobQueryService(repo)
		require.NoError(t, err)

		handlers, err := NewExportJobHandlers(uc, querySvc, storage, ctxProvider, time.Hour, false)
		require.NoError(t, err)

		app := setupExportJobTestApp(handlers.ListExportJobs, "list")

		req := httptest.NewRequest(stdhttp.MethodGet, "/v1/export-jobs?status=SUCCEEDED", stdhttp.NoBody)

		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response ExportJobListResponse

		err = json.NewDecoder(resp.Body).Decode(&response)
		require.NoError(t, err)

		assert.Len(t, response.Items, 1)
		assert.Equal(t, "SUCCEEDED", response.Items[0].Status)
	})

	t.Run("respects limit query parameter", func(t *testing.T) {
		t.Parallel()

		jobs := []*entities.ExportJob{
			{
				ID:         uuid.New(),
				TenantID:   uuid.New(),
				ContextID:  uuid.New(),
				ReportType: "MATCHED",
				Format:     "CSV",
				Status:     entities.ExportJobStatusQueued,
				CreatedAt:  time.Now().UTC(),
				ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
				UpdatedAt:  time.Now().UTC(),
			},
		}

		customLimit := 5
		repo := newExportJobRepoMock(t)
		repo.EXPECT().
			List(gomock.Any(), gomock.Nil(), (*libHTTP.TimestampCursor)(nil), customLimit).
			Return(jobs, libHTTP.CursorPagination{}, nil).
			Times(1)

		storage := newStorageClientMock(t, storageClientMockConfig{})

		uc, err := command.NewExportJobUseCase(repo)
		require.NoError(t, err)

		querySvc, err := query.NewExportJobQueryService(repo)
		require.NoError(t, err)

		handlers, err := NewExportJobHandlers(uc, querySvc, storage, ctxProvider, time.Hour, false)
		require.NoError(t, err)

		app := setupExportJobTestApp(handlers.ListExportJobs, "list")

		req := httptest.NewRequest(stdhttp.MethodGet, "/v1/export-jobs?limit=5", stdhttp.NoBody)

		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response ExportJobListResponse

		err = json.NewDecoder(resp.Body).Decode(&response)
		require.NoError(t, err)

		assert.Equal(t, customLimit, response.Limit)
	})

	t.Run("clamps limit to max value", func(t *testing.T) {
		t.Parallel()

		repo := newExportJobRepoMock(t)
		repo.EXPECT().
			List(gomock.Any(), gomock.Nil(), (*libHTTP.TimestampCursor)(nil), constants.MaximumPaginationLimit).
			Return([]*entities.ExportJob{}, libHTTP.CursorPagination{}, nil).
			Times(1)

		storage := newStorageClientMock(t, storageClientMockConfig{})

		uc, err := command.NewExportJobUseCase(repo)
		require.NoError(t, err)

		querySvc, err := query.NewExportJobQueryService(repo)
		require.NoError(t, err)

		handlers, err := NewExportJobHandlers(uc, querySvc, storage, ctxProvider, time.Hour, false)
		require.NoError(t, err)

		app := setupExportJobTestApp(handlers.ListExportJobs, "list")

		req := httptest.NewRequest(stdhttp.MethodGet, "/v1/export-jobs?limit=9999", stdhttp.NoBody)

		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response ExportJobListResponse

		err = json.NewDecoder(resp.Body).Decode(&response)
		require.NoError(t, err)

		assert.Equal(t, constants.MaximumPaginationLimit, response.Limit)
	})
}

func TestExportJobHandlers_CancelExportJob(t *testing.T) {
	t.Parallel()

	ctxProvider := &mockContextProvider{info: &ReconciliationContextInfo{ID: uuid.New()}}
	cancelHandler := func(h *ExportJobHandlers) fiber.Handler { return h.CancelExportJob }

	t.Run("cancels queued job successfully", func(t *testing.T) {
		t.Parallel()

		jobID := uuid.New()
		job := &entities.ExportJob{
			ID:         jobID,
			TenantID:   testTenantID,
			ContextID:  uuid.New(),
			ReportType: "MATCHED",
			Format:     "CSV",
			Status:     entities.ExportJobStatusQueued,
			CreatedAt:  time.Now().UTC(),
			ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
			UpdatedAt:  time.Now().UTC(),
		}

		repo := newExportJobRepoMock(t)
		repo.EXPECT().GetByID(gomock.Any(), jobID).Return(job, nil).Times(3)
		repo.EXPECT().UpdateStatus(gomock.Any(), gomock.Any()).Return(nil).Times(1)

		app := setupExportJobHandlersForRoute(t, repo, storageClientMockConfig{}, ctxProvider, cancelHandler, "cancel")

		req := httptest.NewRequest(
			stdhttp.MethodPost,
			"/v1/export-jobs/"+jobID.String()+"/cancel",
			stdhttp.NoBody,
		)

		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusOK, resp.StatusCode)
	})

	t.Run("returns conflict for terminal state job", func(t *testing.T) {
		t.Parallel()

		jobID := uuid.New()
		job := &entities.ExportJob{
			ID:         jobID,
			TenantID:   testTenantID,
			ContextID:  uuid.New(),
			ReportType: "MATCHED",
			Format:     "CSV",
			Status:     entities.ExportJobStatusSucceeded,
			CreatedAt:  time.Now().UTC(),
			ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
			UpdatedAt:  time.Now().UTC(),
		}

		repo := newExportJobRepoMock(t)
		repo.EXPECT().GetByID(gomock.Any(), jobID).Return(job, nil).Times(2)

		app := setupExportJobHandlersForRoute(t, repo, storageClientMockConfig{}, ctxProvider, cancelHandler, "cancel")

		req := httptest.NewRequest(
			stdhttp.MethodPost,
			"/v1/export-jobs/"+jobID.String()+"/cancel",
			stdhttp.NoBody,
		)

		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusConflict, resp.StatusCode)
	})

	t.Run("returns bad request for invalid job ID", func(t *testing.T) {
		t.Parallel()

		repo := newExportJobRepoMock(t)

		app := setupExportJobHandlersForRoute(t, repo, storageClientMockConfig{}, ctxProvider, cancelHandler, "cancel")

		req := httptest.NewRequest(
			stdhttp.MethodPost,
			"/v1/export-jobs/invalid-uuid/cancel",
			stdhttp.NoBody,
		)

		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	})

	t.Run("returns not found when job does not exist", func(t *testing.T) {
		t.Parallel()

		jobID := uuid.New()

		repo := newExportJobRepoMock(t)
		repo.EXPECT().GetByID(gomock.Any(), jobID).Return(nil, command.ErrExportJobNotFound).Times(1)

		app := setupExportJobHandlersForRoute(t, repo, storageClientMockConfig{}, ctxProvider, cancelHandler, "cancel")

		req := httptest.NewRequest(
			stdhttp.MethodPost,
			"/v1/export-jobs/"+jobID.String()+"/cancel",
			stdhttp.NoBody,
		)

		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	})

	t.Run("returns internal error on service failure", func(t *testing.T) {
		t.Parallel()

		jobID := uuid.New()
		job := &entities.ExportJob{
			ID:         jobID,
			TenantID:   testTenantID,
			ContextID:  uuid.New(),
			ReportType: "MATCHED",
			Format:     "CSV",
			Status:     entities.ExportJobStatusQueued,
			CreatedAt:  time.Now().UTC(),
			ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
			UpdatedAt:  time.Now().UTC(),
		}

		repo := newExportJobRepoMock(t)
		repo.EXPECT().GetByID(gomock.Any(), jobID).Return(job, nil).Times(2)
		repo.EXPECT().UpdateStatus(gomock.Any(), gomock.Any()).Return(errors.New("database error")).Times(1)

		app := setupExportJobHandlersForRoute(t, repo, storageClientMockConfig{}, ctxProvider, cancelHandler, "cancel")

		req := httptest.NewRequest(
			stdhttp.MethodPost,
			"/v1/export-jobs/"+jobID.String()+"/cancel",
			stdhttp.NoBody,
		)

		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	})

	t.Run("returns not found for job belonging to different tenant", func(t *testing.T) {
		t.Parallel()

		jobID := uuid.New()
		differentTenantID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
		job := &entities.ExportJob{
			ID:         jobID,
			TenantID:   differentTenantID,
			ContextID:  uuid.New(),
			ReportType: "MATCHED",
			Format:     "CSV",
			Status:     entities.ExportJobStatusQueued,
			CreatedAt:  time.Now().UTC(),
			ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
			UpdatedAt:  time.Now().UTC(),
		}

		repo := newExportJobRepoMock(t)
		repo.EXPECT().GetByID(gomock.Any(), jobID).Return(job, nil).Times(1)

		app := setupExportJobHandlersForRoute(t, repo, storageClientMockConfig{}, ctxProvider, cancelHandler, "cancel")

		req := httptest.NewRequest(
			stdhttp.MethodPost,
			"/v1/export-jobs/"+jobID.String()+"/cancel",
			stdhttp.NoBody,
		)

		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	})
}

func setupExportJobHandlersForRoute(
	t *testing.T,
	repo *repomocks.MockExportJobRepository,
	storageCfg storageClientMockConfig,
	ctxProvider *mockContextProvider,
	handler func(*ExportJobHandlers) fiber.Handler,
	route string,
) *fiber.App {
	t.Helper()

	storage := newStorageClientMock(t, storageCfg)

	uc, err := command.NewExportJobUseCase(repo)
	require.NoError(t, err)

	querySvc, err := query.NewExportJobQueryService(repo)
	require.NoError(t, err)

	handlers, err := NewExportJobHandlers(uc, querySvc, storage, ctxProvider, time.Hour, false)
	require.NoError(t, err)

	app := setupExportJobTestApp(handler(handlers), route)

	return app
}

func makeDownloadRequest(t *testing.T, app *fiber.App, jobID uuid.UUID) *stdhttp.Response {
	t.Helper()

	req := httptest.NewRequest(
		stdhttp.MethodGet,
		"/v1/export-jobs/"+jobID.String()+"/download",
		stdhttp.NoBody,
	)

	resp, err := app.Test(req)
	require.NoError(t, err)

	return resp
}

func TestExportJobHandlers_DownloadExportJob(t *testing.T) {
	t.Parallel()

	ctxProvider := &mockContextProvider{info: &ReconciliationContextInfo{ID: uuid.New()}}
	downloadHandler := func(h *ExportJobHandlers) fiber.Handler { return h.DownloadExportJob }

	t.Run("returns presigned URL for succeeded job", func(t *testing.T) {
		t.Parallel()

		jobID := uuid.New()
		job := &entities.ExportJob{
			ID:         jobID,
			TenantID:   testTenantID,
			ContextID:  uuid.New(),
			ReportType: "MATCHED",
			Format:     "CSV",
			Status:     entities.ExportJobStatusSucceeded,
			FileKey:    "exports/test.csv",
			FileName:   "test.csv",
			SHA256:     "abc123",
			CreatedAt:  time.Now().UTC(),
			ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
			UpdatedAt:  time.Now().UTC(),
		}

		repo := newExportJobRepoMock(t)
		repo.EXPECT().GetByID(gomock.Any(), jobID).Return(job, nil).Times(1)

		app := setupExportJobHandlersForRoute(
			t,
			repo,
			storageClientMockConfig{presignURL: "https://storage.example.com/test.csv?presigned"},
			ctxProvider,
			downloadHandler,
			"download",
		)

		resp := makeDownloadRequest(t, app, jobID)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response map[string]any

		err := json.NewDecoder(resp.Body).Decode(&response)
		require.NoError(t, err)

		assert.Equal(t, "https://storage.example.com/test.csv?presigned", response["downloadUrl"])
		assert.Equal(t, "test.csv", response["fileName"])
	})

	t.Run("uses runtime presign expiry when configured", func(t *testing.T) {
		t.Parallel()

		jobID := uuid.New()
		job := &entities.ExportJob{
			ID:         jobID,
			TenantID:   testTenantID,
			ContextID:  uuid.New(),
			ReportType: "MATCHED",
			Format:     "CSV",
			Status:     entities.ExportJobStatusSucceeded,
			FileKey:    "exports/test.csv",
			FileName:   "test.csv",
			SHA256:     "abc123",
			CreatedAt:  time.Now().UTC(),
			ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
			UpdatedAt:  time.Now().UTC(),
		}

		repo := newExportJobRepoMock(t)
		repo.EXPECT().GetByID(gomock.Any(), jobID).Return(job, nil).Times(1)

		ctrl := gomock.NewController(t)
		storage := storageMocks.NewMockBackend(ctrl)
		var gotExpiry time.Duration
		storage.EXPECT().GeneratePresignedURL(gomock.Any(), job.FileKey, gomock.Any()).DoAndReturn(
			func(_ context.Context, _ string, expiry time.Duration) (string, error) {
				gotExpiry = expiry
				return "https://storage.example.com/test.csv?presigned", nil
			},
		)

		uc, err := command.NewExportJobUseCase(repo)
		require.NoError(t, err)
		querySvc, err := query.NewExportJobQueryService(repo)
		require.NoError(t, err)

		handlers, err := NewExportJobHandlers(uc, querySvc, storage, ctxProvider, time.Hour, false)
		require.NoError(t, err)
		handlers.SetRuntimeConfigResolver(func(context.Context) ExportJobRuntimeConfig {
			enabled := true
			return ExportJobRuntimeConfig{Enabled: &enabled, PresignExpiry: 2 * time.Hour}
		})

		app := setupExportJobTestApp(downloadHandler(handlers), "download")
		resp := makeDownloadRequest(t, app, jobID)
		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusOK, resp.StatusCode)
		assert.Equal(t, 2*time.Hour, gotExpiry)

		var response map[string]any
		err = json.NewDecoder(resp.Body).Decode(&response)
		require.NoError(t, err)
		assert.Equal(t, float64((2 * time.Hour).Seconds()), response["expiresIn"])
	})

	t.Run("returns conflict for non-downloadable job", func(t *testing.T) {
		t.Parallel()

		jobID := uuid.New()
		job := &entities.ExportJob{
			ID:         jobID,
			TenantID:   testTenantID,
			ContextID:  uuid.New(),
			ReportType: "MATCHED",
			Format:     "CSV",
			Status:     entities.ExportJobStatusQueued,
			CreatedAt:  time.Now().UTC(),
			ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
			UpdatedAt:  time.Now().UTC(),
		}

		repo := newExportJobRepoMock(t)
		repo.EXPECT().GetByID(gomock.Any(), jobID).Return(job, nil).Times(1)

		app := setupExportJobHandlersForRoute(t, repo, storageClientMockConfig{}, ctxProvider, downloadHandler, "download")

		resp := makeDownloadRequest(t, app, jobID)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusConflict, resp.StatusCode)
	})

	t.Run("returns gone for expired job", func(t *testing.T) {
		t.Parallel()

		jobID := uuid.New()
		job := &entities.ExportJob{
			ID:         jobID,
			TenantID:   testTenantID,
			ContextID:  uuid.New(),
			ReportType: "MATCHED",
			Format:     "CSV",
			Status:     entities.ExportJobStatusSucceeded,
			FileKey:    "exports/test.csv",
			FileName:   "test.csv",
			CreatedAt:  time.Now().UTC().Add(-8 * 24 * time.Hour),
			ExpiresAt:  time.Now().UTC().Add(-1 * time.Hour),
			UpdatedAt:  time.Now().UTC(),
		}

		repo := newExportJobRepoMock(t)
		repo.EXPECT().GetByID(gomock.Any(), jobID).Return(job, nil).Times(1)

		app := setupExportJobHandlersForRoute(t, repo, storageClientMockConfig{}, ctxProvider, downloadHandler, "download")

		resp := makeDownloadRequest(t, app, jobID)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusGone, resp.StatusCode)

		var payload ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
		assert.Equal(t, constant.CodeReportingExportExpired, payload.Code)
		assert.Equal(t, stdhttp.StatusText(stdhttp.StatusGone), payload.Title)
		assert.Equal(t, "export file has expired", payload.Message)
	})

	t.Run("returns bad request for invalid job ID", func(t *testing.T) {
		t.Parallel()

		repo := newExportJobRepoMock(t)

		app := setupExportJobHandlersForRoute(t, repo, storageClientMockConfig{}, ctxProvider, downloadHandler, "download")

		req := httptest.NewRequest(
			stdhttp.MethodGet,
			"/v1/export-jobs/invalid-uuid/download",
			stdhttp.NoBody,
		)

		resp, err := app.Test(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	})

	t.Run("returns not found when job does not exist", func(t *testing.T) {
		t.Parallel()

		jobID := uuid.New()

		repo := newExportJobRepoMock(t)
		repo.EXPECT().GetByID(gomock.Any(), jobID).Return(nil, query.ErrExportJobNotFound).Times(1)

		app := setupExportJobHandlersForRoute(t, repo, storageClientMockConfig{}, ctxProvider, downloadHandler, "download")

		resp := makeDownloadRequest(t, app, jobID)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	})

	t.Run("returns not found for job belonging to different tenant", func(t *testing.T) {
		t.Parallel()

		jobID := uuid.New()
		differentTenantID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
		job := &entities.ExportJob{
			ID:         jobID,
			TenantID:   differentTenantID,
			ContextID:  uuid.New(),
			ReportType: "MATCHED",
			Format:     "CSV",
			Status:     entities.ExportJobStatusSucceeded,
			FileKey:    "exports/test.csv",
			FileName:   "test.csv",
			SHA256:     "abc123",
			CreatedAt:  time.Now().UTC(),
			ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
			UpdatedAt:  time.Now().UTC(),
		}

		repo := newExportJobRepoMock(t)
		repo.EXPECT().GetByID(gomock.Any(), jobID).Return(job, nil).Times(1)

		app := setupExportJobHandlersForRoute(t, repo, storageClientMockConfig{}, ctxProvider, downloadHandler, "download")

		resp := makeDownloadRequest(t, app, jobID)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	})

	t.Run("returns internal error on storage failure", func(t *testing.T) {
		t.Parallel()

		jobID := uuid.New()
		job := &entities.ExportJob{
			ID:         jobID,
			TenantID:   testTenantID,
			ContextID:  uuid.New(),
			ReportType: "MATCHED",
			Format:     "CSV",
			Status:     entities.ExportJobStatusSucceeded,
			FileKey:    "exports/test.csv",
			FileName:   "test.csv",
			SHA256:     "abc123",
			CreatedAt:  time.Now().UTC(),
			ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
			UpdatedAt:  time.Now().UTC(),
		}

		repo := newExportJobRepoMock(t)
		repo.EXPECT().GetByID(gomock.Any(), jobID).Return(job, nil).Times(1)

		app := setupExportJobHandlersForRoute(
			t,
			repo,
			storageClientMockConfig{presignErr: errors.New("storage error")},
			ctxProvider,
			downloadHandler,
			"download",
		)

		resp := makeDownloadRequest(t, app, jobID)

		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	})
}

func TestRegisterExportJobRoutes(t *testing.T) {
	t.Parallel()

	t.Run("registers routes successfully", func(t *testing.T) {
		t.Parallel()

		repo := newExportJobRepoMock(t)
		storage := newStorageClientMock(t, storageClientMockConfig{})
		ctxProvider := &mockContextProvider{info: &ReconciliationContextInfo{ID: uuid.New()}}

		uc, err := command.NewExportJobUseCase(repo)
		require.NoError(t, err)

		querySvc, err := query.NewExportJobQueryService(repo)
		require.NoError(t, err)

		handlers, err := NewExportJobHandlers(uc, querySvc, storage, ctxProvider, time.Hour, false)
		require.NoError(t, err)

		app := fiber.New()
		protected := func(resource string, actions ...string) fiber.Router {
			return app.Group("/api")
		}
		limiter := func(c *fiber.Ctx) error {
			return c.Next()
		}

		err = RegisterExportJobRoutes(protected, handlers, limiter)
		require.NoError(t, err)
	})

	t.Run("returns error for nil protected", func(t *testing.T) {
		t.Parallel()

		repo := newExportJobRepoMock(t)
		storage := newStorageClientMock(t, storageClientMockConfig{})
		ctxProvider := &mockContextProvider{info: &ReconciliationContextInfo{ID: uuid.New()}}

		uc, err := command.NewExportJobUseCase(repo)
		require.NoError(t, err)

		querySvc, err := query.NewExportJobQueryService(repo)
		require.NoError(t, err)

		handlers, err := NewExportJobHandlers(uc, querySvc, storage, ctxProvider, time.Hour, false)
		require.NoError(t, err)

		limiter := func(c *fiber.Ctx) error {
			return c.Next()
		}

		err = RegisterExportJobRoutes(nil, handlers, limiter)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrProtectedRouteHelperRequired)
	})

	t.Run("returns error for nil handlers", func(t *testing.T) {
		t.Parallel()

		app := fiber.New()
		protected := func(resource string, actions ...string) fiber.Router {
			return app.Group("/api")
		}
		limiter := func(c *fiber.Ctx) error {
			return c.Next()
		}

		err := RegisterExportJobRoutes(protected, nil, limiter)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrExportJobHandlersRequired)
	})

	t.Run("returns error for nil limiter", func(t *testing.T) {
		t.Parallel()

		repo := newExportJobRepoMock(t)
		storage := newStorageClientMock(t, storageClientMockConfig{})
		ctxProvider := &mockContextProvider{info: &ReconciliationContextInfo{ID: uuid.New()}}

		uc, err := command.NewExportJobUseCase(repo)
		require.NoError(t, err)

		querySvc, err := query.NewExportJobQueryService(repo)
		require.NoError(t, err)

		handlers, err := NewExportJobHandlers(uc, querySvc, storage, ctxProvider, time.Hour, false)
		require.NoError(t, err)

		app := fiber.New()
		protected := func(resource string, actions ...string) fiber.Router {
			return app.Group("/api")
		}

		err = RegisterExportJobRoutes(protected, handlers, nil)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrExportLimiterRequired)
	})
}
