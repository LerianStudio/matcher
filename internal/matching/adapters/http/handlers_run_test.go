//go:build unit

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	sharedhttp "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	"github.com/LerianStudio/matcher/internal/auth"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	"github.com/LerianStudio/matcher/internal/matching/services/command"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestRunMatchHandler_InvalidPayload(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	uc := newRunMatchUseCase(t, ctxProv, []*shared.Transaction{}, nil)

	handler, err := NewHandler(uc, newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}), ctxProv)
	require.NoError(t, err)

	app.Post("/v1/matching/contexts/:contextId/run", handler.RunMatch)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/contexts/"+contextID.String()+"/run",
		bytes.NewBufferString(`{invalid json`),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "invalid run match payload", errResp.Message)
}

func TestRunMatchHandler_EmptyMode(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	uc := newRunMatchUseCase(t, ctxProv, []*shared.Transaction{}, nil)

	handler, err := NewHandler(uc, newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}), ctxProv)
	require.NoError(t, err)

	app.Post("/v1/matching/contexts/:contextId/run", handler.RunMatch)

	payload := RunMatchRequest{Mode: ""}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/contexts/"+contextID.String()+"/run",
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestRunMatchHandler_InvalidMode(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	uc := newRunMatchUseCase(t, ctxProv, []*shared.Transaction{}, nil)

	handler, err := NewHandler(uc, newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}), ctxProv)
	require.NoError(t, err)

	app.Post("/v1/matching/contexts/:contextId/run", handler.RunMatch)

	payload := RunMatchRequest{Mode: "INVALID_MODE"}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/contexts/"+contextID.String()+"/run",
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	// Validation catches invalid mode at libHTTP.ParseBodyAndValidate level (oneof=DRY_RUN COMMIT)
	require.Equal(t, "invalid run match payload", errResp.Message)
}

func TestRunMatchHandler_ContextNotFound(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{info: nil}
	handler, err := NewHandler(
		&command.UseCase{},
		newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
		ctxProv,
	)
	require.NoError(t, err)

	app.Post("/v1/matching/contexts/:contextId/run", handler.RunMatch)

	payload := RunMatchRequest{Mode: "DRY_RUN"}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/contexts/"+contextID.String()+"/run",
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
}

func TestRunMatchHandler_ContextNotActive(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: false},
	}
	handler, err := NewHandler(
		&command.UseCase{},
		newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
		ctxProv,
	)
	require.NoError(t, err)

	app.Post("/v1/matching/contexts/:contextId/run", handler.RunMatch)

	payload := RunMatchRequest{Mode: "DRY_RUN"}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/contexts/"+contextID.String()+"/run",
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusForbidden, resp.StatusCode)
	require.Equal(t, "context_not_active", errResp.Title)
}

func TestGetMatchRunHandler_InvalidRunID(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handler, err := NewHandler(
		&command.UseCase{},
		newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
		ctxProv,
	)
	require.NoError(t, err)

	app.Get("/v1/matching/runs/:runId", handler.GetMatchRun)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/runs/invalid-uuid?contextId="+contextID.String(),
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "invalid run id", errResp.Message)
}

func TestGetMatchRunHandler_MissingContextID(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	runID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{info: nil}
	handler, err := NewHandler(
		&command.UseCase{},
		newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
		ctxProv,
	)
	require.NoError(t, err)

	app.Get("/v1/matching/runs/:runId", handler.GetMatchRun)

	request := httptest.NewRequest(http.MethodGet, "/v1/matching/runs/"+runID.String(), nil)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestListMatchRunsHandler_InvalidSortOrder(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handler, err := NewHandler(
		&command.UseCase{},
		newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
		ctxProv,
	)
	require.NoError(t, err)

	app.Get("/v1/matching/contexts/:contextId/runs", handler.ListMatchRuns)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/contexts/"+contextID.String()+"/runs?sort_order=invalid",
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Contains(t, errResp.Message, "invalid sort_order")
}

func TestListMatchRunsHandler_LimitBounds(t *testing.T) {
	t.Parallel()

	// In lib-uncommons v2, ParseOpaqueCursorPagination silently clamps
	// limit <= 0 to DefaultLimit instead of returning an error.
	testCases := []struct {
		name          string
		limitParam    string
		expectedLimit int
	}{
		{"below minimum clamps to default", "0", constants.DefaultPaginationLimit},
		{"negative clamps to default", "-10", constants.DefaultPaginationLimit},
		{"above maximum capped", "500", constants.MaximumPaginationLimit},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tenantID := uuid.New()
			contextID := uuid.New()
			ctx := libCommons.ContextWithTracer(
				context.Background(),
				noop.NewTracerProvider().Tracer("test"),
			)
			ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
			app := newFiberTestApp(ctx)

			ctxProv := &stubContextProvider{
				info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
			}
			handler, err := NewHandler(
				&command.UseCase{},
				newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
				ctxProv,
			)
			require.NoError(t, err)

			app.Get("/v1/matching/contexts/:contextId/runs", handler.ListMatchRuns)

			request := httptest.NewRequest(
				http.MethodGet,
				"/v1/matching/contexts/"+contextID.String()+"/runs?limit="+tc.limitParam,
				nil,
			)

			resp, err := app.Test(request)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, fiber.StatusOK, resp.StatusCode)

			var payload ListMatchRunsResponse
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
			require.Equal(t, tc.expectedLimit, payload.Limit)
		})
	}
}

func TestGetMatchRunResultsHandler_InvalidRunID(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handler, err := NewHandler(
		&command.UseCase{},
		newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
		ctxProv,
	)
	require.NoError(t, err)

	app.Get("/v1/matching/runs/:runId/groups", handler.GetMatchRunResults)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/runs/invalid-uuid/groups?contextId="+contextID.String(),
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "invalid run id", errResp.Message)
}

func TestGetMatchRunResultsHandler_InvalidSortOrder(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	runID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handler, err := NewHandler(
		&command.UseCase{},
		newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
		ctxProv,
	)
	require.NoError(t, err)

	app.Get("/v1/matching/runs/:runId/groups", handler.GetMatchRunResults)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/runs/"+runID.String()+"/groups?contextId="+contextID.String()+"&sort_order=invalid",
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Contains(t, errResp.Message, "invalid sort_order")
}

func TestGetMatchRunResultsHandler_InvalidSortBy(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	runID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handler, err := NewHandler(
		&command.UseCase{},
		newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
		ctxProv,
	)
	require.NoError(t, err)

	app.Get("/v1/matching/runs/:runId/groups", handler.GetMatchRunResults)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/runs/"+runID.String()+"/groups?contextId="+contextID.String()+"&sort_by=invalid_field",
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Contains(t, errResp.Message, "invalid sort_by")
}

func TestGetMatchRunHandler_Success(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	runID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	run := &matchingEntities.MatchRun{
		ID:        runID,
		ContextID: contextID,
		Status:    matchingVO.MatchRunStatusCompleted,
	}
	runRepo := &stubMatchRunRepo{run: run}

	handler, err := NewHandler(&command.UseCase{}, newQueryUseCase(t, runRepo, &stubMatchGroupRepo{}), ctxProv)
	require.NoError(t, err)

	app.Get("/v1/matching/runs/:runId", handler.GetMatchRun)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/runs/"+runID.String()+"?contextId="+contextID.String(),
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestGetMatchRunHandler_NotFound(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	runID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	runRepo := &stubMatchRunRepo{run: nil}

	handler, err := NewHandler(&command.UseCase{}, newQueryUseCase(t, runRepo, &stubMatchGroupRepo{}), ctxProv)
	require.NoError(t, err)

	app.Get("/v1/matching/runs/:runId", handler.GetMatchRun)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/runs/"+runID.String()+"?contextId="+contextID.String(),
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
}

func TestGetMatchRunHandler_ContextNotActive(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	runID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: false},
	}
	handler, err := NewHandler(
		&command.UseCase{},
		newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
		ctxProv,
	)
	require.NoError(t, err)

	app.Get("/v1/matching/runs/:runId", handler.GetMatchRun)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/runs/"+runID.String()+"?contextId="+contextID.String(),
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusForbidden, resp.StatusCode)
	require.Equal(t, "context_not_active", errResp.Title)
}

func TestGetMatchRunHandler_ContextNotFound(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	runID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{info: nil}
	handler, err := NewHandler(
		&command.UseCase{},
		newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
		ctxProv,
	)
	require.NoError(t, err)

	app.Get("/v1/matching/runs/:runId", handler.GetMatchRun)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/runs/"+runID.String()+"?contextId="+contextID.String(),
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
}

func TestGetMatchRunHandler_RepoError(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	runID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	runRepo := &stubMatchRunRepo{run: nil, err: errTestDatabaseError}

	handler, err := NewHandler(&command.UseCase{}, newQueryUseCase(t, runRepo, &stubMatchGroupRepo{}), ctxProv)
	require.NoError(t, err)

	app.Get("/v1/matching/runs/:runId", handler.GetMatchRun)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/runs/"+runID.String()+"?contextId="+contextID.String(),
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func TestListMatchRunsHandler_Success(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	run := &matchingEntities.MatchRun{
		ID:        uuid.New(),
		ContextID: contextID,
		Status:    matchingVO.MatchRunStatusCompleted,
	}
	runRepo := &stubMatchRunRepo{run: run}

	handler, err := NewHandler(&command.UseCase{}, newQueryUseCase(t, runRepo, &stubMatchGroupRepo{}), ctxProv)
	require.NoError(t, err)

	app.Get("/v1/matching/contexts/:contextId/runs", handler.ListMatchRuns)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/contexts/"+contextID.String()+"/runs",
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestListMatchRunsHandler_ContextNotFound(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{info: nil}
	handler, err := NewHandler(
		&command.UseCase{},
		newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
		ctxProv,
	)
	require.NoError(t, err)

	app.Get("/v1/matching/contexts/:contextId/runs", handler.ListMatchRuns)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/contexts/"+contextID.String()+"/runs",
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
}

func TestListMatchRunsHandler_RepoError(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	runRepo := &stubMatchRunRepo{err: errTestDatabaseError}

	handler, err := NewHandler(&command.UseCase{}, newQueryUseCase(t, runRepo, &stubMatchGroupRepo{}), ctxProv)
	require.NoError(t, err)

	app.Get("/v1/matching/contexts/:contextId/runs", handler.ListMatchRuns)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/contexts/"+contextID.String()+"/runs",
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func TestGetMatchRunResultsHandler_Success(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	runID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	groupRepo := &stubMatchGroupRepo{groups: []*matchingEntities.MatchGroup{}}

	handler, err := NewHandler(&command.UseCase{}, newQueryUseCase(t, &stubMatchRunRepo{}, groupRepo), ctxProv)
	require.NoError(t, err)

	app.Get("/v1/matching/runs/:runId/groups", handler.GetMatchRunResults)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/runs/"+runID.String()+"/groups?contextId="+contextID.String(),
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestGetMatchRunResultsHandler_RepoError(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	runID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	groupRepo := &stubMatchGroupRepo{err: errTestDatabaseError}

	handler, err := NewHandler(&command.UseCase{}, newQueryUseCase(t, &stubMatchRunRepo{}, groupRepo), ctxProv)
	require.NoError(t, err)

	app.Get("/v1/matching/runs/:runId/groups", handler.GetMatchRunResults)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/runs/"+runID.String()+"/groups?contextId="+contextID.String(),
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func TestGetMatchRunResultsHandler_LimitBounds(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		limitParam     string
		expectedStatus int
		expectedLimit  int
	}{
		{"below minimum clamps to default", "0", fiber.StatusOK, constants.DefaultPaginationLimit},
		{"negative clamps to default", "-10", fiber.StatusOK, constants.DefaultPaginationLimit},
		{"above maximum capped", "500", fiber.StatusOK, constants.MaximumPaginationLimit},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tenantID := uuid.New()
			contextID := uuid.New()
			runID := uuid.New()
			ctx := libCommons.ContextWithTracer(
				context.Background(),
				noop.NewTracerProvider().Tracer("test"),
			)
			ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
			app := newFiberTestApp(ctx)

			ctxProv := &stubContextProvider{
				info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
			}
			groupRepo := &stubMatchGroupRepo{groups: []*matchingEntities.MatchGroup{}}

			handler, err := NewHandler(&command.UseCase{}, newQueryUseCase(t, &stubMatchRunRepo{}, groupRepo), ctxProv)
			require.NoError(t, err)

			app.Get("/v1/matching/runs/:runId/groups", handler.GetMatchRunResults)

			request := httptest.NewRequest(
				http.MethodGet,
				"/v1/matching/runs/"+runID.String()+"/groups?contextId="+contextID.String()+"&limit="+tc.limitParam,
				nil,
			)

			resp, err := app.Test(request)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, tc.expectedStatus, resp.StatusCode)

			var payload ListMatchGroupsResponse
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
			require.Equal(t, tc.expectedLimit, payload.Limit)
		})
	}
}

func TestIsRunMatchBadRequestError(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		err      error
		expected bool
	}{
		{"no sources configured", command.ErrNoSourcesConfigured, true},
		{"at least two sources required", command.ErrAtLeastTwoSourcesRequired, true},
		{"primary source not in context", command.ErrPrimarySourceNotInContext, true},
		{"match run mode required", command.ErrMatchRunModeRequired, true},
		{"context not found - not bad request", command.ErrContextNotFound, false},
		{"generic error - not bad request", errTestDatabaseError, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := isRunMatchBadRequestError(tc.err)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestHandleRunMatchError(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		err            error
		expectedStatus int
	}{
		{"context not found", command.ErrContextNotFound, fiber.StatusNotFound},
		{"context not active", command.ErrContextNotActive, fiber.StatusForbidden},
		{"match run locked", command.ErrMatchRunLocked, fiber.StatusConflict},
		{"no sources configured", command.ErrNoSourcesConfigured, fiber.StatusBadRequest},
		{
			"at least two sources required",
			command.ErrAtLeastTwoSourcesRequired,
			fiber.StatusBadRequest,
		},
		{
			"primary source not in context",
			command.ErrPrimarySourceNotInContext,
			fiber.StatusBadRequest,
		},
		{"match run mode required", command.ErrMatchRunModeRequired, fiber.StatusBadRequest},
		{"generic error", errTestDatabaseError, fiber.StatusInternalServerError},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			app := fiber.New()
			app.Get("/test", func(c *fiber.Ctx) error {
				ctx := c.UserContext()
				logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
				if tracer == nil {
					tracer = noop.NewTracerProvider().Tracer("test")
				}
				_, span := tracer.Start(ctx, "test")
				defer span.End()
				return handleRunMatchError(ctx, c, span, logger, tc.err)
			})

			ctx := libCommons.ContextWithTracer(
				context.Background(),
				noop.NewTracerProvider().Tracer("test"),
			)
			app.Use(func(c *fiber.Ctx) error {
				c.SetUserContext(ctx)
				return c.Next()
			})

			request := httptest.NewRequest(http.MethodGet, "/test", nil)
			resp, err := app.Test(request)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, tc.expectedStatus, resp.StatusCode)
		})
	}
}

func TestListMatchRunsHandler_SortOrderAsc(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handler, err := NewHandler(
		&command.UseCase{},
		newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
		ctxProv,
	)
	require.NoError(t, err)

	app.Get("/v1/matching/contexts/:contextId/runs", handler.ListMatchRuns)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/contexts/"+contextID.String()+"/runs?sort_order=asc",
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestListMatchRunsHandler_WithCursor(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handler, err := NewHandler(
		&command.UseCase{},
		newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
		ctxProv,
	)
	require.NoError(t, err)

	app.Get("/v1/matching/contexts/:contextId/runs", handler.ListMatchRuns)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/contexts/"+contextID.String()+"/runs?cursor=somecursor",
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestGetMatchRunResultsHandler_ValidSortBy(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		sortBy string
	}{
		{"sort by id", "id"},
		{"sort by created_at", "created_at"},
		{"sort by status", "status"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tenantID := uuid.New()
			contextID := uuid.New()
			runID := uuid.New()
			ctx := libCommons.ContextWithTracer(
				context.Background(),
				noop.NewTracerProvider().Tracer("test"),
			)
			ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
			app := newFiberTestApp(ctx)

			ctxProv := &stubContextProvider{
				info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
			}
			groupRepo := &stubMatchGroupRepo{groups: []*matchingEntities.MatchGroup{}}

			handler, err := NewHandler(&command.UseCase{}, newQueryUseCase(t, &stubMatchRunRepo{}, groupRepo), ctxProv)
			require.NoError(t, err)

			app.Get("/v1/matching/runs/:runId/groups", handler.GetMatchRunResults)

			request := httptest.NewRequest(
				http.MethodGet,
				"/v1/matching/runs/"+runID.String()+"/groups?contextId="+contextID.String()+"&sort_by="+tc.sortBy,
				nil,
			)

			resp, err := app.Test(request)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, fiber.StatusOK, resp.StatusCode)
		})
	}
}

func TestGetMatchRunResultsHandler_ContextNotFound(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	runID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{info: nil}
	handler, err := NewHandler(
		&command.UseCase{},
		newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
		ctxProv,
	)
	require.NoError(t, err)

	app.Get("/v1/matching/runs/:runId/groups", handler.GetMatchRunResults)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/runs/"+runID.String()+"/groups?contextId="+contextID.String(),
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
}

func TestGetMatchRunResultsHandler_ContextNotActive(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	runID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: false},
	}
	handler, err := NewHandler(
		&command.UseCase{},
		newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
		ctxProv,
	)
	require.NoError(t, err)

	app.Get("/v1/matching/runs/:runId/groups", handler.GetMatchRunResults)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/matching/runs/"+runID.String()+"/groups?contextId="+contextID.String(),
		nil,
	)

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusForbidden, resp.StatusCode)
	require.Equal(t, "context_not_active", errResp.Title)
}

func TestRunMatchHandler_NoPrimarySourceID_SymmetricMode(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{
			ID:     contextID,
			Active: true,
			Type:   shared.ContextTypeOneToOne,
		},
	}
	uc := newRunMatchUseCase(t, ctxProv, []*shared.Transaction{}, nil)

	handler, err := NewHandler(uc, newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}), ctxProv)
	require.NoError(t, err)

	app.Post("/v1/matching/contexts/:contextId/run", handler.RunMatch)

	// No primarySourceId field — symmetric mode
	payload := RunMatchRequest{Mode: "DRY_RUN"}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/contexts/"+contextID.String()+"/run",
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Symmetric mode (no primarySourceId) passes parsing and the UseCase completes
	// successfully with empty candidates, returning 202 Accepted.
	require.Equal(t, fiber.StatusAccepted, resp.StatusCode)
}

func TestRunMatchHandler_InvalidPrimarySourceID(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	uc := newRunMatchUseCase(t, ctxProv, []*shared.Transaction{}, nil)

	handler, err := NewHandler(uc, newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}), ctxProv)
	require.NoError(t, err)

	app.Post("/v1/matching/contexts/:contextId/run", handler.RunMatch)

	// Invalid UUID for primarySourceId
	payloadJSON := `{"mode":"DRY_RUN","primarySourceId":"not-a-valid-uuid"}`

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/contexts/"+contextID.String()+"/run",
		bytes.NewBufferString(payloadJSON),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "invalid primarySourceId", errResp.Message)
}

func TestRunMatchHandler_ValidPrimarySourceID(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	primarySourceID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{
			ID:     contextID,
			Active: true,
			Type:   shared.ContextTypeOneToOne,
		},
	}
	uc := newRunMatchUseCase(t, ctxProv, []*shared.Transaction{}, nil)

	handler, err := NewHandler(uc, newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}), ctxProv)
	require.NoError(t, err)

	app.Post("/v1/matching/contexts/:contextId/run", handler.RunMatch)

	payloadJSON := `{"mode":"DRY_RUN","primarySourceId":"` + primarySourceID.String() + `"}`

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/contexts/"+contextID.String()+"/run",
		bytes.NewBufferString(payloadJSON),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Valid UUID for primarySourceId passes parsing. The UseCase proceeds but
	// the primary source is not among the stub's configured sources, so
	// classifySources returns ErrPrimarySourceNotInContext -> 400.
	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}
