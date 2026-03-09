//go:build unit

package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/reporting/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/services/query"
	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

var (
	errTestDBUnavailable = errors.New("test db unavailable")
	errTestStorageError  = errors.New("test storage error")
)

type mockContextProvider struct {
	info *ReconciliationContextInfo
	err  error
}

func (m *mockContextProvider) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*ReconciliationContextInfo, error) {
	return m.info, m.err
}

type mockDashboardRepository struct {
	volumeStats      *entities.VolumeStats
	volumeErr        error
	slaStats         *entities.SLAStats
	slaErr           error
	summaryMetrics   *entities.SummaryMetrics
	summaryErr       error
	trendMetrics     *entities.TrendMetrics
	trendErr         error
	breakdownMetrics *entities.BreakdownMetrics
	breakdownErr     error
}

func (m *mockDashboardRepository) GetVolumeStats(
	_ context.Context,
	_ entities.DashboardFilter,
) (*entities.VolumeStats, error) {
	return m.volumeStats, m.volumeErr
}

func (m *mockDashboardRepository) GetSLAStats(
	_ context.Context,
	_ entities.DashboardFilter,
) (*entities.SLAStats, error) {
	return m.slaStats, m.slaErr
}

func (m *mockDashboardRepository) GetSummaryMetrics(
	_ context.Context,
	_ entities.DashboardFilter,
) (*entities.SummaryMetrics, error) {
	return m.summaryMetrics, m.summaryErr
}

func (m *mockDashboardRepository) GetTrendMetrics(
	_ context.Context,
	_ entities.DashboardFilter,
) (*entities.TrendMetrics, error) {
	return m.trendMetrics, m.trendErr
}

func (m *mockDashboardRepository) GetBreakdownMetrics(
	_ context.Context,
	_ entities.DashboardFilter,
) (*entities.BreakdownMetrics, error) {
	return m.breakdownMetrics, m.breakdownErr
}

func (m *mockDashboardRepository) GetSourceBreakdown(
	_ context.Context,
	_ entities.DashboardFilter,
) ([]entities.SourceBreakdown, error) {
	return nil, nil
}

func (m *mockDashboardRepository) GetCashImpactSummary(
	_ context.Context,
	_ entities.DashboardFilter,
) (*entities.CashImpactSummary, error) {
	return nil, nil
}

func newTestExportUseCase(tb testing.TB) *query.UseCase {
	tb.Helper()

	uc, err := query.NewUseCase(&mockReportRepository{})
	if err != nil {
		tb.Fatalf("failed to create UseCase: %v", err)
	}

	return uc
}

type mockReportRepository struct{}

func (m *mockReportRepository) ListMatched(
	_ context.Context,
	_ entities.ReportFilter,
) ([]*entities.MatchedItem, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (m *mockReportRepository) ListUnmatched(
	_ context.Context,
	_ entities.ReportFilter,
) ([]*entities.UnmatchedItem, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (m *mockReportRepository) GetSummary(
	_ context.Context,
	_ entities.ReportFilter,
) (*entities.SummaryReport, error) {
	return nil, nil
}

func (m *mockReportRepository) GetVarianceReport(
	_ context.Context,
	_ entities.VarianceReportFilter,
) ([]*entities.VarianceReportRow, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (m *mockReportRepository) ListMatchedForExport(
	_ context.Context,
	_ entities.ReportFilter,
	_ int,
) ([]*entities.MatchedItem, error) {
	return nil, nil
}

func (m *mockReportRepository) ListUnmatchedForExport(
	_ context.Context,
	_ entities.ReportFilter,
	_ int,
) ([]*entities.UnmatchedItem, error) {
	return nil, nil
}

func (m *mockReportRepository) ListVarianceForExport(
	_ context.Context,
	_ entities.VarianceReportFilter,
	_ int,
) ([]*entities.VarianceReportRow, error) {
	return nil, nil
}

func (m *mockReportRepository) ListMatchedPage(
	_ context.Context,
	_ entities.ReportFilter,
	_ string,
	_ int,
) ([]*entities.MatchedItem, string, error) {
	return nil, "", nil
}

func (m *mockReportRepository) ListUnmatchedPage(
	_ context.Context,
	_ entities.ReportFilter,
	_ string,
	_ int,
) ([]*entities.UnmatchedItem, string, error) {
	return nil, "", nil
}

func (m *mockReportRepository) ListVariancePage(
	_ context.Context,
	_ entities.VarianceReportFilter,
	_ string,
	_ int,
) ([]*entities.VarianceReportRow, string, error) {
	return nil, "", nil
}

func (m *mockReportRepository) CountMatched(
	_ context.Context,
	_ entities.ReportFilter,
) (int64, error) {
	return 0, nil
}

func (m *mockReportRepository) CountUnmatched(
	_ context.Context,
	_ entities.ReportFilter,
) (int64, error) {
	return 0, nil
}

func (m *mockReportRepository) CountTransactions(
	_ context.Context,
	_ entities.ReportFilter,
) (int64, error) {
	return 0, nil
}

func (m *mockReportRepository) CountExceptions(
	_ context.Context,
	_ entities.ReportFilter,
) (int64, error) {
	return 0, nil
}

func setupTestApp(handler fiber.Handler) *fiber.App {
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.TenantIDKey, uuid.New().String())
		c.SetUserContext(ctx)

		return c.Next()
	})
	app.Get("/v1/reports/contexts/:contextId/dashboard", handler)
	app.Get("/v1/reports/contexts/:contextId/dashboard/volume", handler)
	app.Get("/v1/reports/contexts/:contextId/dashboard/match-rate", handler)
	app.Get("/v1/reports/contexts/:contextId/dashboard/sla", handler)

	return app
}

func requireErrorMessage(t *testing.T, response *http.Response, expected string) {
	t.Helper()

	var payload map[string]any
	require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
	require.Equal(t, expected, payload["message"])
}

func requireNotFoundResponse(t *testing.T, response *http.Response, expectedMessage string) {
	t.Helper()

	var payload map[string]any
	require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
	require.Equal(t, float64(404), payload["code"])
	require.Equal(t, "not_found", payload["title"])
	require.Equal(t, expectedMessage, payload["message"])
}

func setupStatsHandlers(
	t *testing.T,
	repo *mockDashboardRepository,
	provider *mockContextProvider,
) *Handlers {
	t.Helper()

	uc, ucErr := query.NewDashboardUseCase(repo, nil)
	require.NoError(t, ucErr)

	handlers, err := NewHandlers(uc, provider, newTestExportUseCase(t), false)
	require.NoError(t, err)

	return handlers
}

func testBadRequestError(t *testing.T, handler fiber.Handler, url, expectedError string) {
	t.Helper()

	app := setupTestApp(handler)

	resp := makeStatsRequest(t, app, url)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	requireErrorMessage(t, resp, expectedError)
}

func test500Error(t *testing.T, handler fiber.Handler, url string) {
	t.Helper()

	app := setupTestApp(handler)

	resp := makeStatsRequest(t, app, url)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	requireErrorMessage(t, resp, "an unexpected error occurred")
}

func makeStatsRequest(t *testing.T, app *fiber.App, url string) *http.Response {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, url, http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	return resp
}

func TestNewHandlers(t *testing.T) {
	t.Parallel()

	t.Run("returns error when dashboard use case is nil", func(t *testing.T) {
		t.Parallel()

		h, err := NewHandlers(nil, &mockContextProvider{}, newTestExportUseCase(t), false)

		assert.Nil(t, h)
		assert.Equal(t, ErrNilDashboardUseCase, err)
	})

	t.Run("returns error when context provider is nil", func(t *testing.T) {
		t.Parallel()

		repo := &mockDashboardRepository{}
		uc, ucErr := query.NewDashboardUseCase(repo, nil)
		require.NoError(t, ucErr)

		h, err := NewHandlers(uc, nil, newTestExportUseCase(t), false)

		assert.Nil(t, h)
		assert.Equal(t, ErrNilContextProvider, err)
	})

	t.Run("returns error when export use case is nil", func(t *testing.T) {
		t.Parallel()

		repo := &mockDashboardRepository{}
		uc, ucErr := query.NewDashboardUseCase(repo, nil)
		require.NoError(t, ucErr)

		provider := &mockContextProvider{
			info: &ReconciliationContextInfo{ID: uuid.New(), Active: true},
		}

		h, err := NewHandlers(uc, provider, nil, false)

		assert.Nil(t, h)
		assert.Equal(t, ErrNilExportUseCase, err)
	})

	t.Run("creates handlers successfully", func(t *testing.T) {
		t.Parallel()

		repo := &mockDashboardRepository{}
		uc, ucErr := query.NewDashboardUseCase(repo, nil)
		require.NoError(t, ucErr)

		provider := &mockContextProvider{
			info: &ReconciliationContextInfo{ID: uuid.New(), Active: true},
		}

		h, err := NewHandlers(uc, provider, newTestExportUseCase(t), false)

		require.NoError(t, err)
		assert.NotNil(t, h)
	})
}

func TestHandlers_GetVolumeStats(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)
	baseURL := "/v1/reports/contexts/" + contextID.String() + "/dashboard/volume"

	t.Run("returns volume stats successfully", func(t *testing.T) {
		t.Parallel()

		repo := &mockDashboardRepository{
			volumeStats: &entities.VolumeStats{
				TotalTransactions:   100,
				MatchedTransactions: 80,
				UnmatchedCount:      20,
				TotalAmount:         decimal.NewFromInt(10000),
				MatchedAmount:       decimal.NewFromInt(8000),
				UnmatchedAmount:     decimal.NewFromInt(2000),
				PeriodStart:         time.Now().UTC().Add(-24 * time.Hour),
				PeriodEnd:           time.Now().UTC(),
			},
		}
		provider := &mockContextProvider{
			info: &ReconciliationContextInfo{ID: contextID, Active: true},
		}
		handlers := setupStatsHandlers(t, repo, provider)
		app := setupTestApp(handlers.GetVolumeStats)

		resp := makeStatsRequest(t, app, baseURL+"?date_from="+dateFrom+"&date_to="+dateTo)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var result dto.VolumeStatsResponse

		err := json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, 100, result.TotalTransactions)
		assert.Equal(t, 80, result.MatchedTransactions)
	})

	t.Run("returns 400 for missing date_from", func(t *testing.T) {
		t.Parallel()

		provider := &mockContextProvider{
			info: &ReconciliationContextInfo{ID: contextID, Active: true},
		}

		handlers := setupStatsHandlers(t, &mockDashboardRepository{}, provider)
		testBadRequestError(
			t,
			handlers.GetVolumeStats,
			baseURL+"?date_to="+dateTo,
			ErrDateFromRequired.Error(),
		)
	})

	t.Run("returns 400 for missing date_to", func(t *testing.T) {
		t.Parallel()

		provider := &mockContextProvider{
			info: &ReconciliationContextInfo{ID: contextID, Active: true},
		}

		handlers := setupStatsHandlers(t, &mockDashboardRepository{}, provider)
		testBadRequestError(
			t,
			handlers.GetVolumeStats,
			baseURL+"?date_from="+dateFrom,
			ErrDateToRequired.Error(),
		)
	})

	t.Run("returns 400 for invalid date format", func(t *testing.T) {
		t.Parallel()

		provider := &mockContextProvider{
			info: &ReconciliationContextInfo{ID: contextID, Active: true},
		}

		handlers := setupStatsHandlers(t, &mockDashboardRepository{}, provider)
		testBadRequestError(
			t,
			handlers.GetVolumeStats,
			baseURL+"?date_from=not-a-date&date_to="+dateTo,
			ErrInvalidDateRange.Error(),
		)
	})

	t.Run("returns 400 for date_from after date_to", func(t *testing.T) {
		t.Parallel()

		provider := &mockContextProvider{
			info: &ReconciliationContextInfo{ID: contextID, Active: true},
		}

		handlers := setupStatsHandlers(t, &mockDashboardRepository{}, provider)

		futureDate := time.Now().UTC().Add(24 * time.Hour).Format(time.DateOnly)
		testBadRequestError(
			t,
			handlers.GetVolumeStats,
			baseURL+"?date_from="+futureDate+"&date_to="+dateTo,
			ErrInvalidDateRange.Error(),
		)
	})

	t.Run("returns 400 for invalid source_id", func(t *testing.T) {
		t.Parallel()

		provider := &mockContextProvider{
			info: &ReconciliationContextInfo{ID: contextID, Active: true},
		}

		handlers := setupStatsHandlers(t, &mockDashboardRepository{}, provider)
		testBadRequestError(
			t,
			handlers.GetVolumeStats,
			baseURL+"?date_from="+dateFrom+"&date_to="+dateTo+"&source_id=bad",
			ErrInvalidSourceID.Error(),
		)
	})

	t.Run("returns 500 when context lookup fails", func(t *testing.T) {
		t.Parallel()

		provider := &mockContextProvider{err: errTestDBUnavailable}

		handlers := setupStatsHandlers(t, &mockDashboardRepository{}, provider)
		test500Error(t, handlers.GetVolumeStats, baseURL+"?date_from="+dateFrom+"&date_to="+dateTo)
	})

	t.Run("returns 500 when use case fails", func(t *testing.T) {
		t.Parallel()

		repo := &mockDashboardRepository{volumeErr: errTestStorageError}

		provider := &mockContextProvider{
			info: &ReconciliationContextInfo{ID: contextID, Active: true},
		}

		handlers := setupStatsHandlers(t, repo, provider)
		test500Error(t, handlers.GetVolumeStats, baseURL+"?date_from="+dateFrom+"&date_to="+dateTo)
	})

	t.Run("returns 400 for invalid context ID", func(t *testing.T) {
		t.Parallel()

		provider := &mockContextProvider{
			info: &ReconciliationContextInfo{ID: contextID, Active: true},
		}

		handlers := setupStatsHandlers(t, &mockDashboardRepository{}, provider)
		testBadRequestError(
			t,
			handlers.GetVolumeStats,
			"/v1/reports/contexts/invalid-uuid/dashboard/volume?date_from="+dateFrom+"&date_to="+dateTo,
			"invalid context_id",
		)
	})
}

func TestHandlers_GetMatchRateStats_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	repo := &mockDashboardRepository{
		volumeStats: &entities.VolumeStats{
			TotalTransactions:   100,
			MatchedTransactions: 75,
			UnmatchedCount:      25,
			TotalAmount:         decimal.NewFromInt(1000),
			MatchedAmount:       decimal.NewFromInt(800),
			UnmatchedAmount:     decimal.NewFromInt(200),
		},
	}
	uc, ucErr := query.NewDashboardUseCase(repo, nil)
	require.NoError(t, ucErr)

	provider := &mockContextProvider{info: &ReconciliationContextInfo{ID: contextID, Active: true}}
	handlers, err := NewHandlers(uc, provider, newTestExportUseCase(t), false)
	require.NoError(t, err)

	app := setupTestApp(handlers.GetMatchRateStats)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/match-rate?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result dto.MatchRateStatsResponse

	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.InDelta(t, 75.0, result.MatchRate, 0.01)
}

func TestHandlers_GetMatchRateStats_MissingDateFrom(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{info: &ReconciliationContextInfo{ID: contextID, Active: true}}
	handlers := setupStatsHandlers(t, &mockDashboardRepository{}, provider)
	testBadRequestError(t, handlers.GetMatchRateStats,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/match-rate?date_to="+dateTo,
		ErrDateFromRequired.Error())
}

func TestHandlers_GetMatchRateStats_InvalidSourceID(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{info: &ReconciliationContextInfo{ID: contextID, Active: true}}
	handlers := setupStatsHandlers(t, &mockDashboardRepository{}, provider)
	testBadRequestError(
		t,
		handlers.GetMatchRateStats,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/match-rate?date_from="+dateFrom+"&date_to="+dateTo+"&source_id=bad",
		ErrInvalidSourceID.Error(),
	)
}

func TestHandlers_GetMatchRateStats_ContextLookupFails(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{err: errTestDBUnavailable}
	handlers := setupStatsHandlers(t, &mockDashboardRepository{}, provider)
	test500Error(
		t,
		handlers.GetMatchRateStats,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/match-rate?date_from="+dateFrom+"&date_to="+dateTo,
	)
}

func TestHandlers_GetMatchRateStats_UseCaseFails(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	repo := &mockDashboardRepository{volumeErr: errTestStorageError}
	provider := &mockContextProvider{info: &ReconciliationContextInfo{ID: contextID, Active: true}}
	handlers := setupStatsHandlers(t, repo, provider)
	test500Error(
		t,
		handlers.GetMatchRateStats,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/match-rate?date_from="+dateFrom+"&date_to="+dateTo,
	)
}

func TestHandlers_GetMatchRateStats_InvalidContextID(t *testing.T) {
	t.Parallel()

	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{info: &ReconciliationContextInfo{ID: uuid.New(), Active: true}}
	handlers := setupStatsHandlers(t, &mockDashboardRepository{}, provider)
	testBadRequestError(
		t,
		handlers.GetMatchRateStats,
		"/v1/reports/contexts/invalid-uuid/dashboard/match-rate?date_from="+dateFrom+"&date_to="+dateTo,
		"invalid context_id",
	)
}

func TestHandlers_GetSLAStats_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	repo := &mockDashboardRepository{
		slaStats: &entities.SLAStats{
			TotalExceptions:     50,
			ResolvedOnTime:      40,
			ResolvedLate:        5,
			PendingWithinSLA:    3,
			PendingOverdue:      2,
			SLAComplianceRate:   88.89,
			AverageResolutionMs: 3600000,
		},
	}
	uc, ucErr := query.NewDashboardUseCase(repo, nil)
	require.NoError(t, ucErr)

	provider := &mockContextProvider{info: &ReconciliationContextInfo{ID: contextID, Active: true}}
	handlers, err := NewHandlers(uc, provider, newTestExportUseCase(t), false)
	require.NoError(t, err)

	app := setupTestApp(handlers.GetSLAStats)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/sla?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result dto.SLAStatsResponse

	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, 50, result.TotalExceptions)
	assert.Equal(t, 40, result.ResolvedOnTime)
}

func TestHandlers_GetSLAStats_MissingDateFrom(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{info: &ReconciliationContextInfo{ID: contextID, Active: true}}
	handlers := setupStatsHandlers(t, &mockDashboardRepository{}, provider)
	testBadRequestError(t, handlers.GetSLAStats,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/sla?date_to="+dateTo,
		ErrDateFromRequired.Error())
}

func TestHandlers_GetSLAStats_InvalidSourceID(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{info: &ReconciliationContextInfo{ID: contextID, Active: true}}
	handlers := setupStatsHandlers(t, &mockDashboardRepository{}, provider)
	testBadRequestError(
		t,
		handlers.GetSLAStats,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/sla?date_from="+dateFrom+"&date_to="+dateTo+"&source_id=bad",
		ErrInvalidSourceID.Error(),
	)
}

func TestHandlers_GetSLAStats_ContextLookupFails(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{err: errTestDBUnavailable}
	handlers := setupStatsHandlers(t, &mockDashboardRepository{}, provider)
	test500Error(
		t,
		handlers.GetSLAStats,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/sla?date_from="+dateFrom+"&date_to="+dateTo,
	)
}

func TestHandlers_GetSLAStats_UseCaseFails(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	repo := &mockDashboardRepository{slaErr: errTestStorageError}
	provider := &mockContextProvider{info: &ReconciliationContextInfo{ID: contextID, Active: true}}
	handlers := setupStatsHandlers(t, repo, provider)
	test500Error(
		t,
		handlers.GetSLAStats,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/sla?date_from="+dateFrom+"&date_to="+dateTo,
	)
}

func TestHandlers_GetSLAStats_InvalidContextID(t *testing.T) {
	t.Parallel()

	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{info: &ReconciliationContextInfo{ID: uuid.New(), Active: true}}
	handlers := setupStatsHandlers(t, &mockDashboardRepository{}, provider)
	testBadRequestError(t, handlers.GetSLAStats,
		"/v1/reports/contexts/invalid-uuid/dashboard/sla?date_from="+dateFrom+"&date_to="+dateTo,
		"invalid context_id")
}

func TestHandlers_GetDashboardAggregates_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	repo := &mockDashboardRepository{
		volumeStats: &entities.VolumeStats{
			TotalTransactions:   100,
			MatchedTransactions: 80,
			TotalAmount:         decimal.NewFromInt(10000),
			MatchedAmount:       decimal.NewFromInt(8000),
		},
		slaStats: &entities.SLAStats{
			TotalExceptions:   10,
			ResolvedOnTime:    8,
			SLAComplianceRate: 80.0,
		},
	}
	uc, ucErr := query.NewDashboardUseCase(repo, nil)
	require.NoError(t, ucErr)

	provider := &mockContextProvider{info: &ReconciliationContextInfo{ID: contextID, Active: true}}
	handlers, err := NewHandlers(uc, provider, newTestExportUseCase(t), false)
	require.NoError(t, err)

	app := setupTestApp(handlers.GetDashboardAggregates)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result dto.DashboardAggregatesResponse

	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.NotNil(t, result.Volume)
	assert.NotNil(t, result.MatchRate)
	assert.NotNil(t, result.SLA)
}

func TestHandlers_GetDashboardAggregates_InactiveContext(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	repo := &mockDashboardRepository{}
	uc, ucErr := query.NewDashboardUseCase(repo, nil)
	require.NoError(t, ucErr)

	provider := &mockContextProvider{info: &ReconciliationContextInfo{ID: contextID, Active: false}}
	handlers, err := NewHandlers(uc, provider, newTestExportUseCase(t), false)
	require.NoError(t, err)

	app := setupTestApp(handlers.GetDashboardAggregates)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)

	var payload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.Equal(t, float64(403), payload["code"])
	require.Equal(t, "context_not_active", payload["title"])
	require.Equal(t, "context is not active", payload["message"])
}

func TestHandlers_GetDashboardAggregates_ContextNotFound(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	repo := &mockDashboardRepository{}
	uc, ucErr := query.NewDashboardUseCase(repo, nil)
	require.NoError(t, ucErr)

	provider := &mockContextProvider{info: nil, err: nil}
	handlers, err := NewHandlers(uc, provider, newTestExportUseCase(t), false)
	require.NoError(t, err)

	app := setupTestApp(handlers.GetDashboardAggregates)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	requireNotFoundResponse(t, resp, "context not found")
}

func TestHandlers_GetDashboardAggregates_MissingDateFrom(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{info: &ReconciliationContextInfo{ID: contextID, Active: true}}
	handlers := setupStatsHandlers(t, &mockDashboardRepository{}, provider)
	testBadRequestError(t, handlers.GetDashboardAggregates,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard?date_to="+dateTo,
		ErrDateFromRequired.Error())
}

func TestHandlers_GetDashboardAggregates_InvalidSourceID(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{info: &ReconciliationContextInfo{ID: contextID, Active: true}}
	handlers := setupStatsHandlers(t, &mockDashboardRepository{}, provider)
	testBadRequestError(
		t,
		handlers.GetDashboardAggregates,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard?date_from="+dateFrom+"&date_to="+dateTo+"&source_id=bad",
		ErrInvalidSourceID.Error(),
	)
}

func TestHandlers_GetDashboardAggregates_ContextLookupFails(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{err: errTestDBUnavailable}
	handlers := setupStatsHandlers(t, &mockDashboardRepository{}, provider)
	test500Error(
		t,
		handlers.GetDashboardAggregates,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard?date_from="+dateFrom+"&date_to="+dateTo,
	)
}

func TestHandlers_GetDashboardAggregates_UseCaseFails(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	repo := &mockDashboardRepository{volumeErr: errTestStorageError}
	provider := &mockContextProvider{info: &ReconciliationContextInfo{ID: contextID, Active: true}}
	handlers := setupStatsHandlers(t, repo, provider)
	test500Error(
		t,
		handlers.GetDashboardAggregates,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard?date_from="+dateFrom+"&date_to="+dateTo,
	)
}

func TestLogSpanError_WithNilLogger(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")
	_, span := tracer.Start(context.Background(), "test")

	defer span.End()

	require.NotPanics(t, func() {
		logSpanError(context.Background(), span, nil, "test message", errors.New("test error"))
	})
}

func TestLogSpanError_WithLogger(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")
	_, span := tracer.Start(context.Background(), "test")

	defer span.End()

	mock := &testutil.TestLogger{}
	logSpanError(context.Background(), span, mock, "test message", errors.New("test error"))
	require.True(t, mock.ErrorCalled)
}
