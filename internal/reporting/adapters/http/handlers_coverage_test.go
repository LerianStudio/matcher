//go:build unit

package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/reporting/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	repomocks "github.com/LerianStudio/matcher/internal/reporting/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/reporting/services/command"
	"github.com/LerianStudio/matcher/internal/reporting/services/query"
	portsmocks "github.com/LerianStudio/matcher/internal/shared/ports/mocks"
)

// --- mock dashboard repository that supports source breakdown and cash impact ---

type fullMockDashboardRepository struct {
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
	sourceBreakdown  []entities.SourceBreakdown
	sourceBreakErr   error
	cashImpact       *entities.CashImpactSummary
	cashImpactErr    error
}

func (m *fullMockDashboardRepository) GetVolumeStats(
	_ context.Context,
	_ entities.DashboardFilter,
) (*entities.VolumeStats, error) {
	return m.volumeStats, m.volumeErr
}

func (m *fullMockDashboardRepository) GetSLAStats(
	_ context.Context,
	_ entities.DashboardFilter,
) (*entities.SLAStats, error) {
	return m.slaStats, m.slaErr
}

func (m *fullMockDashboardRepository) GetSummaryMetrics(
	_ context.Context,
	_ entities.DashboardFilter,
) (*entities.SummaryMetrics, error) {
	return m.summaryMetrics, m.summaryErr
}

func (m *fullMockDashboardRepository) GetTrendMetrics(
	_ context.Context,
	_ entities.DashboardFilter,
) (*entities.TrendMetrics, error) {
	return m.trendMetrics, m.trendErr
}

func (m *fullMockDashboardRepository) GetBreakdownMetrics(
	_ context.Context,
	_ entities.DashboardFilter,
) (*entities.BreakdownMetrics, error) {
	return m.breakdownMetrics, m.breakdownErr
}

func (m *fullMockDashboardRepository) GetSourceBreakdown(
	_ context.Context,
	_ entities.DashboardFilter,
) ([]entities.SourceBreakdown, error) {
	return m.sourceBreakdown, m.sourceBreakErr
}

func (m *fullMockDashboardRepository) GetCashImpactSummary(
	_ context.Context,
	_ entities.DashboardFilter,
) (*entities.CashImpactSummary, error) {
	return m.cashImpact, m.cashImpactErr
}

// --- mock report repository with configurable counts ---

type countMockReportRepository struct {
	mockReportRepository
	matchedCount        int64
	matchedCountErr     error
	unmatchedCount      int64
	unmatchedCountErr   error
	transactionCount    int64
	transactionCountErr error
	exceptionCount      int64
	exceptionCountErr   error
	matchedItems        []*entities.MatchedItem
	matchedItemsErr     error
	unmatchedItems      []*entities.UnmatchedItem
	unmatchedItemsErr   error
	summaryReport       *entities.SummaryReport
	summaryReportErr    error
	varianceRows        []*entities.VarianceReportRow
	varianceRowsErr     error
	listMatchedErr      error
	listUnmatchedErr    error
	getVarianceErr      error
}

func (m *countMockReportRepository) CountMatched(
	_ context.Context,
	_ entities.ReportFilter,
) (int64, error) {
	return m.matchedCount, m.matchedCountErr
}

func (m *countMockReportRepository) CountUnmatched(
	_ context.Context,
	_ entities.ReportFilter,
) (int64, error) {
	return m.unmatchedCount, m.unmatchedCountErr
}

func (m *countMockReportRepository) CountTransactions(
	_ context.Context,
	_ entities.ReportFilter,
) (int64, error) {
	return m.transactionCount, m.transactionCountErr
}

func (m *countMockReportRepository) CountExceptions(
	_ context.Context,
	_ entities.ReportFilter,
) (int64, error) {
	return m.exceptionCount, m.exceptionCountErr
}

func (m *countMockReportRepository) ListMatchedForExport(
	_ context.Context,
	_ entities.ReportFilter,
	_ int,
) ([]*entities.MatchedItem, error) {
	return m.matchedItems, m.matchedItemsErr
}

func (m *countMockReportRepository) ListUnmatchedForExport(
	_ context.Context,
	_ entities.ReportFilter,
	_ int,
) ([]*entities.UnmatchedItem, error) {
	return m.unmatchedItems, m.unmatchedItemsErr
}

func (m *countMockReportRepository) ListVarianceForExport(
	_ context.Context,
	_ entities.VarianceReportFilter,
	_ int,
) ([]*entities.VarianceReportRow, error) {
	return m.varianceRows, m.varianceRowsErr
}

func (m *countMockReportRepository) GetSummary(
	_ context.Context,
	_ entities.ReportFilter,
) (*entities.SummaryReport, error) {
	return m.summaryReport, m.summaryReportErr
}

func (m *countMockReportRepository) ListMatched(
	_ context.Context,
	_ entities.ReportFilter,
) ([]*entities.MatchedItem, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, m.listMatchedErr
}

func (m *countMockReportRepository) ListUnmatched(
	_ context.Context,
	_ entities.ReportFilter,
) ([]*entities.UnmatchedItem, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, m.listUnmatchedErr
}

func (m *countMockReportRepository) GetVarianceReport(
	_ context.Context,
	_ entities.VarianceReportFilter,
) ([]*entities.VarianceReportRow, libHTTP.CursorPagination, error) {
	return m.varianceRows, libHTTP.CursorPagination{}, m.getVarianceErr
}

// --- helpers ---

func setupFullTestApp(handler fiber.Handler, routes ...string) *fiber.App {
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.TenantIDKey, uuid.New().String())
		c.SetUserContext(ctx)

		return c.Next()
	})

	for _, route := range routes {
		app.Get(route, handler)
	}

	return app
}

func setupDashboardHandlers(
	t *testing.T,
	repo *fullMockDashboardRepository,
	provider *mockContextProvider,
	reportRepo *countMockReportRepository,
) *Handlers {
	t.Helper()

	uc, ucErr := query.NewDashboardUseCase(repo, nil)
	require.NoError(t, ucErr)

	var exportUC *query.UseCase

	var err error
	if reportRepo != nil {
		exportUC, err = query.NewUseCase(reportRepo)
	} else {
		exportUC, err = query.NewUseCase(&mockReportRepository{})
	}

	require.NoError(t, err)

	handlers, err := NewHandlers(uc, provider, exportUC, false)
	require.NoError(t, err)

	return handlers
}

// --- GetMatcherDashboardMetrics tests ---

func TestHandlers_GetMatcherDashboardMetrics_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	repo := &fullMockDashboardRepository{
		summaryMetrics: &entities.SummaryMetrics{
			TotalTransactions:  1000,
			TotalMatches:       450,
			MatchRate:          90.0,
			PendingExceptions:  25,
			CriticalExposure:   decimal.NewFromInt(50000),
			OldestExceptionAge: 48.5,
		},
		trendMetrics: &entities.TrendMetrics{
			Dates:      []string{"2024-01-01", "2024-01-02"},
			Ingestion:  []int{100, 150},
			Matches:    []int{80, 120},
			Exceptions: []int{5, 10},
			MatchRates: []float64{80.0, 80.0},
		},
		breakdownMetrics: &entities.BreakdownMetrics{
			BySeverity: map[string]int{"CRITICAL": 5, "HIGH": 10},
			ByReason:   map[string]int{"Amount Mismatch": 8},
			ByRule:     []entities.RuleMatchCount{},
			ByAge:      []entities.AgeBucket{},
		},
	}
	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handlers := setupDashboardHandlers(t, repo, provider, nil)

	app := setupFullTestApp(
		handlers.GetMatcherDashboardMetrics,
		"/v1/reports/contexts/:contextId/dashboard/metrics",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/metrics?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result dto.MatcherDashboardMetricsResponse

	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.NotNil(t, result.Summary)
	assert.Equal(t, 1000, result.Summary.TotalTransactions)
	assert.NotNil(t, result.Trends)
	assert.NotNil(t, result.Breakdowns)
}

func TestHandlers_GetMatcherDashboardMetrics_BadDateFilter(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{}
	reportRepo.getVarianceErr = nil
	reportRepo.varianceRows = []*entities.VarianceReportRow{{
		SourceID:        uuid.New(),
		Currency:        "USD",
		FeeScheduleID:   uuid.MustParse("00000000-0000-0000-0000-00000000aa01"),
		FeeScheduleName: "INTERCHANGE",
		TotalExpected:   decimal.RequireFromString("10.00"),
		TotalActual:     decimal.RequireFromString("12.00"),
		NetVariance:     decimal.RequireFromString("2.00"),
	}}
	handlers := setupDashboardHandlers(t, &fullMockDashboardRepository{}, provider, reportRepo)

	app := setupFullTestApp(
		handlers.GetMatcherDashboardMetrics,
		"/v1/reports/contexts/:contextId/dashboard/metrics",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/metrics?date_to=2024-01-31",
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestHandlers_GetMatcherDashboardMetrics_UseCaseFails(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	repo := &fullMockDashboardRepository{
		summaryErr: errTestDBUnavailable,
		trendMetrics: &entities.TrendMetrics{
			Dates:      []string{},
			Ingestion:  []int{},
			Matches:    []int{},
			Exceptions: []int{},
			MatchRates: []float64{},
		},
		breakdownMetrics: &entities.BreakdownMetrics{
			BySeverity: map[string]int{},
			ByReason:   map[string]int{},
			ByRule:     []entities.RuleMatchCount{},
			ByAge:      []entities.AgeBucket{},
		},
	}
	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handlers := setupDashboardHandlers(t, repo, provider, nil)

	app := setupFullTestApp(
		handlers.GetMatcherDashboardMetrics,
		"/v1/reports/contexts/:contextId/dashboard/metrics",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/metrics?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestHandlers_GetMatcherDashboardMetrics_InvalidContextID(t *testing.T) {
	t.Parallel()

	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: uuid.New(), Active: true},
	}
	handlers := setupDashboardHandlers(t, &fullMockDashboardRepository{}, provider, nil)

	app := setupFullTestApp(
		handlers.GetMatcherDashboardMetrics,
		"/v1/reports/contexts/:contextId/dashboard/metrics",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/invalid-uuid/dashboard/metrics?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- GetSourceBreakdown tests ---

func TestHandlers_GetSourceBreakdown_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	repo := &fullMockDashboardRepository{
		sourceBreakdown: []entities.SourceBreakdown{
			{
				SourceID:        uuid.New(),
				SourceName:      "Bank A",
				TotalTxns:       100,
				MatchedTxns:     80,
				UnmatchedTxns:   20,
				MatchRate:       80.0,
				TotalAmount:     decimal.NewFromInt(10000),
				UnmatchedAmount: decimal.NewFromInt(2000),
				Currency:        "USD",
			},
		},
	}
	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handlers := setupDashboardHandlers(t, repo, provider, nil)

	app := setupFullTestApp(
		handlers.GetSourceBreakdown,
		"/v1/reports/contexts/:contextId/dashboard/source-breakdown",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/source-breakdown?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result dto.SourceBreakdownListResponse

	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.Len(t, result.Sources, 1)
	assert.Equal(t, "Bank A", result.Sources[0].SourceName)
}

func TestHandlers_GetSourceBreakdown_UseCaseFails(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	repo := &fullMockDashboardRepository{sourceBreakErr: errTestStorageError}
	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handlers := setupDashboardHandlers(t, repo, provider, nil)

	app := setupFullTestApp(
		handlers.GetSourceBreakdown,
		"/v1/reports/contexts/:contextId/dashboard/source-breakdown",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/source-breakdown?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestHandlers_GetSourceBreakdown_BadDate(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{}
	reportRepo.varianceRows = []*entities.VarianceReportRow{{
		SourceID:        uuid.New(),
		Currency:        "USD",
		FeeScheduleID:   uuid.MustParse("00000000-0000-0000-0000-00000000aa01"),
		FeeScheduleName: "INTERCHANGE",
		TotalExpected:   decimal.RequireFromString("10.00"),
		TotalActual:     decimal.RequireFromString("12.00"),
		NetVariance:     decimal.RequireFromString("2.00"),
	}}
	handlers := setupDashboardHandlers(t, &fullMockDashboardRepository{}, provider, reportRepo)

	app := setupFullTestApp(
		handlers.GetSourceBreakdown,
		"/v1/reports/contexts/:contextId/dashboard/source-breakdown",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/source-breakdown?date_from=bad&date_to=2024-01-31",
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- GetCashImpactSummary tests ---

func TestHandlers_GetCashImpactSummary_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	repo := &fullMockDashboardRepository{
		cashImpact: &entities.CashImpactSummary{
			TotalUnmatchedAmount: decimal.NewFromInt(5000),
			ByCurrency: []entities.CurrencyExposure{
				{Currency: "USD", Amount: decimal.NewFromInt(3000), TransactionCount: 10},
				{Currency: "EUR", Amount: decimal.NewFromInt(2000), TransactionCount: 5},
			},
			ByAge: []entities.AgeExposure{
				{Bucket: "0-24h", Amount: decimal.NewFromInt(2000), TransactionCount: 6},
			},
		},
	}
	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handlers := setupDashboardHandlers(t, repo, provider, nil)

	app := setupFullTestApp(
		handlers.GetCashImpactSummary,
		"/v1/reports/contexts/:contextId/dashboard/cash-impact",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/cash-impact?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result dto.CashImpactSummaryResponse

	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, "5000", result.TotalUnmatchedAmount)
	assert.Len(t, result.ByCurrency, 2)
	assert.Len(t, result.ByAge, 1)
}

func TestHandlers_GetCashImpactSummary_UseCaseFails(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	repo := &fullMockDashboardRepository{cashImpactErr: errTestStorageError}
	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handlers := setupDashboardHandlers(t, repo, provider, nil)

	app := setupFullTestApp(
		handlers.GetCashImpactSummary,
		"/v1/reports/contexts/:contextId/dashboard/cash-impact",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/cash-impact?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestHandlers_GetCashImpactSummary_BadDate(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{}
	reportRepo.varianceRows = []*entities.VarianceReportRow{{
		SourceID:        uuid.New(),
		Currency:        "USD",
		FeeScheduleID:   uuid.MustParse("00000000-0000-0000-0000-00000000aa01"),
		FeeScheduleName: "INTERCHANGE",
		TotalExpected:   decimal.RequireFromString("10.00"),
		TotalActual:     decimal.RequireFromString("12.00"),
		NetVariance:     decimal.RequireFromString("2.00"),
	}}
	handlers := setupDashboardHandlers(t, &fullMockDashboardRepository{}, provider, reportRepo)

	app := setupFullTestApp(
		handlers.GetCashImpactSummary,
		"/v1/reports/contexts/:contextId/dashboard/cash-impact",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/dashboard/cash-impact?date_from=2024-01-01",
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- Count handler tests ---

func setupCountHandlers(
	t *testing.T,
	provider *mockContextProvider,
	reportRepo *countMockReportRepository,
) *Handlers {
	t.Helper()

	dashRepo := &fullMockDashboardRepository{}
	uc, ucErr := query.NewDashboardUseCase(dashRepo, nil)
	require.NoError(t, ucErr)

	exportUC, err := query.NewUseCase(reportRepo)
	require.NoError(t, err)

	handlers, err := NewHandlers(uc, provider, exportUC, false)
	require.NoError(t, err)

	return handlers
}

func setupCountTestApp(handler fiber.Handler, route string) *fiber.App {
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.TenantIDKey, uuid.New().String())
		c.SetUserContext(ctx)

		return c.Next()
	})
	app.Get(route, handler)

	return app
}

func TestHandlers_CountMatched_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{matchedCount: 42}
	handlers := setupCountHandlers(t, provider, reportRepo)

	app := setupCountTestApp(
		handlers.CountMatched,
		"/v1/reports/contexts/:contextId/matches/count",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/matches/count?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result dto.ExportCountResponse

	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(42), result.Count)
}

func TestHandlers_CountMatched_MissingDateFrom(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handlers := setupCountHandlers(t, provider, &countMockReportRepository{})

	app := setupCountTestApp(
		handlers.CountMatched,
		"/v1/reports/contexts/:contextId/matches/count",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/matches/count?date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestHandlers_CountMatched_UseCaseFails(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{matchedCountErr: errTestStorageError}
	handlers := setupCountHandlers(t, provider, reportRepo)

	app := setupCountTestApp(
		handlers.CountMatched,
		"/v1/reports/contexts/:contextId/matches/count",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/matches/count?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestHandlers_CountMatched_InvalidContext(t *testing.T) {
	t.Parallel()

	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: uuid.New(), Active: true},
	}
	handlers := setupCountHandlers(t, provider, &countMockReportRepository{})

	app := setupCountTestApp(
		handlers.CountMatched,
		"/v1/reports/contexts/:contextId/matches/count",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/invalid-uuid/matches/count?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestHandlers_CountTransactions_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{transactionCount: 999}
	handlers := setupCountHandlers(t, provider, reportRepo)

	app := setupCountTestApp(
		handlers.CountTransactions,
		"/v1/reports/contexts/:contextId/transactions/count",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/transactions/count?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result dto.ExportCountResponse

	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(999), result.Count)
}

func TestHandlers_CountExceptions_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{exceptionCount: 7}
	handlers := setupCountHandlers(t, provider, reportRepo)

	app := setupCountTestApp(
		handlers.CountExceptions,
		"/v1/reports/contexts/:contextId/exceptions/count",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/exceptions/count?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result dto.ExportCountResponse

	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(7), result.Count)
}

// --- Export handler tests ---

func TestHandlers_ExportMatchedReport_CSV_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{
		matchedItems: []*entities.MatchedItem{
			{
				TransactionID: uuid.New(),
				MatchGroupID:  uuid.New(),
				SourceID:      uuid.New(),
				Amount:        decimal.NewFromInt(100),
				Currency:      "USD",
				Date:          time.Now().UTC(),
			},
		},
	}
	handlers := setupCountHandlers(t, provider, reportRepo)

	app := setupCountTestApp(
		handlers.ExportMatchedReport,
		"/v1/reports/contexts/:contextId/matched/export",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/matched/export?date_from="+dateFrom+"&date_to="+dateTo+"&format=csv",
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/csv", resp.Header.Get("Content-Type"))
	assert.Contains(t, resp.Header.Get("Content-Disposition"), "matched_report.csv")
}

func TestHandlers_ExportMatchedReport_PDF_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{
		matchedItems: []*entities.MatchedItem{},
	}
	handlers := setupCountHandlers(t, provider, reportRepo)

	app := setupCountTestApp(
		handlers.ExportMatchedReport,
		"/v1/reports/contexts/:contextId/matched/export",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/matched/export?date_from="+dateFrom+"&date_to="+dateTo+"&format=pdf",
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/pdf", resp.Header.Get("Content-Type"))
}

func TestHandlers_ExportMatchedReport_InvalidFormat(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handlers := setupCountHandlers(t, provider, &countMockReportRepository{})

	app := setupCountTestApp(
		handlers.ExportMatchedReport,
		"/v1/reports/contexts/:contextId/matched/export",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/matched/export?date_from="+dateFrom+"&date_to="+dateTo+"&format=xlsx",
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestHandlers_ExportMatchedReport_UseCaseFails(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{
		matchedItemsErr: errTestStorageError,
	}
	handlers := setupCountHandlers(t, provider, reportRepo)

	app := setupCountTestApp(
		handlers.ExportMatchedReport,
		"/v1/reports/contexts/:contextId/matched/export",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/matched/export?date_from="+dateFrom+"&date_to="+dateTo+"&format=csv",
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestHandlers_ExportUnmatchedReport_CSV_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{
		unmatchedItems: []*entities.UnmatchedItem{},
	}
	handlers := setupCountHandlers(t, provider, reportRepo)

	app := setupCountTestApp(
		handlers.ExportUnmatchedReport,
		"/v1/reports/contexts/:contextId/unmatched/export",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/unmatched/export?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/csv", resp.Header.Get("Content-Type"))
}

func TestHandlers_ExportSummaryReport_CSV_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{
		summaryReport: &entities.SummaryReport{
			MatchedCount:    5,
			UnmatchedCount:  3,
			MatchedAmount:   decimal.NewFromInt(1000),
			UnmatchedAmount: decimal.NewFromInt(300),
			TotalAmount:     decimal.NewFromInt(1300),
		},
	}
	handlers := setupCountHandlers(t, provider, reportRepo)

	app := setupCountTestApp(
		handlers.ExportSummaryReport,
		"/v1/reports/contexts/:contextId/summary/export",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/summary/export?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/csv", resp.Header.Get("Content-Type"))
}

func TestHandlers_ExportVarianceReport_CSV_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{
		varianceRows: []*entities.VarianceReportRow{},
	}
	handlers := setupCountHandlers(t, provider, reportRepo)

	app := setupCountTestApp(
		handlers.ExportVarianceReport,
		"/v1/reports/contexts/:contextId/variance/export",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/variance/export?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/csv", resp.Header.Get("Content-Type"))
}

func TestHandlers_ExportVarianceReport_PDF_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{
		varianceRows: []*entities.VarianceReportRow{},
	}
	handlers := setupCountHandlers(t, provider, reportRepo)

	app := setupCountTestApp(
		handlers.ExportVarianceReport,
		"/v1/reports/contexts/:contextId/variance/export",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/variance/export?date_from="+dateFrom+"&date_to="+dateTo+"&format=pdf",
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/pdf", resp.Header.Get("Content-Type"))
}

func TestHandlers_ExportVarianceReport_InvalidFormat(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handlers := setupCountHandlers(t, provider, &countMockReportRepository{})

	app := setupCountTestApp(
		handlers.ExportVarianceReport,
		"/v1/reports/contexts/:contextId/variance/export",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/variance/export?date_from="+dateFrom+"&date_to="+dateTo+"&format=xlsx",
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestHandlers_ExportVarianceReport_CSVFails(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{
		varianceRowsErr: errTestStorageError,
	}
	handlers := setupCountHandlers(t, provider, reportRepo)

	app := setupCountTestApp(
		handlers.ExportVarianceReport,
		"/v1/reports/contexts/:contextId/variance/export",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/variance/export?date_from="+dateFrom+"&date_to="+dateTo+"&format=csv",
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestHandlers_ExportVarianceReport_PDFFails(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{
		varianceRowsErr: errTestStorageError,
	}
	handlers := setupCountHandlers(t, provider, reportRepo)

	app := setupCountTestApp(
		handlers.ExportVarianceReport,
		"/v1/reports/contexts/:contextId/variance/export",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/variance/export?date_from="+dateFrom+"&date_to="+dateTo+"&format=pdf",
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestHandlers_ExportVarianceReport_BadDateFilter(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handlers := setupCountHandlers(t, provider, &countMockReportRepository{})

	app := setupCountTestApp(
		handlers.ExportVarianceReport,
		"/v1/reports/contexts/:contextId/variance/export",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/variance/export?date_from=bad&date_to=2024-01-31",
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestHandlers_ExportVarianceReport_InvalidContextID(t *testing.T) {
	t.Parallel()

	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: uuid.New(), Active: true},
	}
	handlers := setupCountHandlers(t, provider, &countMockReportRepository{})

	app := setupCountTestApp(
		handlers.ExportVarianceReport,
		"/v1/reports/contexts/:contextId/variance/export",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/invalid-uuid/variance/export?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- Date range exceeded test ---

func TestHandlers_CountMatched_DateRangeExceeded(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handlers := setupCountHandlers(t, provider, &countMockReportRepository{})

	app := setupCountTestApp(
		handlers.CountMatched,
		"/v1/reports/contexts/:contextId/matches/count",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/matches/count?date_from=2024-01-01&date_to=2024-12-31",
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- CountUnmatched tests ---

func TestHandlers_CountUnmatched_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{unmatchedCount: 15}
	handlers := setupCountHandlers(t, provider, reportRepo)

	app := setupCountTestApp(
		handlers.CountUnmatched,
		"/v1/reports/contexts/:contextId/unmatched/count",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/unmatched/count?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result dto.ExportCountResponse

	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, int64(15), result.Count)
}

func TestHandlers_CountUnmatched_UseCaseFails(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{unmatchedCountErr: errTestStorageError}
	handlers := setupCountHandlers(t, provider, reportRepo)

	app := setupCountTestApp(
		handlers.CountUnmatched,
		"/v1/reports/contexts/:contextId/unmatched/count",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/unmatched/count?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// --- Report browsing handler tests ---

func TestHandlers_GetMatchedReport_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{}
	reportRepo.varianceRows = []*entities.VarianceReportRow{{
		SourceID:        uuid.New(),
		Currency:        "USD",
		FeeScheduleID:   uuid.MustParse("00000000-0000-0000-0000-00000000aa01"),
		FeeScheduleName: "INTERCHANGE",
		TotalExpected:   decimal.RequireFromString("10.00"),
		TotalActual:     decimal.RequireFromString("12.00"),
		NetVariance:     decimal.RequireFromString("2.00"),
	}}
	handlers := setupDashboardHandlers(t, &fullMockDashboardRepository{}, provider, reportRepo)

	app := setupFullTestApp(
		handlers.GetMatchedReport,
		"/v1/reports/contexts/:contextId/matched",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/matched?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result dto.ListMatchedReportResponse

	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.NotNil(t, result.Items)
}

func TestHandlers_GetMatchedReport_BadDate(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{}
	reportRepo.varianceRows = []*entities.VarianceReportRow{{
		SourceID:        uuid.New(),
		Currency:        "USD",
		FeeScheduleID:   uuid.MustParse("00000000-0000-0000-0000-00000000aa01"),
		FeeScheduleName: "INTERCHANGE",
		TotalExpected:   decimal.RequireFromString("10.00"),
		TotalActual:     decimal.RequireFromString("12.00"),
		NetVariance:     decimal.RequireFromString("2.00"),
	}}
	handlers := setupDashboardHandlers(t, &fullMockDashboardRepository{}, provider, reportRepo)

	app := setupFullTestApp(
		handlers.GetMatchedReport,
		"/v1/reports/contexts/:contextId/matched",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/matched?date_to=2024-01-31",
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestHandlers_GetUnmatchedReport_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handlers := setupDashboardHandlers(t, &fullMockDashboardRepository{}, provider, nil)

	app := setupFullTestApp(
		handlers.GetUnmatchedReport,
		"/v1/reports/contexts/:contextId/unmatched",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/unmatched?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result dto.ListUnmatchedReportResponse

	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.NotNil(t, result.Items)
}

func TestHandlers_GetSummaryReport_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handlers := setupDashboardHandlers(t, &fullMockDashboardRepository{}, provider, nil)

	app := setupFullTestApp(
		handlers.GetSummaryReport,
		"/v1/reports/contexts/:contextId/summary",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/summary?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result dto.SummaryReportResponse

	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
}

func TestHandlers_GetVarianceReport_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{}
	reportRepo.varianceRows = []*entities.VarianceReportRow{{
		SourceID:        uuid.New(),
		Currency:        "USD",
		FeeScheduleID:   uuid.MustParse("00000000-0000-0000-0000-00000000aa01"),
		FeeScheduleName: "INTERCHANGE",
		TotalExpected:   decimal.RequireFromString("10.00"),
		TotalActual:     decimal.RequireFromString("12.00"),
		NetVariance:     decimal.RequireFromString("2.00"),
	}}
	handlers := setupDashboardHandlers(t, &fullMockDashboardRepository{}, provider, reportRepo)

	app := setupFullTestApp(
		handlers.GetVarianceReport,
		"/v1/reports/contexts/:contextId/variance",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/variance?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result dto.ListVarianceReportResponse

	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	require.Len(t, result.Items, 1)
	assert.Equal(t, "INTERCHANGE", result.Items[0].FeeScheduleName)
	assert.Equal(t, "00000000-0000-0000-0000-00000000aa01", result.Items[0].FeeScheduleID)
}

func TestHandlers_GetVarianceReport_BadDate(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handlers := setupDashboardHandlers(t, &fullMockDashboardRepository{}, provider, nil)

	app := setupFullTestApp(
		handlers.GetVarianceReport,
		"/v1/reports/contexts/:contextId/variance",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/variance?date_from=bad&date_to=2024-01-31",
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- DTO conversion tests ---

func TestSourceBreakdownToResponse_WithData(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	breakdowns := []entities.SourceBreakdown{
		{
			SourceID:        sourceID,
			SourceName:      "Test Source",
			TotalTxns:       100,
			MatchedTxns:     80,
			UnmatchedTxns:   20,
			MatchRate:       80.0,
			TotalAmount:     decimal.NewFromInt(10000),
			UnmatchedAmount: decimal.NewFromInt(2000),
			Currency:        "USD",
		},
	}

	result := dto.SourceBreakdownToResponse(breakdowns)

	require.NotNil(t, result)
	require.Len(t, result.Sources, 1)
	assert.Equal(t, sourceID.String(), result.Sources[0].SourceID)
	assert.Equal(t, "Test Source", result.Sources[0].SourceName)
	assert.Equal(t, int64(100), result.Sources[0].TotalTxns)
	assert.Equal(t, "10000", result.Sources[0].TotalAmount)
	assert.Equal(t, "USD", result.Sources[0].Currency)
}

func TestSourceBreakdownToResponse_NilInput(t *testing.T) {
	t.Parallel()

	result := dto.SourceBreakdownToResponse(nil)

	require.NotNil(t, result)
	assert.Empty(t, result.Sources)
}

func TestCashImpactSummaryToResponse_WithData(t *testing.T) {
	t.Parallel()

	summary := &entities.CashImpactSummary{
		TotalUnmatchedAmount: decimal.NewFromInt(5000),
		ByCurrency: []entities.CurrencyExposure{
			{Currency: "USD", Amount: decimal.NewFromInt(3000), TransactionCount: 10},
			{Currency: "EUR", Amount: decimal.NewFromInt(2000), TransactionCount: 5},
		},
		ByAge: []entities.AgeExposure{
			{Bucket: "0-24h", Amount: decimal.NewFromInt(2000), TransactionCount: 6},
			{Bucket: "1-3d", Amount: decimal.NewFromInt(3000), TransactionCount: 9},
		},
	}

	result := dto.CashImpactSummaryToResponse(summary)

	require.NotNil(t, result)
	assert.Equal(t, "5000", result.TotalUnmatchedAmount)
	require.Len(t, result.ByCurrency, 2)
	assert.Equal(t, "USD", result.ByCurrency[0].Currency)
	assert.Equal(t, "3000", result.ByCurrency[0].Amount)
	assert.Equal(t, int64(10), result.ByCurrency[0].TransactionCount)
	require.Len(t, result.ByAge, 2)
	assert.Equal(t, "0-24h", result.ByAge[0].Bucket)
}

func TestCashImpactSummaryToResponse_NilInput(t *testing.T) {
	t.Parallel()

	result := dto.CashImpactSummaryToResponse(nil)

	require.NotNil(t, result)
	assert.Equal(t, "0", result.TotalUnmatchedAmount)
	assert.Empty(t, result.ByCurrency)
	assert.Empty(t, result.ByAge)
}

// --- parseExportJobRequest coverage ---

func TestParseExportJobRequest_InvalidReportType(t *testing.T) {
	t.Parallel()

	req := &CreateExportJobRequest{
		ReportType: "INVALID",
		Format:     "CSV",
		DateFrom:   "2024-01-01",
		DateTo:     "2024-01-31",
	}

	parsed, msg, err := parseExportJobRequest(req)

	assert.Nil(t, parsed)
	assert.Contains(t, msg, "invalid report_type")
	assert.ErrorIs(t, err, entities.ErrInvalidReportType)
}

func TestParseExportJobRequest_InvalidFormat(t *testing.T) {
	t.Parallel()

	req := &CreateExportJobRequest{
		ReportType: "MATCHED",
		Format:     "INVALID",
		DateFrom:   "2024-01-01",
		DateTo:     "2024-01-31",
	}

	parsed, msg, err := parseExportJobRequest(req)

	assert.Nil(t, parsed)
	assert.Contains(t, msg, "invalid format")
	assert.ErrorIs(t, err, entities.ErrInvalidExportFormat)
}

func TestParseExportJobRequest_InvalidDateTo(t *testing.T) {
	t.Parallel()

	req := &CreateExportJobRequest{
		ReportType: "MATCHED",
		Format:     "CSV",
		DateFrom:   "2024-01-01",
		DateTo:     "not-a-date",
	}

	parsed, msg, err := parseExportJobRequest(req)

	assert.Nil(t, parsed)
	assert.Contains(t, msg, "invalid date_to format")
	assert.Error(t, err)
}

func TestParseExportJobRequest_WithSourceID(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New().String()
	req := &CreateExportJobRequest{
		ReportType: "MATCHED",
		Format:     "CSV",
		DateFrom:   "2024-01-01",
		DateTo:     "2024-01-31",
		SourceID:   &sourceID,
	}

	parsed, msg, err := parseExportJobRequest(req)

	require.NotNil(t, parsed)
	assert.Empty(t, msg)
	assert.NoError(t, err)
	assert.NotNil(t, parsed.sourceID)
}

func TestParseExportJobRequest_WithInvalidSourceID(t *testing.T) {
	t.Parallel()

	invalidID := "not-a-uuid"
	req := &CreateExportJobRequest{
		ReportType: "MATCHED",
		Format:     "CSV",
		DateFrom:   "2024-01-01",
		DateTo:     "2024-01-31",
		SourceID:   &invalidID,
	}

	parsed, msg, err := parseExportJobRequest(req)

	assert.Nil(t, parsed)
	assert.Contains(t, msg, "invalid source_id")
	assert.Error(t, err)
}

func TestParseExportJobRequest_LowercaseNormalization(t *testing.T) {
	t.Parallel()

	req := &CreateExportJobRequest{
		ReportType: "matched",
		Format:     "csv",
		DateFrom:   "2024-01-01",
		DateTo:     "2024-01-31",
	}

	parsed, msg, err := parseExportJobRequest(req)

	require.NotNil(t, parsed)
	assert.Empty(t, msg)
	assert.NoError(t, err)
	assert.Equal(t, entities.ExportReportTypeMatched, parsed.reportType)
	assert.Equal(t, entities.ExportFormatCSV, parsed.format)
}

func TestParseExportJobRequest_LegacyReportTypeAliases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected entities.ExportReportType
	}{
		{name: "MATCHES alias", input: "MATCHES", expected: entities.ExportReportTypeMatched},
		{name: "unmatched transactions alias", input: "UNMATCHED_TRANSACTIONS", expected: entities.ExportReportTypeUnmatched},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := &CreateExportJobRequest{
				ReportType: tc.input,
				Format:     "CSV",
				DateFrom:   "2024-01-01",
				DateTo:     "2024-01-31",
			}

			parsed, msg, err := parseExportJobRequest(req)

			require.NotNil(t, parsed)
			assert.Empty(t, msg)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, parsed.reportType)
		})
	}

	t.Run("EXCEPTIONS rejected as not yet supported", func(t *testing.T) {
		t.Parallel()

		req := &CreateExportJobRequest{
			ReportType: "EXCEPTIONS",
			Format:     "CSV",
			DateFrom:   "2024-01-01",
			DateTo:     "2024-01-31",
		}

		parsed, msg, err := parseExportJobRequest(req)

		assert.Nil(t, parsed)
		assert.Contains(t, msg, "not yet supported")
		assert.ErrorIs(t, err, ErrExceptionsNotSupportedAsync)
	})
}

// --- mapJobToResponse coverage ---

func TestMapJobToResponse_WithErrorField(t *testing.T) {
	t.Parallel()

	ctxProvider := &mockContextProvider{info: &ReconciliationContextInfo{ID: uuid.New()}}
	storage := newStorageClientMock(t, storageClientMockConfig{})

	repo := newExportJobRepoMock(t)

	uc, err := newExportJobUseCase(t, repo)
	require.NoError(t, err)

	querySvc, err := query.NewExportJobQueryService(repo)
	require.NoError(t, err)

	handlers, err := NewExportJobHandlers(uc, querySvc, storage, ctxProvider, time.Hour)
	require.NoError(t, err)

	startedAt := time.Now().UTC().Add(-time.Hour)
	finishedAt := time.Now().UTC()
	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: "MATCHED",
		Format:     "CSV",
		Status:     entities.ExportJobStatusFailed,
		FileName:   "test.csv",
		Error:      "processing failed",
		StartedAt:  &startedAt,
		FinishedAt: &finishedAt,
		CreatedAt:  time.Now().UTC(),
		ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
		UpdatedAt:  time.Now().UTC(),
	}

	response := handlers.mapJobToResponse(context.Background(), job)

	require.NotNil(t, response)
	assert.NotNil(t, response.FileName)
	assert.Equal(t, "test.csv", *response.FileName)
	assert.NotNil(t, response.Error)
	assert.Equal(t, "processing failed", *response.Error)
	assert.NotNil(t, response.StartedAt)
	assert.NotNil(t, response.FinishedAt)
	assert.Nil(t, response.DownloadURL)
}

func TestMapJobToResponse_DownloadableWithFutureExpiry(t *testing.T) {
	t.Parallel()

	ctxProvider := &mockContextProvider{info: &ReconciliationContextInfo{ID: uuid.New()}}
	storage := newStorageClientMock(t, storageClientMockConfig{})

	repo := newExportJobRepoMock(t)

	uc, err := newExportJobUseCase(t, repo)
	require.NoError(t, err)

	querySvc, err := query.NewExportJobQueryService(repo)
	require.NoError(t, err)

	handlers, err := NewExportJobHandlers(uc, querySvc, storage, ctxProvider, time.Hour)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: "MATCHED",
		Format:     "CSV",
		Status:     entities.ExportJobStatusSucceeded,
		FileKey:    "exports/test.csv",
		FileName:   "test.csv",
		CreatedAt:  time.Now().UTC(),
		ExpiresAt:  time.Now().UTC().Add(7 * 24 * time.Hour),
		UpdatedAt:  time.Now().UTC(),
	}

	response := handlers.mapJobToResponse(context.Background(), job)

	require.NotNil(t, response)
	assert.NotNil(t, response.DownloadURL)
	assert.Contains(t, *response.DownloadURL, "/v1/export-jobs/")
	assert.Contains(t, *response.DownloadURL, "/download")
}

func TestMapJobToResponse_NilJobReturnsEmptyResponse(t *testing.T) {
	t.Parallel()

	ctxProvider := &mockContextProvider{info: &ReconciliationContextInfo{ID: uuid.New()}}
	storage := newStorageClientMock(t, storageClientMockConfig{})
	repo := newExportJobRepoMock(t)

	uc, err := newExportJobUseCase(t, repo)
	require.NoError(t, err)

	querySvc, err := query.NewExportJobQueryService(repo)
	require.NoError(t, err)

	handlers, err := NewExportJobHandlers(uc, querySvc, storage, ctxProvider, time.Hour)
	require.NoError(t, err)

	response := handlers.mapJobToResponse(context.Background(), nil)
	require.NotNil(t, response)
	assert.Equal(t, "", response.ID)
	assert.Nil(t, response.DownloadURL)
}

func newExportJobUseCase(t *testing.T, repo *repomocks.MockExportJobRepository) (*command.ExportJobUseCase, error) {
	t.Helper()

	return command.NewExportJobUseCase(repo)
}

// --- ListExportJobsByContext handler tests ---

func setupListByContextApp(handler fiber.Handler) *fiber.App {
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
	app.Get("/v1/contexts/:contextId/export-jobs", handler)

	return app
}

func setupListByContextHandlers(
	t *testing.T,
	repo *repomocks.MockExportJobRepository,
	storage *portsmocks.MockObjectStorageClient,
	ctxProvider *mockContextProvider,
) *ExportJobHandlers {
	t.Helper()

	uc, err := command.NewExportJobUseCase(repo)
	require.NoError(t, err)

	querySvc, err := query.NewExportJobQueryService(repo)
	require.NoError(t, err)

	handlers, err := NewExportJobHandlers(uc, querySvc, storage, ctxProvider, time.Hour)
	require.NoError(t, err)

	return handlers
}

func TestExportJobHandlers_ListExportJobsByContext_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	jobs := []*entities.ExportJob{
		{
			ID:         uuid.New(),
			TenantID:   uuid.New(),
			ContextID:  contextID,
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
		ListByContext(gomock.Any(), contextID, gomock.Any()).
		Return(jobs, nil).
		Times(1)

	storage := newStorageClientMock(t, storageClientMockConfig{})
	ctxProvider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}

	handlers := setupListByContextHandlers(t, repo, storage, ctxProvider)
	app := setupListByContextApp(handlers.ListExportJobsByContext)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/contexts/"+contextID.String()+"/export-jobs",
		http.NoBody,
	)

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var response ExportJobListResponse

	err = json.NewDecoder(resp.Body).Decode(&response)
	require.NoError(t, err)
	assert.Len(t, response.Items, 1)
}

func TestExportJobHandlers_ListExportJobsByContext_ContextNotFound(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	repo := newExportJobRepoMock(t)
	storage := newStorageClientMock(t, storageClientMockConfig{})
	ctxProvider := &mockContextProvider{info: nil, err: nil}

	handlers := setupListByContextHandlers(t, repo, storage, ctxProvider)
	app := setupListByContextApp(handlers.ListExportJobsByContext)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/contexts/"+contextID.String()+"/export-jobs",
		http.NoBody,
	)

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestExportJobHandlers_ListExportJobsByContext_ContextNotActive(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	repo := newExportJobRepoMock(t)
	storage := newStorageClientMock(t, storageClientMockConfig{})
	ctxProvider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: false},
	}

	handlers := setupListByContextHandlers(t, repo, storage, ctxProvider)
	app := setupListByContextApp(handlers.ListExportJobsByContext)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/contexts/"+contextID.String()+"/export-jobs",
		http.NoBody,
	)

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestExportJobHandlers_ListExportJobsByContext_ServiceError(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	repo := newExportJobRepoMock(t)
	repo.EXPECT().
		ListByContext(gomock.Any(), contextID, gomock.Any()).
		Return(nil, errTestStorageError).
		Times(1)

	storage := newStorageClientMock(t, storageClientMockConfig{})
	ctxProvider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}

	handlers := setupListByContextHandlers(t, repo, storage, ctxProvider)
	app := setupListByContextApp(handlers.ListExportJobsByContext)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/contexts/"+contextID.String()+"/export-jobs",
		http.NoBody,
	)

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// --- Report browsing handler error-path tests (H4) ---

func TestHandlers_GetMatchedReport_UseCaseError(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{listMatchedErr: errTestStorageError}
	handlers := setupDashboardHandlers(t, &fullMockDashboardRepository{}, provider, reportRepo)

	app := setupFullTestApp(
		handlers.GetMatchedReport,
		"/v1/reports/contexts/:contextId/matched",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/matched?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestHandlers_GetUnmatchedReport_UseCaseError(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{listUnmatchedErr: errTestStorageError}
	handlers := setupDashboardHandlers(t, &fullMockDashboardRepository{}, provider, reportRepo)

	app := setupFullTestApp(
		handlers.GetUnmatchedReport,
		"/v1/reports/contexts/:contextId/unmatched",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/unmatched?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestHandlers_GetSummaryReport_UseCaseError(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{summaryReportErr: errTestStorageError}
	handlers := setupDashboardHandlers(t, &fullMockDashboardRepository{}, provider, reportRepo)

	app := setupFullTestApp(
		handlers.GetSummaryReport,
		"/v1/reports/contexts/:contextId/summary",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/summary?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestHandlers_GetVarianceReport_UseCaseError(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour).Format(time.DateOnly)
	dateTo := time.Now().UTC().Format(time.DateOnly)

	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	reportRepo := &countMockReportRepository{getVarianceErr: errTestStorageError}
	handlers := setupDashboardHandlers(t, &fullMockDashboardRepository{}, provider, reportRepo)

	app := setupFullTestApp(
		handlers.GetVarianceReport,
		"/v1/reports/contexts/:contextId/variance",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/variance?date_from="+dateFrom+"&date_to="+dateTo,
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// --- Bad-date tests for GetUnmatchedReport and GetSummaryReport (M7) ---

func TestHandlers_GetUnmatchedReport_BadDate(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handlers := setupDashboardHandlers(t, &fullMockDashboardRepository{}, provider, nil)

	app := setupFullTestApp(
		handlers.GetUnmatchedReport,
		"/v1/reports/contexts/:contextId/unmatched",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/unmatched?date_from=bad&date_to=2024-01-31",
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestHandlers_GetSummaryReport_BadDate(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	provider := &mockContextProvider{
		info: &ReconciliationContextInfo{ID: contextID, Active: true},
	}
	handlers := setupDashboardHandlers(t, &fullMockDashboardRepository{}, provider, nil)

	app := setupFullTestApp(
		handlers.GetSummaryReport,
		"/v1/reports/contexts/:contextId/summary",
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/reports/contexts/"+contextID.String()+"/summary?date_from=bad&date_to=2024-01-31",
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
