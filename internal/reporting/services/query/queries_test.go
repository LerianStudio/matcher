//go:build unit

package query

import (
	"context"
	"encoding/csv"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories/mocks"
)

func TestNewUseCase_NilRepository(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(nil)

	require.Nil(t, uc)
	require.ErrorIs(t, err, ErrNilReportRepository)
}

func TestNewUseCase_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)

	uc, err := NewUseCase(mockRepo)

	require.NoError(t, err)
	assert.NotNil(t, uc)
}

func TestGetMatchedReport_ReturnsItemsFromRepo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
		Limit:     10,
	}

	expectedItems := []*entities.MatchedItem{
		{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(100),
			Currency:      "USD",
			Date:          time.Now().UTC(),
		},
		{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(200),
			Currency:      "USD",
			Date:          time.Now().UTC(),
		},
	}

	mockRepo.EXPECT().
		ListMatched(gomock.Any(), filter).
		Return(expectedItems, libHTTP.CursorPagination{}, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	result, pagination, err := uc.GetMatchedReport(ctx, filter)

	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, expectedItems, result)
	assert.Equal(t, libHTTP.CursorPagination{}, pagination)
}

func TestGetUnmatchedReport_ReturnsItemsFromRepo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
		Limit:     10,
	}

	expectedItems := []*entities.UnmatchedItem{
		{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(50),
			Currency:      "USD",
			Status:        "UNMATCHED",
			Date:          time.Now().UTC(),
		},
	}

	mockRepo.EXPECT().
		ListUnmatched(gomock.Any(), filter).
		Return(expectedItems, libHTTP.CursorPagination{}, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	result, pagination, err := uc.GetUnmatchedReport(ctx, filter)

	require.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, expectedItems, result)
	assert.Equal(t, libHTTP.CursorPagination{}, pagination)
}

func TestGetSummaryReport_ReturnsSummaryFromRepo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
		Limit:     10,
	}

	expectedSummary := &entities.SummaryReport{
		MatchedCount:    5,
		UnmatchedCount:  3,
		MatchedAmount:   decimal.NewFromInt(1000),
		UnmatchedAmount: decimal.NewFromInt(300),
		TotalAmount:     decimal.NewFromInt(1300),
	}

	mockRepo.EXPECT().
		GetSummary(gomock.Any(), filter).
		Return(expectedSummary, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	result, err := uc.GetSummaryReport(ctx, filter)

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expectedSummary.MatchedCount, result.MatchedCount)
	assert.Equal(t, expectedSummary.UnmatchedCount, result.UnmatchedCount)
	assert.True(t, expectedSummary.TotalAmount.Equal(result.TotalAmount))
}

func TestExportMatchedCSV_CallsBuilderWithRepoData(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
		Limit:     10,
	}

	matchDate := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	matchedItems := []*entities.MatchedItem{
		{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(100),
			Currency:      "USD",
			Date:          matchDate,
		},
	}

	mockRepo.EXPECT().
		ListMatchedForExport(gomock.Any(), filter, MaxExportRecords).
		Return(matchedItems, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportMatchedCSV(ctx, filter)

	require.NoError(t, err)
	rows := readCSVRows(t, data)
	require.Len(t, rows, 2)
	assert.Equal(
		t,
		[]string{"transaction_id", "match_group_id", "source_id", "amount", "currency", "date"},
		rows[0],
	)
	assert.Equal(t, matchedItems[0].TransactionID.String(), rows[1][0])
	assert.Equal(t, matchedItems[0].MatchGroupID.String(), rows[1][1])
	assert.Equal(t, matchedItems[0].Amount.String(), rows[1][3])
	assert.Equal(t, matchDate.UTC().Format(time.RFC3339), rows[1][5])
}

func TestExportUnmatchedCSV_CallsBuilderWithRepoData(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
		Limit:     10,
	}

	unmatchedDate := time.Date(2026, 1, 3, 10, 0, 0, 0, time.UTC)
	unmatchedItems := []*entities.UnmatchedItem{
		{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(50),
			Currency:      "USD",
			Status:        "UNMATCHED",
			Date:          unmatchedDate,
		},
	}

	mockRepo.EXPECT().
		ListUnmatchedForExport(gomock.Any(), filter, MaxExportRecords).
		Return(unmatchedItems, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportUnmatchedCSV(ctx, filter)

	require.NoError(t, err)
	rows := readCSVRows(t, data)
	require.Len(t, rows, 2)
	assert.Equal(
		t,
		[]string{
			"transaction_id",
			"source_id",
			"amount",
			"currency",
			"status",
			"date",
			"exception_id",
			"due_at",
		},
		rows[0],
	)
	assert.Equal(t, unmatchedItems[0].TransactionID.String(), rows[1][0])
	assert.Equal(t, unmatchedItems[0].Status, rows[1][4])
	assert.Equal(t, unmatchedDate.UTC().Format(time.RFC3339), rows[1][5])
}

func TestExportSummaryCSV_CallsBuilderWithRepoData(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
		Limit:     10,
	}

	summary := &entities.SummaryReport{
		MatchedCount:    5,
		UnmatchedCount:  3,
		MatchedAmount:   decimal.NewFromInt(1000),
		UnmatchedAmount: decimal.NewFromInt(300),
		TotalAmount:     decimal.NewFromInt(1300),
	}

	mockRepo.EXPECT().
		GetSummary(gomock.Any(), filter).
		Return(summary, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportSummaryCSV(ctx, filter)

	require.NoError(t, err)
	rows := readCSVRows(t, data)
	require.Len(t, rows, 2)
	assert.Equal(
		t,
		[]string{
			"matched_count",
			"unmatched_count",
			"total_amount",
			"matched_amount",
			"unmatched_amount",
		},
		rows[0],
	)
	assert.Equal(t, "5", rows[1][0])
	assert.Equal(t, summary.TotalAmount.String(), rows[1][2])
}

func TestExportMatchedPDF_CallsBuilderWithRepoData(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
		Limit:     10,
	}

	matchedItems := []*entities.MatchedItem{
		{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(100),
			Currency:      "USD",
			Date:          time.Now().UTC(),
		},
	}

	mockRepo.EXPECT().
		ListMatchedForExport(gomock.Any(), filter, MaxPDFExportRecords).
		Return(matchedItems, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportMatchedPDF(ctx, filter)

	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(string(data), "%PDF-"))
}

func TestExportUnmatchedPDF_CallsBuilderWithRepoData(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
		Limit:     10,
	}

	unmatchedItems := []*entities.UnmatchedItem{
		{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(50),
			Currency:      "USD",
			Status:        "UNMATCHED",
			Date:          time.Now().UTC(),
		},
	}

	mockRepo.EXPECT().
		ListUnmatchedForExport(gomock.Any(), filter, MaxPDFExportRecords).
		Return(unmatchedItems, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportUnmatchedPDF(ctx, filter)

	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(string(data), "%PDF-"))
}

func TestExportSummaryPDF_CallsBuilderWithRepoData(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
		Limit:     10,
	}

	summary := &entities.SummaryReport{
		MatchedCount:    5,
		UnmatchedCount:  3,
		MatchedAmount:   decimal.NewFromInt(1000),
		UnmatchedAmount: decimal.NewFromInt(300),
		TotalAmount:     decimal.NewFromInt(1300),
	}

	mockRepo.EXPECT().
		GetSummary(gomock.Any(), filter).
		Return(summary, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportSummaryPDF(ctx, filter)

	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(string(data), "%PDF-"))
}

func TestGetVarianceReport_ReturnsRowsFromRepo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.VarianceReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
		Limit:     10,
	}

	variancePct := decimal.NewFromFloat(10)
	rows := []*entities.VarianceReportRow{
		{
			SourceID:        uuid.New(),
			Currency:        "USD",
			FeeScheduleName: "PERCENTAGE",
			TotalExpected:   decimal.NewFromInt(100),
			TotalActual:     decimal.NewFromInt(110),
			NetVariance:     decimal.NewFromInt(10),
			VariancePct:     &variancePct,
		},
	}

	expectedPagination := libHTTP.CursorPagination{
		Next: "next-cursor",
		Prev: "",
	}

	mockRepo.EXPECT().
		GetVarianceReport(gomock.Any(), filter).
		Return(rows, expectedPagination, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	result, pagination, err := uc.GetVarianceReport(ctx, filter)

	require.NoError(t, err)
	assert.Equal(t, rows, result)
	assert.Equal(t, expectedPagination, pagination)
}

func TestExportVarianceCSV_CallsBuilderWithRepoData(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.VarianceReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
		Limit:     10,
	}

	variancePct := decimal.NewFromFloat(5)
	rows := []*entities.VarianceReportRow{
		{
			SourceID:        uuid.New(),
			Currency:        "EUR",
			FeeScheduleName: "FLAT",
			TotalExpected:   decimal.NewFromInt(50),
			TotalActual:     decimal.NewFromInt(55),
			NetVariance:     decimal.NewFromInt(5),
			VariancePct:     &variancePct,
		},
	}

	mockRepo.EXPECT().
		ListVarianceForExport(gomock.Any(), filter, MaxExportRecords).
		Return(rows, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportVarianceCSV(ctx, filter)

	require.NoError(t, err)
	rowsData := readCSVRows(t, data)
	require.Len(t, rowsData, 2)
	assert.Equal(
		t,
		[]string{
			"source_id",
			"currency",
			"fee_schedule_id",
			"fee_schedule_name",
			"total_expected",
			"total_actual",
			"net_variance",
			"variance_pct",
		},
		rowsData[0],
	)
	assert.Equal(t, rows[0].SourceID.String(), rowsData[1][0])
	assert.Equal(t, rows[0].FeeScheduleID.String(), rowsData[1][2])
	assert.Equal(t, rows[0].FeeScheduleName, rowsData[1][3])
}

func TestExportVariancePDF_CallsBuilderWithRepoData(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.VarianceReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
		Limit:     10,
	}

	rows := []*entities.VarianceReportRow{
		{
			SourceID:        uuid.New(),
			Currency:        "BRL",
			FeeScheduleName: "TIERED",
			TotalExpected:   decimal.NewFromInt(80),
			TotalActual:     decimal.NewFromInt(85),
			NetVariance:     decimal.NewFromInt(5),
			VariancePct:     nil,
		},
	}

	mockRepo.EXPECT().
		ListVarianceForExport(gomock.Any(), filter, MaxPDFExportRecords).
		Return(rows, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportVariancePDF(ctx, filter)

	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(string(data), "%PDF-"))
	assert.NotEmpty(t, data)
}

func readCSVRows(t *testing.T, data []byte) [][]string {
	t.Helper()

	reader := csv.NewReader(strings.NewReader(string(data)))
	rows, err := reader.ReadAll()
	require.NoError(t, err)

	return rows
}

// Error tests for repository failures

var errTestRepoError = errors.New("repository error")

func TestGetMatchedReport_ReturnsErrorOnRepoFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		ListMatched(gomock.Any(), filter).
		Return(nil, libHTTP.CursorPagination{}, errTestRepoError)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	result, _, err := uc.GetMatchedReport(ctx, filter)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "listing matched items")
}

func TestGetUnmatchedReport_ReturnsErrorOnRepoFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		ListUnmatched(gomock.Any(), filter).
		Return(nil, libHTTP.CursorPagination{}, errTestRepoError)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	result, _, err := uc.GetUnmatchedReport(ctx, filter)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "listing unmatched items")
}

func TestGetSummaryReport_ReturnsErrorOnRepoFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		GetSummary(gomock.Any(), filter).
		Return(nil, errTestRepoError)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	result, err := uc.GetSummaryReport(ctx, filter)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "getting summary report")
}

func TestExportMatchedCSV_ReturnsErrorOnRepoFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		ListMatchedForExport(gomock.Any(), filter, MaxExportRecords).
		Return(nil, errTestRepoError)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportMatchedCSV(ctx, filter)

	require.Error(t, err)
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "listing matched items for CSV export")
}

func TestExportUnmatchedCSV_ReturnsErrorOnRepoFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		ListUnmatchedForExport(gomock.Any(), filter, MaxExportRecords).
		Return(nil, errTestRepoError)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportUnmatchedCSV(ctx, filter)

	require.Error(t, err)
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "listing unmatched items for CSV export")
}

func TestExportSummaryCSV_ReturnsErrorOnRepoFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		GetSummary(gomock.Any(), filter).
		Return(nil, errTestRepoError)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportSummaryCSV(ctx, filter)

	require.Error(t, err)
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "getting summary for CSV export")
}

func TestExportMatchedPDF_ReturnsErrorOnRepoFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		ListMatchedForExport(gomock.Any(), filter, MaxPDFExportRecords).
		Return(nil, errTestRepoError)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportMatchedPDF(ctx, filter)

	require.Error(t, err)
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "listing matched items for PDF export")
}

func TestExportUnmatchedPDF_ReturnsErrorOnRepoFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		ListUnmatchedForExport(gomock.Any(), filter, MaxPDFExportRecords).
		Return(nil, errTestRepoError)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportUnmatchedPDF(ctx, filter)

	require.Error(t, err)
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "listing unmatched items for PDF export")
}

func TestExportSummaryPDF_ReturnsErrorOnRepoFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		GetSummary(gomock.Any(), filter).
		Return(nil, errTestRepoError)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportSummaryPDF(ctx, filter)

	require.Error(t, err)
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "getting summary for PDF export")
}

func TestGetVarianceReport_ReturnsErrorOnRepoFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.VarianceReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		GetVarianceReport(gomock.Any(), filter).
		Return(nil, libHTTP.CursorPagination{}, errTestRepoError)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	result, _, err := uc.GetVarianceReport(ctx, filter)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "getting variance report")
}

func TestExportVarianceCSV_ReturnsErrorOnRepoFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.VarianceReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		ListVarianceForExport(gomock.Any(), filter, MaxExportRecords).
		Return(nil, errTestRepoError)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportVarianceCSV(ctx, filter)

	require.Error(t, err)
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "getting variance report for CSV export")
}

func TestExportVariancePDF_ReturnsErrorOnRepoFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.VarianceReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		ListVarianceForExport(gomock.Any(), filter, MaxPDFExportRecords).
		Return(nil, errTestRepoError)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportVariancePDF(ctx, filter)

	require.Error(t, err)
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "getting variance report for PDF export")
}

// Empty results tests

func TestGetMatchedReport_EmptyResults(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		ListMatched(gomock.Any(), filter).
		Return([]*entities.MatchedItem{}, libHTTP.CursorPagination{}, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	result, _, err := uc.GetMatchedReport(ctx, filter)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestGetUnmatchedReport_EmptyResults(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		ListUnmatched(gomock.Any(), filter).
		Return([]*entities.UnmatchedItem{}, libHTTP.CursorPagination{}, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	result, _, err := uc.GetUnmatchedReport(ctx, filter)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestExportMatchedCSV_EmptyResults(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		ListMatchedForExport(gomock.Any(), filter, MaxExportRecords).
		Return([]*entities.MatchedItem{}, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportMatchedCSV(ctx, filter)

	require.NoError(t, err)
	assert.NotEmpty(t, data)
	rows := readCSVRows(t, data)
	assert.Len(t, rows, 1)
}

func TestExportUnmatchedCSV_EmptyResults(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		ListUnmatchedForExport(gomock.Any(), filter, MaxExportRecords).
		Return([]*entities.UnmatchedItem{}, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportUnmatchedCSV(ctx, filter)

	require.NoError(t, err)
	assert.NotEmpty(t, data)
	rows := readCSVRows(t, data)
	assert.Len(t, rows, 1)
}

func TestGetVarianceReport_EmptyResults(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.VarianceReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		GetVarianceReport(gomock.Any(), filter).
		Return([]*entities.VarianceReportRow{}, libHTTP.CursorPagination{}, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	result, _, err := uc.GetVarianceReport(ctx, filter)

	require.NoError(t, err)
	assert.Empty(t, result)
}

// Streaming tests

func TestSupportsStreaming_ReturnsFalse_WhenNotStreamingRepo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)

	assert.False(t, uc.SupportsStreaming())
}

func TestStreamMatchedCSV_ReturnsError_WhenStreamingNotSupported(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)

	var buf strings.Builder
	err = uc.StreamMatchedCSV(ctx, filter, &buf)

	require.ErrorIs(t, err, ErrStreamingNotSupported)
}

func TestStreamUnmatchedCSV_ReturnsError_WhenStreamingNotSupported(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)

	var buf strings.Builder
	err = uc.StreamUnmatchedCSV(ctx, filter, &buf)

	require.ErrorIs(t, err, ErrStreamingNotSupported)
}

func TestStreamVarianceCSV_ReturnsError_WhenStreamingNotSupported(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.VarianceReportFilter{
		ContextID: uuid.New(),
	}

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)

	var buf strings.Builder
	err = uc.StreamVarianceCSV(ctx, filter, &buf)

	require.ErrorIs(t, err, ErrStreamingNotSupported)
}

// Pagination tests

func TestGetMatchedReport_WithPagination(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		Limit:     10,
		Cursor:    "next-cursor",
	}

	expectedPagination := libHTTP.CursorPagination{
		Next: "cursor-for-next-page",
		Prev: "cursor-for-prev-page",
	}

	mockRepo.EXPECT().
		ListMatched(gomock.Any(), filter).
		Return([]*entities.MatchedItem{}, expectedPagination, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	_, pagination, err := uc.GetMatchedReport(ctx, filter)

	require.NoError(t, err)
	assert.Equal(t, expectedPagination, pagination)
}

func TestGetUnmatchedReport_WithPagination(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		Limit:     20,
	}

	expectedPagination := libHTTP.CursorPagination{
		Next: "next-cursor",
	}

	mockRepo.EXPECT().
		ListUnmatched(gomock.Any(), filter).
		Return([]*entities.UnmatchedItem{}, expectedPagination, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	_, pagination, err := uc.GetUnmatchedReport(ctx, filter)

	require.NoError(t, err)
	assert.Equal(t, expectedPagination, pagination)
}

// Filter validation tests

func TestGetMatchedReport_WithDateFilters(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()

	dateFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2026, 1, 31, 23, 59, 59, 0, time.UTC)

	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  dateFrom,
		DateTo:    dateTo,
	}

	mockRepo.EXPECT().
		ListMatched(gomock.Any(), filter).
		Return([]*entities.MatchedItem{}, libHTTP.CursorPagination{}, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	_, _, err = uc.GetMatchedReport(ctx, filter)

	require.NoError(t, err)
}

func TestExportSummaryCSV_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	summary := &entities.SummaryReport{
		MatchedCount:    10,
		UnmatchedCount:  5,
		TotalAmount:     decimal.NewFromFloat(1500.50),
		MatchedAmount:   decimal.NewFromFloat(1000.25),
		UnmatchedAmount: decimal.NewFromFloat(500.25),
	}

	mockRepo.EXPECT().
		GetSummary(gomock.Any(), filter).
		Return(summary, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportSummaryCSV(ctx, filter)

	require.NoError(t, err)
	assert.NotEmpty(t, data)
	rows := readCSVRows(t, data)
	require.Len(t, rows, 2)
	assert.Equal(t, "10", rows[1][0])
	assert.Equal(t, "5", rows[1][1])
}

func TestExportMatchedPDF_EmptyResults(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		ListMatchedForExport(gomock.Any(), filter, MaxPDFExportRecords).
		Return([]*entities.MatchedItem{}, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportMatchedPDF(ctx, filter)

	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(string(data), "%PDF-"))
}

func TestExportUnmatchedPDF_EmptyResults(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		ListUnmatchedForExport(gomock.Any(), filter, MaxPDFExportRecords).
		Return([]*entities.UnmatchedItem{}, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportUnmatchedPDF(ctx, filter)

	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(string(data), "%PDF-"))
}

func TestExportVarianceCSV_EmptyResults(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.VarianceReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		ListVarianceForExport(gomock.Any(), filter, MaxExportRecords).
		Return([]*entities.VarianceReportRow{}, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportVarianceCSV(ctx, filter)

	require.NoError(t, err)
	assert.NotEmpty(t, data)
	rows := readCSVRows(t, data)
	assert.Len(t, rows, 1)
}

func TestExportVariancePDF_EmptyResults(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockReportRepository(ctrl)
	ctx := context.Background()
	filter := entities.VarianceReportFilter{
		ContextID: uuid.New(),
	}

	mockRepo.EXPECT().
		ListVarianceForExport(gomock.Any(), filter, MaxPDFExportRecords).
		Return([]*entities.VarianceReportRow{}, nil)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	data, err := uc.ExportVariancePDF(ctx, filter)

	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(string(data), "%PDF-"))
}
