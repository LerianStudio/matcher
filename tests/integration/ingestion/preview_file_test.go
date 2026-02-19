//go:build integration

package ingestion

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTxRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	ingestionQuery "github.com/LerianStudio/matcher/internal/ingestion/services/query"
	"github.com/LerianStudio/matcher/tests/integration"
)

// generateCSV builds a CSV string with the given number of data rows.
// Headers are always: id,amount,currency,date.
func generateCSV(numRows int) string {
	var sb strings.Builder

	sb.WriteString("id,amount,currency,date\n")

	for i := 1; i <= numRows; i++ {
		sb.WriteString(fmt.Sprintf("%d,%.2f,USD,2024-01-%02d\n", i, float64(i)*10.0, (i%28)+1))
	}

	return sb.String()
}

// newPreviewQueryUC constructs an ingestion query UseCase wired to the
// test harness's Postgres provider. PreviewFile never hits the DB, but
// the constructor still validates that both repos are non-nil.
func newPreviewQueryUC(t *testing.T, h *integration.TestHarness) *ingestionQuery.UseCase {
	t.Helper()

	provider := h.Provider()
	jobRepo := ingestionJobRepo.NewRepository(provider)
	txRepo := ingestionTxRepo.NewRepository(provider)

	queryUC, err := ingestionQuery.NewUseCase(jobRepo, txRepo)
	require.NoError(t, err)

	return queryUC
}

func TestPreviewFile_CSV_Basic(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		queryUC := newPreviewQueryUC(t, h)
		ctx := context.Background()

		csvData := "id,amount,currency,date\n1,100.50,USD,2024-01-15\n2,250.00,EUR,2024-01-16\n3,75.25,GBP,2024-01-17\n"

		result, err := queryUC.PreviewFile(ctx, strings.NewReader(csvData), "csv", 5)
		require.NoError(t, err)

		require.Equal(t, []string{"id", "amount", "currency", "date"}, result.Columns)
		require.Len(t, result.SampleRows, 3)
		require.Equal(t, 3, result.RowCount)
		require.Equal(t, "csv", result.Format)

		// Verify actual row content to ensure parsing fidelity.
		require.Equal(t, []string{"1", "100.50", "USD", "2024-01-15"}, result.SampleRows[0])
		require.Equal(t, []string{"2", "250.00", "EUR", "2024-01-16"}, result.SampleRows[1])
		require.Equal(t, []string{"3", "75.25", "GBP", "2024-01-17"}, result.SampleRows[2])
	})
}

func TestPreviewFile_CSV_LimitsRows(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		queryUC := newPreviewQueryUC(t, h)
		ctx := context.Background()

		csvData := generateCSV(10)

		result, err := queryUC.PreviewFile(ctx, strings.NewReader(csvData), "csv", 3)
		require.NoError(t, err)

		require.Len(t, result.SampleRows, 3)
		require.Equal(t, 3, result.RowCount)
		require.Equal(t, "csv", result.Format)
	})
}

func TestPreviewFile_JSON_Basic(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		queryUC := newPreviewQueryUC(t, h)
		ctx := context.Background()

		jsonData := `[{"id":"1","amount":"100.50","currency":"USD","date":"2024-01-15"},{"id":"2","amount":"250.00","currency":"EUR","date":"2024-01-16"}]`

		result, err := queryUC.PreviewFile(ctx, strings.NewReader(jsonData), "json", 5)
		require.NoError(t, err)

		// JSON columns are sorted alphabetically by the implementation.
		require.Equal(t, []string{"amount", "currency", "date", "id"}, result.Columns)
		require.Len(t, result.SampleRows, 2)
		require.Equal(t, 2, result.RowCount)
		require.Equal(t, "json", result.Format)
	})
}

func TestPreviewFile_CSV_DefaultMaxRows(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		queryUC := newPreviewQueryUC(t, h)
		ctx := context.Background()

		csvData := generateCSV(10)

		// maxRows=0 should default to 5 (defaultPreviewMaxRows).
		result, err := queryUC.PreviewFile(ctx, strings.NewReader(csvData), "csv", 0)
		require.NoError(t, err)

		require.LessOrEqual(t, result.RowCount, 5)
		require.Len(t, result.SampleRows, result.RowCount)
		require.Equal(t, "csv", result.Format)
	})
}

func TestPreviewFile_CSV_MaxRowsCapped(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		queryUC := newPreviewQueryUC(t, h)
		ctx := context.Background()

		csvData := generateCSV(25)

		// maxRows=100 should be capped at 20 (maxPreviewMaxRows).
		result, err := queryUC.PreviewFile(ctx, strings.NewReader(csvData), "csv", 100)
		require.NoError(t, err)

		require.LessOrEqual(t, result.RowCount, 20)
		require.Len(t, result.SampleRows, result.RowCount)
		require.Equal(t, "csv", result.Format)
	})
}

func TestPreviewFile_InvalidFormat(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		queryUC := newPreviewQueryUC(t, h)
		ctx := context.Background()

		_, err := queryUC.PreviewFile(ctx, strings.NewReader("anything"), "parquet", 5)
		require.ErrorIs(t, err, ingestionQuery.ErrPreviewInvalidFormat)
	})
}

func TestPreviewFile_EmptyFile(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		queryUC := newPreviewQueryUC(t, h)
		ctx := context.Background()

		_, err := queryUC.PreviewFile(ctx, strings.NewReader(""), "csv", 5)
		require.ErrorIs(t, err, ingestionQuery.ErrPreviewEmptyFile)
	})
}

func TestPreviewFile_NilReader(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		queryUC := newPreviewQueryUC(t, h)
		ctx := context.Background()

		_, err := queryUC.PreviewFile(ctx, nil, "csv", 5)
		require.ErrorIs(t, err, ingestionQuery.ErrPreviewReaderRequired)
	})
}
