//go:build integration

package ingestion

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTxRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	ingestionEntities "github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	ingestionRepositories "github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

// seedSearchTransactions creates a completed ingestion job and 5 transactions with
// varying currencies, amounts, dates, statuses, and descriptions.
func seedSearchTransactions(t *testing.T, h *integration.TestHarness) {
	t.Helper()

	ctx := h.Ctx()
	provider := h.Provider()

	jRepo := ingestionJobRepo.NewRepository(provider)
	tRepo := ingestionTxRepo.NewRepository(provider)

	// Create and complete a job (prerequisite FK for transactions).
	job, err := ingestionEntities.NewIngestionJob(ctx, h.Seed.ContextID, h.Seed.SourceID, "search_test.csv", 1000)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	require.NoError(t, job.Complete(ctx, 5, 0))

	createdJob, err := jRepo.Create(ctx, job)
	require.NoError(t, err)

	txData := []struct {
		externalID  string
		amount      string
		currency    string
		date        time.Time
		description string
		status      shared.TransactionStatus
	}{
		{"SEARCH-TX-001", "100.50", "USD", time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), "Payment for invoice 001", shared.TransactionStatusUnmatched},
		{"SEARCH-TX-002", "250.00", "USD", time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC), "Subscription renewal", shared.TransactionStatusUnmatched},
		{"SEARCH-TX-003", "100.50", "EUR", time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC), "Payment for invoice 003", shared.TransactionStatusMatched},
		{"SEARCH-TX-004", "500.00", "USD", time.Date(2024, 2, 15, 0, 0, 0, 0, time.UTC), "Large wire transfer", shared.TransactionStatusUnmatched},
		{"SEARCH-TX-005", "75.25", "GBP", time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC), "Refund processing", shared.TransactionStatusIgnored},
	}

	transactions := make([]*shared.Transaction, 0, len(txData))

	for _, td := range txData {
		amt, err := decimal.NewFromString(td.amount)
		require.NoError(t, err)

		tx, err := shared.NewTransaction(
			createdJob.ID,
			h.Seed.SourceID,
			td.externalID,
			amt,
			td.currency,
			td.date,
			td.description,
			map[string]any{},
		)
		require.NoError(t, err)

		tx.ExtractionStatus = shared.ExtractionStatusComplete
		tx.Status = td.status

		transactions = append(transactions, tx)
	}

	_, err = tRepo.CreateBatch(ctx, transactions)
	require.NoError(t, err)
}

func TestSearchTransactions_ByCurrency(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seedSearchTransactions(t, h)

		ctx := h.Ctx()
		tRepo := ingestionTxRepo.NewRepository(h.Provider())

		results, total, err := tRepo.SearchTransactions(ctx, h.Seed.ContextID, ingestionRepositories.TransactionSearchParams{
			Currency: "USD",
		})
		require.NoError(t, err)
		require.Equal(t, int64(3), total)
		require.Len(t, results, 3)

		for _, tx := range results {
			require.Equal(t, "USD", tx.Currency)
		}
	})
}

func TestSearchTransactions_ByAmountRange(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seedSearchTransactions(t, h)

		ctx := h.Ctx()
		tRepo := ingestionTxRepo.NewRepository(h.Provider())

		amountMin := decimal.NewFromInt(100)
		amountMax := decimal.NewFromInt(300)

		results, total, err := tRepo.SearchTransactions(ctx, h.Seed.ContextID, ingestionRepositories.TransactionSearchParams{
			AmountMin: &amountMin,
			AmountMax: &amountMax,
		})
		require.NoError(t, err)

		// TX-001=100.50, TX-002=250.00, TX-003=100.50 → all within [100, 300]
		require.Equal(t, int64(3), total)
		require.Len(t, results, 3)

		for _, tx := range results {
			require.True(t, tx.Amount.GreaterThanOrEqual(amountMin), "amount %s should be >= %s", tx.Amount, amountMin)
			require.True(t, tx.Amount.LessThanOrEqual(amountMax), "amount %s should be <= %s", tx.Amount, amountMax)
		}
	})
}

func TestSearchTransactions_ByDateRange(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seedSearchTransactions(t, h)

		ctx := h.Ctx()
		tRepo := ingestionTxRepo.NewRepository(h.Provider())

		dateFrom := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
		dateTo := time.Date(2024, 2, 28, 23, 59, 59, 0, time.UTC)

		results, total, err := tRepo.SearchTransactions(ctx, h.Seed.ContextID, ingestionRepositories.TransactionSearchParams{
			DateFrom: &dateFrom,
			DateTo:   &dateTo,
		})
		require.NoError(t, err)

		// TX-003=2024-02-01, TX-004=2024-02-15 → 2 transactions in February
		require.Equal(t, int64(2), total)
		require.Len(t, results, 2)

		for _, tx := range results {
			require.False(t, tx.Date.Before(dateFrom), "date %s should be >= %s", tx.Date, dateFrom)
			require.False(t, tx.Date.After(dateTo), "date %s should be <= %s", tx.Date, dateTo)
		}
	})
}

func TestSearchTransactions_ByStatus(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seedSearchTransactions(t, h)

		ctx := h.Ctx()
		tRepo := ingestionTxRepo.NewRepository(h.Provider())

		results, total, err := tRepo.SearchTransactions(ctx, h.Seed.ContextID, ingestionRepositories.TransactionSearchParams{
			Status: "UNMATCHED",
		})
		require.NoError(t, err)

		// TX-001, TX-002, TX-004 are UNMATCHED
		require.Equal(t, int64(3), total)
		require.Len(t, results, 3)

		for _, tx := range results {
			require.Equal(t, shared.TransactionStatusUnmatched, tx.Status)
		}
	})
}

func TestSearchTransactions_ByTextQuery(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seedSearchTransactions(t, h)

		ctx := h.Ctx()
		tRepo := ingestionTxRepo.NewRepository(h.Provider())

		results, total, err := tRepo.SearchTransactions(ctx, h.Seed.ContextID, ingestionRepositories.TransactionSearchParams{
			Query: "invoice",
		})
		require.NoError(t, err)

		// TX-001="Payment for invoice 001", TX-003="Payment for invoice 003"
		require.Equal(t, int64(2), total)
		require.Len(t, results, 2)
	})
}

func TestSearchTransactions_Pagination(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seedSearchTransactions(t, h)

		ctx := h.Ctx()
		tRepo := ingestionTxRepo.NewRepository(h.Provider())

		// First page: limit=2, offset=0
		page1, total1, err := tRepo.SearchTransactions(ctx, h.Seed.ContextID, ingestionRepositories.TransactionSearchParams{
			Limit:  2,
			Offset: 0,
		})
		require.NoError(t, err)
		require.Equal(t, int64(5), total1)
		require.Len(t, page1, 2)

		// Second page: limit=2, offset=2
		page2, total2, err := tRepo.SearchTransactions(ctx, h.Seed.ContextID, ingestionRepositories.TransactionSearchParams{
			Limit:  2,
			Offset: 2,
		})
		require.NoError(t, err)
		require.Equal(t, int64(5), total2)
		require.Len(t, page2, 2)

		// Pages must not overlap — every ID on page1 is absent from page2.
		page1IDs := make(map[string]bool, len(page1))
		for _, tx := range page1 {
			page1IDs[tx.ID.String()] = true
		}

		for _, tx := range page2 {
			require.False(t, page1IDs[tx.ID.String()], "page2 transaction %s should not appear in page1", tx.ID)
		}
	})
}

func TestSearchTransactions_NoResults(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seedSearchTransactions(t, h)

		ctx := h.Ctx()
		tRepo := ingestionTxRepo.NewRepository(h.Provider())

		results, total, err := tRepo.SearchTransactions(ctx, h.Seed.ContextID, ingestionRepositories.TransactionSearchParams{
			Currency: "JPY",
		})
		require.NoError(t, err)
		require.Equal(t, int64(0), total)
		require.Empty(t, results)
	})
}

func TestSearchTransactions_CombinedFilters(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seedSearchTransactions(t, h)

		ctx := h.Ctx()
		tRepo := ingestionTxRepo.NewRepository(h.Provider())

		amountMin := decimal.NewFromInt(200)

		results, total, err := tRepo.SearchTransactions(ctx, h.Seed.ContextID, ingestionRepositories.TransactionSearchParams{
			Currency:  "USD",
			AmountMin: &amountMin,
		})
		require.NoError(t, err)

		// USD transactions with amount >= 200:
		//   TX-002=250.00 USD ✓
		//   TX-004=500.00 USD ✓
		//   TX-001=100.50 USD ✗ (below 200)
		require.Equal(t, int64(2), total)
		require.Len(t, results, 2)

		for _, tx := range results {
			require.Equal(t, "USD", tx.Currency)
			require.True(t, tx.Amount.GreaterThanOrEqual(amountMin), "amount %s should be >= %s", tx.Amount, amountMin)
		}
	})
}
