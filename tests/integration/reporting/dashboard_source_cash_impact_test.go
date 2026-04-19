//go:build integration

package reporting

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	reportingDashboard "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/dashboard"
	reportingEntities "github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/tests/integration"
)

// seedSourceBreakdownData inserts an ingestion job, a match run,
// matchedCount transactions with CONFIRMED match groups, and
// unmatchedCount transactions with UNMATCHED status — all under the given sourceID.
// Each matched transaction gets amount 500 + i*100, each unmatched gets 250 + i*50.
func seedSourceBreakdownData(
	t *testing.T,
	h *integration.TestHarness,
	seed dashboardSeed,
	sourceID uuid.UUID,
	matchedCount, unmatchedCount int,
	baseDate time.Time,
	currency string,
) {
	t.Helper()

	ctx := testCtx(t, h)

	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		jobID := uuid.New()

		_, err := tx.ExecContext(ctx, `
			INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
			VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
		`, jobID, seed.ContextID, sourceID)
		if err != nil {
			return struct{}{}, err
		}

		runID := uuid.New()

		_, err = tx.ExecContext(ctx, `
			INSERT INTO match_runs (id, context_id, mode, status, started_at, stats)
			VALUES ($1, $2, 'COMMIT', 'COMPLETED', NOW(), '{}')
		`, runID, seed.ContextID)
		if err != nil {
			return struct{}{}, err
		}

		for i := range matchedCount {
			txID := uuid.New()
			amount := decimal.NewFromFloat(500.00 + float64(i)*100)

			_, err = tx.ExecContext(ctx, `
				INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, amount, currency, date, status)
				VALUES ($1, $2, $3, $4, $5, $6, $7, 'MATCHED')
			`, txID, jobID, sourceID, "SB-MATCHED-"+txID.String()[:8], amount, currency, baseDate.Add(time.Duration(i)*time.Hour))
			if err != nil {
				return struct{}{}, err
			}

			groupID := uuid.New()

			_, err = tx.ExecContext(ctx, `
				INSERT INTO match_groups (id, context_id, run_id, rule_id, confidence, status)
				VALUES ($1, $2, $3, $4, 95, 'CONFIRMED')
			`, groupID, seed.ContextID, runID, seed.RuleID)
			if err != nil {
				return struct{}{}, err
			}

			_, err = tx.ExecContext(ctx, `
				INSERT INTO match_items (id, match_group_id, transaction_id, allocated_amount, allocated_currency)
				VALUES ($1, $2, $3, $4, $5)
			`, uuid.New(), groupID, txID, amount, currency)
			if err != nil {
				return struct{}{}, err
			}
		}

		for i := range unmatchedCount {
			txID := uuid.New()
			amount := decimal.NewFromFloat(250.00 + float64(i)*50)

			_, err = tx.ExecContext(ctx, `
				INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, amount, currency, date, status)
				VALUES ($1, $2, $3, $4, $5, $6, $7, 'UNMATCHED')
			`, txID, jobID, sourceID, "SB-UNMATCHED-"+txID.String()[:8], amount, currency, baseDate.Add(time.Duration(matchedCount+i)*time.Hour))
			if err != nil {
				return struct{}{}, err
			}
		}

		return struct{}{}, nil
	})
	require.NoError(t, err)
}

// TestSourceBreakdown_EmptyDatabase verifies that querying a context with no
// transactions returns an empty (non-nil) slice.
func TestSourceBreakdown_EmptyDatabase(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) { //nolint:thelper // test body
		seed := seedDashboardConfig(t, h)

		dashRepo := reportingDashboard.NewRepository(h.Provider())

		ctx := testCtx(t, h)
		filter := reportingEntities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		result, err := dashRepo.GetSourceBreakdown(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, result, "result must be non-nil even when empty")
		require.Empty(t, result, "no sources → empty slice")
	})
}

// TestSourceBreakdown_SingleSource inserts 5 transactions on one source, matches 3
// of them, and verifies per-source aggregation: totals, matched/unmatched counts,
// amounts, and match rate.
func TestSourceBreakdown_SingleSource(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) { //nolint:thelper // test body
		seed := seedDashboardConfig(t, h)
		baseDate := time.Date(2025, 5, 10, 12, 0, 0, 0, time.UTC)

		// 3 matched (amounts: 500, 600, 700 = 1800) + 2 unmatched (amounts: 250, 300 = 550)
		seedSourceBreakdownData(t, h, seed, seed.SourceID, 3, 2, baseDate, "USD")

		dashRepo := reportingDashboard.NewRepository(h.Provider())

		ctx := testCtx(t, h)
		filter := reportingEntities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		result, err := dashRepo.GetSourceBreakdown(ctx, filter)
		require.NoError(t, err)
		require.Len(t, result, 1, "exactly one source")

		sb := result[0]
		require.Equal(t, seed.SourceID, sb.SourceID)
		require.Equal(t, int64(5), sb.TotalTxns, "3 matched + 2 unmatched = 5")
		require.Equal(t, int64(3), sb.MatchedTxns)
		require.Equal(t, int64(2), sb.UnmatchedTxns)

		// Match rate: 3/5 = 60% (percentage scale, 0-100)
		require.InDelta(t, 60.0, sb.MatchRate, 0.01, "60%% match rate")

		// Amounts: matched = 500+600+700 = 1800, unmatched = 250+300 = 550, total = 2350
		expectedMatched := decimal.NewFromFloat(1800.0)
		expectedUnmatched := decimal.NewFromFloat(550.0)
		expectedTotal := decimal.NewFromFloat(2350.0)

		require.True(t, sb.TotalAmount.Equal(expectedTotal),
			"total amount: want %s got %s", expectedTotal, sb.TotalAmount)
		require.True(t, sb.UnmatchedAmount.Equal(expectedUnmatched),
			"unmatched amount: want %s got %s", expectedUnmatched, sb.UnmatchedAmount)

		// Verify matched amount via subtraction (TotalAmount - UnmatchedAmount = MatchedAmount)
		derivedMatched := sb.TotalAmount.Sub(sb.UnmatchedAmount)
		require.True(t, derivedMatched.Equal(expectedMatched),
			"derived matched amount: want %s got %s", expectedMatched, derivedMatched)

		require.Equal(t, "USD", sb.Currency)
	})
}

// TestSourceBreakdown_MultipleSourceComparison inserts transactions on two sources
// with different match rates and verifies both appear with correct per-source stats.
func TestSourceBreakdown_MultipleSourceComparison(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) { //nolint:thelper // test body
		seed := seedDashboardConfig(t, h)
		baseDate := time.Date(2025, 6, 1, 10, 0, 0, 0, time.UTC)
		now := time.Now().UTC()

		// Create a second source under the same context.
		source2ID := uuid.New()

		ctx := testCtx(t, h)

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			_, err := tx.ExecContext(ctx,
				`INSERT INTO reconciliation_sources (id, context_id, name, type, side, config, created_at, updated_at)
				 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
				source2ID.String(), seed.ContextID.String(), "Bank Source", "BANK", "RIGHT", "{}", now, now)

			return struct{}{}, err
		})
		require.NoError(t, err)

		// Source 1: 4 matched, 1 unmatched → 80% match rate
		seedSourceBreakdownData(t, h, seed, seed.SourceID, 4, 1, baseDate, "USD")

		// Source 2: 1 matched, 3 unmatched → 25% match rate
		seedSourceBreakdownData(t, h, seed, source2ID, 1, 3, baseDate, "USD")

		dashRepo := reportingDashboard.NewRepository(h.Provider())
		filter := reportingEntities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		result, err := dashRepo.GetSourceBreakdown(ctx, filter)
		require.NoError(t, err)
		require.Len(t, result, 2, "two sources should appear")

		// Results are ordered by total_txns DESC, so source1 (5 txns) first, source2 (4 txns) second.
		byID := make(map[uuid.UUID]reportingEntities.SourceBreakdown)
		for _, sb := range result {
			byID[sb.SourceID] = sb
		}

		// Source 1: 4 matched + 1 unmatched = 5
		s1 := byID[seed.SourceID]
		require.Equal(t, int64(5), s1.TotalTxns)
		require.Equal(t, int64(4), s1.MatchedTxns)
		require.Equal(t, int64(1), s1.UnmatchedTxns)
		require.InDelta(t, 80.0, s1.MatchRate, 0.01, "source1: 80%% match rate")

		// Source 2: 1 matched + 3 unmatched = 4
		s2 := byID[source2ID]
		require.Equal(t, int64(4), s2.TotalTxns)
		require.Equal(t, int64(1), s2.MatchedTxns)
		require.Equal(t, int64(3), s2.UnmatchedTxns)
		require.InDelta(t, 25.0, s2.MatchRate, 0.01, "source2: 25%% match rate")

		// Both should report positive amounts.
		require.True(t, s1.TotalAmount.GreaterThan(decimal.Zero), "source1 total > 0")
		require.True(t, s2.TotalAmount.GreaterThan(decimal.Zero), "source2 total > 0")
	})
}

// seedCashImpactCurrencyData inserts unmatched USD and EUR transactions plus one
// matched USD transaction (excluded from cash impact via anti-join).
func seedCashImpactCurrencyData(
	t *testing.T,
	h *integration.TestHarness,
	seed dashboardSeed,
	baseDate time.Time,
) {
	t.Helper()

	ctx := testCtx(t, h)

	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		jobID := uuid.New()

		_, err := tx.ExecContext(ctx, `
			INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
			VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
		`, jobID, seed.ContextID, seed.SourceID)
		if err != nil {
			return struct{}{}, err
		}

		runID := uuid.New()

		_, err = tx.ExecContext(ctx, `
			INSERT INTO match_runs (id, context_id, mode, status, started_at, stats)
			VALUES ($1, $2, 'COMMIT', 'COMPLETED', NOW(), '{}')
		`, runID, seed.ContextID)
		if err != nil {
			return struct{}{}, err
		}

		// 4 unmatched USD transactions: 250 each = $1000 total.
		for i := range 4 {
			txID := uuid.New()

			_, err = tx.ExecContext(ctx, `
				INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, amount, currency, date, status)
				VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'UNMATCHED')
			`, txID, jobID, seed.SourceID, "CI-USD-"+txID.String()[:8],
				decimal.NewFromFloat(250.0), baseDate.Add(time.Duration(i)*time.Hour))
			if err != nil {
				return struct{}{}, err
			}
		}

		// 2 unmatched EUR transactions: 250 each = €500 total.
		for i := range 2 {
			txID := uuid.New()

			_, err = tx.ExecContext(ctx, `
				INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, amount, currency, date, status)
				VALUES ($1, $2, $3, $4, $5, 'EUR', $6, 'UNMATCHED')
			`, txID, jobID, seed.SourceID, "CI-EUR-"+txID.String()[:8],
				decimal.NewFromFloat(250.0), baseDate.Add(time.Duration(4+i)*time.Hour))
			if err != nil {
				return struct{}{}, err
			}
		}

		// 1 matched USD transaction (should be EXCLUDED from cash impact).
		matchedTxID := uuid.New()

		_, err = tx.ExecContext(ctx, `
			INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, amount, currency, date, status)
			VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'MATCHED')
		`, matchedTxID, jobID, seed.SourceID, "CI-MATCHED-"+matchedTxID.String()[:8],
			decimal.NewFromFloat(999.0), baseDate.Add(6*time.Hour))
		if err != nil {
			return struct{}{}, err
		}

		groupID := uuid.New()

		_, err = tx.ExecContext(ctx, `
			INSERT INTO match_groups (id, context_id, run_id, rule_id, confidence, status)
			VALUES ($1, $2, $3, $4, 95, 'CONFIRMED')
		`, groupID, seed.ContextID, runID, seed.RuleID)
		if err != nil {
			return struct{}{}, err
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO match_items (id, match_group_id, transaction_id, allocated_amount, allocated_currency)
			VALUES ($1, $2, $3, $4, 'USD')
		`, uuid.New(), groupID, matchedTxID, decimal.NewFromFloat(999.0))
		if err != nil {
			return struct{}{}, err
		}

		return struct{}{}, nil
	})
	require.NoError(t, err)
}

// TestCashImpactSummary_ByCurrency inserts unmatched transactions in two currencies
// (USD and EUR) and verifies the ByCurrency breakdown has correct amounts and counts.
// Cash impact only counts transactions NOT in any CONFIRMED match group.
func TestCashImpactSummary_ByCurrency(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) { //nolint:thelper // test body
		seed := seedDashboardConfig(t, h)
		baseDate := time.Date(2025, 7, 1, 12, 0, 0, 0, time.UTC)

		ctx := testCtx(t, h)

		seedCashImpactCurrencyData(t, h, seed, baseDate)

		dashRepo := reportingDashboard.NewRepository(h.Provider())
		filter := reportingEntities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		summary, err := dashRepo.GetCashImpactSummary(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, summary)

		// ByCurrency should contain USD and EUR (matched txn excluded by anti-join).
		require.Len(t, summary.ByCurrency, 2, "two currencies with unmatched exposure")

		byCurrency := make(map[string]reportingEntities.CurrencyExposure)
		for _, ce := range summary.ByCurrency {
			byCurrency[ce.Currency] = ce
		}

		// USD: 4 unmatched × $250 = $1000
		usd, ok := byCurrency["USD"]
		require.True(t, ok, "USD must appear in currency breakdown")
		require.Equal(t, int64(4), usd.TransactionCount, "4 unmatched USD transactions")
		require.True(t, usd.Amount.Equal(decimal.NewFromFloat(1000.0)),
			"USD amount: want 1000, got %s", usd.Amount)

		// EUR: 2 unmatched × €250 = €500
		eur, ok := byCurrency["EUR"]
		require.True(t, ok, "EUR must appear in currency breakdown")
		require.Equal(t, int64(2), eur.TransactionCount, "2 unmatched EUR transactions")
		require.True(t, eur.Amount.Equal(decimal.NewFromFloat(500.0)),
			"EUR amount: want 500, got %s", eur.Amount)

		// TotalUnmatchedAmount = $1000 + €500 = 1500 (cross-currency sum)
		expectedTotal := decimal.NewFromFloat(1500.0)
		require.True(t, summary.TotalUnmatchedAmount.Equal(expectedTotal),
			"total unmatched: want %s, got %s", expectedTotal, summary.TotalUnmatchedAmount)
	})
}

// TestCashImpactSummary_ByAge inserts unmatched transactions at different ages
// and verifies the ByAge buckets contain correct counts and amounts.
// The query uses EXTRACT(EPOCH FROM (NOW() - t.date)) / 3600 for age in hours.
func TestCashImpactSummary_ByAge(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) { //nolint:thelper // test body
		seed := seedDashboardConfig(t, h)

		ctx := testCtx(t, h)
		now := time.Now().UTC()

		// Create transactions at known ages relative to NOW:
		//   - 6 hours ago    → "0-24h" bucket  (amount: 100)
		//   - 2 days ago     → "1-3d"  bucket  (amount: 200)
		//   - 5 days ago     → "3-7d"  bucket  (amount: 300)
		//   - 15 days ago    → "7-30d" bucket  (amount: 400)
		type ageTxn struct {
			date   time.Time
			amount decimal.Decimal
			bucket string
		}

		txns := []ageTxn{
			{date: now.Add(-6 * time.Hour), amount: decimal.NewFromFloat(100.0), bucket: "0-24h"},
			{date: now.Add(-48 * time.Hour), amount: decimal.NewFromFloat(200.0), bucket: "1-3d"},
			{date: now.Add(-120 * time.Hour), amount: decimal.NewFromFloat(300.0), bucket: "3-7d"},
			{date: now.Add(-360 * time.Hour), amount: decimal.NewFromFloat(400.0), bucket: "7-30d"},
		}

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			jobID := uuid.New()

			_, err := tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
				VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
			`, jobID, seed.ContextID, seed.SourceID)
			if err != nil {
				return struct{}{}, err
			}

			for _, txn := range txns {
				txID := uuid.New()

				_, err = tx.ExecContext(ctx, `
					INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, amount, currency, date, status)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'UNMATCHED')
				`, txID, jobID, seed.SourceID, "AGE-"+txID.String()[:8], txn.amount, txn.date)
				if err != nil {
					return struct{}{}, err
				}
			}

			return struct{}{}, nil
		})
		require.NoError(t, err)

		dashRepo := reportingDashboard.NewRepository(h.Provider())

		// Use a wide date range that encompasses all transaction dates.
		filter := reportingEntities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  now.Add(-720 * time.Hour), // 30 days back
			DateTo:    now.Add(time.Hour),
		}

		summary, err := dashRepo.GetCashImpactSummary(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, summary)

		// ByAge should have 4 buckets (one per age range that has data).
		require.Len(t, summary.ByAge, 4, "4 age buckets with data")

		byBucket := make(map[string]reportingEntities.AgeExposure)
		for _, ae := range summary.ByAge {
			byBucket[ae.Bucket] = ae
		}

		// Verify each bucket has 1 transaction with the correct amount.
		for _, expected := range txns {
			ae, ok := byBucket[expected.bucket]
			require.True(t, ok, "bucket %q must exist", expected.bucket)
			require.Equal(t, int64(1), ae.TransactionCount,
				"bucket %q: want 1 txn, got %d", expected.bucket, ae.TransactionCount)
			require.True(t, ae.Amount.Equal(expected.amount),
				"bucket %q: want amount %s, got %s", expected.bucket, expected.amount, ae.Amount)
		}

		// Total unmatched = 100 + 200 + 300 + 400 = 1000
		expectedTotal := decimal.NewFromFloat(1000.0)
		require.True(t, summary.TotalUnmatchedAmount.Equal(expectedTotal),
			"total unmatched: want %s, got %s", expectedTotal, summary.TotalUnmatchedAmount)
	})
}
