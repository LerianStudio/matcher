//go:build integration

package reporting

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	reportRepo "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/report"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/tests/integration"
)

// TestGetSummary_EmptyDatabase verifies that GetSummary returns zero counts and
// zero amounts when no transactions exist in the queried date range.
func TestGetSummary_EmptyDatabase(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		filter := entities.ReportFilter{
			ContextID: h.Seed.ContextID,
			DateFrom:  time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2099, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		summary, err := repo.GetSummary(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, summary)

		require.Equal(t, 0, summary.MatchedCount, "MatchedCount should be 0")
		require.Equal(t, 0, summary.UnmatchedCount, "UnmatchedCount should be 0")
		require.True(t, decimal.Zero.Equal(summary.MatchedAmount), "MatchedAmount should be zero")
		require.True(t, decimal.Zero.Equal(summary.UnmatchedAmount), "UnmatchedAmount should be zero")
		require.True(t, decimal.Zero.Equal(summary.TotalAmount), "TotalAmount should be zero")
	})
}

// TestGetSummary_OnlyUnmatched inserts 3 UNMATCHED transactions ($100, $200, $300)
// and verifies the summary reflects only unmatched counts and amounts.
func TestGetSummary_OnlyUnmatched(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		baseDate := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
		amounts := []float64{100.00, 200.00, 300.00}

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			jobID := uuid.New()

			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
				VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
			`, jobID, h.Seed.ContextID, h.Seed.SourceID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			for i, amt := range amounts {
				txID := uuid.New()

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'UNMATCHED', NOW(), NOW())
				`, txID, jobID, h.Seed.SourceID, "SUMM-UM-"+txID.String()[:8], decimal.NewFromFloat(amt), baseDate.Add(time.Duration(i)*time.Hour))
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			return struct{}{}, nil
		})
		require.NoError(t, err)

		filter := entities.ReportFilter{
			ContextID: h.Seed.ContextID,
			DateFrom:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 1, 31, 23, 59, 59, 0, time.UTC),
		}

		summary, err := repo.GetSummary(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, summary)

		require.Equal(t, 0, summary.MatchedCount, "MatchedCount should be 0")
		require.Equal(t, 3, summary.UnmatchedCount, "UnmatchedCount should be 3")
		require.True(t, decimal.Zero.Equal(summary.MatchedAmount), "MatchedAmount should be zero")
		require.True(t, decimal.NewFromFloat(600.00).Equal(summary.UnmatchedAmount),
			"UnmatchedAmount should be 600 (100+200+300)")
		require.True(t, decimal.NewFromFloat(600.00).Equal(summary.TotalAmount),
			"TotalAmount should equal UnmatchedAmount when no matches exist")
	})
}

// TestGetSummary_OnlyMatched inserts 2 matched transactions ($150, $250) with
// CONFIRMED match groups and verifies the summary reflects only matched data.
func TestGetSummary_OnlyMatched(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		seed := seedDashboardConfig(t, h)
		baseDate := time.Date(2025, 2, 10, 8, 0, 0, 0, time.UTC)
		amounts := []float64{150.00, 250.00}

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			jobID := uuid.New()

			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
				VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
			`, jobID, seed.ContextID, seed.SourceID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			runID := uuid.New()

			_, execErr = tx.ExecContext(ctx, `
				INSERT INTO match_runs (id, context_id, mode, status, started_at, stats)
				VALUES ($1, $2, 'COMMIT', 'COMPLETED', NOW(), '{}')
			`, runID, seed.ContextID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			for i, amt := range amounts {
				txID := uuid.New()
				amount := decimal.NewFromFloat(amt)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'MATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "SUMM-M-"+txID.String()[:8], amount, baseDate.Add(time.Duration(i)*time.Hour))
				if execErr != nil {
					return struct{}{}, execErr
				}

				groupID := uuid.New()

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO match_groups (id, context_id, run_id, rule_id, confidence, status)
					VALUES ($1, $2, $3, $4, 95, 'CONFIRMED')
				`, groupID, seed.ContextID, runID, seed.RuleID)
				if execErr != nil {
					return struct{}{}, execErr
				}

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO match_items (id, match_group_id, transaction_id, allocated_amount, allocated_currency)
					VALUES ($1, $2, $3, $4, 'USD')
				`, uuid.New(), groupID, txID, amount)
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			return struct{}{}, nil
		})
		require.NoError(t, err)

		filter := entities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 2, 28, 23, 59, 59, 0, time.UTC),
		}

		summary, err := repo.GetSummary(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, summary)

		require.Equal(t, 2, summary.MatchedCount, "MatchedCount should be 2")
		require.Equal(t, 0, summary.UnmatchedCount, "UnmatchedCount should be 0")
		require.True(t, decimal.NewFromFloat(400.00).Equal(summary.MatchedAmount),
			"MatchedAmount should be 400 (150+250)")
		require.True(t, decimal.Zero.Equal(summary.UnmatchedAmount), "UnmatchedAmount should be zero")
		require.True(t, decimal.NewFromFloat(400.00).Equal(summary.TotalAmount),
			"TotalAmount should equal MatchedAmount when no unmatched exist")
	})
}

// TestGetSummary_MixedMatchedAndUnmatched inserts 3 matched ($100, $200, $300)
// and 2 unmatched ($50, $75) transactions, then verifies all summary fields.
func TestGetSummary_MixedMatchedAndUnmatched(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		seed := seedDashboardConfig(t, h)
		baseDate := time.Date(2025, 3, 5, 12, 0, 0, 0, time.UTC)
		matchedAmounts := []float64{100.00, 200.00, 300.00}
		unmatchedAmounts := []float64{50.00, 75.00}

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			jobID := uuid.New()

			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
				VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
			`, jobID, seed.ContextID, seed.SourceID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			runID := uuid.New()

			_, execErr = tx.ExecContext(ctx, `
				INSERT INTO match_runs (id, context_id, mode, status, started_at, stats)
				VALUES ($1, $2, 'COMMIT', 'COMPLETED', NOW(), '{}')
			`, runID, seed.ContextID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			// Insert matched transactions with CONFIRMED groups.
			for i, amt := range matchedAmounts {
				txID := uuid.New()
				amount := decimal.NewFromFloat(amt)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'MATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "MIX-M-"+txID.String()[:8], amount, baseDate.Add(time.Duration(i)*time.Hour))
				if execErr != nil {
					return struct{}{}, execErr
				}

				groupID := uuid.New()

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO match_groups (id, context_id, run_id, rule_id, confidence, status)
					VALUES ($1, $2, $3, $4, 90, 'CONFIRMED')
				`, groupID, seed.ContextID, runID, seed.RuleID)
				if execErr != nil {
					return struct{}{}, execErr
				}

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO match_items (id, match_group_id, transaction_id, allocated_amount, allocated_currency)
					VALUES ($1, $2, $3, $4, 'USD')
				`, uuid.New(), groupID, txID, amount)
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			// Insert unmatched transactions.
			for i, amt := range unmatchedAmounts {
				txID := uuid.New()
				amount := decimal.NewFromFloat(amt)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'UNMATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "MIX-UM-"+txID.String()[:8], amount, baseDate.Add(time.Duration(len(matchedAmounts)+i)*time.Hour))
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			return struct{}{}, nil
		})
		require.NoError(t, err)

		filter := entities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 3, 31, 23, 59, 59, 0, time.UTC),
		}

		summary, err := repo.GetSummary(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, summary)

		require.Equal(t, 3, summary.MatchedCount, "MatchedCount should be 3")
		require.Equal(t, 2, summary.UnmatchedCount, "UnmatchedCount should be 2")

		expectedMatched := decimal.NewFromFloat(600.00)
		require.True(t, expectedMatched.Equal(summary.MatchedAmount),
			"MatchedAmount should be 600 (100+200+300), got %s", summary.MatchedAmount)

		expectedUnmatched := decimal.NewFromFloat(125.00)
		require.True(t, expectedUnmatched.Equal(summary.UnmatchedAmount),
			"UnmatchedAmount should be 125 (50+75), got %s", summary.UnmatchedAmount)

		expectedTotal := decimal.NewFromFloat(725.00)
		require.True(t, expectedTotal.Equal(summary.TotalAmount),
			"TotalAmount should be 725 (600+125), got %s", summary.TotalAmount)

		// Cross-check: TotalAmount must equal MatchedAmount + UnmatchedAmount.
		require.True(t,
			summary.TotalAmount.Equal(summary.MatchedAmount.Add(summary.UnmatchedAmount)),
			"TotalAmount must equal MatchedAmount + UnmatchedAmount",
		)
	})
}

// TestGetSummary_DateRangeExcludesOutOfRange inserts transactions at different
// dates and verifies that only in-range ones are counted by GetSummary.
func TestGetSummary_DateRangeExcludesOutOfRange(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		seed := seedDashboardConfig(t, h)

		// In-range: April 2025 — 2 matched ($500, $700), 1 unmatched ($300).
		aprilDate := time.Date(2025, 4, 10, 14, 0, 0, 0, time.UTC)
		// Out-of-range: September 2025 — 1 matched ($1000), 2 unmatched ($200, $400).
		septDate := time.Date(2025, 9, 20, 10, 0, 0, 0, time.UTC)

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			jobID := uuid.New()

			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
				VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
			`, jobID, seed.ContextID, seed.SourceID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			runID := uuid.New()

			_, execErr = tx.ExecContext(ctx, `
				INSERT INTO match_runs (id, context_id, mode, status, started_at, stats)
				VALUES ($1, $2, 'COMMIT', 'COMPLETED', NOW(), '{}')
			`, runID, seed.ContextID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			// --- April (in-range): 2 matched ---
			aprilMatchedAmounts := []float64{500.00, 700.00}
			for i, amt := range aprilMatchedAmounts {
				txID := uuid.New()
				amount := decimal.NewFromFloat(amt)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'MATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "APR-M-"+txID.String()[:8], amount, aprilDate.Add(time.Duration(i)*time.Hour))
				if execErr != nil {
					return struct{}{}, execErr
				}

				groupID := uuid.New()

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO match_groups (id, context_id, run_id, rule_id, confidence, status)
					VALUES ($1, $2, $3, $4, 95, 'CONFIRMED')
				`, groupID, seed.ContextID, runID, seed.RuleID)
				if execErr != nil {
					return struct{}{}, execErr
				}

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO match_items (id, match_group_id, transaction_id, allocated_amount, allocated_currency)
					VALUES ($1, $2, $3, $4, 'USD')
				`, uuid.New(), groupID, txID, amount)
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			// --- April (in-range): 1 unmatched ---
			{
				txID := uuid.New()
				amount := decimal.NewFromFloat(300.00)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'UNMATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "APR-UM-"+txID.String()[:8], amount, aprilDate.Add(3*time.Hour))
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			// --- September (out-of-range): 1 matched ---
			{
				txID := uuid.New()
				amount := decimal.NewFromFloat(1000.00)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'MATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "SEP-M-"+txID.String()[:8], amount, septDate)
				if execErr != nil {
					return struct{}{}, execErr
				}

				groupID := uuid.New()

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO match_groups (id, context_id, run_id, rule_id, confidence, status)
					VALUES ($1, $2, $3, $4, 88, 'CONFIRMED')
				`, groupID, seed.ContextID, runID, seed.RuleID)
				if execErr != nil {
					return struct{}{}, execErr
				}

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO match_items (id, match_group_id, transaction_id, allocated_amount, allocated_currency)
					VALUES ($1, $2, $3, $4, 'USD')
				`, uuid.New(), groupID, txID, amount)
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			// --- September (out-of-range): 2 unmatched ---
			septUnmatchedAmounts := []float64{200.00, 400.00}
			for i, amt := range septUnmatchedAmounts {
				txID := uuid.New()
				amount := decimal.NewFromFloat(amt)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'UNMATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "SEP-UM-"+txID.String()[:8], amount, septDate.Add(time.Duration(i+1)*time.Hour))
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			return struct{}{}, nil
		})
		require.NoError(t, err)

		// Filter: April only — should see only the April transactions.
		aprilFilter := entities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 4, 30, 23, 59, 59, 0, time.UTC),
		}

		summary, err := repo.GetSummary(ctx, aprilFilter)
		require.NoError(t, err)
		require.NotNil(t, summary)

		require.Equal(t, 2, summary.MatchedCount, "April: MatchedCount should be 2")
		require.Equal(t, 1, summary.UnmatchedCount, "April: UnmatchedCount should be 1")

		require.True(t, decimal.NewFromFloat(1200.00).Equal(summary.MatchedAmount),
			"April: MatchedAmount should be 1200 (500+700), got %s", summary.MatchedAmount)
		require.True(t, decimal.NewFromFloat(300.00).Equal(summary.UnmatchedAmount),
			"April: UnmatchedAmount should be 300, got %s", summary.UnmatchedAmount)
		require.True(t, decimal.NewFromFloat(1500.00).Equal(summary.TotalAmount),
			"April: TotalAmount should be 1500 (1200+300), got %s", summary.TotalAmount)

		// Filter: September only — should see only the September transactions.
		septFilter := entities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 9, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 9, 30, 23, 59, 59, 0, time.UTC),
		}

		septSummary, err := repo.GetSummary(ctx, septFilter)
		require.NoError(t, err)
		require.NotNil(t, septSummary)

		require.Equal(t, 1, septSummary.MatchedCount, "September: MatchedCount should be 1")
		require.Equal(t, 2, septSummary.UnmatchedCount, "September: UnmatchedCount should be 2")

		require.True(t, decimal.NewFromFloat(1000.00).Equal(septSummary.MatchedAmount),
			"September: MatchedAmount should be 1000, got %s", septSummary.MatchedAmount)
		require.True(t, decimal.NewFromFloat(600.00).Equal(septSummary.UnmatchedAmount),
			"September: UnmatchedAmount should be 600 (200+400), got %s", septSummary.UnmatchedAmount)
		require.True(t, decimal.NewFromFloat(1600.00).Equal(septSummary.TotalAmount),
			"September: TotalAmount should be 1600 (1000+600), got %s", septSummary.TotalAmount)

		// Filter: empty window (January) — should see nothing.
		emptyFilter := entities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 1, 31, 23, 59, 59, 0, time.UTC),
		}

		emptySummary, err := repo.GetSummary(ctx, emptyFilter)
		require.NoError(t, err)
		require.NotNil(t, emptySummary)

		require.Equal(t, 0, emptySummary.MatchedCount, "January: MatchedCount should be 0")
		require.Equal(t, 0, emptySummary.UnmatchedCount, "January: UnmatchedCount should be 0")
		require.True(t, decimal.Zero.Equal(emptySummary.TotalAmount),
			"January: TotalAmount should be zero")
	})
}
