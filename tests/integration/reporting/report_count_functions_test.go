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

// TestIntegration_Reporting_ReportCountFunctions_EmptyDatabase verifies that all four count functions
// return zero when no transactions exist in the filtered date range.
// Uses a date window far from seed data to guarantee isolation.
func TestIntegration_Reporting_ReportCountFunctions_EmptyDatabase(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		// Use a date range in 2099 — no seed data will ever fall in this window.
		filter := entities.ReportFilter{
			ContextID: h.Seed.ContextID,
			DateFrom:  time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2099, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		matchedCount, err := repo.CountMatched(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(0), matchedCount, "CountMatched should be 0 in empty date range")

		unmatchedCount, err := repo.CountUnmatched(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(0), unmatchedCount, "CountUnmatched should be 0 in empty date range")

		txCount, err := repo.CountTransactions(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(0), txCount, "CountTransactions should be 0 in empty date range")

		exceptionCount, err := repo.CountExceptions(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(0), exceptionCount, "CountExceptions should be 0 in empty date range")
	})
}

// TestIntegration_Reporting_ReportCountFunctions_CountTransactions inserts transactions via SQL within
// a tenant transaction, then verifies CountTransactions returns the exact count.
func TestIntegration_Reporting_ReportCountFunctions_CountTransactions(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		baseDate := time.Date(2025, 8, 15, 10, 0, 0, 0, time.UTC)
		const txCount = 5

		// Insert an ingestion job and 5 transactions.
		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			jobID := uuid.New()

			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
				VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
			`, jobID, h.Seed.ContextID, h.Seed.SourceID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			for i := range txCount {
				txID := uuid.New()
				amount := decimal.NewFromFloat(100.00 + float64(i)*25)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'UNMATCHED', NOW(), NOW())
				`, txID, jobID, h.Seed.SourceID, "COUNT-TX-"+txID.String()[:8], amount, baseDate.Add(time.Duration(i)*time.Hour))
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			return struct{}{}, nil
		})
		require.NoError(t, err)

		filter := entities.ReportFilter{
			ContextID: h.Seed.ContextID,
			DateFrom:  time.Date(2025, 8, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 8, 31, 23, 59, 59, 0, time.UTC),
		}

		count, err := repo.CountTransactions(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(txCount), count, "CountTransactions should return exact inserted count")
	})
}

// TestIntegration_Reporting_ReportCountFunctions_CountMatched inserts transactions linked to CONFIRMED
// match groups through match_items, then verifies CountMatched returns the correct count.
func TestIntegration_Reporting_ReportCountFunctions_CountMatched(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		// Seed a field map and match rule (required FK for match_groups.rule_id).
		seed := seedDashboardConfig(t, h)

		baseDate := time.Date(2025, 9, 10, 12, 0, 0, 0, time.UTC)
		const matchedCount = 4

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

			for i := range matchedCount {
				txID := uuid.New()
				amount := decimal.NewFromFloat(300.00 + float64(i)*50)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'MATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "MATCHED-"+txID.String()[:8], amount, baseDate.Add(time.Duration(i)*time.Hour))
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

			return struct{}{}, nil
		})
		require.NoError(t, err)

		filter := entities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 9, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 9, 30, 23, 59, 59, 0, time.UTC),
		}

		count, err := repo.CountMatched(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(matchedCount), count, "CountMatched should return count of match_items in CONFIRMED groups")
	})
}

// TestIntegration_Reporting_ReportCountFunctions_CountUnmatched inserts transactions with status UNMATCHED
// and verifies CountUnmatched correctly counts them while excluding MATCHED ones.
func TestIntegration_Reporting_ReportCountFunctions_CountUnmatched(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		baseDate := time.Date(2025, 10, 5, 8, 0, 0, 0, time.UTC)
		const unmatchedCount = 6
		const matchedCount = 3

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			jobID := uuid.New()

			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
				VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
			`, jobID, h.Seed.ContextID, h.Seed.SourceID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			// Insert unmatched transactions (status != 'MATCHED').
			for i := range unmatchedCount {
				txID := uuid.New()
				amount := decimal.NewFromFloat(200.00 + float64(i)*30)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'UNMATCHED', NOW(), NOW())
				`, txID, jobID, h.Seed.SourceID, "UNMATCHED-"+txID.String()[:8], amount, baseDate.Add(time.Duration(i)*time.Hour))
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			// Insert matched transactions — these should be excluded from CountUnmatched.
			for i := range matchedCount {
				txID := uuid.New()
				amount := decimal.NewFromFloat(500.00 + float64(i)*100)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'MATCHED', NOW(), NOW())
				`, txID, jobID, h.Seed.SourceID, "SHOULDSKIP-"+txID.String()[:8], amount, baseDate.Add(time.Duration(unmatchedCount+i)*time.Hour))
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			return struct{}{}, nil
		})
		require.NoError(t, err)

		filter := entities.ReportFilter{
			ContextID: h.Seed.ContextID,
			DateFrom:  time.Date(2025, 10, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 10, 31, 23, 59, 59, 0, time.UTC),
		}

		count, err := repo.CountUnmatched(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(unmatchedCount), count, "CountUnmatched should exclude MATCHED transactions")

		// Cross-check: CountTransactions should include all of them.
		totalCount, err := repo.CountTransactions(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(unmatchedCount+matchedCount), totalCount, "CountTransactions should include both matched and unmatched")
	})
}

// TestIntegration_Reporting_ReportCountFunctions_CountExceptions inserts transactions with linked exceptions,
// then verifies CountExceptions returns the exception count.
func TestIntegration_Reporting_ReportCountFunctions_CountExceptions(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		baseDate := time.Date(2025, 11, 1, 14, 0, 0, 0, time.UTC)
		const exceptionCount = 3
		const noExceptionCount = 2

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			jobID := uuid.New()

			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
				VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
			`, jobID, h.Seed.ContextID, h.Seed.SourceID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			// Insert transactions that will have exceptions.
			for i := range exceptionCount {
				txID := uuid.New()
				amount := decimal.NewFromFloat(150.00 + float64(i)*20)
				txDate := baseDate.Add(time.Duration(i) * time.Hour)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'UNMATCHED', NOW(), NOW())
				`, txID, jobID, h.Seed.SourceID, "EXCEPTION-"+txID.String()[:8], amount, txDate)
				if execErr != nil {
					return struct{}{}, execErr
				}

				// CountExceptions filters on e.created_at, so set created_at within the test range.
				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO exceptions (id, transaction_id, severity, status, reason, version, created_at, updated_at)
					VALUES ($1, $2, 'HIGH', 'OPEN', 'test exception', 1, $3, $4)
				`, uuid.New(), txID, txDate, txDate)
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			// Insert transactions WITHOUT exceptions — these should not affect CountExceptions.
			for i := range noExceptionCount {
				txID := uuid.New()
				amount := decimal.NewFromFloat(400.00 + float64(i)*50)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'UNMATCHED', NOW(), NOW())
				`, txID, jobID, h.Seed.SourceID, "NOEXCEPTION-"+txID.String()[:8], amount, baseDate.Add(time.Duration(exceptionCount+i)*time.Hour))
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			return struct{}{}, nil
		})
		require.NoError(t, err)

		filter := entities.ReportFilter{
			ContextID: h.Seed.ContextID,
			DateFrom:  time.Date(2025, 11, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 11, 30, 23, 59, 59, 0, time.UTC),
		}

		count, err := repo.CountExceptions(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(exceptionCount), count, "CountExceptions should count only transactions with linked exceptions")
	})
}

// TestIntegration_Reporting_ReportCountFunctions_DateRangeFilter inserts transactions across different
// months and verifies that all count functions respect DateFrom/DateTo boundaries.
func TestIntegration_Reporting_ReportCountFunctions_DateRangeFilter(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		// Seed config (needed for match_groups FK).
		seed := seedDashboardConfig(t, h)

		// Batch 1: February 2025 — 3 unmatched, 2 matched, 1 exception.
		febDate := time.Date(2025, 2, 15, 10, 0, 0, 0, time.UTC)
		// Batch 2: June 2025 — 2 unmatched, 3 matched, 2 exceptions.
		junDate := time.Date(2025, 6, 20, 10, 0, 0, 0, time.UTC)

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

			// --- Batch 1: February ---
			// 3 unmatched transactions.
			for i := range 3 {
				txID := uuid.New()
				amount := decimal.NewFromFloat(100.00 + float64(i)*10)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'UNMATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "FEB-UM-"+txID.String()[:8], amount, febDate.Add(time.Duration(i)*time.Hour))
				if execErr != nil {
					return struct{}{}, execErr
				}

				// Create 1 exception on the first unmatched transaction.
				if i == 0 {
					_, execErr = tx.ExecContext(ctx, `
						INSERT INTO exceptions (id, transaction_id, severity, status, reason, version, created_at, updated_at)
						VALUES ($1, $2, 'HIGH', 'OPEN', 'feb exception', 1, $3, $4)
					`, uuid.New(), txID, febDate, febDate)
					if execErr != nil {
						return struct{}{}, execErr
					}
				}
			}

			// 2 matched transactions.
			for i := range 2 {
				txID := uuid.New()
				amount := decimal.NewFromFloat(500.00 + float64(i)*100)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'MATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "FEB-M-"+txID.String()[:8], amount, febDate.Add(time.Duration(3+i)*time.Hour))
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

			// --- Batch 2: June ---
			// 2 unmatched transactions.
			for i := range 2 {
				txID := uuid.New()
				amount := decimal.NewFromFloat(200.00 + float64(i)*20)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'UNMATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "JUN-UM-"+txID.String()[:8], amount, junDate.Add(time.Duration(i)*time.Hour))
				if execErr != nil {
					return struct{}{}, execErr
				}

				// Create exceptions on both unmatched June transactions.
				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO exceptions (id, transaction_id, severity, status, reason, version, created_at, updated_at)
					VALUES ($1, $2, 'HIGH', 'OPEN', 'jun exception', 1, $3, $4)
				`, uuid.New(), txID, junDate.Add(time.Duration(i)*time.Hour), junDate.Add(time.Duration(i)*time.Hour))
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			// 3 matched transactions.
			for i := range 3 {
				txID := uuid.New()
				amount := decimal.NewFromFloat(700.00 + float64(i)*50)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'MATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "JUN-M-"+txID.String()[:8], amount, junDate.Add(time.Duration(2+i)*time.Hour))
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

			return struct{}{}, nil
		})
		require.NoError(t, err)

		// --- February-only filter ---
		febFilter := entities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 2, 28, 23, 59, 59, 0, time.UTC),
		}

		febTxCount, err := repo.CountTransactions(ctx, febFilter)
		require.NoError(t, err)
		require.Equal(t, int64(5), febTxCount, "February: 3 unmatched + 2 matched = 5 transactions")

		febMatchedCount, err := repo.CountMatched(ctx, febFilter)
		require.NoError(t, err)
		require.Equal(t, int64(2), febMatchedCount, "February: 2 matched transactions")

		febUnmatchedCount, err := repo.CountUnmatched(ctx, febFilter)
		require.NoError(t, err)
		require.Equal(t, int64(3), febUnmatchedCount, "February: 3 unmatched transactions")

		febExceptionCount, err := repo.CountExceptions(ctx, febFilter)
		require.NoError(t, err)
		require.Equal(t, int64(1), febExceptionCount, "February: 1 exception")

		// --- June-only filter ---
		junFilter := entities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 6, 30, 23, 59, 59, 0, time.UTC),
		}

		junTxCount, err := repo.CountTransactions(ctx, junFilter)
		require.NoError(t, err)
		require.Equal(t, int64(5), junTxCount, "June: 2 unmatched + 3 matched = 5 transactions")

		junMatchedCount, err := repo.CountMatched(ctx, junFilter)
		require.NoError(t, err)
		require.Equal(t, int64(3), junMatchedCount, "June: 3 matched transactions")

		junUnmatchedCount, err := repo.CountUnmatched(ctx, junFilter)
		require.NoError(t, err)
		require.Equal(t, int64(2), junUnmatchedCount, "June: 2 unmatched transactions")

		junExceptionCount, err := repo.CountExceptions(ctx, junFilter)
		require.NoError(t, err)
		require.Equal(t, int64(2), junExceptionCount, "June: 2 exceptions")

		// --- Full year filter: both batches ---
		fullFilter := entities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		fullTxCount, err := repo.CountTransactions(ctx, fullFilter)
		require.NoError(t, err)
		require.Equal(t, int64(10), fullTxCount, "Full year: 5+5 = 10 total transactions")

		fullMatchedCount, err := repo.CountMatched(ctx, fullFilter)
		require.NoError(t, err)
		require.Equal(t, int64(5), fullMatchedCount, "Full year: 2+3 = 5 matched transactions")

		fullUnmatchedCount, err := repo.CountUnmatched(ctx, fullFilter)
		require.NoError(t, err)
		require.Equal(t, int64(5), fullUnmatchedCount, "Full year: 3+2 = 5 unmatched transactions")

		fullExceptionCount, err := repo.CountExceptions(ctx, fullFilter)
		require.NoError(t, err)
		require.Equal(t, int64(3), fullExceptionCount, "Full year: 1+2 = 3 exceptions")

		// --- Empty window: no data in April ---
		aprilFilter := entities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 4, 30, 23, 59, 59, 0, time.UTC),
		}

		aprilTxCount, err := repo.CountTransactions(ctx, aprilFilter)
		require.NoError(t, err)
		require.Equal(t, int64(0), aprilTxCount, "April window: no data returns 0")

		aprilMatchedCount, err := repo.CountMatched(ctx, aprilFilter)
		require.NoError(t, err)
		require.Equal(t, int64(0), aprilMatchedCount, "April window: no matched returns 0")

		aprilExceptionCount, err := repo.CountExceptions(ctx, aprilFilter)
		require.NoError(t, err)
		require.Equal(t, int64(0), aprilExceptionCount, "April window: no exceptions returns 0")
	})
}
