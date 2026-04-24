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

// newReportRepo creates a report Repository from the test harness.
func newReportRepo(h *integration.TestHarness) *reportRepo.Repository {
	return reportRepo.NewRepository(h.Provider())
}

// insertMatchedTransactions inserts count matched transactions with CONFIRMED
// match groups and match items into the database within a single tenant
// transaction. It requires a pre-seeded dashboardSeed (context, source, rule).
func insertMatchedTransactions(
	t *testing.T,
	harness *integration.TestHarness,
	seed dashboardSeed,
	count int,
	baseDate time.Time,
) {
	t.Helper()

	ctx := testCtx(t, harness)

	_, err := pgcommon.WithTenantTx(ctx, harness.Connection, func(tx *sql.Tx) (struct{}, error) {
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

		for i := range count {
			txID := uuid.New()
			amount := decimal.NewFromFloat(100.00 + float64(i)*10)

			_, err = tx.ExecContext(ctx, `
				INSERT INTO transactions
					(id, ingestion_job_id, source_id, external_id, amount, currency, date, status)
				VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'MATCHED')
			`, txID, jobID, seed.SourceID,
				"EXP-M-"+txID.String()[:8],
				amount,
				baseDate.Add(time.Duration(i)*time.Hour),
			)
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
			`, uuid.New(), groupID, txID, amount)
			if err != nil {
				return struct{}{}, err
			}
		}

		return struct{}{}, nil
	})
	require.NoError(t, err)
}

// insertUnmatchedTransactions inserts count transactions with UNMATCHED status
// into the database within a single tenant transaction.
func insertUnmatchedTransactions(
	t *testing.T,
	harness *integration.TestHarness,
	seed dashboardSeed,
	count int,
	baseDate time.Time,
) {
	t.Helper()

	ctx := testCtx(t, harness)

	_, err := pgcommon.WithTenantTx(ctx, harness.Connection, func(tx *sql.Tx) (struct{}, error) {
		jobID := uuid.New()

		_, err := tx.ExecContext(ctx, `
			INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
			VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
		`, jobID, seed.ContextID, seed.SourceID)
		if err != nil {
			return struct{}{}, err
		}

		for i := range count {
			txID := uuid.New()
			amount := decimal.NewFromFloat(50.00 + float64(i)*5)

			_, err = tx.ExecContext(ctx, `
				INSERT INTO transactions
					(id, ingestion_job_id, source_id, external_id, amount, currency, date, status)
				VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'UNMATCHED')
			`, txID, jobID, seed.SourceID,
				"EXP-U-"+txID.String()[:8],
				amount,
				baseDate.Add(time.Duration(i)*time.Hour),
			)
			if err != nil {
				return struct{}{}, err
			}
		}

		return struct{}{}, nil
	})
	require.NoError(t, err)
}

// wideReportFilter returns a ReportFilter covering a broad date window around
// the given base date, scoped to the supplied context.
func wideReportFilter(contextID uuid.UUID) entities.ReportFilter {
	return entities.ReportFilter{
		ContextID: contextID,
		DateFrom:  time.Now().UTC().AddDate(0, 0, -30),
		DateTo:    time.Now().UTC().AddDate(0, 0, 1),
	}
}

// TestIntegration_Reporting_ExportLimit_ListMatchedForExport_Empty verifies that querying matched
// export data on a context with no matched transactions returns an empty slice.
func TestIntegration_Reporting_ExportLimit_ListMatchedForExport_Empty(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := newReportRepo(h)

		filter := wideReportFilter(h.Seed.ContextID)

		items, err := repo.ListMatchedForExport(ctx, filter, 10)
		require.NoError(t, err)
		require.Empty(t, items, "empty context should return zero matched export items")
	})
}

// TestIntegration_Reporting_ExportLimit_ListMatchedForExport_BelowLimit verifies that when the
// result set is smaller than maxRecords, all rows are returned without
// triggering overflow detection.
func TestIntegration_Reporting_ExportLimit_ListMatchedForExport_BelowLimit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedDashboardConfig(t, h)
		baseDate := time.Now().UTC().AddDate(0, 0, -5)

		const insertCount = 3
		insertMatchedTransactions(t, h, seed, insertCount, baseDate)

		ctx := testCtx(t, h)
		repo := newReportRepo(h)

		filter := wideReportFilter(seed.ContextID)

		// maxRecords (10) > insertCount (3): no overflow.
		items, err := repo.ListMatchedForExport(ctx, filter, 10)
		require.NoError(t, err)
		require.Len(t, items, insertCount,
			"should return exactly %d items when below limit", insertCount)

		// Verify each item has non-zero fields.
		for _, item := range items {
			require.NotEqual(t, uuid.Nil, item.TransactionID)
			require.NotEqual(t, uuid.Nil, item.MatchGroupID)
			require.NotEqual(t, uuid.Nil, item.SourceID)
			require.True(t, item.Amount.GreaterThan(decimal.Zero))
			require.Equal(t, "USD", item.Currency)
			require.False(t, item.Date.IsZero())
		}
	})
}

// TestIntegration_Reporting_ExportLimit_ListUnmatchedForExport_BelowLimit verifies that unmatched
// export returns all rows when the count is below maxRecords.
func TestIntegration_Reporting_ExportLimit_ListUnmatchedForExport_BelowLimit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedDashboardConfig(t, h)
		baseDate := time.Now().UTC().AddDate(0, 0, -5)

		const insertCount = 5
		insertUnmatchedTransactions(t, h, seed, insertCount, baseDate)

		ctx := testCtx(t, h)
		repo := newReportRepo(h)

		filter := wideReportFilter(seed.ContextID)

		items, err := repo.ListUnmatchedForExport(ctx, filter, 10)
		require.NoError(t, err)
		require.Len(t, items, insertCount,
			"should return exactly %d unmatched items when below limit", insertCount)

		for _, item := range items {
			require.NotEqual(t, uuid.Nil, item.TransactionID)
			require.NotEqual(t, uuid.Nil, item.SourceID)
			require.True(t, item.Amount.GreaterThan(decimal.Zero))
			require.Equal(t, "USD", item.Currency)
			require.Equal(t, "UNMATCHED", item.Status)
			require.False(t, item.Date.IsZero())
		}
	})
}

// TestIntegration_Reporting_ExportLimit_ListMatchedForExport_AtLimit verifies that when the result
// set has exactly N rows and maxRecords=N, all N rows are returned. The query
// uses LIMIT N+1 internally (safeExportLimit), so N rows means no overflow
// row exists—exactly N results come back.
func TestIntegration_Reporting_ExportLimit_ListMatchedForExport_AtLimit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedDashboardConfig(t, h)
		baseDate := time.Now().UTC().AddDate(0, 0, -5)

		const maxRecords = 4
		insertMatchedTransactions(t, h, seed, maxRecords, baseDate)

		ctx := testCtx(t, h)
		repo := newReportRepo(h)

		filter := wideReportFilter(seed.ContextID)

		// maxRecords with exactly that many rows in DB: query does LIMIT maxRecords+1,
		// gets maxRecords rows back. len(items) <= maxRecords so no overflow error.
		items, err := repo.ListMatchedForExport(ctx, filter, maxRecords)
		require.NoError(t, err)
		require.Len(t, items, maxRecords,
			"should return exactly %d items when DB has exactly %d rows", maxRecords, maxRecords)
	})
}

// TestIntegration_Reporting_ExportLimit_ListUnmatchedForExport_OverflowDetection verifies the
// LIMIT+1 overflow detection pattern. When the database holds N+1 rows and
// the caller asks for maxRecords=N, the internal query uses LIMIT N+1 and
// returns N+1 rows. The repository's defensive guard detects
// len(items) > maxRecords and returns ErrExportLimitExceeded.
func TestIntegration_Reporting_ExportLimit_ListUnmatchedForExport_OverflowDetection(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedDashboardConfig(t, h)
		baseDate := time.Now().UTC().AddDate(0, 0, -5)

		const maxRecords = 3
		// Insert maxRecords+1 rows to trigger the overflow guard.
		insertUnmatchedTransactions(t, h, seed, maxRecords+1, baseDate)

		ctx := testCtx(t, h)
		repo := newReportRepo(h)

		filter := wideReportFilter(seed.ContextID)

		// With maxRecords+1 rows in DB: query does LIMIT maxRecords+1, gets
		// maxRecords+1 rows. The defensive guard fires:
		// len(items) > maxRecords → ErrExportLimitExceeded.
		_, err := repo.ListUnmatchedForExport(ctx, filter, maxRecords)
		require.Error(t, err, "should error when result set overflows maxRecords")
		require.ErrorIs(t, err, reportRepo.ErrExportLimitExceeded,
			"error should wrap ErrExportLimitExceeded")
	})
}
