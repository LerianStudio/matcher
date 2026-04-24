//go:build integration

package reporting

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	dashboardRepo "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/dashboard"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/tests/integration"
)

// TestIntegration_Reporting_DashboardTrend_EmptyDatabase verifies that GetTrendMetrics returns a date series
// with all-zero counts when no transactions exist in the queried date range.
func TestIntegration_Reporting_DashboardTrend_EmptyDatabase(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) { //nolint:thelper // test body, not helper
		seed := seedDashboardConfig(t, h)
		ctx := testCtx(t, h)

		repo := dashboardRepo.NewRepository(h.Provider())

		// Use a far-future date range where no data exists.
		filter := entities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2099, 1, 7, 23, 59, 59, 0, time.UTC),
		}

		trend, err := repo.GetTrendMetrics(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, trend)

		// generate_series produces one entry per day in the range (1 Jan to 7 Jan = 7 days).
		require.Len(t, trend.Dates, 7, "should have 7 date entries from generate_series")
		require.Len(t, trend.Ingestion, 7)
		require.Len(t, trend.Matches, 7)
		require.Len(t, trend.Exceptions, 7)
		require.Len(t, trend.MatchRates, 7)

		for i := range 7 {
			require.Equal(t, 0, trend.Ingestion[i], "day %d: ingested should be 0", i)
			require.Equal(t, 0, trend.Matches[i], "day %d: matched should be 0", i)
			require.Equal(t, 0, trend.Exceptions[i], "day %d: exceptions should be 0", i)
			require.InDelta(t, 0.0, trend.MatchRates[i], 0.001, "day %d: match rate should be 0", i)
		}
	})
}

// TestIntegration_Reporting_DashboardTrend_WithData inserts transactions on 3 different days, creates match
// groups and exceptions for subsets, then verifies the trend points contain the correct
// per-day Ingested/Matched/Exceptions counts.
func TestIntegration_Reporting_DashboardTrend_WithData(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) { //nolint:thelper // test body, not helper
		seed := seedDashboardConfig(t, h)
		ctx := testCtx(t, h)

		// Layout: 7-day window starting 2025-08-01.
		// Day 1 (Aug 1): 3 ingested, 2 matched, 1 exception
		// Day 3 (Aug 3): 2 ingested, 0 matched, 0 exceptions
		// Day 5 (Aug 5): 4 ingested, 3 matched, 2 exceptions
		day1 := time.Date(2025, 8, 1, 10, 0, 0, 0, time.UTC)
		day3 := time.Date(2025, 8, 3, 10, 0, 0, 0, time.UTC)
		day5 := time.Date(2025, 8, 5, 10, 0, 0, 0, time.UTC)

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

			// --- Day 1: 3 transactions, 2 matched, 1 exception ---
			day1TxIDs := make([]uuid.UUID, 3)
			for i := range 3 {
				txID := uuid.New()
				day1TxIDs[i] = txID
				amount := decimal.NewFromFloat(100.00 + float64(i)*10)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'UNMATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "D1-"+txID.String()[:8], amount, day1.Add(time.Duration(i)*time.Minute))
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			// Match first 2 transactions on day 1 (match_groups.created_at = day1).
			for i := range 2 {
				groupID := uuid.New()

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO match_groups (id, context_id, run_id, rule_id, confidence, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, 90, 'CONFIRMED', $5, $5)
				`, groupID, seed.ContextID, runID, seed.RuleID, day1)
				if execErr != nil {
					return struct{}{}, execErr
				}

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO match_items (id, match_group_id, transaction_id, allocated_amount, allocated_currency)
					VALUES ($1, $2, $3, $4, 'USD')
				`, uuid.New(), groupID, day1TxIDs[i], decimal.NewFromFloat(100.00+float64(i)*10))
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			// 1 exception on day 1's third transaction.
			_, execErr = tx.ExecContext(ctx, `
				INSERT INTO exceptions (id, transaction_id, severity, status, reason, version, created_at, updated_at)
				VALUES ($1, $2, 'HIGH', 'OPEN', 'day1 exception', 1, $3, $3)
			`, uuid.New(), day1TxIDs[2], day1)
			if execErr != nil {
				return struct{}{}, execErr
			}

			// --- Day 3: 2 transactions, 0 matched, 0 exceptions ---
			for i := range 2 {
				txID := uuid.New()
				amount := decimal.NewFromFloat(200.00 + float64(i)*25)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'UNMATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "D3-"+txID.String()[:8], amount, day3.Add(time.Duration(i)*time.Minute))
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			// --- Day 5: 4 transactions, 3 matched, 2 exceptions ---
			day5TxIDs := make([]uuid.UUID, 4)
			for i := range 4 {
				txID := uuid.New()
				day5TxIDs[i] = txID
				amount := decimal.NewFromFloat(300.00 + float64(i)*15)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'UNMATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "D5-"+txID.String()[:8], amount, day5.Add(time.Duration(i)*time.Minute))
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			// Match first 3 transactions on day 5.
			for i := range 3 {
				groupID := uuid.New()

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO match_groups (id, context_id, run_id, rule_id, confidence, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, 85, 'CONFIRMED', $5, $5)
				`, groupID, seed.ContextID, runID, seed.RuleID, day5)
				if execErr != nil {
					return struct{}{}, execErr
				}

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO match_items (id, match_group_id, transaction_id, allocated_amount, allocated_currency)
					VALUES ($1, $2, $3, $4, 'USD')
				`, uuid.New(), groupID, day5TxIDs[i], decimal.NewFromFloat(300.00+float64(i)*15))
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			// 2 exceptions on day 5 (on the unmatched 4th tx and matched 3rd tx).
			for _, txIdx := range []int{2, 3} {
				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO exceptions (id, transaction_id, severity, status, reason, version, created_at, updated_at)
					VALUES ($1, $2, 'MEDIUM', 'OPEN', 'day5 exception', 1, $3, $3)
				`, uuid.New(), day5TxIDs[txIdx], day5)
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			return struct{}{}, nil
		})
		require.NoError(t, err)

		repo := dashboardRepo.NewRepository(h.Provider())

		filter := entities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 8, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 8, 7, 23, 59, 59, 0, time.UTC),
		}

		trend, err := repo.GetTrendMetrics(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, trend)

		// 7 days: Aug 1-7 inclusive.
		require.Len(t, trend.Dates, 7, "should have 7 date entries")

		// Build a lookup by date string for readable assertions.
		dateIdx := make(map[string]int, len(trend.Dates))
		for i, d := range trend.Dates {
			dateIdx[d] = i
		}

		// Day 1 (Aug 1): 3 ingested, 2 matched, 1 exception.
		idx, ok := dateIdx["2025-08-01"]
		require.True(t, ok, "Aug 1 should be in the series")
		require.Equal(t, 3, trend.Ingestion[idx], "Aug 1 ingested")
		require.Equal(t, 2, trend.Matches[idx], "Aug 1 matched")
		require.Equal(t, 1, trend.Exceptions[idx], "Aug 1 exceptions")

		// Day 2 (Aug 2): empty.
		idx, ok = dateIdx["2025-08-02"]
		require.True(t, ok)
		require.Equal(t, 0, trend.Ingestion[idx], "Aug 2 ingested")
		require.Equal(t, 0, trend.Matches[idx], "Aug 2 matched")

		// Day 3 (Aug 3): 2 ingested, 0 matched, 0 exceptions.
		idx, ok = dateIdx["2025-08-03"]
		require.True(t, ok, "Aug 3 should be in the series")
		require.Equal(t, 2, trend.Ingestion[idx], "Aug 3 ingested")
		require.Equal(t, 0, trend.Matches[idx], "Aug 3 matched")
		require.Equal(t, 0, trend.Exceptions[idx], "Aug 3 exceptions")

		// Day 5 (Aug 5): 4 ingested, 3 matched, 2 exceptions.
		idx, ok = dateIdx["2025-08-05"]
		require.True(t, ok, "Aug 5 should be in the series")
		require.Equal(t, 4, trend.Ingestion[idx], "Aug 5 ingested")
		require.Equal(t, 3, trend.Matches[idx], "Aug 5 matched")
		require.Equal(t, 2, trend.Exceptions[idx], "Aug 5 exceptions")

		// Match rate for day 1: 2/3 ≈ 66.6667% (percentage scale, 0-100).
		idx = dateIdx["2025-08-01"]
		require.InDelta(t, 200.0/3.0, trend.MatchRates[idx], 0.01, "Aug 1 match rate")

		// Match rate for day 5: 3/4 = 75% (percentage scale, 0-100).
		idx = dateIdx["2025-08-05"]
		require.InDelta(t, 75.0, trend.MatchRates[idx], 0.01, "Aug 5 match rate")
	})
}

// TestIntegration_Reporting_DashboardBreakdown_ExceptionsBySeverity inserts 5 exceptions with mixed
// severities and verifies the BySeverity breakdown counts.
func TestIntegration_Reporting_DashboardBreakdown_ExceptionsBySeverity(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) { //nolint:thelper // test body, not helper
		seed := seedDashboardConfig(t, h)
		ctx := testCtx(t, h)

		baseDate := time.Date(2025, 9, 15, 12, 0, 0, 0, time.UTC)

		// Distribution: 2 HIGH, 2 MEDIUM, 1 CRITICAL = 5 total (all OPEN, not RESOLVED).
		severities := []string{"HIGH", "HIGH", "MEDIUM", "MEDIUM", "CRITICAL"}

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			jobID := uuid.New()

			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
				VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
			`, jobID, seed.ContextID, seed.SourceID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			for i, sev := range severities {
				txID := uuid.New()
				amount := decimal.NewFromFloat(100.00 + float64(i)*50)
				txDate := baseDate.Add(time.Duration(i) * time.Minute)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'UNMATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "SEV-"+txID.String()[:8], amount, txDate)
				if execErr != nil {
					return struct{}{}, execErr
				}

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO exceptions (id, transaction_id, severity, status, reason, version, created_at, updated_at)
					VALUES ($1, $2, $3, 'OPEN', 'severity test', 1, $4, $4)
				`, uuid.New(), txID, sev, txDate)
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			return struct{}{}, nil
		})
		require.NoError(t, err)

		repo := dashboardRepo.NewRepository(h.Provider())

		filter := entities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 9, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 9, 30, 23, 59, 59, 0, time.UTC),
		}

		breakdown, err := repo.GetBreakdownMetrics(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, breakdown)

		require.Equal(t, 2, breakdown.BySeverity["HIGH"], "HIGH count")
		require.Equal(t, 2, breakdown.BySeverity["MEDIUM"], "MEDIUM count")
		require.Equal(t, 1, breakdown.BySeverity["CRITICAL"], "CRITICAL count")

		totalBySev := 0
		for _, cnt := range breakdown.BySeverity {
			totalBySev += cnt
		}

		require.Equal(t, 5, totalBySev, "total severity breakdown should equal 5")
	})
}

// TestIntegration_Reporting_DashboardBreakdown_MatchesByRule inserts match groups linked to two different
// rule IDs and verifies ByRule returns the correct count per rule.
func TestIntegration_Reporting_DashboardBreakdown_MatchesByRule(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) { //nolint:thelper // test body, not helper
		seed := seedDashboardConfig(t, h)
		ctx := testCtx(t, h)

		// Create a second match rule so we can distinguish counts per rule.
		secondRuleID := createSecondMatchRule(t, h, seed.ContextID)

		baseDate := time.Date(2025, 10, 1, 8, 0, 0, 0, time.UTC)

		// Plan: 3 match groups on seed.RuleID, 2 match groups on secondRuleID.
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

			type ruleAssignment struct {
				ruleID uuid.UUID
				count  int
			}

			assignments := []ruleAssignment{
				{ruleID: seed.RuleID, count: 3},
				{ruleID: secondRuleID, count: 2},
			}

			for _, assignment := range assignments {
				for i := range assignment.count {
					txID := uuid.New()
					amount := decimal.NewFromFloat(500.00 + float64(i)*75)
					txDate := baseDate.Add(time.Duration(i) * time.Minute)

					_, execErr = tx.ExecContext(ctx, `
						INSERT INTO transactions
							(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
						VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'MATCHED', NOW(), NOW())
					`, txID, jobID, seed.SourceID, "RULE-"+txID.String()[:8], amount, txDate)
					if execErr != nil {
						return struct{}{}, execErr
					}

					groupID := uuid.New()

					_, execErr = tx.ExecContext(ctx, `
						INSERT INTO match_groups (id, context_id, run_id, rule_id, confidence, status, created_at, updated_at)
						VALUES ($1, $2, $3, $4, 92, 'CONFIRMED', $5, $5)
					`, groupID, seed.ContextID, runID, assignment.ruleID, baseDate)
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
			}

			return struct{}{}, nil
		})
		require.NoError(t, err)

		repo := dashboardRepo.NewRepository(h.Provider())

		filter := entities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 10, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 10, 31, 23, 59, 59, 0, time.UTC),
		}

		breakdown, err := repo.GetBreakdownMetrics(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, breakdown)
		require.NotEmpty(t, breakdown.ByRule, "ByRule should have entries")

		// Build a lookup by rule ID for assertions.
		ruleCounts := make(map[uuid.UUID]int, len(breakdown.ByRule))
		for _, rule := range breakdown.ByRule {
			ruleCounts[rule.ID] = rule.Count
		}

		require.Equal(t, 3, ruleCounts[seed.RuleID],
			"seed rule should have 3 confirmed match groups")
		require.Equal(t, 2, ruleCounts[secondRuleID],
			"second rule should have 2 confirmed match groups")
	})
}

// TestIntegration_Reporting_DashboardBreakdown_ExceptionsByAge inserts exceptions with varying created_at
// timestamps to place them in different age buckets (<24h, 1-3d, >3d) and verifies
// the ByAge breakdown has the correct bucket counts.
func TestIntegration_Reporting_DashboardBreakdown_ExceptionsByAge(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) { //nolint:thelper // test body, not helper
		seed := seedDashboardConfig(t, h)
		ctx := testCtx(t, h)

		now := time.Now().UTC()

		// Place exceptions at specific ages relative to NOW().
		// The SQL uses EXTRACT(EPOCH FROM (NOW() - e.created_at)) / 3600:
		//   <24h  → "<24h"
		//   <72h  → "1-3d"
		//   >=72h → ">3d"
		type ageSpec struct {
			offset time.Duration // subtracted from now
			bucket string        // expected bucket
		}

		specs := []ageSpec{
			{offset: 2 * time.Hour, bucket: "<24h"},  // 2h old
			{offset: 10 * time.Hour, bucket: "<24h"}, // 10h old
			{offset: 36 * time.Hour, bucket: "1-3d"}, // 36h old = 1.5 days
			{offset: 60 * time.Hour, bucket: "1-3d"}, // 60h old = 2.5 days
			{offset: 96 * time.Hour, bucket: ">3d"},  // 96h old = 4 days
			{offset: 200 * time.Hour, bucket: ">3d"}, // 200h old ≈ 8.3 days
		}

		// We need the exception created_at within the filter range, and also
		// the transaction date within range. Use a wide filter that covers everything.
		filterStart := now.Add(-300 * time.Hour) // well before the oldest exception
		filterEnd := now.Add(time.Hour)

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			jobID := uuid.New()

			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
				VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
			`, jobID, seed.ContextID, seed.SourceID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			for i, spec := range specs {
				txID := uuid.New()
				amount := decimal.NewFromFloat(100.00 + float64(i)*10)
				createdAt := now.Add(-spec.offset)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'UNMATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "AGE-"+txID.String()[:8], amount, createdAt)
				if execErr != nil {
					return struct{}{}, execErr
				}

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO exceptions (id, transaction_id, severity, status, reason, version, created_at, updated_at)
					VALUES ($1, $2, 'MEDIUM', 'OPEN', 'age test', 1, $3, $3)
				`, uuid.New(), txID, createdAt)
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			return struct{}{}, nil
		})
		require.NoError(t, err)

		repo := dashboardRepo.NewRepository(h.Provider())

		filter := entities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  filterStart,
			DateTo:    filterEnd,
		}

		breakdown, err := repo.GetBreakdownMetrics(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, breakdown)
		require.NotEmpty(t, breakdown.ByAge, "ByAge should have entries")

		// Build bucket counts from the result.
		ageCounts := make(map[string]int, len(breakdown.ByAge))
		for _, ab := range breakdown.ByAge {
			ageCounts[ab.Bucket] = ab.Value
		}

		require.Equal(t, 2, ageCounts["<24h"], "2 exceptions under 24 hours old")
		require.Equal(t, 2, ageCounts["1-3d"], "2 exceptions between 1 and 3 days old")
		require.Equal(t, 2, ageCounts[">3d"], "2 exceptions over 3 days old")

		totalByAge := 0
		for _, cnt := range ageCounts {
			totalByAge += cnt
		}

		require.Equal(t, 6, totalByAge, "total age breakdown should equal 6 exceptions")
	})
}

// createSecondMatchRule creates an additional match rule with priority 2
// under the given context. This lets us test ByRule breakdowns with multiple rules.
func createSecondMatchRule(t *testing.T, h *integration.TestHarness, contextID uuid.UUID) uuid.UUID {
	t.Helper()

	ctx := testCtx(t, h)

	var ruleID uuid.UUID

	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		ruleID = uuid.New()

		_, execErr := tx.ExecContext(ctx, `
			INSERT INTO match_rules (id, context_id, priority, type, config, created_at, updated_at)
			VALUES ($1, $2, 2, 'TOLERANCE', '{"tolerance": 0.01}', NOW(), NOW())
		`, ruleID, contextID)

		return struct{}{}, execErr
	})
	require.NoError(t, err)

	return ruleID
}
