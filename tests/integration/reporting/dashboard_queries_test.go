//go:build integration

package reporting

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	configFieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	configMatchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	reportingDashboard "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/dashboard"
	reportingReport "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/report"
	reportingEntities "github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	reportingQuery "github.com/LerianStudio/matcher/internal/reporting/services/query"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

// dashboardSeed holds prerequisite IDs needed for reporting queries.
type dashboardSeed struct {
	ContextID uuid.UUID
	SourceID  uuid.UUID
	RuleID    uuid.UUID
	TenantID  uuid.UUID
}

// seedDashboardConfig creates a field map and match rule for the harness context.
// These are required before inserting match groups (rule_id FK).
func seedDashboardConfig(t *testing.T, harness *integration.TestHarness) dashboardSeed {
	t.Helper()

	ctx := testCtx(t, harness)
	provider := harness.Provider()

	fmRepo := configFieldMapRepo.NewRepository(provider)
	ruleRepo := configMatchRuleRepo.NewRepository(provider)

	mapping := map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "description",
	}

	fm, err := configEntities.NewFieldMap(
		ctx,
		harness.Seed.ContextID,
		harness.Seed.SourceID,
		configEntities.CreateFieldMapInput{Mapping: mapping},
	)
	require.NoError(t, err)

	_, err = fmRepo.Create(ctx, fm)
	require.NoError(t, err)

	rule, err := configEntities.NewMatchRule(
		ctx,
		harness.Seed.ContextID,
		configEntities.CreateMatchRuleInput{
			Priority: 1,
			Type:     shared.RuleTypeExact,
			Config:   map[string]any{"matchAmount": true},
		},
	)
	require.NoError(t, err)

	createdRule, err := ruleRepo.Create(ctx, rule)
	require.NoError(t, err)

	return dashboardSeed{
		ContextID: harness.Seed.ContextID,
		SourceID:  harness.Seed.SourceID,
		RuleID:    createdRule.ID,
		TenantID:  harness.Seed.TenantID,
	}
}

// seedTransactionsAndMatches inserts an ingestion job, a match run,
// matchedCount transactions with CONFIRMED match groups, and
// unmatchedCount transactions with UNMATCHED status.
// baseDate controls the transaction date; all dates are offset by index hours.
func seedTransactionsAndMatches(
	t *testing.T,
	harness *integration.TestHarness,
	seed dashboardSeed,
	matchedCount, unmatchedCount int,
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

		for i := range matchedCount {
			txID := uuid.New()
			amount := decimal.NewFromFloat(500.00 + float64(i)*100)

			_, err = tx.ExecContext(ctx, `
				INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, amount, currency, date, status)
				VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'MATCHED')
			`, txID, jobID, seed.SourceID, "RPT-MATCHED-"+txID.String()[:8], amount, baseDate.Add(time.Duration(i)*time.Hour))
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

		for i := range unmatchedCount {
			txID := uuid.New()
			amount := decimal.NewFromFloat(250.00 + float64(i)*50)

			_, err = tx.ExecContext(ctx, `
				INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, amount, currency, date, status)
				VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'UNMATCHED')
			`, txID, jobID, seed.SourceID, "RPT-UNMATCHED-"+txID.String()[:8], amount, baseDate.Add(time.Duration(matchedCount+i)*time.Hour))
			if err != nil {
				return struct{}{}, err
			}
		}

		return struct{}{}, nil
	})
	require.NoError(t, err)
}

// TestDashboard_VolumeStats verifies volume aggregation over seeded transactions.
func TestDashboard_VolumeStats(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedDashboardConfig(t, h)
		baseDate := time.Date(2025, 3, 10, 12, 0, 0, 0, time.UTC)
		seedTransactionsAndMatches(t, h, seed, 5, 3, baseDate)

		dashRepo := reportingDashboard.NewRepository(h.Provider())
		uc, err := reportingQuery.NewDashboardUseCase(dashRepo, nil)
		require.NoError(t, err)

		ctx := testCtx(t, h)
		filter := reportingEntities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		volume, err := uc.GetVolumeStats(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, volume)

		// 5 matched + 3 unmatched = 8 total.
		require.Equal(t, 8, volume.TotalTransactions, "total should equal matched + unmatched")
		require.Equal(t, 5, volume.MatchedTransactions, "matched transactions count")
		require.Equal(t, 3, volume.UnmatchedCount, "unmatched transactions count")

		// Amounts must be positive and add up.
		require.True(t, volume.TotalAmount.GreaterThan(decimal.Zero))
		require.True(t, volume.MatchedAmount.GreaterThan(decimal.Zero))
		require.True(t, volume.UnmatchedAmount.GreaterThan(decimal.Zero))
		require.True(t,
			volume.TotalAmount.Equal(volume.MatchedAmount.Add(volume.UnmatchedAmount)),
			"total = matched + unmatched amount",
		)
	})
}

// TestDashboard_MatchRateStats verifies the computed match rate percentage.
func TestDashboard_MatchRateStats(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedDashboardConfig(t, h)
		baseDate := time.Date(2025, 4, 1, 10, 0, 0, 0, time.UTC)
		// 6 matched, 4 unmatched -> 60% match rate.
		seedTransactionsAndMatches(t, h, seed, 6, 4, baseDate)

		dashRepo := reportingDashboard.NewRepository(h.Provider())
		uc, err := reportingQuery.NewDashboardUseCase(dashRepo, nil)
		require.NoError(t, err)

		ctx := testCtx(t, h)
		filter := reportingEntities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		matchRate, err := uc.GetMatchRateStats(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, matchRate)

		require.Equal(t, 10, matchRate.TotalCount)
		require.Equal(t, 6, matchRate.MatchedCount)
		require.Equal(t, 4, matchRate.UnmatchedCount)
		// CalculateMatchRate returns a percentage (0-100).
		require.InDelta(t, 60.0, matchRate.MatchRate, 0.1, "60%% of 10 transactions matched")
	})
}

// TestDashboard_EmptyContext verifies that querying a context with no data returns zero values.
func TestDashboard_EmptyContext(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		// Only config -- no transactions seeded.
		seed := seedDashboardConfig(t, h)

		dashRepo := reportingDashboard.NewRepository(h.Provider())
		uc, err := reportingQuery.NewDashboardUseCase(dashRepo, nil)
		require.NoError(t, err)

		ctx := testCtx(t, h)
		filter := reportingEntities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		volume, err := uc.GetVolumeStats(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, volume)
		require.Equal(t, 0, volume.TotalTransactions)
		require.Equal(t, 0, volume.MatchedTransactions)
		require.Equal(t, 0, volume.UnmatchedCount)
		require.True(t, volume.TotalAmount.IsZero())
		require.True(t, volume.MatchedAmount.IsZero())
		require.True(t, volume.UnmatchedAmount.IsZero())

		matchRate, err := uc.GetMatchRateStats(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, matchRate)
		require.InDelta(t, 0.0, matchRate.MatchRate, 0.001, "empty context returns 0%% match rate")
		require.Equal(t, 0, matchRate.TotalCount)

		sla, err := uc.GetSLAStats(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, sla)
		require.Equal(t, 0, sla.TotalExceptions)
	})
}

// TestReport_CountMatched verifies counting match_items in CONFIRMED groups.
func TestReport_CountMatched(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedDashboardConfig(t, h)
		baseDate := time.Date(2025, 5, 1, 8, 0, 0, 0, time.UTC)
		// 7 matched, 2 unmatched.
		seedTransactionsAndMatches(t, h, seed, 7, 2, baseDate)

		rptRepo := reportingReport.NewRepository(h.Provider())

		ctx := testCtx(t, h)
		filter := reportingEntities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		count, err := rptRepo.CountMatched(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(7), count, "should count 7 match_items in CONFIRMED groups")
	})
}

// TestReport_CountUnmatched verifies counting transactions with status != MATCHED.
func TestReport_CountUnmatched(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedDashboardConfig(t, h)
		baseDate := time.Date(2025, 6, 1, 9, 0, 0, 0, time.UTC)
		// 3 matched, 5 unmatched.
		seedTransactionsAndMatches(t, h, seed, 3, 5, baseDate)

		rptRepo := reportingReport.NewRepository(h.Provider())

		ctx := testCtx(t, h)
		filter := reportingEntities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		count, err := rptRepo.CountUnmatched(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, int64(5), count, "should count 5 transactions with status != MATCHED")
	})
}

// TestReport_CountWithDateFilter verifies that date-range filtering scopes counts correctly.
func TestReport_CountWithDateFilter(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedDashboardConfig(t, h)

		ctx := testCtx(t, h)

		// Seed batch 1: 4 transactions on March 10, 2025.
		marchDate := time.Date(2025, 3, 10, 12, 0, 0, 0, time.UTC)
		seedTransactionsAndMatches(t, h, seed, 2, 2, marchDate)

		// Seed batch 2: 6 transactions on July 15, 2025.
		julyDate := time.Date(2025, 7, 15, 12, 0, 0, 0, time.UTC)
		seedTransactionsAndMatches(t, h, seed, 3, 3, julyDate)

		rptRepo := reportingReport.NewRepository(h.Provider())

		// Filter: only March (should get 4 = 2 matched + 2 unmatched).
		marchFilter := reportingEntities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 3, 31, 23, 59, 59, 0, time.UTC),
		}

		marchTotal, err := rptRepo.CountTransactions(ctx, marchFilter)
		require.NoError(t, err)
		require.Equal(t, int64(4), marchTotal, "March window: 2 matched + 2 unmatched = 4")

		// Filter: only July (should get 6 = 3 matched + 3 unmatched).
		julyFilter := reportingEntities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 7, 31, 23, 59, 59, 0, time.UTC),
		}

		julyTotal, err := rptRepo.CountTransactions(ctx, julyFilter)
		require.NoError(t, err)
		require.Equal(t, int64(6), julyTotal, "July window: 3 matched + 3 unmatched = 6")

		// Filter: full year (should get all 10).
		fullYearFilter := reportingEntities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		fullTotal, err := rptRepo.CountTransactions(ctx, fullYearFilter)
		require.NoError(t, err)
		require.Equal(t, int64(10), fullTotal, "full year: 5 matched + 5 unmatched = 10")

		// Filter: narrow window that misses all data returns zero.
		emptyFilter := reportingEntities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 1, 31, 23, 59, 59, 0, time.UTC),
		}

		emptyTotal, err := rptRepo.CountTransactions(ctx, emptyFilter)
		require.NoError(t, err)
		require.Equal(t, int64(0), emptyTotal, "January window: no data seeded returns 0")
	})
}
