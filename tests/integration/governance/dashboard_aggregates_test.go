//go:build integration

package governance

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	configContextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	configFieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	configMatchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	configSourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	reportingDashboard "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/dashboard"
	reportingEntities "github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	reportingQuery "github.com/LerianStudio/matcher/internal/reporting/services/query"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/tests/integration"
)

type dashboardTestSeed struct {
	ContextID uuid.UUID
	SourceID  uuid.UUID
	RuleID    uuid.UUID
	TenantID  uuid.UUID
}

func seedDashboardTestConfig(t *testing.T, h *integration.TestHarness) dashboardTestSeed {
	t.Helper()

	ctx := h.Ctx()
	ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)
	provider := h.Provider()

	fmRepo := configFieldMapRepo.NewRepository(provider)
	ruleRepo := configMatchRuleRepo.NewRepository(provider)

	mapping := map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "description",
	}

	fm, err := shared.NewFieldMap(
		ctx,
		h.Seed.ContextID,
		h.Seed.SourceID,
		shared.CreateFieldMapInput{Mapping: mapping},
	)
	require.NoError(t, err)
	_, err = fmRepo.Create(ctx, fm)
	require.NoError(t, err)

	rule, err := configEntities.NewMatchRule(
		ctx,
		h.Seed.ContextID,
		configEntities.CreateMatchRuleInput{
			Priority: 1,
			Type:     shared.RuleTypeExact,
			Config:   map[string]any{"matchAmount": true},
		},
	)
	require.NoError(t, err)
	createdRule, err := ruleRepo.Create(ctx, rule)
	require.NoError(t, err)

	return dashboardTestSeed{
		ContextID: h.Seed.ContextID,
		SourceID:  h.Seed.SourceID,
		RuleID:    createdRule.ID,
		TenantID:  h.Seed.TenantID,
	}
}

func seedDashboardData(
	t *testing.T,
	h *integration.TestHarness,
	seed dashboardTestSeed,
	matchedCount, unmatchedCount int,
) {
	t.Helper()

	ctx := h.Ctx()
	ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

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

		baseDate := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

		for i := 0; i < matchedCount; i++ {
			txID := uuid.New()
			amount := decimal.NewFromFloat(500.00 + float64(i)*100)

			_, err := tx.ExecContext(ctx, `
				INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, amount, currency, date, status)
				VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'MATCHED')
			`, txID, jobID, seed.SourceID, "DASHBOARD-MATCHED-"+txID.String()[:8], amount, baseDate.Add(time.Duration(i)*time.Hour))
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

		for i := 0; i < unmatchedCount; i++ {
			txID := uuid.New()
			amount := decimal.NewFromFloat(250.00 + float64(i)*50)

			_, err := tx.ExecContext(ctx, `
				INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, amount, currency, date, status)
				VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'UNMATCHED')
			`, txID, jobID, seed.SourceID, "DASHBOARD-UNMATCHED-"+txID.String()[:8], amount, baseDate.Add(time.Duration(matchedCount+i)*time.Hour))
			if err != nil {
				return struct{}{}, err
			}
		}

		return struct{}{}, nil
	})
	require.NoError(t, err)
}

func seedDashboardDataWithExceptions(
	t *testing.T,
	h *integration.TestHarness,
	seed dashboardTestSeed,
	resolvedOnTime, resolvedLate, pendingWithinSLA, pendingOverdue int,
) {
	t.Helper()

	ctx := h.Ctx()
	ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		jobID := uuid.New()
		_, err := tx.ExecContext(ctx, `
			INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
			VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
		`, jobID, seed.ContextID, seed.SourceID)
		if err != nil {
			return struct{}{}, err
		}

		now := time.Now().UTC()
		baseDate := now.Add(-24 * time.Hour)

		createExceptionTx := func(idx int, status string, dueAt, updatedAt time.Time) error {
			txID := uuid.New()
			amount := decimal.NewFromFloat(100.00 + float64(idx)*10)

			_, err := tx.ExecContext(ctx, `
				INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, amount, currency, date, status)
				VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'PENDING_REVIEW')
			`, txID, jobID, seed.SourceID, "EXCEPTION-"+txID.String()[:8], amount, baseDate)
			if err != nil {
				return err
			}

			_, err = tx.ExecContext(ctx, `
				INSERT INTO exceptions (id, transaction_id, severity, status, due_at, created_at, updated_at)
				VALUES ($1, $2, 'MEDIUM', $3, $4, $5, $6)
			`, uuid.New(), txID, status, dueAt, baseDate, updatedAt)

			return err
		}

		idx := 0

		for i := 0; i < resolvedOnTime; i++ {
			dueAt := now.Add(24 * time.Hour)
			updatedAt := now.Add(-1 * time.Hour)

			if err := createExceptionTx(idx, "RESOLVED", dueAt, updatedAt); err != nil {
				return struct{}{}, err
			}

			idx++
		}

		for i := 0; i < resolvedLate; i++ {
			dueAt := now.Add(-48 * time.Hour)
			updatedAt := now.Add(-1 * time.Hour)

			if err := createExceptionTx(idx, "RESOLVED", dueAt, updatedAt); err != nil {
				return struct{}{}, err
			}

			idx++
		}

		for i := 0; i < pendingWithinSLA; i++ {
			dueAt := now.Add(24 * time.Hour)
			updatedAt := baseDate

			if err := createExceptionTx(idx, "OPEN", dueAt, updatedAt); err != nil {
				return struct{}{}, err
			}

			idx++
		}

		for i := 0; i < pendingOverdue; i++ {
			dueAt := now.Add(-24 * time.Hour)
			updatedAt := baseDate

			if err := createExceptionTx(idx, "OPEN", dueAt, updatedAt); err != nil {
				return struct{}{}, err
			}

			idx++
		}

		return struct{}{}, nil
	})
	require.NoError(t, err)
}

func TestIntegrationDashboardAggregates_VolumeStats(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedDashboardTestConfig(t, h)
		seedDashboardData(t, h, seed, 10, 5)

		repo := reportingDashboard.NewRepository(h.Provider())
		uc, err := reportingQuery.NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		ctx := h.Ctx()
		ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

		filter := reportingEntities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		volume, err := uc.GetVolumeStats(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, volume)
		require.Equal(t, 15, volume.TotalTransactions)
		require.Equal(t, 10, volume.MatchedTransactions)
		require.Equal(t, 5, volume.UnmatchedCount)
		require.True(t, volume.TotalAmount.GreaterThan(decimal.Zero))
		require.True(t, volume.MatchedAmount.GreaterThan(decimal.Zero))
		require.True(t, volume.UnmatchedAmount.GreaterThan(decimal.Zero))
		require.Equal(t, volume.TotalAmount, volume.MatchedAmount.Add(volume.UnmatchedAmount))
	})
}

func TestIntegrationDashboardAggregates_MatchRateStats(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedDashboardTestConfig(t, h)
		seedDashboardData(t, h, seed, 8, 2)

		repo := reportingDashboard.NewRepository(h.Provider())
		uc, err := reportingQuery.NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		ctx := h.Ctx()
		ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

		filter := reportingEntities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		matchRate, err := uc.GetMatchRateStats(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, matchRate)
		require.InDelta(t, 80.0, matchRate.MatchRate, 0.1)
		require.Equal(t, 10, matchRate.TotalCount)
		require.Equal(t, 8, matchRate.MatchedCount)
		require.Equal(t, 2, matchRate.UnmatchedCount)
	})
}

func TestIntegrationDashboardAggregates_SLAStats(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedDashboardTestConfig(t, h)
		seedDashboardDataWithExceptions(t, h, seed, 4, 1, 2, 1)

		repo := reportingDashboard.NewRepository(h.Provider())
		uc, err := reportingQuery.NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		ctx := h.Ctx()
		ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

		filter := reportingEntities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Now().Add(-7 * 24 * time.Hour),
			DateTo:    time.Now().Add(24 * time.Hour),
		}

		slaStats, err := uc.GetSLAStats(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, slaStats)
		require.Equal(t, 8, slaStats.TotalExceptions)
		require.Equal(t, 4, slaStats.ResolvedOnTime)
		require.Equal(t, 1, slaStats.ResolvedLate)
		require.Equal(t, 2, slaStats.PendingWithinSLA)
		require.Equal(t, 1, slaStats.PendingOverdue)
		require.InDelta(t, 66.67, slaStats.SLAComplianceRate, 0.1)
	})
}

func TestIntegrationDashboardAggregates_FullDashboard(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedDashboardTestConfig(t, h)
		seedDashboardData(t, h, seed, 6, 4)

		repo := reportingDashboard.NewRepository(h.Provider())
		uc, err := reportingQuery.NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		ctx := h.Ctx()
		ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

		filter := reportingEntities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		aggregates, err := uc.GetDashboardAggregates(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, aggregates)
		require.NotNil(t, aggregates.Volume)
		require.NotNil(t, aggregates.MatchRate)
		require.NotNil(t, aggregates.SLA)
		require.False(t, aggregates.UpdatedAt.IsZero())

		require.Equal(t, 10, aggregates.Volume.TotalTransactions)
		require.Equal(t, 6, aggregates.Volume.MatchedTransactions)
		require.Equal(t, 4, aggregates.Volume.UnmatchedCount)

		require.InDelta(t, 60.0, aggregates.MatchRate.MatchRate, 0.1)
		require.Equal(t, aggregates.Volume.TotalTransactions, aggregates.MatchRate.TotalCount)
		require.Equal(t, aggregates.Volume.MatchedTransactions, aggregates.MatchRate.MatchedCount)
	})
}

func TestIntegrationDashboardAggregates_EmptyData(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedDashboardTestConfig(t, h)

		repo := reportingDashboard.NewRepository(h.Provider())
		uc, err := reportingQuery.NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		ctx := h.Ctx()
		ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

		filter := reportingEntities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		volume, err := uc.GetVolumeStats(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, 0, volume.TotalTransactions)
		require.Equal(t, 0, volume.MatchedTransactions)
		require.Equal(t, 0, volume.UnmatchedCount)
		require.True(t, volume.TotalAmount.IsZero())

		slaStats, err := uc.GetSLAStats(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, 0, slaStats.TotalExceptions)
		require.InDelta(t, 100.0, slaStats.SLAComplianceRate, 0.1)

		matchRate, err := uc.GetMatchRateStats(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, 0.0, matchRate.MatchRate)
		require.Equal(t, 0, matchRate.TotalCount)
	})
}

func TestIntegrationDashboardAggregates_DateFiltering(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedDashboardTestConfig(t, h)
		seedDashboardData(t, h, seed, 10, 5)

		repo := reportingDashboard.NewRepository(h.Provider())
		uc, err := reportingQuery.NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		ctx := h.Ctx()
		ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

		narrowFilter := reportingEntities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC),
		}

		narrowVolume, err := uc.GetVolumeStats(ctx, narrowFilter)
		require.NoError(t, err)
		require.LessOrEqual(t, narrowVolume.TotalTransactions, 3)

		wideFilter := reportingEntities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		wideVolume, err := uc.GetVolumeStats(ctx, wideFilter)
		require.NoError(t, err)
		require.Equal(t, 15, wideVolume.TotalTransactions)
	})
}

func TestIntegrationDashboardAggregates_SourceFiltering(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedDashboardTestConfig(t, h)
		seedDashboardData(t, h, seed, 5, 3)

		ctx := h.Ctx()
		ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)
		provider := h.Provider()

		srcRepo, err := configSourceRepo.NewRepository(provider)
		require.NoError(t, err)

		ctxRepo := configContextRepo.NewRepository(provider)
		ctxEntity, err := configEntities.NewReconciliationContext(
			ctx,
			seed.TenantID,
			configEntities.CreateReconciliationContextInput{
				Name:     "Dashboard Other Context",
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
		)
		require.NoError(t, err)
		otherCtx, err := ctxRepo.Create(ctx, ctxEntity)
		require.NoError(t, err)

		otherSrc, err := configEntities.NewReconciliationSource(
			ctx,
			otherCtx.ID,
			configEntities.CreateReconciliationSourceInput{
				Name:   "Other Source",
				Type:   configVO.SourceTypeBank,
				Side:   sharedfee.MatchingSideRight,
				Config: map[string]any{},
			},
		)
		require.NoError(t, err)
		createdOtherSrc, err := srcRepo.Create(ctx, otherSrc)
		require.NoError(t, err)

		repo := reportingDashboard.NewRepository(h.Provider())
		uc, err := reportingQuery.NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		filter := reportingEntities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
			SourceID:  &seed.SourceID,
		}

		volume, err := uc.GetVolumeStats(ctx, filter)
		require.NoError(t, err)
		require.Equal(t, 8, volume.TotalTransactions)

		noMatchFilter := reportingEntities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
			SourceID:  &createdOtherSrc.ID,
		}

		noMatchVolume, err := uc.GetVolumeStats(ctx, noMatchFilter)
		require.NoError(t, err)
		require.Equal(t, 0, noMatchVolume.TotalTransactions)
	})
}

func TestIntegrationDashboardAggregates_CountInvariants(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedDashboardTestConfig(t, h)
		seedDashboardData(t, h, seed, 7, 3)

		repo := reportingDashboard.NewRepository(h.Provider())
		uc, err := reportingQuery.NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		ctx := h.Ctx()
		ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

		filter := reportingEntities.DashboardFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		volume, err := uc.GetVolumeStats(ctx, filter)
		require.NoError(t, err)

		require.Equal(t, volume.TotalTransactions, volume.MatchedTransactions+volume.UnmatchedCount)
		require.True(t, volume.TotalAmount.Equal(volume.MatchedAmount.Add(volume.UnmatchedAmount)))

		matchRate := reportingEntities.CalculateMatchRate(volume)
		require.GreaterOrEqual(t, matchRate.MatchRate, 0.0)
		require.LessOrEqual(t, matchRate.MatchRate, 100.0)
		require.GreaterOrEqual(t, matchRate.MatchRateAmount, 0.0)
		require.LessOrEqual(t, matchRate.MatchRateAmount, 100.0)
	})
}
