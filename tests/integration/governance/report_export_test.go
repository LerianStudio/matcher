//go:build integration

package governance

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	configFieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	configMatchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	reportingPostgres "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/report"
	reportingEntities "github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	reportingQuery "github.com/LerianStudio/matcher/internal/reporting/services/query"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

type reportTestSeed struct {
	ContextID uuid.UUID
	SourceID  uuid.UUID
	RuleID    uuid.UUID
	TenantID  uuid.UUID
}

func seedReportTestConfig(t *testing.T, h *integration.TestHarness) reportTestSeed {
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

	return reportTestSeed{
		ContextID: h.Seed.ContextID,
		SourceID:  h.Seed.SourceID,
		RuleID:    createdRule.ID,
		TenantID:  h.Seed.TenantID,
	}
}

func seedTransactionsWithMatches(
	t *testing.T,
	h *integration.TestHarness,
	seed reportTestSeed,
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

		baseDate := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)

		for i := 0; i < matchedCount; i++ {
			txID := uuid.New()
			amount := decimal.NewFromFloat(100.50 + float64(i))

			_, err := tx.ExecContext(ctx, `
				INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, amount, currency, date, status)
				VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'MATCHED')
			`, txID, jobID, seed.SourceID, "TX-MATCHED-"+txID.String()[:8], amount, baseDate.Add(time.Duration(i)*time.Hour))
			if err != nil {
				return struct{}{}, err
			}

			groupID := uuid.New()
			_, err = tx.ExecContext(ctx, `
				INSERT INTO match_groups (id, context_id, run_id, rule_id, confidence, status)
				VALUES ($1, $2, $3, $4, 100, 'CONFIRMED')
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
			amount := decimal.NewFromFloat(200.75 + float64(i))

			_, err := tx.ExecContext(ctx, `
				INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, amount, currency, date, status)
				VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'UNMATCHED')
			`, txID, jobID, seed.SourceID, "TX-UNMATCHED-"+txID.String()[:8], amount, baseDate.Add(time.Duration(matchedCount+i)*time.Hour))
			if err != nil {
				return struct{}{}, err
			}
		}

		return struct{}{}, nil
	})
	require.NoError(t, err)
}

func TestIntegrationReportExports_MatchedCSV(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedReportTestConfig(t, h)
		seedTransactionsWithMatches(t, h, seed, 3, 0)

		repo := reportingPostgres.NewRepository(h.Provider())
		uc, err := reportingQuery.NewUseCase(repo)
		require.NoError(t, err)

		ctx := h.Ctx()
		ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

		filter := reportingEntities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
			Limit:     100,
		}

		csvData, err := uc.ExportMatchedCSV(ctx, filter)
		require.NoError(t, err)
		require.NotEmpty(t, csvData)

		csvStr := string(csvData)
		require.Contains(t, csvStr, "transaction_id,match_group_id,source_id,amount,currency,date")

		lines := strings.Split(strings.TrimSpace(csvStr), "\n")
		require.Len(t, lines, 4)
	})
}

func TestIntegrationReportExports_UnmatchedCSV(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedReportTestConfig(t, h)
		seedTransactionsWithMatches(t, h, seed, 0, 5)

		repo := reportingPostgres.NewRepository(h.Provider())
		uc, err := reportingQuery.NewUseCase(repo)
		require.NoError(t, err)

		ctx := h.Ctx()
		ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

		filter := reportingEntities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
			Limit:     100,
		}

		csvData, err := uc.ExportUnmatchedCSV(ctx, filter)
		require.NoError(t, err)
		require.NotEmpty(t, csvData)

		csvStr := string(csvData)
		require.Contains(
			t,
			csvStr,
			"transaction_id,source_id,amount,currency,status,date,exception_id,due_at",
		)

		lines := strings.Split(strings.TrimSpace(csvStr), "\n")
		require.Len(t, lines, 6)
	})
}

func TestIntegrationReportExports_SummaryCSV(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedReportTestConfig(t, h)
		seedTransactionsWithMatches(t, h, seed, 4, 2)

		repo := reportingPostgres.NewRepository(h.Provider())
		uc, err := reportingQuery.NewUseCase(repo)
		require.NoError(t, err)

		ctx := h.Ctx()
		ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

		filter := reportingEntities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
			Limit:     100,
		}

		csvData, err := uc.ExportSummaryCSV(ctx, filter)
		require.NoError(t, err)
		require.NotEmpty(t, csvData)

		csvStr := string(csvData)
		require.Contains(
			t,
			csvStr,
			"matched_count,unmatched_count,total_amount,matched_amount,unmatched_amount",
		)
		require.Contains(t, csvStr, "4")
		require.Contains(t, csvStr, "2")
	})
}

func TestIntegrationReportExports_MatchedPDF(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedReportTestConfig(t, h)
		seedTransactionsWithMatches(t, h, seed, 2, 0)

		repo := reportingPostgres.NewRepository(h.Provider())
		uc, err := reportingQuery.NewUseCase(repo)
		require.NoError(t, err)

		ctx := h.Ctx()
		ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

		filter := reportingEntities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
			Limit:     100,
		}

		pdfData, err := uc.ExportMatchedPDF(ctx, filter)
		require.NoError(t, err)
		require.NotEmpty(t, pdfData)
		require.True(t, strings.HasPrefix(string(pdfData), "%PDF-"))
	})
}

func TestIntegrationReportExports_UnmatchedPDF(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedReportTestConfig(t, h)
		seedTransactionsWithMatches(t, h, seed, 0, 3)

		repo := reportingPostgres.NewRepository(h.Provider())
		uc, err := reportingQuery.NewUseCase(repo)
		require.NoError(t, err)

		ctx := h.Ctx()
		ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

		filter := reportingEntities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
			Limit:     100,
		}

		pdfData, err := uc.ExportUnmatchedPDF(ctx, filter)
		require.NoError(t, err)
		require.NotEmpty(t, pdfData)
		require.True(t, strings.HasPrefix(string(pdfData), "%PDF-"))
	})
}

func TestIntegrationReportExports_SummaryPDF(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedReportTestConfig(t, h)
		seedTransactionsWithMatches(t, h, seed, 3, 2)

		repo := reportingPostgres.NewRepository(h.Provider())
		uc, err := reportingQuery.NewUseCase(repo)
		require.NoError(t, err)

		ctx := h.Ctx()
		ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

		filter := reportingEntities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
			Limit:     100,
		}

		pdfData, err := uc.ExportSummaryPDF(ctx, filter)
		require.NoError(t, err)
		require.NotEmpty(t, pdfData)
		require.True(t, strings.HasPrefix(string(pdfData), "%PDF-"))
	})
}

func TestIntegrationReportExports_EmptyData(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedReportTestConfig(t, h)

		repo := reportingPostgres.NewRepository(h.Provider())
		uc, err := reportingQuery.NewUseCase(repo)
		require.NoError(t, err)

		ctx := h.Ctx()
		ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

		filter := reportingEntities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
			Limit:     100,
		}

		matchedCSV, err := uc.ExportMatchedCSV(ctx, filter)
		require.NoError(t, err)
		lines := strings.Split(strings.TrimSpace(string(matchedCSV)), "\n")
		require.Len(t, lines, 1)

		unmatchedCSV, err := uc.ExportUnmatchedCSV(ctx, filter)
		require.NoError(t, err)
		lines = strings.Split(strings.TrimSpace(string(unmatchedCSV)), "\n")
		require.Len(t, lines, 1)

		summaryCSV, err := uc.ExportSummaryCSV(ctx, filter)
		require.NoError(t, err)
		require.Contains(t, string(summaryCSV), "0,0")
	})
}

func TestIntegrationReportExports_DateFiltering(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		seed := seedReportTestConfig(t, h)
		seedTransactionsWithMatches(t, h, seed, 5, 0)

		repo := reportingPostgres.NewRepository(h.Provider())
		uc, err := reportingQuery.NewUseCase(repo)
		require.NoError(t, err)

		ctx := h.Ctx()
		ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

		narrowFilter := reportingEntities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2024, 1, 15, 14, 0, 0, 0, time.UTC),
			Limit:     100,
		}

		csvData, err := uc.ExportMatchedCSV(ctx, narrowFilter)
		require.NoError(t, err)
		lines := strings.Split(strings.TrimSpace(string(csvData)), "\n")
		require.LessOrEqual(t, len(lines), 4)

		wideFilter := reportingEntities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
			Limit:     100,
		}

		csvData, err = uc.ExportMatchedCSV(ctx, wideFilter)
		require.NoError(t, err)
		lines = strings.Split(strings.TrimSpace(string(csvData)), "\n")
		require.Len(t, lines, 6)
	})
}
