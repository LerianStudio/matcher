//go:build integration

package matching

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	matchingFeeScheduleRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/fee_schedule"
	matchingFeeVarianceRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/fee_variance"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestIntegration_Matching_FeeVarianceRepository_CreateBatchWithTx_RoundTrip(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		provider := h.Provider()
		repo := matchingFeeVarianceRepo.NewRepository(provider)
		feeScheduleRepository := matchingFeeScheduleRepo.NewRepository(provider)

		schedule := createFlatFeeScheduleForIntegration(t, ctx, feeScheduleRepository, h.Seed.TenantID, "repo-variance", "10.00")

		runID := uuid.New()
		ruleID := uuid.New()
		groupID := uuid.New()
		jobID := uuid.New()
		firstTxID := uuid.New()
		secondTxID := uuid.New()
		createdAt := time.Now().UTC()

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			if _, execErr := tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status)
				VALUES ($1, $2, $3, 'COMPLETED')
			`, jobID, h.Seed.ContextID, h.Seed.SourceID); execErr != nil {
				return struct{}{}, fmt.Errorf("insert ingestion job: %w", execErr)
			}

			for _, transactionID := range []uuid.UUID{firstTxID, secondTxID} {
				if _, execErr := tx.ExecContext(ctx, `
					INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, 100.00, 'USD', $5, 'COMPLETE', 'MATCHED', $5, $5)
				`, transactionID, jobID, h.Seed.SourceID, transactionID.String(), createdAt); execErr != nil {
					return struct{}{}, fmt.Errorf("insert transaction %s: %w", transactionID, execErr)
				}
			}

			if _, execErr := tx.ExecContext(ctx, `
				INSERT INTO match_rules (id, context_id, priority, type, config)
				VALUES ($1, $2, 1, 'EXACT', '{}'::jsonb)
			`, ruleID, h.Seed.ContextID); execErr != nil {
				return struct{}{}, fmt.Errorf("insert match rule: %w", execErr)
			}

			if _, execErr := tx.ExecContext(ctx, `
				INSERT INTO match_runs (id, context_id, mode, status, started_at, stats)
				VALUES ($1, $2, 'COMMIT', 'COMPLETED', $3, '{}'::jsonb)
			`, runID, h.Seed.ContextID, createdAt); execErr != nil {
				return struct{}{}, fmt.Errorf("insert match run: %w", execErr)
			}

			if _, execErr := tx.ExecContext(ctx, `
				INSERT INTO match_groups (id, context_id, run_id, rule_id, confidence, status)
				VALUES ($1, $2, $3, $4, 95, 'CONFIRMED')
			`, groupID, h.Seed.ContextID, runID, ruleID); execErr != nil {
				return struct{}{}, fmt.Errorf("insert match group: %w", execErr)
			}

			firstVariance, varianceErr := matchingEntities.NewFeeVariance(
				ctx,
				h.Seed.ContextID,
				runID,
				groupID,
				firstTxID,
				schedule.ID,
				schedule.Name,
				"USD",
				decimal.RequireFromString("10.00"),
				decimal.RequireFromString("12.00"),
				decimal.RequireFromString("0.01"),
				decimal.RequireFromString("0.05"),
				"OVERCHARGE",
			)
			if varianceErr != nil {
				return struct{}{}, varianceErr
			}

			secondVariance, varianceErr := matchingEntities.NewFeeVariance(
				ctx,
				h.Seed.ContextID,
				runID,
				groupID,
				secondTxID,
				schedule.ID,
				schedule.Name,
				"USD",
				decimal.RequireFromString("10.00"),
				decimal.RequireFromString("10.00"),
				decimal.RequireFromString("0.01"),
				decimal.RequireFromString("0.05"),
				"MATCH",
			)
			if varianceErr != nil {
				return struct{}{}, varianceErr
			}

			persisted, persistErr := repo.CreateBatchWithTx(ctx, tx, []*matchingEntities.FeeVariance{firstVariance, secondVariance})
			if persistErr != nil {
				return struct{}{}, persistErr
			}
			require.Len(t, persisted, 2)

			return struct{}{}, nil
		})
		require.NoError(t, err)

		require.Equal(t, 2, countInt(
			t,
			ctx,
			h.Connection,
			"SELECT count(*) FROM match_fee_variances WHERE context_id=$1 AND run_id=$2 AND fee_schedule_id=$3",
			h.Seed.ContextID.String(),
			runID.String(),
			schedule.ID.String(),
		))

		_, err = pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			rows, queryErr := tx.QueryContext(ctx, `
				SELECT transaction_id::text, fee_schedule_name_snapshot, expected_fee_amount::text, actual_fee_amount::text
				FROM match_fee_variances
				WHERE context_id = $1 AND run_id = $2
				ORDER BY transaction_id
			`, h.Seed.ContextID, runID)
			if queryErr != nil {
				return struct{}{}, queryErr
			}
			defer rows.Close()

			observed := make(map[string][3]string)
			for rows.Next() {
				var transactionID, scheduleName, expectedFee, actualFee string
				if scanErr := rows.Scan(&transactionID, &scheduleName, &expectedFee, &actualFee); scanErr != nil {
					return struct{}{}, scanErr
				}
				observed[transactionID] = [3]string{scheduleName, expectedFee, actualFee}
			}

			require.Len(t, observed, 2)
			require.Equal(t, [3]string{schedule.Name, "10.00000000", "12.00000000"}, observed[firstTxID.String()])
			require.Equal(t, [3]string{schedule.Name, "10.00000000", "10.00000000"}, observed[secondTxID.String()])

			return struct{}{}, rows.Err()
		})
		require.NoError(t, err)
	})
}
