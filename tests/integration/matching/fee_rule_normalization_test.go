//go:build integration

package matching

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	configFeeRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/fee_rule"
	matchingFeeScheduleRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/fee_schedule"
	matchingEnums "github.com/LerianStudio/matcher/internal/matching/domain/enums"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestFeeRuleNormalization_PipelineIntegration(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)

		wired := wireE4T9UseCases(t, h)

		feeScheduleRepository := matchingFeeScheduleRepo.NewRepository(h.Provider())
		schedule, err := sharedfee.NewFeeSchedule(ctx, sharedfee.NewFeeScheduleInput{
			TenantID:         h.Seed.TenantID,
			Name:             fmt.Sprintf("fee-verification-%s", time.Now().UTC().Format("150405.000000000")),
			Currency:         "USD",
			ApplicationOrder: sharedfee.ApplicationOrderParallel,
			RoundingScale:    2,
			RoundingMode:     sharedfee.RoundingModeHalfUp,
			Items: []sharedfee.FeeScheduleItemInput{{
				Name:      "flat-fee",
				Priority:  1,
				Structure: sharedfee.FlatFee{Amount: decimal.RequireFromString("10.00")},
			}},
		})
		require.NoError(t, err)

		schedule, err = feeScheduleRepository.Create(ctx, schedule)
		require.NoError(t, err)

		feeRuleRepository := configFeeRuleRepo.NewRepository(h.Provider())
		rule, err := sharedfee.NewFeeRule(
			ctx,
			seed.ContextID,
			schedule.ID,
			sharedfee.MatchingSideAny,
			"catch-all fee verification rule",
			1,
			nil,
		)
		require.NoError(t, err)
		require.NoError(t, feeRuleRepository.Create(ctx, rule))

		ledgerCSV := buildCSV("FEE-001", "250.00", "USD", "2026-01-15", "payment")
		_, err = wired.IngestionUC.StartIngestion(
			ctx,
			seed.ContextID,
			seed.LedgerSourceID,
			"fee_ledger.csv",
			int64(len(ledgerCSV)),
			"csv",
			strings.NewReader(ledgerCSV),
		)
		require.NoError(t, err)

		bankCSV := buildCSV("fee-001", "250.00", "USD", "2026-01-15", "payment")
		_, err = wired.IngestionUC.StartIngestion(
			ctx,
			seed.ContextID,
			seed.NonLedgerSourceID,
			"fee_bank.csv",
			int64(len(bankCSV)),
			"csv",
			strings.NewReader(bankCSV),
		)
		require.NoError(t, err)

		candidates, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, candidates, 2)

		require.NoError(t, setFeeMetadataForTransactions(ctx, h, candidates, "12.00", "USD"))

		run, groups, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
			TenantID:  h.Seed.TenantID,
			ContextID: seed.ContextID,
			Mode:      matchingVO.MatchRunModeCommit,
		})
		require.NoError(t, err)
		require.NotNil(t, run)
		require.Len(t, groups, 1)

		require.Equal(t, 2, countInt(
			t,
			ctx,
			h.Connection,
			"SELECT count(*) FROM match_fee_variances WHERE context_id=$1 AND run_id=$2 AND fee_schedule_id=$3",
			seed.ContextID.String(),
			run.ID.String(),
			schedule.ID.String(),
		))

		for _, candidate := range candidates {
			require.Equal(t, 1, countInt(
				t,
				ctx,
				h.Connection,
				"SELECT count(*) FROM exceptions WHERE transaction_id=$1 AND reason=$2",
				candidate.ID.String(),
				matchingEnums.ReasonFeeVariance,
			))
		}

		var persistedExpected, persistedActual, persistedScheduleID string
		_, err = pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			if err := tx.QueryRowContext(ctx, `
				SELECT expected_fee_amount::text, actual_fee_amount::text, fee_schedule_id::text
				FROM match_fee_variances
				WHERE context_id = $1 AND run_id = $2
				ORDER BY created_at ASC
				LIMIT 1
			`, seed.ContextID, run.ID).Scan(&persistedExpected, &persistedActual, &persistedScheduleID); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, err)
		require.Equal(t, "10.00000000", persistedExpected)
		require.Equal(t, "12.00000000", persistedActual)
		require.Equal(t, schedule.ID.String(), persistedScheduleID)
	})
}

func TestFeeRuleNormalization_SideSpecificRulesIntegration(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		wired := wireE4T9UseCases(t, h)

		feeScheduleRepository := matchingFeeScheduleRepo.NewRepository(h.Provider())
		leftSchedule := createFlatFeeScheduleForIntegration(t, ctx, feeScheduleRepository, h.Seed.TenantID, "left-fee", "10.00")
		rightSchedule := createFlatFeeScheduleForIntegration(t, ctx, feeScheduleRepository, h.Seed.TenantID, "right-fee", "15.00")

		feeRuleRepository := configFeeRuleRepo.NewRepository(h.Provider())
		createFeeRuleForIntegration(t, ctx, feeRuleRepository, seed.ContextID, leftSchedule.ID, sharedfee.MatchingSideLeft, "left-rule", 1, nil)
		createFeeRuleForIntegration(t, ctx, feeRuleRepository, seed.ContextID, rightSchedule.ID, sharedfee.MatchingSideRight, "right-rule", 2, nil)

		ledgerCSV := buildCSV("SIDE-001", "250.00", "USD", "2026-01-15", "payment")
		_, err := wired.IngestionUC.StartIngestion(ctx, seed.ContextID, seed.LedgerSourceID, "side_ledger.csv", int64(len(ledgerCSV)), "csv", strings.NewReader(ledgerCSV))
		require.NoError(t, err)

		bankCSV := buildCSV("side-001", "250.00", "USD", "2026-01-15", "payment")
		_, err = wired.IngestionUC.StartIngestion(ctx, seed.ContextID, seed.NonLedgerSourceID, "side_bank.csv", int64(len(bankCSV)), "csv", strings.NewReader(bankCSV))
		require.NoError(t, err)

		candidates, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, candidates, 2)

		ledgerTxID := uuid.Nil
		rightTxID := uuid.Nil
		for _, candidate := range candidates {
			if candidate == nil {
				continue
			}

			switch candidate.SourceID {
			case seed.LedgerSourceID:
				ledgerTxID = candidate.ID
			case seed.NonLedgerSourceID:
				rightTxID = candidate.ID
			}
		}
		require.NotEqual(t, uuid.Nil, ledgerTxID)
		require.NotEqual(t, uuid.Nil, rightTxID)

		require.NoError(t, setFeeMetadataForTransactions(ctx, h, candidates, "12.00", "USD"))

		run, groups, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
			TenantID:  h.Seed.TenantID,
			ContextID: seed.ContextID,
			Mode:      matchingVO.MatchRunModeCommit,
		})
		require.NoError(t, err)
		require.NotNil(t, run)
		require.Len(t, groups, 1)

		pairs := make(map[string]string)
		_, err = pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			rows, queryErr := tx.QueryContext(ctx, `
				SELECT transaction_id::text, fee_schedule_id::text
				FROM match_fee_variances
				WHERE context_id = $1 AND run_id = $2
			`, seed.ContextID, run.ID)
			if queryErr != nil {
				return struct{}{}, queryErr
			}
			defer rows.Close()

			for rows.Next() {
				var transactionID, feeScheduleID string
				if scanErr := rows.Scan(&transactionID, &feeScheduleID); scanErr != nil {
					return struct{}{}, scanErr
				}
				pairs[transactionID] = feeScheduleID
			}

			return struct{}{}, rows.Err()
		})
		require.NoError(t, err)
		require.Len(t, pairs, 2)
		require.Equal(t, leftSchedule.ID.String(), pairs[ledgerTxID.String()])
		require.Equal(t, rightSchedule.ID.String(), pairs[rightTxID.String()])
	})
}

func TestFeeRuleNormalization_GrossNormalizationIntegration(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		wired := wireE4T9UseCases(t, h)

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			_, execErr := tx.ExecContext(ctx, `UPDATE reconciliation_contexts SET fee_normalization = 'GROSS' WHERE id = $1`, seed.ContextID)
			return struct{}{}, execErr
		})
		require.NoError(t, err)

		feeScheduleRepository := matchingFeeScheduleRepo.NewRepository(h.Provider())
		schedule, err := sharedfee.NewFeeSchedule(ctx, sharedfee.NewFeeScheduleInput{
			TenantID:         h.Seed.TenantID,
			Name:             fmt.Sprintf("gross-normalization-%s", time.Now().UTC().Format("150405.000000000")),
			Currency:         "USD",
			ApplicationOrder: sharedfee.ApplicationOrderParallel,
			RoundingScale:    2,
			RoundingMode:     sharedfee.RoundingModeHalfUp,
			Items: []sharedfee.FeeScheduleItemInput{{
				Name:      "percentage-fee",
				Priority:  1,
				Structure: sharedfee.PercentageFee{Rate: decimal.RequireFromString("0.10")},
			}},
		})
		require.NoError(t, err)

		schedule, err = feeScheduleRepository.Create(ctx, schedule)
		require.NoError(t, err)

		feeRuleRepository := configFeeRuleRepo.NewRepository(h.Provider())
		createFeeRuleForIntegration(t, ctx, feeRuleRepository, seed.ContextID, schedule.ID, sharedfee.MatchingSideAny, "gross-rule", 1, nil)

		ledgerCSV := buildCSV("GROSS-001", "90.00", "USD", "2026-01-15", "payment")
		_, err = wired.IngestionUC.StartIngestion(ctx, seed.ContextID, seed.LedgerSourceID, "gross_ledger.csv", int64(len(ledgerCSV)), "csv", strings.NewReader(ledgerCSV))
		require.NoError(t, err)

		bankCSV := buildCSV("gross-001", "90.00", "USD", "2026-01-15", "payment")
		_, err = wired.IngestionUC.StartIngestion(ctx, seed.ContextID, seed.NonLedgerSourceID, "gross_bank.csv", int64(len(bankCSV)), "csv", strings.NewReader(bankCSV))
		require.NoError(t, err)

		candidates, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, candidates, 2)

		require.NoError(t, setFeeMetadataForTransactions(ctx, h, candidates, "10.00", "USD"))

		run, groups, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
			TenantID:  h.Seed.TenantID,
			ContextID: seed.ContextID,
			Mode:      matchingVO.MatchRunModeCommit,
		})
		require.NoError(t, err)
		require.NotNil(t, run)
		require.Len(t, groups, 1)

		require.Equal(t, 0, countInt(
			t,
			ctx,
			h.Connection,
			"SELECT count(*) FROM match_fee_variances WHERE context_id=$1 AND run_id=$2",
			seed.ContextID.String(),
			run.ID.String(),
		))

		require.Equal(t, 0, countInt(
			t,
			ctx,
			h.Connection,
			"SELECT count(*) FROM exceptions WHERE reason IN ($1, $2, $3)",
			matchingEnums.ReasonFeeVariance,
			matchingEnums.ReasonFeeDataMissing,
			matchingEnums.ReasonFeeCurrencyMismatch,
		))
	})
}

func TestFeeRuleNormalization_NoMatchingRuleSkipsVerification(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		wired := wireE4T9UseCases(t, h)

		feeScheduleRepository := matchingFeeScheduleRepo.NewRepository(h.Provider())
		schedule := createFlatFeeScheduleForIntegration(t, ctx, feeScheduleRepository, h.Seed.TenantID, "predicate-fee", "10.00")

		feeRuleRepository := configFeeRuleRepo.NewRepository(h.Provider())
		createFeeRuleForIntegration(
			t,
			ctx,
			feeRuleRepository,
			seed.ContextID,
			schedule.ID,
			sharedfee.MatchingSideAny,
			"wire-only-rule",
			1,
			[]sharedfee.FieldPredicate{{Field: "type", Operator: sharedfee.PredicateOperatorEquals, Value: "wire"}},
		)

		ledgerCSV := buildCSV("SKIP-001", "250.00", "USD", "2026-01-15", "payment")
		_, err := wired.IngestionUC.StartIngestion(ctx, seed.ContextID, seed.LedgerSourceID, "skip_ledger.csv", int64(len(ledgerCSV)), "csv", strings.NewReader(ledgerCSV))
		require.NoError(t, err)

		bankCSV := buildCSV("skip-001", "250.00", "USD", "2026-01-15", "payment")
		_, err = wired.IngestionUC.StartIngestion(ctx, seed.ContextID, seed.NonLedgerSourceID, "skip_bank.csv", int64(len(bankCSV)), "csv", strings.NewReader(bankCSV))
		require.NoError(t, err)

		candidates, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, candidates, 2)

		require.NoError(t, setFeeMetadataForTransactions(ctx, h, candidates, "12.00", "USD"))

		run, groups, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
			TenantID:  h.Seed.TenantID,
			ContextID: seed.ContextID,
			Mode:      matchingVO.MatchRunModeCommit,
		})
		require.NoError(t, err)
		require.NotNil(t, run)
		require.Len(t, groups, 1)

		require.Equal(t, 0, countInt(
			t,
			ctx,
			h.Connection,
			"SELECT count(*) FROM match_fee_variances WHERE context_id=$1 AND run_id=$2",
			seed.ContextID.String(),
			run.ID.String(),
		))

		require.Equal(t, 0, countInt(
			t,
			ctx,
			h.Connection,
			"SELECT count(*) FROM exceptions WHERE reason IN ($1, $2, $3)",
			matchingEnums.ReasonFeeVariance,
			matchingEnums.ReasonFeeDataMissing,
			matchingEnums.ReasonFeeCurrencyMismatch,
		))
	})
}

func TestFeeRuleNormalization_FeeExceptionScenarios(t *testing.T) {
	testCases := []struct {
		name      string
		applyMeta func(context.Context, *integration.TestHarness, []*shared.Transaction) error
		reason    string
	}{
		{
			name: "missing fee metadata",
			applyMeta: func(_ context.Context, _ *integration.TestHarness, _ []*shared.Transaction) error {
				return nil
			},
			reason: matchingEnums.ReasonFeeDataMissing,
		},
		{
			name: "fee currency mismatch",
			applyMeta: func(ctx context.Context, h *integration.TestHarness, txs []*shared.Transaction) error {
				return setFeeMetadataForTransactions(ctx, h, txs, "12.00", "EUR")
			},
			reason: matchingEnums.ReasonFeeCurrencyMismatch,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
				ctxBase := e4t9Ctx(t, h)
				ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
				defer cancel()

				seed := seedE4T9Config(t, h)
				wired := wireE4T9UseCases(t, h)

				feeScheduleRepository := matchingFeeScheduleRepo.NewRepository(h.Provider())
				schedule := createFlatFeeScheduleForIntegration(t, ctx, feeScheduleRepository, h.Seed.TenantID, testCase.name, "10.00")

				feeRuleRepository := configFeeRuleRepo.NewRepository(h.Provider())
				createFeeRuleForIntegration(t, ctx, feeRuleRepository, seed.ContextID, schedule.ID, sharedfee.MatchingSideAny, testCase.name+"-rule", 1, nil)

				ledgerCSV := buildCSV("ERR-001", "250.00", "USD", "2026-01-15", "payment")
				_, err := wired.IngestionUC.StartIngestion(ctx, seed.ContextID, seed.LedgerSourceID, "err_ledger.csv", int64(len(ledgerCSV)), "csv", strings.NewReader(ledgerCSV))
				require.NoError(t, err)

				bankCSV := buildCSV("err-001", "250.00", "USD", "2026-01-15", "payment")
				_, err = wired.IngestionUC.StartIngestion(ctx, seed.ContextID, seed.NonLedgerSourceID, "err_bank.csv", int64(len(bankCSV)), "csv", strings.NewReader(bankCSV))
				require.NoError(t, err)

				candidates, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
				require.NoError(t, err)
				require.Len(t, candidates, 2)

				require.NoError(t, testCase.applyMeta(ctx, h, candidates))

				run, groups, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
					TenantID:  h.Seed.TenantID,
					ContextID: seed.ContextID,
					Mode:      matchingVO.MatchRunModeCommit,
				})
				require.NoError(t, err)
				require.NotNil(t, run)
				require.Len(t, groups, 1)

				require.Equal(t, 0, countInt(
					t,
					ctx,
					h.Connection,
					"SELECT count(*) FROM match_fee_variances WHERE context_id=$1 AND run_id=$2",
					seed.ContextID.String(),
					run.ID.String(),
				))

				for _, candidate := range candidates {
					require.Equal(t, 1, countInt(
						t,
						ctx,
						h.Connection,
						"SELECT count(*) FROM exceptions WHERE transaction_id=$1 AND reason=$2",
						candidate.ID.String(),
						testCase.reason,
					))
				}
			})
		})
	}
}

func createFlatFeeScheduleForIntegration(
	t *testing.T,
	ctx context.Context,
	repo *matchingFeeScheduleRepo.Repository,
	tenantID uuid.UUID,
	nameSuffix string,
	amount string,
) *sharedfee.FeeSchedule {
	t.Helper()

	schedule, err := sharedfee.NewFeeSchedule(ctx, sharedfee.NewFeeScheduleInput{
		TenantID:         tenantID,
		Name:             fmt.Sprintf("%s-%s", nameSuffix, time.Now().UTC().Format("150405.000000000")),
		Currency:         "USD",
		ApplicationOrder: sharedfee.ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     sharedfee.RoundingModeHalfUp,
		Items: []sharedfee.FeeScheduleItemInput{{
			Name:      "flat-fee",
			Priority:  1,
			Structure: sharedfee.FlatFee{Amount: decimal.RequireFromString(amount)},
		}},
	})
	require.NoError(t, err)

	created, err := repo.Create(ctx, schedule)
	require.NoError(t, err)

	return created
}

func createFeeRuleForIntegration(
	t *testing.T,
	ctx context.Context,
	repo *configFeeRuleRepo.Repository,
	contextID uuid.UUID,
	feeScheduleID uuid.UUID,
	side sharedfee.MatchingSide,
	name string,
	priority int,
	predicates []sharedfee.FieldPredicate,
) {
	t.Helper()

	rule, err := sharedfee.NewFeeRule(ctx, contextID, feeScheduleID, side, name, priority, predicates)
	require.NoError(t, err)
	require.NoError(t, repo.Create(ctx, rule))
}

func setFeeMetadataForTransactions(
	ctx context.Context,
	h *integration.TestHarness,
	transactions []*shared.Transaction,
	amount string,
	currency string,
) error {
	payload := fmt.Sprintf(`{"fee":{"amount":"%s","currency":"%s"}}`, amount, currency)

	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		for _, transaction := range transactions {
			if transaction == nil {
				continue
			}

			if _, err := tx.ExecContext(
				ctx,
				`UPDATE transactions SET metadata = $1::jsonb, updated_at = NOW() WHERE id = $2`,
				payload,
				transaction.ID,
			); err != nil {
				return struct{}{}, fmt.Errorf("update transaction metadata %s: %w", transaction.ID, err)
			}
		}

		return struct{}{}, nil
	})

	return err
}
