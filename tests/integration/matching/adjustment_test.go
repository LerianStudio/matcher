//go:build integration

package matching

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestIntegration_Matching_CreateAdjustment_WithMatchGroup(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)

		group := runMatchAndGetGroup(t, ctx, h, wired, seed)
		require.NotNil(t, group)

		adjustment, err := wired.MatchingUC.CreateAdjustment(ctx, matchingCommand.CreateAdjustmentInput{
			TenantID:     h.Seed.TenantID,
			ContextID:    seed.ContextID,
			MatchGroupID: &group.ID,
			Type:         string(matchingEntities.AdjustmentTypeBankFee),
			Direction:    string(matchingEntities.AdjustmentDirectionDebit),
			Amount:       decimal.NewFromFloat(5.00),
			Currency:     "USD",
			Description:  "Bank processing fee",
			Reason:       "Fee applied by bank",
			CreatedBy:    "test-user",
		})
		require.NoError(t, err)
		require.NotNil(t, adjustment)
		require.Equal(t, seed.ContextID, adjustment.ContextID)
		require.Equal(t, &group.ID, adjustment.MatchGroupID)
		require.Equal(t, matchingEntities.AdjustmentTypeBankFee, adjustment.Type)
		require.Equal(t, matchingEntities.AdjustmentDirectionDebit, adjustment.Direction)
		require.True(t, decimal.NewFromFloat(5.00).Equal(adjustment.Amount))
		require.Equal(t, "USD", adjustment.Currency)

		// Verify adjustment persisted in database
		adjCount := countInt(t, ctx, h.Connection,
			"SELECT count(*) FROM adjustments WHERE id=$1",
			adjustment.ID.String(),
		)
		require.Equal(t, 1, adjCount)
	})
}

func TestIntegration_Matching_CreateAdjustment_WithTransactionID(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)

		// Ingest a single transaction
		csv := buildCSV("ADJ-TX-001", "100.00", "USD", "2026-02-01", "adjustment target")
		_, err := wired.IngestionUC.StartIngestion(
			ctx, seed.ContextID, seed.LedgerSourceID,
			"adj_tx.csv", int64(len(csv)), "csv", strings.NewReader(csv),
		)
		require.NoError(t, err)

		unmatched, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.NotEmpty(t, unmatched)

		txID := unmatched[0].ID

		adjustment, err := wired.MatchingUC.CreateAdjustment(ctx, matchingCommand.CreateAdjustmentInput{
			TenantID:      h.Seed.TenantID,
			ContextID:     seed.ContextID,
			TransactionID: &txID,
			Type:          string(matchingEntities.AdjustmentTypeRounding),
			Direction:     string(matchingEntities.AdjustmentDirectionDebit),
			Amount:        decimal.NewFromFloat(0.01),
			Currency:      "USD",
			Description:   "Rounding difference",
			Reason:        "Sub-cent rounding",
			CreatedBy:     "test-user",
		})
		require.NoError(t, err)
		require.NotNil(t, adjustment)
		require.Equal(t, &txID, adjustment.TransactionID)
		require.Equal(t, matchingEntities.AdjustmentTypeRounding, adjustment.Type)
	})
}

func TestIntegration_Matching_CreateAdjustment_AllTypes(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)

		group := runMatchAndGetGroup(t, ctx, h, wired, seed)
		require.NotNil(t, group)

		types := []matchingEntities.AdjustmentType{
			matchingEntities.AdjustmentTypeBankFee,
			matchingEntities.AdjustmentTypeFXDifference,
			matchingEntities.AdjustmentTypeRounding,
			matchingEntities.AdjustmentTypeWriteOff,
			matchingEntities.AdjustmentTypeMiscellaneous,
		}

		for _, adjType := range types {
			t.Run(string(adjType), func(t *testing.T) {
				adj, err := wired.MatchingUC.CreateAdjustment(ctx, matchingCommand.CreateAdjustmentInput{
					TenantID:     h.Seed.TenantID,
					ContextID:    seed.ContextID,
					MatchGroupID: &group.ID,
					Type:         string(adjType),
					Direction:    string(matchingEntities.AdjustmentDirectionDebit),
					Amount:       decimal.NewFromFloat(1.00),
					Currency:     "USD",
					Description:  "Testing type " + string(adjType),
					Reason:       "Type coverage test",
					CreatedBy:    "test-user",
				})
				require.NoError(t, err)
				require.NotNil(t, adj)
				require.Equal(t, adjType, adj.Type)
			})
		}
	})
}

func TestIntegration_Matching_CreateAdjustment_CreditDirection(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)

		group := runMatchAndGetGroup(t, ctx, h, wired, seed)
		require.NotNil(t, group)

		adjustment, err := wired.MatchingUC.CreateAdjustment(ctx, matchingCommand.CreateAdjustmentInput{
			TenantID:     h.Seed.TenantID,
			ContextID:    seed.ContextID,
			MatchGroupID: &group.ID,
			Type:         string(matchingEntities.AdjustmentTypeFXDifference),
			Direction:    string(matchingEntities.AdjustmentDirectionCredit),
			Amount:       decimal.NewFromFloat(3.50),
			Currency:     "EUR",
			Description:  "FX credit adjustment",
			Reason:       "Exchange rate difference",
			CreatedBy:    "test-user",
		})
		require.NoError(t, err)
		require.NotNil(t, adjustment)
		require.Equal(t, matchingEntities.AdjustmentDirectionCredit, adjustment.Direction)

		// Verify signed amount is negative for CREDIT
		require.True(t, adjustment.SignedAmount().IsNegative())
	})
}

func TestIntegration_Matching_CreateAdjustment_AuditLogAtomicity(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)

		group := runMatchAndGetGroup(t, ctx, h, wired, seed)
		require.NotNil(t, group)

		// Count audit logs before
		auditBefore := countInt(t, ctx, h.Connection,
			"SELECT count(*) FROM audit_logs WHERE entity_type=$1",
			"adjustment",
		)

		adjustment, err := wired.MatchingUC.CreateAdjustment(ctx, matchingCommand.CreateAdjustmentInput{
			TenantID:     h.Seed.TenantID,
			ContextID:    seed.ContextID,
			MatchGroupID: &group.ID,
			Type:         string(matchingEntities.AdjustmentTypeWriteOff),
			Direction:    string(matchingEntities.AdjustmentDirectionDebit),
			Amount:       decimal.NewFromFloat(10.00),
			Currency:     "USD",
			Description:  "Write-off for stale entry",
			Reason:       "Entry aged beyond 90 days",
			CreatedBy:    "audit-test-user",
		})
		require.NoError(t, err)
		require.NotNil(t, adjustment)

		// Count audit logs after — should have exactly one more
		auditAfter := countInt(t, ctx, h.Connection,
			"SELECT count(*) FROM audit_logs WHERE entity_type=$1",
			"adjustment",
		)
		require.Equal(t, auditBefore+1, auditAfter)

		// Verify audit log references the correct entity
		auditForEntity := countInt(t, ctx, h.Connection,
			"SELECT count(*) FROM audit_logs WHERE entity_type=$1 AND entity_id=$2",
			"adjustment",
			adjustment.ID.String(),
		)
		require.Equal(t, 1, auditForEntity)
	})
}

func TestIntegration_Matching_CreateAdjustment_RejectsZeroAmount(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		wired := wireE4T9UseCases(t, h)

		group := runMatchAndGetGroup(t, ctx, h, wired, seed)
		require.NotNil(t, group)

		_, err := wired.MatchingUC.CreateAdjustment(ctx, matchingCommand.CreateAdjustmentInput{
			TenantID:     h.Seed.TenantID,
			ContextID:    seed.ContextID,
			MatchGroupID: &group.ID,
			Type:         string(matchingEntities.AdjustmentTypeBankFee),
			Direction:    string(matchingEntities.AdjustmentDirectionDebit),
			Amount:       decimal.Zero,
			Currency:     "USD",
			Description:  "Should fail",
			Reason:       "Zero amount",
			CreatedBy:    "test-user",
		})
		require.Error(t, err)
		require.ErrorIs(t, err, matchingCommand.ErrAdjustmentAmountNotPositive)
	})
}
