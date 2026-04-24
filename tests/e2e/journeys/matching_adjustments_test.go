//go:build e2e

package journeys

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/client"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// =============================================================================
// Matching Adjustments Tests
// =============================================================================

// TestAdjustment_BankFee tests creating a BANK_FEE adjustment on a match group.
func TestAdjustment_BankFee(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("adj-bank-fee").
				MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").
				AsBank().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().
				MustCreate(ctx)
			f.Rule.NewRule(reconciliationContext.ID).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("ADJ-FEE-001", "100.00", "USD", "2026-01-15", "ledger bank fee").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("ADJ-FEE-001", "100.00", "USD", "2026-01-15", "bank bank fee").
				Build()

			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx, reconciliationContext.ID, ledgerSource.ID, "ledger.csv", ledgerCSV,
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID))

			bankJob, err := apiClient.Ingestion.UploadCSV(
				ctx, reconciliationContext.ID, bankSource.ID, "bank.csv", bankCSV,
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, bankJob.ID))

			matchResp, err := apiClient.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForMatchRunComplete(ctx, tc, apiClient, reconciliationContext.ID, matchResp.RunID))

			groups, err := apiClient.Matching.GetMatchRunResults(ctx, reconciliationContext.ID, matchResp.RunID)
			require.NoError(t, err)
			require.NotEmpty(t, groups, "should have at least one match group")

			matchGroupID := groups[0].ID

			adjustment, err := apiClient.Matching.CreateAdjustment(ctx, reconciliationContext.ID, client.CreateAdjustmentRequest{
				Type:         "BANK_FEE",
				Amount:       "10.50",
				Currency:     "USD",
				Direction:    "DEBIT",
				Reason:       "Wire transfer processing fee",
				Description:  "Bank wire fee adjustment",
				MatchGroupID: matchGroupID,
			})
			require.NoError(t, err)
			require.NotNil(t, adjustment)

			tc.Logf("✓ BANK_FEE adjustment created on match group %s", matchGroupID)
		},
	)
}

// TestAdjustment_AllTypes tests all valid adjustment types.
func TestAdjustment_AllTypes(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("adj-all-types").
				MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").
				AsBank().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().
				MustCreate(ctx)
			f.Rule.NewRule(reconciliationContext.ID).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)

			ledgerBuilder := factories.NewCSVBuilder(tc.NamePrefix())
			bankBuilder := factories.NewCSVBuilder(tc.NamePrefix())
			for i := 1; i <= 5; i++ {
				ref := fmt.Sprintf("ADJ-TYPE-%03d", i)
				ledgerBuilder.AddRow(ref, "200.00", "USD", "2026-01-15", "ledger type test")
				bankBuilder.AddRow(ref, "200.00", "USD", "2026-01-15", "bank type test")
			}

			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx, reconciliationContext.ID, ledgerSource.ID, "ledger.csv", ledgerBuilder.Build(),
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID))

			bankJob, err := apiClient.Ingestion.UploadCSV(
				ctx, reconciliationContext.ID, bankSource.ID, "bank.csv", bankBuilder.Build(),
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, bankJob.ID))

			matchResp, err := apiClient.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForMatchRunComplete(ctx, tc, apiClient, reconciliationContext.ID, matchResp.RunID))

			groups, err := apiClient.Matching.GetMatchRunResults(ctx, reconciliationContext.ID, matchResp.RunID)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(groups), 5, "should have at least 5 match groups")

			adjustmentTypes := []struct {
				typeName string
				amount   string
				reason   string
			}{
				{"BANK_FEE", "5.00", "Bank processing fee"},
				{"FX_DIFFERENCE", "2.50", "Foreign exchange rate difference"},
				{"ROUNDING", "0.01", "Rounding difference"},
				{"WRITE_OFF", "1.00", "Small balance write-off"},
				{"MISCELLANEOUS", "3.75", "Miscellaneous adjustment"},
			}

			for i, adjType := range adjustmentTypes {
				t.Run(adjType.typeName, func(t *testing.T) {
					adjustment, err := apiClient.Matching.CreateAdjustment(ctx, reconciliationContext.ID, client.CreateAdjustmentRequest{
						Type:         adjType.typeName,
						Amount:       adjType.amount,
						Currency:     "USD",
						Direction:    "DEBIT",
						Reason:       adjType.reason,
						Description:  fmt.Sprintf("Test %s adjustment", adjType.typeName),
						MatchGroupID: groups[i].ID,
					})
					require.NoError(t, err)
					require.NotNil(t, adjustment)

					tc.Logf("✓ %s adjustment created on group %s", adjType.typeName, groups[i].ID)
				})
			}
		},
	)
}

// TestAdjustment_CreditDirection tests creating a CREDIT adjustment.
func TestAdjustment_CreditDirection(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("adj-credit").
				MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").
				AsBank().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().
				MustCreate(ctx)
			f.Rule.NewRule(reconciliationContext.ID).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("ADJ-CREDIT-001", "500.00", "USD", "2026-01-15", "ledger credit test").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("ADJ-CREDIT-001", "500.00", "USD", "2026-01-15", "bank credit test").
				Build()

			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx, reconciliationContext.ID, ledgerSource.ID, "ledger.csv", ledgerCSV,
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID))

			bankJob, err := apiClient.Ingestion.UploadCSV(
				ctx, reconciliationContext.ID, bankSource.ID, "bank.csv", bankCSV,
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, bankJob.ID))

			matchResp, err := apiClient.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForMatchRunComplete(ctx, tc, apiClient, reconciliationContext.ID, matchResp.RunID))

			groups, err := apiClient.Matching.GetMatchRunResults(ctx, reconciliationContext.ID, matchResp.RunID)
			require.NoError(t, err)
			require.NotEmpty(t, groups)

			adjustment, err := apiClient.Matching.CreateAdjustment(ctx, reconciliationContext.ID, client.CreateAdjustmentRequest{
				Type:         "FX_DIFFERENCE",
				Amount:       "15.00",
				Currency:     "USD",
				Direction:    "CREDIT",
				Reason:       "FX rate favorable difference",
				Description:  "Credit direction adjustment",
				MatchGroupID: groups[0].ID,
			})
			require.NoError(t, err)
			require.NotNil(t, adjustment)

			tc.Logf("✓ CREDIT direction adjustment created successfully")
		},
	)
}

// TestAdjustment_WithTransactionID tests creating an adjustment linked to a specific transaction.
func TestAdjustment_WithTransactionID(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("adj-tx-id").
				MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").
				AsBank().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().
				MustCreate(ctx)
			f.Rule.NewRule(reconciliationContext.ID).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("ADJ-TXID-001", "300.00", "USD", "2026-01-15", "ledger tx id test").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("ADJ-TXID-001", "300.00", "USD", "2026-01-15", "bank tx id test").
				Build()

			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx, reconciliationContext.ID, ledgerSource.ID, "ledger.csv", ledgerCSV,
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID))

			bankJob, err := apiClient.Ingestion.UploadCSV(
				ctx, reconciliationContext.ID, bankSource.ID, "bank.csv", bankCSV,
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, bankJob.ID))

			matchResp, err := apiClient.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForMatchRunComplete(ctx, tc, apiClient, reconciliationContext.ID, matchResp.RunID))

			groups, err := apiClient.Matching.GetMatchRunResults(ctx, reconciliationContext.ID, matchResp.RunID)
			require.NoError(t, err)
			require.NotEmpty(t, groups)

			matchGroupID := groups[0].ID

			ledgerTxs, err := apiClient.Ingestion.ListTransactionsByJob(ctx, reconciliationContext.ID, ledgerJob.ID)
			require.NoError(t, err)
			require.NotEmpty(t, ledgerTxs)

			txID := ledgerTxs[0].ID

			adjustment, err := apiClient.Matching.CreateAdjustment(ctx, reconciliationContext.ID, client.CreateAdjustmentRequest{
				Type:          "BANK_FEE",
				Amount:        "7.25",
				Currency:      "USD",
				Direction:     "DEBIT",
				Reason:        "Transaction-specific bank fee",
				Description:   "Adjustment linked to specific transaction",
				MatchGroupID:  matchGroupID,
				TransactionID: txID,
			})
			require.NoError(t, err)
			require.NotNil(t, adjustment)

			tc.Logf("✓ Adjustment created with transaction ID %s on group %s", txID, matchGroupID)
		},
	)
}

// TestAdjustment_NonExistentContext tests creating an adjustment on a non-existent context.
func TestAdjustment_NonExistentContext(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()

		_, err := apiClient.Matching.CreateAdjustment(ctx, "00000000-0000-0000-0000-000000000000", client.CreateAdjustmentRequest{
			Type:        "BANK_FEE",
			Amount:      "10.00",
			Currency:    "USD",
			Direction:   "DEBIT",
			Reason:      "test",
			Description: "test",
		})
		require.Error(t, err)

		var apiErr *client.APIError
		require.True(t, errors.As(err, &apiErr), "expected *client.APIError, got %T", err)

		tc.Logf("✓ Non-existent context adjustment correctly rejected with status %d", apiErr.StatusCode)
	})
}

// TestAdjustment_MissingRequiredFields tests rejection of adjustment with missing required fields.
func TestAdjustment_MissingRequiredFields(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("adj-missing-fields").
				MustCreate(ctx)

			testCases := []struct {
				name string
				req  client.CreateAdjustmentRequest
			}{
				{
					name: "missing type",
					req: client.CreateAdjustmentRequest{
						Amount:      "10.00",
						Currency:    "USD",
						Direction:   "DEBIT",
						Reason:      "test",
						Description: "test",
					},
				},
				{
					name: "missing amount",
					req: client.CreateAdjustmentRequest{
						Type:        "BANK_FEE",
						Currency:    "USD",
						Direction:   "DEBIT",
						Reason:      "test",
						Description: "test",
					},
				},
				{
					name: "missing currency",
					req: client.CreateAdjustmentRequest{
						Type:        "BANK_FEE",
						Amount:      "10.00",
						Direction:   "DEBIT",
						Reason:      "test",
						Description: "test",
					},
				},
				{
					name: "missing direction",
					req: client.CreateAdjustmentRequest{
						Type:        "BANK_FEE",
						Amount:      "10.00",
						Currency:    "USD",
						Reason:      "test",
						Description: "test",
					},
				},
			}

			for _, adjCase := range testCases {
				t.Run(adjCase.name, func(t *testing.T) {
					_, err := apiClient.Matching.CreateAdjustment(ctx, reconciliationContext.ID, adjCase.req)
					require.Error(t, err, "%s should be rejected", adjCase.name)

					var apiErr *client.APIError
					require.True(t, errors.As(err, &apiErr), "expected *client.APIError, got %T", err)
					require.True(t, apiErr.IsBadRequest(),
						"should return 400 for %s, got %d", adjCase.name, apiErr.StatusCode)

					tc.Logf("✓ %s correctly rejected", adjCase.name)
				})
			}
		},
	)
}
