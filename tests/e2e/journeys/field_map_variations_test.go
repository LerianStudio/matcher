//go:build e2e

package journeys

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// TestFieldMapVariations_CustomMapping tests custom field name mappings.
func TestFieldMapVariations_CustomMapping(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().WithName("custom-map").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").
				AsBank().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithMapping(map[string]any{
					"external_id": "transaction_id",
					"amount":      "value",
					"currency":    "ccy",
					"date":        "tx_date",
					"description": "memo",
				}).
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithMapping(map[string]any{
					"external_id": "ref_number",
					"amount":      "total",
					"currency":    "money_type",
					"date":        "posted_date",
					"description": "narrative",
				}).
				MustCreate(ctx)

			f.Rule.NewRule(reconciliationContext.ID).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)

			ledgerCSV := []byte(`transaction_id,value,ccy,tx_date,memo
` + tc.NamePrefix() + `-CM-001,100.00,USD,2026-01-15,custom ledger
` + tc.NamePrefix() + `-CM-002,200.00,EUR,2026-01-16,custom ledger 2
`)

			bankCSV := []byte(`ref_number,total,money_type,posted_date,narrative
` + tc.NamePrefix() + `-CM-001,100.00,USD,2026-01-15,custom bank
` + tc.NamePrefix() + `-CM-002,200.00,EUR,2026-01-16,custom bank 2
`)

			ledgerJob, err := client.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"ledger.csv",
				ledgerCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, ledgerJob.ID),
			)

			bankJob, err := client.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				bankSource.ID,
				"bank.csv",
				bankCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, bankJob.ID),
			)

			matchResp, err := client.Matching.RunMatchCommit(ctx, reconciliationContext.ID, "")
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					client,
					reconciliationContext.ID,
					matchResp.RunID,
				),
			)

			groups, err := client.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(groups), 2, "custom mappings should produce matches")

			tc.Logf("✓ Custom field mapping: %d matches", len(groups))
		},
	)
}

// TestFieldMapVariations_MinimalMapping tests mapping with only required fields.
func TestFieldMapVariations_MinimalMapping(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().WithName("minimal-map").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").
				AsBank().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithMapping(map[string]any{
					"external_id": "id",
					"amount":      "amount",
					"currency":    "currency",
					"date":        "date",
				}).
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithMapping(map[string]any{
					"external_id": "id",
					"amount":      "amount",
					"currency":    "currency",
					"date":        "date",
				}).
				MustCreate(ctx)

			f.Rule.NewRule(reconciliationContext.ID).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)

			ledgerCSV := []byte(`id,amount,currency,date
` + tc.NamePrefix() + `-MIN-001,100.00,USD,2024-01-15
`)
			bankCSV := []byte(`id,amount,currency,date
` + tc.NamePrefix() + `-MIN-001,100.00,USD,2024-01-15
`)

			ledgerJob, err := client.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"l.csv",
				ledgerCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, ledgerJob.ID),
			)

			bankJob, err := client.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				bankSource.ID,
				"b.csv",
				bankCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, bankJob.ID),
			)

			matchResp, err := client.Matching.RunMatchCommit(ctx, reconciliationContext.ID, "")
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					client,
					reconciliationContext.ID,
					matchResp.RunID,
				),
			)

			groups, err := client.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(groups), 1, "minimal mapping should work")

			tc.Logf("✓ Minimal field mapping works")
		},
	)
}

// TestFieldMapVariations_UpdateFieldMap tests updating an existing field map.
func TestFieldMapVariations_UpdateFieldMap(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("update-map").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)

			fieldMap := f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithMapping(map[string]any{
					"external_id": "old_id",
					"amount":      "amount",
				}).
				MustCreate(ctx)

			require.NotNil(t, fieldMap)
			require.Equal(t, "old_id", fieldMap.Mapping["external_id"])

			updatedFieldMap, err := apiClient.Configuration.UpdateFieldMap(
				ctx,
				fieldMap.ID,
				client.UpdateFieldMapRequest{
					Mapping: map[string]any{
						"external_id": "new_id",
						"amount":      "amount",
						"currency":    "currency",
					},
				},
			)
			require.NoError(t, err)
			require.Equal(t, "new_id", updatedFieldMap.Mapping["external_id"])
			require.Nil(t, updatedFieldMap.Mapping["old_id"])

			tc.Logf("✓ Field map updated successfully")
		},
	)
}

// TestFieldMapVariations_DifferentMappingsPerSource tests different mappings for each source.
func TestFieldMapVariations_DifferentMappingsPerSource(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().WithName("diff-maps").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").
				AsBank().
				MustCreate(ctx)
			externalSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("gateway").
				AsGateway().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithMapping(map[string]any{
					"external_id": "ledger_id",
					"amount":      "ledger_amt",
					"currency":    "ccy",
				}).
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithMapping(map[string]any{
					"external_id": "bank_ref",
					"amount":      "value",
					"currency":    "cur",
				}).
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, externalSource.ID).
				WithMapping(map[string]any{
					"external_id": "ext_id",
					"amount":      "ext_value",
					"currency":    "iso_ccy",
				}).
				MustCreate(ctx)

			ledgerMap, err := client.Configuration.GetFieldMapBySource(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
			)
			require.NoError(t, err)
			require.Equal(t, "ledger_id", ledgerMap.Mapping["external_id"])

			bankMap, err := client.Configuration.GetFieldMapBySource(
				ctx,
				reconciliationContext.ID,
				bankSource.ID,
			)
			require.NoError(t, err)
			require.Equal(t, "bank_ref", bankMap.Mapping["external_id"])

			externalMap, err := client.Configuration.GetFieldMapBySource(
				ctx,
				reconciliationContext.ID,
				externalSource.ID,
			)
			require.NoError(t, err)
			require.Equal(t, "ext_id", externalMap.Mapping["external_id"])

			tc.Logf("✓ Different mappings per source configured correctly")
		},
	)
}
