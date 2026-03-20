//go:build e2e

package journeys

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// ---------------------------------------------------------------------------
// Fee Schedule CRUD
// ---------------------------------------------------------------------------

// TestFeeSchedule_CRUD tests the full lifecycle of fee schedule management:
// create, read, list, update, and delete.
func TestFeeSchedule_CRUD(t *testing.T) {
	e2e.RunE2EWithTimeout(t, 2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, c *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, c)

			// Step 1: Create a fee schedule with mixed fee types
			tc.Logf("Step 1: Creating fee schedule with PARALLEL + mixed items")
			schedule := f.FeeSchedule.NewFeeSchedule().
				WithName("card-processing-visa").
				WithCurrency("USD").
				Parallel().
				WithRoundingScale(2).
				WithRoundingMode("HALF_UP").
				WithPercentageFee("interchange", 1, "0.015").
				WithPercentageFee("scheme_fee", 2, "0.001").
				WithFlatFee("acquirer_markup", 3, "0.30").
				MustCreate(ctx)

			require.NotEmpty(t, schedule.ID, "schedule ID should be set")
			assert.Equal(t, "USD", schedule.Currency)
			assert.Equal(t, "PARALLEL", schedule.ApplicationOrder)
			assert.Equal(t, 2, schedule.RoundingScale)
			assert.Equal(t, "HALF_UP", schedule.RoundingMode)
			require.Len(t, schedule.Items, 3)
			tc.Logf("Created schedule: %s", schedule.ID)

			// Step 2: Get by ID
			tc.Logf("Step 2: Getting fee schedule by ID")
			fetched, err := c.FeeSchedule.GetFeeSchedule(ctx, schedule.ID)
			require.NoError(t, err)
			assert.Equal(t, schedule.ID, fetched.ID)
			assert.Equal(t, schedule.Name, fetched.Name)
			require.Len(t, fetched.Items, 3)

			// Step 3: List — should contain our schedule
			tc.Logf("Step 3: Listing fee schedules")
			list, err := c.FeeSchedule.ListFeeSchedules(ctx)
			require.NoError(t, err)
			found := false
			for _, s := range list {
				if s.ID == schedule.ID {
					found = true
					break
				}
			}
			assert.True(t, found, "created schedule should appear in list")

			// Step 4: Update
			tc.Logf("Step 4: Updating fee schedule")
			newName := tc.UniqueName("updated-visa")
			newMode := "BANKERS"
			updated, err := c.FeeSchedule.UpdateFeeSchedule(ctx, schedule.ID, client.UpdateFeeScheduleRequest{
				Name:         &newName,
				RoundingMode: &newMode,
			})
			require.NoError(t, err)
			assert.Equal(t, newName, updated.Name)
			assert.Equal(t, "BANKERS", updated.RoundingMode)

			// Step 5: Delete
			tc.Logf("Step 5: Deleting fee schedule")
			err = c.FeeSchedule.DeleteFeeSchedule(ctx, schedule.ID)
			require.NoError(t, err)

			// Verify deletion
			_, err = c.FeeSchedule.GetFeeSchedule(ctx, schedule.ID)
			require.Error(t, err, "get after delete should fail")
			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr))
			assert.True(t, apiErr.IsNotFound(), "should be 404")

			tc.Logf("PASS: Fee schedule CRUD lifecycle completed")
		},
	)
}

// ---------------------------------------------------------------------------
// Fee Schedule Simulation
// ---------------------------------------------------------------------------

// TestFeeSchedule_SimulateParallel verifies the simulate endpoint computes
// correct fee breakdown for PARALLEL application order.
func TestFeeSchedule_SimulateParallel(t *testing.T) {
	e2e.RunE2EWithTimeout(t, 2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, c *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, c)

			// Create schedule: interchange 1.5% + scheme 0.1% + flat $0.30
			// All on gross (PARALLEL).
			// For $1000.00:
			//   interchange = 1000 * 0.015 = 15.00
			//   scheme_fee  = 1000 * 0.001 = 1.00
			//   flat        = 0.30
			//   totalFee    = 16.30
			//   net         = 983.70
			schedule := f.FeeSchedule.NewFeeSchedule().
				WithName("sim-parallel").
				WithCurrency("USD").
				Parallel().
				WithPercentageFee("interchange", 1, "0.015").
				WithPercentageFee("scheme_fee", 2, "0.001").
				WithFlatFee("acquirer_markup", 3, "0.30").
				MustCreate(ctx)

			tc.Logf("Simulating parallel fees on $1000.00")
			sim, err := c.FeeSchedule.SimulateFeeSchedule(ctx, schedule.ID, client.SimulateFeeRequest{
				GrossAmount: "1000.00",
				Currency:    "USD",
			})
			require.NoError(t, err)

			assert.Equal(t, "1000", sim.GrossAmount)
			assert.Equal(t, "983.7", sim.NetAmount)
			assert.Equal(t, "16.3", sim.TotalFee)
			assert.Equal(t, "USD", sim.Currency)
			require.Len(t, sim.Items, 3)

			// Verify each item fee
			feesByName := make(map[string]client.SimulateFeeItem)
			for _, item := range sim.Items {
				feesByName[item.Name] = item
			}

			assert.Equal(t, "15", feesByName["interchange"].Fee)
			assert.Equal(t, "1000", feesByName["interchange"].BaseUsed)
			assert.Equal(t, "1", feesByName["scheme_fee"].Fee)
			assert.Equal(t, "0.3", feesByName["acquirer_markup"].Fee)

			tc.Logf("PASS: Parallel simulation correct — gross=1000, net=983.70, fee=16.30")
		},
	)
}

// TestFeeSchedule_SimulateCascading verifies the simulate endpoint computes
// correct fee breakdown for CASCADING application order.
func TestFeeSchedule_SimulateCascading(t *testing.T) {
	e2e.RunE2EWithTimeout(t, 2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, c *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, c)

			// CASCADING: fees calculated on remaining balance after prior fees.
			// For $1000.00 with 2% + 1%:
			//   fee1 = 1000 * 0.02 = 20.00, remaining = 980.00
			//   fee2 = 980 * 0.01  = 9.80,  remaining = 970.20
			//   totalFee = 29.80, net = 970.20
			schedule := f.FeeSchedule.NewFeeSchedule().
				WithName("sim-cascading").
				WithCurrency("USD").
				Cascading().
				WithPercentageFee("primary", 1, "0.02").
				WithPercentageFee("secondary", 2, "0.01").
				MustCreate(ctx)

			tc.Logf("Simulating cascading fees on $1000.00")
			sim, err := c.FeeSchedule.SimulateFeeSchedule(ctx, schedule.ID, client.SimulateFeeRequest{
				GrossAmount: "1000.00",
				Currency:    "USD",
			})
			require.NoError(t, err)

			assert.Equal(t, "1000", sim.GrossAmount)
			assert.Equal(t, "970.2", sim.NetAmount)
			assert.Equal(t, "29.8", sim.TotalFee)
			require.Len(t, sim.Items, 2)

			feesByName := make(map[string]client.SimulateFeeItem)
			for _, item := range sim.Items {
				feesByName[item.Name] = item
			}

			// Primary: computed on gross
			assert.Equal(t, "20", feesByName["primary"].Fee)
			assert.Equal(t, "1000", feesByName["primary"].BaseUsed)

			// Secondary: computed on 980 (gross - primary fee)
			assert.Equal(t, "9.8", feesByName["secondary"].Fee)
			assert.Equal(t, "980", feesByName["secondary"].BaseUsed)

			tc.Logf("PASS: Cascading simulation correct — gross=1000, net=970.20, fee=29.80")
		},
	)
}

// ---------------------------------------------------------------------------
// Fee Schedule Validation Errors
// ---------------------------------------------------------------------------

// TestFeeSchedule_ValidationErrors tests that the API rejects invalid requests.
func TestFeeSchedule_ValidationErrors(t *testing.T) {
	e2e.RunE2EWithTimeout(t, 2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, c *e2e.Client) {
			ctx := context.Background()

			t.Run("missing_name", func(t *testing.T) {
				_, err := c.FeeSchedule.CreateFeeSchedule(ctx, client.CreateFeeScheduleRequest{
					Name:             "",
					Currency:         "USD",
					ApplicationOrder: "PARALLEL",
					Items: []client.CreateFeeScheduleItemRequest{
						{Name: "fee", Priority: 1, StructureType: "FLAT", Structure: map[string]any{"amount": "1.00"}},
					},
				})
				require.Error(t, err)
				var apiErr *client.APIError
				require.True(t, errors.As(err, &apiErr))
				assert.True(t, apiErr.IsBadRequest(), "missing name should be 400")
			})

			t.Run("no_items", func(t *testing.T) {
				_, err := c.FeeSchedule.CreateFeeSchedule(ctx, client.CreateFeeScheduleRequest{
					Name:             tc.UniqueName("no-items"),
					Currency:         "USD",
					ApplicationOrder: "PARALLEL",
					Items:            []client.CreateFeeScheduleItemRequest{},
				})
				require.Error(t, err)
				var apiErr *client.APIError
				require.True(t, errors.As(err, &apiErr))
				assert.True(t, apiErr.IsBadRequest(), "no items should be 400")
			})

			t.Run("duplicate_priority", func(t *testing.T) {
				_, err := c.FeeSchedule.CreateFeeSchedule(ctx, client.CreateFeeScheduleRequest{
					Name:             tc.UniqueName("dup-prio"),
					Currency:         "USD",
					ApplicationOrder: "PARALLEL",
					Items: []client.CreateFeeScheduleItemRequest{
						{Name: "fee_a", Priority: 1, StructureType: "FLAT", Structure: map[string]any{"amount": "1.00"}},
						{Name: "fee_b", Priority: 1, StructureType: "FLAT", Structure: map[string]any{"amount": "2.00"}},
					},
				})
				require.Error(t, err)
				var apiErr *client.APIError
				require.True(t, errors.As(err, &apiErr))
				assert.True(t, apiErr.IsBadRequest(), "duplicate priority should be 400")
			})

			t.Run("invalid_application_order", func(t *testing.T) {
				_, err := c.FeeSchedule.CreateFeeSchedule(ctx, client.CreateFeeScheduleRequest{
					Name:             tc.UniqueName("bad-order"),
					Currency:         "USD",
					ApplicationOrder: "INVALID",
					Items: []client.CreateFeeScheduleItemRequest{
						{Name: "fee", Priority: 1, StructureType: "FLAT", Structure: map[string]any{"amount": "1.00"}},
					},
				})
				require.Error(t, err)
				var apiErr *client.APIError
				require.True(t, errors.As(err, &apiErr))
				assert.True(t, apiErr.IsBadRequest(), "invalid application order should be 400")
			})

			t.Run("get_nonexistent", func(t *testing.T) {
				_, err := c.FeeSchedule.GetFeeSchedule(ctx, "00000000-0000-0000-0000-000000000000")
				require.Error(t, err)
				var apiErr *client.APIError
				require.True(t, errors.As(err, &apiErr))
				assert.True(t, apiErr.IsNotFound(), "nonexistent schedule should be 404")
			})

			tc.Logf("PASS: All validation error cases rejected correctly")
		},
	)
}

// ---------------------------------------------------------------------------
// Fee Schedule Matching — NET Normalization
// ---------------------------------------------------------------------------

// TestFeeSchedule_NetNormalization_OneToOneExact tests that when feeNormalization
// is "NET", the matcher deducts fees from gross amounts before comparing,
// allowing a $100 gross transaction to match a $98.50 net transaction.
func TestFeeSchedule_NetNormalization_OneToOneExact(t *testing.T) {
	e2e.RunE2EWithTimeout(t, 3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, c *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, c)

			// Step 1: Create a fee schedule with 1.5% fee.
			// For $100.00 gross: fee = 1.50, net = 98.50
			tc.Logf("Step 1: Creating fee schedule (1.5%% PARALLEL)")
			schedule := f.FeeSchedule.NewFeeSchedule().
				WithName("net-norm-test").
				WithCurrency("USD").
				Parallel().
				WithPercentageFee("processing_fee", 1, "0.015").
				MustCreate(ctx)

			// Step 2: Create context with NET normalization
			tc.Logf("Step 2: Creating context with feeNormalization=NET")
			reconciliationContext := f.Context.NewContext().
				WithName("net-match").
				OneToOne().
				WithFeeNormalization("NET").
				MustCreate(ctx)

			// Step 3: Create sources — gateway (gross) and ledger (net).
			tc.Logf("Step 3: Creating sources")
			gatewaySource := f.Source.NewSource(reconciliationContext.ID).
				WithName("gateway").
				AsGateway().
				MustCreate(ctx)

			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").
				AsLedger().
				MustCreate(ctx)

			// Step 4: Create field maps
			tc.Logf("Step 4: Creating field maps")
			f.Source.NewFieldMap(reconciliationContext.ID, gatewaySource.ID).
				WithStandardMapping().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// Step 4.5: Create a LEFT fee rule so only gateway transactions are normalized.
			f.FeeRule.NewFeeRule(reconciliationContext.ID).
				WithName("gateway-net").
				Left().
				WithFeeScheduleID(schedule.ID).
				WithPriority(1).
				MustCreate(ctx)

			// Step 5: Create tolerance match rule
			// Use a small tolerance to accommodate rounding differences
			tc.Logf("Step 5: Creating tolerance match rule")
			f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(1).
				Tolerance().
				WithToleranceConfig("0.01").
				MustCreate(ctx)

			// Step 6: Upload gateway transactions (GROSS amounts)
			// Gateway reports $100.00, $250.00, $75.00
			tc.Logf("Step 6: Uploading gateway transactions (gross)")
			gatewayCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("GW-001", "100.00", "USD", "2026-01-15", "payment-alpha").
				AddRow("GW-002", "250.00", "USD", "2026-01-16", "payment-beta").
				AddRow("GW-003", "75.00", "USD", "2026-01-17", "payment-gamma").
				Build()

			gatewayJob, err := c.Ingestion.UploadCSV(
				ctx, reconciliationContext.ID, gatewaySource.ID, "gateway.csv", gatewayCSV,
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, c, reconciliationContext.ID, gatewayJob.ID))

			// Step 7: Upload bank transactions (NET amounts = gross - 1.5%)
			// Bank receives $98.50, $246.25, $73.875 (rounds to 73.88)
			tc.Logf("Step 7: Uploading bank transactions (net)")
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("GW-001", "98.50", "USD", "2026-01-15", "deposit-alpha").
				AddRow("GW-002", "246.25", "USD", "2026-01-16", "deposit-beta").
				AddRow("GW-003", "73.88", "USD", "2026-01-17", "deposit-gamma").
				Build()

			bankJob, err := c.Ingestion.UploadCSV(
				ctx, reconciliationContext.ID, bankSource.ID, "bank.csv", bankCSV,
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, c, reconciliationContext.ID, bankJob.ID))

			// Step 8: Run matching
			tc.Logf("Step 8: Running matching")
			matchResp, err := c.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForMatchRunComplete(ctx, tc, c, reconciliationContext.ID, matchResp.RunID))

			// Step 9: Verify results
			tc.Logf("Step 9: Verifying match results")
			matchRun, err := c.Matching.GetMatchRun(ctx, reconciliationContext.ID, matchResp.RunID)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", matchRun.Status)

			groups, err := c.Matching.GetMatchRunResults(ctx, reconciliationContext.ID, matchResp.RunID)
			require.NoError(t, err)
			tc.Logf("Found %d match groups", len(groups))

			// With NET normalization: gateway $100 becomes $98.50 internally,
			// which matches bank $98.50. All 3 should match.
			require.Len(t, groups, 3,
				"all 3 transactions should match when fees are normalized to net")
			for _, group := range groups {
				require.Len(t, group.Items, 2)
			}

			tc.Logf("PASS: NET normalization matching — 3 gross/net pairs matched")
		},
	)
}

// ---------------------------------------------------------------------------
// Cascading Fee Schedule — Matching
// ---------------------------------------------------------------------------

// TestFeeSchedule_CascadingNormalization tests that CASCADING fee schedules
// produce correct normalization during matching.
func TestFeeSchedule_CascadingNormalization(t *testing.T) {
	e2e.RunE2EWithTimeout(t, 3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, c *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, c)

			// CASCADING: 2% on gross, then 1% on remainder
			// For $500.00:
			//   fee1 = 500 * 0.02 = 10.00, remaining = 490.00
			//   fee2 = 490 * 0.01 = 4.90,  remaining = 485.10
			//   totalFee = 14.90, net = 485.10
			tc.Logf("Step 1: Creating cascading fee schedule (2%% + 1%%)")
			schedule := f.FeeSchedule.NewFeeSchedule().
				WithName("cascading-match").
				WithCurrency("USD").
				Cascading().
				WithPercentageFee("primary", 1, "0.02").
				WithPercentageFee("secondary", 2, "0.01").
				MustCreate(ctx)

			// Verify with simulate first
			sim, err := c.FeeSchedule.SimulateFeeSchedule(ctx, schedule.ID, client.SimulateFeeRequest{
				GrossAmount: "500.00",
				Currency:    "USD",
			})
			require.NoError(t, err)
			tc.Logf("Simulation: gross=%s, net=%s, fee=%s", sim.GrossAmount, sim.NetAmount, sim.TotalFee)

			// Step 2: Context with NET normalization
			tc.Logf("Step 2: Creating context with NET normalization")
			reconciliationContext := f.Context.NewContext().
				WithName("cascading-match").
				OneToOne().
				WithFeeNormalization("NET").
				MustCreate(ctx)

			// Step 3: Sources
			tc.Logf("Step 3: Creating sources")
			gatewaySource := f.Source.NewSource(reconciliationContext.ID).
				WithName("gateway").
				AsGateway().
				MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").
				AsLedger().
				MustCreate(ctx)

			// Field maps
			f.Source.NewFieldMap(reconciliationContext.ID, gatewaySource.ID).
				WithStandardMapping().MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().MustCreate(ctx)

			f.FeeRule.NewFeeRule(reconciliationContext.ID).
				WithName("gateway-cascading").
				Left().
				WithFeeScheduleID(schedule.ID).
				WithPriority(1).
				MustCreate(ctx)

			// Tolerance rule
			f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(1).
				Tolerance().WithToleranceConfig("0.01").MustCreate(ctx)

			// Step 4: Upload gateway (gross) and bank (net after cascading)
			tc.Logf("Step 4: Uploading transactions")
			gatewayCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("CSC-001", "500.00", "USD", "2026-02-01", "order-100").
				Build()
			gatewayJob, err := c.Ingestion.UploadCSV(
				ctx, reconciliationContext.ID, gatewaySource.ID, "gateway.csv", gatewayCSV,
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, c, reconciliationContext.ID, gatewayJob.ID))

			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("CSC-001", "485.10", "USD", "2026-02-01", "deposit-100").
				Build()
			bankJob, err := c.Ingestion.UploadCSV(
				ctx, reconciliationContext.ID, bankSource.ID, "bank.csv", bankCSV,
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, c, reconciliationContext.ID, bankJob.ID))

			// Step 5: Match
			tc.Logf("Step 5: Running matching")
			matchResp, err := c.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForMatchRunComplete(ctx, tc, c, reconciliationContext.ID, matchResp.RunID))

			// Step 6: Verify
			groups, err := c.Matching.GetMatchRunResults(ctx, reconciliationContext.ID, matchResp.RunID)
			require.NoError(t, err)
			require.Len(t, groups, 1,
				"cascading-normalized gateway $500 should match bank $485.10")
			require.Len(t, groups[0].Items, 2)

			tc.Logf("PASS: Cascading normalization matching — gross $500 matched net $485.10")
		},
	)
}

// ---------------------------------------------------------------------------
// No Normalization — Amounts Unchanged
// ---------------------------------------------------------------------------

// TestFeeSchedule_NoNormalization tests that when feeNormalization is empty,
// fee rules do not alter matching amounts (transactions pass through as-is).
func TestFeeSchedule_NoNormalization(t *testing.T) {
	e2e.RunE2EWithTimeout(t, 3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, c *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, c)

			// Fee schedule and fee rule exist, but normalization is disabled on the context.
			schedule := f.FeeSchedule.NewFeeSchedule().
				WithName("no-norm").
				WithCurrency("USD").
				Parallel().
				WithPercentageFee("fee", 1, "0.10").
				MustCreate(ctx)

			reconciliationContext := f.Context.NewContext().
				WithName("no-norm-match").
				OneToOne().
				MustCreate(ctx) // No WithFeeNormalization — defaults to none

			gatewaySource := f.Source.NewSource(reconciliationContext.ID).
				WithName("gateway").
				AsGateway().
				MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").
				AsLedger().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, gatewaySource.ID).
				WithStandardMapping().MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().MustCreate(ctx)

			f.FeeRule.NewFeeRule(reconciliationContext.ID).
				WithName("no-norm-rule").
				Any().
				WithFeeScheduleID(schedule.ID).
				WithPriority(1).
				MustCreate(ctx)

			// Exact match rule — amounts must be exactly equal
			f.Rule.NewRule(reconciliationContext.ID).
				Exact().WithExactConfig(true, true).MustCreate(ctx)

			// Both sides report SAME amount (no normalization applied)
			gatewayCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("NN-001", "100.00", "USD", "2026-02-01", "payment").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("NN-001", "100.00", "USD", "2026-02-01", "deposit").
				Build()

			gatewayJob, err := c.Ingestion.UploadCSV(
				ctx, reconciliationContext.ID, gatewaySource.ID, "gateway.csv", gatewayCSV,
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, c, reconciliationContext.ID, gatewayJob.ID))

			bankJob, err := c.Ingestion.UploadCSV(
				ctx, reconciliationContext.ID, bankSource.ID, "bank.csv", bankCSV,
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, c, reconciliationContext.ID, bankJob.ID))

			matchResp, err := c.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForMatchRunComplete(ctx, tc, c, reconciliationContext.ID, matchResp.RunID))

			groups, err := c.Matching.GetMatchRunResults(ctx, reconciliationContext.ID, matchResp.RunID)
			require.NoError(t, err)
			require.Len(t, groups, 1,
				"exact same amounts should match when normalization is disabled")
			require.Len(t, groups[0].Items, 2)

			tc.Logf("PASS: No normalization — raw amounts compared, $100 == $100")
		},
	)
}

// ---------------------------------------------------------------------------
// Currency Mismatch Passthrough
// ---------------------------------------------------------------------------

// TestFeeSchedule_CurrencyMismatchPassthrough verifies that when a fee schedule
// currency (USD) doesn't match a transaction currency (EUR), the fee schedule
// is skipped and amounts pass through unchanged.
func TestFeeSchedule_CurrencyMismatchPassthrough(t *testing.T) {
	e2e.RunE2EWithTimeout(t, 3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, c *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, c)

			// USD fee schedule — should NOT apply to EUR transactions even when a fee rule points to it.
			schedule := f.FeeSchedule.NewFeeSchedule().
				WithName("usd-only").
				WithCurrency("USD").
				Parallel().
				WithPercentageFee("fee", 1, "0.50"). // 50% — would be very visible if applied
				MustCreate(ctx)

			reconciliationContext := f.Context.NewContext().
				WithName("ccy-mismatch").
				OneToOne().
				WithFeeNormalization("NET").
				MustCreate(ctx)

			gatewaySource := f.Source.NewSource(reconciliationContext.ID).
				WithName("gateway").
				AsGateway().
				MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").
				AsLedger().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, gatewaySource.ID).
				WithStandardMapping().MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().MustCreate(ctx)

			f.FeeRule.NewFeeRule(reconciliationContext.ID).
				WithName("usd-only-rule").
				Any().
				WithFeeScheduleID(schedule.ID).
				WithPriority(1).
				MustCreate(ctx)

			f.Rule.NewRule(reconciliationContext.ID).
				Exact().WithExactConfig(true, true).MustCreate(ctx)

			// EUR transactions — fee schedule (USD) shouldn't apply
			gatewayCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("EUR-001", "200.00", "EUR", "2026-02-01", "euro-payment").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("EUR-001", "200.00", "EUR", "2026-02-01", "euro-deposit").
				Build()

			gatewayJob, err := c.Ingestion.UploadCSV(
				ctx, reconciliationContext.ID, gatewaySource.ID, "gateway.csv", gatewayCSV,
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, c, reconciliationContext.ID, gatewayJob.ID))

			bankJob, err := c.Ingestion.UploadCSV(
				ctx, reconciliationContext.ID, bankSource.ID, "bank.csv", bankCSV,
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, c, reconciliationContext.ID, bankJob.ID))

			matchResp, err := c.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForMatchRunComplete(ctx, tc, c, reconciliationContext.ID, matchResp.RunID))

			groups, err := c.Matching.GetMatchRunResults(ctx, reconciliationContext.ID, matchResp.RunID)
			require.NoError(t, err)
			require.Len(t, groups, 1,
				"EUR transactions should match exactly when USD fee schedule is skipped")
			require.Len(t, groups[0].Items, 2)

			tc.Logf("PASS: Currency mismatch — USD fee schedule ignored for EUR transactions")
		},
	)
}

// ---------------------------------------------------------------------------
// Multiple Fee Schedules — Per-Side Rules
// ---------------------------------------------------------------------------

// TestFeeSchedule_PerSideFeeRules tests that different sides can use
// different fee schedules through LEFT/RIGHT fee rules.
func TestFeeSchedule_PerSideFeeRules(t *testing.T) {
	e2e.RunE2EWithTimeout(t, 3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, c *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, c)

			// Left side: 2% fee. For $100 gross -> net = $98.00
			scheduleA := f.FeeSchedule.NewFeeSchedule().
				WithName("gateway-a-fees").
				WithCurrency("USD").
				Parallel().
				WithPercentageFee("processing", 1, "0.02").
				MustCreate(ctx)

			// Right side: 3% fee. For $100 gross -> net = $97.00
			scheduleB := f.FeeSchedule.NewFeeSchedule().
				WithName("gateway-b-fees").
				WithCurrency("USD").
				Parallel().
				WithPercentageFee("processing", 1, "0.03").
				MustCreate(ctx)

			reconciliationContext := f.Context.NewContext().
				WithName("multi-source").
				OneToOne().
				WithFeeNormalization("NET").
				MustCreate(ctx)

			gatewayA := f.Source.NewSource(reconciliationContext.ID).
				WithName("gateway-a").
				AsLedger().
				MustCreate(ctx)

			gatewayB := f.Source.NewSource(reconciliationContext.ID).
				WithName("gateway-b").
				AsGateway().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, gatewayA.ID).
				WithStandardMapping().MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, gatewayB.ID).
				WithStandardMapping().MustCreate(ctx)

			f.FeeRule.NewFeeRule(reconciliationContext.ID).
				WithName("gateway-a-left").
				Left().
				WithFeeScheduleID(scheduleA.ID).
				WithPriority(1).
				MustCreate(ctx)
			f.FeeRule.NewFeeRule(reconciliationContext.ID).
				WithName("gateway-b-right").
				Right().
				WithFeeScheduleID(scheduleB.ID).
				WithPriority(2).
				MustCreate(ctx)

			// Tolerance rule to handle sub-cent differences
			f.Rule.NewRule(reconciliationContext.ID).
				Tolerance().WithToleranceConfig("0.01").MustCreate(ctx)

			// Left side reports $100 gross (net = $98.00)
			csvA := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("MS-001", "100.00", "USD", "2026-02-01", "gw-a-payment").
				Build()
			jobA, err := c.Ingestion.UploadCSV(
				ctx, reconciliationContext.ID, gatewayA.ID, "gw_a.csv", csvA,
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, c, reconciliationContext.ID, jobA.ID))

			// Right side reports $100 gross (net = $97.00) - different fee.
			csvB := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("MS-002", "100.00", "USD", "2026-02-01", "gw-b-payment").
				Build()
			jobB, err := c.Ingestion.UploadCSV(
				ctx, reconciliationContext.ID, gatewayB.ID, "gw_b.csv", csvB,
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, c, reconciliationContext.ID, jobB.ID))

			// Run matching - both gross $100 but different net amounts.
			// Should NOT match each other (98 != 97)
			matchResp, err := c.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForMatchRunComplete(ctx, tc, c, reconciliationContext.ID, matchResp.RunID))

			groups, err := c.Matching.GetMatchRunResults(ctx, reconciliationContext.ID, matchResp.RunID)
			require.NoError(t, err)

			// Both report $100 gross, but after normalization:
			// Left side: $98.00 net, right side: $97.00 net.
			// With tolerance 0.01, these should NOT match (difference = $1.00).
			tc.Logf("Match groups found: %d (expecting 0 - $98 vs $97 exceeds tolerance)", len(groups))
			assert.Equal(t, 0, len(groups),
				"per-side normalization should produce different net amounts that don't match")

			tc.Logf("PASS: Per-side fee rules produce different net amounts")
		},
	)
}
