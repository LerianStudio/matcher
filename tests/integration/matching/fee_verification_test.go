//go:build integration

package matching

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTxRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	ingestionEntities "github.com/LerianStudio/matcher/internal/ingestion/domain/entities"

	matchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"

	feeVarianceRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/fee_variance"
	matchGroupRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_group"
	matchRunRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_run"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

// feeVarianceTestPrereqs holds shared prerequisite entities needed by all fee variance tests.
type feeVarianceTestPrereqs struct {
	ContextID     uuid.UUID
	RunID         uuid.UUID
	GroupID       uuid.UUID
	TransactionID uuid.UUID
	RateID        uuid.UUID
}

// setupFeeVariancePrereqs creates the full prerequisite chain for fee variance persistence:
// rule → ingestion job → transaction → match run → match group, plus a raw rate record.
func setupFeeVariancePrereqs(
	t *testing.T,
	h *integration.TestHarness,
) feeVarianceTestPrereqs {
	t.Helper()

	ctx := h.Ctx()
	provider := h.Provider()

	// 1. Create a match rule (FK for match_group).
	ruleRepo := matchRuleRepo.NewRepository(provider)

	rule, err := configEntities.NewMatchRule(
		ctx,
		h.Seed.ContextID,
		configEntities.CreateMatchRuleInput{
			Priority: 1,
			Type:     configVO.RuleTypeExact,
			Config:   map[string]any{"matchCurrency": true},
		},
	)
	require.NoError(t, err)

	createdRule, err := ruleRepo.Create(ctx, rule)
	require.NoError(t, err)

	// 2. Create ingestion job + transaction (FK for match_fee_variances.transaction_id).
	jRepo := ingestionJobRepo.NewRepository(provider)
	tRepo := ingestionTxRepo.NewRepository(provider)

	job, err := ingestionEntities.NewIngestionJob(
		ctx,
		h.Seed.ContextID,
		h.Seed.SourceID,
		"fee_variance_test.csv",
		100,
	)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	require.NoError(t, job.Complete(ctx, 1, 0))

	createdJob, err := jRepo.Create(ctx, job)
	require.NoError(t, err)

	tx, err := shared.NewTransaction(
		createdJob.ID,
		h.Seed.SourceID,
		"FEE-VAR-TX-"+uuid.NewString()[:8],
		decimal.NewFromFloat(100.00),
		"USD",
		time.Now().UTC(),
		"fee variance test tx",
		map[string]any{},
	)
	require.NoError(t, err)

	tx.ExtractionStatus = shared.ExtractionStatusComplete

	createdTx, err := tRepo.Create(ctx, tx)
	require.NoError(t, err)

	// 3. Create match run.
	runRepo := matchRunRepo.NewRepository(provider)

	run, err := matchingEntities.NewMatchRun(
		ctx,
		h.Seed.ContextID,
		matchingVO.MatchRunModeCommit,
	)
	require.NoError(t, err)

	createdRun, err := runRepo.Create(ctx, run)
	require.NoError(t, err)

	// 4. Create match group (needs run + rule + items).
	grpRepo := matchGroupRepo.NewRepository(provider)

	confidence, err := matchingVO.ParseConfidenceScore(90)
	require.NoError(t, err)

	itemA, err := matchingEntities.NewMatchItem(
		ctx,
		createdTx.ID,
		decimal.NewFromInt(100),
		"USD",
		decimal.NewFromInt(100),
	)
	require.NoError(t, err)

	// second item uses a synthetic transaction ID – we insert a second tx for FK integrity.
	tx2, err := shared.NewTransaction(
		createdJob.ID,
		h.Seed.SourceID,
		"FEE-VAR-TX2-"+uuid.NewString()[:8],
		decimal.NewFromFloat(100.00),
		"USD",
		time.Now().UTC(),
		"fee variance test tx 2",
		map[string]any{},
	)
	require.NoError(t, err)

	tx2.ExtractionStatus = shared.ExtractionStatusComplete

	createdTx2, err := tRepo.Create(ctx, tx2)
	require.NoError(t, err)

	itemB, err := matchingEntities.NewMatchItem(
		ctx,
		createdTx2.ID,
		decimal.NewFromInt(100),
		"USD",
		decimal.NewFromInt(100),
	)
	require.NoError(t, err)

	group, err := matchingEntities.NewMatchGroup(
		ctx,
		h.Seed.ContextID,
		createdRun.ID,
		createdRule.ID,
		confidence,
		[]*matchingEntities.MatchItem{itemA, itemB},
	)
	require.NoError(t, err)

	createdGroups, err := grpRepo.CreateBatch(ctx, []*matchingEntities.MatchGroup{group})
	require.NoError(t, err)
	require.Len(t, createdGroups, 1)

	// 5. Insert a rate row via raw SQL (no Create method on rate repo).
	rateID := uuid.New()

	_, err = pgcommon.WithTenantTx(ctx, h.Connection, func(sqlTx *sql.Tx) (struct{}, error) {
		_, execErr := sqlTx.ExecContext(ctx,
			`INSERT INTO rates (id, tenant_id, name, currency, structure_type, structure, created_at, updated_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`,
			rateID.String(),
			h.Seed.TenantID.String(),
			"test-rate-"+uuid.NewString()[:8],
			"USD",
			"FLAT",
			`{"amount":"1.50"}`,
			time.Now().UTC(),
		)

		return struct{}{}, execErr
	})
	require.NoError(t, err)

	return feeVarianceTestPrereqs{
		ContextID:     h.Seed.ContextID,
		RunID:         createdRun.ID,
		GroupID:       createdGroups[0].ID,
		TransactionID: createdTx.ID,
		RateID:        rateID,
	}
}

// feeVarianceRow holds the columns read back from match_fee_variances for assertion.
type feeVarianceRow struct {
	ID            uuid.UUID
	ContextID     uuid.UUID
	RunID         uuid.UUID
	MatchGroupID  uuid.UUID
	TransactionID uuid.UUID
	RateID        uuid.UUID
	Currency      string
	ExpectedFee   decimal.Decimal
	ActualFee     decimal.Decimal
	Delta         decimal.Decimal
	ToleranceAbs  decimal.Decimal
	TolerancePct  decimal.Decimal
	VarianceType  string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// readFeeVariancesByRunID reads all fee variance rows for a given run from the DB.
func readFeeVariancesByRunID(
	t *testing.T,
	h *integration.TestHarness,
	runID uuid.UUID,
) []feeVarianceRow {
	t.Helper()

	ctx := h.Ctx()

	rows, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) ([]feeVarianceRow, error) {
		sqlRows, queryErr := tx.QueryContext(ctx,
			`SELECT id, context_id, run_id, match_group_id, transaction_id, rate_id,
			        currency, expected_fee_amount, actual_fee_amount, delta,
			        tolerance_abs, tolerance_percent, variance_type, created_at, updated_at
			   FROM match_fee_variances
			  WHERE run_id = $1
			  ORDER BY created_at`, runID.String())
		if queryErr != nil {
			return nil, queryErr
		}
		defer func() {
			if closeErr := sqlRows.Close(); closeErr != nil {
				t.Logf("sqlRows.Close() error: %v", closeErr)
			}
		}()

		var result []feeVarianceRow

		for sqlRows.Next() {
			var r feeVarianceRow

			var idStr, ctxStr, runStr, grpStr, txStr, rateStr string

			if scanErr := sqlRows.Scan(
				&idStr, &ctxStr, &runStr, &grpStr, &txStr, &rateStr,
				&r.Currency, &r.ExpectedFee, &r.ActualFee, &r.Delta,
				&r.ToleranceAbs, &r.TolerancePct, &r.VarianceType,
				&r.CreatedAt, &r.UpdatedAt,
			); scanErr != nil {
				return nil, scanErr
			}

			r.ID = uuid.MustParse(idStr)
			r.ContextID = uuid.MustParse(ctxStr)
			r.RunID = uuid.MustParse(runStr)
			r.MatchGroupID = uuid.MustParse(grpStr)
			r.TransactionID = uuid.MustParse(txStr)
			r.RateID = uuid.MustParse(rateStr)
			result = append(result, r)
		}

		return result, sqlRows.Err()
	})
	require.NoError(t, err)

	return rows
}

// createAndCommitVariance creates fee variance entities via the repo within a managed transaction.
func createAndCommitVariance(
	t *testing.T,
	h *integration.TestHarness,
	variances []*matchingEntities.FeeVariance,
) []*matchingEntities.FeeVariance {
	t.Helper()

	ctx := h.Ctx()
	provider := h.Provider()
	repo := feeVarianceRepo.NewRepository(provider)

	tx, cancel, err := pgcommon.BeginTenantTx(ctx, provider)
	require.NoError(t, err)

	defer cancel()

	created, err := repo.CreateBatchWithTx(ctx, tx, variances)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	return created
}

// TestFeeVariance_CreateAndRetrieve verifies that a single fee variance record
// round-trips through CreateBatchWithTx and can be read back with all fields intact.
func TestFeeVariance_CreateAndRetrieve(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		prereqs := setupFeeVariancePrereqs(t, h)

		variance, err := matchingEntities.NewFeeVariance(
			ctx,
			prereqs.ContextID,
			prereqs.RunID,
			prereqs.GroupID,
			prereqs.TransactionID,
			prereqs.RateID,
			"USD",
			decimal.NewFromFloat(10.00), // expected
			decimal.NewFromFloat(10.50), // actual
			decimal.NewFromFloat(0.05),  // toleranceAbs
			decimal.NewFromFloat(1.0),   // tolerancePct
			"OVERCHARGE",
		)
		require.NoError(t, err)

		created := createAndCommitVariance(t, h, []*matchingEntities.FeeVariance{variance})
		require.Len(t, created, 1)

		rows := readFeeVariancesByRunID(t, h, prereqs.RunID)
		require.Len(t, rows, 1)

		row := rows[0]
		require.Equal(t, variance.ID, row.ID)
		require.Equal(t, prereqs.ContextID, row.ContextID)
		require.Equal(t, prereqs.RunID, row.RunID)
		require.Equal(t, prereqs.GroupID, row.MatchGroupID)
		require.Equal(t, prereqs.TransactionID, row.TransactionID)
		require.Equal(t, prereqs.RateID, row.RateID)
		require.Equal(t, "USD", row.Currency)
		require.True(t, decimal.NewFromFloat(10.00).Equal(row.ExpectedFee))
		require.True(t, decimal.NewFromFloat(10.50).Equal(row.ActualFee))
		// Delta is |expected - actual| = 0.50
		require.True(t, decimal.NewFromFloat(0.50).Equal(row.Delta))
		require.True(t, decimal.NewFromFloat(0.05).Equal(row.ToleranceAbs))
		require.True(t, decimal.NewFromFloat(1.0).Equal(row.TolerancePct))
		require.Equal(t, "OVERCHARGE", row.VarianceType)
		require.False(t, row.CreatedAt.IsZero())
		require.False(t, row.UpdatedAt.IsZero())
	})
}

// TestFeeVariance_MultipleFeeVariancesPerRun creates several fee variance records
// across different match groups within the same run and verifies all are retrievable.
func TestFeeVariance_MultipleFeeVariancesPerRun(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		prereqs := setupFeeVariancePrereqs(t, h)

		// Create a second match group under the same run for the second variance.
		provider := h.Provider()
		grpRepo := matchGroupRepo.NewRepository(provider)
		tRepo := ingestionTxRepo.NewRepository(provider)

		// Need two more transactions for the second group's items.
		jRepo := ingestionJobRepo.NewRepository(provider)

		job, err := ingestionEntities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"fee_multi_test.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 2, 0))

		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		extraTx1, err := shared.NewTransaction(
			createdJob.ID, h.Seed.SourceID,
			"FEE-MULTI-A-"+uuid.NewString()[:8],
			decimal.NewFromFloat(200.00), "USD", time.Now().UTC(),
			"multi test tx a", map[string]any{},
		)
		require.NoError(t, err)
		extraTx1.ExtractionStatus = shared.ExtractionStatusComplete
		createdExtraTx1, err := tRepo.Create(ctx, extraTx1)
		require.NoError(t, err)

		extraTx2, err := shared.NewTransaction(
			createdJob.ID, h.Seed.SourceID,
			"FEE-MULTI-B-"+uuid.NewString()[:8],
			decimal.NewFromFloat(200.00), "USD", time.Now().UTC(),
			"multi test tx b", map[string]any{},
		)
		require.NoError(t, err)
		extraTx2.ExtractionStatus = shared.ExtractionStatusComplete
		createdExtraTx2, err := tRepo.Create(ctx, extraTx2)
		require.NoError(t, err)

		ruleRepo := matchRuleRepo.NewRepository(provider)

		rule2, err := configEntities.NewMatchRule(
			ctx,
			h.Seed.ContextID,
			configEntities.CreateMatchRuleInput{
				Priority: 2,
				Type:     configVO.RuleTypeTolerance,
				Config:   map[string]any{"absTolerance": "0.01"},
			},
		)
		require.NoError(t, err)

		createdRule2, err := ruleRepo.Create(ctx, rule2)
		require.NoError(t, err)

		confidence, err := matchingVO.ParseConfidenceScore(85)
		require.NoError(t, err)

		item1, err := matchingEntities.NewMatchItem(ctx, createdExtraTx1.ID, decimal.NewFromInt(200), "USD", decimal.NewFromInt(200))
		require.NoError(t, err)

		item2, err := matchingEntities.NewMatchItem(ctx, createdExtraTx2.ID, decimal.NewFromInt(200), "USD", decimal.NewFromInt(200))
		require.NoError(t, err)

		group2, err := matchingEntities.NewMatchGroup(
			ctx,
			h.Seed.ContextID,
			prereqs.RunID,
			createdRule2.ID,
			confidence,
			[]*matchingEntities.MatchItem{item1, item2},
		)
		require.NoError(t, err)

		createdGroups, err := grpRepo.CreateBatch(ctx, []*matchingEntities.MatchGroup{group2})
		require.NoError(t, err)
		require.Len(t, createdGroups, 1)

		group2ID := createdGroups[0].ID

		// Create three variances: two for the original group, one for the new group.
		v1, err := matchingEntities.NewFeeVariance(
			ctx, prereqs.ContextID, prereqs.RunID, prereqs.GroupID, prereqs.TransactionID, prereqs.RateID,
			"USD", decimal.NewFromFloat(5.00), decimal.NewFromFloat(5.25),
			decimal.NewFromFloat(0.10), decimal.NewFromFloat(2.0), "OVERCHARGE",
		)
		require.NoError(t, err)

		v2, err := matchingEntities.NewFeeVariance(
			ctx, prereqs.ContextID, prereqs.RunID, prereqs.GroupID, prereqs.TransactionID, prereqs.RateID,
			"USD", decimal.NewFromFloat(3.00), decimal.NewFromFloat(2.80),
			decimal.NewFromFloat(0.50), decimal.NewFromFloat(5.0), "UNDERCHARGE",
		)
		require.NoError(t, err)

		v3, err := matchingEntities.NewFeeVariance(
			ctx, prereqs.ContextID, prereqs.RunID, group2ID, createdExtraTx1.ID, prereqs.RateID,
			"USD", decimal.NewFromFloat(8.00), decimal.NewFromFloat(8.00),
			decimal.NewFromFloat(0.01), decimal.NewFromFloat(0.5), "MATCH",
		)
		require.NoError(t, err)

		created := createAndCommitVariance(t, h, []*matchingEntities.FeeVariance{v1, v2, v3})
		require.Len(t, created, 3)

		rows := readFeeVariancesByRunID(t, h, prereqs.RunID)
		require.Len(t, rows, 3)

		// Verify groups are represented.
		groupIDs := make(map[uuid.UUID]int)
		for _, row := range rows {
			groupIDs[row.MatchGroupID]++
		}

		require.Equal(t, 2, groupIDs[prereqs.GroupID])
		require.Equal(t, 1, groupIDs[group2ID])
	})
}

// TestFeeVariance_WithinTolerance creates a fee variance where the delta is within
// the absolute tolerance threshold and verifies persistence.
func TestFeeVariance_WithinTolerance(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		prereqs := setupFeeVariancePrereqs(t, h)

		// expected=10.00, actual=10.02, delta=0.02, toleranceAbs=0.05
		// delta (0.02) < toleranceAbs (0.05) → within tolerance
		variance, err := matchingEntities.NewFeeVariance(
			ctx,
			prereqs.ContextID,
			prereqs.RunID,
			prereqs.GroupID,
			prereqs.TransactionID,
			prereqs.RateID,
			"USD",
			decimal.NewFromFloat(10.00),
			decimal.NewFromFloat(10.02),
			decimal.NewFromFloat(0.05),
			decimal.NewFromFloat(1.0),
			"MATCH",
		)
		require.NoError(t, err)

		// Constructor computes delta = |10.00 - 10.02| = 0.02
		require.True(t, decimal.NewFromFloat(0.02).Equal(variance.Delta),
			"constructor should compute delta = |expected - actual|")

		createAndCommitVariance(t, h, []*matchingEntities.FeeVariance{variance})

		rows := readFeeVariancesByRunID(t, h, prereqs.RunID)
		require.Len(t, rows, 1)

		row := rows[0]
		require.True(t, decimal.NewFromFloat(0.02).Equal(row.Delta))
		require.True(t, decimal.NewFromFloat(0.05).Equal(row.ToleranceAbs))
		// Confirm the delta is less than tolerance (business meaning: within tolerance).
		require.True(t, row.Delta.LessThan(row.ToleranceAbs),
			"delta %s should be less than tolerance %s", row.Delta, row.ToleranceAbs)
		require.Equal(t, "MATCH", row.VarianceType)
	})
}

// TestFeeVariance_ExceedsTolerance creates a fee variance where the delta exceeds
// the absolute tolerance threshold and verifies persistence.
func TestFeeVariance_ExceedsTolerance(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		prereqs := setupFeeVariancePrereqs(t, h)

		// expected=10.00, actual=11.50, delta=1.50, toleranceAbs=0.05
		// delta (1.50) >> toleranceAbs (0.05) → exceeds tolerance
		variance, err := matchingEntities.NewFeeVariance(
			ctx,
			prereqs.ContextID,
			prereqs.RunID,
			prereqs.GroupID,
			prereqs.TransactionID,
			prereqs.RateID,
			"USD",
			decimal.NewFromFloat(10.00),
			decimal.NewFromFloat(11.50),
			decimal.NewFromFloat(0.05),
			decimal.NewFromFloat(0.5),
			"OVERCHARGE",
		)
		require.NoError(t, err)

		// Constructor computes delta = |10.00 - 11.50| = 1.50
		require.True(t, decimal.NewFromFloat(1.50).Equal(variance.Delta),
			"constructor should compute delta = |expected - actual|")

		createAndCommitVariance(t, h, []*matchingEntities.FeeVariance{variance})

		rows := readFeeVariancesByRunID(t, h, prereqs.RunID)
		require.Len(t, rows, 1)

		row := rows[0]
		require.True(t, decimal.NewFromFloat(10.00).Equal(row.ExpectedFee))
		require.True(t, decimal.NewFromFloat(11.50).Equal(row.ActualFee))
		require.True(t, decimal.NewFromFloat(1.50).Equal(row.Delta))
		require.True(t, decimal.NewFromFloat(0.05).Equal(row.ToleranceAbs))
		// Confirm the delta exceeds tolerance (business meaning: overcharged).
		require.True(t, row.Delta.GreaterThan(row.ToleranceAbs),
			"delta %s should exceed tolerance %s", row.Delta, row.ToleranceAbs)
		require.Equal(t, "OVERCHARGE", row.VarianceType)
	})
}

// TestFeeVariance_ZeroDelta creates a fee variance where expected equals actual (no discrepancy).
// This exercises the edge case of a "no variance" record.
func TestFeeVariance_ZeroDelta(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		prereqs := setupFeeVariancePrereqs(t, h)

		expected := decimal.NewFromFloat(7.50)
		actual := decimal.NewFromFloat(7.50)

		variance, err := matchingEntities.NewFeeVariance(
			ctx,
			prereqs.ContextID,
			prereqs.RunID,
			prereqs.GroupID,
			prereqs.TransactionID,
			prereqs.RateID,
			"EUR",
			expected,
			actual,
			decimal.NewFromFloat(0.01),
			decimal.NewFromFloat(0.5),
			"MATCH",
		)
		require.NoError(t, err)

		// Constructor computes delta = |7.50 - 7.50| = 0
		require.True(t, decimal.Zero.Equal(variance.Delta),
			"constructor should compute delta = 0 when expected equals actual")

		createAndCommitVariance(t, h, []*matchingEntities.FeeVariance{variance})

		rows := readFeeVariancesByRunID(t, h, prereqs.RunID)
		require.Len(t, rows, 1)

		row := rows[0]
		require.True(t, expected.Equal(row.ExpectedFee))
		require.True(t, actual.Equal(row.ActualFee))
		require.True(t, decimal.Zero.Equal(row.Delta),
			"persisted delta should be zero")
		require.Equal(t, "EUR", row.Currency)
		require.Equal(t, "MATCH", row.VarianceType)
	})
}

// TestFeeVariance_PercentageTolerance creates a fee variance with a percentage tolerance value
// and verifies that TolerancePct is stored and retrieved correctly.
func TestFeeVariance_PercentageTolerance(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := h.Ctx()
		prereqs := setupFeeVariancePrereqs(t, h)

		tolerancePct := decimal.NewFromFloat(2.5) // 2.5%
		expected := decimal.NewFromFloat(20.00)
		actual := decimal.NewFromFloat(20.40) // 2% over → within 2.5% tolerance

		variance, err := matchingEntities.NewFeeVariance(
			ctx,
			prereqs.ContextID,
			prereqs.RunID,
			prereqs.GroupID,
			prereqs.TransactionID,
			prereqs.RateID,
			"USD",
			expected,
			actual,
			decimal.NewFromFloat(1.00), // toleranceAbs
			tolerancePct,
			"MATCH",
		)
		require.NoError(t, err)

		// Constructor computes delta = |20.00 - 20.40| = 0.40
		require.True(t, decimal.NewFromFloat(0.40).Equal(variance.Delta))

		createAndCommitVariance(t, h, []*matchingEntities.FeeVariance{variance})

		rows := readFeeVariancesByRunID(t, h, prereqs.RunID)
		require.Len(t, rows, 1)

		row := rows[0]
		require.True(t, tolerancePct.Equal(row.TolerancePct),
			"expected tolerance_pct=%s, got=%s", tolerancePct, row.TolerancePct)
		require.True(t, decimal.NewFromFloat(1.00).Equal(row.ToleranceAbs))
		require.True(t, decimal.NewFromFloat(0.40).Equal(row.Delta))
		require.Equal(t, "USD", row.Currency)
		require.Equal(t, "MATCH", row.VarianceType)

		// Verify the percentage-based tolerance check: delta/expected * 100 < tolerancePct
		pctDeviation := row.Delta.Div(row.ExpectedFee).Mul(decimal.NewFromInt(100))
		require.True(t, pctDeviation.LessThan(tolerancePct),
			"percentage deviation %s%% should be within tolerance %s%%", pctDeviation, tolerancePct)
	})
}
