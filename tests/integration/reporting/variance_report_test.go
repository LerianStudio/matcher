//go:build integration

package reporting

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	reportRepo "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/report"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/tests/integration"
)

// varianceFeeVariance groups the parameters for a single match_fee_variances row.
type varianceFeeVariance struct {
	SourceID     uuid.UUID
	ScheduleID   uuid.UUID
	Currency     string
	ScheduleName string
	Expected     decimal.Decimal
	Actual       decimal.Decimal
	Delta        decimal.Decimal
}

// varianceFeeScheduleKey uniquely identifies a fee schedule by (schedule_name, currency).
type varianceFeeScheduleKey struct {
	scheduleID   uuid.UUID
	scheduleName string
	currency     string
}

func varianceScheduleKey(variance varianceFeeVariance) varianceFeeScheduleKey {
	if variance.ScheduleID != uuid.Nil {
		return varianceFeeScheduleKey{scheduleID: variance.ScheduleID, currency: variance.Currency}
	}

	return varianceFeeScheduleKey{scheduleName: variance.ScheduleName, currency: variance.Currency}
}

// varianceSeedContext holds pre-computed lookup maps for the FK chain.
type varianceSeedContext struct {
	feeScheduleIDs map[varianceFeeScheduleKey]uuid.UUID
	jobIDs         map[uuid.UUID]uuid.UUID
	runID          uuid.UUID
	ruleID         uuid.UUID
}

// insertVarianceData inserts the full FK chain (fee_schedules → ingestion_jobs → transactions →
// match_runs → match_groups → match_items → match_fee_variances) for a set of fee
// variance records. Each varianceFeeVariance produces one transaction and one fee
// variance row. A shared fee schedule per (ScheduleName, Currency) pair is auto-created.
//
// createdAt controls `match_fee_variances.created_at` (the column the repository
// filters on).
func insertVarianceData(
	t *testing.T,
	harness *integration.TestHarness,
	seed dashboardSeed,
	variances []varianceFeeVariance,
	createdAt time.Time,
) {
	t.Helper()

	ctx := testCtx(t, harness)

	_, err := pgcommon.WithTenantTx(ctx, harness.Connection, func(tx *sql.Tx) (struct{}, error) {
		sc, err := insertVarianceParents(ctx, tx, seed, variances, createdAt)
		if err != nil {
			return struct{}{}, err
		}

		for i, variance := range variances {
			if err := insertSingleVariance(ctx, tx, seed, sc, i, variance, createdAt); err != nil {
				return struct{}{}, err
			}
		}

		return struct{}{}, nil
	})
	require.NoError(t, err)
}

// insertVarianceParents creates fee schedules, ingestion jobs, and a match run—the
// parent rows required before inserting individual variances.
func insertVarianceParents(
	ctx context.Context,
	tx *sql.Tx,
	seed dashboardSeed,
	variances []varianceFeeVariance,
	createdAt time.Time,
) (varianceSeedContext, error) {
	sc := varianceSeedContext{
		feeScheduleIDs: make(map[varianceFeeScheduleKey]uuid.UUID),
		jobIDs:         make(map[uuid.UUID]uuid.UUID),
		ruleID:         seed.RuleID,
	}

	// Deduplicate fee schedules by (schedule_name, currency).
	for _, variance := range variances {
		key := varianceScheduleKey(variance)
		if _, exists := sc.feeScheduleIDs[key]; exists {
			continue
		}

		feeScheduleID := variance.ScheduleID
		if feeScheduleID == uuid.Nil {
			feeScheduleID = uuid.New()
		}
		sc.feeScheduleIDs[key] = feeScheduleID

		_, err := tx.ExecContext(ctx, `
			INSERT INTO fee_schedules (id, tenant_id, name, currency, application_order, rounding_scale, rounding_mode, created_at, updated_at)
			VALUES ($1, $2, $3, $4, 'PARALLEL', 2, 'HALF_UP', $5, $5)
		`, feeScheduleID, seed.TenantID, variance.ScheduleName, variance.Currency, createdAt)
		if err != nil {
			return sc, fmt.Errorf("insert fee schedule: %w", err)
		}
	}

	// Deduplicate ingestion jobs by source_id.
	for _, variance := range variances {
		if _, exists := sc.jobIDs[variance.SourceID]; exists {
			continue
		}

		jobID := uuid.New()
		sc.jobIDs[variance.SourceID] = jobID

		_, err := tx.ExecContext(ctx, `
			INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
			VALUES ($1, $2, $3, 'COMPLETED', $4, '{}')
		`, jobID, seed.ContextID, variance.SourceID, createdAt)
		if err != nil {
			return sc, fmt.Errorf("insert ingestion_job: %w", err)
		}
	}

	// Single match run for all variances.
	sc.runID = uuid.New()

	_, err := tx.ExecContext(ctx, `
		INSERT INTO match_runs (id, context_id, mode, status, started_at, stats)
		VALUES ($1, $2, 'COMMIT', 'COMPLETED', $3, '{}')
	`, sc.runID, seed.ContextID, createdAt)
	if err != nil {
		return sc, fmt.Errorf("insert match_run: %w", err)
	}

	return sc, nil
}

// insertSingleVariance creates one transaction + match_group + match_item +
// match_fee_variance row.
func insertSingleVariance(
	ctx context.Context,
	tx *sql.Tx,
	seed dashboardSeed,
	sc varianceSeedContext,
	idx int,
	feeVar varianceFeeVariance,
	createdAt time.Time,
) error {
	txID := uuid.New()
	amount := feeVar.Expected

	_, err := tx.ExecContext(ctx, `
		INSERT INTO transactions
			(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'COMPLETE', 'MATCHED', $8, $8)
	`, txID, sc.jobIDs[feeVar.SourceID], feeVar.SourceID,
		fmt.Sprintf("VAR-%d-%s", idx, txID.String()[:8]),
		amount, feeVar.Currency, createdAt, createdAt)
	if err != nil {
		return fmt.Errorf("insert transaction %d: %w", idx, err)
	}

	groupID := uuid.New()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO match_groups (id, context_id, run_id, rule_id, confidence, status)
		VALUES ($1, $2, $3, $4, 95, 'CONFIRMED')
	`, groupID, seed.ContextID, sc.runID, sc.ruleID)
	if err != nil {
		return fmt.Errorf("insert match_group %d: %w", idx, err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO match_items (id, match_group_id, transaction_id, allocated_amount, allocated_currency)
		VALUES ($1, $2, $3, $4, $5)
	`, uuid.New(), groupID, txID, amount, feeVar.Currency)
	if err != nil {
		return fmt.Errorf("insert match_item %d: %w", idx, err)
	}

	fsKey := varianceScheduleKey(feeVar)

	fsID, ok := sc.feeScheduleIDs[fsKey]
	if !ok {
		return fmt.Errorf("insert match_fee_variance %d: no fee schedule for key %+v", idx, fsKey)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO match_fee_variances
			(id, context_id, run_id, match_group_id, transaction_id, fee_schedule_id, fee_schedule_name_snapshot,
			 currency, expected_fee_amount, actual_fee_amount, delta,
			 tolerance_abs, tolerance_percent, variance_type, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
	`, uuid.New(), seed.ContextID, sc.runID, groupID, txID, fsID, feeVar.ScheduleName,
		feeVar.Currency, feeVar.Expected, feeVar.Actual, feeVar.Delta,
		decimal.NewFromFloat(0.01), decimal.NewFromFloat(0.05), varianceType(feeVar.Delta),
		createdAt, createdAt)
	if err != nil {
		return fmt.Errorf("insert match_fee_variance %d: %w", idx, err)
	}

	return nil
}

// varianceType derives MATCH|UNDERCHARGE|OVERCHARGE from the delta sign.
func varianceType(delta decimal.Decimal) string {
	if delta.IsZero() {
		return "MATCH"
	}

	if delta.IsNegative() {
		return "UNDERCHARGE"
	}

	return "OVERCHARGE"
}

// varianceRowKey returns the composite cursor key for a VarianceReportRow.
func varianceRowKey(row *entities.VarianceReportRow) string {
	return row.SourceID.String() + ":" + row.Currency + ":" + row.FeeScheduleID.String()
}

// TestVarianceReport_EmptyDatabase verifies that GetVarianceReport returns an
// empty slice and zero pagination when no fee variances exist for the filter.
func TestVarianceReport_EmptyDatabase(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		filter := entities.VarianceReportFilter{
			ContextID: h.Seed.ContextID,
			DateFrom:  time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2099, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		rows, pagination, err := repo.GetVarianceReport(ctx, filter)
		require.NoError(t, err)
		require.Empty(t, rows, "should return empty slice when no variances exist")
		require.Empty(t, pagination.Next, "next cursor should be empty")
		require.Empty(t, pagination.Prev, "prev cursor should be empty")
	})
}

// TestVarianceReport_SingleGroupAggregation inserts 3 fee variances with the
// same (source_id, currency, fee schedule) and verifies that GetVarianceReport
// returns a single aggregated row with correct SUMs.
func TestVarianceReport_SingleGroupAggregation(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		seed := seedDashboardConfig(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		baseDate := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)

		// 3 variances: same source, same currency, same fee schedule.
		variances := []varianceFeeVariance{
			{SourceID: seed.SourceID, Currency: "USD", ScheduleName: "Flat USD Standard", Expected: decimal.NewFromFloat(10.00), Actual: decimal.NewFromFloat(8.50), Delta: decimal.NewFromFloat(-1.50)},
			{SourceID: seed.SourceID, Currency: "USD", ScheduleName: "Flat USD Standard", Expected: decimal.NewFromFloat(20.00), Actual: decimal.NewFromFloat(22.00), Delta: decimal.NewFromFloat(2.00)},
			{SourceID: seed.SourceID, Currency: "USD", ScheduleName: "Flat USD Standard", Expected: decimal.NewFromFloat(15.00), Actual: decimal.NewFromFloat(15.00), Delta: decimal.NewFromFloat(0.00)},
		}

		insertVarianceData(t, h, seed, variances, baseDate)

		filter := entities.VarianceReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 6, 30, 23, 59, 59, 0, time.UTC),
		}

		rows, _, err := repo.GetVarianceReport(ctx, filter)
		require.NoError(t, err)
		require.Len(t, rows, 1, "3 variances with same group key should aggregate into 1 row")

		row := rows[0]
		require.Equal(t, seed.SourceID, row.SourceID)
		require.Equal(t, "USD", row.Currency)
		require.Equal(t, "Flat USD Standard", row.FeeScheduleName)

		expectedTotal := decimal.NewFromFloat(45.00) // 10 + 20 + 15
		actualTotal := decimal.NewFromFloat(45.50)   // 8.5 + 22 + 15
		varianceTotal := decimal.NewFromFloat(0.50)  // -1.5 + 2.0 + 0.0

		require.True(t, expectedTotal.Equal(row.TotalExpected),
			"TotalExpected should be 45.00, got %s", row.TotalExpected)
		require.True(t, actualTotal.Equal(row.TotalActual),
			"TotalActual should be 45.50, got %s", row.TotalActual)
		require.True(t, varianceTotal.Equal(row.NetVariance),
			"NetVariance should be 0.50, got %s", row.NetVariance)

		require.NotNil(t, row.VariancePct, "VariancePct should be calculated when TotalExpected is non-zero")
	})
}

// TestVarianceReport_MultipleGroups inserts variances for 2 sources × 2
// currencies and verifies that 4 distinct aggregated rows are returned.
func TestVarianceReport_MultipleGroups(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		seed := seedDashboardConfig(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		// Create a second source.
		source2ID := uuid.New()
		sourceCreatedAt := time.Now().UTC()
		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO reconciliation_sources (id, context_id, name, type, side, config, created_at, updated_at)
				VALUES ($1, $2, 'Variance Source 2', 'BANK', 'RIGHT', '{}', $3, $3)
			`, source2ID, seed.ContextID, sourceCreatedAt)

			return struct{}{}, execErr
		})
		require.NoError(t, err)

		baseDate := time.Date(2025, 7, 10, 8, 0, 0, 0, time.UTC)

		// 2 sources × 2 currencies, 2 variances per group = 8 rows, 4 groups.
		variances := []varianceFeeVariance{
			// Source1 + USD (2 rows)
			{SourceID: seed.SourceID, Currency: "USD", ScheduleName: "Flat USD Standard", Expected: decimal.NewFromFloat(100.00), Actual: decimal.NewFromFloat(90.00), Delta: decimal.NewFromFloat(-10.00)},
			{SourceID: seed.SourceID, Currency: "USD", ScheduleName: "Flat USD Standard", Expected: decimal.NewFromFloat(50.00), Actual: decimal.NewFromFloat(55.00), Delta: decimal.NewFromFloat(5.00)},
			// Source1 + EUR (2 rows)
			{SourceID: seed.SourceID, Currency: "EUR", ScheduleName: "Flat EUR Standard", Expected: decimal.NewFromFloat(200.00), Actual: decimal.NewFromFloat(200.00), Delta: decimal.NewFromFloat(0.00)},
			{SourceID: seed.SourceID, Currency: "EUR", ScheduleName: "Flat EUR Standard", Expected: decimal.NewFromFloat(30.00), Actual: decimal.NewFromFloat(28.00), Delta: decimal.NewFromFloat(-2.00)},
			// Source2 + USD (2 rows)
			{SourceID: source2ID, Currency: "USD", ScheduleName: "Flat USD Reserve", Expected: decimal.NewFromFloat(75.00), Actual: decimal.NewFromFloat(80.00), Delta: decimal.NewFromFloat(5.00)},
			{SourceID: source2ID, Currency: "USD", ScheduleName: "Flat USD Reserve", Expected: decimal.NewFromFloat(25.00), Actual: decimal.NewFromFloat(20.00), Delta: decimal.NewFromFloat(-5.00)},
			// Source2 + EUR (2 rows)
			{SourceID: source2ID, Currency: "EUR", ScheduleName: "Flat EUR Reserve", Expected: decimal.NewFromFloat(60.00), Actual: decimal.NewFromFloat(65.00), Delta: decimal.NewFromFloat(5.00)},
			{SourceID: source2ID, Currency: "EUR", ScheduleName: "Flat EUR Reserve", Expected: decimal.NewFromFloat(40.00), Actual: decimal.NewFromFloat(38.00), Delta: decimal.NewFromFloat(-2.00)},
		}

		insertVarianceData(t, h, seed, variances, baseDate)

		filter := entities.VarianceReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 7, 31, 23, 59, 59, 0, time.UTC),
			Limit:     10,
		}

		rows, _, err := repo.GetVarianceReport(ctx, filter)
		require.NoError(t, err)
		require.Len(t, rows, 4, "2 sources × 2 currencies should produce 4 aggregated rows")

		// Build a lookup keyed on (source_id, currency).
		type groupKey struct {
			sourceID uuid.UUID
			currency string
		}

		byGroup := make(map[groupKey]*entities.VarianceReportRow)
		for _, row := range rows {
			byGroup[groupKey{sourceID: row.SourceID, currency: row.Currency}] = row
		}

		// Source1 + USD: expected=150, actual=145, delta=-5
		s1usd := byGroup[groupKey{sourceID: seed.SourceID, currency: "USD"}]
		require.NotNil(t, s1usd, "Source1 USD group should exist")
		require.True(t, decimal.NewFromFloat(150.00).Equal(s1usd.TotalExpected), "S1 USD expected=%s", s1usd.TotalExpected)
		require.True(t, decimal.NewFromFloat(145.00).Equal(s1usd.TotalActual), "S1 USD actual=%s", s1usd.TotalActual)
		require.True(t, decimal.NewFromFloat(-5.00).Equal(s1usd.NetVariance), "S1 USD variance=%s", s1usd.NetVariance)

		// Source1 + EUR: expected=230, actual=228, delta=-2
		s1eur := byGroup[groupKey{sourceID: seed.SourceID, currency: "EUR"}]
		require.NotNil(t, s1eur, "Source1 EUR group should exist")
		require.True(t, decimal.NewFromFloat(230.00).Equal(s1eur.TotalExpected), "S1 EUR expected=%s", s1eur.TotalExpected)
		require.True(t, decimal.NewFromFloat(228.00).Equal(s1eur.TotalActual), "S1 EUR actual=%s", s1eur.TotalActual)
		require.True(t, decimal.NewFromFloat(-2.00).Equal(s1eur.NetVariance), "S1 EUR variance=%s", s1eur.NetVariance)

		// Source2 + USD: expected=100, actual=100, delta=0
		s2usd := byGroup[groupKey{sourceID: source2ID, currency: "USD"}]
		require.NotNil(t, s2usd, "Source2 USD group should exist")
		require.True(t, decimal.NewFromFloat(100.00).Equal(s2usd.TotalExpected), "S2 USD expected=%s", s2usd.TotalExpected)
		require.True(t, decimal.NewFromFloat(100.00).Equal(s2usd.TotalActual), "S2 USD actual=%s", s2usd.TotalActual)
		require.True(t, decimal.Zero.Equal(s2usd.NetVariance), "S2 USD variance=%s", s2usd.NetVariance)

		// Source2 + EUR: expected=100, actual=103, delta=3
		s2eur := byGroup[groupKey{sourceID: source2ID, currency: "EUR"}]
		require.NotNil(t, s2eur, "Source2 EUR group should exist")
		require.True(t, decimal.NewFromFloat(100.00).Equal(s2eur.TotalExpected), "S2 EUR expected=%s", s2eur.TotalExpected)
		require.True(t, decimal.NewFromFloat(103.00).Equal(s2eur.TotalActual), "S2 EUR actual=%s", s2eur.TotalActual)
		require.True(t, decimal.NewFromFloat(3.00).Equal(s2eur.NetVariance), "S2 EUR variance=%s", s2eur.NetVariance)
	})
}

// TestVarianceReport_ListVariancePage_Pagination inserts 5 distinct
// (source_id, currency, fee schedule) groups and pages through them with
// limit=2. Verifies that all 5 groups are returned across 3 pages and that
// the composite cursor key format is correct.
func TestVarianceReport_ListVariancePage_Pagination(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		seed := seedDashboardConfig(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		// Create a second source for diversity.
		source2ID := uuid.New()
		sourceCreatedAt := time.Now().UTC()
		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO reconciliation_sources (id, context_id, name, type, side, config, created_at, updated_at)
				VALUES ($1, $2, 'Variance Page Source 2', 'BANK', 'RIGHT', '{}', $3, $3)
			`, source2ID, seed.ContextID, sourceCreatedAt)

			return struct{}{}, execErr
		})
		require.NoError(t, err)

		baseDate := time.Date(2025, 8, 1, 12, 0, 0, 0, time.UTC)

		// 5 distinct groups, including two rows that differ only by fee schedule name.
		variances := []varianceFeeVariance{
			{SourceID: seed.SourceID, Currency: "USD", ScheduleName: "Visa Domestic", Expected: decimal.NewFromFloat(10.00), Actual: decimal.NewFromFloat(9.00), Delta: decimal.NewFromFloat(-1.00)},
			{SourceID: seed.SourceID, Currency: "USD", ScheduleName: "Visa Cross Border", Expected: decimal.NewFromFloat(20.00), Actual: decimal.NewFromFloat(18.00), Delta: decimal.NewFromFloat(-2.00)},
			{SourceID: seed.SourceID, Currency: "EUR", ScheduleName: "SEPA Standard", Expected: decimal.NewFromFloat(30.00), Actual: decimal.NewFromFloat(32.00), Delta: decimal.NewFromFloat(2.00)},
			{SourceID: source2ID, Currency: "USD", ScheduleName: "Mastercard Retail", Expected: decimal.NewFromFloat(40.00), Actual: decimal.NewFromFloat(40.00), Delta: decimal.NewFromFloat(0.00)},
			{SourceID: source2ID, Currency: "EUR", ScheduleName: "SEPA Priority", Expected: decimal.NewFromFloat(50.00), Actual: decimal.NewFromFloat(55.00), Delta: decimal.NewFromFloat(5.00)},
		}

		insertVarianceData(t, h, seed, variances, baseDate)

		filter := entities.VarianceReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 8, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 8, 31, 23, 59, 59, 0, time.UTC),
		}

		const pageSize = 2

		// Collect all rows across pages.
		var allRows []*entities.VarianceReportRow

		afterKey := ""
		pageCount := 0

		for {
			pageCount++

			require.LessOrEqual(t, pageCount, 5, "safety: too many pages (infinite loop guard)")

			rows, nextKey, err := repo.ListVariancePage(ctx, filter, afterKey, pageSize)
			require.NoError(t, err)

			allRows = append(allRows, rows...)

			if nextKey == "" {
				break
			}

			// Validate cursor format: "uuid:CCC:fee_schedule_id".
			require.Regexp(t,
				`^[0-9a-f-]{36}:[A-Z]{3}:[0-9a-f-]{36}$`,
				nextKey,
				"composite cursor should be source_id:currency:fee_schedule_id",
			)

			afterKey = nextKey
		}

		require.Len(t, allRows, 5, "should retrieve all 5 groups across pages")
		require.Equal(t, 3, pageCount, "5 groups with pageSize=2 should produce 3 pages (2+2+1)")

		// Verify no duplicate keys.
		seen := make(map[string]bool)

		for _, row := range allRows {
			key := varianceRowKey(row)
			require.False(t, seen[key], "duplicate row key across pages: %s", key)
			seen[key] = true
		}

		// Verify ordering: rows should be sorted by (source_id, currency, fee_schedule_id) ASC.
		keys := make([]string, len(allRows))
		for i, row := range allRows {
			keys[i] = varianceRowKey(row)
		}

		require.True(t, sort.StringsAreSorted(keys),
			"rows across pages should maintain (source_id, currency, fee_schedule_id) ASC order")
	})
}

// TestVarianceReport_SourceFilter inserts variances for 2 sources, then queries
// with a SourceID filter, verifying only the filtered source's rows are returned.
func TestVarianceReport_SourceFilter(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		seed := seedDashboardConfig(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		// Create a second source.
		source2ID := uuid.New()
		sourceCreatedAt := time.Now().UTC()
		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO reconciliation_sources (id, context_id, name, type, side, config, created_at, updated_at)
				VALUES ($1, $2, 'Variance Filter Source 2', 'BANK', 'RIGHT', '{}', $3, $3)
			`, source2ID, seed.ContextID, sourceCreatedAt)

			return struct{}{}, execErr
		})
		require.NoError(t, err)

		baseDate := time.Date(2025, 9, 5, 14, 0, 0, 0, time.UTC)

		variances := []varianceFeeVariance{
			// Source 1: 2 variances.
			{SourceID: seed.SourceID, Currency: "USD", ScheduleName: "Flat USD Standard", Expected: decimal.NewFromFloat(100.00), Actual: decimal.NewFromFloat(95.00), Delta: decimal.NewFromFloat(-5.00)},
			{SourceID: seed.SourceID, Currency: "EUR", ScheduleName: "Interchange EUR", Expected: decimal.NewFromFloat(200.00), Actual: decimal.NewFromFloat(210.00), Delta: decimal.NewFromFloat(10.00)},
			// Source 2: 2 variances.
			{SourceID: source2ID, Currency: "USD", ScheduleName: "Flat USD Reserve", Expected: decimal.NewFromFloat(300.00), Actual: decimal.NewFromFloat(280.00), Delta: decimal.NewFromFloat(-20.00)},
			{SourceID: source2ID, Currency: "GBP", ScheduleName: "Tiered GBP", Expected: decimal.NewFromFloat(400.00), Actual: decimal.NewFromFloat(410.00), Delta: decimal.NewFromFloat(10.00)},
		}

		insertVarianceData(t, h, seed, variances, baseDate)

		// Filter by source 1 only.
		filterSource1 := seed.SourceID
		filter := entities.VarianceReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 9, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 9, 30, 23, 59, 59, 0, time.UTC),
			SourceID:  &filterSource1,
			Limit:     10,
		}

		rows, _, err := repo.GetVarianceReport(ctx, filter)
		require.NoError(t, err)
		require.Len(t, rows, 2, "filtering by source1 should return 2 groups")

		for _, row := range rows {
			require.Equal(t, seed.SourceID, row.SourceID,
				"all returned rows should belong to the filtered source")
		}

		// Verify source 1 USD FLAT aggregation.
		type groupKey struct {
			currency        string
			feeScheduleName string
		}

		byGroup := make(map[groupKey]*entities.VarianceReportRow)
		for _, row := range rows {
			byGroup[groupKey{currency: row.Currency, feeScheduleName: row.FeeScheduleName}] = row
		}

		usdRow := byGroup[groupKey{currency: "USD", feeScheduleName: "Flat USD Standard"}]
		require.NotNil(t, usdRow, "USD Flat USD Standard group should be returned")
		require.True(t, decimal.NewFromFloat(100.00).Equal(usdRow.TotalExpected), "USD expected=%s", usdRow.TotalExpected)
		require.True(t, decimal.NewFromFloat(95.00).Equal(usdRow.TotalActual), "USD actual=%s", usdRow.TotalActual)
		require.True(t, decimal.NewFromFloat(-5.00).Equal(usdRow.NetVariance), "USD variance=%s", usdRow.NetVariance)

		eurRow := byGroup[groupKey{currency: "EUR", feeScheduleName: "Interchange EUR"}]
		require.NotNil(t, eurRow, "EUR Interchange EUR group should be returned")
		require.True(t, decimal.NewFromFloat(200.00).Equal(eurRow.TotalExpected), "EUR expected=%s", eurRow.TotalExpected)
		require.True(t, decimal.NewFromFloat(210.00).Equal(eurRow.TotalActual), "EUR actual=%s", eurRow.TotalActual)
		require.True(t, decimal.NewFromFloat(10.00).Equal(eurRow.NetVariance), "EUR variance=%s", eurRow.NetVariance)

		// Filter by source 2 to verify isolation.
		filterSource2 := source2ID
		filter2 := entities.VarianceReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 9, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 9, 30, 23, 59, 59, 0, time.UTC),
			SourceID:  &filterSource2,
			Limit:     10,
		}

		rows2, _, err := repo.GetVarianceReport(ctx, filter2)
		require.NoError(t, err)
		require.Len(t, rows2, 2, "filtering by source2 should return 2 groups")

		for _, row := range rows2 {
			require.Equal(t, source2ID, row.SourceID,
				"all returned rows should belong to source2")
		}
	})
}
