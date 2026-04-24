//go:build integration

package exception

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	exceptionRepoAdapter "github.com/LerianStudio/matcher/internal/exception/adapters/postgres/exception"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	exceptionVO "github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTxRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
	"github.com/LerianStudio/matcher/tests/integration"
)

func severityPtr(s sharedexception.ExceptionSeverity) *sharedexception.ExceptionSeverity { return &s }
func statusPtr(s exceptionVO.ExceptionStatus) *exceptionVO.ExceptionStatus               { return &s }
func timePtr(t time.Time) *time.Time                                                     { return &t }

// setupListFilterTestData creates the infrastructure needed for list filter tests:
// a seed config, ingestion job repo, transaction repo, and exception repo backed
// by a full tenant-aware infrastructure provider.
func setupListFilterTestData(
	t *testing.T,
	h *integration.TestHarness,
) (seedConfig, *ingestionJobRepo.Repository, *ingestionTxRepo.Repository, *exceptionRepoAdapter.Repository) {
	t.Helper()

	provider := h.Provider()

	seed := seedTestConfig(t, h)
	jRepo := ingestionJobRepo.NewRepository(provider)
	txRepo := ingestionTxRepo.NewRepository(provider)
	excRepo := exceptionRepoAdapter.NewRepository(provider)

	return seed, jRepo, txRepo, excRepo
}

// updateExceptionStatus updates an exception's status directly via SQL.
func updateExceptionStatus(
	t *testing.T,
	ctx context.Context,
	h *integration.TestHarness,
	excID uuid.UUID,
	status exceptionVO.ExceptionStatus,
) {
	t.Helper()

	ts := time.Now().UTC()

	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(dbTx *sql.Tx) (struct{}, error) {
		_, execErr := dbTx.ExecContext(ctx,
			`UPDATE exceptions SET status=$1, updated_at=$2 WHERE id=$3`,
			status.String(), ts, excID.String(),
		)
		return struct{}{}, execErr
	})
	require.NoError(t, err)
}

// overrideExceptionCreatedAt overrides the created_at (and updated_at) of an
// exception to a specific timestamp for date-range filter tests.
func overrideExceptionCreatedAt(
	t *testing.T,
	ctx context.Context,
	h *integration.TestHarness,
	excID uuid.UUID,
	createdAt time.Time,
) {
	t.Helper()

	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(dbTx *sql.Tx) (struct{}, error) {
		_, execErr := dbTx.ExecContext(ctx,
			`UPDATE exceptions SET created_at=$1, updated_at=$1 WHERE id=$2`,
			createdAt, excID.String(),
		)
		return struct{}{}, execErr
	})
	require.NoError(t, err)
}

// --------------------------------------------------------------------------
// 1. TestIntegration_Exception_ListFilters_StatusOnly — 5 exceptions: 3 OPEN, 2 RESOLVED
// --------------------------------------------------------------------------

func TestIntegration_Exception_ListFilters_StatusOnly(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		seed, jRepo, txRepo, excRepo := setupListFilterTestData(t, h)

		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 5)

		var resolvedIDs []uuid.UUID

		for i := range 5 {
			tx := createTransaction(t, ctx, txRepo, job.ID, seed.LedgerSourceID,
				"LST-STATUS-"+uuid.New().String()[:8],
				decimal.NewFromFloat(50.00+float64(i)),
				"USD",
			)

			exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
				sharedexception.ExceptionSeverityLow, "UNMATCHED: status filter test")

			if i >= 3 {
				resolvedIDs = append(resolvedIDs, exc.ID)
			}
		}

		for _, excID := range resolvedIDs {
			updateExceptionStatus(t, ctx, h, excID, exceptionVO.ExceptionStatusResolved)
		}

		filter := repositories.ExceptionFilter{
			Status: statusPtr(exceptionVO.ExceptionStatusOpen),
		}
		cursor := repositories.CursorFilter{Limit: 50}

		results, _, err := excRepo.List(ctx, filter, cursor)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(results), 3,
			"expected at least 3 OPEN exceptions from this test batch")

		for _, exc := range results {
			require.Equal(t, exceptionVO.ExceptionStatusOpen, exc.Status,
				"all returned exceptions must be OPEN, got %s for %s", exc.Status, exc.ID)
		}
	})
}

// --------------------------------------------------------------------------
// 2. TestIntegration_Exception_ListFilters_SeverityOnly — create HIGH, MEDIUM, LOW, CRITICAL
// --------------------------------------------------------------------------

func TestIntegration_Exception_ListFilters_SeverityOnly(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		seed, jRepo, txRepo, excRepo := setupListFilterTestData(t, h)

		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 4)

		severities := []sharedexception.ExceptionSeverity{
			sharedexception.ExceptionSeverityHigh,
			sharedexception.ExceptionSeverityMedium,
			sharedexception.ExceptionSeverityLow,
			sharedexception.ExceptionSeverityCritical,
		}

		var highIDs []uuid.UUID

		for _, sev := range severities {
			tx := createTransaction(t, ctx, txRepo, job.ID, seed.LedgerSourceID,
				"LST-SEV-"+uuid.New().String()[:8],
				decimal.NewFromFloat(100.00),
				"USD",
			)

			exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID, sev,
				"UNMATCHED: severity filter test")

			if sev == sharedexception.ExceptionSeverityHigh {
				highIDs = append(highIDs, exc.ID)
			}
		}

		filter := repositories.ExceptionFilter{
			Severity: severityPtr(sharedexception.ExceptionSeverityHigh),
		}
		cursor := repositories.CursorFilter{Limit: 50}

		results, _, err := excRepo.List(ctx, filter, cursor)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(results), len(highIDs),
			"expected at least %d HIGH-severity exceptions", len(highIDs))

		for _, exc := range results {
			require.Equal(t, sharedexception.ExceptionSeverityHigh, exc.Severity,
				"all returned exceptions must be HIGH severity, got %s for %s",
				exc.Severity, exc.ID)
		}
	})
}

// --------------------------------------------------------------------------
// 3. TestIntegration_Exception_ListFilters_CombinedStatusAndSeverity — mixed status+severity
// --------------------------------------------------------------------------

func TestIntegration_Exception_ListFilters_CombinedStatusAndSeverity(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		seed, jRepo, txRepo, excRepo := setupListFilterTestData(t, h)

		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 6)

		type testCase struct {
			severity     sharedexception.ExceptionSeverity
			markResolved bool
		}

		// Create 6 exceptions:
		// 0: HIGH + OPEN  (target)
		// 1: HIGH + OPEN  (target)
		// 2: HIGH + RESOLVED
		// 3: LOW + OPEN
		// 4: MEDIUM + OPEN
		// 5: CRITICAL + RESOLVED
		cases := []testCase{
			{severity: sharedexception.ExceptionSeverityHigh, markResolved: false},
			{severity: sharedexception.ExceptionSeverityHigh, markResolved: false},
			{severity: sharedexception.ExceptionSeverityHigh, markResolved: true},
			{severity: sharedexception.ExceptionSeverityLow, markResolved: false},
			{severity: sharedexception.ExceptionSeverityMedium, markResolved: false},
			{severity: sharedexception.ExceptionSeverityCritical, markResolved: true},
		}

		var matchingCount int

		for _, tc := range cases {
			tx := createTransaction(t, ctx, txRepo, job.ID, seed.LedgerSourceID,
				"LST-COMBO-"+uuid.New().String()[:8],
				decimal.NewFromFloat(75.00),
				"USD",
			)

			exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID, tc.severity,
				"UNMATCHED: combo filter test")

			if tc.markResolved {
				updateExceptionStatus(t, ctx, h, exc.ID, exceptionVO.ExceptionStatusResolved)
			}

			if tc.severity == sharedexception.ExceptionSeverityHigh && !tc.markResolved {
				matchingCount++
			}
		}

		filter := repositories.ExceptionFilter{
			Status:   statusPtr(exceptionVO.ExceptionStatusOpen),
			Severity: severityPtr(sharedexception.ExceptionSeverityHigh),
		}
		cursor := repositories.CursorFilter{Limit: 50}

		results, _, err := excRepo.List(ctx, filter, cursor)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(results), matchingCount,
			"expected at least %d OPEN+HIGH exceptions", matchingCount)

		for _, exc := range results {
			require.Equal(t, exceptionVO.ExceptionStatusOpen, exc.Status,
				"all returned exceptions must be OPEN, got %s for %s", exc.Status, exc.ID)
			require.Equal(t, sharedexception.ExceptionSeverityHigh, exc.Severity,
				"all returned exceptions must be HIGH severity, got %s for %s",
				exc.Severity, exc.ID)
		}
	})
}

// --------------------------------------------------------------------------
// 4. TestIntegration_Exception_ListFilters_DateRange — backdate some exceptions, filter by range
// --------------------------------------------------------------------------

func TestIntegration_Exception_ListFilters_DateRange(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		seed, jRepo, txRepo, excRepo := setupListFilterTestData(t, h)

		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 4)

		oldDate := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
		recentDate := time.Date(2026, 2, 10, 12, 0, 0, 0, time.UTC)

		type dateSeed struct {
			createdAt time.Time
			inRange   bool
		}

		dateCases := []dateSeed{
			{createdAt: oldDate, inRange: false},
			{createdAt: oldDate, inRange: false},
			{createdAt: recentDate, inRange: true},
			{createdAt: recentDate, inRange: true},
		}

		var inRangeIDs []uuid.UUID

		for _, dc := range dateCases {
			tx := createTransaction(t, ctx, txRepo, job.ID, seed.LedgerSourceID,
				"LST-DATE-"+uuid.New().String()[:8],
				decimal.NewFromFloat(150.00),
				"USD",
			)

			exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
				sharedexception.ExceptionSeverityLow, "UNMATCHED: date filter test")

			overrideExceptionCreatedAt(t, ctx, h, exc.ID, dc.createdAt)

			if dc.inRange {
				inRangeIDs = append(inRangeIDs, exc.ID)
			}
		}

		rangeStart := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
		rangeEnd := time.Date(2026, 2, 28, 23, 59, 59, 0, time.UTC)

		filter := repositories.ExceptionFilter{
			DateFrom: timePtr(rangeStart),
			DateTo:   timePtr(rangeEnd),
		}
		cursor := repositories.CursorFilter{Limit: 50}

		results, _, err := excRepo.List(ctx, filter, cursor)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(results), len(inRangeIDs),
			"expected at least %d exceptions in the date range", len(inRangeIDs))

		for _, exc := range results {
			require.False(t, exc.CreatedAt.Before(rangeStart),
				"exception %s created_at %v is before range start %v",
				exc.ID, exc.CreatedAt, rangeStart)
			require.False(t, exc.CreatedAt.After(rangeEnd),
				"exception %s created_at %v is after range end %v",
				exc.ID, exc.CreatedAt, rangeEnd)
		}

		resultIDs := make(map[uuid.UUID]bool, len(results))
		for _, exc := range results {
			resultIDs[exc.ID] = true
		}

		for _, id := range inRangeIDs {
			require.True(t, resultIDs[id],
				"expected in-range exception %s to appear in results", id)
		}
	})
}

// --------------------------------------------------------------------------
// 5. TestIntegration_Exception_ListFilters_PaginationWithFilter — 7 HIGH exceptions, page size 3
// --------------------------------------------------------------------------

func TestIntegration_Exception_ListFilters_PaginationWithFilter(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		seed, jRepo, txRepo, excRepo := setupListFilterTestData(t, h)

		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 7)

		createdIDs := make(map[uuid.UUID]bool, 7)

		for range 7 {
			tx := createTransaction(t, ctx, txRepo, job.ID, seed.LedgerSourceID,
				"LST-PAGE-"+uuid.New().String()[:8],
				decimal.NewFromFloat(200.00),
				"USD",
			)

			exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
				sharedexception.ExceptionSeverityHigh, "UNMATCHED: pagination filter test")

			createdIDs[exc.ID] = true
		}

		filter := repositories.ExceptionFilter{
			Severity: severityPtr(sharedexception.ExceptionSeverityHigh),
		}

		collectedIDs := make(map[uuid.UUID]bool)
		cursorStr := ""

		const pageSize = 3
		const maxPages = 10

		for page := range maxPages {
			cursor := repositories.CursorFilter{
				Limit:  pageSize,
				Cursor: cursorStr,
			}

			results, pagination, err := excRepo.List(ctx, filter, cursor)
			require.NoError(t, err)

			for _, exc := range results {
				require.Equal(t, sharedexception.ExceptionSeverityHigh, exc.Severity,
					"page %d: all results must be HIGH severity", page)
				collectedIDs[exc.ID] = true
			}

			if pagination.Next == "" {
				break
			}

			cursorStr = pagination.Next
		}

		for id := range createdIDs {
			require.True(t, collectedIDs[id],
				"exception %s was not found across paginated results", id)
		}
	})
}

// --------------------------------------------------------------------------
// 6. TestIntegration_Exception_ListFilters_SortOrder — 3 exceptions with staggered timestamps, DESC
// --------------------------------------------------------------------------

func TestIntegration_Exception_ListFilters_SortOrder(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		seed, jRepo, txRepo, excRepo := setupListFilterTestData(t, h)

		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 3)

		baseTime := time.Date(2026, 2, 10, 10, 0, 0, 0, time.UTC)
		excIDs := make([]uuid.UUID, 3)

		for i := range 3 {
			tx := createTransaction(t, ctx, txRepo, job.ID, seed.LedgerSourceID,
				"LST-SORT-"+uuid.New().String()[:8],
				decimal.NewFromFloat(300.00+float64(i)),
				"USD",
			)

			exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
				sharedexception.ExceptionSeverityMedium, "UNMATCHED: sort order test")

			// Stagger: i=0 earliest, i=2 latest.
			targetTime := baseTime.Add(time.Duration(i) * time.Hour)
			overrideExceptionCreatedAt(t, ctx, h, exc.ID, targetTime)

			excIDs[i] = exc.ID
		}

		// Scope the query tightly to avoid noise from other tests.
		filter := repositories.ExceptionFilter{
			Severity: severityPtr(sharedexception.ExceptionSeverityMedium),
			DateFrom: timePtr(baseTime.Add(-time.Minute)),
			DateTo:   timePtr(baseTime.Add(3 * time.Hour)),
		}
		cursor := repositories.CursorFilter{
			Limit:     50,
			SortBy:    "created_at",
			SortOrder: "DESC",
		}

		results, _, err := excRepo.List(ctx, filter, cursor)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(results), 3,
			"expected at least 3 MEDIUM-severity exceptions in the time range")

		// Verify descending order.
		for i := 1; i < len(results); i++ {
			require.False(t, results[i].CreatedAt.After(results[i-1].CreatedAt),
				"expected descending order: results[%d].CreatedAt (%v) should not be after results[%d].CreatedAt (%v)",
				i, results[i].CreatedAt, i-1, results[i-1].CreatedAt)
		}

		// Verify our 3 specific IDs are present.
		resultIDs := make(map[uuid.UUID]bool, len(results))
		for _, exc := range results {
			resultIDs[exc.ID] = true
		}

		for _, id := range excIDs {
			require.True(t, resultIDs[id],
				"expected exception %s to appear in sorted results", id)
		}
	})
}
