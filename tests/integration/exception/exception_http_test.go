//go:build integration

package exception

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	exceptionRepoAdapter "github.com/LerianStudio/matcher/internal/exception/adapters/postgres/exception"
	exceptionEntities "github.com/LerianStudio/matcher/internal/exception/domain/entities"
	exceptionRepositories "github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	exceptionVO "github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	exceptionCommand "github.com/LerianStudio/matcher/internal/exception/services/command"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTxRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
	"github.com/LerianStudio/matcher/tests/integration"
)

// setupMultipleExceptions creates a batch of exceptions with varying severities
// for list/filter/pagination tests. Returns the exception IDs in creation order.
func setupMultipleExceptions(t *testing.T, h *integration.TestHarness, count int) []uuid.UUID {
	t.Helper()

	ctx := testCtx(t, h)
	provider := h.Provider()

	jRepo := ingestionJobRepo.NewRepository(provider)
	txRepo := ingestionTxRepo.NewRepository(provider)

	job := createIngestionJob(t, ctx, jRepo, h.Seed.ContextID, h.Seed.SourceID, count)

	ids := make([]uuid.UUID, 0, count)

	for i := 0; i < count; i++ {
		tx := createTransaction(t, ctx, txRepo, job.ID, h.Seed.SourceID,
			fmt.Sprintf("HTTP-EXC-TX-%d-%s", i, uuid.New().String()[:8]),
			decimal.NewFromFloat(float64(100+i*50)), "USD")

		severity := sharedexception.ExceptionSeverityMedium
		if i%3 == 0 {
			severity = sharedexception.ExceptionSeverityCritical
		}

		if i%3 == 1 {
			severity = sharedexception.ExceptionSeverityHigh
		}

		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID, severity,
			fmt.Sprintf("reason %d", i))
		ids = append(ids, exc.ID)
	}

	return ids
}

// --------------------------------------------------------------------------
// 1. TestIntegration_Exception_ExceptionList_ReturnsAll
// --------------------------------------------------------------------------

func TestIntegration_Exception_ExceptionList_ReturnsAll(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		const wantCount = 5

		setupMultipleExceptions(t, h, wantCount)

		ctx := testCtx(t, h)
		repo := exceptionRepoAdapter.NewRepository(h.Provider())

		exceptions, _, err := repo.List(ctx,
			exceptionRepositories.ExceptionFilter{},
			exceptionRepositories.CursorFilter{Limit: 50},
		)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(exceptions), wantCount,
			"list should return at least %d exceptions", wantCount)
	})
}

// --------------------------------------------------------------------------
// 2. TestIntegration_Exception_ExceptionList_FilterBySeverity
// --------------------------------------------------------------------------

func TestIntegration_Exception_ExceptionList_FilterBySeverity(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		// Create 6 exceptions: indices 0,3 = CRITICAL; 1,4 = HIGH; 2,5 = MEDIUM
		setupMultipleExceptions(t, h, 6)

		ctx := testCtx(t, h)
		repo := exceptionRepoAdapter.NewRepository(h.Provider())

		severity := sharedexception.ExceptionSeverityCritical
		filter := exceptionRepositories.ExceptionFilter{Severity: &severity}

		exceptions, _, err := repo.List(ctx, filter,
			exceptionRepositories.CursorFilter{Limit: 50},
		)
		require.NoError(t, err)
		require.NotEmpty(t, exceptions, "should find at least one CRITICAL exception")

		for _, exc := range exceptions {
			require.Equal(t, sharedexception.ExceptionSeverityCritical, exc.Severity,
				"all returned exceptions must be CRITICAL, got %s for id %s",
				exc.Severity.String(), exc.ID)
		}
	})
}

// --------------------------------------------------------------------------
// 3. TestIntegration_Exception_ExceptionList_FilterByStatus
// --------------------------------------------------------------------------

func TestIntegration_Exception_ExceptionList_FilterByStatus(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ids := setupMultipleExceptions(t, h, 3)
		ctx := testCtx(t, h)
		repo := exceptionRepoAdapter.NewRepository(h.Provider())

		// Resolve the first exception directly in DB to change its status.
		resolvedID := ids[0]
		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			_, execErr := tx.ExecContext(ctx, `
				UPDATE exceptions
				SET status = $1, resolution_notes = $2, version = version + 1, updated_at = $3
				WHERE id = $4
			`,
				exceptionVO.ExceptionStatusResolved.String(),
				"resolved via test",
				time.Now().UTC(),
				resolvedID.String(),
			)

			return struct{}{}, execErr
		})
		require.NoError(t, err)

		// Filter by OPEN – the resolved exception must be excluded.
		openStatus := exceptionVO.ExceptionStatusOpen
		filter := exceptionRepositories.ExceptionFilter{Status: &openStatus}

		exceptions, _, err := repo.List(ctx, filter,
			exceptionRepositories.CursorFilter{Limit: 50},
		)
		require.NoError(t, err)

		for _, exc := range exceptions {
			require.Equal(t, exceptionVO.ExceptionStatusOpen, exc.Status,
				"filter by OPEN must not return resolved exception %s", exc.ID)
			require.NotEqual(t, resolvedID, exc.ID,
				"resolved exception %s must not appear in OPEN filter results", resolvedID)
		}
	})
}

// --------------------------------------------------------------------------
// 4. TestIntegration_Exception_ExceptionList_Pagination
// --------------------------------------------------------------------------

func TestIntegration_Exception_ExceptionList_Pagination(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		const totalCount = 5

		setupMultipleExceptions(t, h, totalCount)

		ctx := testCtx(t, h)
		repo := exceptionRepoAdapter.NewRepository(h.Provider())

		// Page 1: request 2 items.
		page1, pagination1, err := repo.List(ctx,
			exceptionRepositories.ExceptionFilter{},
			exceptionRepositories.CursorFilter{Limit: 2},
		)
		require.NoError(t, err)
		require.Len(t, page1, 2, "first page must return exactly 2 items")
		require.NotEmpty(t, pagination1.Next, "first page must have a next cursor")

		// Page 2: use cursor from page 1.
		page2, pagination2, err := repo.List(ctx,
			exceptionRepositories.ExceptionFilter{},
			exceptionRepositories.CursorFilter{Limit: 2, Cursor: pagination1.Next},
		)
		require.NoError(t, err)
		require.Len(t, page2, 2, "second page must return exactly 2 items")

		// Verify no overlap between pages.
		page1IDs := make(map[uuid.UUID]struct{}, len(page1))
		for _, exc := range page1 {
			page1IDs[exc.ID] = struct{}{}
		}

		for _, exc := range page2 {
			_, overlap := page1IDs[exc.ID]
			require.False(t, overlap,
				"page 2 exception %s must not overlap with page 1", exc.ID)
		}

		// Page 2 should still have a prev cursor.
		require.NotEmpty(t, pagination2.Prev,
			"second page must have a prev cursor for backward navigation")
	})
}

// --------------------------------------------------------------------------
// 5. TestIntegration_Exception_ExceptionGetByID
// --------------------------------------------------------------------------

func TestIntegration_Exception_ExceptionGetByID(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		provider := h.Provider()

		jRepo := ingestionJobRepo.NewRepository(provider)
		txRepo := ingestionTxRepo.NewRepository(provider)

		job := createIngestionJob(t, ctx, jRepo, h.Seed.ContextID, h.Seed.SourceID, 1)
		tx := createTransaction(t, ctx, txRepo, job.ID, h.Seed.SourceID,
			"GET-BY-ID-"+uuid.New().String()[:8],
			decimal.NewFromFloat(777.77), "GBP")

		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
			sharedexception.ExceptionSeverityHigh, "amount discrepancy")

		repo := exceptionRepoAdapter.NewRepository(provider)

		found, err := repo.FindByID(ctx, exc.ID)
		require.NoError(t, err)
		require.NotNil(t, found)

		require.Equal(t, exc.ID, found.ID)
		require.Equal(t, exc.TransactionID, found.TransactionID)
		require.Equal(t, sharedexception.ExceptionSeverityHigh, found.Severity)
		require.Equal(t, exceptionVO.ExceptionStatusOpen, found.Status)
		require.NotNil(t, found.Reason)
		require.Equal(t, "amount discrepancy", *found.Reason)
		require.False(t, found.CreatedAt.IsZero())
		require.False(t, found.UpdatedAt.IsZero())
	})
}

// --------------------------------------------------------------------------
// 6. TestIntegration_Exception_ExceptionGetByID_NotFound
// --------------------------------------------------------------------------

func TestIntegration_Exception_ExceptionGetByID_NotFound(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := exceptionRepoAdapter.NewRepository(h.Provider())

		_, err := repo.FindByID(ctx, uuid.New())
		require.Error(t, err)
		require.ErrorIs(t, err, exceptionEntities.ErrExceptionNotFound)
	})
}

// --------------------------------------------------------------------------
// 7. TestIntegration_Exception_ExceptionForceMatch_FullFlow
// --------------------------------------------------------------------------

func TestIntegration_Exception_ExceptionForceMatch_FullFlow(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "http-test-operator")
		provider := h.Provider()

		jRepo := ingestionJobRepo.NewRepository(provider)
		txRepo := ingestionTxRepo.NewRepository(provider)

		job := createIngestionJob(t, ctx, jRepo, h.Seed.ContextID, h.Seed.SourceID, 1)
		tx := createTransaction(t, ctx, txRepo, job.ID, h.Seed.SourceID,
			"FORCE-FULL-"+uuid.New().String()[:8],
			decimal.NewFromFloat(420.00), "USD")

		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
			sharedexception.ExceptionSeverityMedium, "UNMATCHED: no counterparty")

		require.Equal(t, exceptionVO.ExceptionStatusOpen, exc.Status,
			"precondition: exception must start as OPEN")

		uc, _ := wireExceptionUseCase(t, h, &noopResolutionExecutor{})

		resolved, err := uc.ForceMatch(ctx, exceptionCommand.ForceMatchCommand{
			ExceptionID:    exc.ID,
			OverrideReason: "DATA_CORRECTION",
			Notes:          "Verified with bank statement – duplicate entry in source",
		})
		require.NoError(t, err)
		require.NotNil(t, resolved)

		// Status transition
		require.Equal(t, exceptionVO.ExceptionStatusResolved, resolved.Status)

		// Resolution metadata
		require.NotNil(t, resolved.ResolutionType)
		require.Equal(t, "FORCE_MATCH", *resolved.ResolutionType)
		require.NotNil(t, resolved.ResolutionReason)
		require.Equal(t, "DATA_CORRECTION", *resolved.ResolutionReason)
		require.NotNil(t, resolved.ResolutionNotes)
		require.Contains(t, *resolved.ResolutionNotes, "Verified with bank statement")

		// Version must have incremented (initial 0 → pending → resolved = at least 2 bumps).
		require.Greater(t, resolved.Version, exc.Version,
			"version must increment after resolution")

		// Audit event in outbox.
		outboxCount := countOutboxEvents(t, ctx, h.Connection, exc.ID)
		require.GreaterOrEqual(t, outboxCount, 1,
			"at least one outbox audit event expected for force match resolution")
	})
}

// --------------------------------------------------------------------------
// 8. TestIntegration_Exception_ExceptionAdjustEntry_FullFlow
// --------------------------------------------------------------------------

func TestIntegration_Exception_ExceptionAdjustEntry_FullFlow(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "http-test-adjuster")
		provider := h.Provider()

		jRepo := ingestionJobRepo.NewRepository(provider)
		txRepo := ingestionTxRepo.NewRepository(provider)

		job := createIngestionJob(t, ctx, jRepo, h.Seed.ContextID, h.Seed.SourceID, 1)
		tx := createTransaction(t, ctx, txRepo, job.ID, h.Seed.SourceID,
			"ADJUST-FULL-"+uuid.New().String()[:8],
			decimal.NewFromFloat(1250.00), "EUR")

		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
			sharedexception.ExceptionSeverityHigh, "UNMATCHED: amount discrepancy")

		require.Equal(t, exceptionVO.ExceptionStatusOpen, exc.Status,
			"precondition: exception must start as OPEN")

		uc, _ := wireExceptionUseCase(t, h, &noopResolutionExecutor{})

		resolved, err := uc.AdjustEntry(ctx, exceptionCommand.AdjustEntryCommand{
			ExceptionID: exc.ID,
			ReasonCode:  "AMOUNT_CORRECTION",
			Notes:       "Adjusted rounding difference – EUR settlement fee applied",
			Amount:      decimal.NewFromFloat(12.50),
			Currency:    "EUR",
			EffectiveAt: time.Now().UTC(),
		})
		require.NoError(t, err)
		require.NotNil(t, resolved)

		// Status transition
		require.Equal(t, exceptionVO.ExceptionStatusResolved, resolved.Status)

		// Resolution metadata
		require.NotNil(t, resolved.ResolutionType)
		require.Equal(t, "ADJUST_ENTRY", *resolved.ResolutionType)
		require.NotNil(t, resolved.ResolutionReason)
		require.Equal(t, "AMOUNT_CORRECTION", *resolved.ResolutionReason)
		require.NotNil(t, resolved.ResolutionNotes)
		require.Contains(t, *resolved.ResolutionNotes, "Adjusted rounding difference")

		// Version must have incremented.
		require.Greater(t, resolved.Version, exc.Version,
			"version must increment after resolution")

		// Audit event in outbox.
		outboxCount := countOutboxEvents(t, ctx, h.Connection, exc.ID)
		require.GreaterOrEqual(t, outboxCount, 1,
			"at least one outbox audit event expected for adjust entry resolution")
	})
}
