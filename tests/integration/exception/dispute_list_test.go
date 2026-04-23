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

	disputeRepoAdapter "github.com/LerianStudio/matcher/internal/exception/adapters/postgres/dispute"
	disputeEntity "github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTxRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
	"github.com/LerianStudio/matcher/tests/integration"
)

// disputeListTestFixture bundles the dispute repository and pre-created exception IDs
// so individual tests can create disputes without repeating the full setup chain.
type disputeListTestFixture struct {
	dRepo  *disputeRepoAdapter.Repository
	excIDs []uuid.UUID // pre-created exception IDs available for creating disputes
}

// createDisputeFixture creates infrastructure repos and a configurable number of exception
// parents, each backed by its own transaction. Returns the fixture for further dispute creation.
func createDisputeFixture(
	t *testing.T,
	h *integration.TestHarness,
	exceptionCount int,
) disputeListTestFixture {
	t.Helper()

	ctx := testCtx(t, h)
	provider := h.Provider()

	jRepo := ingestionJobRepo.NewRepository(provider)
	txRepo := ingestionTxRepo.NewRepository(provider)
	dRepo := disputeRepoAdapter.NewRepository(provider)

	job := createIngestionJob(t, ctx, jRepo, h.Seed.ContextID, h.Seed.SourceID, exceptionCount)

	excIDs := make([]uuid.UUID, 0, exceptionCount)

	for i := range exceptionCount {
		tx := createTransaction(t, ctx, txRepo, job.ID, h.Seed.SourceID,
			fmt.Sprintf("DISP-LIST-%d-%s", i, uuid.New().String()[:8]),
			decimal.NewFromFloat(100.0), "USD")

		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
			sharedexception.ExceptionSeverityHigh, fmt.Sprintf("dispute list test %d", i))

		excIDs = append(excIDs, exc.ID)
	}

	return disputeListTestFixture{
		dRepo:  dRepo,
		excIDs: excIDs,
	}
}

// createAndPersistDispute creates a dispute entity via the domain constructor, transitions
// it to OPEN, and persists it. Returns the persisted dispute.
func createAndPersistDispute(
	t *testing.T,
	h *integration.TestHarness,
	dRepo *disputeRepoAdapter.Repository,
	exceptionID uuid.UUID,
	category disputeEntity.DisputeCategory,
	description string,
) *disputeEntity.Dispute {
	t.Helper()

	ctx := testCtx(t, h)

	d, err := disputeEntity.NewDispute(ctx, exceptionID, category, description, "test-user@example.com")
	require.NoError(t, err)

	// NewDispute creates in DRAFT state; transition to OPEN before persisting
	// so the dispute is fully usable by List queries.
	require.NoError(t, d.Open(ctx))

	created, err := dRepo.Create(ctx, d)
	require.NoError(t, err)

	return created
}

// transitionDisputeToWon closes a dispute as WON via direct SQL update.
// This avoids wiring the full use-case layer for state transitions in repo-level tests.
func transitionDisputeToWon(
	t *testing.T,
	h *integration.TestHarness,
	disputeID uuid.UUID,
	resolution string,
) {
	t.Helper()

	ctx := testCtx(t, h)

	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, `
			UPDATE disputes
			SET state = $2, resolution = $3, updated_at = $4
			WHERE id = $1
		`,
			disputeID.String(),
			disputeEntity.DisputeStateWon.String(),
			resolution,
			time.Now().UTC(),
		)

		return struct{}{}, execErr
	})
	require.NoError(t, err)
}

func TestDisputeList_EmptyResult(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		provider := h.Provider()
		dRepo := disputeRepoAdapter.NewRepository(provider)

		disputes, pagination, err := dRepo.List(ctx, repositories.DisputeFilter{}, repositories.CursorFilter{
			Limit: 10,
		})
		require.NoError(t, err)
		require.NotNil(t, disputes)
		require.Empty(t, disputes)
		require.Empty(t, pagination.Next)
		require.Empty(t, pagination.Prev)
	})
}

func TestDisputeList_FilterByState(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)

		// We need 5 exceptions (one per dispute, since FindByExceptionID returns the latest).
		fixture := createDisputeFixture(t, h, 5)

		// Create 3 OPEN disputes (indices 0-2).
		for i := range 3 {
			createAndPersistDispute(t, h, fixture.dRepo, fixture.excIDs[i],
				disputeEntity.DisputeCategoryAmountMismatch,
				fmt.Sprintf("Open dispute %d", i+1))
		}

		// Create 2 disputes, transition them to WON (terminal state) (indices 3-4).
		for i := 3; i < 5; i++ {
			d := createAndPersistDispute(t, h, fixture.dRepo, fixture.excIDs[i],
				disputeEntity.DisputeCategoryBankFeeError,
				fmt.Sprintf("Closed dispute %d", i+1))

			transitionDisputeToWon(t, h, d.ID, "resolved via investigation")
		}

		// Filter for OPEN — should return exactly 3.
		openState := disputeEntity.DisputeStateOpen
		openDisputes, _, err := fixture.dRepo.List(ctx, repositories.DisputeFilter{
			State: &openState,
		}, repositories.CursorFilter{
			Limit: 10,
		})
		require.NoError(t, err)
		require.Len(t, openDisputes, 3, "expected exactly 3 OPEN disputes")

		for _, d := range openDisputes {
			require.Equal(t, disputeEntity.DisputeStateOpen, d.State,
				"dispute %s should be OPEN but got %s", d.ID, d.State)
		}

		// Sanity check: filter for WON — should return exactly 2.
		wonState := disputeEntity.DisputeStateWon
		wonDisputes, _, err := fixture.dRepo.List(ctx, repositories.DisputeFilter{
			State: &wonState,
		}, repositories.CursorFilter{
			Limit: 10,
		})
		require.NoError(t, err)
		require.Len(t, wonDisputes, 2, "expected exactly 2 WON disputes")

		for _, d := range wonDisputes {
			require.Equal(t, disputeEntity.DisputeStateWon, d.State,
				"dispute %s should be WON but got %s", d.ID, d.State)
		}
	})
}

func TestDisputeList_PaginationForward(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)

		const totalDisputes = 5
		const pageSize = 2

		fixture := createDisputeFixture(t, h, totalDisputes)

		// Create 5 disputes, each on a separate exception, with a small sleep
		// to ensure distinct created_at ordering.
		createdIDs := make(map[uuid.UUID]struct{}, totalDisputes)

		for i := range totalDisputes {
			d := createAndPersistDispute(t, h, fixture.dRepo, fixture.excIDs[i],
				disputeEntity.DisputeCategoryOther,
				fmt.Sprintf("Paginated dispute %d", i+1))
			createdIDs[d.ID] = struct{}{}

			time.Sleep(15 * time.Millisecond)
		}

		// Page through all disputes using forward cursor pagination.
		var allCollected []*disputeEntity.Dispute

		cursor := repositories.CursorFilter{
			Limit:  pageSize,
			SortBy: "created_at",
		}

		for {
			page, pagination, err := fixture.dRepo.List(ctx, repositories.DisputeFilter{}, cursor)
			require.NoError(t, err)

			allCollected = append(allCollected, page...)

			// If no next cursor, we've reached the last page.
			if pagination.Next == "" {
				break
			}

			// Safety: prevent infinite loops if pagination is broken.
			require.Less(t, len(allCollected), totalDisputes*2,
				"pagination loop exceeded expected bounds")

			cursor.Cursor = pagination.Next
		}

		// Verify all 5 disputes were returned across all pages.
		require.Len(t, allCollected, totalDisputes,
			"expected %d total disputes across all pages", totalDisputes)

		// Verify every created dispute ID appears in the collected results.
		collectedIDs := make(map[uuid.UUID]struct{}, len(allCollected))
		for _, d := range allCollected {
			collectedIDs[d.ID] = struct{}{}
		}

		for expectedID := range createdIDs {
			_, found := collectedIDs[expectedID]
			require.True(t, found,
				"dispute %s was created but not returned by paginated List", expectedID)
		}

		// Verify no duplicates.
		require.Len(t, collectedIDs, totalDisputes,
			"pagination returned duplicate dispute IDs")
	})
}

func TestDisputeFindByExceptionID(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)

		fixture := createDisputeFixture(t, h, 1)

		created := createAndPersistDispute(t, h, fixture.dRepo, fixture.excIDs[0],
			disputeEntity.DisputeCategoryDuplicateTransaction,
			"duplicate charge investigation")

		// FindByExceptionID should return the dispute we just created.
		found, err := fixture.dRepo.FindByExceptionID(ctx, fixture.excIDs[0])
		require.NoError(t, err)
		require.NotNil(t, found)
		require.Equal(t, created.ID, found.ID)
		require.Equal(t, fixture.excIDs[0], found.ExceptionID)
		require.Equal(t, disputeEntity.DisputeCategoryDuplicateTransaction, found.Category)
		require.Equal(t, disputeEntity.DisputeStateOpen, found.State)
		require.Equal(t, "duplicate charge investigation", found.Description)
		require.Equal(t, "test-user@example.com", found.OpenedBy)
		require.False(t, found.CreatedAt.IsZero())
		require.False(t, found.UpdatedAt.IsZero())

		// FindByExceptionID for a non-existent exception should return ErrDisputeNotFound.
		_, err = fixture.dRepo.FindByExceptionID(ctx, uuid.New())
		require.Error(t, err)
		require.ErrorIs(t, err, disputeEntity.ErrNotFound)
	})
}

func TestDisputeExistsForTenant(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)

		fixture := createDisputeFixture(t, h, 1)

		created := createAndPersistDispute(t, h, fixture.dRepo, fixture.excIDs[0],
			disputeEntity.DisputeCategoryUnrecognizedCharge,
			"unknown charge from processor")

		// ExistsForTenant should return true for the created dispute.
		exists, err := fixture.dRepo.ExistsForTenant(ctx, created.ID)
		require.NoError(t, err)
		require.True(t, exists, "dispute %s should exist for tenant", created.ID)

		// ExistsForTenant should return false for a random UUID.
		exists, err = fixture.dRepo.ExistsForTenant(ctx, uuid.New())
		require.NoError(t, err)
		require.False(t, exists, "random UUID should not exist for tenant")
	})
}
