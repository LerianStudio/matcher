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

	commentRepoAdapter "github.com/LerianStudio/matcher/internal/exception/adapters/postgres/comment"
	exceptionEntities "github.com/LerianStudio/matcher/internal/exception/domain/entities"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTxRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
	"github.com/LerianStudio/matcher/tests/integration"
)

// setupCommentRepoTest creates the prerequisite chain (job → transaction → exception)
// and returns the comment repository alongside the exception ID.
func setupCommentRepoTest(
	t *testing.T,
	h *integration.TestHarness,
	extIDSuffix string,
) (*commentRepoAdapter.Repository, uuid.UUID) {
	t.Helper()

	ctx := testCtx(t, h)
	provider := h.Provider()

	jRepo := ingestionJobRepo.NewRepository(provider)
	txRepo := ingestionTxRepo.NewRepository(provider)

	job := createIngestionJob(t, ctx, jRepo, h.Seed.ContextID, h.Seed.SourceID, 1)
	tx := createTransaction(t, ctx, txRepo, job.ID, h.Seed.SourceID,
		"CMTREPO-"+extIDSuffix+"-"+uuid.New().String()[:8],
		decimal.NewFromFloat(100.00), "USD")

	exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
		sharedexception.ExceptionSeverityHigh, "repo test reason")

	cRepo := commentRepoAdapter.NewRepository(provider)

	return cRepo, exc.ID
}

// createComment is a helper that builds and persists a comment through the repository.
func createComment(
	t *testing.T,
	h *integration.TestHarness,
	cRepo *commentRepoAdapter.Repository,
	exceptionID uuid.UUID,
	author, content string,
) *exceptionEntities.ExceptionComment {
	t.Helper()

	ctx := testCtx(t, h)

	comment, err := exceptionEntities.NewExceptionComment(ctx, exceptionID, author, content)
	require.NoError(t, err)

	created, err := cRepo.Create(ctx, comment)
	require.NoError(t, err)
	require.NotNil(t, created)

	return created
}

func TestIntegration_Exception_CommentRepository_FindByExceptionID_Empty(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		cRepo, excID := setupCommentRepoTest(t, h, "EMPTY")

		comments, err := cRepo.FindByExceptionID(ctx, excID)
		require.NoError(t, err)
		require.NotNil(t, comments)
		require.Empty(t, comments)
	})
}

func TestIntegration_Exception_CommentRepository_CreateAndFind(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		cRepo, excID := setupCommentRepoTest(t, h, "CRFIND")

		contents := []string{
			"First observation",
			"Follow-up analysis",
			"Final determination",
		}

		created := make([]*exceptionEntities.ExceptionComment, 0, len(contents))
		for i, content := range contents {
			c := createComment(t, h, cRepo, excID,
				fmt.Sprintf("analyst-%d", i), content)
			created = append(created, c)

			// Small delay to ensure distinct created_at ordering within DB precision.
			time.Sleep(10 * time.Millisecond)
		}

		// Retrieve all comments for the exception.
		found, err := cRepo.FindByExceptionID(ctx, excID)
		require.NoError(t, err)
		require.Len(t, found, 3)

		// FindByExceptionID orders by created_at ASC — verify all content present.
		foundContents := make([]string, 0, len(found))
		for _, c := range found {
			require.Equal(t, excID, c.ExceptionID)
			require.NotEqual(t, uuid.Nil, c.ID)
			require.False(t, c.CreatedAt.IsZero())
			require.False(t, c.UpdatedAt.IsZero())
			foundContents = append(foundContents, c.Content)
		}

		for _, expected := range contents {
			require.Contains(t, foundContents, expected)
		}

		// Verify IDs match those returned at creation time.
		createdIDs := make(map[uuid.UUID]struct{}, len(created))
		for _, c := range created {
			createdIDs[c.ID] = struct{}{}
		}

		for _, c := range found {
			_, exists := createdIDs[c.ID]
			require.True(t, exists, "found comment ID %s was not in the created set", c.ID)
		}
	})
}

func TestIntegration_Exception_CommentRepository_FindByExceptionID_OrderingWithManyComments(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		cRepo, excID := setupCommentRepoTest(t, h, "ORDER")

		const commentCount = 5

		createdIDs := make([]uuid.UUID, 0, commentCount)

		for i := range commentCount {
			c := createComment(t, h, cRepo, excID,
				"auditor", fmt.Sprintf("Comment number %d", i+1))
			createdIDs = append(createdIDs, c.ID)

			// Ensure monotonic created_at values.
			time.Sleep(10 * time.Millisecond)
		}

		found, err := cRepo.FindByExceptionID(ctx, excID)
		require.NoError(t, err)
		require.Len(t, found, commentCount)

		// Verify ascending creation order: each comment's CreatedAt must be ≤ the next.
		for i := 1; i < len(found); i++ {
			require.False(t, found[i].CreatedAt.Before(found[i-1].CreatedAt),
				"comment[%d].CreatedAt (%v) is before comment[%d].CreatedAt (%v)",
				i, found[i].CreatedAt, i-1, found[i-1].CreatedAt)
		}

		// Verify all 5 IDs are present.
		foundIDs := make(map[uuid.UUID]struct{}, len(found))
		for _, c := range found {
			foundIDs[c.ID] = struct{}{}
		}

		for _, id := range createdIDs {
			_, exists := foundIDs[id]
			require.True(t, exists, "expected comment %s in result set", id)
		}
	})
}

func TestIntegration_Exception_CommentRepository_DeleteWithTx(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		cRepo, excID := setupCommentRepoTest(t, h, "DELTX")

		comment := createComment(t, h, cRepo, excID, "operator", "Comment to delete")

		// Verify the comment exists via FindByID.
		fetched, err := cRepo.FindByID(ctx, comment.ID)
		require.NoError(t, err)
		require.NotNil(t, fetched)
		require.Equal(t, comment.ID, fetched.ID)
		require.Equal(t, "Comment to delete", fetched.Content)

		// Delete within an explicit tenant transaction.
		_, err = pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			return struct{}{}, cRepo.DeleteWithTx(ctx, tx, comment.ID)
		})
		require.NoError(t, err)

		// Verify the comment is gone — FindByExceptionID should return empty.
		remaining, err := cRepo.FindByExceptionID(ctx, excID)
		require.NoError(t, err)
		require.Empty(t, remaining)

		// A second delete should return ErrCommentNotFound.
		_, err = pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			return struct{}{}, cRepo.DeleteWithTx(ctx, tx, comment.ID)
		})
		require.Error(t, err)
		require.ErrorIs(t, err, exceptionEntities.ErrCommentNotFound)
	})
}

func TestIntegration_Exception_CommentRepository_FindByExceptionID_IsolationBetweenExceptions(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		provider := h.Provider()

		jRepo := ingestionJobRepo.NewRepository(provider)
		txRepo := ingestionTxRepo.NewRepository(provider)

		job := createIngestionJob(t, ctx, jRepo, h.Seed.ContextID, h.Seed.SourceID, 2)

		// Two separate transactions → two separate exceptions.
		tx1 := createTransaction(t, ctx, txRepo, job.ID, h.Seed.SourceID,
			"ISO-A-"+uuid.New().String()[:8], decimal.NewFromFloat(50.00), "USD")
		tx2 := createTransaction(t, ctx, txRepo, job.ID, h.Seed.SourceID,
			"ISO-B-"+uuid.New().String()[:8], decimal.NewFromFloat(75.00), "EUR")

		exc1 := createExceptionForTransaction(t, ctx, h.Connection, tx1.ID,
			sharedexception.ExceptionSeverityHigh, "exception alpha")
		exc2 := createExceptionForTransaction(t, ctx, h.Connection, tx2.ID,
			sharedexception.ExceptionSeverityMedium, "exception beta")

		cRepo := commentRepoAdapter.NewRepository(provider)

		// Add 2 comments to exception 1.
		createComment(t, h, cRepo, exc1.ID, "user-a", "Alpha comment 1")
		createComment(t, h, cRepo, exc1.ID, "user-a", "Alpha comment 2")

		// Add 3 comments to exception 2.
		createComment(t, h, cRepo, exc2.ID, "user-b", "Beta comment 1")
		createComment(t, h, cRepo, exc2.ID, "user-b", "Beta comment 2")
		createComment(t, h, cRepo, exc2.ID, "user-b", "Beta comment 3")

		// Query exception 1 — must see exactly its 2 comments, none from exception 2.
		found1, err := cRepo.FindByExceptionID(ctx, exc1.ID)
		require.NoError(t, err)
		require.Len(t, found1, 2)

		for _, c := range found1 {
			require.Equal(t, exc1.ID, c.ExceptionID,
				"comment %s belongs to wrong exception", c.ID)
		}

		// Query exception 2 — must see exactly its 3 comments, none from exception 1.
		found2, err := cRepo.FindByExceptionID(ctx, exc2.ID)
		require.NoError(t, err)
		require.Len(t, found2, 3)

		for _, c := range found2 {
			require.Equal(t, exc2.ID, c.ExceptionID,
				"comment %s belongs to wrong exception", c.ID)
		}

		// Cross-check: no comment from exc1 appears in exc2's results and vice versa.
		exc1IDs := make(map[uuid.UUID]struct{}, len(found1))
		for _, c := range found1 {
			exc1IDs[c.ID] = struct{}{}
		}

		for _, c := range found2 {
			_, leaked := exc1IDs[c.ID]
			require.False(t, leaked,
				"comment %s from exception 1 leaked into exception 2 results", c.ID)
		}
	})
}
