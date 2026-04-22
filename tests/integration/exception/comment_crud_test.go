//go:build integration

package exception

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	exceptionAdapters "github.com/LerianStudio/matcher/internal/exception/adapters"
	exceptionAudit "github.com/LerianStudio/matcher/internal/exception/adapters/audit"
	commentRepoAdapter "github.com/LerianStudio/matcher/internal/exception/adapters/postgres/comment"
	exceptionRepoAdapter "github.com/LerianStudio/matcher/internal/exception/adapters/postgres/exception"
	exceptionCommand "github.com/LerianStudio/matcher/internal/exception/services/command"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTxRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
	infraTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	"github.com/LerianStudio/matcher/tests/integration"
)

// wireCommentUseCase creates the merged exception UseCase wired with the
// comment repository, plus returns the comment repo directly for read-side
// assertions. Post-T-009, comment reads go handler→repo directly (no query
// UseCase), so tests that previously used queryUC.ListComments now call
// commentRepo.FindByExceptionID.
func wireCommentUseCase(
	t *testing.T,
	h *integration.TestHarness,
) (*exceptionCommand.ExceptionUseCase, *commentRepoAdapter.Repository) {
	t.Helper()

	provider := h.Provider()
	redisConn := mustRedisConn(t, h.RedisAddr)
	fullProvider := infraTestutil.NewSingleTenantInfrastructureProvider(h.Connection, redisConn)

	commentRepo := commentRepoAdapter.NewRepository(provider)
	exceptionRepo := exceptionRepoAdapter.NewRepository(provider)
	actorExtractor := exceptionAdapters.NewAuthActorExtractor()
	outbox := integration.NewTestOutboxRepository(t, h.Connection)
	auditPub, err := exceptionAudit.NewOutboxPublisher(outbox)
	require.NoError(t, err)

	cmdUC, err := exceptionCommand.NewExceptionUseCase(
		exceptionRepo,
		actorExtractor,
		auditPub,
		fullProvider,
		exceptionCommand.WithCommentRepository(commentRepo),
	)
	require.NoError(t, err)

	return cmdUC, commentRepo
}

// setupExceptionForComments creates a transaction and exception ready for comment tests.
func setupExceptionForComments(t *testing.T, h *integration.TestHarness) uuid.UUID {
	t.Helper()

	ctx := testCtx(t, h)
	provider := h.Provider()

	jRepo := ingestionJobRepo.NewRepository(provider)
	txRepo := ingestionTxRepo.NewRepository(provider)

	job := createIngestionJob(t, ctx, jRepo, h.Seed.ContextID, h.Seed.SourceID, 1)
	tx := createTransaction(t, ctx, txRepo, job.ID, h.Seed.SourceID,
		"COMMENT-TX-"+uuid.New().String()[:8], decimal.NewFromFloat(100.00), "USD")

	exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
		sharedexception.ExceptionSeverityMedium, "test exception for comments")

	return exc.ID
}

func TestCommentCRUD_AddComment(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		excID := setupExceptionForComments(t, h)
		ctx := testCtxWithActor(t, h, "commenter-1")
		cmdUC, _ := wireCommentUseCase(t, h)

		comment, err := cmdUC.AddComment(ctx, exceptionCommand.AddCommentInput{
			ExceptionID: excID,
			Content:     "This transaction needs investigation",
		})
		require.NoError(t, err)
		require.NotNil(t, comment)
		require.NotEqual(t, uuid.Nil, comment.ID)
		require.Equal(t, excID, comment.ExceptionID)
		require.Equal(t, "commenter-1", comment.Author)
		require.Equal(t, "This transaction needs investigation", comment.Content)
		require.False(t, comment.CreatedAt.IsZero())
		require.False(t, comment.UpdatedAt.IsZero())
	})
}

func TestCommentCRUD_ListComments(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		excID := setupExceptionForComments(t, h)
		ctx := testCtxWithActor(t, h, "lister-user")
		cmdUC, commentRepo := wireCommentUseCase(t, h)

		contents := []string{
			"First comment",
			"Second comment",
			"Third comment",
		}

		for _, c := range contents {
			_, err := cmdUC.AddComment(ctx, exceptionCommand.AddCommentInput{
				ExceptionID: excID,
				Content:     c,
			})
			require.NoError(t, err)
		}

		comments, err := commentRepo.FindByExceptionID(ctx, excID)
		require.NoError(t, err)
		require.Len(t, comments, 3)

		// Comments are ordered by created_at ASC, verify all content is present.
		returnedContents := make([]string, 0, len(comments))
		for _, c := range comments {
			returnedContents = append(returnedContents, c.Content)
		}

		for _, expected := range contents {
			require.Contains(t, returnedContents, expected)
		}
	})
}

func TestCommentCRUD_DeleteOwnComment(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		excID := setupExceptionForComments(t, h)
		ctx := testCtxWithActor(t, h, "delete-user")
		cmdUC, commentRepo := wireCommentUseCase(t, h)

		comment, err := cmdUC.AddComment(ctx, exceptionCommand.AddCommentInput{
			ExceptionID: excID,
			Content:     "Comment to be deleted",
		})
		require.NoError(t, err)
		require.NotNil(t, comment)

		// Delete as the same actor who created it.
		err = cmdUC.DeleteComment(ctx, excID, comment.ID)
		require.NoError(t, err)

		// Verify the comment no longer appears in the list.
		remaining, err := commentRepo.FindByExceptionID(ctx, excID)
		require.NoError(t, err)
		require.Empty(t, remaining)
	})
}

func TestCommentCRUD_DeleteOtherActorComment_Fails(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		excID := setupExceptionForComments(t, h)
		cmdUC, _ := wireCommentUseCase(t, h)

		// user-A creates a comment.
		ctxA := testCtxWithActor(t, h, "user-A")

		comment, err := cmdUC.AddComment(ctxA, exceptionCommand.AddCommentInput{
			ExceptionID: excID,
			Content:     "user-A's comment",
		})
		require.NoError(t, err)

		// user-B tries to delete it.
		ctxB := testCtxWithActor(t, h, "user-B")

		err = cmdUC.DeleteComment(ctxB, excID, comment.ID)
		require.Error(t, err)
		require.ErrorIs(t, err, exceptionCommand.ErrNotCommentAuthor)
	})
}

func TestCommentCRUD_AddCommentToResolvedExceptionFails(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		excID := setupExceptionForComments(t, h)
		ctx := testCtxWithActor(t, h, "resolved-commenter")
		cmdUC, _ := wireCommentUseCase(t, h)

		// Mark the exception as RESOLVED directly in the database.
		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			_, execErr := tx.ExecContext(ctx,
				`UPDATE exceptions SET status = 'RESOLVED' WHERE id = $1`, excID.String())
			return struct{}{}, execErr
		})
		require.NoError(t, err)

		// Attempting to add a comment should fail.
		_, err = cmdUC.AddComment(ctx, exceptionCommand.AddCommentInput{
			ExceptionID: excID,
			Content:     "This should be rejected",
		})
		require.Error(t, err)
		require.ErrorIs(t, err, exceptionCommand.ErrExceptionAlreadyResolved)
	})
}

func TestCommentCRUD_AddCommentEmptyContent_Fails(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		excID := setupExceptionForComments(t, h)
		ctx := testCtxWithActor(t, h, "empty-commenter")
		cmdUC, _ := wireCommentUseCase(t, h)

		_, err := cmdUC.AddComment(ctx, exceptionCommand.AddCommentInput{
			ExceptionID: excID,
			Content:     "",
		})
		require.Error(t, err)
		require.ErrorIs(t, err, exceptionCommand.ErrCommentContentEmpty)
	})
}

func TestCommentCRUD_ListCommentsEmptyException(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		excID := setupExceptionForComments(t, h)
		ctx := testCtxWithActor(t, h, "empty-lister")
		_, commentRepo := wireCommentUseCase(t, h)

		// No comments added — list should return empty slice, not error.
		comments, err := commentRepo.FindByExceptionID(ctx, excID)
		require.NoError(t, err)
		require.NotNil(t, comments)
		require.Empty(t, comments)
	})
}
