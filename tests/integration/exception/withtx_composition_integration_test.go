//go:build integration

// Package exception contains integration tests for the exception context's
// WithTx repository surface under multi-aggregate composition.
//
// Exception workflows stitch together Comment + Exception + Dispute writes
// in a single tx during resolution and triage paths. A partial rollback
// after a comment has persisted but the parent exception update has failed
// would leave an orphan comment visible to the triage UI — the exact kind
// of cross-aggregate inconsistency sqlmock unit tests cannot see.
//
// Covers FINDING-042 (REFACTOR-051).
package exception

import (
	"database/sql"
	"errors"
	"testing"

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

// errDeliberateRollback forces the composition callback to abort, triggering
// tx rollback so the test can verify atomic undo across all repos involved.
var errDeliberateRollback = errors.New("deliberate rollback for composition test")

// setupExceptionForComposition creates the prerequisite chain (ingestion job
// → transaction → exception) that comment tests need, and returns the
// exception ID ready to attach comments to.
func setupExceptionForComposition(t *testing.T, h *integration.TestHarness, suffix string) uuid.UUID {
	t.Helper()

	ctx := testCtx(t, h)
	provider := h.Provider()

	jRepo := ingestionJobRepo.NewRepository(provider)
	txRepo := ingestionTxRepo.NewRepository(provider)

	job := createIngestionJob(t, ctx, jRepo, h.Seed.ContextID, h.Seed.SourceID, 1)
	tx := createTransaction(t, ctx, txRepo, job.ID, h.Seed.SourceID,
		"COMP-"+suffix+"-"+uuid.New().String()[:8],
		decimal.NewFromFloat(250.00), "USD")

	exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
		sharedexception.ExceptionSeverityHigh, "composition test reason")

	return exc.ID
}

// TestIntegration_Exception_WithTxComposition_CommentChain_Rollback asserts
// that two sequential Comment.CreateWithTx calls inside one tx roll back
// atomically. The triage UI depends on "all comments from this review session
// are either visible or not" — a partial rollback would leak half a session.
func TestIntegration_Exception_WithTxComposition_CommentChain_Rollback(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		exceptionID := setupExceptionForComposition(t, h, "ROLLBACK")

		ctx := testCtx(t, h)
		repo := commentRepoAdapter.NewRepository(h.Provider())

		firstComment, err := exceptionEntities.NewExceptionComment(
			ctx, exceptionID, "reviewer-a", "initial observation")
		require.NoError(t, err)

		secondComment, err := exceptionEntities.NewExceptionComment(
			ctx, exceptionID, "reviewer-a", "follow-up observation")
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := repo.CreateWithTx(ctx, tx, firstComment); err != nil {
				return struct{}{}, err
			}

			if _, err := repo.CreateWithTx(ctx, tx, secondComment); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		// Neither comment must appear in the comment list for the exception.
		comments, err := repo.FindByExceptionID(ctx, exceptionID)
		require.NoError(t, err)
		require.Empty(t, comments,
			"no comments must persist after Create+Create+rollback")
	})
}

// TestIntegration_Exception_WithTxComposition_CommentChain_Commit is the
// commit counterpart — both comments visible under the exception.
func TestIntegration_Exception_WithTxComposition_CommentChain_Commit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		exceptionID := setupExceptionForComposition(t, h, "COMMIT")

		ctx := testCtx(t, h)
		repo := commentRepoAdapter.NewRepository(h.Provider())

		firstComment, err := exceptionEntities.NewExceptionComment(
			ctx, exceptionID, "reviewer-b", "commit observation 1")
		require.NoError(t, err)

		secondComment, err := exceptionEntities.NewExceptionComment(
			ctx, exceptionID, "reviewer-b", "commit observation 2")
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := repo.CreateWithTx(ctx, tx, firstComment); err != nil {
				return struct{}{}, err
			}

			if _, err := repo.CreateWithTx(ctx, tx, secondComment); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		comments, err := repo.FindByExceptionID(ctx, exceptionID)
		require.NoError(t, err)
		require.Len(t, comments, 2,
			"both comments must be visible after Create+Create+commit")
	})
}

// TestIntegration_Exception_WithTxComposition_CommentCreateAndDelete_Rollback
// asserts that Comment.CreateWithTx + DeleteByExceptionAndIDWithTx compose
// atomically. Admin moderation workflows sometimes create a correction
// comment then redact the original in one tx; a partial rollback would
// leave both the correction AND the original, confusing the audit trail.
func TestIntegration_Exception_WithTxComposition_CommentCreateAndDelete_Rollback(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		exceptionID := setupExceptionForComposition(t, h, "DEL-ROLLBACK")

		ctx := testCtx(t, h)
		repo := commentRepoAdapter.NewRepository(h.Provider())

		// Pre-create an original comment outside the composition tx so Delete has a target.
		originalComment, err := exceptionEntities.NewExceptionComment(
			ctx, exceptionID, "reviewer-c", "original comment to redact")
		require.NoError(t, err)
		createdOriginal, err := repo.Create(ctx, originalComment)
		require.NoError(t, err)

		correctionComment, err := exceptionEntities.NewExceptionComment(
			ctx, exceptionID, "reviewer-c", "correction comment")
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := repo.CreateWithTx(ctx, tx, correctionComment); err != nil {
				return struct{}{}, err
			}

			if err := repo.DeleteByExceptionAndIDWithTx(ctx, tx, exceptionID, createdOriginal.ID); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, errDeliberateRollback
		})
		require.ErrorIs(t, txErr, errDeliberateRollback)

		// Original comment must still exist (Delete rolled back).
		comments, err := repo.FindByExceptionID(ctx, exceptionID)
		require.NoError(t, err)
		require.Len(t, comments, 1,
			"only the pre-existing comment must remain after Create+Delete+rollback")
		require.Equal(t, createdOriginal.ID, comments[0].ID)
	})
}

// TestIntegration_Exception_WithTxComposition_CommentCreateAndDelete_Commit is
// the commit counterpart — correction visible, original gone.
func TestIntegration_Exception_WithTxComposition_CommentCreateAndDelete_Commit(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		exceptionID := setupExceptionForComposition(t, h, "DEL-COMMIT")

		ctx := testCtx(t, h)
		repo := commentRepoAdapter.NewRepository(h.Provider())

		originalComment, err := exceptionEntities.NewExceptionComment(
			ctx, exceptionID, "reviewer-d", "original comment commit case")
		require.NoError(t, err)
		createdOriginal, err := repo.Create(ctx, originalComment)
		require.NoError(t, err)

		correctionComment, err := exceptionEntities.NewExceptionComment(
			ctx, exceptionID, "reviewer-d", "correction comment commit case")
		require.NoError(t, err)

		_, txErr := pgcommon.WithTenantTxProvider(ctx, h.Provider(), func(tx *sql.Tx) (struct{}, error) {
			if _, err := repo.CreateWithTx(ctx, tx, correctionComment); err != nil {
				return struct{}{}, err
			}

			if err := repo.DeleteByExceptionAndIDWithTx(ctx, tx, exceptionID, createdOriginal.ID); err != nil {
				return struct{}{}, err
			}

			return struct{}{}, nil
		})
		require.NoError(t, txErr)

		comments, err := repo.FindByExceptionID(ctx, exceptionID)
		require.NoError(t, err)
		require.Len(t, comments, 1,
			"only the new correction must remain after Create+Delete+commit")
		require.Equal(t, correctionComment.ID, comments[0].ID)
	})
}
