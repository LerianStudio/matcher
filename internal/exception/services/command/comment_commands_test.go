// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
)

// --- Constructor tests ---

// TestNewCommentUseCase_NilCommentRepo verifies the method-level
// validation that now owns the optional-dependency check: the merged
// constructor no longer rejects a nil comment repository (it is optional),
// so the caller discovers the missing dependency when invoking AddComment.
func TestNewCommentUseCase_NilCommentRepo(t *testing.T) {
	t.Parallel()

	uc, err := NewExceptionUseCase(&stubExceptionRepo{}, actorExtractor("a"), &stubAuditPublisher{}, &stubInfraProvider{})
	require.NoError(t, err)
	require.NotNil(t, uc)

	_, err = uc.AddComment(context.Background(), AddCommentInput{
		ExceptionID: uuid.New(),
		Content:     "test",
	})
	require.ErrorIs(t, err, ErrNilCommentRepository)
}

func TestNewCommentUseCase_NilExceptionRepo(t *testing.T) {
	t.Parallel()

	mockCommentRepo := &mockCommentRepository{}

	_, err := NewExceptionUseCase(nil, nil, &stubAuditPublisher{}, &stubInfraProvider{}, WithCommentRepository(mockCommentRepo))
	require.ErrorIs(t, err, ErrNilExceptionRepository)
}

func TestNewCommentUseCase_NilActorExtractor(t *testing.T) {
	t.Parallel()

	mockCommentRepo := &mockCommentRepository{}
	mockExceptionRepo := &mockExceptionRepository{}

	_, err := NewExceptionUseCase(mockExceptionRepo, nil, &stubAuditPublisher{}, &stubInfraProvider{}, WithCommentRepository(mockCommentRepo))
	require.ErrorIs(t, err, ErrNilActorExtractor)
}

func TestNewCommentUseCase_Success(t *testing.T) {
	t.Parallel()

	mockCommentRepo := &mockCommentRepository{}
	mockExceptionRepo := &mockExceptionRepository{}
	mockActor := &mockActorExtractor{}

	uc, err := NewExceptionUseCase(mockExceptionRepo, mockActor, &stubAuditPublisher{}, &stubInfraProvider{}, WithCommentRepository(mockCommentRepo))
	require.NoError(t, err)
	assert.NotNil(t, uc)
}

func TestCommentErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	assert.NotErrorIs(t, ErrNilCommentRepository, ErrCommentIDRequired)
	assert.NotErrorIs(t, ErrCommentIDRequired, ErrCommentContentEmpty)
	assert.NotErrorIs(t, ErrExceptionAlreadyResolved, ErrNotCommentAuthor)
}

// --- AddComment tests ---

func TestAddComment_ResolvedExceptionReturnsError(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	exceptionID := uuid.New()
	resolutionNotes := "resolved"

	resolvedException := &entities.Exception{
		ID:              exceptionID,
		TransactionID:   uuid.New(),
		Severity:        sharedexception.ExceptionSeverityHigh,
		Status:          value_objects.ExceptionStatusResolved,
		ResolutionNotes: &resolutionNotes,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	commentRepo := &stubCommentRepository{}
	exceptionRepo := &stubExceptionRepo{exception: resolvedException}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(exceptionRepo, actor, &stubAuditPublisher{}, &stubInfraProvider{}, WithCommentRepository(commentRepo))
	require.NoError(t, err)

	result, err := uc.AddComment(context.Background(), AddCommentInput{
		ExceptionID: exceptionID,
		Content:     "This should fail",
	})

	require.ErrorIs(t, err, ErrExceptionAlreadyResolved)
	assert.Nil(t, result)
}

func TestAddComment_OpenExceptionSucceeds(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	exceptionID := uuid.New()

	openException := &entities.Exception{
		ID:            exceptionID,
		TransactionID: uuid.New(),
		Severity:      sharedexception.ExceptionSeverityMedium,
		Status:        value_objects.ExceptionStatusOpen,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	commentRepo := &stubCommentRepository{}
	exceptionRepo := &stubExceptionRepo{exception: openException}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(exceptionRepo, actor, &stubAuditPublisher{}, &stubInfraProvider{}, WithCommentRepository(commentRepo))
	require.NoError(t, err)

	result, err := uc.AddComment(context.Background(), AddCommentInput{
		ExceptionID: exceptionID,
		Content:     "This should succeed",
	})

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, exceptionID, result.ExceptionID)
	assert.Equal(t, "analyst@example.com", result.Author)
}

func TestAddComment_AssignedExceptionSucceeds(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	exceptionID := uuid.New()
	assignee := "assignee@example.com"

	assignedException := &entities.Exception{
		ID:            exceptionID,
		TransactionID: uuid.New(),
		Severity:      sharedexception.ExceptionSeverityLow,
		Status:        value_objects.ExceptionStatusAssigned,
		AssignedTo:    &assignee,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	commentRepo := &stubCommentRepository{}
	exceptionRepo := &stubExceptionRepo{exception: assignedException}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(exceptionRepo, actor, &stubAuditPublisher{}, &stubInfraProvider{}, WithCommentRepository(commentRepo))
	require.NoError(t, err)

	result, err := uc.AddComment(context.Background(), AddCommentInput{
		ExceptionID: exceptionID,
		Content:     "Comment on assigned exception",
	})

	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestAddComment_ExceptionNotFoundReturnsError(t *testing.T) {
	t.Parallel()

	commentRepo := &stubCommentRepository{}
	exceptionRepo := &stubExceptionRepo{findErr: errTestFind}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(exceptionRepo, actor, &stubAuditPublisher{}, &stubInfraProvider{}, WithCommentRepository(commentRepo))
	require.NoError(t, err)

	result, err := uc.AddComment(context.Background(), AddCommentInput{
		ExceptionID: uuid.New(),
		Content:     "This should fail",
	})

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "find exception")
}

func TestAddComment_NilExceptionIDReturnsError(t *testing.T) {
	t.Parallel()

	commentRepo := &stubCommentRepository{}
	exceptionRepo := &stubExceptionRepo{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(exceptionRepo, actor, &stubAuditPublisher{}, &stubInfraProvider{}, WithCommentRepository(commentRepo))
	require.NoError(t, err)

	result, err := uc.AddComment(context.Background(), AddCommentInput{
		ExceptionID: uuid.Nil,
		Content:     "Test",
	})

	require.ErrorIs(t, err, ErrExceptionIDRequired)
	assert.Nil(t, result)
}

func TestAddComment_EmptyActorReturnsError(t *testing.T) {
	t.Parallel()

	commentRepo := &stubCommentRepository{}
	exceptionRepo := &stubExceptionRepo{}
	actor := actorExtractor("")

	uc, err := NewExceptionUseCase(exceptionRepo, actor, &stubAuditPublisher{}, &stubInfraProvider{}, WithCommentRepository(commentRepo))
	require.NoError(t, err)

	result, err := uc.AddComment(context.Background(), AddCommentInput{
		ExceptionID: uuid.New(),
		Content:     "Test",
	})

	require.ErrorIs(t, err, ErrActorRequired)
	assert.Nil(t, result)
}

func TestAddComment_EmptyContentReturnsError(t *testing.T) {
	t.Parallel()

	commentRepo := &stubCommentRepository{}
	exceptionRepo := &stubExceptionRepo{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(exceptionRepo, actor, &stubAuditPublisher{}, &stubInfraProvider{}, WithCommentRepository(commentRepo))
	require.NoError(t, err)

	result, err := uc.AddComment(context.Background(), AddCommentInput{
		ExceptionID: uuid.New(),
		Content:     "   ",
	})

	require.ErrorIs(t, err, ErrCommentContentEmpty)
	assert.Nil(t, result)
}

// --- DeleteComment tests ---

func TestDeleteComment_OwnCommentSucceeds(t *testing.T) {
	t.Parallel()

	exceptionID := uuid.New()
	commentID := uuid.New()
	now := time.Now().UTC()

	existingComment := &entities.ExceptionComment{
		ID:          commentID,
		ExceptionID: exceptionID,
		Author:      "analyst@example.com",
		Content:     "My comment",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	commentRepo := &stubCommentRepository{comment: existingComment}
	exceptionRepo := &stubExceptionRepo{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(exceptionRepo, actor, &stubAuditPublisher{}, &stubInfraProvider{}, WithCommentRepository(commentRepo))
	require.NoError(t, err)

	err = uc.DeleteComment(context.Background(), exceptionID, commentID)

	require.NoError(t, err)
}

func TestDeleteComment_OtherUsersCommentReturnsError(t *testing.T) {
	t.Parallel()

	exceptionID := uuid.New()
	commentID := uuid.New()
	now := time.Now().UTC()

	existingComment := &entities.ExceptionComment{
		ID:          commentID,
		ExceptionID: exceptionID,
		Author:      "original-author@example.com",
		Content:     "Someone else's comment",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	commentRepo := &stubCommentRepository{comment: existingComment}
	exceptionRepo := &stubExceptionRepo{}
	actor := actorExtractor("different-user@example.com")

	uc, err := NewExceptionUseCase(exceptionRepo, actor, &stubAuditPublisher{}, &stubInfraProvider{}, WithCommentRepository(commentRepo))
	require.NoError(t, err)

	err = uc.DeleteComment(context.Background(), exceptionID, commentID)

	require.ErrorIs(t, err, ErrNotCommentAuthor)
}

func TestDeleteComment_CommentNotFoundReturnsError(t *testing.T) {
	t.Parallel()

	commentRepo := &stubCommentRepository{findErr: entities.ErrCommentNotFound}
	exceptionRepo := &stubExceptionRepo{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(exceptionRepo, actor, &stubAuditPublisher{}, &stubInfraProvider{}, WithCommentRepository(commentRepo))
	require.NoError(t, err)

	err = uc.DeleteComment(context.Background(), uuid.New(), uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "find comment")
}

func TestDeleteComment_NilCommentIDReturnsError(t *testing.T) {
	t.Parallel()

	commentRepo := &stubCommentRepository{}
	exceptionRepo := &stubExceptionRepo{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(exceptionRepo, actor, &stubAuditPublisher{}, &stubInfraProvider{}, WithCommentRepository(commentRepo))
	require.NoError(t, err)

	err = uc.DeleteComment(context.Background(), uuid.New(), uuid.Nil)

	require.ErrorIs(t, err, ErrCommentIDRequired)
}

func TestDeleteComment_EmptyActorReturnsError(t *testing.T) {
	t.Parallel()

	commentRepo := &stubCommentRepository{}
	exceptionRepo := &stubExceptionRepo{}
	actor := actorExtractor("")

	uc, err := NewExceptionUseCase(exceptionRepo, actor, &stubAuditPublisher{}, &stubInfraProvider{}, WithCommentRepository(commentRepo))
	require.NoError(t, err)

	err = uc.DeleteComment(context.Background(), uuid.New(), uuid.New())

	require.ErrorIs(t, err, ErrActorRequired)
}

func TestDeleteComment_DeleteRepoErrorReturnsError(t *testing.T) {
	t.Parallel()

	exceptionID := uuid.New()
	commentID := uuid.New()
	now := time.Now().UTC()

	existingComment := &entities.ExceptionComment{
		ID:          commentID,
		ExceptionID: exceptionID,
		Author:      "analyst@example.com",
		Content:     "My comment",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	commentRepo := &stubCommentRepository{
		comment:   existingComment,
		deleteErr: errTestUpdate,
	}
	exceptionRepo := &stubExceptionRepo{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(exceptionRepo, actor, &stubAuditPublisher{}, &stubInfraProvider{}, WithCommentRepository(commentRepo))
	require.NoError(t, err)

	err = uc.DeleteComment(context.Background(), exceptionID, commentID)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete comment")
}

// TestDeleteComment_CrossExceptionDeletionRejected is a regression test for
// SEC-15: a caller must not be able to delete a comment belonging to
// exception A by sending the request to exception B's URL. The repository
// now filters on both exception_id and comment_id so a mismatch becomes a
// not-found response without revealing that the comment exists elsewhere.
func TestDeleteComment_CrossExceptionDeletionRejected(t *testing.T) {
	t.Parallel()

	ownerExceptionID := uuid.New()
	victimURLExceptionID := uuid.New() // different exception supplied by attacker
	commentID := uuid.New()
	now := time.Now().UTC()

	existingComment := &entities.ExceptionComment{
		ID:          commentID,
		ExceptionID: ownerExceptionID,
		Author:      "analyst@example.com",
		Content:     "My comment",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	commentRepo := &stubCommentRepository{comment: existingComment}
	exceptionRepo := &stubExceptionRepo{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(exceptionRepo, actor, &stubAuditPublisher{}, &stubInfraProvider{}, WithCommentRepository(commentRepo))
	require.NoError(t, err)

	err = uc.DeleteComment(context.Background(), victimURLExceptionID, commentID)

	require.ErrorIs(t, err, entities.ErrCommentNotFound)
}
