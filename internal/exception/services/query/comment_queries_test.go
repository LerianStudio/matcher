//go:build unit

package query

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
)

// stubCommentRepository implements repositories.CommentRepository for testing.
type stubCommentRepository struct {
	comments []*entities.ExceptionComment
	err      error
}

func (repo *stubCommentRepository) Create(
	_ context.Context,
	comment *entities.ExceptionComment,
) (*entities.ExceptionComment, error) {
	return comment, nil
}

func (repo *stubCommentRepository) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*entities.ExceptionComment, error) {
	return nil, nil
}

func (repo *stubCommentRepository) FindByExceptionID(
	_ context.Context,
	_ uuid.UUID,
) ([]*entities.ExceptionComment, error) {
	if repo.err != nil {
		return nil, repo.err
	}

	return repo.comments, nil
}

func (repo *stubCommentRepository) Delete(_ context.Context, _ uuid.UUID) error {
	return nil
}

// NewCommentQueryUseCase Tests.

func TestNewCommentQueryUseCase_Success(t *testing.T) {
	t.Parallel()

	commentRepo := &stubCommentRepository{}

	uc, err := NewCommentQueryUseCase(commentRepo)

	require.NoError(t, err)
	require.NotNil(t, uc)
}

func TestNewCommentQueryUseCase_NilRepository(t *testing.T) {
	t.Parallel()

	uc, err := NewCommentQueryUseCase(nil)

	require.ErrorIs(t, err, ErrNilCommentRepository)
	assert.Nil(t, uc)
}

// ListComments Tests.

func TestListComments_Success(t *testing.T) {
	t.Parallel()

	exceptionID := uuid.New()
	now := time.Now().UTC()

	expectedComments := []*entities.ExceptionComment{
		{
			ID:          uuid.New(),
			ExceptionID: exceptionID,
			Author:      "user@test.com",
			Content:     "first comment",
			CreatedAt:   now,
		},
		{
			ID:          uuid.New(),
			ExceptionID: exceptionID,
			Author:      "admin@test.com",
			Content:     "second comment",
			CreatedAt:   now.Add(time.Minute),
		},
	}

	commentRepo := &stubCommentRepository{comments: expectedComments}

	uc, err := NewCommentQueryUseCase(commentRepo)
	require.NoError(t, err)

	ctx := t.Context()
	comments, err := uc.ListComments(ctx, exceptionID)

	require.NoError(t, err)
	assert.Len(t, comments, 2)
	assert.Equal(t, expectedComments[0].Content, comments[0].Content)
	assert.Equal(t, expectedComments[1].Content, comments[1].Content)
}

func TestListComments_EmptyResult(t *testing.T) {
	t.Parallel()

	commentRepo := &stubCommentRepository{
		comments: []*entities.ExceptionComment{},
	}

	uc, err := NewCommentQueryUseCase(commentRepo)
	require.NoError(t, err)

	ctx := t.Context()
	comments, err := uc.ListComments(ctx, uuid.New())

	require.NoError(t, err)
	assert.Empty(t, comments)
}

func TestListComments_NilExceptionID(t *testing.T) {
	t.Parallel()

	commentRepo := &stubCommentRepository{}

	uc, err := NewCommentQueryUseCase(commentRepo)
	require.NoError(t, err)

	ctx := t.Context()
	comments, err := uc.ListComments(ctx, uuid.Nil)

	require.ErrorIs(t, err, ErrCommentExceptionIDRequired)
	assert.Nil(t, comments)
}

func TestListComments_RepositoryError(t *testing.T) {
	t.Parallel()

	repoErr := errors.New("database connection failed")
	commentRepo := &stubCommentRepository{err: repoErr}

	uc, err := NewCommentQueryUseCase(commentRepo)
	require.NoError(t, err)

	ctx := t.Context()
	comments, err := uc.ListComments(ctx, uuid.New())

	require.Error(t, err)
	require.ErrorIs(t, err, repoErr)
	assert.Contains(t, err.Error(), "list comments")
	assert.Nil(t, comments)
}
