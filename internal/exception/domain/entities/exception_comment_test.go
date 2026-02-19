//go:build unit

package entities

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestNewExceptionComment_ValidInput(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exceptionID := testutil.DeterministicUUID("exception")

	comment, err := NewExceptionComment(ctx, exceptionID, "user@example.com", "This needs review")

	require.NoError(t, err)
	require.NotNil(t, comment)
	assert.NotEqual(t, uuid.Nil, comment.ID)
	assert.Equal(t, exceptionID, comment.ExceptionID)
	assert.Equal(t, "user@example.com", comment.Author)
	assert.Equal(t, "This needs review", comment.Content)
	assert.False(t, comment.CreatedAt.IsZero())
	assert.False(t, comment.UpdatedAt.IsZero())
}

func TestNewExceptionComment_NilExceptionID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	comment, err := NewExceptionComment(ctx, uuid.Nil, "user@example.com", "content")

	require.Error(t, err)
	require.Nil(t, comment)
	assert.ErrorIs(t, err, ErrCommentExceptionIDRequired)
}

func TestNewExceptionComment_EmptyAuthor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	comment, err := NewExceptionComment(ctx, testutil.DeterministicUUID("comment-empty-author"), "", "content")

	require.Error(t, err)
	require.Nil(t, comment)
	assert.ErrorIs(t, err, ErrCommentAuthorRequired)
}

func TestNewExceptionComment_WhitespaceAuthor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	comment, err := NewExceptionComment(ctx, testutil.DeterministicUUID("comment-whitespace-author"), "   ", "content")

	require.Error(t, err)
	require.Nil(t, comment)
	assert.ErrorIs(t, err, ErrCommentAuthorRequired)
}

func TestNewExceptionComment_EmptyContent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	comment, err := NewExceptionComment(ctx, testutil.DeterministicUUID("comment-empty-content"), "author", "")

	require.Error(t, err)
	require.Nil(t, comment)
	assert.ErrorIs(t, err, ErrCommentContentRequired)
}

func TestNewExceptionComment_WhitespaceContent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	comment, err := NewExceptionComment(ctx, testutil.DeterministicUUID("comment-whitespace-content"), "author", "   ")

	require.Error(t, err)
	require.Nil(t, comment)
	assert.ErrorIs(t, err, ErrCommentContentRequired)
}

func TestNewExceptionComment_TrimsWhitespace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	comment, err := NewExceptionComment(ctx, testutil.DeterministicUUID("comment-trims-whitespace"), "  user@example.com  ", "  trimmed content  ")

	require.NoError(t, err)
	require.NotNil(t, comment)
	assert.Equal(t, "user@example.com", comment.Author)
	assert.Equal(t, "trimmed content", comment.Content)
}
