package entities

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// Sentinel errors for ExceptionComment operations.
var (
	ErrCommentExceptionIDRequired = errors.New("exception id is required for comment")
	ErrCommentAuthorRequired      = errors.New("comment author is required")
	ErrCommentContentRequired     = errors.New("comment content is required")
	ErrCommentNotFound            = errors.New("comment not found")
)

// ExceptionComment represents a discussion comment on an exception.
type ExceptionComment struct {
	ID          uuid.UUID
	ExceptionID uuid.UUID
	Author      string
	Content     string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// NewExceptionComment creates a new ExceptionComment with validated fields.
func NewExceptionComment(
	ctx context.Context,
	exceptionID uuid.UUID,
	author, content string,
) (*ExceptionComment, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "exception.comment.new")

	if err := asserter.That(ctx, exceptionID != uuid.Nil, "exception id is required"); err != nil {
		return nil, ErrCommentExceptionIDRequired
	}

	trimmedAuthor := strings.TrimSpace(author)
	if err := asserter.NotEmpty(ctx, trimmedAuthor, "author is required"); err != nil {
		return nil, ErrCommentAuthorRequired
	}

	trimmedContent := strings.TrimSpace(content)
	if err := asserter.NotEmpty(ctx, trimmedContent, "content is required"); err != nil {
		return nil, ErrCommentContentRequired
	}

	now := time.Now().UTC()

	return &ExceptionComment{
		ID:          uuid.New(),
		ExceptionID: exceptionID,
		Author:      trimmedAuthor,
		Content:     trimmedContent,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}
