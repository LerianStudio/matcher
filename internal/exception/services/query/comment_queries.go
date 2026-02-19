package query

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
)

// Comment query errors.
var (
	ErrNilCommentRepository       = errors.New("comment repository is required")
	ErrCommentExceptionIDRequired = errors.New("exception id is required")
)

// CommentQueryUseCase implements read operations for exception comments.
type CommentQueryUseCase struct {
	commentRepo repositories.CommentRepository
}

// NewCommentQueryUseCase creates a new CommentQueryUseCase with the required repository.
func NewCommentQueryUseCase(
	commentRepo repositories.CommentRepository,
) (*CommentQueryUseCase, error) {
	if commentRepo == nil {
		return nil, ErrNilCommentRepository
	}

	return &CommentQueryUseCase{
		commentRepo: commentRepo,
	}, nil
}

// ListComments lists all comments for an exception.
func (uc *CommentQueryUseCase) ListComments(
	ctx context.Context,
	exceptionID uuid.UUID,
) ([]*entities.ExceptionComment, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only tracer needed
	ctx, span := tracer.Start(ctx, "query.comment.list")

	defer span.End()

	if exceptionID == uuid.Nil {
		libOpentelemetry.HandleSpanError(span, "exception id required", ErrCommentExceptionIDRequired)

		return nil, ErrCommentExceptionIDRequired
	}

	comments, err := uc.commentRepo.FindByExceptionID(ctx, exceptionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "list comments failed", err)

		return nil, fmt.Errorf("list comments: %w", err)
	}

	return comments, nil
}
