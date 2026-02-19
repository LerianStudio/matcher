package command

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

// Comment command errors.
var (
	ErrNilCommentRepository     = errors.New("comment repository is required")
	ErrCommentIDRequired        = errors.New("comment id is required")
	ErrCommentContentEmpty      = errors.New("comment content is required")
	ErrExceptionAlreadyResolved = errors.New("cannot add comment to a resolved exception")
	ErrNotCommentAuthor         = errors.New("only the comment author can delete their comment")
)

// CommentUseCase implements comment operations on exceptions.
type CommentUseCase struct {
	commentRepo    repositories.CommentRepository
	exceptionRepo  repositories.ExceptionRepository
	actorExtractor interface {
		GetActor(ctx context.Context) string
	}
}

// NewCommentUseCase creates a new CommentUseCase with required dependencies.
func NewCommentUseCase(
	commentRepo repositories.CommentRepository,
	exceptionRepo repositories.ExceptionRepository,
	actorExtractor interface {
		GetActor(ctx context.Context) string
	},
) (*CommentUseCase, error) {
	if commentRepo == nil {
		return nil, ErrNilCommentRepository
	}

	if exceptionRepo == nil {
		return nil, ErrNilExceptionRepository
	}

	if actorExtractor == nil {
		return nil, ErrNilActorExtractor
	}

	return &CommentUseCase{
		commentRepo:    commentRepo,
		exceptionRepo:  exceptionRepo,
		actorExtractor: actorExtractor,
	}, nil
}

// AddCommentInput contains the input for adding a comment.
type AddCommentInput struct {
	ExceptionID uuid.UUID
	Content     string
}

// AddComment adds a comment to an exception.
func (uc *CommentUseCase) AddComment(
	ctx context.Context,
	input AddCommentInput,
) (*entities.ExceptionComment, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "command.comment.add")

	defer span.End()

	if input.ExceptionID == uuid.Nil {
		return nil, ErrExceptionIDRequired
	}

	author := uc.actorExtractor.GetActor(ctx)
	if author == "" {
		return nil, ErrActorRequired
	}

	if strings.TrimSpace(input.Content) == "" {
		return nil, ErrCommentContentEmpty
	}

	// Verify exception exists and is not in a terminal state.
	exception, err := uc.exceptionRepo.FindByID(ctx, input.ExceptionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "exception not found", err)

		return nil, fmt.Errorf("find exception: %w", err)
	}

	if exception == nil {
		return nil, fmt.Errorf("find exception: %w", entities.ErrExceptionNotFound)
	}

	if exception.Status == value_objects.ExceptionStatusResolved {
		return nil, ErrExceptionAlreadyResolved
	}

	comment, err := entities.NewExceptionComment(ctx, input.ExceptionID, author, input.Content)
	if err != nil {
		return nil, fmt.Errorf("create comment entity: %w", err)
	}

	result, err := uc.commentRepo.Create(ctx, comment)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create comment", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to create comment: %v", err))

		return nil, fmt.Errorf("persist comment: %w", err)
	}

	return result, nil
}

// DeleteComment deletes a comment by ID.
func (uc *CommentUseCase) DeleteComment(
	ctx context.Context,
	commentID uuid.UUID,
) error {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only tracer needed
	ctx, span := tracer.Start(ctx, "command.comment.delete")

	defer span.End()

	if commentID == uuid.Nil {
		return ErrCommentIDRequired
	}

	actor := uc.actorExtractor.GetActor(ctx)
	if actor == "" {
		return ErrActorRequired
	}

	// Load the comment to verify ownership before deletion.
	comment, err := uc.commentRepo.FindByID(ctx, commentID)
	if err != nil {
		if errors.Is(err, entities.ErrCommentNotFound) {
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "comment not found", err)
		} else {
			libOpentelemetry.HandleSpanError(span, "find comment for delete", err)
		}

		return fmt.Errorf("find comment: %w", err)
	}

	if comment == nil {
		return entities.ErrCommentNotFound
	}

	if comment.Author != actor {
		return ErrNotCommentAuthor
	}

	if err := uc.commentRepo.Delete(ctx, commentID); err != nil {
		libOpentelemetry.HandleSpanError(span, "delete comment", err)

		return fmt.Errorf("delete comment: %w", err)
	}

	return nil
}
