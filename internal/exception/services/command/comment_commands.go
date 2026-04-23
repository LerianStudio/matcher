package command

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
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

// validateCommentDeps checks the dependencies required by comment
// operations. Safe on a nil receiver — returns ErrNilCommentRepository so
// nil-UseCase callers get a deterministic error rather than a panic.
func (uc *ExceptionUseCase) validateCommentDeps() error {
	if uc == nil || uc.commentRepo == nil {
		return ErrNilCommentRepository
	}

	if uc.exceptionRepo == nil {
		return ErrNilExceptionRepository
	}

	if uc.actorExtractor == nil {
		return ErrNilActorExtractor
	}

	return nil
}

// AddCommentInput contains the input for adding a comment.
type AddCommentInput struct {
	ExceptionID uuid.UUID
	Content     string
}

// AddComment adds a comment to an exception.
func (uc *ExceptionUseCase) AddComment(
	ctx context.Context,
	input AddCommentInput,
) (*entities.ExceptionComment, error) {
	if err := uc.validateCommentDeps(); err != nil {
		return nil, err
	}

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

// DeleteComment deletes a comment scoped to a specific exception.
//
// Both exceptionID and commentID are mandatory: the delete SQL filters on
// both columns, so a comment that belongs to exception A cannot be deleted
// by submitting its commentID under exception B's URL. The handler already
// verifies tenant ownership of the exception before reaching this method.
func (uc *ExceptionUseCase) DeleteComment(
	ctx context.Context,
	exceptionID, commentID uuid.UUID,
) error {
	if err := uc.validateCommentDeps(); err != nil {
		return err
	}

	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only tracer needed
	ctx, span := tracer.Start(ctx, "command.comment.delete")

	defer span.End()

	if exceptionID == uuid.Nil {
		return ErrExceptionIDRequired
	}

	if commentID == uuid.Nil {
		return ErrCommentIDRequired
	}

	actor := uc.actorExtractor.GetActor(ctx)
	if actor == "" {
		return ErrActorRequired
	}

	// Load the comment to verify ownership before deletion. FindByID
	// returns the comment regardless of its exception, but the actor
	// check below ensures the caller authored it. The DeleteByExceptionAndID
	// call then enforces that the comment still belongs to the exception
	// whose ownership the HTTP handler just verified — so the URL path
	// cannot lie about which exception the comment lives under.
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

	if comment.ExceptionID != exceptionID {
		// URL lied about the comment's parent exception. Return the same
		// not-found sentinel as a genuinely missing comment to avoid
		// leaking existence of comments under other exceptions.
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "comment does not belong to the provided exception", entities.ErrCommentNotFound)

		return entities.ErrCommentNotFound
	}

	if comment.Author != actor {
		return ErrNotCommentAuthor
	}

	if err := uc.commentRepo.DeleteByExceptionAndID(ctx, exceptionID, commentID); err != nil {
		libOpentelemetry.HandleSpanError(span, "delete comment", err)

		return fmt.Errorf("delete comment: %w", err)
	}

	return nil
}
