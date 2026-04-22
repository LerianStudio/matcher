package http

import (
	"context"
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/exception/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/services/command"
	"github.com/LerianStudio/matcher/internal/exception/services/query"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

var _ = sharedhttp.ErrorResponse{}

// AddComment adds a comment to an exception.
// @Summary Add comment to exception
// @Description Adds a discussion comment to an exception. The author is extracted from the JWT context.
// @ID addComment
// @Tags Exception
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param exceptionId path string true "Exception ID" format(uuid)
// @Param request body dto.AddCommentRequest true "Comment payload"
// @Success 201 {object} dto.CommentResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Exception not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/exceptions/{exceptionId}/comments [post]
func (handler *Handlers) AddComment(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.add_comment")
	defer span.End()

	exceptionID, tenantID, err := libHTTP.ParseAndVerifyResourceScopedID(
		fiberCtx,
		"exceptionId",
		libHTTP.IDLocationParam,
		handler.exceptionVerifier,
		auth.GetTenantID,
		ErrMissingExceptionID,
		ErrInvalidExceptionID,
		ErrExceptionAccessDenied,
		"exception",
	)
	if err != nil {
		return handler.handleExceptionVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetExceptionSpanAttributes(span, tenantID, exceptionID)

	var req dto.AddCommentRequest

	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	result, err := handler.commandUC.AddComment(ctx, command.AddCommentInput{
		ExceptionID: exceptionID,
		Content:     req.Content,
	})
	if err != nil {
		return handler.handleCommentError(ctx, fiberCtx, span, logger, err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusCreated, dto.CommentToResponse(result)); err != nil {
		return fmt.Errorf("respond create comment: %w", err)
	}

	return nil
}

// ListComments lists all comments for an exception.
// @Summary List comments for exception
// @Description Retrieves all discussion comments for an exception, ordered by creation time (oldest first).
// @ID listComments
// @Tags Exception
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param exceptionId path string true "Exception ID" format(uuid)
// @Success 200 {object} dto.ListCommentsResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Exception not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/exceptions/{exceptionId}/comments [get]
func (handler *Handlers) ListComments(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.list_comments")
	defer span.End()

	exceptionID, tenantID, err := libHTTP.ParseAndVerifyResourceScopedID(
		fiberCtx,
		"exceptionId",
		libHTTP.IDLocationParam,
		handler.exceptionVerifier,
		auth.GetTenantID,
		ErrMissingExceptionID,
		ErrInvalidExceptionID,
		ErrExceptionAccessDenied,
		"exception",
	)
	if err != nil {
		return handler.handleExceptionVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetExceptionSpanAttributes(span, tenantID, exceptionID)

	comments, err := handler.commentQueryUC.ListComments(ctx, exceptionID)
	if err != nil {
		return handler.handleCommentError(ctx, fiberCtx, span, logger, err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ListCommentsResponse{
		Items: dto.CommentsToResponse(comments),
	}); err != nil {
		return fmt.Errorf("respond list comments: %w", err)
	}

	return nil
}

// DeleteComment deletes a comment by ID.
// @Summary Delete a comment
// @Description Deletes a discussion comment from an exception.
// @ID deleteComment
// @Tags Exception
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param exceptionId path string true "Exception ID" format(uuid)
// @Param commentId path string true "Comment ID" format(uuid)
// @Success 204
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Comment not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/exceptions/{exceptionId}/comments/{commentId} [delete]
func (handler *Handlers) DeleteComment(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.delete_comment")
	defer span.End()

	// Verify exception ownership
	exceptionID, tenantID, err := libHTTP.ParseAndVerifyResourceScopedID(
		fiberCtx,
		"exceptionId",
		libHTTP.IDLocationParam,
		handler.exceptionVerifier,
		auth.GetTenantID,
		ErrMissingExceptionID,
		ErrInvalidExceptionID,
		ErrExceptionAccessDenied,
		"exception",
	)
	if err != nil {
		return handler.handleExceptionVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetExceptionSpanAttributes(span, tenantID, exceptionID)

	commentIDStr := fiberCtx.Params("commentId")
	if commentIDStr == "" {
		return handler.badRequest(ctx, fiberCtx, span, logger, "comment id is required", ErrMissingParameter)
	}

	commentID, err := uuid.Parse(commentIDStr)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid comment id", ErrInvalidParameter)
	}

	if err := handler.commandUC.DeleteComment(ctx, exceptionID, commentID); err != nil {
		return handler.handleCommentError(ctx, fiberCtx, span, logger, err)
	}

	if err := libHTTP.RespondStatus(fiberCtx, fiber.StatusNoContent); err != nil {
		return fmt.Errorf("respond delete comment: %w", err)
	}

	return nil
}

// handleCommentError maps comment use case errors to HTTP responses.
func (handler *Handlers) handleCommentError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	if errors.Is(err, entities.ErrExceptionNotFound) {
		return handler.notFoundWithSlug(ctx, fiberCtx, span, logger, "exception_not_found", "exception not found", err)
	}

	if errors.Is(err, entities.ErrCommentNotFound) {
		return handler.notFoundWithSlug(ctx, fiberCtx, span, logger, "comment_not_found", "comment not found", err)
	}

	if errors.Is(err, command.ErrExceptionIDRequired) ||
		errors.Is(err, query.ErrCommentExceptionIDRequired) ||
		errors.Is(err, command.ErrActorRequired) ||
		errors.Is(err, command.ErrCommentContentEmpty) ||
		errors.Is(err, command.ErrCommentIDRequired) ||
		errors.Is(err, entities.ErrCommentContentRequired) ||
		errors.Is(err, entities.ErrCommentAuthorRequired) {
		return handler.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	return handler.internalError(ctx, fiberCtx, span, logger, "failed to process comment", err)
}
