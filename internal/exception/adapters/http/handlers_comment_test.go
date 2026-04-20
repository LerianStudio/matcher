//go:build unit

package http

import (
	"errors"
	"testing"

	"github.com/gofiber/fiber/v2"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/services/command"
	"github.com/LerianStudio/matcher/internal/exception/services/query"
)

func TestHandleCommentError_ExceptionNotFound(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, (&Handlers{}).handleCommentError, entities.ErrExceptionNotFound)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusNotFound,
		404,
		"not_found",
		"exception not found",
	)
}

func TestHandleCommentError_CommentNotFound(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, (&Handlers{}).handleCommentError, entities.ErrCommentNotFound)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusNotFound,
		404,
		"not_found",
		"comment not found",
	)
}

func TestHandleCommentError_ExceptionIDRequired(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, (&Handlers{}).handleCommentError, command.ErrExceptionIDRequired)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		command.ErrExceptionIDRequired.Error(),
	)
}

func TestHandleCommentError_ActorRequired(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, (&Handlers{}).handleCommentError, command.ErrActorRequired)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		command.ErrActorRequired.Error(),
	)
}

func TestHandleCommentError_CommentContentEmpty(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, (&Handlers{}).handleCommentError, command.ErrCommentContentEmpty)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		command.ErrCommentContentEmpty.Error(),
	)
}

func TestHandleCommentError_CommentIDRequired(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, (&Handlers{}).handleCommentError, command.ErrCommentIDRequired)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		command.ErrCommentIDRequired.Error(),
	)
}

func TestHandleCommentError_CommentContentRequired(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, (&Handlers{}).handleCommentError, entities.ErrCommentContentRequired)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		entities.ErrCommentContentRequired.Error(),
	)
}

func TestHandleCommentError_CommentAuthorRequired(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, (&Handlers{}).handleCommentError, entities.ErrCommentAuthorRequired)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		entities.ErrCommentAuthorRequired.Error(),
	)
}

func TestHandleCommentError_CommentExceptionIDRequired(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, (&Handlers{}).handleCommentError, query.ErrCommentExceptionIDRequired)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		query.ErrCommentExceptionIDRequired.Error(),
	)
}

func TestHandleCommentError_UnknownError(t *testing.T) {
	t.Parallel()

	unknownErr := errors.New("unexpected storage error")
	resp := executeErrorHandler(t, (&Handlers{}).handleCommentError, unknownErr)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusInternalServerError,
		500,
		"internal_server_error",
		"an unexpected error occurred",
	)
}
