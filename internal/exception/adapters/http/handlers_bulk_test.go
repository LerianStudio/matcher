//go:build unit

package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	sharedhttp "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	"github.com/LerianStudio/matcher/internal/exception/services/command"
)

// --- parseUUIDs tests ---

func TestParseUUIDs_ValidUUIDs(t *testing.T) {
	t.Parallel()

	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()

	result, err := parseUUIDs([]string{id1.String(), id2.String(), id3.String()})

	require.NoError(t, err)
	require.Len(t, result, 3)
	assert.Equal(t, id1, result[0])
	assert.Equal(t, id2, result[1])
	assert.Equal(t, id3, result[2])
}

func TestParseUUIDs_EmptySlice(t *testing.T) {
	t.Parallel()

	result, err := parseUUIDs([]string{})

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestParseUUIDs_InvalidUUID(t *testing.T) {
	t.Parallel()

	result, err := parseUUIDs([]string{"not-a-valid-uuid"})

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "invalid uuid")
}

func TestParseUUIDs_NilUUID(t *testing.T) {
	t.Parallel()

	result, err := parseUUIDs([]string{uuid.Nil.String()})

	require.Error(t, err)
	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrNilUUIDNotAllowed)
}

func TestParseUUIDs_WhitespaceHandling(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	paddedID := "  " + id.String() + "  "

	result, err := parseUUIDs([]string{paddedID})

	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, id, result[0])
}

// --- toBulkActionResponse tests ---

func TestToBulkActionResponse_AllSucceeded(t *testing.T) {
	t.Parallel()

	id1 := uuid.New()
	id2 := uuid.New()

	result := &command.BulkActionResult{
		Succeeded: []uuid.UUID{id1, id2},
		Failed:    []command.BulkItemFailure{},
	}

	resp := toBulkActionResponse(result)

	assert.Len(t, resp.Succeeded, 2)
	assert.Empty(t, resp.Failed)
	assert.Equal(t, 2, resp.Total)
	assert.Equal(t, id1.String(), resp.Succeeded[0])
	assert.Equal(t, id2.String(), resp.Succeeded[1])
}

func TestToBulkActionResponse_AllFailed(t *testing.T) {
	t.Parallel()

	id1 := uuid.New()
	id2 := uuid.New()

	result := &command.BulkActionResult{
		Succeeded: []uuid.UUID{},
		Failed: []command.BulkItemFailure{
			{ExceptionID: id1, Error: "not found"},
			{ExceptionID: id2, Error: "access denied"},
		},
	}

	resp := toBulkActionResponse(result)

	assert.Empty(t, resp.Succeeded)
	assert.Len(t, resp.Failed, 2)
	assert.Equal(t, 2, resp.Total)
	assert.Equal(t, id1.String(), resp.Failed[0].ExceptionID)
	assert.Equal(t, "not found", resp.Failed[0].Error)
	assert.Equal(t, id2.String(), resp.Failed[1].ExceptionID)
	assert.Equal(t, "access denied", resp.Failed[1].Error)
}

func TestToBulkActionResponse_MixedResult(t *testing.T) {
	t.Parallel()

	successID := uuid.New()
	failedID := uuid.New()

	result := &command.BulkActionResult{
		Succeeded: []uuid.UUID{successID},
		Failed: []command.BulkItemFailure{
			{ExceptionID: failedID, Error: "state conflict"},
		},
	}

	resp := toBulkActionResponse(result)

	assert.Len(t, resp.Succeeded, 1)
	assert.Len(t, resp.Failed, 1)
	assert.Equal(t, 2, resp.Total)
	assert.Equal(t, successID.String(), resp.Succeeded[0])
	assert.Equal(t, failedID.String(), resp.Failed[0].ExceptionID)
}

func TestToBulkActionResponse_EmptyResult(t *testing.T) {
	t.Parallel()

	result := &command.BulkActionResult{
		Succeeded: []uuid.UUID{},
		Failed:    []command.BulkItemFailure{},
	}

	resp := toBulkActionResponse(result)

	assert.Empty(t, resp.Succeeded)
	assert.Empty(t, resp.Failed)
	assert.Equal(t, 0, resp.Total)
}

// --- handleBulkError tests ---

// executeBulkErrorHandler wraps handleBulkError to match the test helper pattern.
// handleBulkError has signature (fiberCtx, span, logger, message, err) unlike
// the standard error handler signature, so it needs its own test helper.
func executeBulkErrorHandler(
	t *testing.T,
	err error,
) *http.Response {
	t.Helper()

	tracer := noop.NewTracerProvider().Tracer("test")
	ctx := libCommons.ContextWithTracer(context.Background(), tracer)

	app := newFiberTestApp(ctx)
	app.Get("/", func(c *fiber.Ctx) error {
		spanCtx, span := tracer.Start(c.UserContext(), "test")
		c.SetUserContext(spanCtx)

		defer span.End()

		var logger libLog.Logger

		return handleBulkError(spanCtx, c, span, logger, "bulk operation failed", err)
	})

	request := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	resp, reqErr := app.Test(request)
	require.NoError(t, reqErr)

	return resp
}

// requireBulkErrorResponse validates a bulk error HTTP response matches expectations.
func requireBulkErrorResponse(
	t *testing.T,
	resp *http.Response,
	expectedStatus int,
	expectedCode int,
	expectedTitle,
	expectedMessage string,
) {
	t.Helper()

	defer resp.Body.Close()

	require.Equal(t, expectedStatus, resp.StatusCode)

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	require.Equal(t, expectedCode, errResp.Code)
	require.Equal(t, expectedTitle, errResp.Title)
	require.Equal(t, expectedMessage, errResp.Message)
}

func TestHandleBulkError_BulkEmptyIDs(t *testing.T) {
	t.Parallel()

	resp := executeBulkErrorHandler(t, command.ErrBulkEmptyIDs)

	requireBulkErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		command.ErrBulkEmptyIDs.Error(),
	)
}

func TestHandleBulkError_BulkTooManyIDs(t *testing.T) {
	t.Parallel()

	resp := executeBulkErrorHandler(t, command.ErrBulkTooManyIDs)

	requireBulkErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		command.ErrBulkTooManyIDs.Error(),
	)
}

func TestHandleBulkError_BulkAssigneeEmpty(t *testing.T) {
	t.Parallel()

	resp := executeBulkErrorHandler(t, command.ErrBulkAssigneeEmpty)

	requireBulkErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		command.ErrBulkAssigneeEmpty.Error(),
	)
}

func TestHandleBulkError_BulkResolutionEmpty(t *testing.T) {
	t.Parallel()

	resp := executeBulkErrorHandler(t, command.ErrBulkResolutionEmpty)

	requireBulkErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		command.ErrBulkResolutionEmpty.Error(),
	)
}

func TestHandleBulkError_BulkTargetSystemEmpty(t *testing.T) {
	t.Parallel()

	resp := executeBulkErrorHandler(t, command.ErrBulkTargetSystemEmpty)

	requireBulkErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		command.ErrBulkTargetSystemEmpty.Error(),
	)
}

func TestHandleBulkError_ActorRequired(t *testing.T) {
	t.Parallel()

	resp := executeBulkErrorHandler(t, command.ErrActorRequired)

	requireBulkErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		command.ErrActorRequired.Error(),
	)
}

func TestHandleBulkError_UnknownError(t *testing.T) {
	t.Parallel()

	unknownErr := errors.New("unexpected database failure")
	resp := executeBulkErrorHandler(t, unknownErr)

	requireBulkErrorResponse(
		t,
		resp,
		fiber.StatusInternalServerError,
		500,
		"internal_server_error",
		"an unexpected error occurred",
	)
}

// --- ErrNilUUIDNotAllowed sentinel ---

func TestErrNilUUIDNotAllowed_Message(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "nil uuid not allowed", ErrNilUUIDNotAllowed.Error())
}

// --- handleBulkError with wrapped errors ---

func TestHandleBulkError_WrappedBulkEmptyIDs(t *testing.T) {
	t.Parallel()

	wrappedErr := errors.Join(command.ErrBulkEmptyIDs, errors.New("additional context"))
	resp := executeBulkErrorHandler(t, wrappedErr)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

// --- parseUUIDs with mixed valid/invalid ---

func TestParseUUIDs_MixedValidAndInvalid(t *testing.T) {
	t.Parallel()

	validID := uuid.New()

	result, err := parseUUIDs([]string{validID.String(), "garbage"})

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "invalid uuid")
}

func TestParseUUIDs_SingleValidUUID(t *testing.T) {
	t.Parallel()

	id := uuid.New()

	result, err := parseUUIDs([]string{id.String()})

	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, id, result[0])
}

func TestParseUUIDs_NilSlice(t *testing.T) {
	t.Parallel()

	result, err := parseUUIDs(nil)

	require.NoError(t, err)
	assert.Empty(t, result)
}

// --- handleBulkError signature verification ---

func TestHandleBulkError_Signature(t *testing.T) {
	t.Parallel()

	// Verify handleBulkError has the expected function signature by type-asserting it.
	var fn func(context.Context, *fiber.Ctx, trace.Span, libLog.Logger, string, error) error = handleBulkError

	assert.NotNil(t, fn)
}
