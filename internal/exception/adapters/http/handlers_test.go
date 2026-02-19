//go:build unit

package http

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	exceptionRepositories "github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/services/command"
	"github.com/LerianStudio/matcher/internal/exception/services/query"
	govEntities "github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

// errTest is a sentinel error for testing internal error handling.
var errTest = errors.New("boom")

func newFiberTestApp(ctx context.Context) *fiber.App {
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		c.SetUserContext(ctx)
		return c.Next()
	})

	return app
}

func executeErrorHandler(
	t *testing.T,
	handler func(context.Context, *fiber.Ctx, trace.Span, libLog.Logger, error) error,
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

		return handler(spanCtx, c, span, &libLog.NopLogger{}, err)
	})

	request := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	resp, reqErr := app.Test(request)
	require.NoError(t, reqErr)

	return resp
}

func requireErrorResponse(
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

	var errResp libHTTP.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	require.Equal(t, expectedCode, errResp.Code)
	require.Equal(t, expectedTitle, errResp.Title)
	require.Equal(t, expectedMessage, errResp.Message)
}

type stubExceptionRepo struct {
	exception    *entities.Exception
	err          error
	returnCtxErr bool
	seenCtxErr   error
}

func (repo *stubExceptionRepo) FindByID(
	ctx context.Context,
	_ uuid.UUID,
) (*entities.Exception, error) {
	repo.seenCtxErr = ctx.Err()

	if repo.returnCtxErr {
		return nil, fmt.Errorf("context error: %w", ctx.Err())
	}

	if repo.err != nil {
		return nil, repo.err
	}

	return repo.exception, nil
}

func (repo *stubExceptionRepo) List(
	_ context.Context,
	_ exceptionRepositories.ExceptionFilter,
	_ exceptionRepositories.CursorFilter,
) ([]*entities.Exception, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, repo.err
}

func (repo *stubExceptionRepo) Update(
	_ context.Context,
	exception *entities.Exception,
) (*entities.Exception, error) {
	if repo.err != nil {
		return nil, repo.err
	}

	return exception, nil
}

func (repo *stubExceptionRepo) UpdateWithTx(
	ctx context.Context,
	_ exceptionRepositories.Tx,
	exception *entities.Exception,
) (*entities.Exception, error) {
	return repo.Update(ctx, exception)
}

type stubAuditRepo struct{}

func (repo *stubAuditRepo) Create(
	_ context.Context,
	_ *govEntities.AuditLog,
) (*govEntities.AuditLog, error) {
	return nil, nil
}

func (repo *stubAuditRepo) CreateWithTx(
	ctx context.Context,
	_ exceptionRepositories.Tx,
	auditLog *govEntities.AuditLog,
) (*govEntities.AuditLog, error) {
	return repo.Create(ctx, auditLog)
}

func (repo *stubAuditRepo) GetByID(_ context.Context, _ uuid.UUID) (*govEntities.AuditLog, error) {
	return nil, nil
}

func (repo *stubAuditRepo) ListByEntity(
	_ context.Context,
	_ string,
	_ uuid.UUID,
	_ *libHTTP.TimestampCursor,
	_ int,
) ([]*govEntities.AuditLog, string, error) {
	return nil, "", nil
}

func (repo *stubAuditRepo) List(
	_ context.Context,
	_ govEntities.AuditLogFilter,
	_ *libHTTP.TimestampCursor,
	_ int,
) ([]*govEntities.AuditLog, string, error) {
	return nil, "", nil
}

type stubDisputeRepo struct{}

func (repo *stubDisputeRepo) Create(_ context.Context, d *dispute.Dispute) (*dispute.Dispute, error) {
	return d, nil
}

func (repo *stubDisputeRepo) CreateWithTx(_ context.Context, _ exceptionRepositories.Tx, d *dispute.Dispute) (*dispute.Dispute, error) {
	return d, nil
}

func (repo *stubDisputeRepo) FindByID(_ context.Context, _ uuid.UUID) (*dispute.Dispute, error) {
	return nil, nil
}

func (repo *stubDisputeRepo) FindByExceptionID(_ context.Context, _ uuid.UUID) (*dispute.Dispute, error) {
	return nil, nil
}

func (repo *stubDisputeRepo) List(_ context.Context, _ exceptionRepositories.DisputeFilter, _ exceptionRepositories.CursorFilter) ([]*dispute.Dispute, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (repo *stubDisputeRepo) Update(_ context.Context, d *dispute.Dispute) (*dispute.Dispute, error) {
	return d, nil
}

func (repo *stubDisputeRepo) UpdateWithTx(_ context.Context, _ exceptionRepositories.Tx, d *dispute.Dispute) (*dispute.Dispute, error) {
	return d, nil
}

// stubExceptionProvider implements the exceptionProvider interface for testing.
type stubExceptionProvider struct {
	exists bool
	err    error
}

func (p *stubExceptionProvider) ExistsForTenant(_ context.Context, _ uuid.UUID) (bool, error) {
	if p.err != nil {
		return false, p.err
	}

	return p.exists, nil
}

// stubDisputeProvider implements the disputeProvider interface for testing.
type stubDisputeProvider struct {
	exists bool
	err    error
}

func (p *stubDisputeProvider) ExistsForTenant(_ context.Context, _ uuid.UUID) (bool, error) {
	if p.err != nil {
		return false, p.err
	}

	return p.exists, nil
}

func newExceptionHandlers(t *testing.T, exceptionRepo *stubExceptionRepo) *Handlers {
	t.Helper()

	queryUC, err := query.NewUseCase(exceptionRepo, &stubDisputeRepo{}, &stubAuditRepo{}, nil)
	require.NoError(t, err)

	exceptionProvider := &stubExceptionProvider{exists: true}
	disputeProvider := &stubDisputeProvider{exists: true}

	handlers, err := NewHandlers(
		&command.UseCase{},
		&command.DisputeUseCase{},
		queryUC,
		&command.DispatchUseCase{},
		&command.CommentUseCase{},
		&query.CommentQueryUseCase{},
		&command.CallbackUseCase{},
		exceptionProvider,
		disputeProvider,
	)
	require.NoError(t, err)

	return handlers
}

func TestNewHandlers_NilExceptionUseCase(t *testing.T) {
	t.Parallel()

	exceptionProvider := &stubExceptionProvider{exists: true}
	disputeProvider := &stubDisputeProvider{exists: true}

	handlers, err := NewHandlers(
		nil,
		&command.DisputeUseCase{},
		&query.UseCase{},
		&command.DispatchUseCase{},
		&command.CommentUseCase{},
		&query.CommentQueryUseCase{},
		&command.CallbackUseCase{},
		exceptionProvider,
		disputeProvider,
	)

	assert.Nil(t, handlers)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilExceptionUseCase)
}

func TestNewHandlers_NilDisputeUseCase(t *testing.T) {
	t.Parallel()

	exceptionProvider := &stubExceptionProvider{exists: true}
	disputeProvider := &stubDisputeProvider{exists: true}

	handlers, err := NewHandlers(
		&command.UseCase{},
		nil,
		&query.UseCase{},
		&command.DispatchUseCase{},
		&command.CommentUseCase{},
		&query.CommentQueryUseCase{},
		&command.CallbackUseCase{},
		exceptionProvider,
		disputeProvider,
	)

	assert.Nil(t, handlers)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilDisputeUseCase)
}

func TestNewHandlers_NilQueryUseCase(t *testing.T) {
	t.Parallel()

	exceptionProvider := &stubExceptionProvider{exists: true}
	disputeProvider := &stubDisputeProvider{exists: true}

	handlers, err := NewHandlers(
		&command.UseCase{},
		&command.DisputeUseCase{},
		nil,
		&command.DispatchUseCase{},
		&command.CommentUseCase{},
		&query.CommentQueryUseCase{},
		&command.CallbackUseCase{},
		exceptionProvider,
		disputeProvider,
	)

	assert.Nil(t, handlers)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilQueryUseCase)
}

func TestNewHandlers_NilDispatchUseCase(t *testing.T) {
	t.Parallel()

	exceptionProvider := &stubExceptionProvider{exists: true}
	disputeProvider := &stubDisputeProvider{exists: true}

	handlers, err := NewHandlers(
		&command.UseCase{},
		&command.DisputeUseCase{},
		&query.UseCase{},
		nil,
		&command.CommentUseCase{},
		&query.CommentQueryUseCase{},
		&command.CallbackUseCase{},
		exceptionProvider,
		disputeProvider,
	)

	assert.Nil(t, handlers)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilDispatchUseCase)
}

func TestNewHandlers_NilCommentQueryUseCase(t *testing.T) {
	t.Parallel()

	exceptionProvider := &stubExceptionProvider{exists: true}
	disputeProvider := &stubDisputeProvider{exists: true}

	handlers, err := NewHandlers(
		&command.UseCase{},
		&command.DisputeUseCase{},
		&query.UseCase{},
		&command.DispatchUseCase{},
		&command.CommentUseCase{},
		nil,
		&command.CallbackUseCase{},
		exceptionProvider,
		disputeProvider,
	)

	assert.Nil(t, handlers)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilCommentQueryUseCase)
}

func TestNewHandlers_NilExceptionProvider(t *testing.T) {
	t.Parallel()

	disputeProvider := &stubDisputeProvider{exists: true}

	handlers, err := NewHandlers(
		&command.UseCase{},
		&command.DisputeUseCase{},
		&query.UseCase{},
		&command.DispatchUseCase{},
		&command.CommentUseCase{},
		&query.CommentQueryUseCase{},
		&command.CallbackUseCase{},
		nil,
		disputeProvider,
	)

	assert.Nil(t, handlers)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilExceptionProvider)
}

func TestNewHandlers_NilDisputeProvider(t *testing.T) {
	t.Parallel()

	exceptionProvider := &stubExceptionProvider{exists: true}

	handlers, err := NewHandlers(
		&command.UseCase{},
		&command.DisputeUseCase{},
		&query.UseCase{},
		&command.DispatchUseCase{},
		&command.CommentUseCase{},
		&query.CommentQueryUseCase{},
		&command.CallbackUseCase{},
		exceptionProvider,
		nil,
	)

	assert.Nil(t, handlers)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilDisputeProvider)
}

func TestNewHandlers_Success(t *testing.T) {
	t.Parallel()

	exceptionProvider := &stubExceptionProvider{exists: true}
	disputeProvider := &stubDisputeProvider{exists: true}

	handlers, err := NewHandlers(
		&command.UseCase{},
		&command.DisputeUseCase{},
		&query.UseCase{},
		&command.DispatchUseCase{},
		&command.CommentUseCase{},
		&query.CommentQueryUseCase{},
		&command.CallbackUseCase{},
		exceptionProvider,
		disputeProvider,
	)

	require.NoError(t, err)
	assert.NotNil(t, handlers)
}

func TestErrMissingParameter_Message(t *testing.T) {
	t.Parallel()

	// Create a minimal mock fiber context - testing the function logic
	// The function expects params from fiber context, so we test the error wrapping
	testErr := ErrMissingParameter
	assert.Contains(t, testErr.Error(), "missing required parameter")
}

func TestErrInvalidParameter_Message(t *testing.T) {
	t.Parallel()

	testErr := ErrInvalidParameter
	assert.Contains(t, testErr.Error(), "invalid parameter format")
}

func TestHandleExceptionError_Mappings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		err             error
		expectedStatus  int
		expectedCode    int
		expectedTitle   string
		expectedMessage string
	}{
		{
			name:            "not found",
			err:             sql.ErrNoRows,
			expectedStatus:  fiber.StatusNotFound,
			expectedCode:    404,
			expectedTitle:   "not_found",
			expectedMessage: "exception not found",
		},
		{
			name:            "bad request",
			err:             command.ErrExceptionIDRequired,
			expectedStatus:  fiber.StatusBadRequest,
			expectedCode:    400,
			expectedTitle:   "invalid_request",
			expectedMessage: command.ErrExceptionIDRequired.Error(),
		},
		{
			name:            "unprocessable entity",
			err:             entities.ErrExceptionMustBeOpenOrAssignedToResolve,
			expectedStatus:  fiber.StatusUnprocessableEntity,
			expectedCode:    422,
			expectedTitle:   "unprocessable_entity",
			expectedMessage: "exception cannot be resolved in current state",
		},
		{
			name:            "internal error",
			err:             errTest,
			expectedStatus:  fiber.StatusInternalServerError,
			expectedCode:    500,
			expectedTitle:   "internal_server_error",
			expectedMessage: "an unexpected error occurred",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := executeErrorHandler(t, handleExceptionError, tt.err)
			defer resp.Body.Close()

			requireErrorResponse(
				t,
				resp,
				tt.expectedStatus,
				tt.expectedCode,
				tt.expectedTitle,
				tt.expectedMessage,
			)
		})
	}
}

func TestHandleDisputeError_Mappings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		err             error
		expectedStatus  int
		expectedCode    int
		expectedTitle   string
		expectedMessage string
	}{
		{
			name:            "not found",
			err:             sql.ErrNoRows,
			expectedStatus:  fiber.StatusNotFound,
			expectedCode:    404,
			expectedTitle:   "not_found",
			expectedMessage: "dispute not found",
		},
		{
			name:            "bad request",
			err:             command.ErrDisputeIDRequired,
			expectedStatus:  fiber.StatusBadRequest,
			expectedCode:    400,
			expectedTitle:   "invalid_request",
			expectedMessage: command.ErrDisputeIDRequired.Error(),
		},
		{
			name:            "unprocessable entity",
			err:             dispute.ErrCannotAddEvidenceInCurrentState,
			expectedStatus:  fiber.StatusUnprocessableEntity,
			expectedCode:    422,
			expectedTitle:   "unprocessable_entity",
			expectedMessage: dispute.ErrCannotAddEvidenceInCurrentState.Error(),
		},
		{
			name:            "internal error",
			err:             errTest,
			expectedStatus:  fiber.StatusInternalServerError,
			expectedCode:    500,
			expectedTitle:   "internal_server_error",
			expectedMessage: "an unexpected error occurred",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := executeErrorHandler(t, handleDisputeError, tt.err)
			defer resp.Body.Close()

			requireErrorResponse(
				t,
				resp,
				tt.expectedStatus,
				tt.expectedCode,
				tt.expectedTitle,
				tt.expectedMessage,
			)
		})
	}
}

func TestHandleDispatchError_Mappings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		err             error
		expectedStatus  int
		expectedCode    int
		expectedTitle   string
		expectedMessage string
	}{
		{
			name:            "not found",
			err:             sql.ErrNoRows,
			expectedStatus:  fiber.StatusNotFound,
			expectedCode:    404,
			expectedTitle:   "not_found",
			expectedMessage: "exception not found",
		},
		{
			name:            "bad request",
			err:             command.ErrTargetSystemRequired,
			expectedStatus:  fiber.StatusBadRequest,
			expectedCode:    400,
			expectedTitle:   "invalid_request",
			expectedMessage: command.ErrTargetSystemRequired.Error(),
		},
		{
			name:            "unprocessable entity",
			err:             command.ErrUnsupportedTargetSystem,
			expectedStatus:  fiber.StatusUnprocessableEntity,
			expectedCode:    422,
			expectedTitle:   "unprocessable_entity",
			expectedMessage: command.ErrUnsupportedTargetSystem.Error(),
		},
		{
			name:            "internal error",
			err:             errTest,
			expectedStatus:  fiber.StatusInternalServerError,
			expectedCode:    500,
			expectedTitle:   "internal_server_error",
			expectedMessage: "an unexpected error occurred",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := executeErrorHandler(t, handleDispatchError, tt.err)
			defer resp.Body.Close()

			requireErrorResponse(
				t,
				resp,
				tt.expectedStatus,
				tt.expectedCode,
				tt.expectedTitle,
				tt.expectedMessage,
			)
		})
	}
}

func TestHandlersStruct_Fields(t *testing.T) {
	t.Parallel()

	exceptionUC := &command.UseCase{}
	disputeUC := &command.DisputeUseCase{}
	queryUC := &query.UseCase{}
	dispatchUC := &command.DispatchUseCase{}
	exceptionProvider := &stubExceptionProvider{exists: true}
	disputeProvider := &stubDisputeProvider{exists: true}

	commentUC := &command.CommentUseCase{}
	commentQueryUC := &query.CommentQueryUseCase{}
	callbackUC := &command.CallbackUseCase{}

	handlers, err := NewHandlers(
		exceptionUC,
		disputeUC,
		queryUC,
		dispatchUC,
		commentUC,
		commentQueryUC,
		callbackUC,
		exceptionProvider,
		disputeProvider,
	)

	require.NoError(t, err)
	assert.Equal(t, exceptionUC, handlers.exceptionUC)
	assert.Equal(t, disputeUC, handlers.disputeUC)
	assert.Equal(t, queryUC, handlers.queryUC)
	assert.Equal(t, dispatchUC, handlers.dispatchUC)
	assert.Equal(t, commentUC, handlers.commentUC)
	assert.Equal(t, commentQueryUC, handlers.commentQueryUC)
	assert.Equal(t, callbackUC, handlers.callbackUC)
}

func TestValidationOrder(t *testing.T) {
	t.Parallel()

	defaultExceptionProvider := &stubExceptionProvider{exists: true}
	defaultDisputeProvider := &stubDisputeProvider{exists: true}

	// Test that validation occurs in expected order
	tests := []struct {
		name              string
		exceptionUC       *command.UseCase
		disputeUC         *command.DisputeUseCase
		queryUC           *query.UseCase
		dispatchUC        *command.DispatchUseCase
		commentUC         *command.CommentUseCase
		commentQueryUC    *query.CommentQueryUseCase
		callbackUC        *command.CallbackUseCase
		exceptionProvider *stubExceptionProvider
		disputeProvider   *stubDisputeProvider
		expectedErr       error
	}{
		{
			name:              "exception use case checked first",
			exceptionUC:       nil,
			disputeUC:         nil,
			queryUC:           nil,
			dispatchUC:        nil,
			commentUC:         nil,
			commentQueryUC:    nil,
			callbackUC:        nil,
			exceptionProvider: defaultExceptionProvider,
			disputeProvider:   defaultDisputeProvider,
			expectedErr:       ErrNilExceptionUseCase,
		},
		{
			name:              "dispute use case checked second",
			exceptionUC:       &command.UseCase{},
			disputeUC:         nil,
			queryUC:           nil,
			dispatchUC:        nil,
			commentUC:         nil,
			commentQueryUC:    nil,
			callbackUC:        nil,
			exceptionProvider: defaultExceptionProvider,
			disputeProvider:   defaultDisputeProvider,
			expectedErr:       ErrNilDisputeUseCase,
		},
		{
			name:              "query use case checked third",
			exceptionUC:       &command.UseCase{},
			disputeUC:         &command.DisputeUseCase{},
			queryUC:           nil,
			dispatchUC:        nil,
			commentUC:         nil,
			commentQueryUC:    nil,
			callbackUC:        nil,
			exceptionProvider: defaultExceptionProvider,
			disputeProvider:   defaultDisputeProvider,
			expectedErr:       ErrNilQueryUseCase,
		},
		{
			name:              "dispatch use case checked fourth",
			exceptionUC:       &command.UseCase{},
			disputeUC:         &command.DisputeUseCase{},
			queryUC:           &query.UseCase{},
			dispatchUC:        nil,
			commentUC:         nil,
			commentQueryUC:    nil,
			callbackUC:        nil,
			exceptionProvider: defaultExceptionProvider,
			disputeProvider:   defaultDisputeProvider,
			expectedErr:       ErrNilDispatchUseCase,
		},
		{
			name:              "comment use case checked fifth",
			exceptionUC:       &command.UseCase{},
			disputeUC:         &command.DisputeUseCase{},
			queryUC:           &query.UseCase{},
			dispatchUC:        &command.DispatchUseCase{},
			commentUC:         nil,
			commentQueryUC:    nil,
			callbackUC:        nil,
			exceptionProvider: defaultExceptionProvider,
			disputeProvider:   defaultDisputeProvider,
			expectedErr:       ErrNilCommentUseCase,
		},
		{
			name:              "comment query use case checked sixth",
			exceptionUC:       &command.UseCase{},
			disputeUC:         &command.DisputeUseCase{},
			queryUC:           &query.UseCase{},
			dispatchUC:        &command.DispatchUseCase{},
			commentUC:         &command.CommentUseCase{},
			commentQueryUC:    nil,
			callbackUC:        nil,
			exceptionProvider: defaultExceptionProvider,
			disputeProvider:   defaultDisputeProvider,
			expectedErr:       ErrNilCommentQueryUseCase,
		},
		{
			name:              "callback use case checked seventh",
			exceptionUC:       &command.UseCase{},
			disputeUC:         &command.DisputeUseCase{},
			queryUC:           &query.UseCase{},
			dispatchUC:        &command.DispatchUseCase{},
			commentUC:         &command.CommentUseCase{},
			commentQueryUC:    &query.CommentQueryUseCase{},
			callbackUC:        nil,
			exceptionProvider: defaultExceptionProvider,
			disputeProvider:   defaultDisputeProvider,
			expectedErr:       ErrNilCallbackUseCase,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewHandlers(
				tt.exceptionUC,
				tt.disputeUC,
				tt.queryUC,
				tt.dispatchUC,
				tt.commentUC,
				tt.commentQueryUC,
				tt.callbackUC,
				tt.exceptionProvider,
				tt.disputeProvider,
			)

			require.Error(t, err)
			require.ErrorIs(t, err, tt.expectedErr)
		})
	}
}

func TestGetException_InvalidUUID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/exceptions/:exceptionId", handlers.GetException)

	request := httptest.NewRequest(http.MethodGet, "/v1/exceptions/not-a-uuid", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid exception_id",
	)
}

func TestGetException_ValidUUIDNotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/exceptions/:exceptionId", handlers.GetException)

	request := httptest.NewRequest(http.MethodGet, "/v1/exceptions/"+uuid.NewString(), http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	requireErrorResponse(t, resp, fiber.StatusNotFound, 404, "not_found", "exception not found")
}

func TestGetException_ContextCanceled(t *testing.T) {
	t.Parallel()

	baseCtx := context.Background()
	ctx, cancel := context.WithCancel(baseCtx)
	cancel()

	repo := &stubExceptionRepo{returnCtxErr: true}
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, repo)
	app.Get("/v1/exceptions/:exceptionId", handlers.GetException)

	request := httptest.NewRequest(http.MethodGet, "/v1/exceptions/"+uuid.NewString(), http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.ErrorIs(t, repo.seenCtxErr, context.Canceled)
	requireErrorResponse(
		t,
		resp,
		fiber.StatusInternalServerError,
		500,
		"internal_server_error",
		"an unexpected error occurred",
	)
}

func TestHandleExceptionVerificationError_AllCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		err             error
		expectedStatus  int
		expectedCode    int
		expectedTitle   string
		expectedMessage string
	}{
		{
			name:            "missing exception ID returns bad request",
			err:             ErrMissingExceptionID,
			expectedStatus:  fiber.StatusBadRequest,
			expectedCode:    400,
			expectedTitle:   "invalid_request",
			expectedMessage: "invalid exception_id",
		},
		{
			name:            "invalid exception ID returns bad request",
			err:             ErrInvalidExceptionID,
			expectedStatus:  fiber.StatusBadRequest,
			expectedCode:    400,
			expectedTitle:   "invalid_request",
			expectedMessage: "invalid exception_id",
		},
		{
			name:            "tenant ID not found returns unauthorized",
			err:             libHTTP.ErrTenantIDNotFound,
			expectedStatus:  fiber.StatusUnauthorized,
			expectedCode:    401,
			expectedTitle:   "unauthorized",
			expectedMessage: "unauthorized",
		},
		{
			name:            "invalid tenant ID returns unauthorized",
			err:             libHTTP.ErrInvalidTenantID,
			expectedStatus:  fiber.StatusUnauthorized,
			expectedCode:    401,
			expectedTitle:   "unauthorized",
			expectedMessage: "unauthorized",
		},
		{
			name:            "exception not found returns 404",
			err:             ErrExceptionNotFound,
			expectedStatus:  fiber.StatusNotFound,
			expectedCode:    404,
			expectedTitle:   "not_found",
			expectedMessage: "exception not found",
		},
		{
			name:            "lookup failed returns internal server error",
			err:             libHTTP.ErrLookupFailed,
			expectedStatus:  fiber.StatusInternalServerError,
			expectedCode:    500,
			expectedTitle:   "internal_server_error",
			expectedMessage: "an unexpected error occurred",
		},
		{
			name:            "other errors return forbidden",
			err:             ErrExceptionAccessDenied,
			expectedStatus:  fiber.StatusForbidden,
			expectedCode:    403,
			expectedTitle:   "forbidden",
			expectedMessage: "access denied",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := executeErrorHandler(t, handleExceptionVerificationError, tt.err)
			defer resp.Body.Close()

			requireErrorResponse(
				t,
				resp,
				tt.expectedStatus,
				tt.expectedCode,
				tt.expectedTitle,
				tt.expectedMessage,
			)
		})
	}
}

func TestHandleDisputeVerificationError_AllCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		err             error
		expectedStatus  int
		expectedCode    int
		expectedTitle   string
		expectedMessage string
	}{
		{
			name:            "missing dispute ID returns bad request",
			err:             ErrMissingDisputeID,
			expectedStatus:  fiber.StatusBadRequest,
			expectedCode:    400,
			expectedTitle:   "invalid_request",
			expectedMessage: "invalid dispute_id",
		},
		{
			name:            "invalid dispute ID returns bad request",
			err:             ErrInvalidDisputeID,
			expectedStatus:  fiber.StatusBadRequest,
			expectedCode:    400,
			expectedTitle:   "invalid_request",
			expectedMessage: "invalid dispute_id",
		},
		{
			name:            "tenant ID not found returns unauthorized",
			err:             libHTTP.ErrTenantIDNotFound,
			expectedStatus:  fiber.StatusUnauthorized,
			expectedCode:    401,
			expectedTitle:   "unauthorized",
			expectedMessage: "unauthorized",
		},
		{
			name:            "invalid tenant ID returns unauthorized",
			err:             libHTTP.ErrInvalidTenantID,
			expectedStatus:  fiber.StatusUnauthorized,
			expectedCode:    401,
			expectedTitle:   "unauthorized",
			expectedMessage: "unauthorized",
		},
		{
			name:            "dispute not found returns 404",
			err:             ErrDisputeNotFound,
			expectedStatus:  fiber.StatusNotFound,
			expectedCode:    404,
			expectedTitle:   "not_found",
			expectedMessage: "dispute not found",
		},
		{
			name:            "lookup failed returns internal server error",
			err:             libHTTP.ErrLookupFailed,
			expectedStatus:  fiber.StatusInternalServerError,
			expectedCode:    500,
			expectedTitle:   "internal_server_error",
			expectedMessage: "an unexpected error occurred",
		},
		{
			name:            "other errors return forbidden",
			err:             ErrDisputeAccessDenied,
			expectedStatus:  fiber.StatusForbidden,
			expectedCode:    403,
			expectedTitle:   "forbidden",
			expectedMessage: "access denied",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := executeErrorHandler(t, handleDisputeVerificationError, tt.err)
			defer resp.Body.Close()

			requireErrorResponse(
				t,
				resp,
				tt.expectedStatus,
				tt.expectedCode,
				tt.expectedTitle,
				tt.expectedMessage,
			)
		})
	}
}

func TestHandleExceptionError_AllMappings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		err             error
		expectedStatus  int
		expectedCode    int
		expectedTitle   string
		expectedMessage string
	}{
		{
			name:            "sql.ErrNoRows returns not found",
			err:             sql.ErrNoRows,
			expectedStatus:  fiber.StatusNotFound,
			expectedCode:    404,
			expectedTitle:   "not_found",
			expectedMessage: "exception not found",
		},
		{
			name:            "exception ID required returns bad request",
			err:             command.ErrExceptionIDRequired,
			expectedStatus:  fiber.StatusBadRequest,
			expectedCode:    400,
			expectedTitle:   "invalid_request",
			expectedMessage: command.ErrExceptionIDRequired.Error(),
		},
		{
			name:            "actor required returns bad request",
			err:             command.ErrActorRequired,
			expectedStatus:  fiber.StatusBadRequest,
			expectedCode:    400,
			expectedTitle:   "invalid_request",
			expectedMessage: command.ErrActorRequired.Error(),
		},
		{
			name:            "zero adjustment amount returns bad request",
			err:             command.ErrZeroAdjustmentAmount,
			expectedStatus:  fiber.StatusBadRequest,
			expectedCode:    400,
			expectedTitle:   "invalid_request",
			expectedMessage: command.ErrZeroAdjustmentAmount.Error(),
		},
		{
			name:            "invalid currency returns bad request",
			err:             command.ErrInvalidCurrency,
			expectedStatus:  fiber.StatusBadRequest,
			expectedCode:    400,
			expectedTitle:   "invalid_request",
			expectedMessage: command.ErrInvalidCurrency.Error(),
		},
		{
			name:            "resolution notes required returns bad request",
			err:             entities.ErrResolutionNotesRequired,
			expectedStatus:  fiber.StatusBadRequest,
			expectedCode:    400,
			expectedTitle:   "invalid_request",
			expectedMessage: entities.ErrResolutionNotesRequired.Error(),
		},
		{
			name:            "exception must be open or assigned returns unprocessable",
			err:             entities.ErrExceptionMustBeOpenOrAssignedToResolve,
			expectedStatus:  fiber.StatusUnprocessableEntity,
			expectedCode:    422,
			expectedTitle:   "unprocessable_entity",
			expectedMessage: "exception cannot be resolved in current state",
		},
		{
			name:            "unknown error returns internal server error",
			err:             errTest,
			expectedStatus:  fiber.StatusInternalServerError,
			expectedCode:    500,
			expectedTitle:   "internal_server_error",
			expectedMessage: "an unexpected error occurred",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := executeErrorHandler(t, handleExceptionError, tt.err)
			defer resp.Body.Close()

			requireErrorResponse(
				t,
				resp,
				tt.expectedStatus,
				tt.expectedCode,
				tt.expectedTitle,
				tt.expectedMessage,
			)
		})
	}
}

func TestForbiddenWithNilError(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")
	ctx := libCommons.ContextWithTracer(context.Background(), tracer)

	app := newFiberTestApp(ctx)
	app.Get("/", func(c *fiber.Ctx) error {
		spanCtx, span := tracer.Start(c.UserContext(), "test")
		c.SetUserContext(spanCtx)

		defer span.End()

		return forbidden(spanCtx, c, span, &libLog.NopLogger{}, nil)
	})

	request := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	requireErrorResponse(t, resp, fiber.StatusForbidden, 403, "forbidden", "access denied")
}

func TestLogSpanError_WithNilLogger(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")
	_, span := tracer.Start(context.Background(), "test")

	defer span.End()

	require.NotPanics(t, func() {
		logSpanError(context.Background(), span, nil, "test message", errTest)
	})
}

func TestLogSpanError_WithLogger_LogsErrorMessage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tracer := noop.NewTracerProvider().Tracer("test")
	_, span := tracer.Start(ctx, "test")

	defer span.End()

	spyLogger := &testutil.TestLogger{}

	logSpanError(ctx, span, spyLogger, "something went wrong", errTest)

	require.True(t, spyLogger.ErrorCalled, "expected Log to be called at LevelError")
	require.Len(t, spyLogger.Messages, 1)
	assert.Equal(t, "something went wrong", spyLogger.Messages[0])
}

func TestLogSpanError_WithLogger_NilError_DoesNotLog(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tracer := noop.NewTracerProvider().Tracer("test")
	_, span := tracer.Start(ctx, "test")

	defer span.End()

	spyLogger := &testutil.TestLogger{}

	logSpanError(ctx, span, spyLogger, "should not appear", nil)

	assert.False(t, spyLogger.ErrorCalled, "expected Log NOT to be called when err is nil")
	assert.Empty(t, spyLogger.Messages)
}

func TestStartHandlerSpan_WithNilTracer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)

	var resultCtx context.Context

	var resultSpan trace.Span

	app.Get("/", func(c *fiber.Ctx) error {
		resultCtx, resultSpan, _ = startHandlerSpan(c, "test.handler")
		defer resultSpan.End()

		return c.SendStatus(fiber.StatusOK)
	})

	request := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.NotNil(t, resultCtx)
	require.NotNil(t, resultSpan)
}

func TestParseCursorFilter_AllParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		queryString   string
		expectedLimit int
		expectError   bool
		expectedErr   error
	}{
		{
			name:          "default limit when no limit provided",
			queryString:   "",
			expectedLimit: 20,
			expectError:   false,
		},
		{
			name:          "valid limit",
			queryString:   "limit=50",
			expectedLimit: 50,
			expectError:   false,
		},
		{
			name:          "limit capped at max",
			queryString:   "limit=500",
			expectedLimit: 200,
			expectError:   false,
		},
		{
			// In lib-uncommons v2, ParseOpaqueCursorPagination silently clamps
			// limit <= 0 to DefaultLimit instead of returning an error.
			name:          "zero limit clamped to default",
			queryString:   "limit=0",
			expectedLimit: 20,
			expectError:   false,
		},
		{
			// In lib-uncommons v2, ParseOpaqueCursorPagination silently clamps
			// limit <= 0 to DefaultLimit instead of returning an error.
			name:          "negative limit clamped to default",
			queryString:   "limit=-5",
			expectedLimit: 20,
			expectError:   false,
		},
		{
			name:        "invalid limit returns error",
			queryString: "limit=abc",
			expectError: true,
		},
		{
			name:          "invalid sort_by falls back to default",
			queryString:   "sort_by=invalid_column",
			expectedLimit: 20,
			expectError:   false,
		},
		{
			name:        "invalid sort_order returns error",
			queryString: "sort_order=sideways",
			expectError: true,
			expectedErr: ErrInvalidSortOrder,
		},
		{
			name:          "valid sort_by id",
			queryString:   "sort_by=id",
			expectedLimit: 20,
			expectError:   false,
		},
		{
			name:          "valid sort_by created_at",
			queryString:   "sort_by=created_at",
			expectedLimit: 20,
			expectError:   false,
		},
		{
			name:          "valid sort_by updated_at",
			queryString:   "sort_by=updated_at",
			expectedLimit: 20,
			expectError:   false,
		},
		{
			name:          "valid sort_by severity",
			queryString:   "sort_by=severity",
			expectedLimit: 20,
			expectError:   false,
		},
		{
			name:          "valid sort_by status",
			queryString:   "sort_by=status",
			expectedLimit: 20,
			expectError:   false,
		},
		{
			name:          "valid sort_order asc",
			queryString:   "sort_order=asc",
			expectedLimit: 20,
			expectError:   false,
		},
		{
			name:          "valid sort_order desc",
			queryString:   "sort_order=desc",
			expectedLimit: 20,
			expectError:   false,
		},
		{
			name:          "valid cursor parameter",
			queryString:   "cursor=abc123",
			expectedLimit: 20,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			app := fiber.New()
			defer func() { _ = app.Shutdown() }()

			var filter exceptionRepositories.CursorFilter

			var parseErr error

			app.Get("/test", func(c *fiber.Ctx) error {
				filter, parseErr = parseCursorFilter(c)
				return c.SendStatus(fiber.StatusOK)
			})

			request := httptest.NewRequest(http.MethodGet, "/test?"+tt.queryString, http.NoBody)
			resp, err := app.Test(request)
			require.NoError(t, err)
			defer resp.Body.Close()

			if tt.expectError {
				require.Error(t, parseErr)

				if tt.expectedErr != nil {
					require.ErrorIs(t, parseErr, tt.expectedErr)
				}
			} else {
				require.NoError(t, parseErr)
				assert.Equal(t, tt.expectedLimit, filter.Limit)
			}
		})
	}
}

func TestParseExceptionFilter_AllParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		queryString string
		expectError bool
		validate    func(t *testing.T, filter exceptionRepositories.ExceptionFilter)
	}{
		{
			name:        "empty query returns empty filter",
			queryString: "",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.ExceptionFilter) {
				assert.Nil(t, filter.Status)
				assert.Nil(t, filter.Severity)
				assert.Nil(t, filter.AssignedTo)
				assert.Nil(t, filter.ExternalSystem)
				assert.Nil(t, filter.DateFrom)
				assert.Nil(t, filter.DateTo)
			},
		},
		{
			name:        "valid status OPEN",
			queryString: "status=OPEN",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.ExceptionFilter) {
				require.NotNil(t, filter.Status)
				assert.Equal(t, "OPEN", string(*filter.Status))
			},
		},
		{
			name:        "valid status ASSIGNED",
			queryString: "status=ASSIGNED",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.ExceptionFilter) {
				require.NotNil(t, filter.Status)
				assert.Equal(t, "ASSIGNED", string(*filter.Status))
			},
		},
		{
			name:        "valid status RESOLVED",
			queryString: "status=RESOLVED",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.ExceptionFilter) {
				require.NotNil(t, filter.Status)
				assert.Equal(t, "RESOLVED", string(*filter.Status))
			},
		},
		{
			name:        "invalid status returns error",
			queryString: "status=INVALID",
			expectError: true,
		},
		{
			name:        "valid severity LOW",
			queryString: "severity=LOW",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.ExceptionFilter) {
				require.NotNil(t, filter.Severity)
				assert.Equal(t, "LOW", string(*filter.Severity))
			},
		},
		{
			name:        "valid severity MEDIUM",
			queryString: "severity=MEDIUM",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.ExceptionFilter) {
				require.NotNil(t, filter.Severity)
				assert.Equal(t, "MEDIUM", string(*filter.Severity))
			},
		},
		{
			name:        "valid severity HIGH",
			queryString: "severity=HIGH",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.ExceptionFilter) {
				require.NotNil(t, filter.Severity)
				assert.Equal(t, "HIGH", string(*filter.Severity))
			},
		},
		{
			name:        "valid severity CRITICAL",
			queryString: "severity=CRITICAL",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.ExceptionFilter) {
				require.NotNil(t, filter.Severity)
				assert.Equal(t, "CRITICAL", string(*filter.Severity))
			},
		},
		{
			name:        "invalid severity returns error",
			queryString: "severity=VERY_BAD",
			expectError: true,
		},
		{
			name:        "valid assigned_to",
			queryString: "assigned_to=user@example.com",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.ExceptionFilter) {
				require.NotNil(t, filter.AssignedTo)
				assert.Equal(t, "user@example.com", *filter.AssignedTo)
			},
		},
		{
			name:        "valid external_system",
			queryString: "external_system=JIRA",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.ExceptionFilter) {
				require.NotNil(t, filter.ExternalSystem)
				assert.Equal(t, "JIRA", *filter.ExternalSystem)
			},
		},
		{
			name:        "valid date_from",
			queryString: "date_from=2024-01-01T00:00:00Z",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.ExceptionFilter) {
				require.NotNil(t, filter.DateFrom)
				assert.Equal(t, 2024, filter.DateFrom.Year())
				assert.Equal(t, 1, int(filter.DateFrom.Month()))
				assert.Equal(t, 1, filter.DateFrom.Day())
			},
		},
		{
			name:        "invalid date_from returns error",
			queryString: "date_from=not-a-date",
			expectError: true,
		},
		{
			name:        "valid date_to",
			queryString: "date_to=2024-12-31T23:59:59Z",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.ExceptionFilter) {
				require.NotNil(t, filter.DateTo)
				assert.Equal(t, 2024, filter.DateTo.Year())
				assert.Equal(t, 12, int(filter.DateTo.Month()))
				assert.Equal(t, 31, filter.DateTo.Day())
			},
		},
		{
			name:        "invalid date_to returns error",
			queryString: "date_to=invalid",
			expectError: true,
		},
		{
			name:        "multiple valid parameters",
			queryString: "status=OPEN&severity=HIGH&assigned_to=analyst@test.com",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.ExceptionFilter) {
				require.NotNil(t, filter.Status)
				assert.Equal(t, "OPEN", string(*filter.Status))
				require.NotNil(t, filter.Severity)
				assert.Equal(t, "HIGH", string(*filter.Severity))
				require.NotNil(t, filter.AssignedTo)
				assert.Equal(t, "analyst@test.com", *filter.AssignedTo)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			app := fiber.New()
			defer func() { _ = app.Shutdown() }()

			var filter exceptionRepositories.ExceptionFilter

			var parseErr error

			app.Get("/test", func(c *fiber.Ctx) error {
				filter, parseErr = parseExceptionFilter(c)
				return c.SendStatus(fiber.StatusOK)
			})

			request := httptest.NewRequest(http.MethodGet, "/test?"+tt.queryString, http.NoBody)
			resp, err := app.Test(request)
			require.NoError(t, err)
			defer resp.Body.Close()

			if tt.expectError {
				require.Error(t, parseErr)
			} else {
				require.NoError(t, parseErr)

				if tt.validate != nil {
					tt.validate(t, filter)
				}
			}
		})
	}
}

func TestParseListFilters_CombinedErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		queryString string
		expectError bool
	}{
		{
			name:        "all valid parameters",
			queryString: "status=OPEN&limit=50&sort_by=created_at&sort_order=desc",
			expectError: false,
		},
		{
			name:        "exception filter error propagates",
			queryString: "status=BAD_STATUS",
			expectError: true,
		},
		{
			name:        "cursor filter error propagates",
			queryString: "sort_order=invalid",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			app := fiber.New()
			defer func() { _ = app.Shutdown() }()

			var parseErr error

			app.Get("/test", func(c *fiber.Ctx) error {
				_, _, parseErr = parseListFilters(c)
				return c.SendStatus(fiber.StatusOK)
			})

			request := httptest.NewRequest(http.MethodGet, "/test?"+tt.queryString, http.NoBody)
			resp, err := app.Test(request)
			require.NoError(t, err)
			defer resp.Body.Close()

			if tt.expectError {
				require.Error(t, parseErr)
			} else {
				require.NoError(t, parseErr)
			}
		})
	}
}

func TestAllowedSortColumns(t *testing.T) {
	t.Parallel()

	expected := []string{"id", "created_at", "updated_at", "severity", "status"}

	for _, col := range expected {
		assert.Contains(t, allowedSortColumns, col, "column %s should be allowed", col)
	}

	assert.NotContains(t, allowedSortColumns, "invalid")
	assert.NotContains(t, allowedSortColumns, "")
	assert.NotContains(t, allowedSortColumns, "tenant_id")
}

func TestAllowedSortOrders(t *testing.T) {
	t.Parallel()

	assert.True(t, allowedSortOrders["asc"])
	assert.True(t, allowedSortOrders["desc"])

	assert.False(t, allowedSortOrders["ASC"])
	assert.False(t, allowedSortOrders["DESC"])
	assert.False(t, allowedSortOrders[""])
	assert.False(t, allowedSortOrders["random"])
}

func TestBadRequest_Response(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")
	ctx := libCommons.ContextWithTracer(context.Background(), tracer)

	app := newFiberTestApp(ctx)
	app.Get("/", func(c *fiber.Ctx) error {
		spanCtx, span := tracer.Start(c.UserContext(), "test")
		c.SetUserContext(spanCtx)

		defer span.End()

		return badRequest(spanCtx, c, span, &libLog.NopLogger{}, "test message", errTest)
	})

	request := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	requireErrorResponse(t, resp, fiber.StatusBadRequest, 400, "invalid_request", "test message")
}

func TestNotFound_Response(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")
	ctx := libCommons.ContextWithTracer(context.Background(), tracer)

	app := newFiberTestApp(ctx)
	app.Get("/", func(c *fiber.Ctx) error {
		spanCtx, span := tracer.Start(c.UserContext(), "test")
		c.SetUserContext(spanCtx)

		defer span.End()

		return notFound(spanCtx, c, span, &libLog.NopLogger{}, "resource not found", errTest)
	})

	request := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	requireErrorResponse(t, resp, fiber.StatusNotFound, 404, "not_found", "resource not found")
}

func TestUnprocessable_Response(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")
	ctx := libCommons.ContextWithTracer(context.Background(), tracer)

	app := newFiberTestApp(ctx)
	app.Get("/", func(c *fiber.Ctx) error {
		spanCtx, span := tracer.Start(c.UserContext(), "test")
		c.SetUserContext(spanCtx)

		defer span.End()

		return unprocessable(spanCtx, c, span, &libLog.NopLogger{}, "cannot process", errTest)
	})

	request := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusUnprocessableEntity,
		422,
		"unprocessable_entity",
		"cannot process",
	)
}

func TestInternalError_Response(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")
	ctx := libCommons.ContextWithTracer(context.Background(), tracer)

	app := newFiberTestApp(ctx)
	app.Get("/", func(c *fiber.Ctx) error {
		spanCtx, span := tracer.Start(c.UserContext(), "test")
		c.SetUserContext(spanCtx)

		defer span.End()

		return internalError(spanCtx, c, span, &libLog.NopLogger{}, "something went wrong", errTest)
	})

	request := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusInternalServerError,
		500,
		"internal_server_error",
		"an unexpected error occurred",
	)
}

func TestForbidden_WithProvidedError(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")
	ctx := libCommons.ContextWithTracer(context.Background(), tracer)

	app := newFiberTestApp(ctx)
	app.Get("/", func(c *fiber.Ctx) error {
		spanCtx, span := tracer.Start(c.UserContext(), "test")
		c.SetUserContext(spanCtx)

		defer span.End()

		return forbidden(spanCtx, c, span, &libLog.NopLogger{}, errTest)
	})

	request := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	requireErrorResponse(t, resp, fiber.StatusForbidden, 403, "forbidden", "access denied")
}

func TestHandleExceptionError_InvalidCurrencyCode(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, handleExceptionError, value_objects.ErrInvalidCurrencyCode)
	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		value_objects.ErrInvalidCurrencyCode.Error(),
	)
}

func TestHandleExceptionError_InvalidAdjustmentReason(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, handleExceptionError, value_objects.ErrInvalidAdjustmentReason)
	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		value_objects.ErrInvalidAdjustmentReason.Error(),
	)
}

func TestHandleExceptionError_InvalidResolutionTransition(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(
		t,
		handleExceptionError,
		value_objects.ErrInvalidResolutionTransition,
	)
	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusUnprocessableEntity,
		422,
		"unprocessable_entity",
		"exception cannot be resolved in current state",
	)
}

func TestHandleDisputeError_CategoryRequired(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, handleDisputeError, command.ErrDisputeCategoryRequired)
	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		command.ErrDisputeCategoryRequired.Error(),
	)
}

func TestHandleDisputeError_DescriptionRequired(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, handleDisputeError, command.ErrDisputeDescriptionRequired)
	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		command.ErrDisputeDescriptionRequired.Error(),
	)
}

func TestHandleDisputeError_CommentRequired(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, handleDisputeError, command.ErrDisputeCommentRequired)
	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		command.ErrDisputeCommentRequired.Error(),
	)
}

func TestHandleDisputeError_ResolutionRequired(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, handleDisputeError, command.ErrDisputeResolutionRequired)
	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		command.ErrDisputeResolutionRequired.Error(),
	)
}

func TestHandleDisputeError_InvalidDisputeTransition(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, handleDisputeError, dispute.ErrInvalidDisputeTransition)
	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusUnprocessableEntity,
		422,
		"unprocessable_entity",
		dispute.ErrInvalidDisputeTransition.Error(),
	)
}

func TestHandleDispatchError_ExceptionIDRequired(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, handleDispatchError, command.ErrExceptionIDRequired)
	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		command.ErrExceptionIDRequired.Error(),
	)
}

func TestHandleDispatchError_ActorRequired(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, handleDispatchError, command.ErrActorRequired)
	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		command.ErrActorRequired.Error(),
	)
}

func TestValidationOrder_ProviderChecks(t *testing.T) {
	t.Parallel()

	t.Run("exception provider checked after comment query use case", func(t *testing.T) {
		t.Parallel()

		var nilExceptionProvider exceptionProvider = nil

		disputeProvider := &stubDisputeProvider{exists: true}

		_, err := NewHandlers(
			&command.UseCase{},
			&command.DisputeUseCase{},
			&query.UseCase{},
			&command.DispatchUseCase{},
			&command.CommentUseCase{},
			&query.CommentQueryUseCase{},
			&command.CallbackUseCase{},
			nilExceptionProvider,
			disputeProvider,
		)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrNilExceptionProvider)
	})

	t.Run("dispute provider checked last", func(t *testing.T) {
		t.Parallel()

		exceptionProvider := &stubExceptionProvider{exists: true}

		var nilDisputeProvider disputeProvider = nil

		_, err := NewHandlers(
			&command.UseCase{},
			&command.DisputeUseCase{},
			&query.UseCase{},
			&command.DispatchUseCase{},
			&command.CommentUseCase{},
			&query.CommentQueryUseCase{},
			&command.CallbackUseCase{},
			exceptionProvider,
			nilDisputeProvider,
		)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrNilDisputeProvider)
	})
}

func TestGetException_ExceptionNotFoundError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{err: entities.ErrExceptionNotFound})
	app.Get("/v1/exceptions/:exceptionId", handlers.GetException)

	request := httptest.NewRequest(http.MethodGet, "/v1/exceptions/"+uuid.NewString(), http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(t, resp, fiber.StatusNotFound, 404, "not_found", "exception not found")
}

func TestGetException_InternalError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{err: errTest})
	app.Get("/v1/exceptions/:exceptionId", handlers.GetException)

	request := httptest.NewRequest(http.MethodGet, "/v1/exceptions/"+uuid.NewString(), http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusInternalServerError,
		500,
		"internal_server_error",
		"an unexpected error occurred",
	)
}

func TestForceMatch_InvalidExceptionID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/exceptions/:exceptionId/force-match", handlers.ForceMatch)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/exceptions/not-a-uuid/force-match",
		http.NoBody,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid exception_id",
	)
}

func TestAdjustEntry_InvalidExceptionID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/exceptions/:exceptionId/adjust-entry", handlers.AdjustEntry)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/exceptions/invalid-uuid/adjust-entry",
		http.NoBody,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid exception_id",
	)
}

func TestOpenDispute_InvalidExceptionID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/exceptions/:exceptionId/disputes", handlers.OpenDispute)

	request := httptest.NewRequest(http.MethodPost, "/v1/exceptions/bad-uuid/disputes", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid exception_id",
	)
}

func TestCloseDispute_InvalidDisputeID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/disputes/:disputeId/close", handlers.CloseDispute)

	request := httptest.NewRequest(http.MethodPost, "/v1/disputes/not-valid/close", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid dispute_id",
	)
}

func TestSubmitEvidence_InvalidDisputeID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/disputes/:disputeId/evidence", handlers.SubmitEvidence)

	request := httptest.NewRequest(http.MethodPost, "/v1/disputes/bad-id/evidence", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid dispute_id",
	)
}

func TestDispatchToExternal_InvalidExceptionID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/exceptions/:exceptionId/dispatch", handlers.DispatchToExternal)

	request := httptest.NewRequest(http.MethodPost, "/v1/exceptions/xyz/dispatch", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid exception_id",
	)
}

func TestGetHistory_InvalidExceptionID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/exceptions/:exceptionId/history", handlers.GetHistory)

	request := httptest.NewRequest(http.MethodGet, "/v1/exceptions/abc/history", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid exception_id",
	)
}

func TestListExceptions_InvalidStatus(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/exceptions", handlers.ListExceptions)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/exceptions?status=INVALID_STATUS",
		http.NoBody,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid filter parameters",
	)
}

func TestListExceptions_InvalidSeverity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/exceptions", handlers.ListExceptions)

	request := httptest.NewRequest(http.MethodGet, "/v1/exceptions?severity=VERY_BAD", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid filter parameters",
	)
}

func TestListExceptions_InvalidSortByFallsBackToDefault(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/exceptions", handlers.ListExceptions)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/exceptions?sort_by=invalid_field",
		http.NoBody,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Invalid sort_by silently falls back to default ("id") via libHTTP.ValidateSortColumn.
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestListExceptions_InvalidDateFrom(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/exceptions", handlers.ListExceptions)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/exceptions?date_from=not-a-date",
		http.NoBody,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid filter parameters",
	)
}

func TestGetHistory_InvalidLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/exceptions/:exceptionId/history", handlers.GetHistory)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/exceptions/"+uuid.NewString()+"/history?limit=abc",
		http.NoBody,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(t, resp, fiber.StatusBadRequest, 400, "invalid_request", "invalid pagination parameters")
}

func TestListExceptions_AssignedToExceedsMaxLength(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/exceptions", handlers.ListExceptions)

	longAssignedTo := strings.Repeat("a", 256)
	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/exceptions?assigned_to="+longAssignedTo,
		http.NoBody,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid filter parameters",
	)
}

func TestListExceptions_ExternalSystemExceedsMaxLength(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/exceptions", handlers.ListExceptions)

	longExtSystem := strings.Repeat("x", 51)
	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/exceptions?external_system="+longExtSystem,
		http.NoBody,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid filter parameters",
	)
}

func TestParseExceptionFilter_AssignedToExceedsMaxLength(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	var parseErr error

	app.Get("/test", func(c *fiber.Ctx) error {
		_, parseErr = parseExceptionFilter(c)
		return c.SendStatus(fiber.StatusOK)
	})

	longAssignedTo := strings.Repeat("a", 256)
	request := httptest.NewRequest(http.MethodGet, "/test?assigned_to="+longAssignedTo, http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Error(t, parseErr)
	assert.ErrorIs(t, parseErr, libHTTP.ErrQueryParamTooLong)
	assert.Contains(t, parseErr.Error(), "assigned_to")
}

func TestParseExceptionFilter_ExternalSystemExceedsMaxLength(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	var parseErr error

	app.Get("/test", func(c *fiber.Ctx) error {
		_, parseErr = parseExceptionFilter(c)
		return c.SendStatus(fiber.StatusOK)
	})

	longExtSystem := strings.Repeat("x", 51)
	request := httptest.NewRequest(http.MethodGet, "/test?external_system="+longExtSystem, http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Error(t, parseErr)
	assert.ErrorIs(t, parseErr, libHTTP.ErrQueryParamTooLong)
	assert.Contains(t, parseErr.Error(), "external_system")
}

func TestParseExceptionFilter_AssignedToAtExactLimit(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	var parseErr error

	var result exceptionRepositories.ExceptionFilter

	app.Get("/test", func(c *fiber.Ctx) error {
		result, parseErr = parseExceptionFilter(c)
		return c.SendStatus(fiber.StatusOK)
	})

	exactAssignedTo := strings.Repeat("a", 255)
	request := httptest.NewRequest(http.MethodGet, "/test?assigned_to="+exactAssignedTo, http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.NoError(t, parseErr)
	require.NotNil(t, result.AssignedTo)
	assert.Equal(t, exactAssignedTo, *result.AssignedTo)
}

func TestParseExceptionFilter_ExternalSystemAtExactLimit(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	var parseErr error

	var result exceptionRepositories.ExceptionFilter

	app.Get("/test", func(c *fiber.Ctx) error {
		result, parseErr = parseExceptionFilter(c)
		return c.SendStatus(fiber.StatusOK)
	})

	exactExtSystem := strings.Repeat("x", 50)
	request := httptest.NewRequest(http.MethodGet, "/test?external_system="+exactExtSystem, http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.NoError(t, parseErr)
	require.NotNil(t, result.ExternalSystem)
	assert.Equal(t, exactExtSystem, *result.ExternalSystem)
}
