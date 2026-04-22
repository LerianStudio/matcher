//go:build unit

package http

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	exceptionRepositories "github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/services/command"
	"github.com/LerianStudio/matcher/internal/exception/services/query"
	governanceRepositories "github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	crossAdapters "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	govEntities "github.com/LerianStudio/matcher/internal/shared/domain"
)

// ---------------------------------------------------------------------------
// Additional stubs for extended coverage
// ---------------------------------------------------------------------------

// stubTenantExtractor implements query.TenantExtractor for testing.
type stubTenantExtractor struct {
	tenantID uuid.UUID
}

func (s *stubTenantExtractor) GetTenantID(_ context.Context) uuid.UUID {
	return s.tenantID
}

// configurableAuditRepo allows per-test audit repo responses.
type configurableAuditRepo struct {
	entries    []*govEntities.AuditLog
	nextCursor string
	err        error
}

func (r *configurableAuditRepo) Create(
	_ context.Context,
	_ *govEntities.AuditLog,
) (*govEntities.AuditLog, error) {
	return nil, nil
}

func (r *configurableAuditRepo) CreateWithTx(
	ctx context.Context,
	_ *sql.Tx,
	a *govEntities.AuditLog,
) (*govEntities.AuditLog, error) {
	return r.Create(ctx, a)
}

func (r *configurableAuditRepo) GetByID(_ context.Context, _ uuid.UUID) (*govEntities.AuditLog, error) {
	return nil, nil
}

func (r *configurableAuditRepo) ListByEntity(
	_ context.Context,
	_ string,
	_ uuid.UUID,
	_ *libHTTP.TimestampCursor,
	_ int,
) ([]*govEntities.AuditLog, string, error) {
	return r.entries, r.nextCursor, r.err
}

func (r *configurableAuditRepo) List(
	_ context.Context,
	_ govEntities.AuditLogFilter,
	_ *libHTTP.TimestampCursor,
	_ int,
) ([]*govEntities.AuditLog, string, error) {
	return r.entries, r.nextCursor, r.err
}

// configurableDisputeRepo allows per-test dispute repo responses.
type configurableDisputeRepo struct {
	findByIDResult *dispute.Dispute
	findByIDErr    error
	listResult     []*dispute.Dispute
	listPagination libHTTP.CursorPagination
	listErr        error
}

func (r *configurableDisputeRepo) Create(_ context.Context, d *dispute.Dispute) (*dispute.Dispute, error) {
	return d, nil
}

func (r *configurableDisputeRepo) CreateWithTx(_ context.Context, _ exceptionRepositories.Tx, d *dispute.Dispute) (*dispute.Dispute, error) {
	return d, nil
}

func (r *configurableDisputeRepo) FindByID(_ context.Context, _ uuid.UUID) (*dispute.Dispute, error) {
	return r.findByIDResult, r.findByIDErr
}

func (r *configurableDisputeRepo) FindByExceptionID(_ context.Context, _ uuid.UUID) (*dispute.Dispute, error) {
	return nil, nil
}

func (r *configurableDisputeRepo) List(
	_ context.Context,
	_ exceptionRepositories.DisputeFilter,
	_ exceptionRepositories.CursorFilter,
) ([]*dispute.Dispute, libHTTP.CursorPagination, error) {
	return r.listResult, r.listPagination, r.listErr
}

func (r *configurableDisputeRepo) Update(_ context.Context, d *dispute.Dispute) (*dispute.Dispute, error) {
	return d, nil
}

func (r *configurableDisputeRepo) UpdateWithTx(_ context.Context, _ exceptionRepositories.Tx, d *dispute.Dispute) (*dispute.Dispute, error) {
	return d, nil
}

// ---------------------------------------------------------------------------
// Helper constructors
// ---------------------------------------------------------------------------

// newHandlersWithQueryOptions creates handlers with a configurable queryUC.
func newHandlersWithQueryOptions(
	t *testing.T,
	exRepo *stubExceptionRepo,
	dRepo exceptionRepositories.DisputeRepository,
	auditRepo governanceRepositories.AuditLogRepository,
	tenantExtractor query.TenantExtractor,
) *Handlers {
	t.Helper()

	queryUC, err := query.NewUseCase(exRepo, dRepo, auditRepo, tenantExtractor)
	require.NoError(t, err)

	exceptionProvider := &stubExceptionProvider{exists: true}
	disputeProvider := &stubDisputeProvider{exists: true}

	handlers, err := NewHandlers(&command.ExceptionUseCase{}, queryUC, &stubCommentRepo{}, exceptionProvider, disputeProvider, false)
	require.NoError(t, err)

	return handlers
}

// ---------------------------------------------------------------------------
// NewHandlers missing nil check
// ---------------------------------------------------------------------------

// TestNewHandlers_NilCommentUseCase is retained as a documentation marker
// that the previously separate CommentUseCase has been merged into the
// single ExceptionUseCase. NilCommentUseCase is no longer a valid
// constructor error — Handlers only accept the merged command use case.
func TestNewHandlers_NilCommentUseCase(t *testing.T) {
	t.Parallel()
	t.Skip("merged into single ExceptionUseCase; no separate comment UC argument")
}

// ---------------------------------------------------------------------------
// parseDisputeFilter tests
// ---------------------------------------------------------------------------

func TestParseDisputeFilter_AllParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		queryString string
		expectError bool
		validate    func(t *testing.T, filter exceptionRepositories.DisputeFilter)
	}{
		{
			name:        "empty query returns empty filter",
			queryString: "",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.DisputeFilter) {
				assert.Nil(t, filter.State)
				assert.Nil(t, filter.Category)
				assert.Nil(t, filter.DateFrom)
				assert.Nil(t, filter.DateTo)
			},
		},
		{
			name:        "valid state OPEN",
			queryString: "state=OPEN",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.DisputeFilter) {
				require.NotNil(t, filter.State)
				assert.Equal(t, "OPEN", string(*filter.State))
			},
		},
		{
			name:        "valid state WON",
			queryString: "state=WON",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.DisputeFilter) {
				require.NotNil(t, filter.State)
				assert.Equal(t, "WON", string(*filter.State))
			},
		},
		{
			name:        "valid state LOST",
			queryString: "state=LOST",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.DisputeFilter) {
				require.NotNil(t, filter.State)
				assert.Equal(t, "LOST", string(*filter.State))
			},
		},
		{
			name:        "invalid state returns error",
			queryString: "state=INVALID",
			expectError: true,
		},
		{
			name:        "valid category BANK_FEE_ERROR",
			queryString: "category=BANK_FEE_ERROR",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.DisputeFilter) {
				require.NotNil(t, filter.Category)
				assert.Equal(t, "BANK_FEE_ERROR", string(*filter.Category))
			},
		},
		{
			name:        "valid category OTHER",
			queryString: "category=OTHER",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.DisputeFilter) {
				require.NotNil(t, filter.Category)
				assert.Equal(t, "OTHER", string(*filter.Category))
			},
		},
		{
			name:        "invalid category returns error",
			queryString: "category=NONEXISTENT",
			expectError: true,
		},
		{
			name:        "valid date_from",
			queryString: "date_from=2024-06-01T00:00:00Z",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.DisputeFilter) {
				require.NotNil(t, filter.DateFrom)
				assert.Equal(t, 2024, filter.DateFrom.Year())
				assert.Equal(t, 6, int(filter.DateFrom.Month()))
			},
		},
		{
			name:        "invalid date_from returns error",
			queryString: "date_from=bad-date",
			expectError: true,
		},
		{
			name:        "valid date_to",
			queryString: "date_to=2024-12-31T23:59:59Z",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.DisputeFilter) {
				require.NotNil(t, filter.DateTo)
				assert.Equal(t, 2024, filter.DateTo.Year())
				assert.Equal(t, 12, int(filter.DateTo.Month()))
			},
		},
		{
			name:        "invalid date_to returns error",
			queryString: "date_to=invalid",
			expectError: true,
		},
		{
			name:        "multiple valid parameters",
			queryString: "state=OPEN&category=BANK_FEE_ERROR&date_from=2024-01-01T00:00:00Z",
			expectError: false,
			validate: func(t *testing.T, filter exceptionRepositories.DisputeFilter) {
				require.NotNil(t, filter.State)
				assert.Equal(t, "OPEN", string(*filter.State))
				require.NotNil(t, filter.Category)
				assert.Equal(t, "BANK_FEE_ERROR", string(*filter.Category))
				require.NotNil(t, filter.DateFrom)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			app := fiber.New()
			defer func() { _ = app.Shutdown() }()

			var filter exceptionRepositories.DisputeFilter

			var parseErr error

			app.Get("/test", func(c *fiber.Ctx) error {
				filter, parseErr = parseDisputeFilter(c)
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

func TestParseDisputeListFilters_CombinedErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		queryString string
		expectError bool
	}{
		{
			name:        "all valid parameters",
			queryString: "state=OPEN&limit=50&sort_by=created_at&sort_order=desc",
			expectError: false,
		},
		{
			name:        "dispute filter error propagates",
			queryString: "state=BAD_STATE",
			expectError: true,
		},
		{
			name:        "cursor filter error propagates",
			queryString: "sort_order=invalid",
			expectError: true,
		},
		{
			name:        "invalid category propagates",
			queryString: "category=NOPE",
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
				_, _, parseErr = parseDisputeListFilters(c)
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

// ---------------------------------------------------------------------------
// ListDisputes handler tests
// ---------------------------------------------------------------------------

func TestListDisputes_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/disputes", handlers.ListDisputes)

	request := httptest.NewRequest(http.MethodGet, "/v1/disputes", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestListDisputes_InvalidState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/disputes", handlers.ListDisputes)

	request := httptest.NewRequest(http.MethodGet, "/v1/disputes?state=INVALID", http.NoBody)
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

func TestListDisputes_InvalidCategory(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/disputes", handlers.ListDisputes)

	request := httptest.NewRequest(http.MethodGet, "/v1/disputes?category=WRONG", http.NoBody)
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

func TestListDisputes_InvalidDateFrom(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/disputes", handlers.ListDisputes)

	request := httptest.NewRequest(http.MethodGet, "/v1/disputes?date_from=bad", http.NoBody)
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

func TestListDisputes_InvalidDateTo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/disputes", handlers.ListDisputes)

	request := httptest.NewRequest(http.MethodGet, "/v1/disputes?date_to=bad", http.NoBody)
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

func TestListDisputes_InvalidSortByFallsBackToDefault(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/disputes", handlers.ListDisputes)

	request := httptest.NewRequest(http.MethodGet, "/v1/disputes?sort_by=badcol", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Invalid sort_by silently falls back to default ("id") via libHTTP.ValidateSortColumn.
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestListDisputes_UCInternalError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)

	disputeRepo := &configurableDisputeRepo{listErr: errTest}
	handlers := newHandlersWithQueryOptions(
		t, &stubExceptionRepo{}, disputeRepo, &stubAuditRepo{}, nil,
	)
	app.Get("/v1/disputes", handlers.ListDisputes)

	request := httptest.NewRequest(http.MethodGet, "/v1/disputes", http.NoBody)
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

// ---------------------------------------------------------------------------
// GetDispute handler tests
// ---------------------------------------------------------------------------

func TestGetDispute_InvalidDisputeID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/disputes/:disputeId", handlers.GetDispute)

	request := httptest.NewRequest(http.MethodGet, "/v1/disputes/not-a-uuid", http.NoBody)
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

func TestGetDispute_NotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	// Default stubDisputeRepo.FindByID returns nil,nil -> UC returns ErrDisputeNotFound
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/disputes/:disputeId", handlers.GetDispute)

	request := httptest.NewRequest(http.MethodGet, "/v1/disputes/"+uuid.NewString(), http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusNotFound,
		404,
		"not_found",
		"dispute not found",
	)
}

func TestGetDispute_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)

	now := time.Now().UTC()
	d := &dispute.Dispute{
		ID:          uuid.New(),
		ExceptionID: uuid.New(),
		Category:    dispute.DisputeCategoryBankFeeError,
		State:       dispute.DisputeStateOpen,
		Description: "test dispute",
		OpenedBy:    "test-user",
		Evidence:    []dispute.Evidence{},
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	disputeRepo := &configurableDisputeRepo{findByIDResult: d}
	handlers := newHandlersWithQueryOptions(
		t, &stubExceptionRepo{}, disputeRepo, &stubAuditRepo{}, nil,
	)
	app.Get("/v1/disputes/:disputeId", handlers.GetDispute)

	request := httptest.NewRequest(http.MethodGet, "/v1/disputes/"+uuid.NewString(), http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestGetDispute_InternalError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)

	disputeRepo := &configurableDisputeRepo{findByIDErr: errTest}
	handlers := newHandlersWithQueryOptions(
		t, &stubExceptionRepo{}, disputeRepo, &stubAuditRepo{}, nil,
	)
	app.Get("/v1/disputes/:disputeId", handlers.GetDispute)

	request := httptest.NewRequest(http.MethodGet, "/v1/disputes/"+uuid.NewString(), http.NoBody)
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

// ---------------------------------------------------------------------------
// ListExceptions extended tests
// ---------------------------------------------------------------------------

func TestListExceptions_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/exceptions", handlers.ListExceptions)

	request := httptest.NewRequest(http.MethodGet, "/v1/exceptions", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))

	items, ok := body["items"].([]any)
	require.True(t, ok)
	assert.Empty(t, items)
}

func TestListExceptions_UCInternalError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{err: errTest})
	app.Get("/v1/exceptions", handlers.ListExceptions)

	request := httptest.NewRequest(http.MethodGet, "/v1/exceptions", http.NoBody)
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

func TestListExceptions_ValidFilters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/exceptions", handlers.ListExceptions)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/exceptions?status=OPEN&severity=HIGH&limit=10&sort_by=created_at&sort_order=asc",
		http.NoBody,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestListExceptions_InvalidDateTo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/exceptions", handlers.ListExceptions)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/exceptions?date_to=not-a-date",
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

// ---------------------------------------------------------------------------
// Body parse failure tests for command handlers
// ---------------------------------------------------------------------------

func TestForceMatch_InvalidBody(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/exceptions/:exceptionId/force-match", handlers.ForceMatch)

	body := strings.NewReader(`{"invalid json`)
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/exceptions/"+uuid.NewString()+"/force-match",
		body,
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid request body",
	)
}

func TestAdjustEntry_InvalidBody(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/exceptions/:exceptionId/adjust-entry", handlers.AdjustEntry)

	body := strings.NewReader(`not json at all`)
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/exceptions/"+uuid.NewString()+"/adjust-entry",
		body,
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid request body",
	)
}

func TestOpenDispute_InvalidBody(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/exceptions/:exceptionId/disputes", handlers.OpenDispute)

	body := strings.NewReader(`{broken`)
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/exceptions/"+uuid.NewString()+"/disputes",
		body,
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid request body",
	)
}

func TestCloseDispute_InvalidBody(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/disputes/:disputeId/close", handlers.CloseDispute)

	body := strings.NewReader(`{{{`)
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/disputes/"+uuid.NewString()+"/close",
		body,
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid request body",
	)
}

func TestSubmitEvidence_InvalidBody(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/disputes/:disputeId/evidence", handlers.SubmitEvidence)

	body := strings.NewReader(`invalid`)
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/disputes/"+uuid.NewString()+"/evidence",
		body,
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid request body",
	)
}

func TestDispatchToExternal_InvalidBody(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/exceptions/:exceptionId/dispatch", handlers.DispatchToExternal)

	body := strings.NewReader(`{bad}`)
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/exceptions/"+uuid.NewString()+"/dispatch",
		body,
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid request body",
	)
}

// ---------------------------------------------------------------------------
// Bulk handler tests
// ---------------------------------------------------------------------------

func TestBulkAssign_InvalidBody(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/exceptions/bulk/assign", handlers.BulkAssign)

	body := strings.NewReader(`{corrupted json`)
	request := httptest.NewRequest(http.MethodPost, "/v1/exceptions/bulk/assign", body)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid request body",
	)
}

func TestBulkAssign_NilUUIDs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/exceptions/bulk/assign", handlers.BulkAssign)

	// Nil UUID passes DTO validation but is caught by parseUUIDs
	body := strings.NewReader(`{"exception_ids":["00000000-0000-0000-0000-000000000000"],"assignee":"user@example.com"}`)
	request := httptest.NewRequest(http.MethodPost, "/v1/exceptions/bulk/assign", body)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid exception id",
	)
}

func TestBulkAssign_ValidationFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/exceptions/bulk/assign", handlers.BulkAssign)

	// Non-UUID strings fail DTO validation
	body := strings.NewReader(`{"exception_ids":["not-a-uuid"],"assignee":"user@example.com"}`)
	request := httptest.NewRequest(http.MethodPost, "/v1/exceptions/bulk/assign", body)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid request body",
	)
}

func TestBulkResolve_InvalidBody(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/exceptions/bulk/resolve", handlers.BulkResolve)

	body := strings.NewReader(`not valid json`)
	request := httptest.NewRequest(http.MethodPost, "/v1/exceptions/bulk/resolve", body)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid request body",
	)
}

func TestBulkResolve_NilUUIDs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/exceptions/bulk/resolve", handlers.BulkResolve)

	body := strings.NewReader(`{"exceptionIds":["00000000-0000-0000-0000-000000000000"],"resolution":"fixed"}`)
	request := httptest.NewRequest(http.MethodPost, "/v1/exceptions/bulk/resolve", body)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid exception id",
	)
}

func TestBulkDispatch_InvalidBody(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/exceptions/bulk/dispatch", handlers.BulkDispatch)

	body := strings.NewReader(`{garbage`)
	request := httptest.NewRequest(http.MethodPost, "/v1/exceptions/bulk/dispatch", body)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid request body",
	)
}

func TestBulkDispatch_NilUUIDs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/exceptions/bulk/dispatch", handlers.BulkDispatch)

	body := strings.NewReader(`{"exception_ids":["00000000-0000-0000-0000-000000000000"],"target_system":"JIRA"}`)
	request := httptest.NewRequest(http.MethodPost, "/v1/exceptions/bulk/dispatch", body)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid exception id",
	)
}

func TestBulkDispatch_EmptyTargetSystem(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/exceptions/bulk/dispatch", handlers.BulkDispatch)

	id := uuid.New().String()
	body := strings.NewReader(`{"exception_ids":["` + id + `"],"target_system":"   "}`)
	request := httptest.NewRequest(http.MethodPost, "/v1/exceptions/bulk/dispatch", body)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		command.ErrBulkTargetSystemEmpty.Error(),
	)
}

// ---------------------------------------------------------------------------
// Comment handler tests
// ---------------------------------------------------------------------------

func TestAddComment_InvalidExceptionID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/exceptions/:exceptionId/comments", handlers.AddComment)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/exceptions/not-uuid/comments",
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

func TestAddComment_InvalidBody(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Post("/v1/exceptions/:exceptionId/comments", handlers.AddComment)

	body := strings.NewReader(`{broken json`)
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/exceptions/"+uuid.NewString()+"/comments",
		body,
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid request body",
	)
}

func TestListComments_InvalidExceptionID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/exceptions/:exceptionId/comments", handlers.ListComments)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/exceptions/bad-id/comments",
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

func TestDeleteComment_InvalidExceptionID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Delete("/v1/exceptions/:exceptionId/comments/:commentId", handlers.DeleteComment)

	request := httptest.NewRequest(
		http.MethodDelete,
		"/v1/exceptions/bad-id/comments/"+uuid.NewString(),
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

func TestDeleteComment_MissingCommentID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	// Register WITHOUT the :commentId param to simulate missing param
	app.Delete("/v1/exceptions/:exceptionId/comments/", handlers.DeleteComment)

	request := httptest.NewRequest(
		http.MethodDelete,
		"/v1/exceptions/"+uuid.NewString()+"/comments/",
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
		"comment id is required",
	)
}

func TestDeleteComment_InvalidCommentID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Delete("/v1/exceptions/:exceptionId/comments/:commentId", handlers.DeleteComment)

	request := httptest.NewRequest(
		http.MethodDelete,
		"/v1/exceptions/"+uuid.NewString()+"/comments/not-a-uuid",
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
		"invalid comment id",
	)
}

// ---------------------------------------------------------------------------
// handleExceptionError cross-context error tests
// ---------------------------------------------------------------------------

func TestHandleExceptionError_TransactionNotFound(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, (&Handlers{}).handleExceptionError, crossAdapters.ErrTransactionNotFound)
	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusNotFound,
		404,
		"not_found",
		"transaction referenced by exception not found",
	)
}

func TestHandleExceptionError_IngestionJobNotFound(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, (&Handlers{}).handleExceptionError, crossAdapters.ErrIngestionJobNotFound)
	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusUnprocessableEntity,
		422,
		"unprocessable_entity",
		"unable to resolve reconciliation context for exception",
	)
}

func TestHandleExceptionError_SourceNotFound(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, (&Handlers{}).handleExceptionError, crossAdapters.ErrSourceNotFound)
	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusUnprocessableEntity,
		422,
		"unprocessable_entity",
		"unable to resolve reconciliation context for exception",
	)
}

func TestHandleExceptionError_ContextNotFound(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, (&Handlers{}).handleExceptionError, crossAdapters.ErrContextNotFound)
	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusUnprocessableEntity,
		422,
		"unprocessable_entity",
		"unable to resolve reconciliation context for exception",
	)
}

func TestHandleExceptionError_ContextLookupNotInitialized(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, (&Handlers{}).handleExceptionError, crossAdapters.ErrContextLookupNotInitialized)
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

func TestHandleExceptionError_EntityNotFound(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, (&Handlers{}).handleExceptionError, entities.ErrExceptionNotFound)
	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusNotFound,
		404,
		"not_found",
		"exception not found",
	)
}

// ---------------------------------------------------------------------------
// GetHistory extended tests
// ---------------------------------------------------------------------------

func TestGetHistory_TenantIDRequired(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)
	// Default newExceptionHandlers uses nil tenantExtractor
	handlers := newExceptionHandlers(t, &stubExceptionRepo{})
	app.Get("/v1/exceptions/:exceptionId/history", handlers.GetHistory)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/exceptions/"+uuid.NewString()+"/history",
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
		"tenant context required",
	)
}

func TestGetHistory_Success_EmptyEntries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)

	tenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	handlers := newHandlersWithQueryOptions(
		t,
		&stubExceptionRepo{},
		&stubDisputeRepo{},
		&configurableAuditRepo{entries: nil, nextCursor: "", err: nil},
		&stubTenantExtractor{tenantID: tenantID},
	)
	app.Get("/v1/exceptions/:exceptionId/history", handlers.GetHistory)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/exceptions/"+uuid.NewString()+"/history",
		http.NoBody,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))

	hasMore, ok := body["hasMore"].(bool)
	require.True(t, ok, "hasMore should be a bool")
	assert.False(t, hasMore)
}

func TestGetHistory_Success_WithEntries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)

	tenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	now := time.Now().UTC()

	entries := []*govEntities.AuditLog{
		{
			ID:        uuid.New(),
			Action:    "FORCE_MATCH",
			ActorID:   strPtr("user@example.com"),
			Changes:   []byte(`{"status":"RESOLVED"}`),
			CreatedAt: now,
		},
		{
			ID:        uuid.New(),
			Action:    "ADJUST_ENTRY",
			ActorID:   nil,
			Changes:   nil,
			CreatedAt: now.Add(-time.Hour),
		},
	}

	handlers := newHandlersWithQueryOptions(
		t,
		&stubExceptionRepo{},
		&stubDisputeRepo{},
		&configurableAuditRepo{entries: entries, nextCursor: "next123", err: nil},
		&stubTenantExtractor{tenantID: tenantID},
	)
	app.Get("/v1/exceptions/:exceptionId/history", handlers.GetHistory)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/exceptions/"+uuid.NewString()+"/history",
		http.NoBody,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))

	items, ok := body["items"].([]any)
	require.True(t, ok, "items should be a []any")
	assert.Len(t, items, 2)

	hasMore, ok := body["hasMore"].(bool)
	require.True(t, ok, "hasMore should be a bool")
	assert.True(t, hasMore)

	nextCursor, ok := body["nextCursor"].(string)
	require.True(t, ok, "nextCursor should be a string")
	assert.Equal(t, "next123", nextCursor)
}

func TestGetHistory_Success_WithInvalidChangesJSON(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)

	tenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	now := time.Now().UTC()

	entries := []*govEntities.AuditLog{
		{
			ID:        uuid.New(),
			Action:    "RESOLVE",
			ActorID:   strPtr("actor"),
			Changes:   []byte(`{invalid json`),
			CreatedAt: now,
		},
	}

	handlers := newHandlersWithQueryOptions(
		t,
		&stubExceptionRepo{},
		&stubDisputeRepo{},
		&configurableAuditRepo{entries: entries},
		&stubTenantExtractor{tenantID: tenantID},
	)
	app.Get("/v1/exceptions/:exceptionId/history", handlers.GetHistory)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/exceptions/"+uuid.NewString()+"/history",
		http.NoBody,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Should still return 200 -- invalid JSON is logged but not a fatal error
	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))

	items, ok := body["items"].([]any)
	require.True(t, ok, "items should be a []any")
	assert.Len(t, items, 1)

	// Changes should be nil (unmarshaling failed)
	firstItem, ok := items[0].(map[string]any)
	require.True(t, ok, "first item should be a map[string]any")
	assert.Nil(t, firstItem["changes"])
}

func TestGetHistory_AuditRepoError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)

	tenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	handlers := newHandlersWithQueryOptions(
		t,
		&stubExceptionRepo{},
		&stubDisputeRepo{},
		&configurableAuditRepo{err: errTest},
		&stubTenantExtractor{tenantID: tenantID},
	)
	app.Get("/v1/exceptions/:exceptionId/history", handlers.GetHistory)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/exceptions/"+uuid.NewString()+"/history",
		http.NoBody,
	)
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

func TestListExceptions_InvalidCursor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)

	// Create a repo that returns libHTTP.ErrInvalidCursor to test that handler branch
	cursorErr := &stubExceptionRepo{err: libHTTP.ErrInvalidCursor}
	handlers := newExceptionHandlers(t, cursorErr)
	app.Get("/v1/exceptions", handlers.ListExceptions)

	request := httptest.NewRequest(http.MethodGet, "/v1/exceptions", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid pagination parameters",
	)
}

func TestListDisputes_InvalidCursor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)

	disputeRepo := &configurableDisputeRepo{listErr: libHTTP.ErrInvalidCursor}
	handlers := newHandlersWithQueryOptions(
		t, &stubExceptionRepo{}, disputeRepo, &stubAuditRepo{}, nil,
	)
	app.Get("/v1/disputes", handlers.ListDisputes)

	request := httptest.NewRequest(http.MethodGet, "/v1/disputes", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid pagination parameters",
	)
}

// ---------------------------------------------------------------------------
// GetException success test
// ---------------------------------------------------------------------------

func TestGetException_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)

	now := time.Now().UTC()
	exception := &entities.Exception{
		ID:            uuid.New(),
		TransactionID: uuid.New(),
		Severity:      "HIGH",
		Status:        "OPEN",
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	handlers := newExceptionHandlers(t, &stubExceptionRepo{exception: exception})
	app.Get("/v1/exceptions/:exceptionId", handlers.GetException)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/exceptions/"+uuid.NewString(),
		http.NoBody,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// handleDisputeError additional coverage
// ---------------------------------------------------------------------------

func TestHandleDisputeError_InvalidDisputeCategory(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, (&Handlers{}).handleDisputeError, dispute.ErrInvalidDisputeCategory)
	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		dispute.ErrInvalidDisputeCategory.Error(),
	)
}

func TestHandleDisputeError_ActorRequired(t *testing.T) {
	t.Parallel()

	resp := executeErrorHandler(t, (&Handlers{}).handleDisputeError, command.ErrActorRequired)
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func strPtr(s string) *string {
	return &s
}
