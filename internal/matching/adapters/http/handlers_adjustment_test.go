//go:build unit

package http

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepositories "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	"github.com/LerianStudio/matcher/internal/matching/services/command"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

type failingAdjustmentRepo struct {
	err error
}

func (r *failingAdjustmentRepo) Create(
	_ context.Context,
	_ *matchingEntities.Adjustment,
) (*matchingEntities.Adjustment, error) {
	return nil, r.err
}

func (r *failingAdjustmentRepo) CreateWithTx(
	_ context.Context,
	_ *sql.Tx,
	_ *matchingEntities.Adjustment,
) (*matchingEntities.Adjustment, error) {
	return nil, r.err
}

func (r *failingAdjustmentRepo) CreateWithAuditLog(
	_ context.Context,
	_ *matchingEntities.Adjustment,
	_ *shared.AuditLog,
) (*matchingEntities.Adjustment, error) {
	return nil, r.err
}

func (r *failingAdjustmentRepo) CreateWithAuditLogWithTx(
	_ context.Context,
	_ *sql.Tx,
	_ *matchingEntities.Adjustment,
	_ *shared.AuditLog,
) (*matchingEntities.Adjustment, error) {
	return nil, r.err
}

func (r *failingAdjustmentRepo) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*matchingEntities.Adjustment, error) {
	return nil, r.err
}

func (r *failingAdjustmentRepo) ListByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ matchingRepositories.CursorFilter,
) ([]*matchingEntities.Adjustment, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, r.err
}

func (r *failingAdjustmentRepo) ListByMatchGroupID(
	_ context.Context,
	_, _ uuid.UUID,
) ([]*matchingEntities.Adjustment, error) {
	return nil, r.err
}

func newAdjustmentUseCase(
	t *testing.T,
	ctxProvider ports.ContextProvider,
	matchGroup *matchingEntities.MatchGroup,
	adjErr error,
) *command.UseCase {
	t.Helper()

	sourceProvider := &runMatchSourceProvider{sources: []*ports.SourceInfo{
		{ID: uuid.New(), Type: ports.SourceTypeLedger},
		{ID: uuid.New(), Type: ports.SourceTypeAPI},
	}}

	ruleProvider := &runMatchRuleProvider{rules: shared.MatchRules{}}
	txRepo := &runMatchTxRepo{candidates: []*shared.Transaction{}, err: nil}
	lockManager := &runMatchLockManager{}
	runRepo := &runMatchRunRepo{}
	groupRepo := &runMatchGroupRepo{groups: []*matchingEntities.MatchGroup{matchGroup}}
	itemRepo := &runMatchItemRepo{}
	exceptionCreator := &runMatchExceptionCreator{}
	outboxRepo := &runMatchOutboxRepo{}
	rateRepo := &runMatchRateRepo{}
	feeVarianceRepo := &runMatchFeeVarianceRepo{}
	adjustmentRepo := &failingAdjustmentRepo{err: adjErr}
	infraProvider := &runMatchInfraProvider{}
	auditLogRepo := &runMatchAuditLogRepo{}

	feeScheduleRepo := &runMatchFeeScheduleRepo{}

	uc, err := command.New(command.UseCaseDeps{
		ContextProvider:  ctxProvider,
		SourceProvider:   sourceProvider,
		RuleProvider:     ruleProvider,
		TxRepo:           txRepo,
		LockManager:      lockManager,
		MatchRunRepo:     runRepo,
		MatchGroupRepo:   groupRepo,
		MatchItemRepo:    itemRepo,
		ExceptionCreator: exceptionCreator,
		OutboxRepo:       outboxRepo,
		RateRepo:         rateRepo,
		FeeVarianceRepo:  feeVarianceRepo,
		AdjustmentRepo:   adjustmentRepo,
		InfraProvider:    infraProvider,
		AuditLogRepo:     auditLogRepo,
		FeeScheduleRepo:  feeScheduleRepo,
	})
	require.NoError(t, err)

	return uc
}

// TestCreateAdjustmentHandlerRouting tests that the adjustment handler is properly wired
// and responds to requests. Since stubs don't return real match groups, we expect a 404.
func TestCreateAdjustmentHandlerRouting(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	matchGroupID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	uc := newRunMatchUseCase(t, ctxProv, []*shared.Transaction{}, nil)

	handler, err := NewHandler(uc, newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}), ctxProv, false)
	require.NoError(t, err)

	app.Post("/v1/matching/adjustments", handler.CreateAdjustment)

	payload := CreateAdjustmentRequest{
		MatchGroupID: matchGroupID.String(),
		Type:         "BANK_FEE",
		Direction:    "DEBIT",
		Amount:       "10.50",
		Currency:     "USD",
		Description:  "Bank wire fee adjustment",
		Reason:       "Variance due to bank processing fee",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/adjustments?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	// Stubs don't return match groups, so we expect 404 (match group not found)
	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
}

func TestCreateAdjustmentMissingTargets(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	uc := newRunMatchUseCase(t, ctxProv, []*shared.Transaction{}, nil)

	handler, err := NewHandler(uc, newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}), ctxProv, false)
	require.NoError(t, err)

	app.Post("/v1/matching/adjustments", handler.CreateAdjustment)

	// Missing both match_group_id and transaction_id
	payload := CreateAdjustmentRequest{
		Type:        "BANK_FEE",
		Direction:   "DEBIT",
		Amount:      "10.50",
		Currency:    "USD",
		Description: "Bank wire fee adjustment",
		Reason:      "Variance due to bank processing fee",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/adjustments?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	var errResp libHTTP.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "match_group_id or transaction_id is required", errResp.Message)
}

// TestCreateAdjustmentMissingRequiredFields consolidates tests for missing required fields.
// Each case omits a different required field and expects a 400 response with the service validation message.
func TestCreateAdjustmentMissingRequiredFields(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		payload CreateAdjustmentRequest
		message string
	}{
		{
			name: "missing_type",
			payload: CreateAdjustmentRequest{
				MatchGroupID: uuid.New().String(),
				Direction:    "DEBIT",
				Amount:       "10.50",
				Currency:     "USD",
				Description:  "Bank wire fee adjustment",
				Reason:       "Variance due to bank processing fee",
			},
			message: "invalid adjustment payload",
		},
		{
			name: "missing_direction",
			payload: CreateAdjustmentRequest{
				MatchGroupID: uuid.New().String(),
				Type:         "BANK_FEE",
				Amount:       "10.50",
				Currency:     "USD",
				Description:  "Bank wire fee adjustment",
				Reason:       "Variance due to bank processing fee",
			},
			message: "invalid adjustment payload",
		},
		{
			name: "missing_currency",
			payload: CreateAdjustmentRequest{
				MatchGroupID: uuid.New().String(),
				Type:         "BANK_FEE",
				Direction:    "DEBIT",
				Amount:       "10.50",
				Description:  "Bank wire fee adjustment",
				Reason:       "Variance due to bank processing fee",
			},
			message: "invalid adjustment payload",
		},
		{
			name: "missing_description",
			payload: CreateAdjustmentRequest{
				MatchGroupID: uuid.New().String(),
				Type:         "BANK_FEE",
				Direction:    "DEBIT",
				Amount:       "10.50",
				Currency:     "USD",
				Reason:       "Variance due to bank processing fee",
			},
			message: "invalid adjustment payload",
		},
		{
			name: "missing_reason",
			payload: CreateAdjustmentRequest{
				MatchGroupID: uuid.New().String(),
				Type:         "BANK_FEE",
				Direction:    "DEBIT",
				Amount:       "10.50",
				Currency:     "USD",
				Description:  "Bank wire fee adjustment",
			},
			message: "invalid adjustment payload",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tenantID := uuid.New()
			contextID := uuid.New()
			ctx := libCommons.ContextWithTracer(
				context.Background(),
				noop.NewTracerProvider().Tracer("test"),
			)
			ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
			app := newFiberTestApp(ctx)

			ctxProv := &stubContextProvider{
				info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
			}
			uc := newRunMatchUseCase(t, ctxProv, []*shared.Transaction{}, nil)

			handler, err := NewHandler(uc, newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}), ctxProv, false)
			require.NoError(t, err)

			app.Post("/v1/matching/adjustments", handler.CreateAdjustment)

			body, err := json.Marshal(tc.payload)
			require.NoError(t, err)

			request := httptest.NewRequest(
				http.MethodPost,
				"/v1/matching/adjustments?contextId="+contextID.String(),
				bytes.NewBuffer(body),
			)
			request.Header.Set("Content-Type", "application/json")

			resp, err := app.Test(request)
			require.NoError(t, err)

			defer resp.Body.Close()

			var errResp libHTTP.ErrorResponse
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

			require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
			require.Equal(t, tc.message, errResp.Message)
		})
	}
}

func TestCreateAdjustmentInvalidAmountFormat(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	matchGroupID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	uc := newRunMatchUseCase(t, ctxProv, []*shared.Transaction{}, nil)

	handler, err := NewHandler(uc, newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}), ctxProv, false)
	require.NoError(t, err)

	app.Post("/v1/matching/adjustments", handler.CreateAdjustment)

	payload := CreateAdjustmentRequest{
		MatchGroupID: matchGroupID.String(),
		Type:         "BANK_FEE",
		Direction:    "DEBIT",
		Amount:       "invalid-amount",
		Currency:     "USD",
		Description:  "Bank wire fee adjustment",
		Reason:       "Variance due to bank processing fee",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/adjustments?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	var errResp libHTTP.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "invalid adjustment payload", errResp.Message)
}

func TestCreateAdjustmentContextNotFound(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	matchGroupID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	// Context provider returns nil (not found)
	ctxProv := &stubContextProvider{info: nil}
	handler, err := NewHandler(
		&command.UseCase{},
		newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
		ctxProv,
		false,
	)
	require.NoError(t, err)

	app.Post("/v1/matching/adjustments", handler.CreateAdjustment)

	payload := CreateAdjustmentRequest{
		MatchGroupID: matchGroupID.String(),
		Type:         "BANK_FEE",
		Direction:    "DEBIT",
		Amount:       "10.50",
		Currency:     "USD",
		Description:  "Bank wire fee adjustment",
		Reason:       "Variance due to bank processing fee",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/adjustments?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	var errResp libHTTP.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	require.Equal(t, "context not found", errResp.Message)
}

func TestCreateAdjustmentContextNotActive(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	matchGroupID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: false},
	}
	uc := newRunMatchUseCase(t, ctxProv, []*shared.Transaction{}, nil)

	handler, err := NewHandler(uc, newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}), ctxProv, false)
	require.NoError(t, err)

	app.Post("/v1/matching/adjustments", handler.CreateAdjustment)

	payload := CreateAdjustmentRequest{
		MatchGroupID: matchGroupID.String(),
		Type:         "BANK_FEE",
		Direction:    "DEBIT",
		Amount:       "10.50",
		Currency:     "USD",
		Description:  "Bank wire fee adjustment",
		Reason:       "Variance due to bank processing fee",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/adjustments?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	var errResp libHTTP.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusForbidden, resp.StatusCode)
	require.Equal(t, "context_not_active", errResp.Title)
}

func TestCreateAdjustmentInvalidPayload(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	uc := newRunMatchUseCase(t, ctxProv, []*shared.Transaction{}, nil)

	handler, err := NewHandler(uc, newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}), ctxProv, false)
	require.NoError(t, err)

	app.Post("/v1/matching/adjustments", handler.CreateAdjustment)

	// Invalid JSON
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/adjustments?contextId="+contextID.String(),
		bytes.NewBufferString(`{invalid json`),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	var errResp libHTTP.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "invalid adjustment payload", errResp.Message)
}

func TestCreateAdjustmentInvalidMatchGroupID(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	uc := newRunMatchUseCase(t, ctxProv, []*shared.Transaction{}, nil)

	handler, err := NewHandler(uc, newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}), ctxProv, false)
	require.NoError(t, err)

	app.Post("/v1/matching/adjustments", handler.CreateAdjustment)

	payload := CreateAdjustmentRequest{
		MatchGroupID: "invalid-uuid",
		Type:         "BANK_FEE",
		Direction:    "DEBIT",
		Amount:       "10.50",
		Currency:     "USD",
		Description:  "Bank wire fee adjustment",
		Reason:       "Variance due to bank processing fee",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/adjustments?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	var errResp libHTTP.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "invalid match_group_id", errResp.Message)
}

func TestCreateAdjustmentInvalidTransactionID(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	uc := newRunMatchUseCase(t, ctxProv, []*shared.Transaction{}, nil)

	handler, err := NewHandler(uc, newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}), ctxProv, false)
	require.NoError(t, err)

	app.Post("/v1/matching/adjustments", handler.CreateAdjustment)

	payload := CreateAdjustmentRequest{
		TransactionID: "invalid-uuid",
		Type:          "BANK_FEE",
		Direction:     "DEBIT",
		Amount:        "10.50",
		Currency:      "USD",
		Description:   "Bank wire fee adjustment",
		Reason:        "Variance due to bank processing fee",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/adjustments?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	var errResp libHTTP.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "invalid transaction_id", errResp.Message)
}

func TestCreateAdjustmentMissingContextID(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{info: nil}
	handler, err := NewHandler(
		&command.UseCase{},
		newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
		ctxProv,
		false,
	)
	require.NoError(t, err)

	app.Post("/v1/matching/adjustments", handler.CreateAdjustment)

	matchGroupID := uuid.New()
	payload := CreateAdjustmentRequest{
		MatchGroupID: matchGroupID.String(),
		Type:         "BANK_FEE",
		Direction:    "DEBIT",
		Amount:       "10.50",
		Currency:     "USD",
		Description:  "Bank wire fee adjustment",
		Reason:       "Variance due to bank processing fee",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	// No contextId query param
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/adjustments",
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestCreateAdjustmentServiceError(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	matchGroupID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	matchGroup := &matchingEntities.MatchGroup{ID: matchGroupID, ContextID: contextID}
	uc := newAdjustmentUseCase(t, ctxProv, matchGroup, errTestDatabaseError)

	handler, err := NewHandler(uc, newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}), ctxProv, false)
	require.NoError(t, err)

	app.Post("/v1/matching/adjustments", handler.CreateAdjustment)

	payload := CreateAdjustmentRequest{
		MatchGroupID: matchGroupID.String(),
		Type:         "BANK_FEE",
		Direction:    "DEBIT",
		Amount:       "10.50",
		Currency:     "USD",
		Description:  "Bank wire fee adjustment",
		Reason:       "Variance due to bank processing fee",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/adjustments?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	var errResp libHTTP.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	require.Equal(t, "an unexpected error occurred", errResp.Message)
}
