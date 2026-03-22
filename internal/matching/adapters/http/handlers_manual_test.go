//go:build unit

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"

	sharedhttp "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	"github.com/LerianStudio/matcher/internal/matching/services/command"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// TestCreateManualMatchHandlerRouting tests that the manual match handler is properly wired
// and responds to requests. Since stubs don't return real transactions, we expect a 404.
func TestCreateManualMatchHandlerRouting(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	txID1 := uuid.New()
	txID2 := uuid.New()
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

	app.Post("/v1/matching/manual", handler.CreateManualMatch)

	payload := CreateManualMatchRequest{
		TransactionIDs: []string{txID1.String(), txID2.String()},
		Notes:          "Manual match for Q4 reconciliation",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/manual?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	// Stubs don't return transactions, so we expect 404 (transaction not found)
	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
}

func TestCreateManualMatchTooFewTransactions(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	txID1 := uuid.New()
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

	app.Post("/v1/matching/manual", handler.CreateManualMatch)

	// Only one transaction ID (need at least 2)
	payload := CreateManualMatchRequest{
		TransactionIDs: []string{txID1.String()},
		Notes:          "Manual match for Q4 reconciliation",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/manual?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "invalid manual match payload", errResp.Message)
}

func TestCreateManualMatchInvalidTransactionUUID(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	txID1 := uuid.New()
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

	app.Post("/v1/matching/manual", handler.CreateManualMatch)

	// Second transaction ID is invalid
	payload := CreateManualMatchRequest{
		TransactionIDs: []string{txID1.String(), "not-a-valid-uuid"},
		Notes:          "Manual match for Q4 reconciliation",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/manual?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "invalid manual match payload", errResp.Message)
}

func TestCreateManualMatchDuplicateTransactionIDs(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	txID1 := uuid.New()
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

	app.Post("/v1/matching/manual", handler.CreateManualMatch)

	// Same transaction ID twice (duplicate)
	payload := CreateManualMatchRequest{
		TransactionIDs: []string{txID1.String(), txID1.String()},
		Notes:          "Manual match for Q4 reconciliation",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/manual?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "duplicate transaction IDs provided", errResp.Message)
}

func TestCreateManualMatchContextNotFound(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	txID1 := uuid.New()
	txID2 := uuid.New()
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

	app.Post("/v1/matching/manual", handler.CreateManualMatch)

	payload := CreateManualMatchRequest{
		TransactionIDs: []string{txID1.String(), txID2.String()},
		Notes:          "Manual match for Q4 reconciliation",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/manual?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	require.Equal(t, "context not found", errResp.Message)
}

func TestCreateManualMatchInvalidPayload(t *testing.T) {
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

	app.Post("/v1/matching/manual", handler.CreateManualMatch)

	// Invalid JSON
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/manual?contextId="+contextID.String(),
		bytes.NewBufferString(`{invalid json`),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "invalid manual match payload", errResp.Message)
}

func TestCreateManualMatchEmptyTransactionIDs(t *testing.T) {
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

	app.Post("/v1/matching/manual", handler.CreateManualMatch)

	// Empty transaction IDs list
	payload := CreateManualMatchRequest{
		TransactionIDs: []string{},
		Notes:          "Manual match for Q4 reconciliation",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/manual?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "invalid manual match payload", errResp.Message)
}

func TestCreateManualMatchMissingContextID(t *testing.T) {
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

	app.Post("/v1/matching/manual", handler.CreateManualMatch)

	txID1 := uuid.New()
	txID2 := uuid.New()
	payload := CreateManualMatchRequest{
		TransactionIDs: []string{txID1.String(), txID2.String()},
		Notes:          "Manual match for Q4 reconciliation",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	// No contextId query param
	request := httptest.NewRequest(http.MethodPost, "/v1/matching/manual", bytes.NewBuffer(body))
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestCreateManualMatchContextNotActive(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	txID1 := uuid.New()
	txID2 := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	// Context is not active
	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: false},
	}
	handler, err := NewHandler(
		&command.UseCase{},
		newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}),
		ctxProv,
		false,
	)
	require.NoError(t, err)

	app.Post("/v1/matching/manual", handler.CreateManualMatch)

	payload := CreateManualMatchRequest{
		TransactionIDs: []string{txID1.String(), txID2.String()},
		Notes:          "Manual match for Q4 reconciliation",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/manual?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusForbidden, resp.StatusCode)
	require.Equal(t, "context_not_active", errResp.Title)
}

func TestCreateManualMatchSameSourceTransactions(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	sourceID := uuid.New()
	txID1 := uuid.New()
	txID2 := uuid.New()

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	// Both transactions share the same SourceID
	transactions := []*shared.Transaction{
		{
			ID:       txID1,
			SourceID: sourceID,
			Status:   shared.TransactionStatusUnmatched,
			Amount:   decimal.NewFromInt(100),
			Currency: "USD",
		},
		{
			ID:       txID2,
			SourceID: sourceID,
			Status:   shared.TransactionStatusUnmatched,
			Amount:   decimal.NewFromInt(200),
			Currency: "USD",
		},
	}
	uc := newRunMatchUseCase(t, ctxProv, transactions, nil)

	handler, err := NewHandler(uc, newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}), ctxProv, false)
	require.NoError(t, err)

	app.Post("/v1/matching/manual", handler.CreateManualMatch)

	payload := CreateManualMatchRequest{
		TransactionIDs: []string{txID1.String(), txID2.String()},
		Notes:          "Manual match attempt with same source",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/manual?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "transactions must come from at least two different sources", errResp.Message)
}

func TestCreateManualMatchServiceError(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextID := uuid.New()
	txID1 := uuid.New()
	txID2 := uuid.New()

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: contextID, Active: true},
	}
	transactions := []*shared.Transaction{
		{
			ID:       txID1,
			Status:   shared.TransactionStatusUnmatched,
			Amount:   decimal.Zero,
			Currency: "USD",
		},
		{
			ID:       txID2,
			Status:   shared.TransactionStatusUnmatched,
			Amount:   decimal.Zero,
			Currency: "USD",
		},
	}
	uc := newRunMatchUseCase(t, ctxProv, transactions, errTestDatabaseError)

	handler, err := NewHandler(uc, newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{}), ctxProv, false)
	require.NoError(t, err)

	app.Post("/v1/matching/manual", handler.CreateManualMatch)

	payload := CreateManualMatchRequest{
		TransactionIDs: []string{txID1.String(), txID2.String()},
		Notes:          "Manual match for Q4 reconciliation",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/matching/manual?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	require.Equal(t, "an unexpected error occurred", errResp.Message)
}
