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
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	"github.com/LerianStudio/matcher/internal/matching/services/command"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestUnmatchHandler_InvalidMatchGroupID(t *testing.T) {
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
	handler, err := newTestHandler(t, &command.UseCase{}, &stubMatchRunRepo{}, &stubMatchGroupRepo{}, ctxProv, false)
	require.NoError(t, err)

	app.Delete("/v1/matching/groups/:matchGroupId", handler.Unmatch)

	payload := UnmatchRequest{Reason: "test reason"}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodDelete,
		"/v1/matching/groups/invalid-uuid?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "invalid match group id", errResp.Message)
}

func TestUnmatchHandler_MissingContextID(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	matchGroupID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	app := newFiberTestApp(ctx)

	ctxProv := &stubContextProvider{info: nil}
	handler, err := newTestHandler(t, &command.UseCase{}, &stubMatchRunRepo{}, &stubMatchGroupRepo{}, ctxProv, false)
	require.NoError(t, err)

	app.Delete("/v1/matching/groups/:matchGroupId", handler.Unmatch)

	payload := UnmatchRequest{Reason: "test reason"}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodDelete,
		"/v1/matching/groups/"+matchGroupID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestUnmatchHandler_InvalidPayload(t *testing.T) {
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
	handler, err := newTestHandler(t, &command.UseCase{}, &stubMatchRunRepo{}, &stubMatchGroupRepo{}, ctxProv, false)
	require.NoError(t, err)

	app.Delete("/v1/matching/groups/:matchGroupId", handler.Unmatch)

	request := httptest.NewRequest(
		http.MethodDelete,
		"/v1/matching/groups/"+matchGroupID.String()+"?contextId="+contextID.String(),
		bytes.NewBufferString(`{invalid json`),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "invalid unmatch payload", errResp.Message)
}

func TestUnmatchHandler_MissingReason(t *testing.T) {
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
	handler, err := newTestHandler(t, &command.UseCase{}, &stubMatchRunRepo{}, &stubMatchGroupRepo{}, ctxProv, false)
	require.NoError(t, err)

	app.Delete("/v1/matching/groups/:matchGroupId", handler.Unmatch)

	payload := UnmatchRequest{Reason: ""}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodDelete,
		"/v1/matching/groups/"+matchGroupID.String()+"?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	// Validation catches it at the payload level before we check the reason field
	require.Equal(t, "invalid unmatch payload", errResp.Message)
}

func TestUnmatchHandler_ContextNotFound(t *testing.T) {
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

	ctxProv := &stubContextProvider{info: nil}
	handler, err := newTestHandler(t, &command.UseCase{}, &stubMatchRunRepo{}, &stubMatchGroupRepo{}, ctxProv, false)
	require.NoError(t, err)

	app.Delete("/v1/matching/groups/:matchGroupId", handler.Unmatch)

	payload := UnmatchRequest{Reason: "test reason"}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodDelete,
		"/v1/matching/groups/"+matchGroupID.String()+"?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
}

func TestUnmatchHandler_ContextNotActive(t *testing.T) {
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
	handler, err := newTestHandler(t, &command.UseCase{}, &stubMatchRunRepo{}, &stubMatchGroupRepo{}, ctxProv, false)
	require.NoError(t, err)

	app.Delete("/v1/matching/groups/:matchGroupId", handler.Unmatch)

	payload := UnmatchRequest{Reason: "test reason"}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodDelete,
		"/v1/matching/groups/"+matchGroupID.String()+"?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	var errResp sharedhttp.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, fiber.StatusForbidden, resp.StatusCode)
	require.Equal(t, http.StatusText(fiber.StatusForbidden), errResp.Title)
}

func TestUnmatchHandler_MatchGroupNotFound(t *testing.T) {
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

	handler, err := newTestHandler(t, uc, &stubMatchRunRepo{}, &stubMatchGroupRepo{}, ctxProv, false)
	require.NoError(t, err)

	app.Delete("/v1/matching/groups/:matchGroupId", handler.Unmatch)

	payload := UnmatchRequest{Reason: "test reason"}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(
		http.MethodDelete,
		"/v1/matching/groups/"+matchGroupID.String()+"?contextId="+contextID.String(),
		bytes.NewBuffer(body),
	)
	request.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(request)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
}
