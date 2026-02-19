//go:build unit

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/mock/gomock"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/ingestion/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	outboxEntities "github.com/LerianStudio/matcher/internal/outbox/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

var errTestGenericDBError = errors.New("some internal error")

// newMultipartRequestWithFileContent builds a multipart/form-data request where
// the file part has the given filename and literal content.
func newMultipartRequestWithFileContent(
	t *testing.T,
	path, format, filename, content string,
) *http.Request {
	t.Helper()

	buffer := &bytes.Buffer{}
	writer := multipart.NewWriter(buffer)

	if format != "" {
		require.NoError(t, writer.WriteField("format", format))
	}

	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, filename))
	h.Set("Content-Type", "application/octet-stream")

	fileWriter, err := writer.CreatePart(h)
	require.NoError(t, err)

	_, err = fileWriter.Write([]byte(content))
	require.NoError(t, err)

	require.NoError(t, writer.Close())

	request := httptest.NewRequest(http.MethodPost, path, buffer)
	request.Header.Set("Content-Type", writer.FormDataContentType())

	return request
}

// newMultipartRequestWithFormField creates a multipart request with an extra
// form field besides format and file.
func newMultipartRequestWithFormField(
	t *testing.T,
	path, format, filename, content, fieldName, fieldValue string,
) *http.Request {
	t.Helper()

	buffer := &bytes.Buffer{}
	writer := multipart.NewWriter(buffer)

	if format != "" {
		require.NoError(t, writer.WriteField("format", format))
	}

	if fieldName != "" {
		require.NoError(t, writer.WriteField(fieldName, fieldValue))
	}

	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, filename))
	h.Set("Content-Type", "application/octet-stream")

	fileWriter, err := writer.CreatePart(h)
	require.NoError(t, err)

	_, err = fileWriter.Write([]byte(content))
	require.NoError(t, err)

	require.NoError(t, writer.Close())

	request := httptest.NewRequest(http.MethodPost, path, buffer)
	request.Header.Set("Content-Type", writer.FormDataContentType())

	return request
}

func testTracerCtx() context.Context {
	return libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
}

// ---------------------------------------------------------------------------
// detectFormatFromFilename
// ---------------------------------------------------------------------------

func TestDetectFormatFromFilename(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filename string
		want     string
	}{
		{"data.csv", "csv"},
		{"DATA.CSV", "csv"},
		{"report.json", "json"},
		{"REPORT.JSON", "json"},
		{"feed.xml", "xml"},
		{"FEED.XML", "xml"},
		{"file.txt", ""},
		{"noextension", ""},
		{"archive.tar.gz", ""},
		{"data.CSV", "csv"},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			t.Parallel()

			got := detectFormatFromFilename(tt.filename)
			assert.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// parseSearchParams
// ---------------------------------------------------------------------------

func TestParseSearchParams_Defaults(t *testing.T) {
	t.Parallel()

	params, err := parseSearchParams(ingestionSearchRequest("", "", "", "", "", "", "", "", 0, 0))
	require.NoError(t, err)
	assert.Equal(t, 20, params.Limit)
	assert.Equal(t, 0, params.Offset)
}

func TestParseSearchParams_ClampLimit(t *testing.T) {
	t.Parallel()

	params, err := parseSearchParams(ingestionSearchRequest("", "", "", "", "", "", "", "", 100, 0))
	require.NoError(t, err)
	assert.Equal(t, 50, params.Limit)
}

func TestParseSearchParams_NegativeOffset(t *testing.T) {
	t.Parallel()

	params, err := parseSearchParams(ingestionSearchRequest("", "", "", "", "", "", "", "", 10, -5))
	require.NoError(t, err)
	assert.Equal(t, 0, params.Offset)
}

func TestParseSearchParams_InvalidAmountMin(t *testing.T) {
	t.Parallel()

	_, err := parseSearchParams(ingestionSearchRequest("", "not-a-number", "", "", "", "", "", "", 0, 0))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid amount_min")
}

func TestParseSearchParams_InvalidAmountMax(t *testing.T) {
	t.Parallel()

	_, err := parseSearchParams(ingestionSearchRequest("", "", "xyz", "", "", "", "", "", 0, 0))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid amount_max")
}

func TestParseSearchParams_InvalidDateFrom(t *testing.T) {
	t.Parallel()

	_, err := parseSearchParams(ingestionSearchRequest("", "", "", "2025-01-01", "", "", "", "", 0, 0))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid date_from")
}

func TestParseSearchParams_InvalidDateTo(t *testing.T) {
	t.Parallel()

	_, err := parseSearchParams(ingestionSearchRequest("", "", "", "", "not-a-time", "", "", "", 0, 0))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid date_to")
}

func TestParseSearchParams_InvalidSourceID(t *testing.T) {
	t.Parallel()

	_, err := parseSearchParams(ingestionSearchRequest("", "", "", "", "", "", "", "bad-uuid", 0, 0))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid source_id")
}

func TestParseSearchParams_AllFieldsValid(t *testing.T) {
	t.Parallel()

	srcID := uuid.New()
	params, err := parseSearchParams(ingestionSearchRequest(
		"search-term",
		"10.00",
		"500.00",
		"2025-01-01T00:00:00Z",
		"2025-12-31T23:59:59Z",
		"TXN-123",
		"USD",
		srcID.String(),
		25,
		5,
	))
	require.NoError(t, err)

	assert.Equal(t, "search-term", params.Query)
	assert.NotNil(t, params.AmountMin)
	assert.True(t, params.AmountMin.Equal(decimal.NewFromInt(10)))
	assert.NotNil(t, params.AmountMax)
	assert.True(t, params.AmountMax.Equal(decimal.NewFromInt(500)))
	assert.NotNil(t, params.DateFrom)
	assert.NotNil(t, params.DateTo)
	assert.Equal(t, "TXN-123", params.Reference)
	assert.Equal(t, "USD", params.Currency)
	assert.NotNil(t, params.SourceID)
	assert.Equal(t, srcID, *params.SourceID)
	assert.Equal(t, 25, params.Limit)
	assert.Equal(t, 5, params.Offset)
}

func ingestionSearchRequest(
	q, amountMin, amountMax, dateFrom, dateTo, ref, currency, srcID string,
	limit, offset int,
) dto.SearchTransactionsRequest {
	return dto.SearchTransactionsRequest{
		Query:     q,
		AmountMin: amountMin,
		AmountMax: amountMax,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
		Reference: ref,
		Currency:  currency,
		SourceID:  srcID,
		Limit:     limit,
		Offset:    offset,
	}
}

// ---------------------------------------------------------------------------
// SearchTransactions handler
// ---------------------------------------------------------------------------

func TestSearchTransactionsHandler_Success(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	txID := uuid.New()
	fixture.txs.EXPECT().
		SearchTransactions(gomock.Any(), contextID, gomock.Any()).
		Return([]*shared.Transaction{
			{
				ID:               txID,
				SourceID:         uuid.New(),
				ExternalID:       "EXT-001",
				Amount:           decimal.NewFromFloat(100.50),
				Currency:         "USD",
				Status:           shared.TransactionStatusUnmatched,
				ExtractionStatus: shared.ExtractionStatusComplete,
			},
		}, int64(1), nil)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get("/v1/imports/contexts/:contextId/transactions/search", handlers.SearchTransactions)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/transactions/search?q=EXT",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var payload dto.SearchTransactionsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.Len(t, payload.Items, 1)
	assert.Equal(t, int64(1), payload.Total)
	assert.Equal(t, 20, payload.Limit)
	assert.Equal(t, 0, payload.Offset)
}

func TestSearchTransactionsHandler_WithAllFilters(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	srcID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	fixture.txs.EXPECT().
		SearchTransactions(gomock.Any(), contextID, gomock.Any()).
		Return([]*shared.Transaction{}, int64(0), nil)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get("/v1/imports/contexts/:contextId/transactions/search", handlers.SearchTransactions)

	queryParams := fmt.Sprintf(
		"q=wire&amount_min=10.00&amount_max=500.00"+
			"&date_from=2025-01-01T00:00:00Z&date_to=2025-12-31T23:59:59Z"+
			"&reference=TXN-999&currency=EUR&source_id=%s&status=MATCHED&limit=30&offset=10",
		srcID.String(),
	)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/transactions/search?"+queryParams,
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var payload dto.SearchTransactionsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	assert.Equal(t, 30, payload.Limit)
	assert.Equal(t, 10, payload.Offset)
}

func TestSearchTransactionsHandler_InvalidContext(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get("/v1/imports/contexts/:contextId/transactions/search", handlers.SearchTransactions)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/not-a-uuid/transactions/search",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestSearchTransactionsHandler_InvalidAmountMin(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get("/v1/imports/contexts/:contextId/transactions/search", handlers.SearchTransactions)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/transactions/search?amount_min=abc",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireErrorResponse(t, resp, 400, "invalid_request", "invalid search parameters")
}

func TestSearchTransactionsHandler_InvalidAmountMax(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get("/v1/imports/contexts/:contextId/transactions/search", handlers.SearchTransactions)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/transactions/search?amount_max=xyz",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireErrorResponse(t, resp, 400, "invalid_request", "invalid search parameters")
}

func TestSearchTransactionsHandler_InvalidDateFrom(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get("/v1/imports/contexts/:contextId/transactions/search", handlers.SearchTransactions)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/transactions/search?date_from=2025-01-01",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestSearchTransactionsHandler_InvalidDateTo(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get("/v1/imports/contexts/:contextId/transactions/search", handlers.SearchTransactions)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/transactions/search?date_to=not-a-time",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestSearchTransactionsHandler_InvalidSourceID(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get("/v1/imports/contexts/:contextId/transactions/search", handlers.SearchTransactions)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/transactions/search?source_id=bad",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestSearchTransactionsHandler_InternalError(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	fixture.txs.EXPECT().
		SearchTransactions(gomock.Any(), contextID, gomock.Any()).
		Return(nil, int64(0), errTestGenericDBError)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get("/v1/imports/contexts/:contextId/transactions/search", handlers.SearchTransactions)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/transactions/search",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func TestSearchTransactionsHandler_EmptyResult(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	fixture.txs.EXPECT().
		SearchTransactions(gomock.Any(), contextID, gomock.Any()).
		Return([]*shared.Transaction{}, int64(0), nil)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get("/v1/imports/contexts/:contextId/transactions/search", handlers.SearchTransactions)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/transactions/search?q=nonexistent",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var payload dto.SearchTransactionsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.Empty(t, payload.Items)
	assert.Equal(t, int64(0), payload.Total)
}

// ---------------------------------------------------------------------------
// PreviewFile handler
// ---------------------------------------------------------------------------

const sampleCSV = "id,amount,currency\n1,100.50,USD\n2,200.00,EUR\n"

func TestPreviewFileHandler_SuccessCSV(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/preview", handlers.PreviewFile)

	request := newMultipartRequestWithFileContent(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/preview",
		"csv",
		"transactions.csv",
		sampleCSV,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var payload dto.FilePreviewResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))

	assert.Equal(t, []string{"id", "amount", "currency"}, payload.Columns)
	assert.Equal(t, 2, payload.RowCount)
	assert.Equal(t, "csv", payload.Format)
}

func TestPreviewFileHandler_SuccessJSON(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/preview", handlers.PreviewFile)

	jsonContent := `[{"id":"1","amount":"100.50","currency":"USD"}]`
	request := newMultipartRequestWithFileContent(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/preview",
		"json",
		"transactions.json",
		jsonContent,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var payload dto.FilePreviewResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	assert.Equal(t, "json", payload.Format)
	assert.Equal(t, 1, payload.RowCount)
}

func TestPreviewFileHandler_InvalidContextID(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/preview", handlers.PreviewFile)

	request := newMultipartRequestWithFileContent(
		t,
		"/v1/imports/contexts/bad-uuid/sources/"+uuid.NewString()+"/preview",
		"csv",
		"file.csv",
		sampleCSV,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestPreviewFileHandler_InvalidSourceID(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/preview", handlers.PreviewFile)

	request := newMultipartRequestWithFileContent(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/not-a-uuid/preview",
		"csv",
		"file.csv",
		sampleCSV,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	var errResp libHTTP.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	assert.Equal(t, "invalid source_id", errResp.Message)
}

func TestPreviewFileHandler_MissingFile(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/preview", handlers.PreviewFile)

	// Build a multipart request with no file part
	buffer := &bytes.Buffer{}
	writer := multipart.NewWriter(buffer)
	require.NoError(t, writer.WriteField("format", "csv"))
	require.NoError(t, writer.Close())

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/preview",
		buffer,
	)
	request.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestPreviewFileHandler_EmptyFile(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/preview", handlers.PreviewFile)

	request := newMultipartRequest(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/preview",
		"csv",
		0,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestPreviewFileHandler_FormatAutoDetection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filename string
		content  string
		wantFmt  string
	}{
		{
			name:     "detects csv from extension",
			filename: "data.csv",
			content:  "a,b\n1,2\n",
			wantFmt:  "csv",
		},
		{
			name:     "detects json from extension",
			filename: "data.json",
			content:  `[{"a":"1"}]`,
			wantFmt:  "json",
		},
		{
			name:     "detects xml from extension",
			filename: "data.xml",
			content:  `<root><transaction><a>1</a></transaction></root>`,
			wantFmt:  "xml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fixture := newIngestionHandlerFixture(t)
			contextID := uuid.New()
			sourceID := uuid.New()

			fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

			handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
			require.NoError(t, err)

			ctx := testTracerCtx()
			app := newFiberTestApp(ctx)
			app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/preview", handlers.PreviewFile)

			// No format field -> auto-detect from filename
			request := newMultipartRequestWithFileContent(
				t,
				"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/preview",
				"", // no format
				tt.filename,
				tt.content,
			)
			resp, err := app.Test(request)
			require.NoError(t, err)

			defer resp.Body.Close()

			require.Equal(t, fiber.StatusOK, resp.StatusCode)

			var payload dto.FilePreviewResponse
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
			assert.Equal(t, tt.wantFmt, payload.Format)
		})
	}
}

func TestPreviewFileHandler_UnknownExtensionNoFormat(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/preview", handlers.PreviewFile)

	// No format + unknown extension => error
	request := newMultipartRequestWithFileContent(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/preview",
		"",
		"data.txt",
		"some data",
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestPreviewFileHandler_MaxRowsFromForm(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/preview", handlers.PreviewFile)

	// CSV with 5 data rows, but request max_rows=2
	csvData := "id,amount\n1,10\n2,20\n3,30\n4,40\n5,50\n"
	request := newMultipartRequestWithFormField(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/preview",
		"csv",
		"data.csv",
		csvData,
		"max_rows",
		"2",
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var payload dto.FilePreviewResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	assert.Equal(t, 2, payload.RowCount)
}

func TestPreviewFileHandler_PreviewEmptyJSON(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/preview", handlers.PreviewFile)

	// Empty JSON array → triggers ErrPreviewEmptyFile
	request := newMultipartRequestWithFileContent(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/preview",
		"json",
		"empty.json",
		"[]",
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	// Empty JSON array has no columns → handlePreviewError returns 400
	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestPreviewFileHandler_GenericInternalError(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/preview", handlers.PreviewFile)

	// Send invalid JSON content with json format to trigger a parse error
	request := newMultipartRequestWithFileContent(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/preview",
		"json",
		"bad.json",
		"this is not json at all {{{}",
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	// The preview parser returns a wrapped error for bad JSON tokens,
	// which the handler maps through handlePreviewError to 500.
	require.True(t,
		resp.StatusCode == fiber.StatusInternalServerError ||
			resp.StatusCode == fiber.StatusBadRequest,
		"expected 400 or 500, got: %d", resp.StatusCode,
	)
}

// ---------------------------------------------------------------------------
// handleIngestionError — remaining branches
// ---------------------------------------------------------------------------

func TestHandleIngestionError_EOFError(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}
	fixture.parser.result = nil
	fixture.parser.err = io.EOF

	fixture.jobs.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, job *entities.IngestionJob) (*entities.IngestionJob, error) {
			job.Status = "PROCESSING"
			return job, nil
		})
	fixture.jobs.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, job *entities.IngestionJob) (*entities.IngestionJob, error) {
			return job, nil
		}).AnyTimes()
	fixture.outbox.EXPECT().
		CreateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&outboxEntities.OutboxEvent{}, nil).AnyTimes()

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/upload", handlers.UploadFile)

	request := newMultipartRequest(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/upload",
		"csv",
		10,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	// EOF from parser should map to 400 (file is empty or has no content)
	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestHandleIngestionError_EmptyFileFromParser(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}
	// Parser returns empty result (no transactions) -> command UC returns ErrEmptyFile
	fixture.parser.result = &ports.ParseResult{
		Transactions: []*shared.Transaction{},
	}
	fixture.parser.err = nil

	fixture.jobs.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, job *entities.IngestionJob) (*entities.IngestionJob, error) {
			job.Status = "PROCESSING"
			return job, nil
		})
	fixture.jobs.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, job *entities.IngestionJob) (*entities.IngestionJob, error) {
			return job, nil
		}).AnyTimes()
	fixture.outbox.EXPECT().
		CreateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&outboxEntities.OutboxEvent{}, nil).AnyTimes()

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/upload", handlers.UploadFile)

	request := newMultipartRequest(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/upload",
		"csv",
		10,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	// Empty parse result should result in 400
	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// handleIgnoreTransactionError — internal error branch
// ---------------------------------------------------------------------------

func TestIgnoreTransactionHandler_InternalError(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	txID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	// Mock the transaction repository to return an unmatched transaction
	fixture.txs.EXPECT().FindByID(gomock.Any(), txID).Return(&shared.Transaction{
		ID:       txID,
		Status:   shared.TransactionStatusUnmatched,
		Amount:   decimal.NewFromFloat(50.00),
		Currency: "USD",
	}, nil)

	// Mock the update call to fail with a generic error
	fixture.txs.EXPECT().
		UpdateStatus(gomock.Any(), txID, contextID, shared.TransactionStatusIgnored).
		Return(nil, errTestGenericDBError)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Post(
		"/v1/imports/contexts/:contextId/transactions/:transactionId/ignore",
		handlers.IgnoreTransaction,
	)

	reqBody := `{"reason": "test reason"}`
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/imports/contexts/"+contextID.String()+"/transactions/"+txID.String()+"/ignore",
		bytes.NewBufferString(reqBody),
	)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// handleContextVerificationError — tenant-related branches
// ---------------------------------------------------------------------------

func TestHandleContextVerificationError_InvalidTenantID(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	// Create an app with an invalid (non-UUID) tenant ID in context
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := libCommons.ContextWithTracer(
			context.Background(),
			noop.NewTracerProvider().Tracer("test"),
		)
		// Set an invalid tenant ID that cannot be parsed as UUID
		ctx = context.WithValue(ctx, auth.TenantIDKey, "not-a-valid-uuid")
		c.SetUserContext(ctx)

		return c.Next()
	})
	app.Get("/v1/imports/contexts/:contextId/jobs", handlers.ListJobsByContext)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+uuid.NewString()+"/jobs",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	// Invalid tenant ID → unauthorized
	require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// UploadFile — missing format field
// ---------------------------------------------------------------------------

func TestUploadFileHandler_MissingFormat(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/upload", handlers.UploadFile)

	// No format field
	request := newMultipartRequest(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/upload",
		"",
		10,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireErrorResponse(t, resp, 400, "invalid_request", "format is required")
}

// ---------------------------------------------------------------------------
// UploadFile — empty file (zero bytes)
// ---------------------------------------------------------------------------

func TestUploadFileHandler_ZeroByteFile(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/upload", handlers.UploadFile)

	request := newMultipartRequest(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/upload",
		"csv",
		0,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireErrorResponse(t, resp, 400, "invalid_request", "file is empty")
}

// ---------------------------------------------------------------------------
// ListJobsByContext — with cursor pagination returning invalid cursor error
// ---------------------------------------------------------------------------

func TestListJobsByContextHandler_InvalidCursorError(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	fixture.jobs.EXPECT().
		FindByContextID(gomock.Any(), contextID, gomock.Any()).
		Return(nil, libHTTP.CursorPagination{}, libHTTP.ErrInvalidCursor)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get("/v1/imports/contexts/:contextId/jobs", handlers.ListJobsByContext)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/jobs?cursor=bad-cursor",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireErrorResponse(t, resp, 400, "invalid_request", "invalid pagination parameters")
}

// ---------------------------------------------------------------------------
// ListTransactionsByJob — with cursor error
// ---------------------------------------------------------------------------

func TestListTransactionsByJobHandler_InvalidCursorError(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	jobID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	fixture.jobs.EXPECT().
		FindByID(gomock.Any(), jobID).
		Return(&entities.IngestionJob{ID: jobID, ContextID: contextID}, nil)
	fixture.txs.EXPECT().
		FindByJobAndContextID(gomock.Any(), jobID, contextID, gomock.Any()).
		Return(nil, libHTTP.CursorPagination{}, libHTTP.ErrInvalidCursor)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get(
		"/v1/imports/contexts/:contextId/jobs/:jobId/transactions",
		handlers.ListTransactionsByJob,
	)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/jobs/"+jobID.String()+"/transactions?cursor=bad",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireErrorResponse(t, resp, 400, "invalid_request", "invalid pagination parameters")
}

// ---------------------------------------------------------------------------
// ListJobsByContext — with sort_order=asc (valid non-default)
// ---------------------------------------------------------------------------

func TestListJobsByContextHandler_AscSortOrder(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	fixture.jobs.EXPECT().
		FindByContextID(gomock.Any(), contextID, gomock.Any()).
		Return([]*entities.IngestionJob{}, libHTTP.CursorPagination{}, nil)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get("/v1/imports/contexts/:contextId/jobs", handlers.ListJobsByContext)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/jobs?sort_order=asc&sort_by=created_at",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// ListTransactionsByJob — with sort_order=asc
// ---------------------------------------------------------------------------

func TestListTransactionsByJobHandler_AscSortOrder(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	jobID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	fixture.jobs.EXPECT().
		FindByID(gomock.Any(), jobID).
		Return(&entities.IngestionJob{ID: jobID, ContextID: contextID}, nil)
	fixture.txs.EXPECT().
		FindByJobAndContextID(gomock.Any(), jobID, contextID, gomock.Any()).
		Return([]*shared.Transaction{}, libHTTP.CursorPagination{}, nil)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get(
		"/v1/imports/contexts/:contextId/jobs/:jobId/transactions",
		handlers.ListTransactionsByJob,
	)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/jobs/"+jobID.String()+"/transactions?sort_order=asc",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// GetJob — ErrJobNotFound (non-sql.ErrNoRows path)
// ---------------------------------------------------------------------------

func TestGetJobHandler_QueryErrJobNotFound(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	jobID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	// Return nil job (which causes query UC to return ErrJobNotFound)
	fixture.jobs.EXPECT().FindByID(gomock.Any(), jobID).Return(nil, nil)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get("/v1/imports/contexts/:contextId/jobs/:jobId", handlers.GetJob)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/jobs/"+jobID.String(),
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// ListTransactionsByJob — internal error from GetJobByContext
// ---------------------------------------------------------------------------

func TestListTransactionsByJobHandler_GetJobInternalError(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	jobID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	fixture.jobs.EXPECT().FindByID(gomock.Any(), jobID).Return(nil, errTestGenericDBError)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get(
		"/v1/imports/contexts/:contextId/jobs/:jobId/transactions",
		handlers.ListTransactionsByJob,
	)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/jobs/"+jobID.String()+"/transactions",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// ListTransactionsByJob — with query.ErrJobNotFound (not sql.ErrNoRows)
// ---------------------------------------------------------------------------

func TestListTransactionsByJobHandler_JobNotFoundByQuery(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	jobID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	// Return nil job → query UC wraps to ErrJobNotFound
	fixture.jobs.EXPECT().FindByID(gomock.Any(), jobID).Return(nil, nil)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get(
		"/v1/imports/contexts/:contextId/jobs/:jobId/transactions",
		handlers.ListTransactionsByJob,
	)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/jobs/"+jobID.String()+"/transactions",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// ListJobsByContext — pagination with HasMore true
// ---------------------------------------------------------------------------

func TestListJobsByContextHandler_WithPagination(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	job := &entities.IngestionJob{ID: uuid.New(), ContextID: contextID, Status: "COMPLETED"}
	fixture.jobs.EXPECT().
		FindByContextID(gomock.Any(), contextID, gomock.Any()).
		Return([]*entities.IngestionJob{job}, libHTTP.CursorPagination{
			Next: "next-cursor-token",
		}, nil)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get("/v1/imports/contexts/:contextId/jobs", handlers.ListJobsByContext)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/jobs?limit=1",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var payload struct {
		Items      []map[string]any `json:"items"`
		NextCursor string           `json:"nextCursor"`
		HasMore    bool             `json:"hasMore"`
		Limit      int              `json:"limit"`
	}

	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	assert.True(t, payload.HasMore)
	assert.Equal(t, "next-cursor-token", payload.NextCursor)
	assert.Equal(t, 1, payload.Limit)
}

// ---------------------------------------------------------------------------
// Infrastructure provider error returns 500 (not 403)
// ---------------------------------------------------------------------------

func TestProviderError_ReturnsInternalServerError(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	// Context provider returns a generic infrastructure error.
	// The verifier wraps it without ErrContextAccessDenied, the lib classifier
	// maps it to ErrContextLookupFailed, and the handler returns 500.
	fixture.contextProvider.info = nil
	fixture.contextProvider.err = fmt.Errorf("unexpected provider error")

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get("/v1/imports/contexts/:contextId/jobs", handlers.ListJobsByContext)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/jobs",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// SearchTransactions — forbidden cross-tenant access
// ---------------------------------------------------------------------------

func TestSearchTransactionsHandler_ForbiddenCrossTenant(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = nil
	fixture.contextProvider.err = libHTTP.ErrContextNotOwned

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Get("/v1/imports/contexts/:contextId/transactions/search", handlers.SearchTransactions)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/transactions/search",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusForbidden, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// PreviewFile — forbidden cross-tenant access
// ---------------------------------------------------------------------------

func TestPreviewFileHandler_ForbiddenCrossTenant(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	fixture.contextProvider.info = nil
	fixture.contextProvider.err = libHTTP.ErrContextNotOwned

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/preview", handlers.PreviewFile)

	request := newMultipartRequestWithFileContent(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/preview",
		"csv",
		"file.csv",
		sampleCSV,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusForbidden, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// IgnoreTransaction — forbidden cross-tenant access
// ---------------------------------------------------------------------------

func TestIgnoreTransactionHandler_ForbiddenCrossTenant(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	txID := uuid.New()

	fixture.contextProvider.info = nil
	fixture.contextProvider.err = libHTTP.ErrContextNotOwned

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Post(
		"/v1/imports/contexts/:contextId/transactions/:transactionId/ignore",
		handlers.IgnoreTransaction,
	)

	reqBody := `{"reason": "test"}`
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/imports/contexts/"+contextID.String()+"/transactions/"+txID.String()+"/ignore",
		bytes.NewBufferString(reqBody),
	)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusForbidden, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// IgnoreTransaction — invalid context (not active)
// ---------------------------------------------------------------------------

func TestIgnoreTransactionHandler_ContextNotActive(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	txID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: false}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider)
	require.NoError(t, err)

	ctx := testTracerCtx()
	app := newFiberTestApp(ctx)
	app.Post(
		"/v1/imports/contexts/:contextId/transactions/:transactionId/ignore",
		handlers.IgnoreTransaction,
	)

	reqBody := `{"reason": "test"}`
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/imports/contexts/"+contextID.String()+"/transactions/"+txID.String()+"/ignore",
		bytes.NewBufferString(reqBody),
	)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusForbidden, resp.StatusCode)
}
