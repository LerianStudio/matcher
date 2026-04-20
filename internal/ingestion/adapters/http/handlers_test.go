//go:build unit

package http

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/mock/gomock"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	ingestionRepos "github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	ingestionRepoMock "github.com/LerianStudio/matcher/internal/ingestion/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	"github.com/LerianStudio/matcher/internal/ingestion/services/command"
	"github.com/LerianStudio/matcher/internal/ingestion/services/query"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	outboxMocks "github.com/LerianStudio/matcher/internal/shared/ports/mocks"
	"github.com/LerianStudio/matcher/internal/shared/testutil"
	"github.com/LerianStudio/matcher/pkg/constant"
)

var (
	errTestDatabaseConnectionFailed = errors.New("database connection failed")
	errTestDatabaseTimeout          = errors.New("database timeout")
	errTestInternalDBError          = errors.New("internal DB error")
)

type stubParser struct {
	result *ports.ParseResult
	err    error
}

func (p *stubParser) Parse(
	_ context.Context,
	_ io.Reader,
	_ *entities.IngestionJob,
	_ *shared.FieldMap,
) (*ports.ParseResult, error) {
	return p.result, p.err
}

func (p *stubParser) SupportedFormat() string {
	return "csv"
}

type stubParserRegistry struct {
	parser ports.Parser
	err    error
}

func (r *stubParserRegistry) GetParser(_ string) (ports.Parser, error) {
	if r.err != nil {
		return nil, r.err
	}

	return r.parser, nil
}

func (r *stubParserRegistry) Register(_ ports.Parser) {}

type stubDedupe struct {
	markErr error
}

func (d *stubDedupe) CalculateHash(sourceID uuid.UUID, externalID string) string {
	return sourceID.String() + ":" + externalID
}

func (d *stubDedupe) IsDuplicate(_ context.Context, _ uuid.UUID, _ string) (bool, error) {
	return false, nil
}

func (d *stubDedupe) MarkSeen(_ context.Context, _ uuid.UUID, _ string, _ time.Duration) error {
	return nil
}

func (d *stubDedupe) MarkSeenWithRetry(
	_ context.Context,
	_ uuid.UUID,
	_ string,
	_ time.Duration,
	_ int,
) error {
	return d.markErr
}

func (d *stubDedupe) MarkSeenBulk(
	_ context.Context,
	_ uuid.UUID,
	hashes []string,
	_ time.Duration,
) (map[string]bool, error) {
	if d.markErr != nil {
		return nil, d.markErr
	}

	result := make(map[string]bool, len(hashes))
	for _, h := range hashes {
		result[h] = true
	}

	return result, nil
}

func (d *stubDedupe) Clear(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

func (d *stubDedupe) ClearBatch(_ context.Context, _ uuid.UUID, _ []string) error {
	return nil
}

type stubPublisher struct{}

func (p *stubPublisher) PublishIngestionCompleted(
	_ context.Context,
	_ *entities.IngestionCompletedEvent,
) error {
	return nil
}

func (p *stubPublisher) PublishIngestionFailed(
	_ context.Context,
	_ *entities.IngestionFailedEvent,
) error {
	return nil
}

type stubFieldMapRepo struct {
	fieldMap *shared.FieldMap
	err      error
}

func (r *stubFieldMapRepo) FindBySourceID(
	_ context.Context,
	_ uuid.UUID,
) (*shared.FieldMap, error) {
	return r.fieldMap, r.err
}

type stubSourceRepo struct {
	source *shared.ReconciliationSource
	err    error
}

func (r *stubSourceRepo) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*shared.ReconciliationSource, error) {
	return r.source, r.err
}

type stubContextProvider struct {
	info              *ReconciliationContextInfo
	err               error
	receivedContextID uuid.UUID
	mu                sync.Mutex
}

func (prov *stubContextProvider) FindByID(
	_ context.Context,
	contextID uuid.UUID,
) (*ReconciliationContextInfo, error) {
	prov.mu.Lock()
	prov.receivedContextID = contextID
	prov.mu.Unlock()

	return prov.info, prov.err
}

type jobRepoAdapter struct {
	base *ingestionRepoMock.MockJobRepository
}

func (a *jobRepoAdapter) Create(
	ctx context.Context,
	job *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	result, err := a.base.Create(ctx, job)
	if err != nil {
		return nil, fmt.Errorf("mock job create: %w", err)
	}

	return result, nil
}

func (a *jobRepoAdapter) FindByID(
	ctx context.Context,
	id uuid.UUID,
) (*entities.IngestionJob, error) {
	result, err := a.base.FindByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("mock job find by id: %w", err)
	}

	return result, nil
}

func (a *jobRepoAdapter) FindByContextID(
	ctx context.Context,
	contextID uuid.UUID,
	filter ingestionRepos.CursorFilter,
) ([]*entities.IngestionJob, libHTTP.CursorPagination, error) {
	result, pagination, err := a.base.FindByContextID(ctx, contextID, filter)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, fmt.Errorf("mock job find by context id: %w", err)
	}

	return result, pagination, nil
}

func (a *jobRepoAdapter) Update(
	ctx context.Context,
	job *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	result, err := a.base.Update(ctx, job)
	if err != nil {
		return nil, fmt.Errorf("mock job update: %w", err)
	}

	return result, nil
}

func (a *jobRepoAdapter) FindLatestByExtractionID(
	_ context.Context,
	_ uuid.UUID,
) (*entities.IngestionJob, error) {
	return nil, nil
}

func (a *jobRepoAdapter) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	return fn(nil)
}

func (a *jobRepoAdapter) UpdateWithTx(
	ctx context.Context,
	_ *sql.Tx,
	job *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	result, err := a.base.Update(ctx, job)
	if err != nil {
		return nil, fmt.Errorf("mock job update with tx: %w", err)
	}

	return result, nil
}

type outboxRepoAdapter struct {
	base *outboxMocks.MockOutboxRepository
}

type transactionRepoAdapter struct {
	*ingestionRepoMock.MockTransactionRepository
}

func (a *transactionRepoAdapter) CleanupFailedJobTransactionsWithTx(
	_ context.Context,
	_ *sql.Tx,
	_ uuid.UUID,
) error {
	return nil
}

func (a *outboxRepoAdapter) Create(
	ctx context.Context,
	event *shared.OutboxEvent,
) (*shared.OutboxEvent, error) {
	result, err := a.base.Create(ctx, event)
	if err != nil {
		return nil, fmt.Errorf("mock outbox create: %w", err)
	}

	return result, nil
}

func (a *outboxRepoAdapter) CreateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	event *shared.OutboxEvent,
) (*shared.OutboxEvent, error) {
	result, err := a.base.CreateWithTx(ctx, tx, event)
	if err != nil {
		return nil, fmt.Errorf("mock outbox create with tx: %w", err)
	}

	return result, nil
}

func (a *outboxRepoAdapter) ListPending(
	ctx context.Context,
	limit int,
) ([]*shared.OutboxEvent, error) {
	result, err := a.base.ListPending(ctx, limit)
	if err != nil {
		return nil, fmt.Errorf("mock outbox list pending: %w", err)
	}

	return result, nil
}

func (a *outboxRepoAdapter) ListPendingByType(
	ctx context.Context,
	eventType string,
	limit int,
) ([]*shared.OutboxEvent, error) {
	result, err := a.base.ListPendingByType(ctx, eventType, limit)
	if err != nil {
		return nil, fmt.Errorf("mock outbox list pending by type: %w", err)
	}

	return result, nil
}

func (a *outboxRepoAdapter) ListTenants(ctx context.Context) ([]string, error) {
	result, err := a.base.ListTenants(ctx)
	if err != nil {
		return nil, fmt.Errorf("mock outbox list tenants: %w", err)
	}

	return result, nil
}

func (a *outboxRepoAdapter) MarkPublished(
	ctx context.Context,
	id uuid.UUID,
	publishedAt time.Time,
) error {
	if err := a.base.MarkPublished(ctx, id, publishedAt); err != nil {
		return fmt.Errorf("mock outbox mark published: %w", err)
	}

	return nil
}

func (a *outboxRepoAdapter) MarkFailed(ctx context.Context, id uuid.UUID, errMsg string, maxAttempts int) error {
	if err := a.base.MarkFailed(ctx, id, errMsg, maxAttempts); err != nil {
		return fmt.Errorf("mock outbox mark failed: %w", err)
	}

	return nil
}

func (a *outboxRepoAdapter) GetByID(
	ctx context.Context,
	id uuid.UUID,
) (*shared.OutboxEvent, error) {
	result, err := a.base.GetByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("mock outbox get by id: %w", err)
	}

	return result, nil
}

func (a *outboxRepoAdapter) ListFailedForRetry(
	ctx context.Context,
	limit int,
	failedBefore time.Time,
	maxAttempts int,
) ([]*shared.OutboxEvent, error) {
	result, err := a.base.ListFailedForRetry(ctx, limit, failedBefore, maxAttempts)
	if err != nil {
		return nil, fmt.Errorf("mock outbox list failed for retry: %w", err)
	}

	return result, nil
}

func (a *outboxRepoAdapter) ResetForRetry(
	ctx context.Context,
	limit int,
	failedBefore time.Time,
	maxAttempts int,
) ([]*shared.OutboxEvent, error) {
	result, err := a.base.ResetForRetry(ctx, limit, failedBefore, maxAttempts)
	if err != nil {
		return nil, fmt.Errorf("mock outbox reset for retry: %w", err)
	}

	return result, nil
}

func (a *outboxRepoAdapter) ResetStuckProcessing(
	ctx context.Context,
	limit int,
	processingBefore time.Time,
	maxAttempts int,
) ([]*shared.OutboxEvent, error) {
	result, err := a.base.ResetStuckProcessing(ctx, limit, processingBefore, maxAttempts)
	if err != nil {
		return nil, fmt.Errorf("mock outbox reset stuck processing: %w", err)
	}

	return result, nil
}

func (a *outboxRepoAdapter) MarkInvalid(ctx context.Context, id uuid.UUID, errMsg string) error {
	if err := a.base.MarkInvalid(ctx, id, errMsg); err != nil {
		return fmt.Errorf("mock outbox mark invalid: %w", err)
	}

	return nil
}

type ingestionHandlerFixture struct {
	parser          *stubParser
	commandUC       *command.UseCase
	queryUC         *query.UseCase
	contextProvider *stubContextProvider
	jobs            *ingestionRepoMock.MockJobRepository
	txs             *ingestionRepoMock.MockTransactionRepository
	outbox          *outboxMocks.MockOutboxRepository
}

func newIngestionHandlerFixture(t *testing.T) *ingestionHandlerFixture {
	t.Helper()

	controller := gomock.NewController(t)
	jobs := ingestionRepoMock.NewMockJobRepository(controller)
	txs := ingestionRepoMock.NewMockTransactionRepository(controller)
	outbox := outboxMocks.NewMockOutboxRepository(controller)

	parser := &stubParser{result: &ports.ParseResult{}}
	parserRegistry := &stubParserRegistry{parser: parser}
	jobAdapter := &jobRepoAdapter{base: jobs}
	txAdapter := &transactionRepoAdapter{MockTransactionRepository: txs}
	outboxAdapter := &outboxRepoAdapter{base: outbox}
	ctxProvider := &stubContextProvider{
		info: &ReconciliationContextInfo{ID: uuid.New(), Active: true},
	}

	commandUC, err := command.NewUseCase(command.UseCaseDeps{
		JobRepo:         jobAdapter,
		TransactionRepo: txAdapter,
		Dedupe:          &stubDedupe{},
		Publisher:       &stubPublisher{},
		OutboxRepo:      outboxAdapter,
		Parsers:         parserRegistry,
		FieldMapRepo:    &stubFieldMapRepo{fieldMap: &shared.FieldMap{ID: uuid.New()}},
		SourceRepo:      &stubSourceRepo{source: &shared.ReconciliationSource{ID: uuid.New()}},
	})
	require.NoError(t, err)

	queryUC, err := query.NewUseCase(jobAdapter, txAdapter)
	require.NoError(t, err)

	return &ingestionHandlerFixture{
		parser:          parser,
		commandUC:       commandUC,
		queryUC:         queryUC,
		contextProvider: ctxProvider,
		jobs:            jobs,
		txs:             txs,
		outbox:          outbox,
	}
}

func newFiberTestApp(ctx context.Context) *fiber.App {
	return newFiberTestAppWithTenant(ctx, "11111111-1111-1111-1111-111111111111")
}

func newFiberTestAppWithTenant(ctx context.Context, tenantID string) *fiber.App {
	app := fiber.New(fiber.Config{
		BodyLimit: 110 * 1024 * 1024, // 110MB to allow handler to validate file size
	})
	app.Use(func(c *fiber.Ctx) error {
		testCtx := context.WithValue(ctx, auth.TenantIDKey, tenantID)
		c.SetUserContext(testCtx)

		return c.Next()
	})

	return app
}

func newMultipartRequest(t *testing.T, path, format string, fileSize int64) *http.Request {
	t.Helper()

	buffer := &bytes.Buffer{}

	writer := multipart.NewWriter(buffer)
	if format != "" {
		require.NoError(t, writer.WriteField("format", format))
	}

	if fileSize >= 0 {
		fileWriter, err := writer.CreateFormFile("file", "file.csv")
		require.NoError(t, err)

		if fileSize > 0 {
			const chunkSize = 32 * 1024

			payload := bytes.Repeat([]byte("a"), chunkSize)
			remaining := fileSize

			for remaining > 0 {
				writeSize := int64(len(payload))

				if remaining < writeSize {
					writeSize = remaining
				}

				_, err = fileWriter.Write(payload[:int(writeSize)])

				require.NoError(t, err)

				remaining -= writeSize
			}
		}
	}

	require.NoError(t, writer.Close())

	request := httptest.NewRequest(http.MethodPost, path, buffer)
	request.Header.Set("Content-Type", writer.FormDataContentType())

	return request
}

func requireErrorResponse(
	t *testing.T,
	response *http.Response,
	expectedCode int, expectedTitle, expectedMessage string,
) {
	t.Helper()

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&errResp))
	require.Equal(t, expectedIngestionCode(expectedTitle, expectedMessage), errResp.Code)
	require.Equal(t, http.StatusText(expectedCode), errResp.Title)
	require.Equal(t, expectedMessage, errResp.Message)
}

func expectedIngestionCode(expectedTitle, expectedMessage string) string {
	switch expectedMessage {
	case "source not found":
		return constant.CodeIngestionSourceNotFound
	case "field mapping not found for source":
		return constant.CodeIngestionFieldMapNotFound
	case "job not found":
		return constant.CodeIngestionJobNotFound
	case "invalid context id":
		return constant.CodeInvalidContextID
	case "format is required":
		return constant.CodeIngestionFormatRequired
	case "file is empty", "file is empty or has no content", "file contains no data rows":
		return constant.CodeIngestionEmptyFile
	}

	switch expectedTitle {
	case "invalid_request":
		return constant.CodeInvalidRequest
	case "not_found":
		return constant.CodeNotFound
	case "payload_too_large":
		return constant.CodeRequestEntityTooLarge
	case "ingestion_source_not_found":
		return constant.CodeIngestionSourceNotFound
	case "ingestion_field_map_not_found":
		return constant.CodeIngestionFieldMapNotFound
	case "ingestion_format_required":
		return constant.CodeIngestionFormatRequired
	case "ingestion_empty_file":
		return constant.CodeIngestionEmptyFile
	case "ingestion_job_not_found":
		return constant.CodeIngestionJobNotFound
	case "invalid_state":
		return constant.CodeIngestionInvalidState
	case "unauthorized":
		return constant.CodeUnauthorized
	case "forbidden":
		return constant.CodeForbidden
	case "context_not_active":
		return constant.CodeContextNotActive
	case "internal_server_error":
		return constant.CodeInternalServerError
	default:
		return constant.CodeInternalServerError
	}
}

func TestNewHandlersValidation(t *testing.T) {
	t.Parallel()

	ctxProv := &stubContextProvider{info: &ReconciliationContextInfo{ID: uuid.New()}}

	_, err := NewHandlers(nil, nil, ctxProv, false)
	require.ErrorIs(t, err, ErrNilCommandUseCase)

	fixture := newIngestionHandlerFixture(t)
	_, err = NewHandlers(fixture.commandUC, nil, ctxProv, false)
	require.ErrorIs(t, err, ErrNilQueryUseCase)

	_, err = NewHandlers(fixture.commandUC, fixture.queryUC, nil, false)
	require.ErrorIs(t, err, ErrNilContextProvider)
}

func TestUploadFileValidatesInput(t *testing.T) {
	t.Parallel()

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)

	t.Run("invalid context", func(t *testing.T) {
		t.Parallel()

		fixture := newIngestionHandlerFixture(t)
		handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
		require.NoError(t, err)

		app := newFiberTestApp(ctx)
		app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/upload", handlers.UploadFile)

		request := newMultipartRequest(
			t,
			"/v1/imports/contexts/not-a-uuid/sources/"+uuid.NewString()+"/upload",
			"csv",
			1,
		)
		resp, err := app.Test(request)
		require.NoError(t, err)

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireErrorResponse(t, resp, 400, "invalid_context_id", "invalid context id")
	})

	t.Run("invalid format", func(t *testing.T) {
		t.Parallel()

		fixture := newIngestionHandlerFixture(t)
		contextID := uuid.New()

		// Set up the context provider to return the correct contextID for ownership verification
		fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

		handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
		require.NoError(t, err)

		app := newFiberTestApp(ctx)
		app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/upload", handlers.UploadFile)

		request := newMultipartRequest(
			t,
			"/v1/imports/contexts/"+contextID.String()+"/sources/"+uuid.NewString()+"/upload",
			"txt",
			1,
		)
		resp, err := app.Test(request)
		require.NoError(t, err)

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireErrorResponse(
			t,
			resp,
			400,
			"invalid_request",
			"invalid format: must be one of csv, json, xml",
		)
	})

	t.Run("file too large", func(t *testing.T) {
		t.Parallel()

		fixture := newIngestionHandlerFixture(t)
		contextID := uuid.New()
		sourceID := uuid.New()

		// Set up the context provider to return the correct contextID for ownership verification
		fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

		handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
		require.NoError(t, err)

		app := newFiberTestApp(ctx)
		app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/upload", handlers.UploadFile)

		// 100MB + 1 byte
		request := newMultipartRequest(
			t,
			"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/upload",
			"csv",
			100*1024*1024+1,
		)
		resp, err := app.Test(request, -1) // disable timeout for large file test
		require.NoError(t, err)

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusRequestEntityTooLarge, resp.StatusCode)
		requireErrorResponse(t, resp, 413, "payload_too_large", "file exceeds 100MB limit")
	})
}

func TestUploadFileSuccess(t *testing.T) {
	t.Parallel()

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	// Set up the context provider to return the correct contextID for ownership verification
	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	tx := &shared.Transaction{
		SourceID:   sourceID,
		ExternalID: "ext",
		Amount:     decimal.NewFromInt(10),
		Currency:   "USD",
		Date:       time.Now(),
	}
	fixture.parser.result = &ports.ParseResult{Transactions: []*shared.Transaction{tx}}

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
		})
	fixture.txs.EXPECT().
		ExistsBulkBySourceAndExternalID(gomock.Any(), gomock.Any()).
		Return(nil, nil)
	fixture.txs.EXPECT().
		CreateBatch(gomock.Any(), gomock.Any()).
		Return([]*shared.Transaction{}, nil)
	fixture.outbox.EXPECT().
		CreateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&shared.OutboxEvent{}, nil)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/upload", handlers.UploadFile)

	request := newMultipartRequest(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/upload",
		"csv",
		1,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusAccepted, resp.StatusCode)
}

func TestGetJobResponses(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	jobID := uuid.New()

	// Set up the context provider to return the correct contextID for ownership verification
	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	job := &entities.IngestionJob{ID: jobID, ContextID: contextID, Status: "COMPLETED"}
	fixture.jobs.EXPECT().FindByID(gomock.Any(), jobID).Return(job, nil)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
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

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestListJobsByContextBadSortOrder(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	// Set up the context provider to return the correct contextID for ownership verification
	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	app := newFiberTestApp(ctx)
	app.Get("/v1/imports/contexts/:contextId/jobs", handlers.ListJobsByContext)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/jobs?sort_order=foo",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, constant.CodeInvalidRequest, errResp.Code)
	require.Equal(t, http.StatusText(http.StatusBadRequest), errResp.Title)
	require.Equal(t, "invalid sort_order: must be asc or desc", errResp.Message)
}

func TestGetJobNotFound(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	jobID := uuid.New()

	// Set up the context provider to return the correct contextID for ownership verification
	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	fixture.jobs.EXPECT().FindByID(gomock.Any(), jobID).Return(nil, sql.ErrNoRows)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
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

func TestListJobsByContextSuccess(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	// Set up the context provider to return the correct contextID for ownership verification
	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	job := &entities.IngestionJob{ID: uuid.New(), ContextID: contextID, Status: "COMPLETED"}
	fixture.jobs.EXPECT().
		FindByContextID(gomock.Any(), contextID, gomock.Any()).
		Return([]*entities.IngestionJob{job}, libHTTP.CursorPagination{}, nil)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
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

	var payload struct {
		Items      []map[string]any `json:"items"`
		NextCursor string           `json:"nextCursor"`
		PrevCursor string           `json:"prevCursor"`
		Limit      int              `json:"limit"`
		HasMore    bool             `json:"hasMore"`
	}

	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	require.Len(t, payload.Items, 1)
	require.Equal(t, constants.DefaultPaginationLimit, payload.Limit)
	require.Empty(t, payload.NextCursor)
	require.Empty(t, payload.PrevCursor)
}

func TestListTransactionsByJobBadSortOrder(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	jobID := uuid.New()

	// Set up the context provider to return the correct contextID for ownership verification
	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	app := newFiberTestApp(ctx)
	app.Get(
		"/v1/imports/contexts/:contextId/jobs/:jobId/transactions",
		handlers.ListTransactionsByJob,
	)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/jobs/"+jobID.String()+"/transactions?sort_order=foo",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, constant.CodeInvalidRequest, errResp.Code)
	require.Equal(t, http.StatusText(http.StatusBadRequest), errResp.Title)
	require.Equal(t, "invalid sort_order: must be asc or desc", errResp.Message)
}

func TestListTransactionsByJobSuccess(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	jobID := uuid.New()

	// Set up the context provider to return the correct contextID for ownership verification
	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	fixture.jobs.EXPECT().
		FindByID(gomock.Any(), jobID).
		Return(&entities.IngestionJob{ID: jobID, ContextID: contextID}, nil)
	fixture.txs.EXPECT().
		FindByJobAndContextID(gomock.Any(), jobID, contextID, gomock.Any()).
		Return([]*shared.Transaction{
			{
				ID:               uuid.New(),
				SourceID:         uuid.New(),
				ExternalID:       "ext",
				Amount:           decimal.NewFromInt(10),
				Currency:         "USD",
				Status:           shared.TransactionStatusMatched,
				ExtractionStatus: shared.ExtractionStatusComplete,
			},
		}, libHTTP.CursorPagination{}, nil)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
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

	var payload struct {
		Items      []map[string]any `json:"items"`
		NextCursor string           `json:"nextCursor"`
		PrevCursor string           `json:"prevCursor"`
		Limit      int              `json:"limit"`
		HasMore    bool             `json:"hasMore"`
	}

	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	require.Len(t, payload.Items, 1)
	require.Equal(t, constants.DefaultPaginationLimit, payload.Limit)
	require.Empty(t, payload.NextCursor)
	require.Empty(t, payload.PrevCursor)
}

func TestListJobsByContextContextNotActive(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: false}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
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

	require.Equal(t, fiber.StatusForbidden, resp.StatusCode)

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))

	require.Equal(t, constant.CodeContextNotActive, errResp.Code)
	require.Equal(t, http.StatusText(http.StatusForbidden), errResp.Title)
	require.Equal(t, "context is not active", errResp.Message)
}

func TestListJobsByContextContextNotFound(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = nil
	fixture.contextProvider.err = nil

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
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

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireErrorResponse(t, resp, 404, "not_found", "context not found")
}

func TestGetJobForbiddenCrossTenantAccess(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	jobID := uuid.New()

	// Simulate cross-tenant access: context exists but belongs to different tenant
	fixture.contextProvider.info = nil
	fixture.contextProvider.err = libHTTP.ErrContextNotOwned

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
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

	require.Equal(t, fiber.StatusForbidden, resp.StatusCode)
	require.Equal(t, contextID, fixture.contextProvider.receivedContextID)
}

func TestUploadFileForbiddenCrossTenantAccess(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	// Simulate cross-tenant access: context exists but belongs to different tenant
	fixture.contextProvider.info = nil
	fixture.contextProvider.err = libHTTP.ErrContextNotOwned

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/upload", handlers.UploadFile)

	request := newMultipartRequest(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/upload",
		"csv",
		1,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusForbidden, resp.StatusCode)
	require.Equal(t, contextID, fixture.contextProvider.receivedContextID)
}

func TestListJobsByContextForbiddenCrossTenantAccess(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	// Simulate cross-tenant access: context exists but belongs to different tenant
	fixture.contextProvider.info = nil
	fixture.contextProvider.err = libHTTP.ErrContextNotOwned

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
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

	require.Equal(t, fiber.StatusForbidden, resp.StatusCode)
	require.Equal(t, contextID, fixture.contextProvider.receivedContextID)
}

func TestListJobsByContextInternalError(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}
	fixture.jobs.EXPECT().
		FindByContextID(gomock.Any(), contextID, gomock.Any()).
		Return(nil, libHTTP.CursorPagination{}, errTestDatabaseConnectionFailed)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
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

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	require.Equal(t, constant.CodeInternalServerError, errResp.Code)
	require.Equal(t, http.StatusText(http.StatusInternalServerError), errResp.Title)
}

func TestListTransactionsByJobInternalError(t *testing.T) {
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
		Return(nil, libHTTP.CursorPagination{}, errTestDatabaseTimeout)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
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

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	require.Equal(t, constant.CodeInternalServerError, errResp.Code)
	require.Equal(t, http.StatusText(http.StatusInternalServerError), errResp.Title)
}

func TestListTransactionsByJobNotFound(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	jobID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}
	fixture.jobs.EXPECT().FindByID(gomock.Any(), jobID).Return(nil, sql.ErrNoRows)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
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

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	require.Equal(t, constant.CodeIngestionJobNotFound, errResp.Code)
	require.Equal(t, http.StatusText(http.StatusNotFound), errResp.Title)
	require.Equal(t, "job not found", errResp.Message)
}

func TestListTransactionsByJobInvalidJobID(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	app := newFiberTestApp(ctx)
	app.Get(
		"/v1/imports/contexts/:contextId/jobs/:jobId/transactions",
		handlers.ListTransactionsByJob,
	)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/jobs/not-a-uuid/transactions",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	require.Equal(t, constant.CodeInvalidRequest, errResp.Code)
	require.Equal(t, "invalid job_id", errResp.Message)
}

func TestGetJobInternalError(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	jobID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}
	fixture.jobs.EXPECT().FindByID(gomock.Any(), jobID).Return(nil, errTestInternalDBError)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
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

	require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func TestGetJobInvalidJobID(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	app := newFiberTestApp(ctx)
	app.Get("/v1/imports/contexts/:contextId/jobs/:jobId", handlers.GetJob)

	resp, err := app.Test(
		httptest.NewRequest(
			http.MethodGet,
			"/v1/imports/contexts/"+contextID.String()+"/jobs/invalid-uuid",
			http.NoBody,
		),
	)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	require.Equal(t, "invalid job_id", errResp.Message)
}

func TestConcurrentUploads(t *testing.T) {
	t.Parallel()

	// Test that concurrent validation requests don't cause panics or race conditions
	// We test with invalid requests that fail validation (before hitting mocks)
	contextID := uuid.New()
	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		testCtx := context.WithValue(ctx, auth.TenantIDKey, "11111111-1111-1111-1111-111111111111")

		c.SetUserContext(testCtx)

		return c.Next()
	})

	// Use a stub context provider that returns not found to test concurrency at validation level
	stubProvider := &stubContextProvider{info: nil, err: nil} // Context not found

	handlers, err := NewHandlers(
		&command.UseCase{},
		&query.UseCase{},
		stubProvider,
		false,
	)

	require.NoError(t, err)

	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/upload", handlers.UploadFile)

	const numConcurrent = 10

	results := make(chan int, numConcurrent)
	sourceID := uuid.New()

	for i := 0; i < numConcurrent; i++ {
		request := newMultipartRequest(
			t,
			"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/upload",
			"csv",
			10,
		)

		go func() {
			resp, respErr := app.Test(request, -1)
			if respErr != nil {
				results <- 0

				return
			}
			defer resp.Body.Close()

			results <- resp.StatusCode
		}()
	}

	for i := 0; i < numConcurrent; i++ {
		statusCode := <-results
		// All requests should complete with 404 (context not found) - not panic
		require.Equal(
			t,
			fiber.StatusNotFound,
			statusCode,
			"Expected 404 for context not found, got: %d",
			statusCode,
		)
	}
}

func TestUploadFileInvalidSourceID(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/upload", handlers.UploadFile)

	request := newMultipartRequest(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/not-a-uuid/upload",
		"csv",
		10,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	require.Equal(t, "invalid source_id", errResp.Message)
}

func TestUploadFileInvalidContextID(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/upload", handlers.UploadFile)

	sourceID := uuid.New()
	request := newMultipartRequest(
		t,
		"/v1/imports/contexts/not-a-uuid/sources/"+sourceID.String()+"/upload",
		"csv",
		10,
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestIgnoreTransactionHandler_InvalidTransactionID(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	app := newFiberTestApp(ctx)
	app.Post(
		"/v1/imports/contexts/:contextId/transactions/:transactionId/ignore",
		handlers.IgnoreTransaction,
	)

	reqBody := `{"reason": "duplicate entry"}`
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/imports/contexts/"+contextID.String()+"/transactions/not-a-uuid/ignore",
		bytes.NewBufferString(reqBody),
	)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	require.Equal(t, "invalid transaction_id", errResp.Message)
}

func TestIgnoreTransactionHandler_InvalidRequestBody(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	txID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	app := newFiberTestApp(ctx)
	app.Post(
		"/v1/imports/contexts/:contextId/transactions/:transactionId/ignore",
		handlers.IgnoreTransaction,
	)

	// Invalid JSON
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/imports/contexts/"+contextID.String()+"/transactions/"+txID.String()+"/ignore",
		bytes.NewBufferString("not valid json"),
	)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestIgnoreTransactionHandler_TransactionNotFound(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	txID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	// Mock the transaction repository to return not found
	fixture.txs.EXPECT().FindByID(gomock.Any(), txID).Return(nil, sql.ErrNoRows)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	app := newFiberTestApp(ctx)
	app.Post(
		"/v1/imports/contexts/:contextId/transactions/:transactionId/ignore",
		handlers.IgnoreTransaction,
	)

	reqBody := `{"reason": "duplicate entry"}`
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/imports/contexts/"+contextID.String()+"/transactions/"+txID.String()+"/ignore",
		bytes.NewBufferString(reqBody),
	)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireErrorResponse(t, resp, 404, "not_found", "transaction not found")
}

func TestIgnoreTransactionHandler_TransactionNotIgnorable(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	txID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	// Mock the transaction repository to return a matched transaction
	fixture.txs.EXPECT().FindByID(gomock.Any(), txID).Return(&shared.Transaction{
		ID:     txID,
		Status: shared.TransactionStatusMatched,
	}, nil)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	app := newFiberTestApp(ctx)
	app.Post(
		"/v1/imports/contexts/:contextId/transactions/:transactionId/ignore",
		handlers.IgnoreTransaction,
	)

	reqBody := `{"reason": "duplicate entry"}`
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/imports/contexts/"+contextID.String()+"/transactions/"+txID.String()+"/ignore",
		bytes.NewBufferString(reqBody),
	)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusConflict, resp.StatusCode)

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	require.Equal(t, http.StatusText(http.StatusConflict), errResp.Title)
	require.Contains(t, errResp.Message, "only UNMATCHED transactions can be ignored")
}

func TestIgnoreTransactionHandler_ReasonRequired(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	txID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	app := newFiberTestApp(ctx)
	app.Post(
		"/v1/imports/contexts/:contextId/transactions/:transactionId/ignore",
		handlers.IgnoreTransaction,
	)

	// Empty reason
	reqBody := `{"reason": ""}`
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/imports/contexts/"+contextID.String()+"/transactions/"+txID.String()+"/ignore",
		bytes.NewBufferString(reqBody),
	)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	require.Equal(t, "invalid request body", errResp.Message)
}

func TestIgnoreTransactionHandler_Success(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	txID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	// Mock the transaction repository to return an unmatched transaction
	fixture.txs.EXPECT().FindByID(gomock.Any(), txID).Return(&shared.Transaction{
		ID:       txID,
		Status:   shared.TransactionStatusUnmatched,
		Amount:   decimal.NewFromFloat(100.50),
		Currency: "USD",
	}, nil)

	// Mock the update call
	fixture.txs.EXPECT().
		UpdateStatus(gomock.Any(), txID, contextID, shared.TransactionStatusIgnored).
		Return(&shared.Transaction{
			ID:       txID,
			Status:   shared.TransactionStatusIgnored,
			Amount:   decimal.NewFromFloat(100.50),
			Currency: "USD",
		}, nil)

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	app := newFiberTestApp(ctx)
	app.Post(
		"/v1/imports/contexts/:contextId/transactions/:transactionId/ignore",
		handlers.IgnoreTransaction,
	)

	reqBody := `{"reason": "duplicate entry from legacy system"}`
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/imports/contexts/"+contextID.String()+"/transactions/"+txID.String()+"/ignore",
		bytes.NewBufferString(reqBody),
	)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestHandleIngestionError_SourceNotFound(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}
	fixture.contextProvider.err = nil

	// Set up source repo to return not found
	commandUC, err := command.NewUseCase(command.UseCaseDeps{
		JobRepo:         &jobRepoAdapter{base: fixture.jobs},
		TransactionRepo: &transactionRepoAdapter{MockTransactionRepository: fixture.txs},
		Dedupe:          &stubDedupe{},
		Publisher:       &stubPublisher{},
		OutboxRepo:      &outboxRepoAdapter{base: fixture.outbox},
		Parsers:         &stubParserRegistry{parser: fixture.parser},
		FieldMapRepo:    &stubFieldMapRepo{fieldMap: &shared.FieldMap{ID: uuid.New()}},
		SourceRepo:      &stubSourceRepo{source: nil, err: sql.ErrNoRows}, // Source not found
	})
	require.NoError(t, err)

	handlers, err := NewHandlers(commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
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

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireErrorResponse(t, resp, 404, "not_found", "source not found")
}

func TestHandleIngestionError_FieldMapNotFound(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}
	fixture.contextProvider.err = nil

	// Set up field map repo to return not found
	commandUC, err := command.NewUseCase(command.UseCaseDeps{
		JobRepo:         &jobRepoAdapter{base: fixture.jobs},
		TransactionRepo: &transactionRepoAdapter{MockTransactionRepository: fixture.txs},
		Dedupe:          &stubDedupe{},
		Publisher:       &stubPublisher{},
		OutboxRepo:      &outboxRepoAdapter{base: fixture.outbox},
		Parsers:         &stubParserRegistry{parser: fixture.parser},
		FieldMapRepo:    &stubFieldMapRepo{fieldMap: nil, err: sql.ErrNoRows}, // FieldMap not found
		SourceRepo: &stubSourceRepo{
			source: &shared.ReconciliationSource{ID: sourceID, ContextID: contextID},
		},
	})
	require.NoError(t, err)

	handlers, err := NewHandlers(commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
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

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireErrorResponse(t, resp, 404, "not_found", "field mapping not found for source")
}

func TestValidateFileContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		format      string
		wantValid   bool
	}{
		// CSV format
		{"csv with text/csv", "text/csv", "csv", true},
		{"csv with application/csv", "application/csv", "csv", true},
		{"csv with text/plain", "text/plain", "csv", true},
		{"csv with application/octet-stream", "application/octet-stream", "csv", true},
		{"csv with empty content type", "", "csv", true},
		{"csv with application/json", "application/json", "csv", false},
		{"csv with application/xml", "application/xml", "csv", false},

		// JSON format
		{"json with application/json", "application/json", "json", true},
		{"json with text/plain", "text/plain", "json", true},
		{"json with application/octet-stream", "application/octet-stream", "json", true},
		{"json with empty content type", "", "json", true},
		{"json with text/csv", "text/csv", "json", false},
		{"json with application/xml", "application/xml", "json", false},

		// XML format
		{"xml with application/xml", "application/xml", "xml", true},
		{"xml with text/xml", "text/xml", "xml", true},
		{"xml with text/plain", "text/plain", "xml", true},
		{"xml with application/octet-stream", "application/octet-stream", "xml", true},
		{"xml with empty content type", "", "xml", true},
		{"xml with text/csv", "text/csv", "xml", false},
		{"xml with application/json", "application/json", "xml", false},

		// Unknown format (should pass)
		{"unknown format", "text/csv", "unknown", true},

		// Content type with charset (should be stripped and validated)
		{"json with charset", "application/json; charset=utf-8", "json", true},
		{"csv with uppercase", "TEXT/CSV", "csv", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := validateFileContentType(tt.contentType, tt.format)
			require.Equal(t, tt.wantValid, got, "validateFileContentType(%q, %q)", tt.contentType, tt.format)
		})
	}
}

func newMultipartRequestWithContentType(
	t *testing.T,
	path, format string,
	fileSize int64,
	fileContentType string,
) *http.Request {
	t.Helper()

	buffer := &bytes.Buffer{}
	writer := multipart.NewWriter(buffer)

	if format != "" {
		require.NoError(t, writer.WriteField("format", format))
	}

	if fileSize >= 0 {
		h := make(textproto.MIMEHeader)
		h.Set("Content-Disposition", `form-data; name="file"; filename="file.csv"`)
		if fileContentType != "" {
			h.Set("Content-Type", fileContentType)
		}

		fileWriter, err := writer.CreatePart(h)
		require.NoError(t, err)

		if fileSize > 0 {
			payload := bytes.Repeat([]byte("a"), int(fileSize))
			_, err = fileWriter.Write(payload)
			require.NoError(t, err)
		}
	}

	require.NoError(t, writer.Close())

	request := httptest.NewRequest(http.MethodPost, path, buffer)
	request.Header.Set("Content-Type", writer.FormDataContentType())

	return request
}

func TestUploadFileHandler_InvalidContentType(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}

	handlers, err := NewHandlers(fixture.commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/upload", handlers.UploadFile)

	request := newMultipartRequestWithContentType(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/upload",
		"csv",
		10,
		"application/json",
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireErrorResponse(t, resp, 400, "invalid_request", "file content type does not match declared format")
}

func TestUploadFileHandler_ValidContentType(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}
	fixture.contextProvider.err = nil

	commandUC, err := command.NewUseCase(command.UseCaseDeps{
		JobRepo:         &jobRepoAdapter{base: fixture.jobs},
		TransactionRepo: &transactionRepoAdapter{MockTransactionRepository: fixture.txs},
		Dedupe:          &stubDedupe{},
		Publisher:       &stubPublisher{},
		OutboxRepo:      &outboxRepoAdapter{base: fixture.outbox},
		Parsers:         &stubParserRegistry{parser: fixture.parser},
		FieldMapRepo:    &stubFieldMapRepo{fieldMap: &shared.FieldMap{ID: uuid.New()}},
		SourceRepo:      &stubSourceRepo{source: nil, err: sql.ErrNoRows},
	})
	require.NoError(t, err)

	handlers, err := NewHandlers(commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/upload", handlers.UploadFile)

	request := newMultipartRequestWithContentType(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/upload",
		"csv",
		10,
		"text/csv",
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireErrorResponse(t, resp, 404, "not_found", "source not found")
}

func TestUploadFileHandler_EmptyContentTypePassesValidation(t *testing.T) {
	t.Parallel()

	fixture := newIngestionHandlerFixture(t)
	contextID := uuid.New()
	sourceID := uuid.New()

	fixture.contextProvider.info = &ReconciliationContextInfo{ID: contextID, Active: true}
	fixture.contextProvider.err = nil

	commandUC, err := command.NewUseCase(command.UseCaseDeps{
		JobRepo:         &jobRepoAdapter{base: fixture.jobs},
		TransactionRepo: &transactionRepoAdapter{MockTransactionRepository: fixture.txs},
		Dedupe:          &stubDedupe{},
		Publisher:       &stubPublisher{},
		OutboxRepo:      &outboxRepoAdapter{base: fixture.outbox},
		Parsers:         &stubParserRegistry{parser: fixture.parser},
		FieldMapRepo:    &stubFieldMapRepo{fieldMap: &shared.FieldMap{ID: uuid.New()}},
		SourceRepo:      &stubSourceRepo{source: nil, err: sql.ErrNoRows},
	})
	require.NoError(t, err)

	handlers, err := NewHandlers(commandUC, fixture.queryUC, fixture.contextProvider, false)
	require.NoError(t, err)

	ctx := libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
	app := newFiberTestApp(ctx)
	app.Post("/v1/imports/contexts/:contextId/sources/:sourceId/upload", handlers.UploadFile)

	request := newMultipartRequestWithContentType(
		t,
		"/v1/imports/contexts/"+contextID.String()+"/sources/"+sourceID.String()+"/upload",
		"json",
		10,
		"",
	)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireErrorResponse(t, resp, 404, "not_found", "source not found")
}

func TestLogSpanError_WithNilLogger(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")
	ctx, span := tracer.Start(context.Background(), "test")

	defer span.End()

	require.NotPanics(t, func() {
		(&Handlers{}).logSpanError(ctx, span, nil, "test message", errors.New("test error"))
	})
}

func TestLogSpanError_WithLogger(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")
	ctx, span := tracer.Start(context.Background(), "test")

	defer span.End()

	mock := &testutil.TestLogger{}
	(&Handlers{}).logSpanError(ctx, span, mock, "test message", errors.New("test error"))
	require.True(t, mock.ErrorCalled)
}
