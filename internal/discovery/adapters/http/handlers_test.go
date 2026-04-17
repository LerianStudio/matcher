//go:build unit

package http

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"

	"github.com/LerianStudio/matcher/internal/discovery/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	discoveryCommand "github.com/LerianStudio/matcher/internal/discovery/services/command"
	discoveryQuery "github.com/LerianStudio/matcher/internal/discovery/services/query"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/pkg/constant"
)

// --- Mocks ---

// mockFetcherClient implements sharedPorts.FetcherClient for testing.
type mockFetcherClient struct {
	healthy      bool
	connections  []*sharedPorts.FetcherConnection
	schema       *sharedPorts.FetcherSchema
	testResult   *sharedPorts.FetcherTestResult
	listErr      error
	schemaErr    error
	testErr      error
	submitJobID  string
	submitErr    error
	jobStatus    *sharedPorts.ExtractionJobStatus
	jobStatusErr error
}

func (m *mockFetcherClient) IsHealthy(_ context.Context) bool { return m.healthy }
func (m *mockFetcherClient) ListConnections(_ context.Context, _ string) ([]*sharedPorts.FetcherConnection, error) {
	return m.connections, m.listErr
}

func (m *mockFetcherClient) GetSchema(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
	return m.schema, m.schemaErr
}

func (m *mockFetcherClient) TestConnection(_ context.Context, _ string) (*sharedPorts.FetcherTestResult, error) {
	return m.testResult, m.testErr
}

func (m *mockFetcherClient) SubmitExtractionJob(_ context.Context, _ sharedPorts.ExtractionJobInput) (string, error) {
	return m.submitJobID, m.submitErr
}

func (m *mockFetcherClient) GetExtractionJobStatus(_ context.Context, _ string) (*sharedPorts.ExtractionJobStatus, error) {
	return m.jobStatus, m.jobStatusErr
}

// mockConnectionRepo implements repositories.ConnectionRepository.
type mockConnectionRepo struct {
	connections map[uuid.UUID]*entities.FetcherConnection
	upsertErr   error
}

func newMockConnectionRepo() *mockConnectionRepo {
	return &mockConnectionRepo{connections: make(map[uuid.UUID]*entities.FetcherConnection)}
}

func (r *mockConnectionRepo) Upsert(_ context.Context, conn *entities.FetcherConnection) error {
	if r.upsertErr != nil {
		return r.upsertErr
	}
	r.connections[conn.ID] = conn
	return nil
}

func (r *mockConnectionRepo) UpsertWithTx(_ context.Context, _ *sql.Tx, conn *entities.FetcherConnection) error {
	return r.Upsert(context.Background(), conn)
}

func (r *mockConnectionRepo) FindAll(_ context.Context) ([]*entities.FetcherConnection, error) {
	result := make([]*entities.FetcherConnection, 0, len(r.connections))
	for _, c := range r.connections {
		result = append(result, c)
	}
	return result, nil
}

// errConnectionNotFound re-exports the domain sentinel so mocks return the proper error identity.
var errConnectionNotFound = repositories.ErrConnectionNotFound

func (r *mockConnectionRepo) FindByID(_ context.Context, id uuid.UUID) (*entities.FetcherConnection, error) {
	conn, ok := r.connections[id]
	if !ok {
		return nil, errConnectionNotFound
	}
	return conn, nil
}

func (r *mockConnectionRepo) FindByFetcherID(_ context.Context, fetcherConnID string) (*entities.FetcherConnection, error) {
	for _, c := range r.connections {
		if c.FetcherConnID == fetcherConnID {
			return c, nil
		}
	}
	return nil, errConnectionNotFound
}

func (r *mockConnectionRepo) DeleteStale(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}

func (r *mockConnectionRepo) DeleteStaleWithTx(_ context.Context, _ *sql.Tx, _ time.Duration) (int64, error) {
	return 0, nil
}

// mockSchemaRepo implements repositories.SchemaRepository.
type mockSchemaRepo struct {
	schemas map[uuid.UUID][]*entities.DiscoveredSchema
}

func newMockSchemaRepo() *mockSchemaRepo {
	return &mockSchemaRepo{schemas: make(map[uuid.UUID][]*entities.DiscoveredSchema)}
}

func (r *mockSchemaRepo) UpsertBatch(_ context.Context, schemas []*entities.DiscoveredSchema) error {
	for _, s := range schemas {
		r.schemas[s.ConnectionID] = append(r.schemas[s.ConnectionID], s)
	}
	return nil
}

func (r *mockSchemaRepo) UpsertBatchWithTx(_ context.Context, _ *sql.Tx, schemas []*entities.DiscoveredSchema) error {
	return r.UpsertBatch(context.Background(), schemas)
}

func (r *mockSchemaRepo) FindByConnectionID(_ context.Context, connectionID uuid.UUID) ([]*entities.DiscoveredSchema, error) {
	return r.schemas[connectionID], nil
}

func (r *mockSchemaRepo) DeleteByConnectionID(_ context.Context, connectionID uuid.UUID) error {
	delete(r.schemas, connectionID)
	return nil
}

func (r *mockSchemaRepo) DeleteByConnectionIDWithTx(_ context.Context, _ *sql.Tx, connectionID uuid.UUID) error {
	return r.DeleteByConnectionID(context.Background(), connectionID)
}

// mockExtractionRepo implements repositories.ExtractionRepository.
type mockExtractionRepo struct {
	createCount int
	updateCount int
	createErr   error
	updateErr   error
	findByIDReq *entities.ExtractionRequest
	findByIDErr error
}

func (r *mockExtractionRepo) Create(_ context.Context, req *entities.ExtractionRequest) error {
	r.createCount++
	if r.findByIDReq == nil {
		r.findByIDReq = req
	}

	return r.createErr
}

func (r *mockExtractionRepo) CreateWithTx(_ context.Context, _ *sql.Tx, _ *entities.ExtractionRequest) error {
	return r.createErr
}

func (r *mockExtractionRepo) Update(_ context.Context, req *entities.ExtractionRequest) error {
	r.updateCount++
	r.findByIDReq = req
	return r.updateErr
}

func (r *mockExtractionRepo) UpdateIfUnchanged(_ context.Context, req *entities.ExtractionRequest, _ time.Time) error {
	r.updateCount++
	r.findByIDReq = req
	return r.updateErr
}

func (r *mockExtractionRepo) UpdateIfUnchangedWithTx(_ context.Context, _ *sql.Tx, req *entities.ExtractionRequest, _ time.Time) error {
	r.updateCount++
	r.findByIDReq = req
	return r.updateErr
}

func (r *mockExtractionRepo) UpdateWithTx(_ context.Context, _ *sql.Tx, _ *entities.ExtractionRequest) error {
	return r.updateErr
}

func (r *mockExtractionRepo) FindByID(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
	if r.findByIDErr != nil {
		return nil, r.findByIDErr
	}

	if r.findByIDReq == nil {
		return nil, repositories.ErrExtractionNotFound
	}

	return r.findByIDReq, nil
}

func (r *mockExtractionRepo) LinkIfUnlinked(_ context.Context, _ uuid.UUID, _ uuid.UUID) error {
	return nil
}

func (r *mockExtractionRepo) MarkBridgeFailed(_ context.Context, _ *entities.ExtractionRequest) error {
	return nil
}

func (r *mockExtractionRepo) MarkBridgeFailedWithTx(_ context.Context, _ sharedPorts.Tx, _ *entities.ExtractionRequest) error {
	return nil
}

func (r *mockExtractionRepo) IncrementBridgeAttempts(_ context.Context, _ uuid.UUID, _ int) error {
	return nil
}

func (r *mockExtractionRepo) IncrementBridgeAttemptsWithTx(_ context.Context, _ sharedPorts.Tx, _ uuid.UUID, _ int) error {
	return nil
}

func (r *mockExtractionRepo) FindEligibleForBridge(_ context.Context, _ int) ([]*entities.ExtractionRequest, error) {
	return nil, nil
}

func (r *mockExtractionRepo) CountBridgeReadiness(_ context.Context, _ time.Duration) (repositories.BridgeReadinessCounts, error) {
	return repositories.BridgeReadinessCounts{}, nil
}

func (r *mockExtractionRepo) ListBridgeCandidates(
	_ context.Context,
	_ string,
	_ time.Duration,
	_ time.Time,
	_ uuid.UUID,
	_ int,
) ([]*entities.ExtractionRequest, error) {
	return nil, nil
}

func (r *mockExtractionRepo) FindBridgeRetentionCandidates(
	_ context.Context,
	_ time.Duration,
	_ int,
) ([]*entities.ExtractionRequest, error) {
	return nil, nil
}

func (r *mockExtractionRepo) MarkCustodyDeleted(
	_ context.Context,
	_ uuid.UUID,
	_ time.Time,
) error {
	return nil
}

func (r *mockExtractionRepo) MarkCustodyDeletedWithTx(
	_ context.Context,
	_ sharedPorts.Tx,
	_ uuid.UUID,
	_ time.Time,
) error {
	return nil
}

// --- Test Helpers ---

type handlerFixture struct {
	handler        *Handler
	fetcherMock    *mockFetcherClient
	connRepo       *mockConnectionRepo
	schemaRepo     *mockSchemaRepo
	extractionRepo *mockExtractionRepo
}

func newHandlerFixture(t *testing.T) *handlerFixture {
	t.Helper()

	fetcherMock := &mockFetcherClient{healthy: true}
	connRepo := newMockConnectionRepo()
	schemaRepo := newMockSchemaRepo()
	extractionRepo := &mockExtractionRepo{}

	cmdUC, err := discoveryCommand.NewUseCase(fetcherMock, connRepo, schemaRepo, extractionRepo, nil)
	require.NoError(t, err)

	queryUC, err := discoveryQuery.NewUseCase(fetcherMock, connRepo, schemaRepo, extractionRepo, nil)
	require.NoError(t, err)

	handler, err := NewHandler(cmdUC, queryUC, false)
	require.NoError(t, err)

	return &handlerFixture{
		handler:        handler,
		fetcherMock:    fetcherMock,
		connRepo:       connRepo,
		schemaRepo:     schemaRepo,
		extractionRepo: extractionRepo,
	}
}

func setupTestApp(t *testing.T, handler *Handler) *fiber.App {
	t.Helper()

	app := fiber.New(fiber.Config{
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			var e *fiber.Error
			if errors.As(err, &e) {
				code = e.Code
			}
			return c.Status(code).JSON(fiber.Map{"error": err.Error()})
		},
	})

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	tracer := tp.Tracer("test")

	// Inject tracer context via middleware so startHandlerSpan can resolve it.
	app.Use(func(c *fiber.Ctx) error {
		ctx := c.UserContext()
		ctx = libCommons.ContextWithTracer(ctx, tracer)
		c.SetUserContext(ctx)
		return c.Next()
	})

	app.Get("/v1/discovery/status", handler.GetDiscoveryStatus)
	app.Get("/v1/discovery/connections", handler.ListConnections)
	app.Get("/v1/discovery/connections/:connectionId", handler.GetConnection)
	app.Get("/v1/discovery/connections/:connectionId/schema", handler.GetConnectionSchema)
	app.Post("/v1/discovery/connections/:connectionId/test", handler.TestConnection)
	app.Post("/v1/discovery/connections/:connectionId/extractions", handler.StartExtraction)
	app.Get("/v1/discovery/extractions/:extractionId", handler.GetExtraction)
	app.Post("/v1/discovery/extractions/:extractionId/poll", handler.PollExtraction)
	app.Post("/v1/discovery/refresh", handler.RefreshDiscovery)

	return app
}

func assertStructuredErrorResponse(t *testing.T, resp *http.Response, expectedStatus int, expectedType, expectedMessage string) {
	t.Helper()

	assert.Equal(t, expectedStatus, resp.StatusCode)

	var body ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, expectedDiscoveryCode(expectedType, expectedMessage), body.Code)
	assert.Equal(t, http.StatusText(expectedStatus), body.Title)
	assert.Equal(t, expectedMessage, body.Message)
}

func expectedDiscoveryCode(expectedType, expectedMessage string) string {
	switch expectedMessage {
	case "connection not found":
		return constant.CodeDiscoveryConnectionNotFound
	case "extraction not found":
		return constant.CodeDiscoveryExtractionNotFound
	case "fetcher service unavailable":
		return constant.CodeDiscoveryFetcherUnavailable
	case "discovery refresh already in progress":
		return constant.CodeDiscoveryRefreshInProgress
	}

	if expectedType == "invalid_request" && strings.HasPrefix(expectedMessage, "invalid extraction request:") {
		return constant.CodeDiscoveryInvalidExtraction
	}

	switch expectedType {
	case "invalid_request":
		return constant.CodeInvalidRequest
	case "not_found":
		return constant.CodeNotFound
	case "conflict":
		return constant.CodeConflict
	case "discovery_connection_not_found":
		return constant.CodeDiscoveryConnectionNotFound
	case "discovery_extraction_not_found":
		return constant.CodeDiscoveryExtractionNotFound
	case "discovery_invalid_extraction":
		return constant.CodeDiscoveryInvalidExtraction
	case "discovery_fetcher_unavailable":
		return constant.CodeDiscoveryFetcherUnavailable
	case "refresh_in_progress":
		return constant.CodeDiscoveryRefreshInProgress
	default:
		return constant.CodeInternalServerError
	}
}

func (f *handlerFixture) seedExtraction(t *testing.T, connectionID uuid.UUID) *entities.ExtractionRequest {
	t.Helper()

	extraction, err := entities.NewExtractionRequest(context.Background(), connectionID, map[string]any{"transactions": true}, "2026-03-01", "2026-03-08", map[string]any{"currency": "USD"})
	require.NoError(t, err)
	require.NoError(t, extraction.MarkSubmitted("job-123"))
	f.extractionRepo.findByIDReq = extraction

	return extraction
}

func (f *handlerFixture) seedConnection(t *testing.T) *entities.FetcherConnection {
	t.Helper()

	conn := &entities.FetcherConnection{
		ID:               uuid.New(),
		FetcherConnID:    "fc-test-123",
		ConfigName:       "test-config",
		DatabaseType:     "postgresql",
		Host:             "localhost",
		Port:             5432,
		DatabaseName:     "testdb",
		ProductName:      "PostgreSQL 15",
		Status:           vo.ConnectionStatusAvailable,
		SchemaDiscovered: true,
		LastSeenAt:       time.Now().UTC(),
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}

	f.connRepo.connections[conn.ID] = conn

	return conn
}

func (f *handlerFixture) seedSchema(t *testing.T, connID uuid.UUID) {
	t.Helper()

	schema := &entities.DiscoveredSchema{
		ID:           uuid.New(),
		ConnectionID: connID,
		TableName:    "users",
		Columns: []entities.ColumnInfo{
			{Name: "id", Type: "integer", Nullable: false},
			{Name: "email", Type: "varchar", Nullable: true},
		},
		DiscoveredAt: time.Now().UTC(),
	}

	f.schemaRepo.schemas[connID] = []*entities.DiscoveredSchema{schema}
}

// --- Tests ---

func TestNewHandler_NilCommand(t *testing.T) {
	t.Parallel()

	_, err := NewHandler(nil, &discoveryQuery.UseCase{}, false)

	require.ErrorIs(t, err, ErrNilCommandUseCase)
}

func TestNewHandler_NilQuery(t *testing.T) {
	t.Parallel()

	fetcherMock := &mockFetcherClient{healthy: true}
	connRepo := newMockConnectionRepo()
	schemaRepo := newMockSchemaRepo()
	extractionRepo := &mockExtractionRepo{}

	cmdUC, err := discoveryCommand.NewUseCase(fetcherMock, connRepo, schemaRepo, extractionRepo, nil)
	require.NoError(t, err)

	_, err = NewHandler(cmdUC, nil, false)

	require.ErrorIs(t, err, ErrNilQueryUseCase)
}

func TestGetDiscoveryStatus_Success(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	fixture.seedConnection(t)

	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/status", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body dto.DiscoveryStatusResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.True(t, body.FetcherHealthy)
	assert.Equal(t, 1, body.ConnectionCount)
	assert.NotNil(t, body.LastSyncAt)
}

func TestGetDiscoveryStatus_NoSyncOmitsLastSyncAt(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/status", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body dto.DiscoveryStatusResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Nil(t, body.LastSyncAt)
}

func TestListConnections_Success(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	fixture.seedConnection(t)

	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/connections", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body dto.ConnectionListResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Len(t, body.Connections, 1)
	assert.Equal(t, "test-config", body.Connections[0].ConfigName)
}

func TestListConnections_Empty(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/connections", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body dto.ConnectionListResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Empty(t, body.Connections)
}

func TestGetConnection_Success(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	conn := fixture.seedConnection(t)

	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/connections/"+conn.ID.String(), nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body dto.ConnectionResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, conn.ID, body.ID)
	assert.Equal(t, "test-config", body.ConfigName)
}

func TestGetConnection_InvalidID(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/connections/not-a-uuid", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	assertStructuredErrorResponse(t, resp, http.StatusBadRequest, "invalid_request", "invalid connection ID")
}

func TestGetConnection_NotFound(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/connections/"+uuid.New().String(), nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	assertStructuredErrorResponse(t, resp, http.StatusNotFound, "not_found", "connection not found")
}

func TestGetConnectionSchema_Success(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	conn := fixture.seedConnection(t)
	fixture.seedSchema(t, conn.ID)

	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/connections/"+conn.ID.String()+"/schema", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body dto.ConnectionSchemaResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, conn.ID, body.ConnectionID)
	assert.Len(t, body.Tables, 1)
	assert.Equal(t, "users", body.Tables[0].TableName)
	assert.Len(t, body.Tables[0].Columns, 2)
}

func TestGetConnectionSchema_InvalidID(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/connections/bad-id/schema", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestGetConnectionSchema_NotFound(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/connections/"+uuid.New().String()+"/schema", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestTestConnection_Success(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	conn := fixture.seedConnection(t)
	fixture.fetcherMock.testResult = &sharedPorts.FetcherTestResult{
		Status:    "success",
		LatencyMs: 42,
	}

	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodPost, "/v1/discovery/connections/"+conn.ID.String()+"/test", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body dto.TestConnectionResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, conn.ID, body.ConnectionID)
	assert.True(t, body.Healthy)
	assert.Equal(t, int64(42), body.LatencyMs)
}

func TestTestConnection_UnhealthyResponseSanitizesErrorMessage(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	conn := fixture.seedConnection(t)
	fixture.fetcherMock.testResult = &sharedPorts.FetcherTestResult{
		Status:    "failed",
		Message:   "dial tcp 10.0.0.8:5432: connection refused",
		LatencyMs: 7,
	}

	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodPost, "/v1/discovery/connections/"+conn.ID.String()+"/test", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body dto.TestConnectionResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.False(t, body.Healthy)
	assert.Equal(t, "connection test failed", body.ErrorMessage)
}

func TestTestConnection_FetcherUnavailable(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	fixture.fetcherMock.healthy = false
	conn := fixture.seedConnection(t)

	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodPost, "/v1/discovery/connections/"+conn.ID.String()+"/test", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	assertStructuredErrorResponse(t, resp, http.StatusServiceUnavailable, "discovery_fetcher_unavailable", "fetcher service unavailable")
}

func TestRefreshDiscovery_Success(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	fixture.fetcherMock.connections = []*sharedPorts.FetcherConnection{
		{
			ID:           "fc-1",
			ConfigName:   "config-1",
			DatabaseType: "postgresql",
			Host:         "db1.example.com",
			Port:         5432,
			DatabaseName: "db1",
		},
	}

	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodPost, "/v1/discovery/refresh", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body dto.RefreshDiscoveryResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, 1, body.ConnectionsSynced)
}

func TestRefreshDiscovery_FetcherUnavailable(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	fixture.fetcherMock.healthy = false

	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodPost, "/v1/discovery/refresh", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestTestConnection_FetcherError(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	// Set up mock to return a non-health-related error (not ErrFetcherUnavailable)
	fixture.fetcherMock.testErr = errors.New("database connection timeout")
	fixture.fetcherMock.healthy = true // Fetcher is healthy, but test connection fails
	conn := fixture.seedConnection(t)

	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodPost, "/v1/discovery/connections/"+conn.ID.String()+"/test", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	// Should return 500, not 503, because it's not a health-related error.
	assertStructuredErrorResponse(t, resp, http.StatusInternalServerError, "internal_server_error", "failed to test connection")
}

func TestTestConnection_InvalidConnectionID(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodPost, "/v1/discovery/connections/not-a-uuid/test", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestTestConnection_ConnectionNotFound(t *testing.T) {
	t.Parallel()

	fixture := newHandlerFixture(t)
	app := setupTestApp(t, fixture.handler)

	req := httptest.NewRequest(http.MethodPost, "/v1/discovery/connections/"+uuid.New().String()+"/test", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestHandlerSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{"ErrNilCommandUseCase", ErrNilCommandUseCase, "command use case is required"},
		{"ErrNilQueryUseCase", ErrNilQueryUseCase, "query use case is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}
