//go:build unit

package command

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

type nilAwarePoller struct{}

func (*nilAwarePoller) PollUntilComplete(context.Context, uuid.UUID, func(context.Context, string) error, func(context.Context, string)) {
}

// --- Mock implementations for testing ---

// mockFetcherClient is a configurable mock for sharedPorts.FetcherClient.
type mockFetcherClient struct {
	healthy            bool
	connections        []*sharedPorts.FetcherConnection
	listErr            error
	lastListOrgID      string
	schema             *sharedPorts.FetcherSchema
	schemaErr          error
	testResult         *sharedPorts.FetcherTestResult
	testErr            error
	submitJobID        string
	submitErr          error
	lastSubmitInput    sharedPorts.ExtractionJobInput
	jobStatus          *sharedPorts.ExtractionJobStatus
	jobStatusErr       error
	listCallCount      int
	schemaCallCount    int
	submitCallCount    int
	jobStatusCallCount int
}

func (m *mockFetcherClient) IsHealthy(_ context.Context) bool {
	return m.healthy
}

func (m *mockFetcherClient) ListConnections(_ context.Context, productName string) ([]*sharedPorts.FetcherConnection, error) {
	m.listCallCount++
	m.lastListOrgID = productName

	return m.connections, m.listErr
}

func (m *mockFetcherClient) GetSchema(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
	m.schemaCallCount++

	return m.schema, m.schemaErr
}

func (m *mockFetcherClient) TestConnection(_ context.Context, _ string) (*sharedPorts.FetcherTestResult, error) {
	return m.testResult, m.testErr
}

func (m *mockFetcherClient) SubmitExtractionJob(_ context.Context, input sharedPorts.ExtractionJobInput) (string, error) {
	m.submitCallCount++
	m.lastSubmitInput = input

	return m.submitJobID, m.submitErr
}

func (m *mockFetcherClient) GetExtractionJobStatus(_ context.Context, _ string) (*sharedPorts.ExtractionJobStatus, error) {
	m.jobStatusCallCount++

	return m.jobStatus, m.jobStatusErr
}

// mockConnectionRepo is a configurable mock for repositories.ConnectionRepository.
type mockConnectionRepo struct {
	upsertErr         error
	upsertCount       int
	upsertFn          func(ctx context.Context, conn *entities.FetcherConnection) error
	findAllConns      []*entities.FetcherConnection
	findAllErr        error
	findByIDConn      *entities.FetcherConnection
	findByIDErr       error
	findByFetcherConn *entities.FetcherConnection
	findByFetcherErr  error
}

func (m *mockConnectionRepo) Upsert(ctx context.Context, conn *entities.FetcherConnection) error {
	m.upsertCount++
	if m.upsertFn != nil {
		return m.upsertFn(ctx, conn)
	}

	return m.upsertErr
}

func (m *mockConnectionRepo) UpsertWithTx(_ context.Context, _ *sql.Tx, _ *entities.FetcherConnection) error {
	return m.upsertErr
}

func (m *mockConnectionRepo) FindAll(_ context.Context) ([]*entities.FetcherConnection, error) {
	return m.findAllConns, m.findAllErr
}

func (m *mockConnectionRepo) FindByID(_ context.Context, _ uuid.UUID) (*entities.FetcherConnection, error) {
	return m.findByIDConn, m.findByIDErr
}

func (m *mockConnectionRepo) FindByFetcherID(_ context.Context, _ string) (*entities.FetcherConnection, error) {
	return m.findByFetcherConn, m.findByFetcherErr
}

func (m *mockConnectionRepo) DeleteStale(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}

func (m *mockConnectionRepo) DeleteStaleWithTx(_ context.Context, _ *sql.Tx, _ time.Duration) (int64, error) {
	return 0, nil
}

// mockSchemaRepo is a configurable mock for repositories.SchemaRepository.
type mockSchemaRepo struct {
	upsertBatchErr error
	upsertCount    int
	deleteCount    int
	findByConnID   []*entities.DiscoveredSchema
	findByConnErr  error
}

func (m *mockSchemaRepo) UpsertBatch(_ context.Context, _ []*entities.DiscoveredSchema) error {
	m.upsertCount++

	return m.upsertBatchErr
}

func (m *mockSchemaRepo) UpsertBatchWithTx(_ context.Context, _ *sql.Tx, _ []*entities.DiscoveredSchema) error {
	return m.upsertBatchErr
}

func (m *mockSchemaRepo) FindByConnectionID(_ context.Context, _ uuid.UUID) ([]*entities.DiscoveredSchema, error) {
	return m.findByConnID, m.findByConnErr
}

func (m *mockSchemaRepo) DeleteByConnectionID(_ context.Context, _ uuid.UUID) error {
	m.deleteCount++

	return nil
}

func (m *mockSchemaRepo) DeleteByConnectionIDWithTx(_ context.Context, _ *sql.Tx, _ uuid.UUID) error {
	return nil
}

// mockExtractionRepo is a configurable mock for repositories.ExtractionRepository.
type mockExtractionRepo struct {
	createErr   error
	createCount int
	createdReq  *entities.ExtractionRequest
	updateErr   error
	updateCount int
	updatedReq  *entities.ExtractionRequest
	updateFn    func(ctx context.Context, req *entities.ExtractionRequest) error
	updateIfFn  func(ctx context.Context, req *entities.ExtractionRequest, expectedUpdatedAt time.Time) error
	findByIDReq *entities.ExtractionRequest
	findByIDErr error
	findByIDFn  func(ctx context.Context, id uuid.UUID) (*entities.ExtractionRequest, error)
}

func (m *mockExtractionRepo) Create(_ context.Context, req *entities.ExtractionRequest) error {
	m.createCount++
	m.createdReq = req

	return m.createErr
}

func (m *mockExtractionRepo) CreateWithTx(_ context.Context, _ *sql.Tx, _ *entities.ExtractionRequest) error {
	return m.createErr
}

func (m *mockExtractionRepo) Update(ctx context.Context, req *entities.ExtractionRequest) error {
	m.updateCount++
	m.updatedReq = req
	if m.updateFn != nil {
		return m.updateFn(ctx, req)
	}

	return m.updateErr
}

func (m *mockExtractionRepo) UpdateIfUnchanged(ctx context.Context, req *entities.ExtractionRequest, expectedUpdatedAt time.Time) error {
	m.updateCount++
	m.updatedReq = req
	if m.updateIfFn != nil {
		return m.updateIfFn(ctx, req, expectedUpdatedAt)
	}

	return m.updateErr
}

func (m *mockExtractionRepo) UpdateIfUnchangedWithTx(
	ctx context.Context,
	_ *sql.Tx,
	req *entities.ExtractionRequest,
	expectedUpdatedAt time.Time,
) error {
	return m.UpdateIfUnchanged(ctx, req, expectedUpdatedAt)
}

func (m *mockExtractionRepo) UpdateWithTx(_ context.Context, _ *sql.Tx, _ *entities.ExtractionRequest) error {
	return m.updateErr
}

func (m *mockExtractionRepo) FindByID(ctx context.Context, id uuid.UUID) (*entities.ExtractionRequest, error) {
	if m.findByIDFn != nil {
		return m.findByIDFn(ctx, id)
	}

	return m.findByIDReq, m.findByIDErr
}

func (m *mockExtractionRepo) LinkIfUnlinked(_ context.Context, _ uuid.UUID, _ uuid.UUID) error {
	return nil
}

func (m *mockExtractionRepo) MarkBridgeFailed(_ context.Context, _ *entities.ExtractionRequest) error {
	return nil
}

func (m *mockExtractionRepo) MarkBridgeFailedWithTx(_ context.Context, _ *sql.Tx, _ *entities.ExtractionRequest) error {
	return nil
}

func (m *mockExtractionRepo) IncrementBridgeAttempts(_ context.Context, _ uuid.UUID, _ int) error {
	return nil
}

func (m *mockExtractionRepo) IncrementBridgeAttemptsWithTx(_ context.Context, _ *sql.Tx, _ uuid.UUID, _ int) error {
	return nil
}

func (m *mockExtractionRepo) FindEligibleForBridge(_ context.Context, _ int) ([]*entities.ExtractionRequest, error) {
	return nil, nil
}

func (m *mockExtractionRepo) CountBridgeReadiness(_ context.Context, _ time.Duration) (repositories.BridgeReadinessCounts, error) {
	return repositories.BridgeReadinessCounts{}, nil
}

func (m *mockExtractionRepo) ListBridgeCandidates(
	_ context.Context,
	_ string,
	_ time.Duration,
	_ time.Time,
	_ uuid.UUID,
	_ int,
) ([]*entities.ExtractionRequest, error) {
	return nil, nil
}

func (m *mockExtractionRepo) FindBridgeRetentionCandidates(
	_ context.Context,
	_ time.Duration,
	_ int,
) ([]*entities.ExtractionRequest, error) {
	return nil, nil
}

func (m *mockExtractionRepo) MarkCustodyDeleted(
	_ context.Context,
	_ uuid.UUID,
	_ time.Time,
) error {
	return nil
}

func (m *mockExtractionRepo) MarkCustodyDeletedWithTx(
	_ context.Context,
	_ sharedPorts.Tx,
	_ uuid.UUID,
	_ time.Time,
) error {
	return nil
}

// --- Tests ---

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{"ErrFetcherUnavailable", ErrFetcherUnavailable, "fetcher service is unavailable"},
		{"ErrConnectionNotFound", ErrConnectionNotFound, "fetcher connection not found"},
		{"ErrInvalidExtractionRequest", ErrInvalidExtractionRequest, "invalid extraction request"},
		{"ErrNilFetcherClient", ErrNilFetcherClient, "fetcher client is required"},
		{"ErrNilConnectionRepository", ErrNilConnectionRepository, "connection repository is required"},
		{"ErrNilSchemaRepository", ErrNilSchemaRepository, "schema repository is required"},
		{"ErrNilExtractionRepository", ErrNilExtractionRepository, "extraction repository is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestNewUseCase_Success(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)

	require.NoError(t, err)
	require.NotNil(t, uc)
	assert.NotNil(t, uc.fetcherClient)
	assert.NotNil(t, uc.connRepo)
	assert.NotNil(t, uc.schemaRepo)
	assert.NotNil(t, uc.extractionRepo)
	assert.NotNil(t, uc.logger)
}

func TestWithExtractionPoller_TypedNilNormalizedToNil(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	var poller *nilAwarePoller
	uc.WithExtractionPoller(poller)

	assert.Nil(t, uc.extractionPoller)
}

func TestNewUseCase_NilFetcherClient(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		nil,
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)

	assert.Nil(t, uc)
	require.ErrorIs(t, err, ErrNilFetcherClient)
}

func TestNewUseCase_NilConnectionRepo(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{},
		nil,
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)

	assert.Nil(t, uc)
	require.ErrorIs(t, err, ErrNilConnectionRepository)
}

func TestNewUseCase_NilSchemaRepo(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		nil,
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)

	assert.Nil(t, uc)
	require.ErrorIs(t, err, ErrNilSchemaRepository)
}

func TestNewUseCase_NilExtractionRepo(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		nil,
		&libLog.NopLogger{},
	)

	assert.Nil(t, uc)
	require.ErrorIs(t, err, ErrNilExtractionRepository)
}

func TestNewUseCase_NilLogger_DefaultsToNop(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		nil,
	)

	require.NoError(t, err)
	require.NotNil(t, uc)
	assert.IsType(t, &libLog.NopLogger{}, uc.logger)
}
