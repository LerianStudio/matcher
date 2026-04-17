//go:build unit

package query

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// --- Mock implementations for testing ---

// mockFetcherClient is a configurable mock for sharedPorts.FetcherClient.
type mockFetcherClient struct {
	healthy      bool
	connections  []*sharedPorts.FetcherConnection
	listErr      error
	schema       *sharedPorts.FetcherSchema
	schemaErr    error
	testResult   *sharedPorts.FetcherTestResult
	testErr      error
	submitJobID  string
	submitErr    error
	jobStatus    *sharedPorts.ExtractionJobStatus
	jobStatusErr error
}

func (m *mockFetcherClient) IsHealthy(_ context.Context) bool {
	return m.healthy
}

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

// mockConnectionRepo is a configurable mock for repositories.ConnectionRepository.
type mockConnectionRepo struct {
	upsertErr        error
	findAllConns     []*entities.FetcherConnection
	findAllErr       error
	findByIDConn     *entities.FetcherConnection
	findByIDErr      error
	findByFetcherID  *entities.FetcherConnection
	findByFetcherErr error
}

func (m *mockConnectionRepo) Upsert(_ context.Context, _ *entities.FetcherConnection) error {
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
	return m.findByFetcherID, m.findByFetcherErr
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
	findByConnID   []*entities.DiscoveredSchema
	findByConnErr  error
}

type mockExtractionRepo struct {
	findByIDReq *entities.ExtractionRequest
	findByIDErr error
}

func (m *mockExtractionRepo) Create(_ context.Context, _ *entities.ExtractionRequest) error {
	return nil
}

func (m *mockExtractionRepo) CreateWithTx(_ context.Context, _ *sql.Tx, _ *entities.ExtractionRequest) error {
	return nil
}

func (m *mockExtractionRepo) Update(_ context.Context, _ *entities.ExtractionRequest) error {
	return nil
}

func (m *mockExtractionRepo) UpdateIfUnchanged(_ context.Context, _ *entities.ExtractionRequest, _ time.Time) error {
	return nil
}

func (m *mockExtractionRepo) UpdateIfUnchangedWithTx(_ context.Context, _ *sql.Tx, _ *entities.ExtractionRequest, _ time.Time) error {
	return nil
}

func (m *mockExtractionRepo) UpdateWithTx(_ context.Context, _ *sql.Tx, _ *entities.ExtractionRequest) error {
	return nil
}

func (m *mockExtractionRepo) FindByID(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
	if m.findByIDErr != nil {
		return nil, m.findByIDErr
	}

	if m.findByIDReq == nil {
		return nil, repositories.ErrExtractionNotFound
	}

	return m.findByIDReq, nil
}

func (m *mockExtractionRepo) LinkIfUnlinked(_ context.Context, _ uuid.UUID, _ uuid.UUID) error {
	return nil
}

func (m *mockExtractionRepo) MarkBridgeFailed(_ context.Context, _ *entities.ExtractionRequest) error {
	return nil
}

func (m *mockExtractionRepo) MarkBridgeFailedWithTx(_ context.Context, _ sharedPorts.Tx, _ *entities.ExtractionRequest) error {
	return nil
}

func (m *mockExtractionRepo) IncrementBridgeAttempts(_ context.Context, _ uuid.UUID, _ int) error {
	return nil
}

func (m *mockExtractionRepo) IncrementBridgeAttemptsWithTx(_ context.Context, _ sharedPorts.Tx, _ uuid.UUID, _ int) error {
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

func (m *mockSchemaRepo) UpsertBatch(_ context.Context, _ []*entities.DiscoveredSchema) error {
	return m.upsertBatchErr
}

func (m *mockSchemaRepo) UpsertBatchWithTx(_ context.Context, _ *sql.Tx, _ []*entities.DiscoveredSchema) error {
	return m.upsertBatchErr
}

func (m *mockSchemaRepo) FindByConnectionID(_ context.Context, _ uuid.UUID) ([]*entities.DiscoveredSchema, error) {
	return m.findByConnID, m.findByConnErr
}

func (m *mockSchemaRepo) DeleteByConnectionID(_ context.Context, _ uuid.UUID) error {
	return nil
}

func (m *mockSchemaRepo) DeleteByConnectionIDWithTx(_ context.Context, _ *sql.Tx, _ uuid.UUID) error {
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
	assert.NotNil(t, uc.logger)
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

func TestGetExtraction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		repo         *mockExtractionRepo
		wantErr      error
		wantStatus   vo.ExtractionStatus
		wantContains string
	}{
		{
			name:       "found extraction",
			repo:       &mockExtractionRepo{findByIDReq: &entities.ExtractionRequest{ID: uuid.New(), Status: vo.ExtractionStatusSubmitted}},
			wantStatus: vo.ExtractionStatusSubmitted,
		},
		{
			name:    "not found",
			repo:    &mockExtractionRepo{findByIDErr: repositories.ErrExtractionNotFound},
			wantErr: ErrExtractionNotFound,
		},
		{
			name:         "wrapped repository error",
			repo:         &mockExtractionRepo{findByIDErr: errors.New("db down")},
			wantContains: "get extraction",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uc, err := NewUseCase(
				&mockFetcherClient{},
				&mockConnectionRepo{},
				&mockSchemaRepo{},
				tt.repo,
				&libLog.NopLogger{},
			)
			require.NoError(t, err)

			req, err := uc.GetExtraction(context.Background(), uuid.New())

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, req)
				return
			}

			if tt.wantContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantContains)
				assert.Nil(t, req)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, req)
			assert.Equal(t, tt.wantStatus, req.Status)
		})
	}
}
