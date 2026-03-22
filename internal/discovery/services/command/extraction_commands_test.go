//go:build unit

package command

import (
	"context"
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

func testConnectionEntity() *entities.FetcherConnection {
	return &entities.FetcherConnection{
		ID:               uuid.New(),
		FetcherConnID:    "fetcher-conn-1",
		ConfigName:       "config-1",
		DatabaseType:     "postgresql",
		Status:           vo.ConnectionStatusAvailable,
		SchemaDiscovered: true,
	}
}

func testDiscoveredSchema(connectionID uuid.UUID) []*entities.DiscoveredSchema {
	return []*entities.DiscoveredSchema{
		{
			ID:           uuid.New(),
			ConnectionID: connectionID,
			TableName:    "transactions",
			Columns: []entities.ColumnInfo{
				{Name: "id", Type: "uuid"},
				{Name: "amount", Type: "numeric"},
			},
			DiscoveredAt: time.Now().UTC(),
		},
	}
}

func TestStartExtraction_FetcherUnhealthy(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: false},
		&mockConnectionRepo{findByIDConn: testConnectionEntity()},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.StartExtraction(
		context.Background(),
		uuid.New(),
		map[string]any{"transactions": map[string]any{"columns": []string{"id"}}},
		sharedPorts.ExtractionParams{},
	)

	require.ErrorIs(t, err, ErrFetcherUnavailable)
}

func TestStartExtraction_SubmitJobError(t *testing.T) {
	t.Parallel()

	submitErr := errors.New("submit failed")

	connection := testConnectionEntity()
	extractionRepo := &mockExtractionRepo{}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, submitErr: submitErr},
		&mockConnectionRepo{findByIDConn: connection},
		&mockSchemaRepo{findByConnID: testDiscoveredSchema(connection.ID)},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.StartExtraction(
		context.Background(),
		connection.ID,
		map[string]any{"transactions": map[string]any{"columns": []string{"id"}}},
		sharedPorts.ExtractionParams{Filters: &sharedPorts.ExtractionFilters{Equals: map[string]string{"currency": "USD"}}},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "submit extraction job")
	assert.Equal(t, 1, extractionRepo.createCount)
	assert.Equal(t, 1, extractionRepo.updateCount)
	require.NotNil(t, extractionRepo.updatedReq)
	assert.Equal(t, vo.ExtractionStatusFailed, extractionRepo.updatedReq.Status)
	assert.Equal(t, entities.SanitizedExtractionFailureMessage, extractionRepo.updatedReq.ErrorMessage)
}

func TestStartExtraction_SubmittedPersistenceFailure_RecoversWithConditionalUpdate(t *testing.T) {
	t.Parallel()

	connection := testConnectionEntity()
	extractionRepo := &mockExtractionRepo{
		updateErr: errors.New("temporary update failure"),
		findByIDFn: func(_ context.Context, id uuid.UUID) (*entities.ExtractionRequest, error) {
			return &entities.ExtractionRequest{
				ID:           id,
				ConnectionID: connection.ID,
				Tables:       map[string]any{"transactions": map[string]any{"columns": []any{"id"}}},
				Filters:      map[string]any{"equals": map[string]any{"currency": "USD"}},
				Status:       vo.ExtractionStatusPending,
				CreatedAt:    time.Now().UTC(),
				UpdatedAt:    time.Now().UTC(),
			}, nil
		},
		updateIfFn: func(_ context.Context, req *entities.ExtractionRequest, _ time.Time) error {
			assert.Equal(t, vo.ExtractionStatusSubmitted, req.Status)
			assert.Equal(t, "job-123", req.FetcherJobID)

			return nil
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, submitJobID: "job-123"},
		&mockConnectionRepo{findByIDConn: connection},
		&mockSchemaRepo{findByConnID: testDiscoveredSchema(connection.ID)},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	extraction, err := uc.StartExtraction(
		context.Background(),
		connection.ID,
		map[string]any{"transactions": map[string]any{"columns": []string{"id"}}},
		sharedPorts.ExtractionParams{Filters: &sharedPorts.ExtractionFilters{Equals: map[string]string{"currency": "USD"}}},
	)

	require.NoError(t, err)
	require.NotNil(t, extraction)
	assert.Equal(t, vo.ExtractionStatusSubmitted, extraction.Status)
	assert.Equal(t, "job-123", extraction.FetcherJobID)
	assert.Equal(t, 1, extractionRepo.createCount)
	assert.Equal(t, 2, extractionRepo.updateCount)
}

func TestStartExtraction_SubmittedPersistenceFailure_ReturnsTrackingError(t *testing.T) {
	t.Parallel()

	connection := testConnectionEntity()
	extractionRepo := &mockExtractionRepo{
		updateErr: errors.New("primary update failed"),
		findByIDFn: func(_ context.Context, id uuid.UUID) (*entities.ExtractionRequest, error) {
			return &entities.ExtractionRequest{
				ID:           id,
				ConnectionID: connection.ID,
				Tables:       map[string]any{"transactions": map[string]any{"columns": []any{"id"}}},
				Status:       vo.ExtractionStatusPending,
				CreatedAt:    time.Now().UTC(),
				UpdatedAt:    time.Now().UTC(),
			}, nil
		},
		updateIfFn: func(_ context.Context, _ *entities.ExtractionRequest, _ time.Time) error {
			return errors.New("repair update failed")
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, submitJobID: "job-123"},
		&mockConnectionRepo{findByIDConn: connection},
		&mockSchemaRepo{findByConnID: testDiscoveredSchema(connection.ID)},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.StartExtraction(
		context.Background(),
		connection.ID,
		map[string]any{"transactions": map[string]any{"columns": []string{"id"}}},
		sharedPorts.ExtractionParams{},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrExtractionTrackingIncomplete)
	assert.Contains(t, err.Error(), "repair submitted extraction request")
}

func TestStartExtraction_PersistError(t *testing.T) {
	t.Parallel()

	extractionRepo := &mockExtractionRepo{createErr: errors.New("db error")}
	connection := testConnectionEntity()

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, submitJobID: "job-abc"},
		&mockConnectionRepo{findByIDConn: connection},
		&mockSchemaRepo{findByConnID: testDiscoveredSchema(connection.ID)},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.StartExtraction(
		context.Background(),
		connection.ID,
		map[string]any{"transactions": map[string]any{"columns": []string{"id"}}},
		sharedPorts.ExtractionParams{},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist pending extraction request")
}

func TestStartExtraction_Success(t *testing.T) {
	t.Parallel()

	extractionRepo := &mockExtractionRepo{}
	fetcherClient := &mockFetcherClient{healthy: true, submitJobID: "job-123"}
	connection := testConnectionEntity()

	uc, err := NewUseCase(
		fetcherClient,
		&mockConnectionRepo{findByIDConn: connection},
		&mockSchemaRepo{findByConnID: testDiscoveredSchema(connection.ID)},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	extraction, err := uc.StartExtraction(
		context.Background(),
		connection.ID,
		map[string]interface{}{
			"transactions": map[string]any{"columns": []any{"id", "amount"}},
		},
		sharedPorts.ExtractionParams{
			StartDate: "2026-03-01",
			EndDate:   "2026-03-08",
			Filters:   &sharedPorts.ExtractionFilters{Equals: map[string]string{"currency": "USD"}},
		},
	)

	require.NoError(t, err)
	require.NotNil(t, extraction)
	assert.Equal(t, 1, fetcherClient.submitCallCount)
	assert.Equal(t, 1, extractionRepo.createCount)
	assert.Equal(t, 1, extractionRepo.updateCount)
	assert.Equal(t, connection.ID, extraction.ConnectionID)
	assert.Equal(t, "job-123", extraction.FetcherJobID)
	assert.Equal(t, vo.ExtractionStatusSubmitted, extraction.Status)
	assert.Equal(t, connection.FetcherConnID, fetcherClient.lastSubmitInput.ConnectionID)
	require.NotNil(t, fetcherClient.lastSubmitInput.Filters)
	assert.Equal(t, map[string]string{"currency": "USD"}, fetcherClient.lastSubmitInput.Filters.Equals)
	require.Contains(t, fetcherClient.lastSubmitInput.Tables, "transactions")
	assert.Equal(t, []string{"id", "amount"}, fetcherClient.lastSubmitInput.Tables["transactions"].Columns)
	assert.Equal(t, "2026-03-01", fetcherClient.lastSubmitInput.Tables["transactions"].StartDate)
	assert.Equal(t, "2026-03-08", fetcherClient.lastSubmitInput.Tables["transactions"].EndDate)
}

func TestBuildTableConfig(t *testing.T) {
	t.Parallel()

	params := sharedPorts.ExtractionParams{StartDate: "2026-03-01", EndDate: "2026-03-08"}

	tests := []struct {
		name     string
		input    any
		expected sharedPorts.ExtractionTableConfig
		errText  string
	}{
		{
			name:    "non_map_rejected",
			input:   true,
			errText: "table configuration must be an object",
		},
		{
			name:  "string_slice_is_preserved",
			input: map[string]any{"columns": []string{"id", "amount"}},
			expected: sharedPorts.ExtractionTableConfig{
				Columns:   []string{"id", "amount"},
				StartDate: "2026-03-01",
				EndDate:   "2026-03-08",
			},
		},
		{
			name:    "non_string_columns_rejected",
			input:   map[string]any{"columns": []any{"id", 42}},
			errText: "columns must be strings",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			config, err := buildTableConfig(tt.input, params)
			if tt.errText != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errText)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, config)
		})
	}
}

func TestStartExtraction_ConnectionNotFound(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findByIDErr: repositories.ErrConnectionNotFound},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.StartExtraction(
		context.Background(),
		uuid.New(),
		map[string]any{"transactions": map[string]any{"columns": []string{"id"}}},
		sharedPorts.ExtractionParams{},
	)

	require.ErrorIs(t, err, ErrConnectionNotFound)
}

func TestStartExtraction_ConnectionNotReadyRejected(t *testing.T) {
	t.Parallel()

	connection := testConnectionEntity()
	connection.Status = vo.ConnectionStatusUnreachable
	connection.SchemaDiscovered = false

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findByIDConn: connection},
		&mockSchemaRepo{findByConnID: testDiscoveredSchema(connection.ID)},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.StartExtraction(
		context.Background(),
		connection.ID,
		map[string]any{"transactions": map[string]any{"columns": []string{"id"}}},
		sharedPorts.ExtractionParams{},
	)

	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
	assert.Contains(t, err.Error(), "connection schema is not available")
}

func TestStartExtraction_InvalidRequestRejected(t *testing.T) {
	t.Parallel()

	connection := testConnectionEntity()
	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findByIDConn: connection},
		&mockSchemaRepo{findByConnID: testDiscoveredSchema(connection.ID)},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.StartExtraction(
		context.Background(),
		connection.ID,
		map[string]any{"transactions": map[string]any{"columns": []any{"id", 99}}},
		sharedPorts.ExtractionParams{StartDate: "2026-03-10", EndDate: "2026-03-01"},
	)

	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
}

func TestStartExtraction_UnknownSchemaScopeRejected(t *testing.T) {
	t.Parallel()

	connection := testConnectionEntity()
	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findByIDConn: connection},
		&mockSchemaRepo{findByConnID: testDiscoveredSchema(connection.ID)},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.StartExtraction(
		context.Background(),
		connection.ID,
		map[string]any{"transactions": map[string]any{"columns": []string{"missing_column"}}},
		sharedPorts.ExtractionParams{},
	)

	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
}

func TestPollExtractionStatus_FindError(t *testing.T) {
	t.Parallel()

	extractionRepo := &mockExtractionRepo{findByIDErr: repositories.ErrExtractionNotFound}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.PollExtractionStatus(context.Background(), uuid.New())

	require.ErrorIs(t, err, ErrExtractionNotFound)
}

func TestPollExtractionStatus_AlreadyComplete(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-done",
		Status:       vo.ExtractionStatusComplete,
		ResultPath:   "/data/output.csv",
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	assert.Equal(t, req, result)
	// No update should be made since the status is already terminal.
	assert.Equal(t, 0, extractionRepo.updateCount)
}

func TestPollExtractionStatus_AlreadyFailed(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-failed",
		Status:       vo.ExtractionStatusFailed,
		ErrorMessage: "disk full",
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	assert.Equal(t, req, result)
	assert.Equal(t, 0, extractionRepo.updateCount)
}

func TestPollExtractionStatus_TransitionsToRunning(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-running",
		Status:       vo.ExtractionStatusSubmitted,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			jobStatus: &sharedPorts.ExtractionJobStatus{
				JobID:  "job-running",
				Status: "RUNNING",
			},
		},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, extractionRepo.updateCount)
	assert.Equal(t, vo.ExtractionStatusExtracting, req.Status)
}

func TestPollExtractionStatus_QueuedStatusDoesNotUpdate(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-queued",
		Status:       vo.ExtractionStatusSubmitted,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			jobStatus: &sharedPorts.ExtractionJobStatus{
				JobID:  "job-queued",
				Status: "SUBMITTED",
			},
		},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	assert.Equal(t, req, result)
	assert.Equal(t, vo.ExtractionStatusSubmitted, req.Status)
	assert.Equal(t, 0, extractionRepo.updateCount)
}

func TestPollExtractionStatus_TransitionsToComplete(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-complete",
		Status:       vo.ExtractionStatusExtracting,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			jobStatus: &sharedPorts.ExtractionJobStatus{
				JobID:      "job-complete",
				Status:     "COMPLETE",
				ResultPath: "/data/result.csv",
			},
		},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, extractionRepo.updateCount)
	assert.Equal(t, vo.ExtractionStatusComplete, req.Status)
	assert.Equal(t, "/data/result.csv", req.ResultPath)
}

func TestPollExtractionStatus_TransitionsToFailed(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-fail",
		Status:       vo.ExtractionStatusExtracting,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			jobStatus: &sharedPorts.ExtractionJobStatus{
				JobID:        "job-fail",
				Status:       "FAILED",
				ErrorMessage: "connection refused",
			},
		},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, extractionRepo.updateCount)
	assert.Equal(t, vo.ExtractionStatusFailed, req.Status)
	assert.Equal(t, entities.SanitizedExtractionFailureMessage, req.ErrorMessage)
}

func TestPollExtractionStatus_GetStatusError(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-x",
		Status:       vo.ExtractionStatusSubmitted,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy:      true,
			jobStatusErr: errors.New("fetcher timeout"),
		},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "get extraction job status")
}

func TestPollExtractionStatus_MissingFetcherJobID(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		Status:       vo.ExtractionStatusPending,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.ErrorIs(t, err, ErrExtractionTrackingIncomplete)
	assert.Equal(t, 0, extractionRepo.updateCount)
}

func TestPollExtractionStatus_FetcherUnavailable(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-unavailable",
		Status:       vo.ExtractionStatusSubmitted,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, jobStatusErr: sharedPorts.ErrFetcherUnavailable},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.ErrorIs(t, err, ErrFetcherUnavailable)
}

func TestPollExtractionStatus_RemoteNotFoundCancelsExtraction(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-missing",
		Status:       vo.ExtractionStatusSubmitted,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, jobStatusErr: sharedPorts.ErrFetcherResourceNotFound},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	assert.Equal(t, vo.ExtractionStatusCancelled, result.Status)
	assert.Equal(t, 1, extractionRepo.updateCount)
	assert.Empty(t, result.ErrorMessage)
}

func TestPollExtractionStatus_TransitionsToCancelled(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-cancelled",
		Status:       vo.ExtractionStatusExtracting,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			jobStatus: &sharedPorts.ExtractionJobStatus{
				JobID:  "job-cancelled",
				Status: "CANCELLED",
			},
		},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, vo.ExtractionStatusCancelled, result.Status)
	assert.Equal(t, 1, extractionRepo.updateCount)
}

func TestPollExtractionStatus_ConcurrentUpdateReturnsLatestState(t *testing.T) {
	t.Parallel()

	staleReq := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-complete",
		Status:       vo.ExtractionStatusSubmitted,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC().Add(-time.Minute),
		UpdatedAt:    time.Now().UTC().Add(-time.Second),
	}
	latestReq := &entities.ExtractionRequest{
		ID:           staleReq.ID,
		FetcherJobID: staleReq.FetcherJobID,
		Status:       vo.ExtractionStatusComplete,
		ConnectionID: staleReq.ConnectionID,
		ResultPath:   "/data/already-complete.csv",
		CreatedAt:    staleReq.CreatedAt,
		UpdatedAt:    time.Now().UTC(),
	}

	var findCount int
	extractionRepo := &mockExtractionRepo{
		updateIfFn: func(_ context.Context, _ *entities.ExtractionRequest, _ time.Time) error {
			return repositories.ErrExtractionConflict
		},
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			findCount++
			if findCount == 1 {
				return staleReq, nil
			}

			return latestReq, nil
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			jobStatus: &sharedPorts.ExtractionJobStatus{
				JobID:      staleReq.FetcherJobID,
				Status:     "COMPLETE",
				ResultPath: "/data/newer.csv",
			},
		},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), staleReq.ID)

	require.NoError(t, err)
	assert.Same(t, latestReq, result)
	assert.Equal(t, 2, findCount)
	assert.Equal(t, 1, extractionRepo.updateCount)
	assert.Equal(t, vo.ExtractionStatusComplete, latestReq.Status)
	assert.Equal(t, "/data/already-complete.csv", latestReq.ResultPath)
}

func TestPollExtractionStatus_UpdateError(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-up",
		Status:       vo.ExtractionStatusSubmitted,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{
		findByIDReq: req,
		updateErr:   errors.New("update failed"),
	}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			jobStatus: &sharedPorts.ExtractionJobStatus{
				JobID:  "job-up",
				Status: "RUNNING",
			},
		},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "update extraction request")
}

func TestPollExtractionStatus_UnknownStatus_PersistsWithoutTransition(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-weird",
		Status:       vo.ExtractionStatusSubmitted,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			jobStatus: &sharedPorts.ExtractionJobStatus{
				JobID:  "job-weird",
				Status: "SUSPENDED",
			},
		},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	assert.Equal(t, req, result)
	assert.Equal(t, 0, extractionRepo.updateCount)
	assert.Equal(t, vo.ExtractionStatusSubmitted, req.Status)
}

func TestPollExtractionStatus_NilStatus_ReturnsError(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-nil",
		Status:       vo.ExtractionStatusSubmitted,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, jobStatus: nil},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil extraction status")
}
