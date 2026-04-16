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
	require.NotNil(t, fetcherClient.lastSubmitInput.MappedFields)
	configKey := connection.ConfigName
	require.Contains(t, fetcherClient.lastSubmitInput.MappedFields, configKey)
	require.Contains(t, fetcherClient.lastSubmitInput.MappedFields[configKey], "transactions")
	assert.Equal(t, []string{"id", "amount"}, fetcherClient.lastSubmitInput.MappedFields[configKey]["transactions"])
	require.NotNil(t, fetcherClient.lastSubmitInput.Filters)
	require.Contains(t, fetcherClient.lastSubmitInput.Filters, configKey)
	require.Contains(t, fetcherClient.lastSubmitInput.Filters[configKey], "transactions")
	currencyCond := fetcherClient.lastSubmitInput.Filters[configKey]["transactions"]["currency"].(map[string]any)
	assert.Equal(t, []any{"USD"}, currencyCond["eq"])
	require.NotNil(t, fetcherClient.lastSubmitInput.Metadata)
	assert.Equal(t, connection.ConfigName, fetcherClient.lastSubmitInput.Metadata["source"])
}

func TestExtractRequestedColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    any
		expected []string
		errText  string
	}{
		{
			name:    "non_map_rejected",
			input:   true,
			errText: "table configuration must be an object",
		},
		{
			name:     "string_slice_is_preserved",
			input:    map[string]any{"columns": []string{"id", "amount"}},
			expected: []string{"id", "amount"},
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
			columns, err := extractRequestedColumns(tt.input)
			if tt.errText != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errText)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, columns)
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
