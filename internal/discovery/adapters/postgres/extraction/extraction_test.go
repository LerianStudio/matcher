//go:build unit

package extraction

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
)

// validExtractionModel returns a fully-populated ExtractionModel for test fixtures.
func validExtractionModel() *ExtractionModel {
	now := time.Date(2026, 3, 8, 14, 0, 0, 0, time.UTC)

	return &ExtractionModel{
		ID:             uuid.MustParse("11111111-2222-3333-4444-555555555555"),
		ConnectionID:   uuid.MustParse("99999999-8888-7777-6666-555555555555"),
		IngestionJobID: uuid.NullUUID{UUID: uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"), Valid: true},
		FetcherJobID:   sql.NullString{String: "fetcher-job-abc", Valid: true},
		Tables:         []byte(`{"transactions":{"columns":["id","amount"]}}`),
		StartDate:      sql.NullString{String: "2026-03-01", Valid: true},
		EndDate:        sql.NullString{String: "2026-03-08", Valid: true},
		Filters:        []byte(`{"equals":{"currency":"USD"}}`),
		Status:         "PENDING",
		ResultPath:     sql.NullString{String: "/data/result.csv", Valid: true},
		ErrorMessage:   sql.NullString{},
		CreatedAt:      now.Add(-time.Hour),
		UpdatedAt:      now,
	}
}

// validExtractionEntity returns a fully-populated ExtractionRequest entity for test fixtures.
func validExtractionEntity() *entities.ExtractionRequest {
	now := time.Date(2026, 3, 8, 14, 0, 0, 0, time.UTC)

	return &entities.ExtractionRequest{
		ID:             uuid.MustParse("11111111-2222-3333-4444-555555555555"),
		ConnectionID:   uuid.MustParse("99999999-8888-7777-6666-555555555555"),
		IngestionJobID: uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
		FetcherJobID:   "fetcher-job-abc",
		Tables:         map[string]any{"transactions": map[string]any{"columns": []any{"id", "amount"}}},
		StartDate:      "2026-03-01",
		EndDate:        "2026-03-08",
		Filters:        map[string]any{"equals": map[string]any{"currency": "USD"}},
		Status:         vo.ExtractionStatusPending,
		ResultPath:     "/data/result.csv",
		ErrorMessage:   "",
		CreatedAt:      now.Add(-time.Hour),
		UpdatedAt:      now,
	}
}

func TestExtractionModel_ToDomain_ValidModel(t *testing.T) {
	t.Parallel()

	model := validExtractionModel()
	entity, err := model.ToDomain()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, model.ID, entity.ID)
	assert.Equal(t, model.ConnectionID, entity.ConnectionID)
	assert.Equal(t, model.IngestionJobID.UUID, entity.IngestionJobID)
	assert.Equal(t, "fetcher-job-abc", entity.FetcherJobID)
	assert.Equal(t, "2026-03-01", entity.StartDate)
	assert.Equal(t, "2026-03-08", entity.EndDate)
	assert.Equal(t, vo.ExtractionStatusPending, entity.Status)
	assert.Equal(t, "/data/result.csv", entity.ResultPath)
	assert.Empty(t, entity.ErrorMessage)
	assert.NotNil(t, entity.Tables)
	assert.NotNil(t, entity.Filters)
	assert.Equal(t, model.CreatedAt, entity.CreatedAt)
	assert.Equal(t, model.UpdatedAt, entity.UpdatedAt)
}

func TestExtractionModel_ToDomain_NilModel_ReturnsError(t *testing.T) {
	t.Parallel()

	var model *ExtractionModel
	entity, err := model.ToDomain()

	assert.ErrorIs(t, err, ErrModelRequired)
	assert.Nil(t, entity)
}

func TestExtractionModel_ToDomain_InvalidStatus_ReturnsError(t *testing.T) {
	t.Parallel()

	model := validExtractionModel()
	model.Status = "BOGUS_STATUS"

	entity, err := model.ToDomain()

	assert.Error(t, err)
	assert.Nil(t, entity)
	assert.Contains(t, err.Error(), "parse extraction status")
}

func TestExtractionModel_ToDomain_NullIngestionJobID(t *testing.T) {
	t.Parallel()

	model := validExtractionModel()
	model.IngestionJobID = uuid.NullUUID{}

	entity, err := model.ToDomain()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, uuid.Nil, entity.IngestionJobID)
}

func TestExtractionModel_ToDomain_NullOptionalStrings(t *testing.T) {
	t.Parallel()

	model := validExtractionModel()
	model.FetcherJobID = sql.NullString{}
	model.StartDate = sql.NullString{}
	model.EndDate = sql.NullString{}
	model.ResultPath = sql.NullString{}
	model.ErrorMessage = sql.NullString{}

	entity, err := model.ToDomain()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Empty(t, entity.FetcherJobID)
	assert.Empty(t, entity.StartDate)
	assert.Empty(t, entity.EndDate)
	assert.Empty(t, entity.ResultPath)
	assert.Empty(t, entity.ErrorMessage)
}

func TestExtractionModel_ToDomain_InvalidTablesJSON(t *testing.T) {
	t.Parallel()

	model := validExtractionModel()
	model.Tables = []byte("{broken json")

	entity, err := model.ToDomain()

	assert.Error(t, err)
	assert.Nil(t, entity)
	assert.Contains(t, err.Error(), "unmarshal tables")
}

func TestExtractionModel_ToDomain_InvalidFiltersJSON(t *testing.T) {
	t.Parallel()

	model := validExtractionModel()
	model.Filters = []byte("{broken json")

	entity, err := model.ToDomain()

	assert.Error(t, err)
	assert.Nil(t, entity)
	assert.Contains(t, err.Error(), "unmarshal filters")
}

func TestExtractionModel_ToDomain_EmptyTables(t *testing.T) {
	t.Parallel()

	model := validExtractionModel()
	model.Tables = nil
	model.Filters = nil

	entity, err := model.ToDomain()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Nil(t, entity.Tables)
	assert.Nil(t, entity.Filters)
}

func TestExtractionFromDomain_ValidEntity(t *testing.T) {
	t.Parallel()

	entity := validExtractionEntity()
	model, err := FromDomain(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.Equal(t, entity.ID, model.ID)
	assert.Equal(t, entity.ConnectionID, model.ConnectionID)
	assert.True(t, model.IngestionJobID.Valid)
	assert.Equal(t, entity.IngestionJobID, model.IngestionJobID.UUID)
	assert.True(t, model.FetcherJobID.Valid)
	assert.Equal(t, entity.FetcherJobID, model.FetcherJobID.String)
	assert.True(t, model.StartDate.Valid)
	assert.Equal(t, entity.StartDate, model.StartDate.String)
	assert.True(t, model.EndDate.Valid)
	assert.Equal(t, entity.EndDate, model.EndDate.String)
	assert.Equal(t, "PENDING", model.Status)
	assert.True(t, model.ResultPath.Valid)
	assert.Equal(t, entity.ResultPath, model.ResultPath.String)
	assert.False(t, model.ErrorMessage.Valid, "empty string should produce invalid NullString")
	assert.Equal(t, entity.CreatedAt, model.CreatedAt)
	assert.Equal(t, entity.UpdatedAt, model.UpdatedAt)
}

func TestExtractionFromDomain_NilEntity_ReturnsError(t *testing.T) {
	t.Parallel()

	model, err := FromDomain(nil)

	assert.ErrorIs(t, err, ErrEntityRequired)
	assert.Nil(t, model)
}

func TestExtractionFromDomain_ZeroIngestionJobID(t *testing.T) {
	t.Parallel()

	entity := validExtractionEntity()
	entity.IngestionJobID = uuid.Nil

	model, err := FromDomain(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.False(t, model.IngestionJobID.Valid, "zero UUID should produce invalid NullUUID")
}

func TestExtractionFromDomain_NilFiltersUsesSQLNull(t *testing.T) {
	t.Parallel()

	entity := validExtractionEntity()
	entity.Filters = nil

	model, err := FromDomain(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.Nil(t, model.Filters)
}

func TestExtractionModel_RoundTrip_PreservesAllFields(t *testing.T) {
	t.Parallel()

	original := validExtractionEntity()

	model, err := FromDomain(original)
	require.NoError(t, err)
	require.NotNil(t, model)

	roundTripped, err := model.ToDomain()
	require.NoError(t, err)
	require.NotNil(t, roundTripped)

	assert.Equal(t, original.ID, roundTripped.ID)
	assert.Equal(t, original.ConnectionID, roundTripped.ConnectionID)
	assert.Equal(t, original.IngestionJobID, roundTripped.IngestionJobID)
	assert.Equal(t, original.FetcherJobID, roundTripped.FetcherJobID)
	assert.Equal(t, original.StartDate, roundTripped.StartDate)
	assert.Equal(t, original.EndDate, roundTripped.EndDate)
	assert.Equal(t, original.Status, roundTripped.Status)
	assert.Equal(t, original.ResultPath, roundTripped.ResultPath)
	assert.Equal(t, original.ErrorMessage, roundTripped.ErrorMessage)
	assert.Equal(t, original.CreatedAt, roundTripped.CreatedAt)
	assert.Equal(t, original.UpdatedAt, roundTripped.UpdatedAt)
}

func TestExtractionModel_RoundTrip_NilFilters(t *testing.T) {
	t.Parallel()

	original := validExtractionEntity()
	original.Filters = nil

	model, err := FromDomain(original)
	require.NoError(t, err)
	require.NotNil(t, model)

	roundTripped, err := model.ToDomain()
	require.NoError(t, err)
	require.NotNil(t, roundTripped)
	assert.Nil(t, model.Filters)
	assert.Nil(t, roundTripped.Filters)
}

func TestExtractionModel_RoundTrip_AllStatuses(t *testing.T) {
	t.Parallel()

	statuses := []vo.ExtractionStatus{
		vo.ExtractionStatusPending,
		vo.ExtractionStatusSubmitted,
		vo.ExtractionStatusExtracting,
		vo.ExtractionStatusComplete,
		vo.ExtractionStatusFailed,
		vo.ExtractionStatusCancelled,
	}

	for _, status := range statuses {
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()

			original := validExtractionEntity()
			original.Status = status

			model, err := FromDomain(original)
			require.NoError(t, err)
			assert.Equal(t, string(status), model.Status)

			roundTripped, err := model.ToDomain()
			require.NoError(t, err)
			assert.Equal(t, status, roundTripped.Status)
		})
	}
}

func TestNullStringToString_ValidString(t *testing.T) {
	t.Parallel()

	ns := sql.NullString{String: "hello world", Valid: true}

	assert.Equal(t, "hello world", nullStringToString(ns))
}

func TestNullStringToString_NullString(t *testing.T) {
	t.Parallel()

	ns := sql.NullString{}

	assert.Equal(t, "", nullStringToString(ns))
}

func TestNullStringToString_ValidEmpty(t *testing.T) {
	t.Parallel()

	ns := sql.NullString{String: "", Valid: true}

	assert.Equal(t, "", nullStringToString(ns))
}
