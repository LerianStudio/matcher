//go:build unit

package job

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgcommon "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
)

func TestRepository_PostgreSQLNilConnection(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	_, err := repo.Create(ctx, &entities.IngestionJob{})
	require.ErrorIs(t, err, errRepoNotInit)

	_, err = repo.FindByID(ctx, uuid.New())
	require.ErrorIs(t, err, errRepoNotInit)

	_, _, err = repo.FindByContextID(ctx, uuid.New(), repositories.CursorFilter{Limit: 10})
	require.ErrorIs(t, err, errRepoNotInit)

	_, err = repo.Update(ctx, &entities.IngestionJob{})
	require.ErrorIs(t, err, errRepoNotInit)

	err = repo.WithTx(ctx, func(tx *sql.Tx) error { return nil })
	require.ErrorIs(t, err, errRepoNotInit)
}

func TestRepository_PostgreSQLNilEntity(t *testing.T) {
	t.Parallel()

	// Nil connection check happens before entity check
	repo := &Repository{}
	_, err := repo.Create(context.Background(), nil)
	require.ErrorIs(t, err, errRepoNotInit)

	_, err = repo.Update(context.Background(), nil)
	require.ErrorIs(t, err, errRepoNotInit)
}

func TestRepository_PostgreSQLSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"errJobEntityRequired", errJobEntityRequired},
		{"errJobModelRequired", errJobModelRequired},
		{"errInvalidJobStatus", errInvalidJobStatus},
		{"errRepoNotInit", errRepoNotInit},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			require.NotEmpty(t, tt.err.Error())
		})
	}
}

func TestNewJobPostgreSQLModel_Success(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	completedAt := now.Add(time.Hour)
	jobID := uuid.New()
	contextID := uuid.New()
	sourceID := uuid.New()

	entity := &entities.IngestionJob{
		ID:          jobID,
		ContextID:   contextID,
		SourceID:    sourceID,
		Status:      value_objects.JobStatusCompleted,
		StartedAt:   now,
		CompletedAt: &completedAt,
		Metadata: entities.JobMetadata{
			FileName:   "test.csv",
			FileSize:   1024,
			TotalRows:  100,
			FailedRows: 0,
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewJobPostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.Equal(t, jobID.String(), model.ID)
	assert.Equal(t, contextID.String(), model.ContextID)
	assert.Equal(t, sourceID.String(), model.SourceID)
	assert.Equal(t, "COMPLETED", model.Status)
	assert.Equal(t, now, model.StartedAt)
	assert.True(t, model.CompletedAt.Valid)
	assert.Equal(t, completedAt, model.CompletedAt.Time)
	assert.NotNil(t, model.Metadata)
	assert.Contains(t, string(model.Metadata), "fileName")
}

func TestNewJobPostgreSQLModel_NilEntity(t *testing.T) {
	t.Parallel()

	model, err := NewJobPostgreSQLModel(nil)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, errJobEntityRequired)
}

func TestNewJobPostgreSQLModel_GeneratesIDWhenNil(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &entities.IngestionJob{
		ID:        uuid.Nil,
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Status:    value_objects.JobStatusQueued,
		StartedAt: now,
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewJobPostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.NotEmpty(t, model.ID)
	parsedID, err := uuid.Parse(model.ID)
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, parsedID)
}

func TestNewJobPostgreSQLModel_SetsTimestampsWhenZero(t *testing.T) {
	t.Parallel()

	entity := &entities.IngestionJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Status:    value_objects.JobStatusProcessing,
	}

	model, err := NewJobPostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.False(t, model.CreatedAt.IsZero())
	require.False(t, model.UpdatedAt.IsZero())
	require.False(t, model.StartedAt.IsZero())
	require.True(t, model.UpdatedAt.Equal(model.CreatedAt))
}

func TestNewJobPostgreSQLModel_InvalidStatus(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &entities.IngestionJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Status:    value_objects.JobStatus("INVALID"),
		StartedAt: now,
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewJobPostgreSQLModel(entity)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, errInvalidJobStatus)
}

func TestNewJobPostgreSQLModel_NilCompletedAt(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &entities.IngestionJob{
		ID:          uuid.New(),
		ContextID:   uuid.New(),
		SourceID:    uuid.New(),
		Status:      value_objects.JobStatusProcessing,
		StartedAt:   now,
		CompletedAt: nil,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	model, err := NewJobPostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.False(t, model.CompletedAt.Valid)
}

func TestJobModelToEntity_Success(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	jobID := uuid.New()
	contextID := uuid.New()
	sourceID := uuid.New()

	model := &pgcommon.JobPostgreSQLModel{
		ID:          jobID.String(),
		ContextID:   contextID.String(),
		SourceID:    sourceID.String(),
		Status:      "COMPLETED",
		StartedAt:   now,
		CompletedAt: sql.NullTime{Time: now.Add(time.Hour), Valid: true},
		Metadata: []byte(
			`{"fileName":"test.csv","fileSize":1024,"totalRows":50,"failedRows":0}`,
		),
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := jobModelToEntity(model)

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, jobID, entity.ID)
	assert.Equal(t, contextID, entity.ContextID)
	assert.Equal(t, sourceID, entity.SourceID)
	assert.Equal(t, value_objects.JobStatusCompleted, entity.Status)
	assert.Equal(t, now, entity.StartedAt)
	require.NotNil(t, entity.CompletedAt)
	assert.Equal(t, "test.csv", entity.Metadata.FileName)
	assert.Equal(t, int64(1024), entity.Metadata.FileSize)
	assert.Equal(t, 50, entity.Metadata.TotalRows)
}

func TestJobModelToEntity_NilModel(t *testing.T) {
	t.Parallel()

	entity, err := jobModelToEntity(nil)

	require.Error(t, err)
	require.Nil(t, entity)
	require.ErrorIs(t, err, errJobModelRequired)
}

func TestJobModelToEntity_InvalidID(t *testing.T) {
	t.Parallel()

	model := &pgcommon.JobPostgreSQLModel{
		ID:        "not-a-uuid",
		ContextID: uuid.New().String(),
		SourceID:  uuid.New().String(),
		Status:    "QUEUED",
	}

	entity, err := jobModelToEntity(model)

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parsing ID")
}

func TestJobModelToEntity_InvalidContextID(t *testing.T) {
	t.Parallel()

	model := &pgcommon.JobPostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: "invalid-context",
		SourceID:  uuid.New().String(),
		Status:    "QUEUED",
	}

	entity, err := jobModelToEntity(model)

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parsing ContextID")
}

func TestJobModelToEntity_InvalidSourceID(t *testing.T) {
	t.Parallel()

	model := &pgcommon.JobPostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: uuid.New().String(),
		SourceID:  "invalid-source",
		Status:    "QUEUED",
	}

	entity, err := jobModelToEntity(model)

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parsing SourceID")
}

func TestJobModelToEntity_InvalidStatus(t *testing.T) {
	t.Parallel()

	model := &pgcommon.JobPostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: uuid.New().String(),
		SourceID:  uuid.New().String(),
		Status:    "INVALID_STATUS",
	}

	entity, err := jobModelToEntity(model)

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parsing Status")
}

func TestJobModelToEntity_InvalidMetadataJSON(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &pgcommon.JobPostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: uuid.New().String(),
		SourceID:  uuid.New().String(),
		Status:    "PROCESSING",
		Metadata:  []byte(`{invalid json}`),
		StartedAt: now,
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := jobModelToEntity(model)

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parsing Metadata")
}

func TestJobModelToEntity_NullCompletedAt(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &pgcommon.JobPostgreSQLModel{
		ID:          uuid.New().String(),
		ContextID:   uuid.New().String(),
		SourceID:    uuid.New().String(),
		Status:      "PROCESSING",
		StartedAt:   now,
		CompletedAt: sql.NullTime{Valid: false},
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	entity, err := jobModelToEntity(model)

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Nil(t, entity.CompletedAt)
}

func TestJobModelToEntity_EmptyMetadata(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &pgcommon.JobPostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: uuid.New().String(),
		SourceID:  uuid.New().String(),
		Status:    "QUEUED",
		StartedAt: now,
		Metadata:  nil,
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := jobModelToEntity(model)

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, 0, entity.Metadata.TotalRows)
}

func TestRepository_CreateWithTx_Validations(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	t.Run("nil repository provider", func(t *testing.T) {
		t.Parallel()

		_, err := repo.CreateWithTx(ctx, nil, &entities.IngestionJob{})
		require.ErrorIs(t, err, errRepoNotInit)
	})
}

func TestRepository_UpdateWithTx_Validations(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	t.Run("nil repository provider", func(t *testing.T) {
		t.Parallel()

		_, err := repo.UpdateWithTx(ctx, nil, &entities.IngestionJob{})
		require.ErrorIs(t, err, errRepoNotInit)
	})
}
