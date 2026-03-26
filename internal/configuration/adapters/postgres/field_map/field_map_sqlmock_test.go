//go:build unit

package field_map

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

var (
	errTestQuery = errors.New("query error")
	errTestExec  = errors.New("exec error")
)

func setupMock(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	finish := func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	}

	return repo, mock, finish
}

func createTestFieldMap(t *testing.T) *entities.FieldMap {
	t.Helper()

	now := time.Now().UTC()

	return &entities.FieldMap{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Mapping:   map[string]any{"external_id": "transaction_id", "amount": "payment_amount"},
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func TestNewRepository(t *testing.T) {
	t.Parallel()

	t.Run("with valid provider", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)

		require.NotNil(t, repo)
		assert.Equal(t, provider, repo.provider)
	})

	t.Run("with nil provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)

		require.NotNil(t, repo)
		assert.Nil(t, repo.provider)
	})
}

func TestRepository_Create_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()
	validEntity := &entities.FieldMap{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Mapping:   map[string]any{"field": "value"},
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.Create(ctx, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.Create(ctx, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil entity returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		result, err := repo.Create(ctx, nil)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrFieldMapEntityRequired)
	})
}

func TestRepository_CreateWithTx_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()
	validEntity := &entities.FieldMap{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Mapping:   map[string]any{"field": "value"},
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.CreateWithTx(ctx, nil, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.CreateWithTx(ctx, nil, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil entity returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		result, err := repo.CreateWithTx(ctx, nil, nil)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrFieldMapEntityRequired)
	})

	t.Run("nil transaction returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		result, err := repo.CreateWithTx(ctx, nil, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrTransactionRequired)
	})
}

func TestRepository_FindByID_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testID := uuid.New()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.FindByID(ctx, testID)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.FindByID(ctx, testID)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_FindBySourceID_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testSourceID := uuid.New()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.FindBySourceID(ctx, testSourceID)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.FindBySourceID(ctx, testSourceID)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_Update_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()
	validEntity := &entities.FieldMap{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Mapping:   map[string]any{"field": "value"},
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.Update(ctx, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.Update(ctx, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil entity returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		result, err := repo.Update(ctx, nil)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrFieldMapEntityRequired)
	})
}

func TestRepository_UpdateWithTx_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()
	validEntity := &entities.FieldMap{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Mapping:   map[string]any{"field": "value"},
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.UpdateWithTx(ctx, nil, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.UpdateWithTx(ctx, nil, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil entity returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		result, err := repo.UpdateWithTx(ctx, nil, nil)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrFieldMapEntityRequired)
	})

	t.Run("nil transaction returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		result, err := repo.UpdateWithTx(ctx, nil, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrTransactionRequired)
	})
}

func TestRepository_Delete_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testID := uuid.New()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		err := repo.Delete(ctx, testID)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		err := repo.Delete(ctx, testID)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_DeleteWithTx_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testID := uuid.New()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		err := repo.DeleteWithTx(ctx, nil, testID)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		err := repo.DeleteWithTx(ctx, nil, testID)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil transaction returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		err := repo.DeleteWithTx(ctx, nil, testID)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrTransactionRequired)
	})
}

func TestRepository_ExistsBySourceIDs_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.ExistsBySourceIDs(ctx, []uuid.UUID{uuid.New()})

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.ExistsBySourceIDs(ctx, []uuid.UUID{uuid.New()})

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("empty source IDs returns empty map", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		result, err := repo.ExistsBySourceIDs(ctx, []uuid.UUID{})

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Empty(t, result)
	})
}

func TestRepository_ProviderConnectionError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	connectionErr := errors.New("connection failed")

	t.Run("FindByID returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo := NewRepository(provider)
		result, err := repo.FindByID(ctx, uuid.New())

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, connectionErr)
	})

	t.Run("FindBySourceID returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo := NewRepository(provider)
		result, err := repo.FindBySourceID(ctx, uuid.New())

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, connectionErr)
	})

	t.Run("ExistsBySourceIDs returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo := NewRepository(provider)
		result, err := repo.ExistsBySourceIDs(ctx, []uuid.UUID{uuid.New()})

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, connectionErr)
	})
}

func TestModelConversion_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("model with empty mapping", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		entity := &entities.FieldMap{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			SourceID:  uuid.New(),
			Mapping:   map[string]any{},
			Version:   1,
			CreatedAt: now,
			UpdatedAt: now,
		}

		model, err := NewFieldMapPostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.NotNil(t, resultEntity.Mapping)
		require.Empty(t, resultEntity.Mapping)
	})

	t.Run("model with complex mapping", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		entity := &entities.FieldMap{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			SourceID:  uuid.New(),
			Mapping: map[string]any{
				"external_id":     "transaction_id",
				"amount":          "payment_amount",
				"date":            "transaction_date",
				"currency":        "payment_currency",
				"description":     "memo",
				"reference_id":    "ref_number",
				"counterparty":    "payer_name",
				"account_number":  "account",
				"transaction_fee": "fee",
			},
			Version:   1,
			CreatedAt: now,
			UpdatedAt: now,
		}

		model, err := NewFieldMapPostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.NotNil(t, resultEntity.Mapping)
		require.Len(t, resultEntity.Mapping, 9)
		require.Equal(t, "transaction_id", resultEntity.Mapping["external_id"])
	})

	t.Run("model preserves version", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		for _, version := range []int{1, 5, 10, 100} {
			entity := &entities.FieldMap{
				ID:        uuid.New(),
				ContextID: uuid.New(),
				SourceID:  uuid.New(),
				Mapping:   map[string]any{"field": "value"},
				Version:   version,
				CreatedAt: now,
				UpdatedAt: now,
			}

			model, err := NewFieldMapPostgreSQLModel(entity)
			require.NoError(t, err)

			resultEntity, err := model.ToEntity()
			require.NoError(t, err)
			require.Equal(t, version, resultEntity.Version)
		}
	})

	t.Run("model preserves timestamps", func(t *testing.T) {
		t.Parallel()

		createdAt := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
		updatedAt := time.Date(2024, 6, 20, 14, 45, 0, 0, time.UTC)
		entity := &entities.FieldMap{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			SourceID:  uuid.New(),
			Mapping:   map[string]any{"field": "value"},
			Version:   1,
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		}

		model, err := NewFieldMapPostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.Equal(t, createdAt, resultEntity.CreatedAt)
		require.Equal(t, updatedAt, resultEntity.UpdatedAt)
	})

	t.Run("model with nested mapping values", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		entity := &entities.FieldMap{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			SourceID:  uuid.New(),
			Mapping: map[string]any{
				"simple": "value",
				"nested": map[string]any{
					"level1": map[string]any{
						"level2": "deepValue",
					},
				},
				"array":  []any{"item1", "item2", "item3"},
				"number": float64(42),
			},
			Version:   1,
			CreatedAt: now,
			UpdatedAt: now,
		}

		model, err := NewFieldMapPostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.NotNil(t, resultEntity.Mapping)
		require.Equal(t, "value", resultEntity.Mapping["simple"])
	})
}

func TestMappingEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("mapping with special characters in keys", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		entity := &entities.FieldMap{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			SourceID:  uuid.New(),
			Mapping: map[string]any{
				"field.with.dots":        "value1",
				"field-with-dashes":      "value2",
				"field_with_underscores": "value3",
				"field with spaces":      "value4",
			},
			Version:   1,
			CreatedAt: now,
			UpdatedAt: now,
		}

		model, err := NewFieldMapPostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.Equal(t, "value1", resultEntity.Mapping["field.with.dots"])
		require.Equal(t, "value2", resultEntity.Mapping["field-with-dashes"])
		require.Equal(t, "value3", resultEntity.Mapping["field_with_underscores"])
		require.Equal(t, "value4", resultEntity.Mapping["field with spaces"])
	})

	t.Run("mapping with unicode keys and values", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		entity := &entities.FieldMap{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			SourceID:  uuid.New(),
			Mapping: map[string]any{
				"日本語":      "Japanese",
				"emoji🔥":   "fire",
				"mixed_キー": "value_値",
			},
			Version:   1,
			CreatedAt: now,
			UpdatedAt: now,
		}

		model, err := NewFieldMapPostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.Equal(t, "Japanese", resultEntity.Mapping["日本語"])
		require.Equal(t, "fire", resultEntity.Mapping["emoji🔥"])
		require.Equal(t, "value_値", resultEntity.Mapping["mixed_キー"])
	})
}

func TestRepository_Create_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	entity := createTestFieldMap(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO field_maps")).
		WithArgs(
			sqlmock.AnyArg(),
			entity.ContextID.String(),
			entity.SourceID.String(),
			sqlmock.AnyArg(),
			entity.Version,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	result, err := repo.Create(context.Background(), entity)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, entity.ContextID, result.ContextID)
	assert.Equal(t, entity.SourceID, result.SourceID)
	assert.Equal(t, entity.Version, result.Version)
}

func TestRepository_Create_ExecError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	entity := createTestFieldMap(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO field_maps")).
		WithArgs(
			sqlmock.AnyArg(),
			entity.ContextID.String(),
			entity.SourceID.String(),
			sqlmock.AnyArg(),
			entity.Version,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnError(errTestExec)
	mock.ExpectRollback()

	result, err := repo.Create(context.Background(), entity)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "create field map")
}

func TestRepository_FindByID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	now := time.Now().UTC()
	id := uuid.New()
	contextID := uuid.New()
	sourceID := uuid.New()
	mappingJSON := []byte(`{"external_id":"transaction_id"}`)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, context_id, source_id, mapping, version, created_at, updated_at FROM field_maps WHERE id = $1")).
		WithArgs(id.String()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "context_id", "source_id", "mapping", "version", "created_at", "updated_at"}).
			AddRow(id.String(), contextID.String(), sourceID.String(), mappingJSON, 1, now, now))

	result, err := repo.FindByID(context.Background(), id)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, id, result.ID)
	assert.Equal(t, contextID, result.ContextID)
	assert.Equal(t, sourceID, result.SourceID)
	assert.Equal(t, 1, result.Version)
}

func TestRepository_FindByID_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	id := uuid.New()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, context_id, source_id, mapping, version, created_at, updated_at FROM field_maps WHERE id = $1")).
		WithArgs(id.String()).
		WillReturnError(sql.ErrNoRows)

	result, err := repo.FindByID(context.Background(), id)

	require.Error(t, err)
	require.Nil(t, result)
	assert.ErrorIs(t, err, sql.ErrNoRows)
}

func TestRepository_FindByID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	id := uuid.New()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, context_id, source_id, mapping, version, created_at, updated_at FROM field_maps WHERE id = $1")).
		WithArgs(id.String()).
		WillReturnError(errTestQuery)

	result, err := repo.FindByID(context.Background(), id)

	require.Error(t, err)
	require.Nil(t, result)
}

func TestRepository_FindBySourceID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	now := time.Now().UTC()
	id := uuid.New()
	contextID := uuid.New()
	sourceID := uuid.New()
	mappingJSON := []byte(`{"field":"value"}`)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, context_id, source_id, mapping, version, created_at, updated_at FROM field_maps WHERE source_id = $1 ORDER BY version DESC LIMIT 1")).
		WithArgs(sourceID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "context_id", "source_id", "mapping", "version", "created_at", "updated_at"}).
			AddRow(id.String(), contextID.String(), sourceID.String(), mappingJSON, 2, now, now))

	result, err := repo.FindBySourceID(context.Background(), sourceID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, sourceID, result.SourceID)
	assert.Equal(t, 2, result.Version)
}

func TestRepository_FindBySourceID_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	sourceID := uuid.New()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, context_id, source_id, mapping, version, created_at, updated_at FROM field_maps WHERE source_id = $1 ORDER BY version DESC LIMIT 1")).
		WithArgs(sourceID.String()).
		WillReturnError(sql.ErrNoRows)

	result, err := repo.FindBySourceID(context.Background(), sourceID)

	require.Error(t, err)
	require.Nil(t, result)
	assert.ErrorIs(t, err, sql.ErrNoRows)
}

func TestRepository_FindBySourceID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	sourceID := uuid.New()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, context_id, source_id, mapping, version, created_at, updated_at FROM field_maps WHERE source_id = $1 ORDER BY version DESC LIMIT 1")).
		WithArgs(sourceID.String()).
		WillReturnError(errTestQuery)

	result, err := repo.FindBySourceID(context.Background(), sourceID)

	require.Error(t, err)
	require.Nil(t, result)
}

func TestRepository_Update_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	entity := createTestFieldMap(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE field_maps SET mapping = $1, version = $2, updated_at = $3 WHERE id = $4")).
		WithArgs(
			sqlmock.AnyArg(),
			entity.Version,
			sqlmock.AnyArg(),
			entity.ID.String(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := repo.Update(context.Background(), entity)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, entity.ID, result.ID)
}

func TestRepository_Update_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	entity := createTestFieldMap(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE field_maps SET mapping = $1, version = $2, updated_at = $3 WHERE id = $4")).
		WithArgs(
			sqlmock.AnyArg(),
			entity.Version,
			sqlmock.AnyArg(),
			entity.ID.String(),
		).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	result, err := repo.Update(context.Background(), entity)

	require.Error(t, err)
	require.Nil(t, result)
	assert.ErrorIs(t, err, sql.ErrNoRows)
}

func TestRepository_Update_ExecError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	entity := createTestFieldMap(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE field_maps SET mapping = $1, version = $2, updated_at = $3 WHERE id = $4")).
		WithArgs(
			sqlmock.AnyArg(),
			entity.Version,
			sqlmock.AnyArg(),
			entity.ID.String(),
		).
		WillReturnError(errTestExec)
	mock.ExpectRollback()

	result, err := repo.Update(context.Background(), entity)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "update field map")
}

func TestRepository_Delete_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM field_maps WHERE id = $1")).
		WithArgs(id.String()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.Delete(context.Background(), id)

	require.NoError(t, err)
}

func TestRepository_Delete_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM field_maps WHERE id = $1")).
		WithArgs(id.String()).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	err := repo.Delete(context.Background(), id)

	require.Error(t, err)
	assert.ErrorIs(t, err, sql.ErrNoRows)
}

func TestRepository_Delete_ExecError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM field_maps WHERE id = $1")).
		WithArgs(id.String()).
		WillReturnError(errTestExec)
	mock.ExpectRollback()

	err := repo.Delete(context.Background(), id)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete field map")
}

func TestRepository_ExistsBySourceIDs_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	sourceID1 := uuid.New()
	sourceID2 := uuid.New()
	sourceID3 := uuid.New()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT DISTINCT source_id FROM field_maps WHERE source_id IN")).
		WithArgs(sourceID1.String(), sourceID2.String(), sourceID3.String()).
		WillReturnRows(sqlmock.NewRows([]string{"source_id"}).
			AddRow(sourceID1.String()).
			AddRow(sourceID3.String()))

	result, err := repo.ExistsBySourceIDs(
		context.Background(),
		[]uuid.UUID{sourceID1, sourceID2, sourceID3},
	)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result[sourceID1])
	assert.False(t, result[sourceID2])
	assert.True(t, result[sourceID3])
}

func TestRepository_ExistsBySourceIDs_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	sourceID := uuid.New()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT DISTINCT source_id FROM field_maps WHERE source_id IN")).
		WithArgs(sourceID.String()).
		WillReturnError(errTestQuery)

	result, err := repo.ExistsBySourceIDs(context.Background(), []uuid.UUID{sourceID})

	require.Error(t, err)
	require.Nil(t, result)
}

func TestRepository_ExistsBySourceIDs_DeduplicatesInput(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	sourceID := uuid.New()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT DISTINCT source_id FROM field_maps WHERE source_id IN")).
		WithArgs(sourceID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"source_id"}).
			AddRow(sourceID.String()))

	result, err := repo.ExistsBySourceIDs(
		context.Background(),
		[]uuid.UUID{sourceID, sourceID, sourceID},
	)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result[sourceID])
}

func TestScanFieldMap_Success(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	id := uuid.New()
	contextID := uuid.New()
	sourceID := uuid.New()
	mappingJSON := []byte(`{"field":"value"}`)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"id", "context_id", "source_id", "mapping", "version", "created_at", "updated_at"}).
			AddRow(id.String(), contextID.String(), sourceID.String(), mappingJSON, 1, now, now))

	rows, err := db.Query("SELECT")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanFieldMap(rows)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, id, result.ID)
	assert.Equal(t, contextID, result.ContextID)
	assert.Equal(t, sourceID, result.SourceID)
	assert.Equal(t, 1, result.Version)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanFieldMap_ScanError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"id", "context_id", "source_id"}).
			AddRow("invalid-uuid", "invalid", "invalid"))

	rows, err := db.Query("SELECT")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanFieldMap(rows)

	require.Error(t, err)
	require.Nil(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanFieldMap_InvalidUUID(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	mappingJSON := []byte(`{"field":"value"}`)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"id", "context_id", "source_id", "mapping", "version", "created_at", "updated_at"}).
			AddRow("not-a-uuid", uuid.New().String(), uuid.New().String(), mappingJSON, 1, now, now),
		)

	rows, err := db.Query("SELECT")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanFieldMap(rows)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "parsing ID")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanFieldMap_InvalidMappingJSON(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	id := uuid.New()
	contextID := uuid.New()
	sourceID := uuid.New()
	invalidJSON := []byte(`{invalid json}`)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"id", "context_id", "source_id", "mapping", "version", "created_at", "updated_at"}).
			AddRow(id.String(), contextID.String(), sourceID.String(), invalidJSON, 1, now, now))

	rows, err := db.Query("SELECT")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanFieldMap(rows)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "unmarshal mapping")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_CreateWithTx_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn := testutil.NewClientWithResolver(dbresolver.New(dbresolver.WithPrimaryDBs(db)))

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{PostgresConn: conn, Tx: tx}
	repo := NewRepository(provider)

	entity := createTestFieldMap(t)

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO field_maps")).
		WithArgs(
			sqlmock.AnyArg(),
			entity.ContextID.String(),
			entity.SourceID.String(),
			sqlmock.AnyArg(),
			entity.Version,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := repo.CreateWithTx(context.Background(), tx, entity)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, entity.ContextID, result.ContextID)
}

func TestRepository_UpdateWithTx_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn := testutil.NewClientWithResolver(dbresolver.New(dbresolver.WithPrimaryDBs(db)))

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{PostgresConn: conn, Tx: tx}
	repo := NewRepository(provider)

	entity := createTestFieldMap(t)

	mock.ExpectExec(regexp.QuoteMeta("UPDATE field_maps SET mapping = $1, version = $2, updated_at = $3 WHERE id = $4")).
		WithArgs(
			sqlmock.AnyArg(),
			entity.Version,
			sqlmock.AnyArg(),
			entity.ID.String(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	result, err := repo.UpdateWithTx(context.Background(), tx, entity)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, entity.ID, result.ID)
}

func TestRepository_DeleteWithTx_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn := testutil.NewClientWithResolver(dbresolver.New(dbresolver.WithPrimaryDBs(db)))

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{PostgresConn: conn, Tx: tx}
	repo := NewRepository(provider)

	id := uuid.New()

	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM field_maps WHERE id = $1")).
		WithArgs(id.String()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = repo.DeleteWithTx(context.Background(), tx, id)

	require.NoError(t, err)
}

func TestRepository_DeleteWithTx_NotFound(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn := testutil.NewClientWithResolver(dbresolver.New(dbresolver.WithPrimaryDBs(db)))

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{PostgresConn: conn, Tx: tx}
	repo := NewRepository(provider)

	id := uuid.New()

	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM field_maps WHERE id = $1")).
		WithArgs(id.String()).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = repo.DeleteWithTx(context.Background(), tx, id)

	require.Error(t, err)
	assert.ErrorIs(t, err, sql.ErrNoRows)
}

func TestExistsBySourceIDsBatch_ScanError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conn := testutil.NewClientWithResolver(dbresolver.New(dbresolver.WithPrimaryDBs(db)))
	provider := &testutil.MockInfrastructureProvider{PostgresConn: conn}
	repo := NewRepository(provider)

	sourceID := uuid.New()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT DISTINCT source_id FROM field_maps WHERE source_id IN")).
		WithArgs(sourceID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"source_id"}).
			AddRow("not-a-valid-uuid"))

	result, err := repo.ExistsBySourceIDs(context.Background(), []uuid.UUID{sourceID})

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "parse source ID")
}
