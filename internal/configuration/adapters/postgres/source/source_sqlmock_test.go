//go:build unit

package source

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func encodeLibCommonsTestCursor(t *testing.T, id uuid.UUID, direction string) string {
	t.Helper()

	encoded, err := libHTTP.EncodeCursor(libHTTP.Cursor{ID: id.String(), Direction: direction})
	require.NoError(t, err)

	return encoded
}

func setupMock(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)

	repo, err := NewRepository(provider)
	require.NoError(t, err)

	return repo, mock, func() { db.Close() }
}

func TestNewRepository(t *testing.T) {
	t.Parallel()

	t.Run("with valid provider", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo, err := NewRepository(provider)

		require.NoError(t, err)
		require.NotNil(t, repo)
		assert.Equal(t, provider, repo.provider)
	})

	t.Run("with nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo, err := NewRepository(nil)

		require.Error(t, err)
		require.Nil(t, repo)
		require.ErrorIs(t, err, ErrConnectionRequired)
	})
}

func TestRepository_Create_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()
	validEntity := &entities.ReconciliationSource{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Name:      "Test Source",
		Type:      value_objects.SourceTypeLedger,
		Side:      sharedfee.MatchingSideLeft,
		Config:    map[string]any{"key": "value"},
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
		repo, err := NewRepository(provider)
		require.NoError(t, err)

		result, err := repo.Create(ctx, nil)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrSourceEntityRequired)
	})
}

func TestRepository_CreateWithTx_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()
	validEntity := &entities.ReconciliationSource{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Name:      "Test Source",
		Type:      value_objects.SourceTypeLedger,
		Side:      sharedfee.MatchingSideLeft,
		Config:    map[string]any{"key": "value"},
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
		repo, err := NewRepository(provider)
		require.NoError(t, err)

		result, err := repo.CreateWithTx(ctx, nil, nil)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrSourceEntityRequired)
	})

	t.Run("nil transaction returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo, err := NewRepository(provider)
		require.NoError(t, err)

		result, err := repo.CreateWithTx(ctx, nil, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrTransactionRequired)
	})
}

func TestRepository_FindByID_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testContextID := uuid.New()
	testID := uuid.New()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.FindByID(ctx, testContextID, testID)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.FindByID(ctx, testContextID, testID)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_FindByContextID_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testContextID := uuid.New()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, _, err := repo.FindByContextID(ctx, testContextID, "", 10)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, _, err := repo.FindByContextID(ctx, testContextID, "", 10)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_FindByContextIDAndType_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testContextID := uuid.New()
	sourceType := value_objects.SourceTypeLedger

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, _, err := repo.FindByContextIDAndType(ctx, testContextID, sourceType, "", 10)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, _, err := repo.FindByContextIDAndType(ctx, testContextID, sourceType, "", 10)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_Update_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()
	validEntity := &entities.ReconciliationSource{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Name:      "Test Source",
		Type:      value_objects.SourceTypeLedger,
		Side:      sharedfee.MatchingSideLeft,
		Config:    map[string]any{"key": "value"},
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
		repo, err := NewRepository(provider)
		require.NoError(t, err)

		result, err := repo.Update(ctx, nil)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrSourceEntityRequired)
	})
}

func TestRepository_UpdateWithTx_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()
	validEntity := &entities.ReconciliationSource{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Name:      "Test Source",
		Type:      value_objects.SourceTypeLedger,
		Side:      sharedfee.MatchingSideLeft,
		Config:    map[string]any{"key": "value"},
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
		repo, err := NewRepository(provider)
		require.NoError(t, err)

		result, err := repo.UpdateWithTx(ctx, nil, nil)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrSourceEntityRequired)
	})

	t.Run("nil transaction returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo, err := NewRepository(provider)
		require.NoError(t, err)

		result, err := repo.UpdateWithTx(ctx, nil, validEntity)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrTransactionRequired)
	})
}

func TestRepository_Delete_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testContextID := uuid.New()
	testID := uuid.New()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		err := repo.Delete(ctx, testContextID, testID)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		err := repo.Delete(ctx, testContextID, testID)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_DeleteWithTx_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testContextID := uuid.New()
	testID := uuid.New()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		err := repo.DeleteWithTx(ctx, nil, testContextID, testID)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		err := repo.DeleteWithTx(ctx, nil, testContextID, testID)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil transaction returns error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo, err := NewRepository(provider)
		require.NoError(t, err)

		err = repo.DeleteWithTx(ctx, nil, testContextID, testID)

		require.Error(t, err)
		require.ErrorIs(t, err, ErrTransactionRequired)
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
		repo, err := NewRepository(provider)
		require.NoError(t, err)

		result, err := repo.FindByID(ctx, uuid.New(), uuid.New())

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, connectionErr)
	})

	t.Run("FindByContextID returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo, err := NewRepository(provider)
		require.NoError(t, err)

		result, _, err := repo.FindByContextID(ctx, uuid.New(), "", 10)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, connectionErr)
	})

	t.Run("FindByContextIDAndType returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo, err := NewRepository(provider)
		require.NoError(t, err)

		result, _, err := repo.FindByContextIDAndType(
			ctx,
			uuid.New(),
			value_objects.SourceTypeLedger,
			"",
			10,
		)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, connectionErr)
	})

	t.Run("Delete returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo, err := NewRepository(provider)
		require.NoError(t, err)

		err = repo.Delete(ctx, uuid.New(), uuid.New())

		require.Error(t, err)
		require.ErrorIs(t, err, connectionErr)
	})
}

func TestModelConversion_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("model with empty config", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		entity := &entities.ReconciliationSource{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			Name:      "Test Source",
			Type:      value_objects.SourceTypeLedger,
			Side:      sharedfee.MatchingSideLeft,
			Config:    map[string]any{},
			CreatedAt: now,
			UpdatedAt: now,
		}

		model, err := NewSourcePostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.NotNil(t, resultEntity.Config)
		require.Empty(t, resultEntity.Config)
	})

	t.Run("model with complex config", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		entity := &entities.ReconciliationSource{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			Name:      "Test Source",
			Type:      value_objects.SourceTypeGateway,
			Side:      sharedfee.MatchingSideRight,
			Config: map[string]any{
				"string":  "value",
				"number":  float64(42),
				"boolean": true,
				"nested": map[string]any{
					"key": "nestedValue",
				},
				"array": []any{"item1", "item2"},
			},
			CreatedAt: now,
			UpdatedAt: now,
		}

		model, err := NewSourcePostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.NotNil(t, resultEntity.Config)
		require.Equal(t, "value", resultEntity.Config["string"])
		require.InDelta(t, float64(42), resultEntity.Config["number"], 0.01)
		require.Equal(t, true, resultEntity.Config["boolean"])
	})

	t.Run("model with all source types", func(t *testing.T) {
		t.Parallel()

		sourceTypes := []value_objects.SourceType{
			value_objects.SourceTypeLedger,
			value_objects.SourceTypeGateway,
			value_objects.SourceTypeBank,
			value_objects.SourceTypeCustom,
		}

		for _, srcType := range sourceTypes {
			now := time.Now().UTC()
			entity := &entities.ReconciliationSource{
				ID:        uuid.New(),
				ContextID: uuid.New(),
				Name:      "Test " + srcType.String(),
				Type:      srcType,
				Config:    map[string]any{},
				CreatedAt: now,
				UpdatedAt: now,
			}

			model, err := NewSourcePostgreSQLModel(entity)
			require.NoError(t, err)

			resultEntity, err := model.ToEntity()
			require.NoError(t, err)
			require.Equal(t, srcType, resultEntity.Type)
		}
	})

	t.Run("model preserves timestamps", func(t *testing.T) {
		t.Parallel()

		createdAt := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
		updatedAt := time.Date(2024, 6, 20, 14, 45, 0, 0, time.UTC)
		entity := &entities.ReconciliationSource{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			Name:      "Test Source",
			Type:      value_objects.SourceTypeLedger,
			Side:      sharedfee.MatchingSideLeft,
			Config:    map[string]any{},
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		}

		model, err := NewSourcePostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.Equal(t, createdAt, resultEntity.CreatedAt)
		require.Equal(t, updatedAt, resultEntity.UpdatedAt)
	})

	t.Run("model with special characters in name", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		entity := &entities.ReconciliationSource{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			Name:      "Test Source with 'quotes' and \"double quotes\" and special chars: @#$%",
			Type:      value_objects.SourceTypeLedger,
			Side:      sharedfee.MatchingSideLeft,
			Config:    map[string]any{},
			CreatedAt: now,
			UpdatedAt: now,
		}

		model, err := NewSourcePostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.Equal(t, entity.Name, resultEntity.Name)
	})
}

func createValidSourceEntity(t *testing.T) *entities.ReconciliationSource {
	t.Helper()

	now := time.Now().UTC()
	return &entities.ReconciliationSource{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Name:      "Test Source",
		Type:      value_objects.SourceTypeLedger,
		Side:      sharedfee.MatchingSideLeft,
		Config:    map[string]any{"key": "value"},
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func TestExecuteCreate_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := createValidSourceEntity(t)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO reconciliation_sources").
		WillReturnResult(sqlmock.NewResult(1, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	result, err := repo.executeCreate(ctx, tx, entity)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, entity.ID, result.ID)
	require.Equal(t, entity.Name, result.Name)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteCreate_InsertError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := createValidSourceEntity(t)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO reconciliation_sources").
		WillReturnError(errors.New("duplicate key violation"))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	result, err := repo.executeCreate(ctx, tx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "duplicate key violation")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteCreate_NilEntity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	result, err := repo.executeCreate(ctx, tx, nil)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrSourceEntityRequired)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteCreate_EntityWithZeroID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	entity := createValidSourceEntity(t)
	entity.ID = uuid.Nil

	provider := &testutil.MockInfrastructureProvider{}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	result, err := repo.executeCreate(ctx, tx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrSourceEntityIDRequired)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteUpdate_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := createValidSourceEntity(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE reconciliation_sources").
		WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	result, err := repo.executeUpdate(ctx, tx, entity)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, entity.ID, result.ID)
	require.Equal(t, entity.Name, result.Name)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteUpdate_NoRowsAffected(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := createValidSourceEntity(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE reconciliation_sources").
		WillReturnResult(sqlmock.NewResult(0, 0))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	result, err := repo.executeUpdate(ctx, tx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteUpdate_DatabaseError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := createValidSourceEntity(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE reconciliation_sources").
		WillReturnError(errors.New("database connection lost"))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	result, err := repo.executeUpdate(ctx, tx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "database connection lost")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteUpdate_NilEntity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	result, err := repo.executeUpdate(ctx, tx, nil)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrSourceEntityRequired)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteUpdate_EntityWithZeroID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	entity := createValidSourceEntity(t)
	entity.ID = uuid.Nil

	provider := &testutil.MockInfrastructureProvider{}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	result, err := repo.executeUpdate(ctx, tx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrSourceEntityIDRequired)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteUpdate_RowsAffectedError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := createValidSourceEntity(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE reconciliation_sources").
		WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected error")))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	result, err := repo.executeUpdate(ctx, tx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "rows affected error")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteDelete_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_sources").
		WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	deleted, err := repo.executeDelete(ctx, tx, contextID, id)

	require.NoError(t, err)
	require.True(t, deleted)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteDelete_NoRowsAffected(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_sources").
		WillReturnResult(sqlmock.NewResult(0, 0))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	deleted, err := repo.executeDelete(ctx, tx, contextID, id)

	require.Error(t, err)
	require.False(t, deleted)
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteDelete_DatabaseError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_sources").
		WillReturnError(errors.New("foreign key constraint"))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	deleted, err := repo.executeDelete(ctx, tx, contextID, id)

	require.Error(t, err)
	require.False(t, deleted)
	require.Contains(t, err.Error(), "foreign key constraint")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteDelete_RowsAffectedError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_sources").
		WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected error")))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	deleted, err := repo.executeDelete(ctx, tx, contextID, id)

	require.Error(t, err)
	require.False(t, deleted)
	require.Contains(t, err.Error(), "rows affected error")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanSource_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()
	id := uuid.New()
	contextID := uuid.New()
	configJSON := []byte(`{"key":"value"}`)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
	}).AddRow(
		id.String(), contextID.String(), "Test Source", "LEDGER", "LEFT", configJSON, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanSource(sqlRows)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, id, result.ID)
	require.Equal(t, contextID, result.ContextID)
	require.Equal(t, "Test Source", result.Name)
	require.Equal(t, value_objects.SourceTypeLedger, result.Type)
	require.Equal(t, "value", result.Config["key"])
}

func TestScanSource_InvalidID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()
	configJSON := []byte(`{}`)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
	}).AddRow(
		"invalid-uuid", uuid.New().String(), "Test Source", "LEDGER", "LEFT", configJSON, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanSource(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "parse ID")
}

func TestScanSource_InvalidContextID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()
	configJSON := []byte(`{}`)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
	}).AddRow(
		uuid.New().String(), "invalid-uuid", "Test Source", "LEDGER", "LEFT", configJSON, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanSource(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "parse context ID")
}

func TestScanSource_InvalidType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()
	configJSON := []byte(`{}`)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
	}).AddRow(
		uuid.New().
			String(),
		uuid.New().String(), "Test Source", "INVALID_TYPE", "LEFT", configJSON, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanSource(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "parse source type")
}

func TestScanSource_InvalidConfig(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()
	invalidConfigJSON := []byte(`{invalid json}`)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
	}).AddRow(
		uuid.New().
			String(),
		uuid.New().String(), "Test Source", "LEDGER", "LEFT", invalidConfigJSON, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanSource(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "unmarshal config")
}

func TestScanSource_ScanError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
	}).AddRow(
		nil, nil, nil, nil, nil, nil, nil, nil,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanSource(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
}

func TestScanSource_AllSourceTypes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		typeStr    string
		sourceType value_objects.SourceType
	}{
		{"ledger", "LEDGER", value_objects.SourceTypeLedger},
		{"gateway", "GATEWAY", value_objects.SourceTypeGateway},
		{"bank", "BANK", value_objects.SourceTypeBank},
		{"custom", "CUSTOM", value_objects.SourceTypeCustom},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			defer db.Close()

			now := time.Now().UTC()
			configJSON := []byte(`{}`)

			rows := sqlmock.NewRows([]string{
				"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
			}).AddRow(
				uuid.New().
					String(),
				uuid.New().String(), "Test Source", tc.typeStr, "LEFT", configJSON, now, now,
			)
			mock.ExpectQuery("SELECT").WillReturnRows(rows)

			sqlRows, err := db.QueryContext(ctx, "SELECT 1")
			require.NoError(t, err)

			defer sqlRows.Close()

			require.True(t, sqlRows.Next())

			result, err := scanSource(sqlRows)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, tc.sourceType, result.Type)
		})
	}
}

func TestScanSource_EmptyConfig(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()
	emptyConfigJSON := []byte(`{}`)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
	}).AddRow(
		uuid.New().
			String(),
		uuid.New().String(), "Test Source", "LEDGER", "LEFT", emptyConfigJSON, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanSource(sqlRows)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Config)
	require.Empty(t, result.Config)
}

func TestModelCreation_ValidEntity(t *testing.T) {
	t.Parallel()

	entity := createValidSourceEntity(t)

	model, err := NewSourcePostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.Equal(t, entity.ID.String(), model.ID)
	require.Equal(t, entity.ContextID.String(), model.ContextID)
	require.Equal(t, entity.Name, model.Name)
	require.Equal(t, entity.Type.String(), model.Type)
}

func TestModelCreation_ZeroTimestampsAutoFill(t *testing.T) {
	t.Parallel()

	entity := createValidSourceEntity(t)
	entity.CreatedAt = time.Time{}
	entity.UpdatedAt = time.Time{}

	model, err := NewSourcePostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.False(t, model.CreatedAt.IsZero())
	require.False(t, model.UpdatedAt.IsZero())
}

func setupMockWithReplica(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db), dbresolver.WithReplicaDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := &testutil.MockInfrastructureProvider{PostgresConn: conn}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	return repo, mock, func() { db.Close() }
}

func TestRepository_Create_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	entity := createValidSourceEntity(t)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO reconciliation_sources").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	result, err := repo.Create(ctx, entity)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, entity.ID, result.ID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_Create_InsertErrorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	entity := createValidSourceEntity(t)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO reconciliation_sources").
		WillReturnError(errors.New("duplicate key"))
	mock.ExpectRollback()

	result, err := repo.Create(ctx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "create reconciliation source")
}

func TestRepository_FindByID_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	id := uuid.New()
	contextID := uuid.New()
	now := time.Now().UTC()
	configJSON := []byte(`{"key":"value"}`)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
	}).AddRow(
		id.String(), contextID.String(), "Test Source", "LEDGER", "LEFT", configJSON, now, now,
	)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_sources WHERE context_id").WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.FindByID(ctx, contextID, id)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, id, result.ID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_FindByID_NotFoundWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	id := uuid.New()
	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_sources WHERE context_id").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	result, err := repo.FindByID(ctx, contextID, id)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestRepository_FindByID_QueryErrorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	id := uuid.New()
	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_sources WHERE context_id").
		WillReturnError(errors.New("connection timeout"))
	mock.ExpectRollback()

	result, err := repo.FindByID(ctx, contextID, id)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "find reconciliation source by id")
}

func TestRepository_FindByContextID_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	contextID := uuid.New()
	id1 := uuid.New()
	id2 := uuid.New()
	now := time.Now().UTC()
	configJSON := []byte(`{}`)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
	}).
		AddRow(id1.String(), contextID.String(), "Source 1", "LEDGER", "LEFT", configJSON, now, now).
		AddRow(id2.String(), contextID.String(), "Source 2", "GATEWAY", "RIGHT", configJSON, now, now)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_sources WHERE context_id").WillReturnRows(rows)
	mock.ExpectCommit()

	results, _, err := repo.FindByContextID(ctx, contextID, "", 10)

	require.NoError(t, err)
	require.Len(t, results, 2)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_FindByContextID_WithCursorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	contextID := uuid.New()
	cursorID := uuid.New()
	id := uuid.New()
	now := time.Now().UTC()
	configJSON := []byte(`{}`)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
	}).AddRow(id.String(), contextID.String(), "Source", "BANK", "LEFT", configJSON, now, now)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_sources WHERE context_id").WillReturnRows(rows)
	mock.ExpectCommit()

	cursorStr := encodeLibCommonsTestCursor(t, cursorID, libHTTP.CursorDirectionNext)
	results, _, err := repo.FindByContextID(ctx, contextID, cursorStr, 10)

	require.NoError(t, err)
	require.Len(t, results, 1)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_FindByContextID_EmptyWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	contextID := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
	})

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_sources WHERE context_id").WillReturnRows(rows)
	mock.ExpectCommit()

	results, _, err := repo.FindByContextID(ctx, contextID, "", 10)

	require.NoError(t, err)
	require.Empty(t, results)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_FindByContextID_QueryErrorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_sources WHERE context_id").
		WillReturnError(errors.New("database error"))
	mock.ExpectRollback()

	results, _, err := repo.FindByContextID(ctx, contextID, "", 10)

	require.Error(t, err)
	require.Nil(t, results)
	require.Contains(t, err.Error(), "find reconciliation sources by context")
}

func TestRepository_FindByContextIDAndType_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	contextID := uuid.New()
	id := uuid.New()
	now := time.Now().UTC()
	configJSON := []byte(`{}`)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
	}).AddRow(id.String(), contextID.String(), "Source", "LEDGER", "LEFT", configJSON, now, now)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_sources WHERE context_id").WillReturnRows(rows)
	mock.ExpectCommit()

	results, _, err := repo.FindByContextIDAndType(
		ctx,
		contextID,
		value_objects.SourceTypeLedger,
		"",
		10,
	)

	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, value_objects.SourceTypeLedger, results[0].Type)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_FindByContextIDAndType_WithCursorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	contextID := uuid.New()
	cursorID := uuid.New()
	id := uuid.New()
	now := time.Now().UTC()
	configJSON := []byte(`{}`)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
	}).AddRow(id.String(), contextID.String(), "Source", "GATEWAY", "RIGHT", configJSON, now, now)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_sources WHERE context_id").WillReturnRows(rows)
	mock.ExpectCommit()

	cursorStr := encodeLibCommonsTestCursor(t, cursorID, libHTTP.CursorDirectionNext)
	results, _, err := repo.FindByContextIDAndType(
		ctx,
		contextID,
		value_objects.SourceTypeGateway,
		cursorStr,
		10,
	)

	require.NoError(t, err)
	require.Len(t, results, 1)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_FindByContextIDAndType_QueryErrorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_sources WHERE context_id").
		WillReturnError(errors.New("network error"))
	mock.ExpectRollback()

	results, _, err := repo.FindByContextIDAndType(
		ctx,
		contextID,
		value_objects.SourceTypeLedger,
		"",
		10,
	)

	require.Error(t, err)
	require.Nil(t, results)
	require.Contains(t, err.Error(), "find reconciliation sources by context and type")
}

func TestRepository_Update_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	entity := createValidSourceEntity(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE reconciliation_sources").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := repo.Update(ctx, entity)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, entity.ID, result.ID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_Update_NotFoundWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	entity := createValidSourceEntity(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE reconciliation_sources").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	result, err := repo.Update(ctx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestRepository_Update_DatabaseErrorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	entity := createValidSourceEntity(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE reconciliation_sources").
		WillReturnError(errors.New("constraint violation"))
	mock.ExpectRollback()

	result, err := repo.Update(ctx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "update reconciliation source")
}

func TestRepository_Delete_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_sources").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.Delete(ctx, contextID, id)

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_Delete_NotFoundWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_sources").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	err := repo.Delete(ctx, contextID, id)

	require.Error(t, err)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestRepository_Delete_DatabaseErrorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_sources").
		WillReturnError(errors.New("foreign key violation"))
	mock.ExpectRollback()

	err := repo.Delete(ctx, contextID, id)

	require.Error(t, err)
	require.Contains(t, err.Error(), "delete reconciliation source")
}

func setupMockForTxTests(t *testing.T) (*Repository, *sql.DB, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db), dbresolver.WithReplicaDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := &testutil.MockInfrastructureProvider{PostgresConn: conn}
	repo, err := NewRepository(provider)
	require.NoError(t, err)

	return repo, db, mock, func() { db.Close() }
}

func TestRepository_CreateWithTx_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	entity := createValidSourceEntity(t)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO reconciliation_sources").WillReturnResult(sqlmock.NewResult(1, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	result, err := repo.CreateWithTx(ctx, tx, entity)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, entity.ID, result.ID)
}

func TestRepository_CreateWithTx_InsertErrorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	entity := createValidSourceEntity(t)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO reconciliation_sources").
		WillReturnError(errors.New("unique violation"))

	tx, err := db.Begin()
	require.NoError(t, err)

	result, err := repo.CreateWithTx(ctx, tx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "create reconciliation source")
}

func TestRepository_UpdateWithTx_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	entity := createValidSourceEntity(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE reconciliation_sources").WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	result, err := repo.UpdateWithTx(ctx, tx, entity)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, entity.ID, result.ID)
}

func TestRepository_UpdateWithTx_NotFoundWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	entity := createValidSourceEntity(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE reconciliation_sources").WillReturnResult(sqlmock.NewResult(0, 0))

	tx, err := db.Begin()
	require.NoError(t, err)

	result, err := repo.UpdateWithTx(ctx, tx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestRepository_DeleteWithTx_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_sources").WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.DeleteWithTx(ctx, tx, contextID, id)

	require.NoError(t, err)
}

func TestRepository_DeleteWithTx_NotFoundWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_sources").WillReturnResult(sqlmock.NewResult(0, 0))

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.DeleteWithTx(ctx, tx, contextID, id)

	require.Error(t, err)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestRepository_DeleteWithTx_DatabaseErrorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, db, mock, cleanup := setupMockForTxTests(t)
	defer cleanup()

	contextID := uuid.New()
	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_sources").
		WillReturnError(errors.New("constraint error"))

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.DeleteWithTx(ctx, tx, contextID, id)

	require.Error(t, err)
	require.Contains(t, err.Error(), "delete reconciliation source")
}

func TestModelConversion_ConfigEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("config with unicode characters", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		entity := &entities.ReconciliationSource{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			Name:      "Test Source",
			Type:      value_objects.SourceTypeLedger,
			Side:      sharedfee.MatchingSideLeft,
			Config: map[string]any{
				"unicode":  "こんにちは",
				"emoji":    "🔥",
				"currency": "€100",
			},
			CreatedAt: now,
			UpdatedAt: now,
		}

		model, err := NewSourcePostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.Equal(t, "こんにちは", resultEntity.Config["unicode"])
		require.Equal(t, "🔥", resultEntity.Config["emoji"])
		require.Equal(t, "€100", resultEntity.Config["currency"])
	})

	t.Run("config with deeply nested structure", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		entity := &entities.ReconciliationSource{
			ID:        uuid.New(),
			ContextID: uuid.New(),
			Name:      "Test Source",
			Type:      value_objects.SourceTypeGateway,
			Side:      sharedfee.MatchingSideRight,
			Config: map[string]any{
				"level1": map[string]any{
					"level2": map[string]any{
						"level3": map[string]any{
							"deepValue": "found",
						},
					},
				},
			},
			CreatedAt: now,
			UpdatedAt: now,
		}

		model, err := NewSourcePostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.NotNil(t, resultEntity.Config["level1"])
	})
}

func TestParseCursor(t *testing.T) {
	t.Parallel()

	t.Run("empty cursor returns default cursor with Direction next", func(t *testing.T) {
		t.Parallel()

		cursor, err := parseCursor("")

		require.NoError(t, err)
		assert.Equal(t, libHTTP.CursorDirectionNext, cursor.Direction)
		assert.Equal(t, libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext}, cursor)
	})

	t.Run("invalid base64 cursor returns libHTTP.ErrInvalidCursor", func(t *testing.T) {
		t.Parallel()

		invalidCursor := "not-valid-base64-!@#$%"

		cursor, err := parseCursor(invalidCursor)

		require.Error(t, err)
		require.ErrorIs(t, err, libHTTP.ErrInvalidCursor)
		assert.Equal(t, libHTTP.Cursor{}, cursor)
	})

	t.Run("valid base64 but invalid JSON structure returns error", func(t *testing.T) {
		t.Parallel()

		malformedCursor := base64.StdEncoding.EncodeToString([]byte("invalid-json"))

		cursor, err := parseCursor(malformedCursor)

		require.Error(t, err)
		require.ErrorIs(t, err, libHTTP.ErrInvalidCursor)
		assert.Equal(t, libHTTP.Cursor{}, cursor)
	})

	t.Run("valid cursor with Direction next is handled correctly", func(t *testing.T) {
		t.Parallel()

		cursorID := uuid.New()
		cursorJSON := `{"id":"` + cursorID.String() + `","direction":"next"}`
		validCursor := base64.StdEncoding.EncodeToString([]byte(cursorJSON))

		cursor, err := parseCursor(validCursor)

		require.NoError(t, err)
		assert.Equal(t, libHTTP.CursorDirectionNext, cursor.Direction)
		assert.Equal(t, cursorID.String(), cursor.ID)
	})

	t.Run("valid cursor with Direction prev is handled correctly", func(t *testing.T) {
		t.Parallel()

		cursorID := uuid.New()
		cursorJSON := `{"id":"` + cursorID.String() + `","direction":"prev"}`
		validCursor := base64.StdEncoding.EncodeToString([]byte(cursorJSON))

		cursor, err := parseCursor(validCursor)

		require.NoError(t, err)
		assert.Equal(t, libHTTP.CursorDirectionPrev, cursor.Direction)
		assert.Equal(t, cursorID.String(), cursor.ID)
	})
}

func TestRepository_FindByContextID_InvalidCursor(t *testing.T) {
	t.Parallel()

	repo, _, cleanup := setupMock(t)
	defer cleanup()

	ctx := context.Background()
	contextID := uuid.New()

	t.Run("invalid base64 cursor returns error", func(t *testing.T) {
		t.Parallel()

		invalidCursor := "not-valid-base64-!@#$%"

		results, pagination, err := repo.FindByContextID(ctx, contextID, invalidCursor, 10)

		require.Error(t, err)
		require.ErrorIs(t, err, libHTTP.ErrInvalidCursor)
		assert.Nil(t, results)
		assert.Empty(t, pagination.Next)
		assert.Empty(t, pagination.Prev)
	})

	t.Run("malformed JSON cursor returns error", func(t *testing.T) {
		t.Parallel()

		malformedCursor := base64.StdEncoding.EncodeToString([]byte("invalid-json"))

		results, pagination, err := repo.FindByContextID(ctx, contextID, malformedCursor, 10)

		require.Error(t, err)
		require.ErrorIs(t, err, libHTTP.ErrInvalidCursor)
		assert.Nil(t, results)
		assert.Empty(t, pagination.Next)
		assert.Empty(t, pagination.Prev)
	})
}

// TestScanSource_InvalidPersistedSide verifies adapter boundary behavior when
// the database contains an unrecognized side value. The toDomain conversion at
// source.go:96-99 casts any string to MatchingSide without validation.
// This test ensures the scan path does not panic and faithfully passes the raw
// value through, leaving domain-level validation to the caller.
func TestScanSource_InvalidPersistedSide(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()
	id := uuid.New()
	contextID := uuid.New()
	configJSON := []byte(`{}`)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
	}).AddRow(
		id.String(), contextID.String(), "Test Source", "LEDGER", "INVALID", configJSON, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanSource(sqlRows)
	require.NoError(t, err, "scanSource must not panic or error on unrecognized side value")
	require.NotNil(t, result)
	require.Equal(t, id, result.ID)

	// The adapter faithfully passes through the raw string; domain validation
	// (MatchingSide.IsValid()) is the caller's responsibility.
	assert.Equal(t, sharedfee.MatchingSide("INVALID"), result.Side)
	assert.False(t, result.Side.IsValid(), "INVALID side must not pass domain validation")
}

// TestScanSource_EmptyPersistedSide verifies that a NULL side from the database
// produces an empty MatchingSide (zero value).
func TestScanSource_EmptyPersistedSide(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()
	id := uuid.New()
	contextID := uuid.New()
	configJSON := []byte(`{}`)

	// sql.NullString with empty string simulates a NULL column.
	rows := sqlmock.NewRows([]string{
		"id", "context_id", "name", "type", "side", "config", "created_at", "updated_at",
	}).AddRow(
		id.String(), contextID.String(), "Test Source", "LEDGER", nil, configJSON, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanSource(sqlRows)
	require.NoError(t, err, "scanSource must not panic on NULL side")
	require.NotNil(t, result)
	assert.Equal(t, sharedfee.MatchingSide(""), result.Side)
	assert.False(t, result.Side.IsExclusive(), "empty side must not be exclusive")
}

func TestRepository_FindByContextIDAndType_InvalidCursor(t *testing.T) {
	t.Parallel()

	repo, _, cleanup := setupMock(t)
	defer cleanup()

	ctx := context.Background()
	contextID := uuid.New()
	sourceType := value_objects.SourceTypeLedger

	t.Run("invalid base64 cursor returns error", func(t *testing.T) {
		t.Parallel()

		invalidCursor := "not-valid-base64-!@#$%"

		results, pagination, err := repo.FindByContextIDAndType(ctx, contextID, sourceType, invalidCursor, 10)

		require.Error(t, err)
		require.ErrorIs(t, err, libHTTP.ErrInvalidCursor)
		assert.Nil(t, results)
		assert.Empty(t, pagination.Next)
		assert.Empty(t, pagination.Prev)
	})

	t.Run("malformed JSON cursor returns error", func(t *testing.T) {
		t.Parallel()

		malformedCursor := base64.StdEncoding.EncodeToString([]byte("invalid-json"))

		results, pagination, err := repo.FindByContextIDAndType(ctx, contextID, sourceType, malformedCursor, 10)

		require.Error(t, err)
		require.ErrorIs(t, err, libHTTP.ErrInvalidCursor)
		assert.Nil(t, results)
		assert.Empty(t, pagination.Next)
		assert.Empty(t, pagination.Prev)
	})
}
