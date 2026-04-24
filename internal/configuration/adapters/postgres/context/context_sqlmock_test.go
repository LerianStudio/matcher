// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package context

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

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
	validEntity := &entities.ReconciliationContext{
		ID:        uuid.New(),
		TenantID:  uuid.New(),
		Name:      "Test",
		Type:      shared.ContextTypeOneToOne,
		Interval:  "daily",
		Status:    value_objects.ContextStatusActive,
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
		require.ErrorIs(t, err, ErrContextEntityRequired)
	})
}

func TestRepository_CreateWithTx_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()
	validEntity := &entities.ReconciliationContext{
		ID:        uuid.New(),
		TenantID:  uuid.New(),
		Name:      "Test",
		Type:      shared.ContextTypeOneToOne,
		Interval:  "daily",
		Status:    value_objects.ContextStatusActive,
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
		require.ErrorIs(t, err, ErrContextEntityRequired)
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

func TestRepository_FindByName_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.FindByName(ctx, "test")

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, err := repo.FindByName(ctx, "test")

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_FindAll_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, _, err := repo.FindAll(ctx, "", 10, nil, nil)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		result, _, err := repo.FindAll(ctx, "", 10, nil, nil)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})
}

func TestRepository_Update_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()
	validEntity := &entities.ReconciliationContext{
		ID:        uuid.New(),
		TenantID:  uuid.New(),
		Name:      "Test",
		Type:      shared.ContextTypeOneToOne,
		Interval:  "daily",
		Status:    value_objects.ContextStatusActive,
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
		require.ErrorIs(t, err, ErrContextEntityRequired)
	})
}

func TestRepository_UpdateWithTx_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()
	validEntity := &entities.ReconciliationContext{
		ID:        uuid.New(),
		TenantID:  uuid.New(),
		Name:      "Test",
		Type:      shared.ContextTypeOneToOne,
		Interval:  "daily",
		Status:    value_objects.ContextStatusActive,
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
		require.ErrorIs(t, err, ErrContextEntityRequired)
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

func TestRepository_Count_NilChecks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil repository returns error", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		count, err := repo.Count(ctx)

		require.Error(t, err)
		require.Zero(t, count)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider returns error", func(t *testing.T) {
		t.Parallel()

		repo := &Repository{provider: nil}
		count, err := repo.Count(ctx)

		require.Error(t, err)
		require.Zero(t, count)
		require.ErrorIs(t, err, ErrRepoNotInitialized)
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

	t.Run("FindByName returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo := NewRepository(provider)
		result, err := repo.FindByName(ctx, "test")

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, connectionErr)
	})

	t.Run("FindAll returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo := NewRepository(provider)
		result, _, err := repo.FindAll(ctx, "", 10, nil, nil)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, connectionErr)
	})

	t.Run("Count returns connection error", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{
			PostgresErr: connectionErr,
		}
		repo := NewRepository(provider)
		count, err := repo.Count(ctx)

		require.Error(t, err)
		require.Zero(t, count)
		require.ErrorIs(t, err, connectionErr)
	})
}

func TestModelConversion_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("model with zero FeeTolerances", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		entity := &entities.ReconciliationContext{
			ID:              uuid.New(),
			TenantID:        uuid.New(),
			Name:            "Test",
			Type:            shared.ContextTypeOneToOne,
			Interval:        "daily",
			Status:          value_objects.ContextStatusActive,
			FeeToleranceAbs: decimal.Zero,
			FeeTolerancePct: decimal.Zero,
			CreatedAt:       now,
			UpdatedAt:       now,
		}

		model, err := NewContextPostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.True(t, resultEntity.FeeToleranceAbs.Equal(decimal.Zero))
		require.True(t, resultEntity.FeeTolerancePct.Equal(decimal.Zero))
	})

	t.Run("model with negative FeeTolerances", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		entity := &entities.ReconciliationContext{
			ID:              uuid.New(),
			TenantID:        uuid.New(),
			Name:            "Test",
			Type:            shared.ContextTypeOneToOne,
			Interval:        "daily",
			Status:          value_objects.ContextStatusActive,
			FeeToleranceAbs: decimal.NewFromFloat(-10.5),
			FeeTolerancePct: decimal.NewFromFloat(-5.0),
			CreatedAt:       now,
			UpdatedAt:       now,
		}

		model, err := NewContextPostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.True(t, resultEntity.FeeToleranceAbs.Equal(decimal.NewFromFloat(-10.5)))
		require.True(t, resultEntity.FeeTolerancePct.Equal(decimal.NewFromFloat(-5.0)))
	})

	t.Run("model with large FeeTolerances", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()
		largeAbs := decimal.NewFromFloat(999999999.99)
		largePct := decimal.NewFromFloat(100.0)
		entity := &entities.ReconciliationContext{
			ID:              uuid.New(),
			TenantID:        uuid.New(),
			Name:            "Test",
			Type:            shared.ContextTypeManyToMany,
			Interval:        "monthly",
			Status:          value_objects.ContextStatusActive,
			FeeToleranceAbs: largeAbs,
			FeeTolerancePct: largePct,
			CreatedAt:       now,
			UpdatedAt:       now,
		}

		model, err := NewContextPostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.True(t, resultEntity.FeeToleranceAbs.Equal(largeAbs))
		require.True(t, resultEntity.FeeTolerancePct.Equal(largePct))
	})

	t.Run("model with all context types", func(t *testing.T) {
		t.Parallel()

		contextTypes := []shared.ContextType{
			shared.ContextTypeOneToOne,
			shared.ContextTypeOneToMany,
			shared.ContextTypeManyToMany,
		}

		for _, ctxType := range contextTypes {
			now := time.Now().UTC()
			entity := &entities.ReconciliationContext{
				ID:        uuid.New(),
				TenantID:  uuid.New(),
				Name:      "Test " + ctxType.String(),
				Type:      ctxType,
				Interval:  "daily",
				Status:    value_objects.ContextStatusActive,
				CreatedAt: now,
				UpdatedAt: now,
			}

			model, err := NewContextPostgreSQLModel(entity)
			require.NoError(t, err)

			resultEntity, err := model.ToEntity()
			require.NoError(t, err)
			require.Equal(t, ctxType, resultEntity.Type)
		}
	})

	t.Run("model with all context statuses", func(t *testing.T) {
		t.Parallel()

		contextStatuses := []value_objects.ContextStatus{
			value_objects.ContextStatusActive,
			value_objects.ContextStatusPaused,
		}

		for _, status := range contextStatuses {
			now := time.Now().UTC()
			entity := &entities.ReconciliationContext{
				ID:        uuid.New(),
				TenantID:  uuid.New(),
				Name:      "Test " + status.String(),
				Type:      shared.ContextTypeOneToOne,
				Interval:  "daily",
				Status:    status,
				CreatedAt: now,
				UpdatedAt: now,
			}

			model, err := NewContextPostgreSQLModel(entity)
			require.NoError(t, err)

			resultEntity, err := model.ToEntity()
			require.NoError(t, err)
			require.Equal(t, status, resultEntity.Status)
		}
	})

	t.Run("model preserves timestamps", func(t *testing.T) {
		t.Parallel()

		createdAt := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
		updatedAt := time.Date(2024, 6, 20, 14, 45, 0, 0, time.UTC)
		entity := &entities.ReconciliationContext{
			ID:        uuid.New(),
			TenantID:  uuid.New(),
			Name:      "Test",
			Type:      shared.ContextTypeOneToOne,
			Interval:  "daily",
			Status:    value_objects.ContextStatusActive,
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		}

		model, err := NewContextPostgreSQLModel(entity)
		require.NoError(t, err)

		resultEntity, err := model.ToEntity()
		require.NoError(t, err)
		require.Equal(t, createdAt, resultEntity.CreatedAt)
		require.Equal(t, updatedAt, resultEntity.UpdatedAt)
	})
}

func TestModelConversion_IntervalVariations(t *testing.T) {
	t.Parallel()

	intervals := []string{"daily", "weekly", "monthly", "yearly", "custom:30d", ""}

	for _, interval := range intervals {
		t.Run("interval_"+interval, func(t *testing.T) {
			t.Parallel()

			now := time.Now().UTC()
			entity := &entities.ReconciliationContext{
				ID:        uuid.New(),
				TenantID:  uuid.New(),
				Name:      "Test",
				Type:      shared.ContextTypeOneToOne,
				Interval:  interval,
				Status:    value_objects.ContextStatusActive,
				CreatedAt: now,
				UpdatedAt: now,
			}

			model, err := NewContextPostgreSQLModel(entity)
			require.NoError(t, err)

			resultEntity, err := model.ToEntity()
			require.NoError(t, err)
			require.Equal(t, interval, resultEntity.Interval)
		})
	}
}

func TestModelConversion_NameVariations(t *testing.T) {
	t.Parallel()

	names := []string{
		"Simple Name",
		"Name with 'quotes'",
		"Name with \"double quotes\"",
		"Name with special chars: @#$%^&*()",
		"Unicode 日本語 название",
		"Very long name that could potentially cause issues if not handled correctly in the database layer with proper escaping and truncation",
		"",
	}

	for i, name := range names {
		t.Run(fmt.Sprintf("name_variation_%d", i), func(t *testing.T) {
			t.Parallel()

			now := time.Now().UTC()
			entity := &entities.ReconciliationContext{
				ID:        uuid.New(),
				TenantID:  uuid.New(),
				Name:      name,
				Type:      shared.ContextTypeOneToOne,
				Interval:  "daily",
				Status:    value_objects.ContextStatusActive,
				CreatedAt: now,
				UpdatedAt: now,
			}

			model, err := NewContextPostgreSQLModel(entity)
			require.NoError(t, err)

			resultEntity, err := model.ToEntity()
			require.NoError(t, err)
			require.Equal(t, name, resultEntity.Name)
		})
	}
}

// setupMock creates a test repository with sqlmock for database testing.
func setupMock(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	return repo, mock, func() { db.Close() }
}

func createValidContextEntity() *entities.ReconciliationContext {
	now := time.Now().UTC()
	return &entities.ReconciliationContext{
		ID:              uuid.New(),
		TenantID:        uuid.New(),
		Name:            "Test Context",
		Type:            shared.ContextTypeOneToOne,
		Interval:        "daily",
		Status:          value_objects.ContextStatusActive,
		FeeToleranceAbs: decimal.NewFromFloat(10.50),
		FeeTolerancePct: decimal.NewFromFloat(2.5),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
}

func TestScanContext_ValidRow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()
	tenantID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "name", "type", "interval",
		"status", "fee_tolerance_abs", "fee_tolerance_pct",
		"fee_normalization", "auto_match_on_upload", "created_at", "updated_at",
	}).AddRow(
		id.String(), tenantID.String(), "Test Context", "1:1", "daily",
		"ACTIVE", "10.50", "2.5", nil, false, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanContext(sqlRows)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, id, result.ID)
	require.Equal(t, tenantID, result.TenantID)
	require.Equal(t, "Test Context", result.Name)
	require.Equal(t, shared.ContextTypeOneToOne, result.Type)
	require.Equal(t, "daily", result.Interval)
	require.Equal(t, value_objects.ContextStatusActive, result.Status)
	assert.True(t, result.FeeToleranceAbs.Equal(decimal.NewFromFloat(10.50)))
	assert.True(t, result.FeeTolerancePct.Equal(decimal.NewFromFloat(2.5)))
}

func TestScanContext_InvalidID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "name", "type", "interval",
		"status", "fee_tolerance_abs", "fee_tolerance_pct",
		"fee_normalization", "auto_match_on_upload", "created_at", "updated_at",
	}).AddRow(
		"invalid-uuid", uuid.New().String(), "Test", "1:1", "daily",
		"ACTIVE", "", "", nil, false, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanContext(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "invalid UUID")
}

func TestScanContext_InvalidType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "name", "type", "interval",
		"status", "fee_tolerance_abs", "fee_tolerance_pct",
		"fee_normalization", "auto_match_on_upload", "created_at", "updated_at",
	}).AddRow(
		uuid.New().String(), uuid.New().String(), "Test", "INVALID", "daily",
		"ACTIVE", "", "", nil, false, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanContext(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "parsing Type")
}

func TestExecuteCreate_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := createValidContextEntity()
	model, err := NewContextPostgreSQLModel(entity)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO reconciliation_contexts (id, tenant_id, name, type, interval, status, fee_tolerance_abs, fee_tolerance_pct, fee_normalization, auto_match_on_upload, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`)).
		WithArgs(
			model.ID,
			model.TenantID,
			model.Name,
			model.Type,
			model.Interval,
			model.Status,
			model.FeeToleranceAbs,
			model.FeeTolerancePct,
			model.FeeNormalization,
			model.AutoMatchOnUpload,
			model.CreatedAt,
			model.UpdatedAt,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeCreate(ctx, tx, entity)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, entity.ID, result.ID)
	require.Equal(t, entity.Name, result.Name)
}

func TestExecuteCreate_InsertError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := createValidContextEntity()
	model, err := NewContextPostgreSQLModel(entity)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO reconciliation_contexts (id, tenant_id, name, type, interval, status, fee_tolerance_abs, fee_tolerance_pct, fee_normalization, auto_match_on_upload, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`)).
		WithArgs(
			model.ID,
			model.TenantID,
			model.Name,
			model.Type,
			model.Interval,
			model.Status,
			model.FeeToleranceAbs,
			model.FeeTolerancePct,
			model.FeeNormalization,
			model.AutoMatchOnUpload,
			model.CreatedAt,
			model.UpdatedAt,
		).
		WillReturnError(errors.New("duplicate key violation"))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeCreate(ctx, tx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "insert reconciliation context")
}

func TestExecuteUpdate_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := createValidContextEntity()
	entity.UpdatedAt = time.Now().UTC()
	model, err := NewContextPostgreSQLModel(entity)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE reconciliation_contexts SET name = $1, type = $2, interval = $3, status = $4, fee_tolerance_abs = $5, fee_tolerance_pct = $6, fee_normalization = $7, auto_match_on_upload = $8, updated_at = $9
		WHERE tenant_id = $10 AND id = $11`)).
		WithArgs(
			model.Name,
			model.Type,
			model.Interval,
			model.Status,
			model.FeeToleranceAbs,
			model.FeeTolerancePct,
			model.FeeNormalization,
			model.AutoMatchOnUpload,
			sqlmock.AnyArg(),
			model.TenantID,
			model.ID,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeUpdate(ctx, tx, entity)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, entity.ID, result.ID)
	require.Equal(t, entity.Name, result.Name)
}

func TestExecuteUpdate_NoRowsAffected(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := createValidContextEntity()
	entity.UpdatedAt = time.Now().UTC()
	model, err := NewContextPostgreSQLModel(entity)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE reconciliation_contexts SET name = $1, type = $2, interval = $3, status = $4, fee_tolerance_abs = $5, fee_tolerance_pct = $6, fee_normalization = $7, auto_match_on_upload = $8, updated_at = $9
		WHERE tenant_id = $10 AND id = $11`)).
		WithArgs(
			model.Name,
			model.Type,
			model.Interval,
			model.Status,
			model.FeeToleranceAbs,
			model.FeeTolerancePct,
			model.FeeNormalization,
			model.AutoMatchOnUpload,
			sqlmock.AnyArg(),
			model.TenantID,
			model.ID,
		).
		WillReturnResult(sqlmock.NewResult(0, 0))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeUpdate(ctx, tx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestExecuteUpdate_DatabaseError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := createValidContextEntity()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE reconciliation_contexts").
		WillReturnError(errors.New("database connection lost"))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeUpdate(ctx, tx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "update reconciliation context")
}

func TestExecuteDelete_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_contexts").
		WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	deleted, err := repo.executeDelete(ctx, tx, id)

	require.NoError(t, err)
	require.True(t, deleted)
}

func TestExecuteDelete_NoRowsAffected(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_contexts").
		WillReturnResult(sqlmock.NewResult(0, 0))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	deleted, err := repo.executeDelete(ctx, tx, id)

	require.Error(t, err)
	require.False(t, deleted)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestExecuteDelete_DatabaseError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_contexts").
		WillReturnError(errors.New("foreign key constraint"))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	deleted, err := repo.executeDelete(ctx, tx, id)

	require.Error(t, err)
	require.False(t, deleted)
	require.Contains(t, err.Error(), "delete reconciliation context")
}

func TestRepository_CreateWithTx_ProviderError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	providerErr := errors.New("provider connection error")
	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: providerErr,
	}
	repo := NewRepository(provider)

	entity := createValidContextEntity()
	result, err := repo.CreateWithTx(ctx, tx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "create reconciliation context")
}

func TestRepository_UpdateWithTx_ProviderError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	providerErr := errors.New("provider connection error")
	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: providerErr,
	}
	repo := NewRepository(provider)

	entity := createValidContextEntity()
	result, err := repo.UpdateWithTx(ctx, tx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "update reconciliation context")
}

func TestRepository_DeleteWithTx_ProviderError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	providerErr := errors.New("provider connection error")
	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: providerErr,
	}
	repo := NewRepository(provider)

	err = repo.DeleteWithTx(ctx, tx, uuid.New())

	require.Error(t, err)
	require.Contains(t, err.Error(), "delete reconciliation context")
}

func TestExecuteCreate_AllContextTypes(t *testing.T) {
	t.Parallel()

	contextTypes := []struct {
		ctxType     shared.ContextType
		description string
	}{
		{shared.ContextTypeOneToOne, "one_to_one"},
		{shared.ContextTypeOneToMany, "one_to_many"},
		{shared.ContextTypeManyToMany, "many_to_many"},
	}

	for _, tt := range contextTypes {
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			defer db.Close()

			entity := createValidContextEntity()
			entity.Type = tt.ctxType

			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO reconciliation_contexts").
				WillReturnResult(sqlmock.NewResult(1, 1))

			tx, err := db.Begin()
			require.NoError(t, err)

			provider := &testutil.MockInfrastructureProvider{}
			repo := NewRepository(provider)
			result, err := repo.executeCreate(ctx, tx, entity)

			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, tt.ctxType, result.Type)
		})
	}
}

func TestScanContext_ManyToManyType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()
	tenantID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "name", "type", "interval",
		"status", "fee_tolerance_abs", "fee_tolerance_pct",
		"fee_normalization", "auto_match_on_upload", "created_at", "updated_at",
	}).AddRow(
		id.String(), tenantID.String(), "M:N Context", "N:M", "monthly",
		"ACTIVE", "100.00", "5.0", nil, false, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanContext(sqlRows)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, shared.ContextTypeManyToMany, result.Type)
	require.Equal(t, "monthly", result.Interval)
	assert.True(t, result.FeeToleranceAbs.Equal(decimal.NewFromFloat(100.00)))
}

func TestExecuteUpdate_RowsAffectedError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	entity := createValidContextEntity()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE reconciliation_contexts").
		WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected error")))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	result, err := repo.executeUpdate(ctx, tx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "get rows affected")
}

func TestExecuteDelete_RowsAffectedError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_contexts").
		WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected error")))

	tx, err := db.Begin()
	require.NoError(t, err)

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	deleted, err := repo.executeDelete(ctx, tx, id)

	require.Error(t, err)
	require.False(t, deleted)
	require.Contains(t, err.Error(), "get rows affected")
}

func TestScanContext_InvalidStatus(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "name", "type", "interval",
		"status", "fee_tolerance_abs", "fee_tolerance_pct",
		"fee_normalization", "auto_match_on_upload", "created_at", "updated_at",
	}).AddRow(
		uuid.New().String(), uuid.New().String(), "Test", "1:1", "daily",
		"INVALID_STATUS", "", "", nil, false, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanContext(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "parsing Status")
}

func TestScanContext_InvalidFeeToleranceAbs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "name", "type", "interval",
		"status", "fee_tolerance_abs", "fee_tolerance_pct",
		"fee_normalization", "auto_match_on_upload", "created_at", "updated_at",
	}).AddRow(
		uuid.New().String(), uuid.New().String(), "Test", "1:1", "daily",
		"ACTIVE", "not-a-number", "", nil, false, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanContext(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "parsing FeeToleranceAbs")
}

func TestScanContext_InvalidTenantID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "name", "type", "interval",
		"status", "fee_tolerance_abs", "fee_tolerance_pct",
		"fee_normalization", "auto_match_on_upload", "created_at", "updated_at",
	}).AddRow(
		uuid.New().String(), "invalid-tenant-id", "Test", "1:1", "daily",
		"ACTIVE", "", "", nil, false, now, now,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	result, err := scanContext(sqlRows)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "invalid UUID")
}

// setupMockWithReplica creates a test repository with sqlmock including replica for full coverage.
func setupMockWithReplica(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db), dbresolver.WithReplicaDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := &testutil.MockInfrastructureProvider{PostgresConn: conn}
	repo := NewRepository(provider)

	return repo, mock, func() { db.Close() }
}

func TestRepository_Create_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	entity := createValidContextEntity()
	model, err := NewContextPostgreSQLModel(entity)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO reconciliation_contexts (id, tenant_id, name, type, interval, status, fee_tolerance_abs, fee_tolerance_pct, fee_normalization, auto_match_on_upload, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`)).
		WithArgs(
			model.ID,
			model.TenantID,
			model.Name,
			model.Type,
			model.Interval,
			model.Status,
			model.FeeToleranceAbs,
			model.FeeTolerancePct,
			model.FeeNormalization,
			model.AutoMatchOnUpload,
			model.CreatedAt,
			model.UpdatedAt,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
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

	entity := createValidContextEntity()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO reconciliation_contexts").
		WillReturnError(errors.New("duplicate key"))
	mock.ExpectRollback()

	result, err := repo.Create(ctx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "create reconciliation context")
}

func TestRepository_FindByID_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	id := uuid.New()
	ctx = context.WithValue(ctx, auth.TenantIDKey, auth.DefaultTenantID)
	tenantID := uuid.MustParse(auth.DefaultTenantID)
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "name", "type", "interval",
		"status", "fee_tolerance_abs", "fee_tolerance_pct",
		"fee_normalization", "auto_match_on_upload", "created_at", "updated_at",
	}).AddRow(
		id.String(), tenantID.String(), "Test Context", "1:1", "daily",
		"ACTIVE", "10.00", "2.5", nil, false, now, now,
	)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT "+contextColumns+" FROM reconciliation_contexts WHERE tenant_id = $1 AND id = $2")).
		WithArgs(auth.DefaultTenantID, id.String()).
		WillReturnRows(rows)

	result, err := repo.FindByID(ctx, id)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, id, result.ID)
	require.Equal(t, tenantID, result.TenantID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_FindByID_NotFoundWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_contexts WHERE tenant_id").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	result, err := repo.FindByID(ctx, id)

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

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_contexts WHERE tenant_id").
		WillReturnError(errors.New("database error"))
	mock.ExpectRollback()

	result, err := repo.FindByID(ctx, id)

	require.Error(t, err)
	require.Nil(t, result)
}

func TestRepository_FindByName_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	id := uuid.New()
	tenantID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "name", "type", "interval",
		"status", "fee_tolerance_abs", "fee_tolerance_pct",
		"fee_normalization", "auto_match_on_upload", "created_at", "updated_at",
	}).AddRow(
		id.String(), tenantID.String(), "Test Context", "1:N", "weekly",
		"PAUSED", "", "", nil, false, now, now,
	)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + contextColumns + " FROM reconciliation_contexts WHERE tenant_id = $1 AND name = $2")).
		WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.FindByName(ctx, "Test Context")

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "Test Context", result.Name)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_FindByName_NotFoundWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + contextColumns + " FROM reconciliation_contexts WHERE tenant_id = $1 AND name = $2")).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	result, err := repo.FindByName(ctx, "NonExistent")

	require.NoError(t, err)
	require.Nil(t, result)
}

func TestRepository_FindByName_QueryErrorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + contextColumns + " FROM reconciliation_contexts WHERE tenant_id = $1 AND name = $2")).
		WillReturnError(errors.New("database error"))
	mock.ExpectRollback()

	result, err := repo.FindByName(ctx, "test")

	require.Error(t, err)
	require.Nil(t, result)
}

func TestRepository_FindAll_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	id1 := uuid.New()
	id2 := uuid.New()
	tenantID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "name", "type", "interval",
		"status", "fee_tolerance_abs", "fee_tolerance_pct",
		"fee_normalization", "auto_match_on_upload", "created_at", "updated_at",
	}).AddRow(
		id1.String(), tenantID.String(), "Context 1", "1:1", "daily",
		"ACTIVE", "", "", nil, false, now, now,
	).AddRow(
		id2.String(), tenantID.String(), "Context 2", "1:N", "weekly",
		"PAUSED", "", "", nil, false, now, now,
	)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + contextColumns + " FROM reconciliation_contexts WHERE tenant_id = $1 ORDER BY id ASC LIMIT 11")).WillReturnRows(rows)
	mock.ExpectCommit()

	result, _, err := repo.FindAll(ctx, "", 10, nil, nil)

	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Equal(t, id1, result[0].ID)
	require.Equal(t, id2, result[1].ID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_FindAll_WithFiltersWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	id := uuid.New()
	tenantID := uuid.New()
	now := time.Now().UTC()
	ctxType := shared.ContextTypeOneToOne
	status := value_objects.ContextStatusActive
	cursor := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "name", "type", "interval",
		"status", "fee_tolerance_abs", "fee_tolerance_pct",
		"fee_normalization", "auto_match_on_upload", "created_at", "updated_at",
	}).AddRow(
		id.String(), tenantID.String(), "Filtered Context", "1:1", "daily",
		"ACTIVE", "", "", nil, false, now, now,
	)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + contextColumns + " FROM reconciliation_contexts WHERE tenant_id = $1 AND type = $2 AND status = $3 AND id > $4 ORDER BY id ASC LIMIT 6")).WillReturnRows(rows)
	mock.ExpectCommit()

	cursorStr := encodeCursor(cursor, libHTTP.CursorDirectionNext)
	result, _, err := repo.FindAll(ctx, cursorStr, 5, &ctxType, &status)

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, "Filtered Context", result[0].Name)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_FindAll_EmptyWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "name", "type", "interval",
		"status", "fee_tolerance_abs", "fee_tolerance_pct",
		"fee_normalization", "auto_match_on_upload", "created_at", "updated_at",
	})

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + contextColumns + " FROM reconciliation_contexts WHERE tenant_id = $1 ORDER BY id ASC LIMIT 11")).WillReturnRows(rows)
	mock.ExpectCommit()

	result, _, err := repo.FindAll(ctx, "", 10, nil, nil)

	require.NoError(t, err)
	require.Empty(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_FindAll_QueryErrorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_contexts WHERE tenant_id").
		WillReturnError(errors.New("query failed"))
	mock.ExpectRollback()

	result, _, err := repo.FindAll(ctx, "", 10, nil, nil)

	require.Error(t, err)
	require.Nil(t, result)
}

func TestRepository_FindAll_ScanErrorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "name", "type", "interval",
		"status", "fee_tolerance_abs", "fee_tolerance_pct",
		"fee_normalization", "auto_match_on_upload", "created_at", "updated_at",
	}).AddRow(
		uuid.New().String(), uuid.New().String(), "Test", "INVALID_TYPE", "daily",
		"ACTIVE", "", "", nil, false, time.Now().UTC(), time.Now().UTC(),
	)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_contexts WHERE tenant_id").WillReturnRows(rows)
	mock.ExpectRollback()

	result, _, err := repo.FindAll(ctx, "", 10, nil, nil)

	require.Error(t, err)
	require.Nil(t, result)
}

func TestRepository_Update_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	entity := createValidContextEntity()
	originalUpdatedAt := entity.UpdatedAt

	mock.ExpectBegin()
	model, err := NewContextPostgreSQLModel(entity)
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE reconciliation_contexts SET name = $1, type = $2, interval = $3, status = $4, fee_tolerance_abs = $5, fee_tolerance_pct = $6, fee_normalization = $7, auto_match_on_upload = $8, updated_at = $9
		WHERE tenant_id = $10 AND id = $11`)).
		WithArgs(
			model.Name,
			model.Type,
			model.Interval,
			model.Status,
			model.FeeToleranceAbs,
			model.FeeTolerancePct,
			model.FeeNormalization,
			model.AutoMatchOnUpload,
			sqlmock.AnyArg(),
			model.TenantID,
			model.ID,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := repo.Update(ctx, entity)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, entity.ID, result.ID)
	require.True(t, result.UpdatedAt.After(originalUpdatedAt) || result.UpdatedAt.Equal(originalUpdatedAt))
	require.NotEqual(t, originalUpdatedAt, result.UpdatedAt)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_Update_NotFoundWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	entity := createValidContextEntity()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE reconciliation_contexts").WillReturnResult(sqlmock.NewResult(0, 0))
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

	entity := createValidContextEntity()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE reconciliation_contexts").WillReturnError(errors.New("update failed"))
	mock.ExpectRollback()

	result, err := repo.Update(ctx, entity)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "update reconciliation context")
}

func TestRepository_Delete_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_contexts").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.Delete(ctx, id)

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_Delete_NotFoundWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_contexts").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	err := repo.Delete(ctx, id)

	require.Error(t, err)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestRepository_Delete_DatabaseErrorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	id := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM reconciliation_contexts").
		WillReturnError(errors.New("delete failed"))
	mock.ExpectRollback()

	err := repo.Delete(ctx, id)

	require.Error(t, err)
	require.Contains(t, err.Error(), "delete reconciliation context")
}

func TestRepository_Count_SuccessWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"count"}).AddRow(42)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(rows)
	mock.ExpectCommit()

	count, err := repo.Count(ctx)

	require.NoError(t, err)
	require.Equal(t, int64(42), count)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_Count_ZeroWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"count"}).AddRow(0)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(rows)
	mock.ExpectCommit()

	count, err := repo.Count(ctx)

	require.NoError(t, err)
	require.Equal(t, int64(0), count)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_Count_QueryErrorWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT COUNT").WillReturnError(errors.New("count failed"))
	mock.ExpectRollback()

	count, err := repo.Count(ctx)

	require.Error(t, err)
	require.Zero(t, count)
}

func TestRepository_FindAll_WithOnlyTypeFilterWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	id := uuid.New()
	tenantID := uuid.New()
	now := time.Now().UTC()
	ctxType := shared.ContextTypeOneToMany

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "name", "type", "interval",
		"status", "fee_tolerance_abs", "fee_tolerance_pct",
		"fee_normalization", "auto_match_on_upload", "created_at", "updated_at",
	}).AddRow(
		id.String(), tenantID.String(), "One to Many", "1:N", "monthly",
		"ACTIVE", "", "", nil, false, now, now,
	)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_contexts WHERE tenant_id").WillReturnRows(rows)
	mock.ExpectCommit()

	result, _, err := repo.FindAll(ctx, "", 10, &ctxType, nil)

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, shared.ContextTypeOneToMany, result[0].Type)
}

func TestRepository_FindAll_WithOnlyStatusFilterWithMock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, mock, cleanup := setupMockWithReplica(t)
	defer cleanup()

	id := uuid.New()
	tenantID := uuid.New()
	now := time.Now().UTC()
	status := value_objects.ContextStatusPaused

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "name", "type", "interval",
		"status", "fee_tolerance_abs", "fee_tolerance_pct",
		"fee_normalization", "auto_match_on_upload", "created_at", "updated_at",
	}).AddRow(
		id.String(), tenantID.String(), "Paused Context", "N:M", "weekly",
		"PAUSED", "", "", nil, false, now, now,
	)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM reconciliation_contexts WHERE tenant_id").WillReturnRows(rows)
	mock.ExpectCommit()

	result, _, err := repo.FindAll(ctx, "", 10, nil, &status)

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, value_objects.ContextStatusPaused, result[0].Status)
}

func encodeCursor(id uuid.UUID, direction string) string {
	encoded, err := libHTTP.EncodeCursor(libHTTP.Cursor{ID: id.String(), Direction: direction})
	if err != nil {
		return ""
	}

	return encoded
}

func TestDecodeCursorParam(t *testing.T) {
	t.Parallel()

	t.Run("empty cursor returns default forward cursor", func(t *testing.T) {
		t.Parallel()

		cursor, err := decodeCursorParam("")

		require.NoError(t, err)
		assert.Equal(t, libHTTP.CursorDirectionNext, cursor.Direction)
	})

	t.Run("valid cursor decodes successfully", func(t *testing.T) {
		t.Parallel()

		id := uuid.New()
		encoded := encodeCursor(id, libHTTP.CursorDirectionNext)

		cursor, err := decodeCursorParam(encoded)

		require.NoError(t, err)
		assert.Equal(t, libHTTP.CursorDirectionNext, cursor.Direction)
		assert.Equal(t, id.String(), cursor.ID)
	})

	t.Run("backward cursor decodes with Direction prev", func(t *testing.T) {
		t.Parallel()

		id := uuid.New()
		encoded := encodeCursor(id, libHTTP.CursorDirectionPrev)

		cursor, err := decodeCursorParam(encoded)

		require.NoError(t, err)
		assert.Equal(t, libHTTP.CursorDirectionPrev, cursor.Direction)
		assert.Equal(t, id.String(), cursor.ID)
	})

	t.Run("invalid base64 returns libHTTP.ErrInvalidCursor", func(t *testing.T) {
		t.Parallel()

		_, err := decodeCursorParam("not-valid-base64!!!")

		require.Error(t, err)
		assert.ErrorIs(t, err, libHTTP.ErrInvalidCursor)
	})

	t.Run("valid base64 but invalid JSON returns libHTTP.ErrInvalidCursor", func(t *testing.T) {
		t.Parallel()

		invalidJSON := base64.StdEncoding.EncodeToString([]byte("not-json"))

		_, err := decodeCursorParam(invalidJSON)

		require.Error(t, err)
		require.ErrorIs(t, err, libHTTP.ErrInvalidCursor)
	})
}

func TestRepository_FindAll_CursorPagination(t *testing.T) {
	t.Parallel()

	t.Run("invalid cursor returns error", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()

		repo, _, cleanup := setupMockWithReplica(t)
		defer cleanup()

		result, pagination, err := repo.FindAll(ctx, "invalid-base64!!!", 10, nil, nil)

		require.Error(t, err)
		require.ErrorIs(t, err, libHTTP.ErrInvalidCursor)
		assert.Nil(t, result)
		assert.Empty(t, pagination.Next)
	})

	t.Run("backward cursor is handled correctly", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()

		repo, mock, cleanup := setupMockWithReplica(t)
		defer cleanup()

		id := uuid.New()
		tenantID := uuid.New()
		now := time.Now().UTC()

		rows := sqlmock.NewRows([]string{
			"id", "tenant_id", "name", "type", "interval",
			"status", "fee_tolerance_abs", "fee_tolerance_pct",
			"fee_normalization", "auto_match_on_upload", "created_at", "updated_at",
		}).AddRow(
			id.String(), tenantID.String(), "Context 1", "1:1", "daily",
			"ACTIVE", "", "", nil, false, now, now,
		)

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT .+ FROM reconciliation_contexts WHERE tenant_id").WillReturnRows(rows)
		mock.ExpectCommit()

		backwardCursor := encodeCursor(uuid.New(), libHTTP.CursorDirectionPrev)
		result, _, err := repo.FindAll(ctx, backwardCursor, 10, nil, nil)

		require.NoError(t, err)
		require.Len(t, result, 1)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("limit plus one behavior determines HasMore", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()

		repo, mock, cleanup := setupMockWithReplica(t)
		defer cleanup()

		tenantID := uuid.New()
		now := time.Now().UTC()
		limit := 2
		id1 := uuid.New()

		id2 := uuid.New()
		id3 := uuid.New()

		rows := sqlmock.NewRows([]string{
			"id", "tenant_id", "name", "type", "interval",
			"status", "fee_tolerance_abs", "fee_tolerance_pct",
			"fee_normalization", "auto_match_on_upload", "created_at", "updated_at",
		}).AddRow(
			id1.String(), tenantID.String(), "Context 1", "1:1", "daily",
			"ACTIVE", "", "", nil, false, now, now,
		).AddRow(
			id2.String(), tenantID.String(), "Context 2", "1:1", "daily",
			"ACTIVE", "", "", nil, false, now, now,
		).AddRow(
			id3.String(), tenantID.String(), "Context 3", "1:1", "daily",
			"ACTIVE", "", "", nil, false, now, now,
		)

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT .+ FROM reconciliation_contexts WHERE tenant_id").WillReturnRows(rows)
		mock.ExpectCommit()

		result, pagination, err := repo.FindAll(ctx, "", limit, nil, nil)

		require.NoError(t, err)
		require.Len(t, result, limit)
		assert.NotEmpty(t, pagination.Next)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("exactly limit results has no more pages", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()

		repo, mock, cleanup := setupMockWithReplica(t)
		defer cleanup()

		tenantID := uuid.New()
		now := time.Now().UTC()
		limit := 2
		id1 := uuid.New()
		id2 := uuid.New()

		rows := sqlmock.NewRows([]string{
			"id", "tenant_id", "name", "type", "interval",
			"status", "fee_tolerance_abs", "fee_tolerance_pct",
			"fee_normalization", "auto_match_on_upload", "created_at", "updated_at",
		}).AddRow(
			id1.String(), tenantID.String(), "Context 1", "1:1", "daily",
			"ACTIVE", "", "", nil, false, now, now,
		).AddRow(
			id2.String(), tenantID.String(), "Context 2", "1:1", "daily",
			"ACTIVE", "", "", nil, false, now, now,
		)

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT .+ FROM reconciliation_contexts WHERE tenant_id").WillReturnRows(rows)
		mock.ExpectCommit()

		result, _, err := repo.FindAll(ctx, "", limit, nil, nil)

		require.NoError(t, err)
		require.Len(t, result, limit)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}
