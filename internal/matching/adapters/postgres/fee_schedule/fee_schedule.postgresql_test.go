// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fee_schedule

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Compile-time interface conformance check.
var _ sharedPorts.FeeScheduleRepository = (*Repository)(nil)

func TestNewRepository(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	require.NotNil(t, repo)
	assert.Equal(t, provider, repo.provider)
}

func TestNewRepository_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)

	require.NotNil(t, repo)
	assert.Nil(t, repo.provider)
}

func TestRepository_Create_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	result, err := repo.Create(context.Background(), nil)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_Create_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	result, err := repo.Create(context.Background(), nil)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_GetByID_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	result, err := repo.GetByID(context.Background(), uuid.New())

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_GetByID_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	result, err := repo.GetByID(context.Background(), uuid.New())

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_Update_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	result, err := repo.Update(context.Background(), nil)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_Update_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	result, err := repo.Update(context.Background(), nil)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_Delete_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	err := repo.Delete(context.Background(), uuid.New())

	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_Delete_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	err := repo.Delete(context.Background(), uuid.New())

	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_List_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	result, err := repo.List(context.Background(), 100)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_List_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	result, err := repo.List(context.Background(), 100)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_List_DefaultLimit(t *testing.T) {
	t.Parallel()

	var repo *Repository

	// With limit <= 0, should default to 100 internally, but still return
	// ErrRepoNotInitialized because repo is nil
	result, err := repo.List(context.Background(), 0)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_GetByIDs_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	result, err := repo.GetByIDs(context.Background(), []uuid.UUID{uuid.New()})

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_GetByIDs_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	result, err := repo.GetByIDs(context.Background(), []uuid.UUID{uuid.New()})

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_GetByIDs_EmptyIDs(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	result, err := repo.GetByIDs(context.Background(), nil)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result)
}

func TestRepository_GetByIDs_EmptySlice(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	result, err := repo.GetByIDs(context.Background(), []uuid.UUID{})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result)
}

func TestColumns_Constants(t *testing.T) {
	t.Parallel()

	assert.Equal(t,
		"id, tenant_id, name, currency, application_order, rounding_scale, rounding_mode, created_at, updated_at",
		scheduleColumns,
	)
	assert.Equal(t,
		"id, fee_schedule_id, name, priority, structure_type, structure_data, created_at, updated_at",
		itemColumns,
	)
}

func TestRepository_ImplementsInterface(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	assert.NotNil(t, repo)
}

func TestRepository_CreateWithTx_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	result, err := repo.CreateWithTx(context.Background(), nil, nil)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_CreateWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	result, err := repo.CreateWithTx(context.Background(), nil, nil)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_CreateWithTx_NilTx(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	result, err := repo.CreateWithTx(context.Background(), nil, nil)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrInvalidTx)
}

func TestRepository_UpdateWithTx_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	result, err := repo.UpdateWithTx(context.Background(), nil, nil)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_UpdateWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	result, err := repo.UpdateWithTx(context.Background(), nil, nil)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_UpdateWithTx_NilTx(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	result, err := repo.UpdateWithTx(context.Background(), nil, nil)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrInvalidTx)
}

func TestRepository_DeleteWithTx_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	err := repo.DeleteWithTx(context.Background(), nil, uuid.New())

	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_DeleteWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	err := repo.DeleteWithTx(context.Background(), nil, uuid.New())

	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_DeleteWithTx_NilTx(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	err := repo.DeleteWithTx(context.Background(), nil, uuid.New())

	require.ErrorIs(t, err, ErrInvalidTx)
}

func TestRepository_Create_NilModel(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	result, err := repo.Create(context.Background(), nil)

	require.Error(t, err)
	require.Nil(t, result)
}

func TestRepository_Update_NilModel(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	result, err := repo.Update(context.Background(), nil)

	require.Error(t, err)
	require.Nil(t, result)
}

func TestRepository_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		repo        *Repository
		wantErr     bool
		errIs       error
		errContains string
	}{
		{
			name:    "nil repository returns ErrRepoNotInitialized",
			repo:    nil,
			wantErr: true,
			errIs:   ErrRepoNotInitialized,
		},
		{
			name:    "nil provider returns ErrRepoNotInitialized",
			repo:    &Repository{provider: nil},
			wantErr: true,
			errIs:   ErrRepoNotInitialized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			id := uuid.New()

			result, err := tt.repo.GetByID(ctx, id)

			if tt.wantErr {
				require.Error(t, err)

				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}

				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, result)
			}
		})
	}
}
